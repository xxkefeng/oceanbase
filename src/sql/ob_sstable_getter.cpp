/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/

#include "ob_sstable_getter.h"
#include "ob_sql_get_param.h"
#include "sstable/ob_sstable_schema.h"
#include "sstable/ob_sstable_reader.h"
#include "common/serialization.h"
#include "sstable/ob_sstable_row_cache.h"
#include "sstable/ob_sstable_block_index_v2.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_simple_column_indexes_builder.h"
#include "common/ob_record_header.h"
#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"
#include "common/ob_profile_log.h"
#include "chunkserver/ob_tablet_manager.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

#include "common/ob_vector.h"

ObSSTableGetter::ObSSTableGetter() : table_id_(OB_INVALID_ID), rowkey_list_(NULL), columns_(NULL), column_size_(0),
  context_(NULL), is_ups_getter_(false), cache_row_buf_(DEFAULT_ROW_BUF_SIZE),
  block_buf_(DEFAULT_BLOCK_BUF_SIZE)
{
}

ObSSTableGetter::~ObSSTableGetter()
{
}

const ObRowkey* ObSSTableGetter::get_row_key(int64_t idx)
{
  const common::ObRowkey *rowkey = NULL;
  if (NULL == rowkey_list_)
  {
    TBSYS_LOG(ERROR, "rowkey_list_ must not be null");
  }
  else if (idx >=0 && idx < rowkey_list_->count())
  {
    rowkey = &rowkey_list_->at(idx);
  }
  return rowkey;
}


int ObSSTableGetter::initialize(const uint64_t table_id,
    const ObArray<common::ObRowkey>* rowkey_list, const uint64_t *columns,
    const int64_t column_size, const int64_t rowkey_cell_count,
    chunkserver::ObGetThreadContext *context, bool is_ups_getter)
{
  int rc = OB_SUCCESS;
  const sstable::ObSSTableSchema *schema = NULL;

  // %context->readers_count_ may less than %param->get_row_size()
  // in case of tablet not found.
  if (NULL == rowkey_list || NULL == context)
  {
    rc = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "invalid argument rowkey_list=%p, context=%p", rowkey_list, context);
  }
  else if (rowkey_list->count() == 0 || column_size == 0 || !context
      || context->readers_count_ > rowkey_list->count())
  {
    rc = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "invalid argument, get row size: %ld, column size %ld, reader_size %ld",
        rowkey_list->count(), column_size, context->readers_count_);
  }

  if (OB_SUCCESS == rc)
  {
    row_desc_.reset();
    column_index_.reset();
    cur_idx_ = 0;

    table_id_ = table_id;
    rowkey_list_ = rowkey_list;
    columns_ = columns;
    column_size_ = column_size;
    context_ = context;
    is_ups_getter_ = is_ups_getter;

    for (int64_t i = 0; i < context_->readers_count_; i++)
    {
      if (context_->readers_[i])
      {
        schema = context_->readers_[i]->get_schema();
        break;
      }
    }

    if (schema)
    {
      rc = init_by_schema(schema);
    }
  }

  if (OB_SUCCESS == rc)
  {
    rc = build_row_desc(rowkey_cell_count);
  }

  return rc;
}

int ObSSTableGetter::init_by_schema(const sstable::ObSSTableSchema *schema)
{
  int rc = OB_SUCCESS;
  int64_t table_id = table_id_;
  
  if (schema->is_binary_rowkey_format(table_id)
      && OB_SUCCESS != (rc = sstable::get_global_schema_rowkey_info(table_id, rowkey_info_)))
  {
    TBSYS_LOG(ERROR, "old fashion rowkey format, table %ld get rowkey schema failed, rc %d",
        table_id, rc);
  }

  uint64_t group_array[OB_MAX_COLUMN_GROUP_NUMBER];
  int64_t group_size = OB_MAX_COLUMN_GROUP_NUMBER;
  if (OB_SUCCESS == rc)
  {
    if (OB_SUCCESS != (rc = schema->get_table_column_groups_id(
            table_id, group_array, group_size)))
    {
      TBSYS_LOG(ERROR, "get column groups failed, table_id %ld", table_id);
    }
    else if (group_size < 0 || group_size > 1)
    {
      TBSYS_LOG(ERROR, "invalid column group size %ld, we only support on colun group now!",
          group_size);
      rc = group_size ? OB_NOT_SUPPORTED : OB_COLUMN_GROUP_NOT_FOUND;
    }
    else if (group_size == 0)
    {
      group_array[0] = 0;
      group_size = 1;
    }
  }

  if (OB_SUCCESS == rc)
  {
    sstable::ObSimpleColumnIndexesBuilder::GroupIdDesc group;
    group.id_ = group_array[0];
    group.seq_ = 0;
    group.size_ = group_size;

    sstable::ObSimpleColumnIndexesBuilder builder(table_id_,
        *schema, &rowkey_info_, NULL);

    column_index_.set_table_id(table_id_);
    rc = builder.build_input_col_index(column_index_, group, columns_, column_size_);
  }

  return rc;
}

int ObSSTableGetter::build_row_desc(const int64_t rowkey_cell_count)
{
  int rc = OB_SUCCESS;
  row_desc_.reset();

  for (int64_t i = 0; OB_SUCCESS == rc && i < column_size_; ++i)
  {
    rc = row_desc_.add_column_desc(table_id_, columns_[i]);
  }
  if (OB_SUCCESS == rc)
  {
    row_desc_.set_rowkey_cell_count(rowkey_cell_count);
    rc = row_desc_.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
  }

  return rc;
}

int ObSSTableGetter::get_next_row(const common::ObRow *&row)
{
  int rc = OB_SUCCESS;

  if (cur_idx_ >= context_->readers_count_)
  {
    rc = OB_ITER_END;
  }
  else if (OB_SUCCESS == (rc = get_row(row)))
  {
    cur_idx_++;
  }
  return rc;
}

void ObSSTableGetter::get_non_exist_row(const common::ObRowkey &key, const common::ObRow *&row)
{
  row_non_exist_.set_row_desc(row_desc_);
  row_non_exist_.reset(false, is_ups_getter() ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);

  ObObj obj;
  obj.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  row_non_exist_.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, obj);

  // fill rowkey 
  for (int64_t i = 0; i < row_desc_.get_rowkey_cell_count(); i++)
  {
    row_non_exist_.raw_set_cell(i, *(key.get_obj_ptr() + i));
  }

  row = &row_non_exist_;
}

int ObSSTableGetter::row_from_rowcache(const common::ObRow *&row)
{
  OB_ASSERT(context_->row_cache_);

  const ObRowkey *rowkey = get_row_key(cur_idx_);

  sstable::ObSSTableRowCacheKey cache_key(
      context_->readers_[cur_idx_]->get_sstable_id().sstable_file_id_,
      is_ups_getter() ? table_id_ : 0 /* column gorup id */,
      const_cast<ObRowkey&>(*rowkey));
  sstable::ObSSTableRowCacheValue cache_value;

  int rc = context_->row_cache_->get_row(cache_key, cache_value, cache_row_buf_);
  if (OB_SUCCESS == rc)
  {
#ifndef _SSTABLE_NO_STAT_
    OB_STAT_TABLE_INC(SSTABLE, table_id_,
        INDEX_SSTABLE_ROW_CACHE_HIT, 1);
#endif
    if (cache_value.buf_ && cache_value.size_ > 0)
    {
      rc = block_getter_.get_cached_row(cache_value,
          context_->readers_[cur_idx_]->get_trailer().get_row_value_store_style(),
          &row_desc_, is_ups_getter() ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL,
          column_index_, *rowkey, row);
    }
    else
    {
      get_non_exist_row(*rowkey, row);
    }
  }
  else
  {
#ifndef _SSTABLE_NO_STAT_
    OB_STAT_TABLE_INC(SSTABLE, table_id_,
        INDEX_SSTABLE_ROW_CACHE_MISS, 1);
#endif
    rc = OB_SEARCH_NOT_FOUND;
  }

  return rc;
}

int ObSSTableGetter::row_from_block(const common::ObRow *&row)
{
  int rc = OB_SUCCESS;
  const sstable::ObSSTableReader *reader = context_->readers_[cur_idx_];
  const ObRowkey *rowkey = get_row_key(cur_idx_);

  sstable::ObBlockPositionInfo pos;
  sstable::ObBlockIndexPositionInfo info;
  info.sstable_file_id_ = reader->get_sstable_id().sstable_file_id_;
  info.offset_ = reader->get_trailer().get_block_index_record_offset();
  info.size_ = reader->get_trailer().get_block_index_record_size();

  if (info.size_ > 0)
  {
    rc = context_->block_index_cache_->get_single_block_pos_info(info,
        table_id_, 0 /* column group id */,
        *rowkey, sstable::OB_SEARCH_MODE_GREATER_EQUAL, pos);
    if (OB_BEYOND_THE_RANGE == rc)
    {
      rc = OB_SEARCH_NOT_FOUND; // not found in block
    }
  }
  else // block index not exist
  {
    rc = OB_SEARCH_NOT_FOUND;
  }

  // init block getter return OB_SEARCH_NOT_FOUND if rowkey not found.
  if (OB_SUCCESS == rc && OB_SUCCESS == (rc = init_block_getter(pos)))
  {
    rc = block_getter_.get_row(row);
  }

  return rc;
}

int ObSSTableGetter::update_rowcache(bool found_in_block)
{
  OB_ASSERT(context_->row_cache_);
  const ObRowkey *rowkey = get_row_key(cur_idx_);

  sstable::ObSSTableRowCacheKey cache_key(
      context_->readers_[cur_idx_]->get_sstable_id().sstable_file_id_,
      is_ups_getter() ? table_id_ : 0 /* column gorup id */,
      const_cast<ObRowkey&>(*rowkey));
  sstable::ObSSTableRowCacheValue cache_value;

  int rc = OB_SUCCESS;
  if (found_in_block)
  {
    block_getter_.get_cache_row_value(cache_value);
  }

  if (OB_SUCCESS == rc)
  {
    rc = context_->row_cache_->put_row(cache_key, cache_value);
  }

  return rc;
}

int ObSSTableGetter::get_row(const common::ObRow *&row)
{
  int rc = OB_SUCCESS;
  const sstable::ObSSTableReader *reader = context_->readers_[cur_idx_];
  const ObRowkey *rowkey = get_row_key(cur_idx_);

  INIT_PROFILE_LOG_TIMER();

  if (!reader || reader->get_row_count() == 0 || !bloom_filter_may_contain())
  {
    get_non_exist_row(*rowkey, row);
    FILL_TRACE_LOG("row_non_exist");
  }
  else
  {
    // row_from_rowcache() and row_from_block() return OB_SEARCH_NOT_FOUND on rowkey not found.
    rc = OB_SEARCH_NOT_FOUND;
    if (context_->row_cache_)
    {
      rc = row_from_rowcache(row);
      FILL_TRACE_LOG("search_rowcache");//will the trace log too much here?
    }
    if (OB_SEARCH_NOT_FOUND == rc)
    {
      rc = row_from_block(row);
      FILL_TRACE_LOG("read_block");
      if (context_->row_cache_ && (OB_SUCCESS == rc || OB_SEARCH_NOT_FOUND == rc))
      {
        int inner_rc = update_rowcache(OB_SUCCESS == rc);
        if (OB_SUCCESS != inner_rc)
        {
          rc = inner_rc;
        }
        FILL_TRACE_LOG("update_rowcache");
      }
    }

    if (OB_SEARCH_NOT_FOUND == rc)
    {
      get_non_exist_row(*rowkey, row);
      FILL_TRACE_LOG("no_row_found");
      rc = OB_SUCCESS;
    }
  }

  PROFILE_LOG_TIME(DEBUG, "get row, rc %d, rowkey [%s]", rc, to_cstring(*rowkey));

  return rc;
}

// FIXME: baihua: check bloom filter is enabled? to avoid memory copy here.
bool ObSSTableGetter::bloom_filter_may_contain()
{
  int rc = OB_SUCCESS;
  bool ret = true;
  const ObRowkey *rowkey = get_row_key(cur_idx_);
  int64_t pos = 0;
  int64_t buf_size = sizeof(uint64_t) + rowkey->get_serialize_size();
  char *buf = NULL;


  if (OB_SUCCESS == (rc = bf_key_.ensure_space(buf_size, ObModIds::OB_SSTABLE_GET_SCAN)))
  {
    buf = bf_key_.get_buffer();

    // sstable bloom filter key format: see ObSSTableWriter::update_bloom_filter
    if (OB_SUCCESS == (rc = serialization::encode_i16(buf, buf_size, pos, 0))
        && OB_SUCCESS == (rc = serialization::encode_i16(
            buf, buf_size, pos, 0)) // column group id
        && OB_SUCCESS == (rc = serialization::encode_i32(
            buf, buf_size, pos, static_cast<int32_t>(table_id_)))
        && OB_SUCCESS == (rc = rowkey->serialize(buf, buf_size, pos)))
    {
      ret = context_->readers_[cur_idx_]->may_contain(ObString(0,
            static_cast<ObString::obstr_size_t>(buf_size), buf));
    }
  }

  if (OB_SUCCESS != rc)
  {
    TBSYS_LOG(ERROR, "fill bloom filter key failed, result: %d", rc);
  }

  return ret;
}

int ObSSTableGetter::init_block_getter(const sstable::ObBlockPositionInfo &pos)
{
  int rc = OB_SUCCESS;
  sstable::ObBufferHandle handler;
  const sstable::ObSSTableReader *reader = context_->readers_[cur_idx_];
  INIT_PROFILE_LOG_TIMER();

  rc = context_->block_cache_->get_block(reader->get_sstable_id().sstable_file_id_,
      pos.offset_, pos.size_, handler, table_id_);
  if (rc <= 0)
  {
    TBSYS_LOG(WARN, "load block from block cache failed, rc %d", rc);
    rc = OB_ERROR;
  }
  else
  {
    rc = OB_SUCCESS;
  }

  PROFILE_LOG_TIME(DEBUG, "get block data, rc %d", rc);

  ObRecordHeader header;
  const char *buf;
  int64_t buf_size;
  if (OB_SUCCESS == rc && OB_SUCCESS != (rc = ObRecordHeader::get_record_header(
          handler.get_buffer(), pos.size_, header, buf, buf_size)))
  {
    TBSYS_LOG(WARN, "invalid block data, sstable_id %lu, block offset %ld, block size %ld",
        reader->get_sstable_id().sstable_file_id_, pos.offset_, pos.size_);
  }
  if (OB_SUCCESS == rc && OB_SUCCESS == (rc = block_buf_.ensure_space(header.data_length_,
          ObModIds::OB_SSTABLE_GET_SCAN)))
  {
    if (header.is_compress())
    {
      int64_t real_size;
      ObCompressor *compressor =
        const_cast<sstable::ObSSTableReader *>(reader)->get_decompressor();
      if (!compressor)
      {
        TBSYS_LOG(ERROR, "sstable reader get compressor failed");
        rc = OB_ERROR;
      }
      else
      {
        rc = compressor->decompress(buf, buf_size, block_buf_.get_buffer(),
            block_buf_.get_buffer_size(), real_size);
        if (OB_SUCCESS == rc && real_size != header.data_length_)
        {
          TBSYS_LOG(ERROR, "decompressed data lenght %ld and recorded data length %d mismatch",
              real_size, header.data_length_);
          rc = OB_ERROR;
        }
      }
    }
    else
    {
      ::memcpy(block_buf_.get_buffer(), buf, buf_size);
    }

    if (OB_SUCCESS == rc)
    {
      buf = block_buf_.get_buffer();
      buf_size = header.data_length_;
    }
  }

  PROFILE_LOG_TIME(DEBUG, "decompress or memcpy block data, rc %d", rc);

  if (OB_SUCCESS == rc)
  {
    int64_t rowkey_column_count;
    reader->get_schema()->get_rowkey_column_count(table_id_, rowkey_column_count);
    sstable::ObSSTableBlockReader::BlockDataDesc data_desc(&rowkey_info_, rowkey_column_count,
        reader->get_trailer().get_row_value_store_style());

    rc = block_getter_.initialize(get_row_key(cur_idx_), &row_desc_, &column_index_,
        buf, buf_size, data_desc, is_ups_getter() ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);
  }

  PROFILE_LOG_TIME(DEBUG, "init sstable block getter, rc %d", rc);

  return rc;
}

