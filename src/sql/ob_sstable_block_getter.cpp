/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* ob_sstable_block_getter.cpp is for ...
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/

#include "ob_sstable_block_getter.h"
#include "sstable/ob_sstable_trailer.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

ObSSTableBlockGetter::ObSSTableBlockGetter() : block_index_buf_(DEFAULT_INDEX_BUF_SIZE)
{
}

ObSSTableBlockGetter::~ObSSTableBlockGetter()
{
}

int ObSSTableBlockGetter::initialize(const common::ObRowkey *rowkey,
    const common::ObRowDesc *row_desc,
    const sstable::ObSimpleColumnIndexes *column_index,
    const char *buf, const int64_t buf_size,
    const sstable::ObSSTableBlockReader::BlockDataDesc &data_desc,
    common::ObRow::DefaultValue row_def_value)
{
  int rc = OB_SUCCESS; 

  if (!rowkey || !row_desc || !column_index)
  {
    rc = OB_INVALID_ARGUMENT;
  }
  else if (!buf || 0 == buf_size)
  {
    rc = OB_SEARCH_NOT_FOUND;
  }

  if (OB_SUCCESS == rc)
  {
    // Reset row, and set extend column to ObActionFlag::OP_VALID if it exist.
    row_.set_row_desc(*row_desc);
    row_.reset(false, row_def_value);

    int64_t ext_col_idx = row_desc->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
    if (OB_INVALID_INDEX != ext_col_idx)
    {
      ObObj obj;
      obj.set_ext(ObActionFlag::OP_VALID);
      row_.raw_set_cell(ext_col_idx, obj);
    }
  }

  if (OB_SUCCESS == rc)
  {
    rc = block_index_buf_.ensure_space(DEFAULT_INDEX_BUF_SIZE, ObModIds::OB_SSTABLE_GET_SCAN);
  }
  sstable::ObSSTableBlockReader::BlockData data(block_index_buf_.get_buffer(),
      block_index_buf_.get_buffer_size(), buf, buf_size);

  reader_.reset();
  if (OB_SUCCESS == rc && OB_SUCCESS != (rc = reader_.deserialize(data_desc, data)))
  {
    TBSYS_LOG(ERROR, "block deserialize failed, rc %d", rc);
  }
  else if (OB_SUCCESS == rc)
  {
    cur_iter_ = reader_.find(*rowkey);
    if (cur_iter_ == reader_.end())
    {
      TBSYS_LOG(DEBUG, "rowkey [%s] not found in block, rc %d", to_cstring(*rowkey), rc);
      rc = OB_SEARCH_NOT_FOUND;
    }
    else
    {
      rc = reader_.get_row(static_cast<int>(data_desc.store_style_), cur_iter_, false,
          *column_index, row_);
      if (OB_SUCCESS != rc)
      {
        TBSYS_LOG(ERROR, "read row error rowkey [%s] failed, rc %d", to_cstring(*rowkey), rc);
      }
    }
  }

  return rc;
}

int ObSSTableBlockGetter::get_cached_row(const sstable::ObSSTableRowCacheValue &cache_value,
    const int store_style, const common::ObRowDesc *row_desc,
    common::ObRow::DefaultValue row_def_value,
    const sstable::ObSimpleColumnIndexes &column_index,
    const common::ObRowkey &key, const common::ObRow *&row)
{
  OB_ASSERT(cache_value.buf_ && cache_value.size_);

  int rc = OB_SUCCESS;
  int64_t filled = 0;
  int64_t value_index = 0;
  int64_t result_index = 0;
  int64_t pos = 0;
  int64_t not_null_col_num = column_index.get_not_null_column_count();

  // Reset row, and set extend column to ObActionFlag::OP_VALID if it exist.
  row_.set_row_desc(*row_desc);
  row_.reset(false, row_def_value);

  int64_t ext_col_idx = row_desc->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
  if (OB_INVALID_INDEX != ext_col_idx)
  {
    ObObj obj;
    obj.set_ext(ObActionFlag::OP_VALID);
    row_.raw_set_cell(ext_col_idx, obj);
  }

  // traverse rowkey
  while (value_index < key.get_obj_cnt() && filled < not_null_col_num)
  {
    if ((result_index = column_index.find_by_offset(value_index)) >= 0)
    {
      row_.raw_set_cell(result_index, *(key.get_obj_ptr() + value_index));
      filled++;
    }
    value_index++;
  }

  // traverse rowvalue
  if (OB_SUCCESS == rc)
  {
    if (store_style == sstable::OB_SSTABLE_STORE_SPARSE)
    {
      rc = sstable::ObSSTableBlockReader::get_sparse_rowvalue(cache_value.buf_,
          cache_value.buf_ + cache_value.size_, pos, filled, column_index, row_);
    }
    else if (store_style == sstable::OB_SSTABLE_STORE_DENSE)
    {
      rc = sstable::ObSSTableBlockReader::get_dense_rowvalue(cache_value.buf_,
          cache_value.buf_ + cache_value.size_, pos, value_index, filled, column_index, row_);
    }
    else
    {
      TBSYS_LOG(ERROR, "unsupported store style %d", store_style);
      rc = OB_NOT_SUPPORTED;
    }
  }

  if (OB_SUCCESS == rc)
  {
    row = &row_;
  }
  return rc;
}

