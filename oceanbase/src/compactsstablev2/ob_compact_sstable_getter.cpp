/**
 *  (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_get_param.cpp is for compact sstable getter param
 *
 *  Authors:
 *     zian < yunliang.shi@alipay.com >
 *
 */

#include "ob_compact_sstable_getter.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
      ObCompactSSTableGetter::ObCompactSSTableGetter()
        :
          table_id_(OB_INVALID_ID),
          rowkey_list_(NULL),
          context_(NULL),
          cur_row_index_(0),
          uncomp_buf_(NULL),
          uncomp_buf_size_(UNCOMP_BUF_SIZE),
          internal_buf_(NULL),
          internal_buf_size_(INTERNAL_BUF_SIZE)
      {
      }

      const common::ObRowkey* ObCompactSSTableGetter::get_row_key(int64_t idx)
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

      int ObCompactSSTableGetter::initialize(const uint64_t table_id, const ObArray<common::ObRowkey>*  rowkey_list,
          const uint64_t *columns, const int64_t column_size, const int64_t rowkey_cell_count,
          const ObCompactGetThreadContext* context, bool is_ups_getter)
      {
        int ret = OB_SUCCESS;

        const ObSSTableSchema *schema = NULL;

        if (NULL == rowkey_list || NULL == columns || NULL == context || NULL == context->readers_)
        {
          TBSYS_LOG(ERROR, "invalid argument, rowkey_list=%p columns=%p context=%p readers=%p",
            rowkey_list, columns, context, context->readers_);
          ret = OB_INVALID_ARGUMENT;
        }
        else if (rowkey_list->count() == 0 || column_size == 0 || context->readers_count_ > rowkey_list->count())
        {
          TBSYS_LOG(ERROR, "invalid argument, get row size: %ld, column size %ld, reader_size %ld",
              rowkey_list->count(), column_size, context->readers_count_);

          ret = OB_INVALID_ARGUMENT;
        }

        if(OB_SUCCESS == ret)
        {
          table_id_ = table_id;
          rowkey_list_ = rowkey_list;
          row_desc_.reset();
          column_index_.reset();
          cur_row_index_ = 0;

          columns_ = columns;
          column_size_ = column_size;
          is_ups_getter_ = is_ups_getter;
          context_ = context;


          for (int64_t i = 0; i < context_->readers_count_; i++)
          {
            if (NULL != context_->readers_[i])
            {
              schema = context_->readers_[i]->get_schema();
              break;
            }
          }

          if(schema)
          {
            ret = build_column_index(schema);
          }

          if(OB_SUCCESS == ret)
          {
            ret = build_row_desc(rowkey_cell_count);
          }
        }

        if(OB_SUCCESS == ret)
        {
          //alloc memory
          if (OB_SUCCESS != (ret = alloc_buffer(uncomp_buf_, uncomp_buf_size_)))
          {
            TBSYS_LOG(WARN, "alloc buffer error:ret=[%d], uncomp_buf_size_=[%ld]", ret, uncomp_buf_size_);
          }
          else if (OB_SUCCESS != (ret = alloc_buffer(internal_buf_, internal_buf_size_)))
          {
            TBSYS_LOG(WARN, "alloc buffer error:ret=[%d], internal_buf_size_=[%ld]", ret, internal_buf_size_);
          }
          else
          {
            TBSYS_LOG(DEBUG, "sstable getter initialize success");
          }
        }

        return ret;
      }


      int ObCompactSSTableGetter::get_non_exist_row(const common::ObRowkey &key, const common::ObRow *&row)
      {
        int ret = OB_SUCCESS;
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

        return ret;
      }



    int ObCompactSSTableGetter::build_column_index(const ObSSTableSchema* const schema)
    {
      int ret = OB_SUCCESS;


      compactsstablev2::ObSimpleColumnIndexesBuilder builder(table_id_, *schema);

      column_index_.set_table_id(table_id_);
      ret = builder.build_input_col_index(column_index_, columns_, column_size_);

      return ret;
    }


    int ObCompactSSTableGetter::build_row_desc(const int64_t rowkey_cell_count)
    {
      int ret = OB_SUCCESS;
      row_desc_.reset();

      for (int64_t i = 0; OB_SUCCESS == ret && i < column_size_; ++i)
      {
        ret = row_desc_.add_column_desc(table_id_, columns_[i]);
      }

      if (OB_SUCCESS == ret)
      {
        row_desc_.set_rowkey_cell_count(rowkey_cell_count);
        ret = row_desc_.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
      }
      return ret;
    }


    int ObCompactSSTableGetter::alloc_buffer(char*& buf, const int64_t buf_size)
    {
      int ret = OB_SUCCESS;

      ModuleArena* arena = GET_TSI_MULT(ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);

      if (buf_size <= 0)
      {
        TBSYS_LOG(ERROR, "invalid buf size: buf_size=[%ld]", buf_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == arena)
      {
        TBSYS_LOG(ERROR, "failed to get tsi mult arena");
        ret = OB_ERROR;
      }
      else if (NULL == (buf = arena->alloc(buf_size)))
      {
        TBSYS_LOG(ERROR, "failed to alloc memory for buf:buf_size=%ld", buf_size);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        TBSYS_LOG(DEBUG, "alloc buf success: buf_size=%ld", buf_size);
      }

      return ret;
    }


    //1. check the bloom filter
    //2. get from the row cache
    //3. load the block index and block data
    int ObCompactSSTableGetter::get_next_row(const common::ObRow *&row)
    {
      int ret = OB_SUCCESS;
      const ObCompactSSTableReader *reader = context_->readers_[cur_row_index_];
      const ObRowkey *rowkey = get_row_key(cur_row_index_);

      //whether work done
      if (cur_row_index_  >= context_->readers_count_)
      {
        ret = OB_ITER_END;
      }

      if(OB_SUCCESS == ret)
      {
        if(NULL == reader || reader->get_row_count() == 0)
        {
            ret = OB_SEARCH_NOT_FOUND;
        }
        else
        {
          //check the bloom filter
#ifdef OB_COMPACT_SSTABLE_ALLOW_BLOOMFILTER_
          const uint64_t table_id = table_id_;
          const TableBloomFilter* bloom_filter = get_context_->sstable_reader_->get_table_bloomfilter(table_id);
          if(!bloom_filter->contain(table_id, *rowkey))
          {
            ret = OB_SEARCH_NOT_FOUND;
          }
#endif

          if(OB_SUCCESS == ret)
          {
            bool row_cache_hit = false;

            //see the row cache
            if( NULL != context_->row_cache_ && OB_SUCCESS == (ret = get_row_from_rowcache(row)))
            {
              row_cache_hit = true;
            }

            //will get the block index and block data if row cache miss
            //we get the row from the sstable and put into the rowcache
            if(!row_cache_hit)
            {
              ret = get_row_from_sstable(row);
            }
          }
        }

        if(OB_SUCCESS == ret)
        {
        }
        else if(OB_SEARCH_NOT_FOUND == ret)
        {
          get_non_exist_row(*rowkey, row);
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(WARN, "get rowkey error: ret=[%d]", ret);
        }

        //next row
        cur_row_index_ ++;
      }

      return ret;
    }

    int ObCompactSSTableGetter::get_row_from_rowcache(const common::ObRow*& row)
    {
      int ret = OB_SUCCESS;

      const uint64_t sstable_id = context_->readers_[cur_row_index_]->get_sstable_id();
      const ObCompactStoreType row_store_type = context_->readers_[cur_row_index_]->get_row_store_type();

      const ObRowkey *rowkey = get_row_key(cur_row_index_);
      const uint64_t table_id = table_id_;
      sstable::ObSSTableRowCacheValue row_cache_val;

      sstable::ObSSTableRowCacheKey row_cache_key(sstable_id, table_id, *const_cast<common::ObRowkey*>(rowkey));

      //get the row key from rowcache
      ret = context_->row_cache_->get_row(row_cache_key, row_cache_val, cache_row_buf_);

      if (OB_SUCCESS == ret)
      {
        //cache hit
#ifndef  _SSTABLE_NO_STAT_
        OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SSTABLE_ROW_CACHE_HIT, 1);
#endif
        //get the row cache
        if(NULL != row_cache_val.buf_ && row_cache_val.size_ > 0)
        {
          ret = getter_.get_cached_row(row_cache_val, row_store_type, &row_desc_, is_ups_getter() ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL, column_index_, row);

          if(OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "row init error:ret=%d, row_buf=%p,row_store_type_=%d",
                ret, row_cache_val.buf_, row_store_type);
          }
        }
        else
        {
          get_non_exist_row(*rowkey, row);
        }

      }
      else
      {
#ifndef  _SSTABLE_NO_STAT_
        OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SSTABLE_ROW_CACHE_MISS, 1);
#endif
      }

      return ret;
    }


    int ObCompactSSTableGetter::get_row_from_sstable(const common::ObRow*& row)
    {
      int ret = OB_SUCCESS;

      const ObCompactSSTableReader *reader = context_->readers_[cur_row_index_];
      const ObRowkey *rowkey = get_row_key(cur_row_index_);
      const char* block_data_ptr = NULL;
      int64_t block_data_size = 0;

      if(OB_SUCCESS != (ret = load_block_index()) && OB_SEARCH_NOT_FOUND != ret)
      {
        TBSYS_LOG(WARN, "load block index error:ret=%d", ret);
      }
      else if(OB_SUCCESS == ret && OB_SUCCESS == (ret = load_block_data(block_data_ptr, block_data_size)))
      {
        //get the cell iterator
        ObSSTableBlockReader::BlockData block_data(
            internal_buf_, internal_buf_size_, block_data_ptr, block_data_size);
        ret = getter_.init(*rowkey, block_data, reader->get_row_store_type(), column_index_, &row_desc_, is_ups_getter() ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);

        if(OB_SUCCESS != ret && OB_BEYOND_THE_RANGE != ret)
        {
          TBSYS_LOG(WARN, "block getter init error:ret=%d", ret);
        }
        else
        {

          if(OB_SUCCESS == ret)
          {
            ret = getter_.get_row(row);
          }
          else
          {
            // not found will change the ret
            ret = OB_SEARCH_NOT_FOUND;
          }
        }
      }

      //update the cache
      if(context_->row_cache_ != NULL)
      {
        update_row_cache(OB_SUCCESS == ret);
      }

      return ret;
    }

    int ObCompactSSTableGetter::update_row_cache(bool found_in_block)
    {
      int ret = OB_SUCCESS;
      const ObCompactSSTableReader *reader = context_->readers_[cur_row_index_];
      uint64_t sstable_id = reader->get_sstable_id();
      uint64_t table_id = table_id_;
      sstable::ObSSTableRowCacheValue row_cache_val;
      const ObRowkey *rowkey = get_row_key(cur_row_index_);
      sstable::ObSSTableRowCacheKey row_cache_key(sstable_id, table_id, *const_cast<common::ObRowkey*>(rowkey));

      //found it
      if(found_in_block)
      {
        //get row cache value, for put into rowcache
        ret = getter_.get_cache_row_value(row_cache_val);

        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get cache row value error:ret=%d", ret);
        }
      }


      if (OB_SUCCESS == ret)
      {
        ret = context_->row_cache_->put_row(row_cache_key, row_cache_val);
      }

      return ret;
    }

    int ObCompactSSTableGetter::load_block_index()
    {
      int ret = OB_SUCCESS;

      const ObCompactSSTableReader *reader = context_->readers_[cur_row_index_];

      // get the sstable id and table id
      uint64_t sstable_id = reader->get_sstable_id();
      uint64_t table_id = table_id_;

      // prepare for search the block index of the rowkey
      ObBlockIndexPositionInfo info;
      SearchMode mode = OB_SEARCH_MODE_GREATER_EQUAL;
      const ObRowkey* look_key = get_row_key(cur_row_index_);

      // get the sstable info
      const ObSSTableTableIndex* table_index
        = reader->get_table_index(table_id);
      info.sstable_file_id_ = sstable_id;
      info.index_offset_ = table_index->block_index_offset_;
      info.index_size_ = table_index->block_index_size_;
      info.endkey_offset_ = table_index->block_endkey_offset_;
      info.endkey_size_ = table_index->block_endkey_size_;
      info.block_count_ = table_index->block_count_;

      // search the block index where has the look key
      ret = context_->block_index_cache_->get_single_block_pos_info(
          info, table_id, *look_key, mode, block_pos_);

      if(OB_BEYOND_THE_RANGE == ret)
      {
        ret = OB_SEARCH_NOT_FOUND;
      }

      return ret;
    }



    int ObCompactSSTableGetter::load_block_data(const char*& buf, int64_t& buf_size)
    {
      int ret = OB_SUCCESS;

      ObSSTableBlockBufferHandle handler;

      const char* comp_buf = NULL;
      int64_t comp_buf_size = 0;

      const ObCompactSSTableReader *reader = context_->readers_[cur_row_index_];
      uint64_t sstable_file_id = reader->get_sstable_id();
      const char* block_data_ptr = NULL;
      int64_t block_data_size = 0;
      const uint64_t table_id = table_id_;
      ObRecordHeaderV2 header;

      if (OB_INVALID_ID == sstable_file_id)
      {
        TBSYS_LOG(WARN, "invalid sstable file id");
        ret = OB_ERROR;
      }
      else if (0 == table_id || OB_INVALID_ID == table_id)
      {
        TBSYS_LOG(WARN, "invalid table id:table_id=%lu", table_id);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = context_->block_cache_->get_block(
            sstable_file_id,
            block_pos_.offset_, block_pos_.size_, handler,table_id);
        if (block_pos_.size_ == ret)
        {
          block_data_ptr = handler.get_buffer();
          block_data_size = block_pos_.size_;
          ret = OB_SUCCESS;
        }
        else
        {
          TBSYS_LOG(ERROR, "IO ERROR:ret=%d,sstable_file_id=%lu,"
              "table_id=%lu", ret, sstable_file_id, table_id);
          ret = OB_IO_ERROR;
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = ObRecordHeaderV2::get_record_header(
                block_data_ptr, block_data_size, header,
                comp_buf, comp_buf_size)))
        {
          TBSYS_LOG(WARN, "get record header error:ret=%d,"
              "block_data_ptr=%p,block_data_size=%ld",
              ret, block_data_ptr, block_data_size);
        }
        else if (header.data_length_ > uncomp_buf_size_)
        {
          uncomp_buf_size_ = header.data_length_;
          if (OB_SUCCESS != (ret = alloc_buffer(uncomp_buf_,
                  uncomp_buf_size_)))
          {
            TBSYS_LOG(ERROR, "failed to alloc buffer:ret=%d,"
                "uncomp_buf_size=%ld", ret, uncomp_buf_size_);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        int64_t real_size = 0;
        if (header.is_compress())
        {
          ObCompressor* dec = const_cast<ObCompactSSTableReader*>(reader)->get_decompressor();
          if (NULL != dec)
          {
            if (OB_SUCCESS != (ret = dec->decompress(
                    comp_buf, comp_buf_size,
                    uncomp_buf_, header.data_length_, real_size)))
            {
                TBSYS_LOG(WARN, "decompress error:ret=%d,"
                    "comp_buf=%p,comp_buf_size=%ld,data_length_=%ld",
                    ret, comp_buf, comp_buf_size, header.data_length_);
            }
            else
            {
              buf = uncomp_buf_;
              buf_size = real_size;
            }
          }
          else
          {
            TBSYS_LOG(ERROR, "compressor is NULL");
          }
        }
        else
        {
          memcpy(uncomp_buf_, comp_buf, comp_buf_size);
          buf = uncomp_buf_;
          buf_size = comp_buf_size;
        }
      }

      return ret;
    }
  }//end namespace sstable
}//end namespace oceanbase
