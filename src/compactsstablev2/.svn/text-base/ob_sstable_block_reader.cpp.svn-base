/**
 *  (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_reader.cpp
 *
 *  Authors:
 *     jiangzhe < jiangzhe.lxh@alipay.com >
 *     zian < yunliang.shi@alipay.com >
 *
 */

#include "ob_sstable_block_reader.h"
using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    ObSSTableBlockReader::ObSSTableBlockReader()
      : data_begin_(NULL),
        data_end_(NULL),
        index_begin_(NULL),
        index_end_(NULL),
        row_store_type_(INVALID_COMPACT_STORE_TYPE)
    {
      memset(&block_header_, 0, sizeof(block_header_));
    }

    ObSSTableBlockReader::~ObSSTableBlockReader()
    {
      index_begin_ = NULL;
      index_end_ = NULL;
      data_begin_ = NULL;
      data_end_ = NULL;
    }

    int ObSSTableBlockReader::init(const BlockData& data, const common::ObCompactStoreType& row_store_type)
    {
      int ret = OB_SUCCESS;
      char* internal_buf_ptr = NULL;
      int64_t index_item_length = 0;

      reset();

      if (!data.is_valid())
      {
        TBSYS_LOG(WARN, "invalid BlockData: data.internal_buf_=[%p], data.internal_buf_size_=[%ld],"
            "data.data_buf_=[%p], data.data_buf_size_=[%ld]",
            data.internal_buf_, data.internal_buf_size_, data.data_buf_, data.data_buf_size_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (DENSE_SPARSE != row_store_type && DENSE_DENSE != row_store_type)
      {
        TBSYS_LOG(WARN, "invalid row store type: row_store_type=[%d]", row_store_type);
      }
      else
      {
        row_store_type_ = row_store_type;

        //block header
        memcpy(&block_header_, data.data_buf_, BLOCK_HEADER_SIZE);

        //block data
        data_begin_ = data.data_buf_;
        data_end_ = data.data_buf_ + block_header_.row_index_offset_;

        //block row index
        internal_buf_ptr = data.internal_buf_;
        index_item_length = INTERNAL_ROW_INDEX_ITEM_SIZE * (block_header_.row_count_ + 1);

        //传过来的row index预留空间不够
        if (index_item_length > data.internal_buf_size_)
        {
          ModuleArena* arena = GET_TSI_MULT(ModuleArena, TSI_SSTABLE_MODULE_ARENA_1);
          if (arena == NULL)
          {
            TBSYS_LOG(ERROR, "GET_TSI_MULT error: TSI_SSTABLE_MODULE_ARENA_1");
          }
          else if (NULL == (internal_buf_ptr = arena->alloc_aligned(index_item_length)))
          {
            TBSYS_LOG(ERROR, "failed to alloc memrory for internal_buf_ptr: index_item_length=[%ld]", index_item_length);
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL != internal_buf_ptr)
        {
          //row index
          char* row_index = const_cast<char*>(data_end_);
          iterator index_ptr = reinterpret_cast<iterator>(internal_buf_ptr);
          index_begin_ = index_ptr;
          index_end_ = index_begin_ + block_header_.row_count_ + 1;

          int32_t offset = 0;
          int32_t pos = 0;

          //row offset--->row offset, row size
          for (int i = 0; i < (block_header_.row_count_ + 1); i ++)
          {
            memcpy(&offset, row_index + pos, ROW_INDEX_ITEM_SIZE);
            pos = pos + ROW_INDEX_ITEM_SIZE;
            index_ptr[i].offset_ = offset;
            if (i > 0)
            {
              index_ptr[i - 1].size_ = offset - index_ptr[i - 1].offset_;
            }
          }

          //转化完成,index end --
          index_end_ --;
        }
      }

      return ret;
    }

    int ObSSTableBlockReader::get_cache_row_value(const_iterator index, sstable::ObSSTableRowCacheValue& row_value) const
    {
      int ret = OB_SUCCESS;
      ObRowkey rowkey;

      if (NULL == index)
      {
        TBSYS_LOG(WARN, "invalid argument: index is null");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = get_row_key(index, rowkey)))
      {
        TBSYS_LOG(WARN, "get row key error: ret=[%d], offset=[%d], size=[%d]", ret, index->offset_, index->size_);
      }
      else
      {
        row_value.buf_ = const_cast<char*>(data_begin_ + index->offset_);
        row_value.size_ = index->size_;
      }

      return ret;
    }


    int ObSSTableBlockReader::get_row(const_iterator index, const sstable::ObSimpleColumnIndexes& query_columns, common::ObRow& value)
    {
      int ret = OB_SUCCESS;
      common::ObCompactCellIterator row;

      //get the Iterator
      if(OB_SUCCESS != (ret = get_row(index, row)))
      {
        TBSYS_LOG(WARN, "block get row error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = row.get_row(value, query_columns)))
      {
        TBSYS_LOG(WARN, "compact cell interator get row error: ret=[%d]", ret);
      }

      return ret;
    }

  }//end namespace compactsstablev2
}//end namespace oceanbase
