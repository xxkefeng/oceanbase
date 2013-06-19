/**
 *  (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_reader.h
 *
 *  Authors:
 *     jiangzhe < jiangzhe.lxh@alipay.com >
 *     zian < yunliang.shi@alipay.com >
 *
 */
#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_READER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_READER_H_

#include "common/ob_define.h"
#include "common/ob_compact_cell_iterator.h"
#include "common/ob_rowkey.h"
#include "common/ob_row.h"
#include "common/ob_tsi_factory.h"
#include "sstable/ob_sstable_row_cache.h"
#include "sstable/ob_scan_column_indexes.h"
#include "ob_sstable_store_struct.h"
#include "common/ob_buffer_helper.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableBlockReader
    {
    public:
      //block表示(row index buf + row data buf) row index buf包含(row offset, row size), 在sstable文件中，只有row offset
      struct BlockData
      {
        //解析后的row index的存储位置(空间在ObSSTableBlockScanner内申请，当不足时在ObSSTableBlockReader内申请
        char* internal_buf_;         //index buf
        int64_t internal_buf_size_;  //index size
        const char* data_buf_;       //block buf
        int64_t data_buf_size_;      //block size

        BlockData()
        {
          memset(this, 0, sizeof(BlockData));
        }

        BlockData(char* ib,
            const int64_t ib_sz,
            const char* db,
            const int64_t db_sz)
          : internal_buf_(ib),
            internal_buf_size_(ib_sz),
            data_buf_(db),
            data_buf_size_(db_sz)
        {
        }

        inline bool is_valid() const
        {
          return (NULL != internal_buf_) && (0 < internal_buf_size_) && (NULL != data_buf_) && (0 < data_buf_size_);
        }
      };

      //单条row index(offset, size)
      struct RowIndexItemType
      {
        int32_t offset_;   //row开始offset
        int32_t size_;     //row size
      };

      //重载lower_bound的比较类
      //根据row index找到对应的rowkey然后与要比较的key进行比较
      class Compare
      {
      public:
        Compare(const ObSSTableBlockReader& block_reader) 
          : block_reader_(block_reader)
        {
        }

        inline bool operator()(const RowIndexItemType& index, const common::ObRowkey& key)
        {
          bool ret = false;
          int tmp_ret = common::OB_SUCCESS;
          common::ObRowkey row_key;

          if (common::OB_SUCCESS != (tmp_ret = block_reader_.get_row_key(&index, row_key)))
          {
            TBSYS_LOG(WARN, "block reader get row key error: tmp_ret=[%d], index.offset_=[%d], index.size_=[%d]",
                tmp_ret, index.offset_, index.size_);
          }
          else
          {
            ret = row_key.compare(key) < 0;
          }

          return ret;
        }
      
      private:
        const ObSSTableBlockReader& block_reader_;
      };

    public:
      typedef RowIndexItemType* iterator;
      typedef const RowIndexItemType* const_iterator;

    public:
      static const int32_t ROW_INDEX_ITEM_SIZE = static_cast<int32_t>(sizeof(ObSSTableBlockRowIndex));       
      static const int32_t BLOCK_HEADER_SIZE = static_cast<int32_t>(sizeof(ObSSTableBlockHeader));
      static const int32_t INTERNAL_ROW_INDEX_ITEM_SIZE = static_cast<int32_t>(sizeof(RowIndexItemType));

    public:
      ObSSTableBlockReader();
      ~ObSSTableBlockReader();

      int reset();
      int init(const BlockData& data, const common::ObCompactStoreType& row_store_type);

      int get_row(const_iterator index, common::ObCompactCellIterator& row) const;
      int get_row(const_iterator index, const sstable::ObSimpleColumnIndexes& query_columns, common::ObRow& value);
      int get_row_key(const_iterator index, common::ObRowkey& key) const;

      ObSSTableBlockReader::const_iterator lower_bound(const common::ObRowkey& key);
      const_iterator begin() const;
      const_iterator end() const;

      int get_cache_row_value(const_iterator index, sstable::ObSSTableRowCacheValue& row_value) const;

      const ObSSTableBlockHeader* get_block_header() const;

    private:
      int get_row_key(common::ObCompactCellIterator& row, common::ObRowkey& key) const;
      const char* find_row(const_iterator index) const;

    private:
      ObSSTableBlockHeader block_header_;
      const char* data_begin_; //contain the block header
      const char* data_end_;
      const_iterator index_begin_;
      const_iterator index_end_;

      common::ObCompactStoreType row_store_type_;
      mutable common::ObObj rowkey_buf_array_[common::OB_MAX_ROWKEY_COLUMN_NUMBER + 1]; //用于rowkey比较的临时Obj数组
    };


    inline int ObSSTableBlockReader::reset()
    {
      int ret = common::OB_SUCCESS;
      block_header_.reset();
      data_begin_ = NULL;
      data_end_ = NULL;
      index_begin_ = NULL;
      index_end_ = NULL;
      row_store_type_ = common::INVALID_COMPACT_STORE_TYPE;
      return ret;
    }

    inline __attribute__((always_inline)) int ObSSTableBlockReader::get_row(
        const_iterator index, common::ObCompactCellIterator& row) const
    {
      int ret = common::OB_SUCCESS;
      const char* row_buf = find_row(index);

      if (NULL == row_buf)
      {
        TBSYS_LOG(WARN, "row buf is NULL");
        ret = common::OB_SEARCH_NOT_FOUND;
      }
      else if (common::OB_SUCCESS != (ret = row.init(row_buf, row_store_type_)))
      {
        TBSYS_LOG(WARN, "row iterator init error: ret=[%d], row_buf=[%p], row_store_type_=[%d]", ret, row_buf, row_store_type_);
      }

      return ret;
    }

    inline __attribute__((always_inline)) int ObSSTableBlockReader::get_row_key(
        common::ObCompactCellIterator& row, common::ObRowkey& key) const
    {
      int ret = common::OB_SUCCESS;
      int64_t rowkey_obj_count = common::OB_MAX_ROWKEY_COLUMN_NUMBER + 1;
      
      if (common::OB_SUCCESS != (ret = row.get_one_row_dense(rowkey_buf_array_, rowkey_obj_count)))
      {
        TBSYS_LOG(WARN, "row iterator get one row dense error: ret=[%d], rowkey_buf_array=[%p]", ret, rowkey_buf_array_);
      }
      else
      {
        key.assign(rowkey_buf_array_, rowkey_obj_count);
      }

      return ret;
    }

    inline __attribute__((always_inline)) int ObSSTableBlockReader::get_row_key(
        const_iterator index, common::ObRowkey& key) const
    {
      int ret = common::OB_SUCCESS;
      common::ObCompactCellIterator row;
      
      if (common::OB_SUCCESS != (ret = get_row(index, row)))
      {
        TBSYS_LOG(WARN, "get row error: ret=[%d], index.offset_=[%d], index.size_=[%d]", ret, index->offset_, index->size_);
      }
      else if (common::OB_SUCCESS != (ret = get_row_key(row, key)))
      {
        TBSYS_LOG(WARN, "ger row key error: ret=[%d]", ret); 
      }

      return ret;
    }

    inline ObSSTableBlockReader::const_iterator ObSSTableBlockReader::lower_bound(const common::ObRowkey& key)
    {
      return std::lower_bound(index_begin_, index_end_, key, Compare(*this));
    }

    inline ObSSTableBlockReader::const_iterator ObSSTableBlockReader::begin() const
    {
      return index_begin_;
    }

    inline ObSSTableBlockReader::const_iterator ObSSTableBlockReader::end() const
    {
      return index_end_;
    }

    inline const ObSSTableBlockHeader* ObSSTableBlockReader::get_block_header() const
    {
      return &block_header_;
    }

    inline const char* ObSSTableBlockReader::find_row(const_iterator index) const
    {
      return (data_begin_ + index->offset_);
    }
  }//end namespace compactsstablev2
}//end namesapce oceanbase
#endif
