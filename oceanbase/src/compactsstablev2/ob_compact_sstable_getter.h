/**
 *  (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_get_param.h is for compact sstable getter param
 *
 *  Authors:
 *     zian < yunliang.shi@alipay.com >
 *
 */


#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_GETTER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_COMPACT_SSTABLE_GETTER_H_

#include "common/ob_iterator.h"
#include "common/ob_rowkey.h"
#include "common/ob_row.h"
#include "common/ob_get_param.h"
#include "common/ob_common_param.h"
#include "ob_compact_sstable_reader.h"
#include "ob_sstable_block_index_cache.h"
#include "ob_sstable_block_cache.h"
#include "ob_sstable_block_index_mgr.h"
#include "ob_sstable_scan_column_indexes.h"
#include "ob_sstable_block_getter.h"
#include "sstable/ob_sstable_row_cache.h"
#include "sql/ob_multi_cg_scanner.h"
#include "sql/ob_sql_get_param.h"
#include "ob_simple_column_indexes_builder.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObCompactSSTableGetter : public sql::ObRowkeyIterator
    {
      static const int64_t UNCOMP_BUF_SIZE = 1024 * 1024;
      static const int64_t INTERNAL_BUF_SIZE = 1024 * 1024;
      static const int32_t TIME_OUT_US = 10 * 1000 * 1000;

    public:
      struct ObCompactGetThreadContext
      {
        compactsstablev2::ObSSTableBlockIndexCache* block_index_cache_;
        compactsstablev2::ObSSTableBlockCache* block_cache_;
        sstable::ObSSTableRowCache *row_cache_;

        int64_t readers_count_;
        compactsstablev2::ObCompactSSTableReader* readers_[common::OB_MAX_GET_ROW_NUMBER];

        ObCompactGetThreadContext() : block_index_cache_(NULL),
        block_cache_(NULL), row_cache_(NULL), readers_count_(0)
        {
        }
      };

    public:

      ObCompactSSTableGetter();

      virtual ~ObCompactSSTableGetter() { }

      virtual int get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row)
      {
        UNUSED(rowkey);
        UNUSED(row);
        return common::OB_NOT_SUPPORTED;
      }

      /**
       * get next row
       * @param row_key: rowkey
       * @param row_value: rowvalue
       * @return
       */
      virtual int get_next_row(const common::ObRow*& rowvalue);


      /**
       * get row desc
       * @param row_desc: row_desc of row_value
       */
      virtual int get_row_desc(const common::ObRowDesc*& row_desc) const
      {
        int ret = common::OB_SUCCESS;

        if (NULL == row_desc)
        {
          row_desc = &row_desc_;
        }
        else
        {
          TBSYS_LOG(WARN, "get row desc param error");
          ret = common::OB_NOT_INIT;
        }

        return ret;
      }


      int initialize(const uint64_t table_id,
          const ObArray<common::ObRowkey>*  rowkey_list,
          const uint64_t *columns,
          const int64_t column_size,
          const int64_t rowkey_cell_count,
          const ObCompactGetThreadContext* context,
          bool is_ups_getter);

    private:

      const common::ObRowkey* get_row_key(int64_t idx);

      /* build row desc*/
      int build_row_desc(const int64_t rowkey_cell_count);

      /* build column index*/
      int build_column_index(const ObSSTableSchema* const schema);

      /**
       * alloc buffer from TSI_COMPACTSSTABLEV2_MODULE_ARENA_2
       * @param buf: buf
       * @param buf_size: buf_size
       */
      int alloc_buffer(char*& buf, const int64_t buf_size);

      /**
       * get the row key from rowcache
       * @return OB_SUCCESS cache hit, SEARCH_NOT_FOUND misS
       **/
      int get_row_from_rowcache(const common::ObRow*& row);

      /**
       * get the row key from sstable, and put into the row cache
       **/
      int get_row_from_sstable(const common::ObRow*& row);

      /**
       * update the row cache
       **/
      int update_row_cache(bool found_in_block);

      /**
       *  load the block index, if asyn read, will advise libaio
       */
      int load_block_index();

      /**
       * load the block data, and init the ObSSTableBlockGetter
       */
      int load_block_data(const char*& buf, int64_t& buf_size);

      /**
       * if the row not exist ,will return not exist row
       */
      int get_non_exist_row(const common::ObRowkey &key, const common::ObRow *&row);

      /**
       * decide the default obj
       **/
      inline bool is_ups_getter() { return is_ups_getter_; }

    private:
      //param
      uint64_t table_id_;
      const ObArray<common::ObRowkey>*  rowkey_list_;
      const uint64_t *columns_;
      int64_t column_size_;
      bool is_ups_getter_;

      //column index
      sstable::ObSimpleColumnIndexes column_index_;

      //Getter context
      const ObCompactGetThreadContext *context_;

      //store type
      ObCompactStoreType row_store_type_;

      //pos info
      int64_t cur_row_index_;

      /* block location */
      ObBlockPositionInfo block_pos_;
      ObSSTableBlockGetter getter_;

      common::ObRowDesc row_desc_;

      // Row value for key not exist.
      common::ObRow row_non_exist_;

      //buffer for row
      ObMemBuf cache_row_buf_;
      char* uncomp_buf_;
      int64_t uncomp_buf_size_;
      char* internal_buf_;
      int64_t internal_buf_size_;
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif  // OCEANBASE_SSTABLE_OB_SSTABLE_GETTER_H_
