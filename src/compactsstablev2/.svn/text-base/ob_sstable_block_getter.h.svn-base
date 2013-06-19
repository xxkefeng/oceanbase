/**
 *  (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_getter.h
 *
 *  Authors:
 *     jiangzhe < jiangzhe.lxh@alipay.com >
 *     zian < yunliang.shi@alipay.com >
 *
 */
#ifndef OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_GETTER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_GETTER_H_

#include "common/ob_string.h"
#include "common/ob_common_param.h"
#include "common/ob_rowkey.h"
#include "common/ob_compact_cell_iterator.h"
#include "ob_sstable_block_reader.h"
#include "sstable/ob_sstable_row_cache.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableBlockGetter
    {
    public:
      static const int64_t DEFAULT_INDEX_BUF_SIZE = 256 * 1024;
      typedef ObSSTableBlockReader::const_iterator const_iterator;
      typedef ObSSTableBlockReader::iterator iterator;

    public:
      ObSSTableBlockGetter()
        : inited_(false), 
          row_cursor_(NULL)
      {
      }

      ~ObSSTableBlockGetter()
      {
      }

      inline int get_row(const common::ObRow *&row) const
      {
        int ret = common::OB_SUCCESS;
        row = &rowvalue_;
        return ret;
      }

      int init(const common::ObRowkey& row_key,
          const ObSSTableBlockReader::BlockData& block_data,
          const common::ObCompactStoreType& row_store_type);

      int init(const common::ObRowkey& row_key,
          const ObSSTableBlockReader::BlockData& block_data,
          const common::ObCompactStoreType& row_store_type,
          const sstable::ObSimpleColumnIndexes& query_columns,
          const common::ObRowDesc *row_desc,
          common::ObRow::DefaultValue row_def_value);


      inline int get_cache_row_value(
          sstable::ObSSTableRowCacheValue& row_value)
      {
        return block_reader_.get_cache_row_value(row_cursor_, row_value);
      }


      // Parse cache row value to row.
      // This func can be called before initialize(). (it reset class status)
      int get_cached_row(const sstable::ObSSTableRowCacheValue &row_cache_val,
          const common::ObCompactStoreType& row_store_type,
          const common::ObRowDesc *row_desc,
          common::ObRow::DefaultValue row_def_value,
          const sstable::ObSimpleColumnIndexes &column_index,
          const common::ObRow *&value);

    private:
      bool inited_;
      ObSSTableBlockReader block_reader_;
      const_iterator row_cursor_;
      common::ObRow rowvalue_;
    };
  }//end namespace sstable
}//end namespace oceanbase

#endif
