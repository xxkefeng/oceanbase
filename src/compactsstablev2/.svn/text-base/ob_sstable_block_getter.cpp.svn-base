/**
 *  (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_block_getter.cpp
 *
 *  Authors:
 *     jiangzhe < jiangzhe.lxh@alipay.com >
 *     zian < yunliang.shi@alipay.com >
 *
 */

#include "ob_sstable_block_getter.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableBlockGetter::init(const common::ObRowkey& row_key,
        const ObSSTableBlockReader::BlockData& block_data,
        const common::ObCompactStoreType& row_store_type,
        const sstable::ObSimpleColumnIndexes& query_columns,
        const common::ObRowDesc *row_desc,
        common::ObRow::DefaultValue row_def_value)
    {
      int ret = OB_SUCCESS;

      // Reset row, and set extend column to ObActionFlag::OP_VALID if it exist.
      rowvalue_.set_row_desc(*row_desc);
      rowvalue_.reset(false, row_def_value);

      int64_t ext_col_idx = row_desc->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
      if (OB_INVALID_INDEX != ext_col_idx)
      {
        ObObj obj;
        obj.set_ext(ObActionFlag::OP_VALID);
        rowvalue_.raw_set_cell(ext_col_idx, obj);
      }

      if (OB_SUCCESS != (ret = block_reader_.init(
              block_data, row_store_type)))
      {
        TBSYS_LOG(WARN, "block reader init error: ret=[%d]", ret);
      }
      else
      {
        ObSSTableBlockReader::const_iterator iterator;
        iterator = block_reader_.lower_bound(row_key);
        ObRowkey key;
        if (iterator == block_reader_.end())
        {
          ret = OB_BEYOND_THE_RANGE;
        }
        else if (OB_SUCCESS != (ret = block_reader_.get_row_key(
                iterator, key)))
        {
          TBSYS_LOG(WARN, "get row key error: ret=[%d]", ret);
        }
        else
        {
          if (0 != key.compare(row_key))
          {
            ret = OB_BEYOND_THE_RANGE;
          }
          else if (OB_SUCCESS != (ret = block_reader_.get_row(iterator, query_columns, rowvalue_)))
          {
            TBSYS_LOG(WARN, "load current row error: ret=[%d]", ret);
          }
          else
          {
            row_cursor_ = iterator;
          }
        }
      }

      return ret;
    }

    int ObSSTableBlockGetter::get_cached_row(const sstable::ObSSTableRowCacheValue &row_cache_val,
        const common::ObCompactStoreType& row_store_type,
        const common::ObRowDesc *row_desc,
        common::ObRow::DefaultValue row_def_value,
        const sstable::ObSimpleColumnIndexes &column_index,
        const common::ObRow *&value)
    {
      int ret = OB_SUCCESS;

      // Reset row, and set extend column to ObActionFlag::OP_VALID if it exist.
      rowvalue_.set_row_desc(*row_desc);
      rowvalue_.reset(false, row_def_value);

      int64_t ext_col_idx = row_desc->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
      if (OB_INVALID_INDEX != ext_col_idx)
      {
        ObObj obj;
        obj.set_ext(ObActionFlag::OP_VALID);
        rowvalue_.raw_set_cell(ext_col_idx, obj);
      }

      common::ObCompactCellIterator row;

      if(OB_SUCCESS != (ret = row.init(row_cache_val.buf_, row_store_type)))
      {
        ret = OB_SEARCH_NOT_FOUND;
        TBSYS_LOG(WARN, "row init error:ret=[%d], row_buf=[%p],row_store_type_=[%d]", ret, row_cache_val.buf_, row_store_type);
      }
      else if(OB_SUCCESS != (ret = row.get_row(rowvalue_, column_index)))
      {
        TBSYS_LOG(WARN, "compact cell interator get row error: ret=[%d]", ret);
      }

      if(OB_SUCCESS == ret)
      {
        value = &rowvalue_;
      }

      return ret;
    }

  }//end namespace sstable
}//end namespace oceanbase
