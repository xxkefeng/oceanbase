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
#ifndef OCEANBASE_SQL_OB_SSTABLE_BLOCK_GETTER_H_
#define OCEANBASE_SQL_OB_SSTABLE_BLOCK_GETTER_H_

#include "common/ob_row.h"
#include "sstable/ob_scan_column_indexes.h"
#include "sstable/ob_sstable_block_reader.h"

namespace oceanbase
{
namespace sql
{

class ObSSTableBlockGetter
{
  const static int64_t DEFAULT_INDEX_BUF_SIZE = 1 << 18; // 256KB
public:
  ObSSTableBlockGetter();
  ~ObSSTableBlockGetter();

  // Reset and initialize
  // @return int
  //  success:
  //    OB_SUCCESS : rowkey found in block
  //    OB_SEARCH_NOT_FOUND : rowkey not found in block
  //  fail:
  //    OB_ERROR
  //    or other error code defined in ob_define.h
  int initialize(const common::ObRowkey *rowkey, const common::ObRowDesc *row_desc,
      const sstable::ObSimpleColumnIndexes *column_index,
      const char *buf, const int64_t buf_size,
      const sstable::ObSSTableBlockReader::BlockDataDesc &data_desc,
      common::ObRow::DefaultValue row_def_value);

  // Get current row, must be called after initialize() return success.
  inline int get_row(const common::ObRow *&row) const
  {
    row = &row_;
    return common::OB_SUCCESS;
  }

  // Get row cache value, must be called after initialize() return success.
  inline int get_cache_row_value(sstable::ObSSTableRowCacheValue &row_value) const
  {
    return reader_.get_cache_row_value(cur_iter_, row_value);
  }

  // Parse cache row value to row.
  // This func can be called before initialize(). (it reset class status)
  int get_cached_row(const sstable::ObSSTableRowCacheValue &cache_value, const int store_style,
      const common::ObRowDesc *row_desc, common::ObRow::DefaultValue row_def_value,
      const sstable::ObSimpleColumnIndexes &column_index,
      const common::ObRowkey &key, const common::ObRow *&row);

private:
  common::ObRow row_;

  common::ObMemBuf block_index_buf_;
  sstable::ObSSTableBlockReader reader_;
  sstable::ObSSTableBlockReader::const_iterator cur_iter_;
};
} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_OB_SSTABLE_BLOCK_GETTER_H_
