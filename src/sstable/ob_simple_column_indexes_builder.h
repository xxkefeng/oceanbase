/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* ob_simple_column_indexes_builder.h : build ObSimpleColumnIndexes
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/
#ifndef OCEANBASE_SSTABLE_OB_SIMPLE_COLUMN_INDEXES_BUILDER_H_
#define OCEANBASE_SSTABLE_OB_SIMPLE_COLUMN_INDEXES_BUILDER_H_

#include "common/ob_define.h"

namespace oceanbase
{

namespace common
{
class ObRowkeyInfo;
class ObRowDesc;
}

namespace sstable
{

class ObSSTableSchema;
class ObSimpleColumnIndexes;

// bulding ObSimpleColumnIndexes
class ObSimpleColumnIndexesBuilder
{
public:
  struct GroupIdDesc
  {
    uint64_t id_;
    uint64_t seq_;
    uint64_t size_;
  };

public:
  ObSimpleColumnIndexesBuilder(const uint64_t table_id,
      const ObSSTableSchema &schema,
      const common::ObRowkeyInfo *rowkey_info,
      common::ObRowDesc *row_desc)
    : table_id_(table_id),
    schema_(schema), rowkey_info_(rowkey_info), row_desc_(row_desc)
  {
  }

  ~ObSimpleColumnIndexesBuilder() {}

  // Build index for all columns.
  int build_full_row_index(ObSimpleColumnIndexes &column_indexes, const GroupIdDesc &group);

  // Build index for specified columns.
  int build_input_col_index(ObSimpleColumnIndexes &column_indexes, const GroupIdDesc &group,
      const uint64_t *const column_id_begin, const int64_t column_id_size);

private:
  int add_column_offset(ObSimpleColumnIndexes &column_indexes, const uint64_t column_id,
      const int64_t offset);

  // We only call this function once, so it is always inline and implement in .cpp file.
  inline __attribute__((always_inline)) int add_rowkey_columns(
      ObSimpleColumnIndexes &column_indexes);

private:
  uint64_t table_id_;
  const ObSSTableSchema &schema_;
  const common::ObRowkeyInfo *rowkey_info_;
  common::ObRowDesc *row_desc_;
};

} // end namespace sstable
} // end namespace oceanbase

#endif // OCEANBASE_SSTABLE_OB_SIMPLE_COLUMN_INDEXES_BUILDER_H_
