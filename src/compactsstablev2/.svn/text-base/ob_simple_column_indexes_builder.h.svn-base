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
*   zian <yunliang.shi@alipay.com>
*/
#ifndef OCEANBASE_SSTABLE_OB_SIMPLE_COLUMN_INDEXES_BUILDER_H_
#define OCEANBASE_SSTABLE_OB_SIMPLE_COLUMN_INDEXES_BUILDER_H_

#include "common/ob_define.h"
#include "sstable/ob_scan_column_indexes.h"
#include "ob_sstable_schema.h"

namespace oceanbase
{
  namespace sstable
  {
    class ObSimpleColumnIndexes;
  }

  namespace compactsstablev2
  {

    class ObSSTableSchema;

    // bulding ObSimpleColumnIndexes
    class ObSimpleColumnIndexesBuilder
    {
      public:
        ObSimpleColumnIndexesBuilder(const uint64_t table_id,
            const ObSSTableSchema &schema)
          : table_id_(table_id),schema_(schema)
      {
      }

        ~ObSimpleColumnIndexesBuilder() {}

        // Build index for all columns.
        int build_full_row_index(sstable::ObSimpleColumnIndexes &column_indexes);

        // Build index for specified columns.
        int build_input_col_index(sstable::ObSimpleColumnIndexes &column_indexes,
            const uint64_t *const column_id_begin, const int64_t column_id_size);

      private:
        uint64_t table_id_;
        const ObSSTableSchema &schema_;
    };

  } // end namespace sstable
} // end namespace oceanbase

#endif // OCEANBASE_SSTABLE_OB_SIMPLE_COLUMN_INDEXES_BUILDER_H_
