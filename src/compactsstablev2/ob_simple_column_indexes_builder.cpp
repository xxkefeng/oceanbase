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
*   zian <yunliang.shi@alipay.com>
*/

#include "ob_simple_column_indexes_builder.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    using namespace common;
    int ObSimpleColumnIndexesBuilder::build_full_row_index(sstable::ObSimpleColumnIndexes &column_indexes)
    {
      int ret = OB_SUCCESS;
      UNUSED(column_indexes);
      return ret;
    }

    int ObSimpleColumnIndexesBuilder::build_input_col_index(sstable::ObSimpleColumnIndexes &column_indexes, const uint64_t *const column_id_begin, const int64_t column_id_size)
    {
      int ret = OB_SUCCESS;

      const ObSSTableSchemaColumnDef* def = NULL;
      uint64_t cur_column_id = OB_INVALID_ID;
      int64_t rowkey_column_cnt = 0;
      schema_.get_rowkey_column_count(table_id_, rowkey_column_cnt);

      for(int64_t i = 0; OB_SUCCESS == ret && i < column_id_size; i++)
      {
        cur_column_id = column_id_begin[i];

        if(0 == cur_column_id || OB_INVALID_ID == cur_column_id)
        {
          TBSYS_LOG(WARN, "invalid column id: column_id=[%lu]", cur_column_id);
          ret = OB_ERROR;
        }
        else if (NULL != (def = schema_.get_column_def(table_id_, cur_column_id)))
        {
          if(def->is_rowkey_column())
          {
            if (OB_SUCCESS != (ret = column_indexes.add_column(def->offset_)))
            {
              TBSYS_LOG(WARN, "add_column_offset ret=%d, table_id=%ld, column_id=%ld, offset=%ld",
                  ret, table_id_, cur_column_id, def->offset_);
            }
          }
          else
          {
            if (OB_SUCCESS != (ret = column_indexes.add_column(def->offset_ + rowkey_column_cnt)))
            {
              TBSYS_LOG(WARN, "add_column_offset ret=%d, table_id=%ld, column_id=%ld, offset=%ld",
                  ret, table_id_, cur_column_id, def->offset_);
            }
          }
        }
        else
        {
          if (OB_SUCCESS != (ret = column_indexes.add_column(-1)))
          {
            TBSYS_LOG(WARN, "add_column_offset ret=%d, table_id=%ld, column_id=%ld, offset=-1",
                ret, table_id_, cur_column_id);
          }
        }
      }

      return ret;
    }
  }
}

