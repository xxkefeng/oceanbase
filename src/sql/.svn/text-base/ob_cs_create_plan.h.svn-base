/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cs_create_plan.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_CS_CREATE_PLAN_H
#define _OB_CS_CREATE_PLAN_H 1

#include "common/ob_schema.h"
#include "ob_project.h"
#include "ob_tablet_join.h"
#include "ob_sql_read_param.h"
#include "common/ob_array.h"
#include "common/ob_bitmap.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    class ObCSCreatePlan
    {
      public:
        ObCSCreatePlan() : column_ids_(OB_ALL_MAX_COLUMN_ID) {}
        virtual ~ObCSCreatePlan() {}

        virtual int create_plan(const ObSchemaManagerV2 &schema_mgr) = 0;
      
      protected:
        int get_basic_column_and_join_info(
            const ObSqlReadParam& param, 
            const ObSchemaManagerV2& schema_mgr, 
            uint64_t *basic_columns, 
            int64_t &basic_column_count,
            int64_t &rowkey_cell_count,
            ObTabletJoin::TableJoinInfo &table_join_info,
            bool &is_plain_query);

      private:
        int add_unique_column( uint64_t column_id, 
            uint64_t *basic_columns, int64_t &basic_column_count, 
            int64_t &pos, bool &is_duplicate);
        //max column id is OB_ALL_MAX_COLUMN_ID 
        common::ObBitmap<uint64_t> column_ids_;
    };
  }
}

#endif /* _OB_CS_CREATE_PLAN_H */

