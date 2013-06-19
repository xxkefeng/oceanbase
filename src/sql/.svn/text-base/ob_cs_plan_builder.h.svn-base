/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cs_plan_builder.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_CS_PLAN_BUILDER_H
#define _OB_CS_PLAN_BUILDER_H 1

#include "ob_sql_plan_param.h"
#include "common/ob_schema.h"
#include "common/ob_bitmap.h"

namespace oceanbase
{
  namespace sql
  {
    class ObCsPlanBuilder
    {
      public:
        ObCsPlanBuilder() {}
        virtual ~ObCsPlanBuilder() {}
        virtual int build(ObSqlPlanParam &plan_param, ObSqlPlanContext &context) = 0;
      protected:
        int get_basic_column(
            ObSqlPlanParam &plan_param,
            const common::ObSchemaManagerV2 *schema_mgr,
            ObSqlReadSimpleParam &read_param,
            bool &is_plain_query) const;

        int add_unique_column(
            common::ObBitmap<uint64_t> &column_ids,
            uint64_t column_id,
            ObSqlReadSimpleParam &read_param,
            bool &is_duplicate) const;
       
    };
  }
}
#endif // _OB_CS_PLAN_BUILDER_H
