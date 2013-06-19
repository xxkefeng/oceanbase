/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cs_get_plan_builder.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_CS_GET_PLAN_BUILDER_H
#define _OB_CS_GET_PLAN_BUILDER_H 1
#include "ob_phy_operator.h"
#include "ob_sql_plan_param.h"
#include "ob_cs_plan_builder.h"

namespace oceanbase
{
  namespace sql
  {
    class ObCsGetPlanBuilder : public ObCsPlanBuilder
    {
      public:
        ObCsGetPlanBuilder();
        ~ObCsGetPlanBuilder();
        int build(ObSqlPlanParam &plan_param, ObSqlPlanContext &plan_context);
      private:
        int cons_get_rows(ObSqlGetSimpleParam &get_param, ObSqlReadStrategy &read_strategy) const;
        int init_tablet_get_param(ObSqlPlanParam &plan_param, ObSqlPlanContext &plan_context, ObSqlGetSimpleParam &tablet_get_param);
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_CS_GET_PLAN_BUILDER_H */
