/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 *  ob_plan_executor.h
 *
 * Authors:
 *  yongle.xh<yongle.xh@alipay.com>
 *
 */

#ifndef _OB_CS_PLAN_EXECUTOR_H
#define _OB_CS_PLAN_EXECUTOR_H 1

#include "common/ob_ups_rpc_proxy.h"
#include "sql/ob_tablet_scan.h"
#include "sql/ob_tablet_get.h"
#include "sql/ob_tablet_sstable_scan.h"
#include "sql/ob_sstable_scan.h"
#include "sql/ob_ups_scan.h"
#include "sql/ob_ups_multi_get.h"
#include "sql/ob_plan_context.h"
#include "ob_chunk_server.h"
#include "ob_sql_query_service.h"

namespace oceanbase
{
  using namespace common;
  using namespace sql;

  namespace chunkserver
  {
    class ObCsPlanExecutor
    {
      public:
        ObCsPlanExecutor();
        virtual ~ObCsPlanExecutor();

        int open(const ObPhysicalPlan& plan, ObPlanContext& context);
        int fill_scan_data(ObNewScanner &new_scanner);
        int close();
        void reset();
        void set_timeout_us(int64_t timeout_us) {timeout_us_ = timeout_us;}

      private:
        int64_t timeout_us_;
        const ObRowkey *cur_rowkey_;
        const ObRow *cur_row_;
        ObPhyOperator *root_op_;
        ObRowkey last_rowkey_;
        CharArena rowkey_allocator_;
        ObPlanContext *context_;
    };
  }
}

#endif /* _OB_CS_PLAN_EXECUTOR_H */

