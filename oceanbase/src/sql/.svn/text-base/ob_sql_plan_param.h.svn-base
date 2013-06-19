/*
 * (C) 1999-2013 Alibaba Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_plan_param.h,  04/26/2013 07:52:36 PM Yu Huang Exp $
 * 
 * Author:  
 *   Huang Yu <xiaochu.yh@alipay.com>
 * Description:  
 *   This file defines parameters for building ms->cs sql plan
 */
#ifndef __OB_SQL_PLAN_PARAM__
#define __OB_SQL_PLAN_PARAM__
#include "sql/ob_sql_scan_simple_param.h"
#include "sql/ob_sql_get_simple_param.h"
#include "sql/ob_sql_get_param.h"
#include "sql/ob_project.h"
#include "sql/ob_sort.h"
#include "sql/ob_limit.h"
#include "sql/ob_filter.h"
#include "sql/ob_table_rename.h"
#include "sql/ob_merge_groupby.h"
#include "sql/ob_scalar_aggregate.h"
#include "sql/ob_husk_tablet_scan_v2.h"
#include "sql/ob_husk_tablet_get_v2.h"
#include "sql/ob_sql_read_strategy.h"
#include "sql/ob_physical_plan.h"
#include "common/ob_stack_allocator.h"

#define DEF_GET_OP_FUNCTION(type_name, member_name) \
  type_name *get_##member_name(bool create_if_not_exist = false)

namespace oceanbase
{
  namespace common
  {
     class ObTabletLocationCacheProxy;
  }
  namespace mergeserver
  {
    class ObMergerAsyncRpcStub;
    class ObMergeServerService;
  }
  namespace sql
  {
    class ObSQLSessionInfo;
  }

  namespace sql
  {
    class ObSqlPlanContext
    {
      public:
        ObSqlPlanContext() : 
          session_info_(NULL),
          merge_service_(NULL),
          schema_mgr_(NULL)
      {
      }
      public:
        ObSQLSessionInfo *session_info_;
        const oceanbase::mergeserver::ObMergeServerService *merge_service_;
        const ObSchemaManagerV2 *schema_mgr_;
    };

    class ObSqlPlanParam
    {
      public:
        ObSqlPlanParam() : 
          read_method_(0),
          scan_param_(),
          get_param_(),
          rowkey_info_(),
          max_parallel_count_(1),
          max_memory_limit_(0),
          initial_timeout_us_(0),
          table_id_(OB_INVALID_ID),
          base_table_id_(OB_INVALID_ID),
          is_skip_empty_row_(true),
          is_read_consistency_(true),
          only_static_data_(false),
          only_frozen_version_data_(false),
          data_version_(OB_NEWEST_DATA_VERSION),
          op_root_(NULL),
          inner_plan_(),
          allocator_(NULL),
          op_project_(NULL),
          op_scalar_agg_(NULL),
          op_group_(NULL),
          op_group_columns_sort_(NULL),
          op_limit_(NULL),
          op_filter_(NULL),
          op_table_rename_(NULL),
          op_tablet_scan_(NULL),
          op_tablet_get_(NULL)
        {
          sql_read_strategy_.set_rowkey_info(rowkey_info_);
        }

      public:
        inline ObSqlScanSimpleParam & get_request_scan_param() 
        {
          return scan_param_;
        }
        inline ObSqlGetSimpleParam & get_request_get_param()
        {
          return get_param_;
        }
      public:
        // scan or get info
        int32_t read_method_;        
        
        ObSqlScanSimpleParam scan_param_;
        ObSqlGetSimpleParam get_param_;
        ObRowkeyInfo rowkey_info_;

        // request info
        int64_t max_parallel_count_;
        int64_t max_memory_limit_;
        int64_t initial_timeout_us_;        

        // table info
        uint64_t table_id_;
        uint64_t base_table_id_;

        // data info
        bool is_skip_empty_row_;
        bool is_read_consistency_;
        bool only_static_data_;
        bool only_frozen_version_data_;
        int64_t data_version_;
      public:
        ObSqlReadStrategy sql_read_strategy_;
        ObPhyOperator *op_root_;
        ObPhysicalPlan inner_plan_;
        ObIAllocator *allocator_;
      public:
        DEF_GET_OP_FUNCTION(ObProject, op_project);
        DEF_GET_OP_FUNCTION(ObScalarAggregate, op_scalar_agg);
        DEF_GET_OP_FUNCTION(ObMergeGroupBy, op_group);
        DEF_GET_OP_FUNCTION(ObSort, op_group_columns_sort);
        DEF_GET_OP_FUNCTION(ObLimit, op_limit);
        DEF_GET_OP_FUNCTION(ObFilter, op_filter);
        DEF_GET_OP_FUNCTION(ObTableRename, op_table_rename);
        DEF_GET_OP_FUNCTION(ObHuskTabletScanV2, op_tablet_scan);
        DEF_GET_OP_FUNCTION(ObHuskTabletGetV2, op_tablet_get);
      private:
        ObProject *op_project_;
        ObScalarAggregate *op_scalar_agg_;
        ObMergeGroupBy *op_group_;
        ObSort *op_group_columns_sort_;
        ObLimit *op_limit_;
        ObFilter *op_filter_;
        ObTableRename *op_table_rename_;
        ObHuskTabletScanV2 *op_tablet_scan_;
        ObHuskTabletGetV2 *op_tablet_get_;
      private:
        inline void *trans_malloc(const size_t nbyte);
        inline void trans_free(void* p);
    };
  }
}


#endif // __OB_SQL_PLAN_PARAM__


