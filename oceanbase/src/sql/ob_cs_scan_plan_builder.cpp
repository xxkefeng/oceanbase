/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cs_scan_plan_builder.cpp
 *
 * Authors:
 *   Huang Yu <xiaochu.yh@alipay.com>
 *
 */

#include "ob_cs_scan_plan_builder.h"
#include "mergeserver/ob_merge_server_service.h"
#include "mergeserver/ob_merge_server_main.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObCsScanPlanBuilder::ObCsScanPlanBuilder()
{
}

ObCsScanPlanBuilder::~ObCsScanPlanBuilder()
{
}

// need to set scan range, consistency info, scan columns
int ObCsScanPlanBuilder::init_tablet_scan_param(ObSqlPlanParam &plan_param, ObSqlPlanContext &plan_context, ObSqlScanSimpleParam &tablet_scan_param)
{
  int ret = OB_SUCCESS;
  int64_t read_consistency_val = 1;
  ObObj val;
  ObSqlScanSimpleParam &request_param = plan_param.get_request_scan_param();

  /* check if param is valid */
  if (OB_INVALID_ID == plan_param.table_id_ || OB_INVALID_ID == plan_param.base_table_id_)
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    request_param.get_range().table_id_ = plan_param.base_table_id_;
  }
  
#if 0
  // step 1. determine scan range
  if (OB_SUCCESS == ret)
  {
    request_param.get_range().border_flag_.set_inclusive_start();
    request_param.get_range().border_flag_.set_inclusive_end();
    // range 指向sql_read_strategy_的空间
    OB_ASSERT(plan_param.rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    if (OB_SUCCESS != (ret = plan_param.sql_read_strategy_.find_scan_range(request_param.get_range(), found, false))) // TODO: remove ?
    {
      TBSYS_LOG(WARN, "fail to find range %lu", plan_param.base_table_id_);
    }
    TBSYS_LOG(INFO, "dump scan request range: %s", to_cstring(request_param.get_range()));
  }
#endif

  // step 2. get read consistency
  if (OB_SUCCESS == ret)
  {
    OB_ASSERT(NULL != plan_context.session_info_);
    if (OB_SUCCESS != (ret = plan_context.session_info_->get_sys_variable_value(ObString::make_string("ob_read_consistency"), val)))
    {
      const mergeserver::ObMergeServerService &service = mergeserver::ObMergeServerMain::get_instance()->get_merge_server().get_service();
      read_consistency_val = service.check_instance_role(true);
      ret = OB_SUCCESS;
    }
    else if (OB_SUCCESS != (ret = val.get_int(read_consistency_val)))
    {
      TBSYS_LOG(WARN, "wrong obj type for ob_read_consistency, err=%d", ret);
    }    
    tablet_scan_param.set_is_read_consistency(read_consistency_val > 0);
  }

  // step 3. Basic column info
  if (OB_SUCCESS == ret)
  {
    bool is_plain_query = false;
    ret = get_basic_column(plan_param, plan_context.schema_mgr_, tablet_scan_param, is_plain_query);
  }
  
  // step 4. other meta data for ObSqlReadSimpeParam
  if (OB_SUCCESS == ret)
  {
    tablet_scan_param.set_request_timeout(plan_param.initial_timeout_us_);
    tablet_scan_param.set_is_only_static_data(plan_param.only_static_data_);
    tablet_scan_param.set_data_version(plan_param.data_version_);
    tablet_scan_param.set_table_id(plan_param.table_id_, plan_param.base_table_id_);
  }
  return ret;
}

int ObCsScanPlanBuilder::build(ObSqlPlanParam &plan_param, ObSqlPlanContext &plan_context)
{
  int ret = OB_SUCCESS;
  bool is_plain_query = false;

  ObHuskTabletScanV2 *op_tablet_scan = plan_param.get_op_tablet_scan(true);
  if (NULL == op_tablet_scan) // need memory alloc check
  {
    TBSYS_LOG(WARN, "fail to allocate memory for hust_tablet_scan_op");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = init_tablet_scan_param(plan_param, plan_context, op_tablet_scan->get_scan_param())))
    {
      TBSYS_LOG(WARN, "fail to init tablet scan param. ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    plan_param.op_root_  = op_tablet_scan;
  }

  // in 'as' case (e.g. select c1+c2 as my_col from t;) rename is required
  if (plan_param.base_table_id_ != plan_param.table_id_)
  {
    ObTableRename *op_table_rename = plan_param.get_op_table_rename(true); // alloc
    if (NULL == op_table_rename)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    if (OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (op_table_rename->set_table(plan_param.table_id_, plan_param.base_table_id_)))
      {
        TBSYS_LOG(WARN, "fail to set table id. renamed:%lu, base:%lu",
            plan_param.table_id_, plan_param.base_table_id_);
      }
      else if(OB_SUCCESS != (ret = op_table_rename->set_child(0, *plan_param.op_root_)))
      {
        TBSYS_LOG(WARN, "fail to set rename child. ret=%d", ret);
      }
      else
      {
        plan_param.op_root_ = op_table_rename;
      }
    }
  }

  ObFilter *op_filter = plan_param.get_op_filter();
  if ((OB_SUCCESS == ret) && (NULL != op_filter))
  {
    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = op_filter->set_child(0, *plan_param.op_root_)))
      {
        TBSYS_LOG(WARN, "fail to set child of filter. ret=%d", ret);
      }
      else
      {
        plan_param.op_root_  = op_filter;
      }
    }
  }

  if (OB_SUCCESS == ret && !is_plain_query)
  {
    ObProject *op_project = plan_param.get_op_project();
    if (NULL == op_project)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(WARN, "must have a project. ret=%d", ret);
    }
    if (OB_SUCCESS == ret) 
    {
      if (OB_SUCCESS != (ret = op_project->set_child(0, *plan_param.op_root_)))
      {
        TBSYS_LOG(WARN, "fail to set child of project. ret=%d", ret);
      }
      else
      {
        plan_param.op_root_  = op_project;
      }
    }
  }

  ObScalarAggregate *op_scalar_agg = plan_param.get_op_scalar_agg();
  ObMergeGroupBy *op_group = plan_param.get_op_group();
  if ((OB_SUCCESS == ret) && (NULL != op_scalar_agg || NULL != op_group))
  {
    if (NULL != op_scalar_agg && NULL != op_group)
    {
      ret = OB_ERR_GEN_PLAN;
      TBSYS_LOG(WARN, "Group operator and scalar aggregate operator"
          " can not appear in TabletScan at the same time. ret=%d", ret);
    }
    else if (NULL != op_scalar_agg)
    {
      if (OB_SUCCESS != (ret = op_scalar_agg->set_child(0, *plan_param.op_root_)))
      {
        TBSYS_LOG(WARN, "fail to set child of scalar agg. ret=%d", ret);
      }
      else
      {
        plan_param.op_root_  = op_scalar_agg;
      }
    }
    else if (NULL != op_group)
    {
      // add group by
      ObSort *op_group_columns_sort = plan_param.get_op_group_columns_sort();
      if (NULL == op_group_columns_sort)
      {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Physical plan error, group need a sort operator. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = op_group_columns_sort->set_child(0, *plan_param.op_root_)))
      {
        TBSYS_LOG(WARN, "Fail to set child of sort operator. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = op_group->set_child(0, *op_group_columns_sort)))
      {
        TBSYS_LOG(WARN, "fail to set child of group. ret=%d", ret);
      }
      else
      {
        plan_param.op_root_  = op_group;
      }
    }
  }

  ObLimit *op_limit = plan_param.get_op_limit();
  if ((OB_SUCCESS == ret) && (NULL != op_limit))
  {
    if (OB_SUCCESS != (ret = op_limit->set_child(0, *plan_param.op_root_)))
    {
      TBSYS_LOG(WARN, "fail to set child of ObLimit. ret=%d", ret);
    }
    else
    {
      plan_param.op_root_  = op_limit;
    }
  }
  return ret;
}

