
#include "ob_cs_get_plan_builder.h"
#include "common/ob_new_scanner_helper.h"
#include "mergeserver/ob_merge_server_service.h"
#include "mergeserver/ob_merge_server_main.h"


ObCsGetPlanBuilder::ObCsGetPlanBuilder()
{

}
ObCsGetPlanBuilder::~ObCsGetPlanBuilder()
{

}

int ObCsGetPlanBuilder::init_tablet_get_param(ObSqlPlanParam &plan_param, ObSqlPlanContext &plan_context, ObSqlGetSimpleParam &tablet_get_param)
{
  int ret = OB_SUCCESS;
  ObObj val;
  int64_t read_consistency_val = 0;
  bool is_plain_query = false;

  // step 1. determine rowkeys for get
  if (OB_SUCCESS == ret)
  {
    // ObMsSqlGetRequest中需要两个参数：table id, rowkey list
    // range 指向sql_read_strategy_的空间
    OB_ASSERT(plan_param.rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    if (OB_SUCCESS != (ret = cons_get_rows(plan_param.get_request_get_param(), plan_param.sql_read_strategy_)))
    {
      TBSYS_LOG(WARN, "fail to construct rowkeys for get. ret=%d", ret);
    }
    plan_param.get_request_get_param().set_table_id(plan_param.table_id_, plan_param.base_table_id_);
  }
  
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
    tablet_get_param.set_is_read_consistency(read_consistency_val > 0);
  }

  // step 3. Basic column info 
  if (OB_SUCCESS == ret)    
  {
    ret = get_basic_column(plan_param, plan_context.schema_mgr_, tablet_get_param, is_plain_query);
  }

  // step 4. other meta data for ObSqlReadSimpeParam
  if (OB_SUCCESS == ret)
  {
    tablet_get_param.set_request_timeout(plan_param.initial_timeout_us_);
    tablet_get_param.set_is_only_static_data(plan_param.only_static_data_);
    tablet_get_param.set_data_version(plan_param.data_version_);
    tablet_get_param.set_table_id(plan_param.table_id_, plan_param.base_table_id_);
  }

  return ret;
}

int ObCsGetPlanBuilder::build(ObSqlPlanParam &plan_param, ObSqlPlanContext &plan_context)
{
  int ret = OB_SUCCESS;
  ObHuskTabletGetV2 *op_tablet_get = plan_param.get_op_tablet_get(true); // alloc
  if (NULL == op_tablet_get)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  if (OB_SUCCESS == ret)
  {
    ret = init_tablet_get_param(plan_param, plan_context, op_tablet_get->get_get_param());
    if (OB_SUCCESS == ret)
    {
      plan_param.op_root_ = op_tablet_get;
    }
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

  ObProject *op_project = plan_param.get_op_project();
  if (OB_SUCCESS == ret)
  {
    if (NULL == op_project)
    {
      ret = OB_NOT_INIT;
    }
    if (OB_SUCCESS == ret)
    {
      if (plan_param.is_skip_empty_row_)
      {
        ObSqlExpression special_column;
        special_column.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
        if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, special_column)))
        {
          TBSYS_LOG(WARN, "fail to create column expression. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = op_project->add_output_column(special_column)))
        {
          TBSYS_LOG(WARN, "fail to add special is-row-empty-column to project. ret=%d", ret);
        }
      }
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
  return ret;
}

int ObCsGetPlanBuilder::cons_get_rows(ObSqlGetSimpleParam &get_param, ObSqlReadStrategy &read_strategy) const
{
  // cons get rows in ObRpcScan::open(), due to prepare statement
  UNUSED(get_param);
  UNUSED(read_strategy);
  return OB_SUCCESS;
#if 0
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  ObArray<ObRowkey> rowkey_array;
  // TODO lide.wd: rowkey obj storage needed. varchar use orginal buffer, will be copied later
  PageArena<ObObj,ModulePageAllocator> rowkey_objs_allocator(
      PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_MOD_DEFAULT));
  // try  'where (k1,k2,kn) in ((a,b,c), (e,f,g))'
  if (OB_SUCCESS != (ret = read_strategy.find_rowkeys_from_in_expr(rowkey_array, rowkey_objs_allocator)))
  {
    TBSYS_LOG(WARN, "fail to find rowkeys in IN operator. ret=%d", ret);
  }
  else if (rowkey_array.count() > 0)
  {
    for (idx = 0; idx < rowkey_array.count(); idx++)
    {
      //深拷贝，从rowkey_objs_allocator 拷贝到了allocator_中
      if (OB_SUCCESS != (ret = get_param.add_rowkey(rowkey_array.at(idx), true)))
      {
        TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
        break;
      }
    }
  }
  // try  'where k1=a and k2=b and kn=n', only one rowkey
  else if (OB_SUCCESS != (ret = read_strategy.find_rowkeys_from_equal_expr(rowkey_array, rowkey_objs_allocator)))
  {
    TBSYS_LOG(WARN, "fail to find rowkeys from where equal condition, ret=%d", ret);
  }
  else if (rowkey_array.count() > 0)
  {
    for (idx = 0; idx < rowkey_array.count(); idx++)
    {
      if (OB_SUCCESS != (ret = get_param.add_rowkey(rowkey_array.at(idx), true)))
      {
        TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
        break;
      }
    }
    OB_ASSERT(idx == 1);
  }
  rowkey_objs_allocator.free();
  return ret;
#endif
}

