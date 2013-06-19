/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rpc_scan.cpp
 *
 * ObRpcScan operator
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_rpc_scan.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_obj_cast.h"
#include "mergeserver/ob_merge_server_service.h"
#include "mergeserver/ob_merge_server_main.h"
#include "mergeserver/ob_ms_sql_get_request.h"
#include "ob_sql_read_strategy.h"
#include "ob_cs_scan_plan_builder.h"
#include "ob_cs_get_plan_builder.h"
#include "ob_stmt.h"
#include "common/ob_profile_type.h"
#include "common/ob_profile_log.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::mergeserver;

ObRpcScan::ObRpcScan() :
  plan_param_(), 
  sql_scan_request_(),
  sql_get_request_(),
  cur_row_(),
  cur_row_desc_(),
  get_row_desc_(),
  timeout_us_(0)
{
}

ObRpcScan::~ObRpcScan()
{
}

int ObRpcScan::init(ObSqlContext *context)
{
  int ret = OB_SUCCESS;
  const ObTableSchema * schema = NULL;
  
  if (NULL == context)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == context->cache_proxy_
           || NULL == context->async_rpc_
           || NULL == context->schema_manager_
           || NULL == context->session_info_
           || NULL == context->merge_service_
           || NULL == context->transformer_allocator_)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (plan_param_.base_table_id_ == OB_INVALID_ID)
  {
    TBSYS_LOG(WARN, "must set table_id_ first. table_id_=%ld", plan_param_.table_id_);
    ret = OB_NOT_INIT;
  }
  else
  {
    // init rowkey_info
    if (NULL == (schema = context->schema_manager_->get_table_schema(plan_param_.base_table_id_)))
    {
      TBSYS_LOG(WARN, "fail to get table schema. table_id[%ld]", plan_param_.base_table_id_);
      ret = OB_ERROR;
    }
    else
    {
      plan_param_.rowkey_info_ = schema->get_rowkey_info(); // copy
      plan_param_.allocator_ = context->transformer_allocator_;

      plan_context_.session_info_ = context->session_info_;
      plan_context_.merge_service_ = context->merge_service_;
      plan_context_.schema_mgr_ = context->schema_manager_;

      sql_scan_request_.set_tablet_location_cache_proxy(context->cache_proxy_);
      sql_get_request_.set_tablet_location_cache_proxy(context->cache_proxy_);
      sql_scan_request_.set_merger_async_rpc_stub(context->async_rpc_);
      sql_get_request_.set_merger_async_rpc_stub(context->async_rpc_);
    }
  }
  return ret;
}

// called in ObTransformer, plan created only once
// Not implemented in open() can avoid rebuilding plan
int ObRpcScan::create_plan()
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == ret)
  {
    if (plan_param_.read_method_ == ObSqlReadStrategy::USE_SCAN)
    {
      ObCsScanPlanBuilder scan_builder;
      if (OB_SUCCESS != (ret = scan_builder.build(plan_param_, plan_context_)))
      {
        TBSYS_LOG(WARN, "fail to build plan for scan. ret=%d", ret);
      }
      else
      {//TODO: remove this
        TBSYS_LOG(DEBUG, "build scan plan: %s", to_cstring(plan_param_.inner_plan_));
      }
    }
    else if (plan_param_.read_method_ == ObSqlReadStrategy::USE_GET)
    {
      ObCsGetPlanBuilder get_builder;
      if (OB_SUCCESS != (ret = get_builder.build(plan_param_, plan_context_)))
      {
        TBSYS_LOG(WARN, "fail to build plan for get. ret=%d", ret);
      }
      else
      {//TODO: remove this
        TBSYS_LOG(DEBUG, "build get plan: %s", to_cstring(plan_param_.inner_plan_));
      }
    }
  }

  /* other init */
  if (OB_SUCCESS == ret)
  {
    // cur_row_desc_ contains selected columns
    // get_row_desc_ contains selected columns and a special column
    // for scan operation, outer and internal can share same row desc
    // but for get operation, can not share them.
    cur_row_.set_row_desc(cur_row_desc_);
    if (plan_param_.read_method_ == ObSqlReadStrategy::USE_GET)
    {
      get_row_desc_.reset();
      if (OB_SUCCESS != (ret = cons_row_desc(get_row_desc_)))
      {
        TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
      }
    }
    TBSYS_LOG(DEBUG, "read_method_ [%s]", plan_param_.read_method_ == ObSqlReadStrategy::USE_SCAN ? "SCAN" : "GET");
    FILL_TRACE_LOG("cur row desc: %s", to_cstring(cur_row_desc_));
  }

  if (OB_SUCCESS == ret)
  {
    OB_ASSERT(NULL != plan_param_.op_root_);
    if (OB_SUCCESS != (ret = plan_param_.inner_plan_.add_phy_query(plan_param_.op_root_, NULL, true)))
    {
      TBSYS_LOG(WARN, "fail to add phy query");
    }
  }
  return ret;
}

int ObRpcScan::cons_row_desc(ObRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  ObProject *op_project = plan_param_.get_op_project();
  if (NULL == op_project || op_project->get_output_column_size() <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "should has project");
  }

  if (OB_SUCCESS == ret)
  {
    const common::ObArray<ObSqlExpression> &columns = op_project->get_output_columns();
    for (int64_t i = 0; OB_SUCCESS == ret && i < columns.count(); i ++)
    {
      const ObSqlExpression &expr = columns.at(i);
      if (OB_SUCCESS != (ret = row_desc.add_column_desc(expr.get_table_id(), expr.get_column_id())))
      {
        TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
      }
    }
  }
  return ret;
}


void ObRpcScan::set_hint(const common::ObRpcScanHint &hint)
{
  // max_parallel_count
  if (hint.max_parallel_count <= 0)
  {
    plan_param_.max_parallel_count_ = 20;
  }
  else
  {
    plan_param_.max_parallel_count_ = hint.max_parallel_count;
  }
  // max_memory_limit
  if (hint.max_memory_limit < 1024 * 1024 * 2)
  {
    plan_param_.max_memory_limit_ = 1024 * 1024 * 2;
  }
  else
  {
    plan_param_.max_memory_limit_ = hint.max_memory_limit;
  }
  plan_param_.initial_timeout_us_ = hint.timeout_us;
  plan_param_.only_frozen_version_data_ = hint.only_frozen_version_data_;
  plan_param_.only_static_data_ = hint.only_static_data_;
}

int ObRpcScan::cons_scan_range(ObSqlScanSimpleParam &scan_param)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObNewRange &range = scan_param.get_range();
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  if (OB_SUCCESS != (ret = plan_param_.sql_read_strategy_.find_scan_range(range, found, false)))
  {
    TBSYS_LOG(WARN, "fail to find range. ret=%d", ret);
  }
  return ret;
}

int ObRpcScan::cons_get_rows(ObSqlGetSimpleParam &get_param)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  ObArray<ObRowkey> rowkey_array;
  // TODO lide.wd: rowkey obj storage needed. varchar use orginal buffer, will be copied later
  PageArena<ObObj,ModulePageAllocator> rowkey_objs_allocator(
      PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_MOD_DEFAULT));
  // try  'where (k1,k2,kn) in ((a,b,c), (e,f,g))'
  if (OB_SUCCESS != (ret = plan_param_.sql_read_strategy_.find_rowkeys_from_in_expr(rowkey_array, rowkey_objs_allocator)))
  {
    TBSYS_LOG(WARN, "fail to find rowkeys in IN operator. ret=%d", ret);
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
  }
  // try  'where k1=a and k2=b and kn=n', only one rowkey
  else if (OB_SUCCESS != (ret = plan_param_.sql_read_strategy_.find_rowkeys_from_equal_expr(rowkey_array, rowkey_objs_allocator)))
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

  // extra init for get row desc, until now we can do this
  if (OB_SUCCESS == ret)
  {
    if (get_param.get_row_size() <= 0 )
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "should has a least one row");
    }
    else
    {
      get_row_desc_.set_rowkey_cell_count(get_param[0]->length());
    }
  }
 
  rowkey_objs_allocator.free();
  return ret;
}

int ObRpcScan::open()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(my_phy_plan_);
  ObResultSet * my_result_set = my_phy_plan_->get_result_set();
  plan_param_.inner_plan_.set_result_set(my_result_set);
  ObVersion frozen_version = my_phy_plan_->get_curr_frozen_version();
  plan_param_.inner_plan_.set_curr_frozen_version(frozen_version);
  timeout_us_ = plan_param_.initial_timeout_us_; 
  if (plan_param_.only_frozen_version_data_)
  {
    plan_param_.data_version_ = frozen_version;
    FILL_TRACE_LOG("static_data_version=%s", to_cstring(frozen_version));
  }
  
  if (NULL == plan_context_.merge_service_)
  {
    ret = OB_NOT_INIT;
  }
  // Get
  else if (OB_SUCCESS == ret && plan_param_.read_method_ == ObSqlReadStrategy::USE_GET)
  {
    sql_get_request_.alloc_request_id();
    if (OB_SUCCESS != (ret = cons_get_rows(plan_param_.get_param_)))
    {
      TBSYS_LOG(WARN, "fail to construct get rows.ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = sql_get_request_.set_row_desc(get_row_desc_)))
    {
      TBSYS_LOG(WARN, "fail to set row desc:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = sql_get_request_.init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_GET)))
    {
      TBSYS_LOG(WARN, "fail to init sql_get_request. ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = sql_get_request_.set_request_param(plan_param_)))
    {
      TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
          plan_param_.max_parallel_count_, ret);
    }
    if (OB_SUCCESS == ret)
    {
      sql_get_request_.set_timeout_percent((int32_t)plan_context_.merge_service_->get_config().timeout_percent);
      if (OB_SUCCESS != (ret = sql_get_request_.open()))
      {
        TBSYS_LOG(WARN, "fail to open get request. ret=%d", ret);
      }
    }
  }
  // Scan
  else if (OB_SUCCESS == ret && plan_param_.read_method_ == ObSqlReadStrategy::USE_SCAN)
  {
    if (OB_SUCCESS != (ret = cons_scan_range(plan_param_.scan_param_)))
    {
      TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = sql_scan_request_.initialize()))
    {
      TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
    }
    else
    {
      sql_scan_request_.alloc_request_id();
      if (OB_SUCCESS != (ret = sql_scan_request_.init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN)))
      {
        TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
      }
    }
    if (OB_SUCCESS == ret)
    {
      sql_scan_request_.set_timeout_percent((int32_t)plan_context_.merge_service_->get_config().timeout_percent);
      if(OB_SUCCESS != (ret = sql_scan_request_.set_request_param(plan_param_)))
      {
        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
            plan_param_.max_parallel_count_, ret);
      }
    }
  }
  return ret;
}

int ObRpcScan::close()
{
  int ret = OB_SUCCESS;
  if (plan_param_.read_method_ == ObSqlReadStrategy::USE_GET)
  {
    sql_get_request_.close();
    sql_get_request_.reset();
  }
  else
  {
    sql_scan_request_.close();
    sql_scan_request_.reset();
  }
  return ret;
}

int ObRpcScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(plan_param_.base_table_id_ <= 0 || 0 >= cur_row_desc_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init, tid=%lu, column_num=%ld", plan_param_.base_table_id_, cur_row_desc_.get_column_num());
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &cur_row_desc_;
  }
  return ret;
}

int ObRpcScan::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (ObSqlReadStrategy::USE_GET == plan_param_.read_method_)
  {
    row = NULL;
    ret = sql_get_request_.get_next_row(cur_row_);
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      row = &cur_row_;
    }
  }
  else if (ObSqlReadStrategy::USE_SCAN == plan_param_.read_method_)
  {
    ret = get_next_compact_row(row); // 可能需要等待CS返回
  }
  else
  {
    TBSYS_LOG(WARN, "not init. read_method_=%d", plan_param_.read_method_);
    ret = OB_NOT_INIT;
  }
  return ret;
}

/**
 * 函数功能： 从scan_event中获取一行数据
 * 说明：
 * wait的功能：从finish_queue中阻塞地pop出一个事件（如果没有事件则阻塞）， 然后调用process_result()处理事件
 */
int ObRpcScan::get_next_compact_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool can_break = false;
  int64_t remain_us = 0;
  row = NULL;
  do
  {
    if (OB_UNLIKELY(my_phy_plan_->is_timeout(&remain_us)))
    {
      can_break = true;
      ret = OB_PROCESS_TIMEOUT;
    }
    else if (OB_LIKELY(OB_SUCCESS == (ret = sql_scan_request_.get_next_row(cur_row_))))
    {
      // got a row without block,
      // no need to check timeout, leave this work to upper layer
      can_break = true;
    }
    else if (OB_ITER_END == ret && sql_scan_request_.is_finish())
    {
      // finish all data
      // can break;
      can_break = true;
    }
    else if (OB_ITER_END == ret)
    {
      // need to wait for incomming data
      can_break = false;
      timeout_us_ = std::min(timeout_us_, remain_us);
      if( OB_SUCCESS != (ret = sql_scan_request_.wait_single_event(timeout_us_)))
      {
        if (timeout_us_ <= 0)
        {
          TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", timeout_us_);
        }
        can_break = true;
      }
      else
      {
        TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", timeout_us_);
      }
    }
    else
    {
      // encounter an unexpected error or
      TBSYS_LOG(WARN, "Unexprected error. ret=%d, cur_row_desc[%s], read_method_[%d]", 
          ret, to_cstring(cur_row_desc_), plan_param_.read_method_);
      can_break = true;
    }
  } while(false == can_break);
  if (OB_SUCCESS == ret)
  {
    row = &cur_row_;
  }
  return ret;
}


int ObRpcScan::add_output_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  bool is_cid = false;
  ObProject *op_project = plan_param_.get_op_project(true); // alloc if not exsit
  if (plan_param_.base_table_id_ <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "must call set_table() first. base_table_id_=%lu",
        plan_param_.base_table_id_);
  }
  else if (NULL == op_project)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "fail to allocate memory");
  }
  else if ((OB_SUCCESS == (ret = expr.is_column_index_expr(is_cid)))  && (true == is_cid))
  {
    // 添加基本列
    if (OB_SUCCESS != (ret = op_project->add_output_column(expr)))
    {
      TBSYS_LOG(WARN, "fail to add output column ret=%d", ret);
    }
  }
  else
  {
    // 添加复合列
    if (OB_SUCCESS != (ret = op_project->add_output_column(expr)))
    {
      TBSYS_LOG(WARN, "fail to add output column ret=%d", ret);
    }
  }
  // cons row desc
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(expr.get_table_id(), expr.get_column_id())))
    {
      TBSYS_LOG(WARN, "fail to add column to scan param. ret=%d, tid_=%lu, cid=%lu", ret, expr.get_table_id(), expr.get_column_id());
    }
  }
  return ret;
}

int ObRpcScan::set_table(const uint64_t table_id, const uint64_t base_table_id)
{
  int ret = OB_SUCCESS;
  if (0 >= base_table_id)
  {
    TBSYS_LOG(WARN, "invalid table id: %lu", base_table_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    plan_param_.table_id_ = table_id;
    plan_param_.base_table_id_ = base_table_id;
  }
  return ret;
}


int ObRpcScan::add_filter(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  ObFilter *op_filter = plan_param_.get_op_filter(true); // alloc if not exsit
  if (NULL == op_filter)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "fail to allocate memory");
  }
  else if (OB_SUCCESS != (ret = plan_param_.sql_read_strategy_.add_filter(expr)))
  {
    TBSYS_LOG(WARN, "fail to add filter to sql read strategy:ret[%d]", ret);
  }
  else
  {  
    if (OB_SUCCESS != (ret = op_filter->add_filter(expr)))
    {
      TBSYS_LOG(WARN, "fail to add composite column to scan param. ret=%d", ret);
    }
  }
  return ret;
}

// add_group_column, add_aggr_column这两个函数的调用顺序有一个约定：
// 总是先调用add_group_column，再调用add_aggr_column
// 在这个假设的支持下：
//  如果group operator存在，则add_aggr_column也直接把数据存到group operator下
//  如果group operator不存在，则add_aggr_column把数据存到aggregate operator下
int ObRpcScan::add_group_column(const uint64_t tid, const uint64_t cid)
{
  int ret = OB_SUCCESS;
  ObMergeGroupBy *op_group = NULL;
  ObSort *op_group_columns_sort = NULL;
  
  // when you get here, must call add_group_column before add_aggr_column
  OB_ASSERT(NULL == plan_param_.get_op_scalar_agg());

  op_group = plan_param_.get_op_group(true); // alloc if not exist
  op_group_columns_sort = plan_param_.get_op_group_columns_sort(true); // alloc if not exist
  if (NULL == op_group || NULL == op_group_columns_sort)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "fail to allocate memory.group=%p, sort=%p", op_group, op_group_columns_sort);
  }
  else if (OB_SUCCESS != (ret = op_group_columns_sort->add_sort_column(tid, cid, true)))
  {
    TBSYS_LOG(WARN, "Add sort column of ObSqlReadParam sort operator failed. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = op_group->add_group_column(tid, cid)))
  {
    TBSYS_LOG(WARN, "fail to add group column. ret=%d", ret);    
  }
  return ret;
}

int ObRpcScan::add_aggr_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
  ObMergeGroupBy *op_group = NULL;
  ObScalarAggregate *op_scalar_agg = NULL; 
  
  if (cur_row_desc_.get_column_num() <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "Output column(s) of ObRpcScan must be set first, ret=%d", ret);
  }
  else if ((ret = cur_row_desc_.add_column_desc(
          expr.get_table_id(),
          expr.get_column_id())) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Failed to add column desc, err=%d", ret);
  }
  else if (OB_LIKELY(NULL != plan_param_.get_op_group()))
  {
    op_group = plan_param_.get_op_group();
    if ((ret = op_group->add_aggr_column(expr)) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Failed to add aggregate column desc, err=%d", ret);
    }
  }
  else
  {
    op_scalar_agg = plan_param_.get_op_scalar_agg(true); // alloc if not exist
    if (NULL == op_scalar_agg)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "fail to allocate memory");
    }
    else if ((ret = op_scalar_agg->add_aggr_column(expr)) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Failed to add aggregate column desc, err=%d", ret);
    }
  }
  return ret;
}

int ObRpcScan::set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset)
{
  int ret = OB_SUCCESS;
  ObLimit *op_limit = plan_param_.get_op_limit(true);
  if (NULL == op_limit)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "fail to allocate memory");
  }
  else if (OB_SUCCESS != (ret = op_limit->set_limit(limit, offset)))
  {
    TBSYS_LOG(WARN, "fail to set limit. ret=%d", ret);
  }
  return ret;
}

int64_t ObRpcScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "RpcScan(");
  if (ObSqlReadStrategy::USE_GET == plan_param_.read_method_)
  {
    databuff_printf(buf, buf_len, pos, "type=sql_get_request");
  }
  else if (ObSqlReadStrategy::USE_SCAN == plan_param_.read_method_)
  {
    databuff_printf(buf, buf_len, pos, "type=sql_scan_request");
  }
  databuff_printf(buf, buf_len, pos, ")");
  return pos;
}


