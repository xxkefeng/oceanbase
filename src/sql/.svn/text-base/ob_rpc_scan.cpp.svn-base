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
#include "common/ob_profile_type.h"
#include "common/ob_profile_log.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::mergeserver;

ObRpcScan::ObRpcScan() :
  timeout_us_(0),
  scan_param_(NULL),
  get_param_(NULL),
  read_param_(NULL),
  cache_proxy_(NULL),
  async_rpc_(NULL),
  session_info_(NULL),
  merge_service_(NULL),
  cur_row_(),
  cur_row_desc_(),
  table_id_(OB_INVALID_ID),
  base_table_id_(OB_INVALID_ID),
  start_key_buf_(NULL),
  end_key_buf_(NULL)
{
  sql_read_strategy_.set_rowkey_info(rowkey_info_);
}


ObRpcScan::~ObRpcScan()
{
  this->destroy();
}


int ObRpcScan::init(ObSqlContext *context, const common::ObRpcScanHint &hint)
{
  int ret = OB_SUCCESS;
  if (NULL == context)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == context->cache_proxy_
           || NULL == context->async_rpc_
           || NULL == context->schema_manager_
           || NULL == context->merge_service_)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (base_table_id_ == OB_INVALID_ID)
  {
    TBSYS_LOG(WARN, "must set table_id_ first. table_id_=%ld", table_id_);
    ret = OB_NOT_INIT;
  }
  else
  {
    // init rowkey_info
    const ObTableSchema * schema = NULL;
    if (NULL == (schema = context->schema_manager_->get_table_schema(base_table_id_)))
    {
      TBSYS_LOG(WARN, "fail to get table schema. table_id[%ld]", base_table_id_);
      ret = OB_ERROR;
    }
    else
    {
      cache_proxy_ = context->cache_proxy_;
      sql_scan_request_.set_tablet_location_cache_proxy(cache_proxy_);
      sql_get_request_.set_tablet_location_cache_proxy(cache_proxy_);
      async_rpc_ = context->async_rpc_;
      sql_scan_request_.set_merger_async_rpc_stub(async_rpc_);
      sql_get_request_.set_merger_async_rpc_stub(async_rpc_);
      session_info_ = context->session_info_;
      merge_service_ = context->merge_service_;
      // copy
      rowkey_info_ = schema->get_rowkey_info();
    }
  }
  this->set_hint(hint);
  if (hint_.read_method_ == ObSqlReadStrategy::USE_SCAN)
  {
    OB_ASSERT(NULL == scan_param_);
    scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
    if (NULL == scan_param_)
    {
      TBSYS_LOG(WARN, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      read_param_ = scan_param_;
    }
  }
  else if (hint_.read_method_ == ObSqlReadStrategy::USE_GET)
  {
    OB_ASSERT(NULL == get_param_);
    get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
    if (NULL == get_param_)
    {
      TBSYS_LOG(WARN, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      read_param_ = get_param_;
    }
    if (OB_SUCCESS == ret && hint_.is_get_skip_empty_row_)
    {
      ObSqlExpression special_column;
      special_column.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
      if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, special_column)))
      {
        TBSYS_LOG(WARN, "fail to create column expression. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = get_param_->add_output_column(special_column)))
      {
        TBSYS_LOG(WARN, "fail to add special is-row-empty-column to project. ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "add special column to read param");
      }
    }


  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "read method must be either scan or get. method=%d", hint_.read_method_);
  }
  return ret;
}

int ObRpcScan::cast_range(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  bool need_buf = false;
  int64_t used_buf_len = 0;
  if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(rowkey_info_, range.start_key_, need_buf)))
  {
    TBSYS_LOG(WARN, "err=%d", ret);
  }
  else if (need_buf)
  {
    if (NULL == start_key_buf_)
    {
      start_key_buf_ = (char*)ob_malloc(OB_MAX_ROW_LENGTH, ObModIds::OB_SQL_RPC_SCAN);
    }
    if (NULL == start_key_buf_)
    {
      TBSYS_LOG(ERROR, "no memory");
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (OB_SUCCESS != (ret = ob_cast_rowkey(rowkey_info_, range.start_key_,
                                            start_key_buf_, OB_MAX_ROW_LENGTH, used_buf_len)))
    {
      TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
    }
  }
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(rowkey_info_, range.end_key_, need_buf)))
    {
      TBSYS_LOG(WARN, "err=%d", ret);
    }
    else if (need_buf)
    {
      if (NULL == end_key_buf_)
      {
        end_key_buf_ = (char*)ob_malloc(OB_MAX_ROW_LENGTH, ObModIds::OB_SQL_RPC_SCAN);
      }
      if (NULL == end_key_buf_)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      if (OB_SUCCESS != (ret = ob_cast_rowkey(rowkey_info_, range.end_key_,
                                              end_key_buf_, OB_MAX_ROW_LENGTH, used_buf_len)))
      {
        TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
      }
    }
  }
  return ret;
}

int ObRpcScan::create_scan_param(ObSqlScanParam &scan_param)
{
  int64_t start_create_scan_param = tbsys::CTimeUtil::getTime();
  int ret = OB_SUCCESS;
  ObNewRange range;
  // until all columns and filters are set, we could know the exact range
  if (OB_SUCCESS != (ret = fill_read_param(scan_param)))
  {
    TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = cons_scan_range(range)))
  {
    TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
  }
  // TODO: lide.wd 将range 深拷贝到ObSqlScanParam内部的buffer_pool_中
  else if (OB_SUCCESS != (ret = scan_param.set_range(range)))
  {
    TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
  }
  int64_t end_create_scan_param = tbsys::CTimeUtil::getTime();
  PROFILE_LOG(DEBUG, CREATE_SCAN_PARAM, end_create_scan_param - start_create_scan_param);
  TBSYS_LOG(INFO, "dump scan range: %s", to_cstring(range));
  TBSYS_LOG(DEBUG, "scan_param=%s", to_cstring(scan_param));
  return ret;
}


int ObRpcScan::create_get_param(ObSqlGetParam &get_param)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = fill_read_param(get_param)))
  {
    TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = cons_get_rows(get_param)))
    {
      TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
    }
  }
  TBSYS_LOG(DEBUG, "get_param=%s", to_cstring(get_param));
  return ret;
}

int ObRpcScan::fill_read_param(ObSqlReadParam &dest_param)
{
  int ret = OB_SUCCESS;
  ObObj val;
  int64_t read_consistency_val = 0;
  OB_ASSERT(NULL != session_info_);
  if (OB_SUCCESS != (ret = session_info_->get_sys_variable_value(ObString::make_string("ob_read_consistency"), val)))
  {
    const ObMergeServerService &service
      = ObMergeServerMain::get_instance()->get_merge_server().get_service();
    read_consistency_val = service.check_instance_role(true);
    dest_param.set_is_read_consistency(read_consistency_val);
    ret = OB_SUCCESS;
  }
  else if (OB_SUCCESS != (ret = val.get_int(read_consistency_val)))
  {
    TBSYS_LOG(WARN, "wrong obj type for ob_read_consistency, err=%d", ret);
  }
  else
  {
    dest_param.set_is_read_consistency(read_consistency_val);
  }
  if (OB_SUCCESS == ret)
  {
    dest_param.set_is_result_cached(true);
    if (OB_SUCCESS != (ret = dest_param.set_table_id(table_id_, base_table_id_)))
    {
      TBSYS_LOG(WARN, "fail to set table id and scan range. ret=%d", ret);
    }
  }

  return ret;
}

int64_t ObRpcScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "RpcScan(");
  pos += read_param_->to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ")");
  return pos;
}

void ObRpcScan::set_hint(const common::ObRpcScanHint &hint)
{
  hint_ = hint;
  // max_parallel_count
  if (hint_.max_parallel_count <= 0)
  {
    hint_.max_parallel_count = 20;
  }
  // max_memory_limit
  if (hint_.max_memory_limit < 1024 * 1024 * 2)
  {
    hint_.max_memory_limit = 1024 * 1024 * 2;
  }
}

int ObRpcScan::open()
{
  int64_t start_open_rpc_scan = tbsys::CTimeUtil::getTime();
  int ret = OB_SUCCESS;
  timeout_us_ = hint_.timeout_us;

  OB_ASSERT(my_phy_plan_);
  if (hint_.only_frozen_version_data_)
  {
    ObVersion frozen_version = my_phy_plan_->get_curr_frozen_version();
    read_param_->set_data_version(frozen_version);
    FILL_TRACE_LOG("static_data_version=%s", to_cstring(frozen_version));
  }
  read_param_->set_is_only_static_data(hint_.only_static_data_);
  FILL_TRACE_LOG("only_static=%c", hint_.only_static_data_?'Y':'N');
  if (NULL == cache_proxy_ || NULL == async_rpc_)
  {
    ret = OB_NOT_INIT;
  }


  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "read_method_ [%s]", hint_.read_method_ == ObSqlReadStrategy::USE_SCAN ? "SCAN" : "GET");
    // common initialization
    cur_row_.set_row_desc(cur_row_desc_);
    FILL_TRACE_LOG("open %s", to_cstring(cur_row_desc_));
  }
  // Scan
  if (OB_SUCCESS == ret && hint_.read_method_ == ObSqlReadStrategy::USE_SCAN)
  {
    int64_t start_cons_scan = tbsys::CTimeUtil::getTime();
    if (OB_SUCCESS != (ret = sql_scan_request_.initialize()))
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
      else if (OB_SUCCESS != (ret = create_scan_param(*scan_param_)))
      {
        TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      sql_scan_request_.set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
      if(OB_SUCCESS != (ret = sql_scan_request_.set_request_param(*scan_param_, hint_)))
      {
        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
            hint_.max_parallel_count, ret);
      }
    }
    int64_t end_cons_scan = tbsys::CTimeUtil::getTime();
    PROFILE_LOG(DEBUG, CONS_SQL_SCAN_REQUEST, end_cons_scan - start_cons_scan);
  }
  // Get
  if (OB_SUCCESS == ret && hint_.read_method_ == ObSqlReadStrategy::USE_GET)
  {
    int64_t start_cons_get = tbsys::CTimeUtil::getTime();
    get_row_desc_.reset();
    sql_get_request_.alloc_request_id();
    if (OB_SUCCESS != (ret = sql_get_request_.init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_GET)))
    {
      TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = create_get_param(*get_param_)))
    {
      TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = cons_row_desc(*get_param_, get_row_desc_)))
    {
      TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = sql_get_request_.set_row_desc(get_row_desc_)))
    {
      TBSYS_LOG(WARN, "fail to set row desc:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = sql_get_request_.set_request_param(*get_param_, timeout_us_)))
    {
      TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
          hint_.max_parallel_count, ret);
    }
    int64_t end_cons_get = tbsys::CTimeUtil::getTime();
    PROFILE_LOG(DEBUG, CONS_SQL_GET_REQUEST, end_cons_get - start_cons_get);
    if (OB_SUCCESS == ret)
    {
      sql_get_request_.set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
      if (OB_SUCCESS != (ret = sql_get_request_.open()))
      {
        TBSYS_LOG(WARN, "fail to open get request. ret=%d", ret);
      }
    }
    int64_t end_open_sql_get_request = tbsys::CTimeUtil::getTime();
    PROFILE_LOG(DEBUG, OPEN_SQL_GET_REQUEST, end_open_sql_get_request - end_cons_get);
  }
  int64_t end_open_rpc_scan = tbsys::CTimeUtil::getTime();
  PROFILE_LOG(DEBUG, OPEN_RPC_SCAN, end_open_rpc_scan - start_open_rpc_scan);
  return ret;
}

void ObRpcScan::destroy()
{
  sql_read_strategy_.destroy();
  if (NULL != start_key_buf_)
  {
    ob_free(start_key_buf_, ObModIds::OB_SQL_RPC_SCAN);
    start_key_buf_ = NULL;
  }
  if (NULL != end_key_buf_)
  {
    ob_free(end_key_buf_, ObModIds::OB_SQL_RPC_SCAN);
    end_key_buf_ = NULL;
  }
  if (NULL != get_param_)
  {
    get_param_->~ObSqlGetParam();
    ob_free(get_param_);
    get_param_ = NULL;
  }
  if (NULL != scan_param_)
  {
    scan_param_->~ObSqlScanParam();
    ob_free(scan_param_);
    scan_param_ = NULL;
  }
}

int ObRpcScan::close()
{
  int ret = OB_SUCCESS;
  sql_scan_request_.close();
  sql_scan_request_.reset();
  if (NULL != scan_param_)
  {
    scan_param_->reset_local();
  }
  sql_get_request_.close();
  sql_get_request_.reset();
  if (NULL != get_param_)
  {
    get_param_->reset_local();
  }
  return ret;
}

int ObRpcScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(base_table_id_ <= 0 || 0 >= cur_row_desc_.get_column_num()))
  {
    TBSYS_LOG(ERROR, "not init, tid=%lu, column_num=%ld", base_table_id_, cur_row_desc_.get_column_num());
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
  if (ObSqlReadStrategy::USE_SCAN == hint_.read_method_)
  {
    ret = get_next_compact_row(row); // 可能需要等待CS返回
  }
  else if (ObSqlReadStrategy::USE_GET == hint_.read_method_)
  {
    ret = sql_get_request_.get_next_row(cur_row_);
  }
  else
  {
    TBSYS_LOG(WARN, "not init. read_method_=%d", hint_.read_method_);
    ret = OB_NOT_INIT;
  }
  row = &cur_row_;
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
      TBSYS_LOG(WARN, "Unexprected error. ret=%d, cur_row_desc[%s], read_method_[%d]", ret, to_cstring(cur_row_desc_), hint_.read_method_);
      can_break = true;
    }
  }while(false == can_break);
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
  //if (table_id_ <= 0 || table_id_ != expr.get_table_id())
  if (base_table_id_ <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "must call set_table() first. base_table_id_=%lu",
        base_table_id_);
  }
  else if ((OB_SUCCESS == (ret = expr.is_column_index_expr(is_cid)))  && (true == is_cid))
  {
    // 添加基本列
    if (OB_SUCCESS != (ret = read_param_->add_output_column(expr)))
    {
      TBSYS_LOG(WARN, "fail to add output column ret=%d", ret);
    }
  }
  else
  {
    // 添加复合列
    if (OB_SUCCESS != (ret = read_param_->add_output_column(expr)))
    {
      TBSYS_LOG(WARN, "fail to add output column ret=%d", ret);
    }
  }
  // cons row desc
  if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(expr.get_table_id(), expr.get_column_id()))))
  {
    TBSYS_LOG(WARN, "fail to add column to scan param. ret=%d, tid_=%lu, cid=%lu", ret, expr.get_table_id(), expr.get_column_id());
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
    table_id_ = table_id;
    base_table_id_ = base_table_id;
  }
  return ret;
}

int ObRpcScan::cons_get_rows(ObSqlGetParam &get_param)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  ObArray<ObRowkey> rowkey_array;
  // TODO lide.wd: rowkey obj storage needed. varchar use orginal buffer, will be copied later
  PageArena<ObObj,ModulePageAllocator> rowkey_objs_allocator(
      PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_COMMON));
  // try  'where (k1,k2,kn) in ((a,b,c), (e,f,g))'
  if (OB_SUCCESS != (ret = sql_read_strategy_.find_rowkeys_from_in_expr(rowkey_array, rowkey_objs_allocator)))
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
  else if (OB_SUCCESS != (ret = sql_read_strategy_.find_rowkeys_from_equal_expr(rowkey_array, rowkey_objs_allocator)))
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
}

int ObRpcScan::cons_scan_range(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  bool found = false;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  range.table_id_ = base_table_id_;
  OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
  // range 指向sql_read_strategy_的空间
  if (OB_SUCCESS != (ret = sql_read_strategy_.find_scan_range(range, found, false)))
  {
    TBSYS_LOG(WARN, "fail to find range %lu", base_table_id_);
  }
  return ret;
}

int ObRpcScan::get_min_max_rowkey(const ObArray<ObRowkey> &rowkey_array, ObObj *start_key_objs_, ObObj *end_key_objs_, int64_t rowkey_size)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  if (1 == rowkey_array.count())
  {
    const ObRowkey &rowkey = rowkey_array.at(0);
    for (i = 0; i < rowkey_size && i < rowkey.get_obj_cnt(); i++)
    {
      start_key_objs_[i] = rowkey.ptr()[i];
      end_key_objs_[i] = rowkey.ptr()[i];
    }
    for ( ; i < rowkey_size; i++)
    {
      start_key_objs_[i] = ObRowkey::MIN_OBJECT;
      end_key_objs_[i] = ObRowkey::MAX_OBJECT;
    }
  }
  else
  {
    TBSYS_LOG(WARN, "only support single insert row for scan optimization. rowkey_array.count=%ld", rowkey_array.count());
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObRpcScan::add_filter(ObSqlExpression* expr)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = sql_read_strategy_.add_filter(*expr)))
  {
    TBSYS_LOG(WARN, "fail to add filter to sql read strategy:ret[%d]", ret);
  }
  if (OB_SUCCESS == ret && OB_SUCCESS != (ret = read_param_->add_filter(expr)))
  {
    TBSYS_LOG(WARN, "fail to add composite column to scan param. ret=%d", ret);
  }
  return ret;
}

int ObRpcScan::add_group_column(const uint64_t tid, const uint64_t cid)
{
  return read_param_->add_group_column(tid, cid);
}

int ObRpcScan::add_aggr_column(const ObSqlExpression& expr)
{
  int ret = OB_SUCCESS;
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
  else if ((ret = read_param_->add_aggr_column(expr)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Failed to add aggregate column desc, err=%d", ret);
  }
  return ret;
}

int ObRpcScan::set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset)
{
  return read_param_->set_limit(limit, offset);
}

int ObRpcScan::cons_row_desc(const ObSqlGetParam &sql_get_param, ObRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  if ( !sql_get_param.has_project() )
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "should has project");
  }

  if (OB_SUCCESS == ret)
  {
    const common::ObArray<ObSqlExpression> &columns = sql_get_param.get_project().get_output_columns();
    for (int64_t i = 0; OB_SUCCESS == ret && i < columns.count(); i ++)
    {
      const ObSqlExpression &expr = columns.at(i);
      if (OB_SUCCESS != (ret = row_desc.add_column_desc(expr.get_table_id(), expr.get_column_id())))
      {
        TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if ( sql_get_param.get_row_size() <= 0 )
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "should has a least one row");
    }
    else
    {
      row_desc.set_rowkey_cell_count(sql_get_param[0]->length());
    }
  }

  return ret;
}
