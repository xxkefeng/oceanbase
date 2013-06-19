/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_result_set.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_result_set.h"
#include "sql/ob_physical_plan.h"
#include "sql/ob_multi_logic_plan.h"
#include "parse_malloc.h"
#include "ob_sql_session_info.h"
#include "common/ob_trace_log.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObResultSet::~ObResultSet()
{
  if (own_physical_plan_ && NULL != physical_plan_)
  {
    TBSYS_LOG(DEBUG, "destruct physical plan, addr=%p", physical_plan_);
    physical_plan_->~ObPhysicalPlan();
    physical_plan_ = NULL;
  }
  if (NULL != ps_trans_allocator_)
  {
    OB_ASSERT(my_session_);
    my_session_->free_transformer_mem_pool_for_ps(ps_trans_allocator_);
    ps_trans_allocator_ = NULL;
  }
  for (int64_t i = 0; i < params_.count(); i++)
  {
    ObObj *value = params_.at(i);
    value->~ObObj();
    //parse_free(value);
  }
  params_.clear();
  params_type_.clear();
}

int ObResultSet::init()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObResultSet::open()
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(NULL == physical_plan_))
  {
    if (ObBasicStmt::T_PREPARE != stmt_type_)
    {
      TBSYS_LOG(WARN, "physical_plan not init, stmt_type=%d", stmt_type_);
      ret = common::OB_NOT_INIT;
    }
  }
  else
  {
    // get current frozen version
    OB_ASSERT(my_session_);
    FILL_TRACE_LOG("curr_frozen_version=%s", to_cstring(my_session_->get_frozen_version()));
    physical_plan_->set_curr_frozen_version(my_session_->get_frozen_version());
    ObPhyOperator *rt = physical_plan_->get_main_query();
    ret = rt->open();
  }
  set_errcode(ret);
  return ret;
}


int ObResultSet::reset()
{
  int ret = OB_SUCCESS;

  if (own_physical_plan_ && NULL != physical_plan_)
  {
    TBSYS_LOG(DEBUG, "destruct physical plan, addr=%p", physical_plan_);
    physical_plan_->~ObPhysicalPlan();
    physical_plan_ = NULL;
  }
  statement_id_ = OB_INVALID_ID;
  affected_rows_ = 0;
  statement_name_.reset();
  warning_count_ = 0;
  message_[0] = '\0';
  field_columns_.clear();
  param_columns_.clear();
  for (int64_t i = 0; i < params_.count(); i++)
  {
    ObObj *value = params_.at(i);
    value->~ObObj();
    //parse_free(value);
  }
  params_.clear();
  params_type_.clear();
  errcode_ = 0;
  // don't set stmt_type_ = ObBasicStmt::T_NONE;
  return ret;
}

void ObResultSet::set_statement_name(const common::ObString name)
{
  statement_name_ = name;
}

int ObResultSet::pre_assign_params_room(const int64_t& size, common::StackAllocator &alloc)
{
  int ret = OB_SUCCESS;
  ObObj *place_holder = NULL;
  Field param_field;
  param_field.type_.set_type(ObIntType); // @bug
  for (int64_t i = 0; i < size; i++)
  {
    if (NULL == (place_holder = (ObObj*)alloc.alloc(sizeof(ObObj))))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      break;
    }
    else
    {
      place_holder = new(place_holder) ObObj();
    }
    if (OB_SUCCESS != (ret = params_.push_back(place_holder)))
    {
      break;
    }
    else if (OB_SUCCESS != (ret = param_columns_.push_back(param_field)))
    {
      break;
    }
  }
  return ret;
}

int ObResultSet::get_param_idx(const common::ObObj *param_addr, int64_t &idx)
{
  int ret = OB_ENTRY_NOT_EXIST;
  for (int64_t i = 0; i < params_.count(); ++i)
  {
    if (param_addr == params_.at(i))
    {
      ret = OB_SUCCESS;
      idx = i;
      break;
    }
  }
  return ret;
}

int ObResultSet::fill_params(const common::ObArray<obmysql::EMySQLFieldType>& types,
                             const common::ObArray<common::ObObj>& values)
{
  int ret = OB_SUCCESS;
  if (values.count() != params_.count())
  {
    ret = OB_ERR_WRONG_DYNAMIC_PARAM;
    TBSYS_LOG(USER_ERROR, "Incorrect arguments number to EXECUTE, need %ld arguments but give %ld",
              params_.count(), values.count());
  }
  else
  {
    if (0 < types.count())
    {
      // bound types
      params_type_ = types;
    }
    TBSYS_LOG(DEBUG, "execute with params=%s", to_cstring(values));
    for (int64_t i = 0; ret == OB_SUCCESS && i < params_.count(); i++)
    {
      *params_.at(i) = values.at(i); // shallow copy
    }
  }
  return ret;
}

int ObResultSet::from_prepared(const ObResultSet& stored_result_set)
{
  int ret = OB_SUCCESS;
  statement_id_ = stored_result_set.statement_id_;
  affected_rows_ = 0;

  physical_plan_ = stored_result_set.physical_plan_;
  own_physical_plan_ = false;

  field_columns_ = stored_result_set.field_columns_;
  param_columns_ = stored_result_set.param_columns_;
  params_ = stored_result_set.params_;
  ps_trans_allocator_ = NULL;
  // Outer statement has itsown stmt_type_, it needs the plan only not the stmt_type_
  // If outer resultset with stmt_type_ = T_EXECUTE, changing to T_PREPARE will arise error
  // stmt_type_ = stored_result_set.stmt_type_;
  inner_stmt_type_ = stored_result_set.stmt_type_;
  return ret;
}

int ObResultSet::to_prepare(ObResultSet& other)
{
  int ret = OB_SUCCESS;
  other.statement_id_ = statement_id_;
  other.affected_rows_ = 0;
  other.warning_count_ = 0;
  other.statement_name_ = statement_name_;
  other.message_[0] = '\0';
  other.field_columns_ = field_columns_;
  other.param_columns_ = param_columns_;
  other.params_ = params_;
  other.physical_plan_ = physical_plan_;
  other.physical_plan_->set_result_set(&other); // the new ownership
  other.own_physical_plan_ = true;
  other.stmt_type_ = stmt_type_;
  other.errcode_ = OB_SUCCESS;
  other.my_session_ = my_session_;
  other.ps_trans_allocator_ = ps_trans_allocator_;

  this->statement_name_.reset();
  this->physical_plan_ = NULL;
  this->own_physical_plan_ = false;
  this->ps_trans_allocator_ = NULL;
  // keep params_, field_columns_, param_columns_ etc.
  return ret;
}
