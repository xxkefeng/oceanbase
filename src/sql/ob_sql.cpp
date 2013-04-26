/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "sql/ob_values.h"
#include "sql/ob_sql.h"
#include "sql/parse_node.h"
#include "sql/build_plan.h"
#include "sql/ob_transformer.h"
#include "sql/ob_schema_checker.h"
#include "sql/parse_malloc.h"
#include "sql/ob_select_stmt.h"
#include "sql/ob_update_stmt.h"
#include "sql/ob_delete_stmt.h"
#include "common/ob_privilege.h"
#include "common/ob_array.h"
#include "common/ob_privilege_type.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_type.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_trace_id.h"
#include "common/ob_encrypted_helper.h"
#include "sql/ob_priv_executor.h"
#include "sql/ob_grant_stmt.h"
#include "sql/ob_create_user_stmt.h"
#include "sql/ob_drop_user_stmt.h"
#include "sql/ob_drop_table_stmt.h"
#include "sql/ob_revoke_stmt.h"
#include "sql/ob_lock_user_stmt.h"
#include "sql/ob_set_password_stmt.h"
#include "sql/ob_rename_user_stmt.h"
#include "sql/ob_show_stmt.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

template <typename OperatorT>
static int init_hook_env(ObSqlContext &context, ObPhysicalPlan *&phy_plan, OperatorT *&op)
{
  int ret = OB_SUCCESS;
  phy_plan = NULL;
  op = NULL;
  StackAllocator& allocator = context.session_info_->get_transformer_mem_pool();
  void *ptr1 = allocator.alloc(sizeof(ObPhysicalPlan));
  void *ptr2 = allocator.alloc(sizeof(OperatorT));
  if (NULL == ptr1 || NULL == ptr2)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "fail to malloc memory for ObValues, op=%p", op);
  }
  else
  {
    op = new(ptr2) OperatorT();
    phy_plan = new(ptr1) ObPhysicalPlan();
    if (OB_SUCCESS != (ret = phy_plan->store_phy_operator(op)))
    {
      TBSYS_LOG(WARN, "failed to add operator, err=%d", ret);
      op->~OperatorT();
      phy_plan->~ObPhysicalPlan();
    }
    else if (OB_SUCCESS != (ret = phy_plan->add_phy_query(op, NULL, true)))
    {
      TBSYS_LOG(WARN, "failed to add query, err=%d", ret);
      phy_plan->~ObPhysicalPlan();
    }
  }
  return ret;
}

int ObSql::direct_execute(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  result.set_session(context.session_info_);
  if (NULL != context.session_info_)
  {
    context.session_info_->set_version_provider(context.merge_service_);
  }
  // Step special: process some special statment here, like "show warning" etc
  if (OB_UNLIKELY(no_enough_memory()))
  {
    TBSYS_LOG(WARN, "no memory");
    result.set_message("no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (true == process_special_stmt_hook(stmt, result, context))
  {
    if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_TRACE))
    {
      TBSYS_LOG(TRACE, "execute special sql statement success [%.*s]", stmt.length(), stmt.ptr());
    }
  }
  else
  {
    ResultPlan result_plan;
    ObMultiPhyPlan multi_phy_plan;
    ObMultiLogicPlan *multi_logic_plan = NULL;
    ObLogicalPlan *logic_plan = NULL;
    result_plan.is_prepare_ = context.is_prepare_protocol_ ? 1 : 0;
    ret = generate_logical_plan(stmt, context, result_plan, result);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "generate logical plan failed, ret=%d sql=%.*s", ret, stmt.length(), stmt.ptr());
    }
    else
    {
      TBSYS_LOG(DEBUG, "generate logical plan succ");
      multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
      logic_plan = multi_logic_plan->at(0);
      if (!context.disable_privilege_check_)
      {
        ret = do_privilege_check(context.session_info_->get_user_name(), context.pp_privilege_, logic_plan);
        if (OB_SUCCESS != ret)
        {
          result.set_message("no privilege");
          TBSYS_LOG(WARN, "no privilege,sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ObBasicStmt::StmtType stmt_type = logic_plan->get_main_stmt()->get_stmt_type();
        result.set_stmt_type(stmt_type);
        result.set_inner_stmt_type(stmt_type);
        if (OB_SUCCESS != (ret = logic_plan->fill_result_set(result, context.session_info_, context.session_info_->get_transformer_mem_pool())))
        {
          TBSYS_LOG(WARN, "fill result set failed,ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = generate_physical_plan(context, result_plan, multi_phy_plan, result)))
        {
          TBSYS_LOG(WARN, "generate physical plan failed, ret=%d", ret);
        }
        else
        {
          clean_result_plan(result_plan);
          TBSYS_LOG(DEBUG, "generete physical plan success, sql=%.*s", stmt.length(), stmt.ptr());
          //TBSYS_LOG(INFO, "[zhuweng] after_gen_phy fields=%s", to_cstring(result.get_field_columns()));
          // SHOULD have only one physical plan now
          ObPhysicalPlan *phy_plan = multi_phy_plan.at(0);
          OB_ASSERT(phy_plan);
          if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_TRACE))
          {
            TBSYS_LOG(TRACE, "ExecutionPlan: \n%s", to_cstring(*phy_plan));
          }
          result.set_physical_plan(phy_plan, true);
          multi_phy_plan.clear();

        }
      }
    }
  }
  result.set_errcode(ret);
  return ret;
}

int ObSql::generate_logical_plan(const common::ObString &stmt, ObSqlContext & context, ResultPlan  &result_plan, ObResultSet & result)
{
  int ret = OB_SUCCESS;
  common::ObStringBuf &parser_mem_pool = context.session_info_->get_parser_mem_pool();
  ParseResult parse_result;
  static const int MAX_ERROR_LENGTH = 80;

  parse_result.malloc_pool_ = &parser_mem_pool;
  if (0 != (ret = parse_init(&parse_result)))
  {
    TBSYS_LOG(WARN, "parser init err, err=%s", strerror(errno));
    ret = OB_ERR_PARSER_INIT;
  }
  else
  {
    // generate syntax tree
    int64_t st = tbsys::CTimeUtil::getTime();
    FILL_TRACE_LOG("before_parse");
    if (parse_sql(&parse_result, stmt.ptr(), static_cast<size_t>(stmt.length())) != 0
      || NULL == parse_result.result_tree_)
    {
      TBSYS_LOG(WARN, "parse: %p, %p, %p, msg=[%s], start_col_=[%d], end_col_[%d], line_[%d], yycolumn[%d], yylineno_[%d]",
          parse_result.yyscan_info_,
          parse_result.result_tree_,
          parse_result.malloc_pool_,
          parse_result.error_msg_,
          parse_result.start_col_,
          parse_result.end_col_,
          parse_result.line_,
          parse_result.yycolumn_,
          parse_result.yylineno_);

      int64_t error_length = min(stmt.length() - (parse_result.start_col_ - 1), MAX_ERROR_LENGTH);
      snprintf(parse_result.error_msg_, MAX_ERROR_MSG,
          "You have an error in your SQL syntax; check the manual that corresponds to your OceanBase version for the right syntax to use near '%.*s' at line %d", static_cast<int32_t>(error_length), stmt.ptr() + parse_result.start_col_ - 1, parse_result.line_);
      TBSYS_LOG(WARN, "failed to parse sql=%.*s err=%s", stmt.length(), stmt.ptr(), parse_result.error_msg_);
      result.set_message(parse_result.error_msg_);
      ret = OB_ERR_PARSE_SQL;
    }
    else if (NULL == context.schema_manager_)
    {
      TBSYS_LOG(WARN, "context.schema_manager_ is null");
      ret = OB_ERR_UNEXPECTED;
    }
    else
    {
      FILL_TRACE_LOG("parse");
      result_plan.name_pool_ = &parser_mem_pool;
      ObSchemaChecker *schema_checker =  (ObSchemaChecker*)parse_malloc(sizeof(ObSchemaChecker), result_plan.name_pool_);
      if (NULL == schema_checker)
      {
        TBSYS_LOG(WARN, "out of memory");
        ret = OB_ERR_PARSER_MALLOC_FAILED;
      }
      else
      {
        schema_checker->set_schema(*context.schema_manager_);
        result_plan.schema_checker_ = schema_checker;
        result_plan.plan_tree_ = NULL;
        // generate logical plan
        ret = resolve(&result_plan, parse_result.result_tree_);
	FILL_TRACE_LOG("resolve");
        int64_t ed = tbsys::CTimeUtil::getTime();
        PROFILE_LOG(DEBUG, SQL_TO_LOGICALPLAN_TIME_US, ed - st);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to generate logical plan, err=%d sql=%.*s result_plan.err_stat_.err_msg_=[%s]",
              ret, stmt.length(), stmt.ptr(), result_plan.err_stat_.err_msg_);
          result.set_message(result_plan.err_stat_.err_msg_);
        }
        else
        {
          ObMultiLogicPlan *multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
          if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG))
          {
            multi_logic_plan->print();
          }
        }
      }
    }
  }
  // destroy syntax tree
  destroy_tree(parse_result.result_tree_);
  parse_terminate(&parse_result);
  result.set_errcode(ret);
  return ret;
}

void ObSql::clean_result_plan(ResultPlan &result_plan)
{
  ObMultiLogicPlan *multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
  multi_logic_plan->~ObMultiLogicPlan();
  destroy_plan(&result_plan);
}

int ObSql::generate_physical_plan(ObSqlContext & context, ResultPlan &result_plan, ObMultiPhyPlan & multi_phy_plan, ObResultSet & result)
{
  int ret = OB_SUCCESS;
  if (NULL == context.transformer_allocator_)
  {
    OB_ASSERT(!context.is_prepare_protocol_);
    context.transformer_allocator_ = &context.session_info_->get_transformer_mem_pool();
  }

  ObTransformer trans(context);
  ErrStat err_stat;
  ObMultiLogicPlan *multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
  int64_t st = tbsys::CTimeUtil::getTime();
  if (OB_SUCCESS != (ret = trans.generate_physical_plans(*multi_logic_plan, multi_phy_plan, err_stat)))
  {
    TBSYS_LOG(WARN, "failed to transform to physical plan");
    result.set_message(err_stat.err_msg_);
    ret = OB_ERR_GEN_PLAN;
  }
  else
  {
    int64_t ed = tbsys::CTimeUtil::getTime();
    PROFILE_LOG(DEBUG, LOGICALPLAN_TO_PHYSICALPLAN_TIME_US, ed - st);
    for (int32_t i = 0; i < multi_phy_plan.size(); ++i)
    {
      multi_phy_plan.at(i)->set_result_set(&result);
    }
  }
  return ret;
}

int ObSql::stmt_prepare(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  result.set_session(context.session_info_);
  if (NULL != context.session_info_)
  {
    context.session_info_->set_version_provider(context.merge_service_);
  }
  ObString stmt_name;
  ObArenaAllocator *allocator = NULL;
  context.is_prepare_protocol_ = true;
  if (OB_UNLIKELY(no_enough_memory()))
  {
    TBSYS_LOG(WARN, "no memory");
    result.set_message("no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (NULL == context.session_info_)
  {
    TBSYS_LOG(WARN, "context.session_info_(null)");
    ret = OB_NOT_INIT;
  }
  else if (NULL == (allocator = context.session_info_->get_transformer_mem_pool_for_ps()))
  {
    TBSYS_LOG(WARN, "failed to get new allocator");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    OB_ASSERT(NULL == context.transformer_allocator_);
    context.transformer_allocator_ = allocator;
    // the transformer's allocator is owned by the result set now, and will be free by the result set
    result.set_ps_transformer_allocator(allocator);

    if (OB_SUCCESS != (ret = direct_execute(stmt, result, context)))
    {
      TBSYS_LOG(WARN, "direct execute failed");
    }
    else if (OB_SUCCESS != (ret = context.session_info_->store_plan(stmt_name, result)))
    {
      TBSYS_LOG(WARN, "Store result set to session failed");
    }
  }
  //store errcode in resultset
  result.set_stmt_type(ObBasicStmt::T_PREPARE);
  result.set_errcode(ret);
  return ret;
}

int ObSql::stmt_execute(const uint64_t stmt_id,
                        const common::ObArray<obmysql::EMySQLFieldType> &params_type,
                        const common::ObArray<common::ObObj> &params,
                        ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  result.set_session(context.session_info_);
  if (NULL != context.session_info_)
  {
    context.session_info_->set_version_provider(context.merge_service_);
  }
  ObResultSet *stored_result = NULL;
  if (OB_UNLIKELY(no_enough_memory()))
  {
    TBSYS_LOG(WARN, "no memory");
    result.set_message("no memory");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (NULL == context.session_info_)
  {
    TBSYS_LOG(WARN, "context.session_info_(null)");
    ret = OB_NOT_INIT;
  }
  else if(NULL == (stored_result = context.session_info_->get_plan(stmt_id)))
  {
    ret = OB_ERR_PREPARE_STMT_UNKNOWN;
    TBSYS_LOG(USER_ERROR, "statement not prepared, stmt_id=%lu", stmt_id);
  }
  // set running param values
  else if (OB_SUCCESS != (ret = stored_result->fill_params(params_type, params)))
  {
    TBSYS_LOG(WARN, "Incorrect arguments to EXECUTE");
  }
  else if (OB_SUCCESS != result.from_prepared(*stored_result))
  {
    TBSYS_LOG(ERROR, "Fill result set failed");
  }
  else
  {
    if (OB_UNLIKELY(TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_TRACE))
    {
      TBSYS_LOG(TRACE, "ExecutionPlan: \n%s", to_cstring(*stored_result->get_physical_plan()->get_main_query()));
    }
  }
  //store errcode in resultset
  result.set_errcode(ret);
  return ret;
}

int ObSql::stmt_close(const uint64_t stmt_id, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  if (NULL == context.session_info_)
  {
    TBSYS_LOG(WARN, "context.session_info_(null)");
    ret = OB_NOT_INIT;
  }
  else if(OB_SUCCESS != (ret = context.session_info_->remove_plan(stmt_id)))
  {
    TBSYS_LOG(WARN, "remove prepared statement failed, stmt_id=%lu", stmt_id);
  }
  return ret;
}

bool ObSql::process_special_stmt_hook(const common::ObString &stmt, ObResultSet &result, ObSqlContext &context)
{
  int ret = OB_SUCCESS;
  const char *select_collation = (const char *)"SHOW COLLATION";
  int64_t select_collation_len = strlen(select_collation);
  const char *show_charset = (const char *)"SHOW CHARACTER SET";
  int64_t show_charset_len = strlen(show_charset);
  // SET NAMES latin1/gb2312/utf8/etc...
  const char *set_names = (const char *)"SET NAMES ";
  int64_t set_names_len = strlen(set_names);
  const char *set_session_transaction_isolation = (const char *)"SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED";
  int64_t set_session_transaction_isolation_len = strlen(set_session_transaction_isolation);

  ObRow row;
  ObRowDesc row_desc;
  ObValues *op = NULL;
  ObPhysicalPlan *phy_plan = NULL;

  if (stmt.length() >= select_collation_len && 0 == strncasecmp(stmt.ptr(), select_collation, select_collation_len))
  {
    /*
       mysql> SHOW COLLATION;
        +------------------------+----------+-----+---------+----------+---------+
        | Collation              | Charset  | Id  | Default | Compiled | Sortlen |
        +------------------------+----------+-----+---------+----------+---------+
        | big5_chinese_ci    | big5       |   1 | Yes       | Yes         |       1 |
        | ...                        | ...          |  ... |   ...      | ...            |       ... |
       +------------------------+----------+-----+---------+----------+---------+
       */
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {

      // construct table header
      ObResultSet::Field field;
      ObString tname = ObString::make_string("tmp_table");
      field.tname_ = tname;
      field.org_tname_ = tname;
      ObString cname[6];
      cname[0] = ObString::make_string("Collation");
      cname[1] = ObString::make_string("Charset");
      cname[2] = ObString::make_string("Id");
      cname[3] = ObString::make_string("Default");
      cname[4] = ObString::make_string("Compiled");
      cname[5] = ObString::make_string("Sortlen");
      ObObjType type[6];
      type[0] = ObVarcharType;
      type[1] = ObVarcharType;
      type[2] = ObIntType;
      type[3] = ObVarcharType;
      type[4] = ObVarcharType;
      type[5] = ObIntType;
      for (int i = 0; i < 6; i++)
      {
        field.cname_ = cname[i];
        field.org_cname_ = cname[i];
        field.type_.set_type(type[i]);
        if (OB_SUCCESS != (ret = result.add_field_column(field)))
        {
          TBSYS_LOG(WARN, "fail to add field column %d", i);
          break;
        }
      }

      // construct table body
      for (int i = 0; i < 6; ++i)
      {
        ret = row_desc.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      row.set_row_desc(row_desc);
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
      // | binary               | binary   |  63 | Yes     | Yes      |       1 |
      ObObj cells[6];
      ObString cell0 = ObString::make_string("binary");
      cells[0].set_varchar(cell0);
      ObString cell1 = ObString::make_string("binary");
      cells[1].set_varchar(cell1);
      cells[2].set_int(63);
      ObString cell3 = ObString::make_string("Yes");
      cells[3].set_varchar(cell3);
      ObString cell4 = ObString::make_string("Yes");
      cells[4].set_varchar(cell4);
      cells[5].set_int(1);
      ObRow one_row;
      one_row.set_row_desc(row_desc);
      for (int i = 0; i < 6; ++i)
      {
        ret = one_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i, cells[i]);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      ret = op->add_values(one_row);
      OB_ASSERT(OB_SUCCESS == ret);
    }
  }
  else if (stmt.length() >= show_charset_len && 0 == strncasecmp(stmt.ptr(), show_charset, show_charset_len))
  {
    /*
       mysql> SHOW CHARACTER SET
       +----------+-----------------------------+---------------------+--------+
       | Charset  | Description                 | Default collation   | Maxlen |
       +----------+-----------------------------+---------------------+--------+
       | binary   | Binary pseudo charset       | binary              |      1 |
       +----------+-----------------------------+---------------------+--------+
    */
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {

      // construct table header
      ObResultSet::Field field;
      ObString tname = ObString::make_string("tmp_table");
      field.tname_ = tname;
      field.org_tname_ = tname;
      ObString cname[4];
      cname[0] = ObString::make_string("Charset");
      cname[1] = ObString::make_string("Description");
      cname[2] = ObString::make_string("Default collation");
      cname[3] = ObString::make_string("Maxlen");
      ObObjType type[4];
      type[0] = ObVarcharType;
      type[1] = ObVarcharType;
      type[2] = ObVarcharType;
      type[3] = ObIntType;
      for (int i = 0; i < 4; i++)
      {
        field.cname_ = cname[i];
        field.org_cname_ = cname[i];
        field.type_.set_type(type[i]);
        if (OB_SUCCESS != (ret = result.add_field_column(field)))
        {
          TBSYS_LOG(WARN, "fail to add field column %d", i);
          break;
        }
      }

      // construct table body
      for (int i = 0; i < 4; ++i)
      {
        ret = row_desc.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      row.set_row_desc(row_desc);
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
      // | binary   | Binary pseudo charset       | binary              |      1 |
      ObObj cells[4];
      ObString cell0 = ObString::make_string("binary");
      cells[0].set_varchar(cell0);
      ObString cell1 = ObString::make_string("Binary pseudo charset");
      cells[1].set_varchar(cell1);
      ObString cell2 = ObString::make_string("binary");
      cells[2].set_varchar(cell2);
      cells[3].set_int(1);
      ObRow one_row;
      one_row.set_row_desc(row_desc);
      for (int i = 0; i < 4; ++i)
      {
        ret = one_row.set_cell(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID+i, cells[i]);
        OB_ASSERT(OB_SUCCESS == ret);
      }
      ret = op->add_values(one_row);
      OB_ASSERT(OB_SUCCESS == ret);
    }
  }
  else if (stmt.length() >= set_names_len && 0 == strncasecmp(stmt.ptr(), set_names, set_names_len))
  {
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {
      // SET NAMES ...
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
    }
  }
  else if (stmt.length() >= set_session_transaction_isolation_len &&
      0 == strncasecmp(stmt.ptr(), set_session_transaction_isolation, set_session_transaction_isolation_len))
  {
    if (OB_SUCCESS != init_hook_env(context, phy_plan, op))
    {
    }
    else
    {
      // SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;
      OB_ASSERT(NULL != op);
      op->set_row_desc(row_desc);
    }
  }
  else
  {
    ret = OB_NOT_SUPPORTED;
  }

  if (OB_SUCCESS != ret)
  {
    if (NULL != phy_plan)
    {
      phy_plan->~ObPhysicalPlan(); // will destruct op automatically
    }
  }
  else
  {
    result.set_physical_plan(phy_plan, true);
  }
  return (OB_SUCCESS == ret);
}

int ObSql::do_privilege_check(const ObString & username, const ObPrivilege **pp_privilege, ObLogicalPlan *plan)
{
  int err = OB_SUCCESS;
  ObBasicStmt *stmt = NULL;
  ObArray<ObPrivilege::TablePrivilege> table_privileges;
  for (int32_t i = 0;i < plan->get_stmts_count(); ++i)
  {
    stmt = plan->get_stmt(i);
    switch (stmt->get_stmt_type())
    {
      case ObBasicStmt::T_SELECT:
        {
          ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt*>(stmt);
          if (OB_UNLIKELY(NULL == select_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObSelectStmt::SetOperator set_op  = select_stmt->get_set_op();
            if (set_op != ObSelectStmt::NONE)
            {
              continue;
            }
            else
            {
              int32_t table_item_size = select_stmt->get_table_size();
              for (int32_t j = 0; j < table_item_size; ++j)
              {
                ObPrivilege::TablePrivilege table_privilege;
                const TableItem &table_item = select_stmt->get_table_item(j);
                uint64_t table_id = OB_INVALID_ID;
                if (table_item.type_ == TableItem::BASE_TABLE || table_item.type_ == TableItem::ALIAS_TABLE)
                {
                  table_id = table_item.ref_id_;
                }
                else
                {
                  continue;
                }
                table_privilege.table_id_ = table_id;
                OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_SELECT));
                err = table_privileges.push_back(table_privilege);
                if (OB_UNLIKELY(OB_SUCCESS != err))
                {
                  TBSYS_LOG(WARN, "push table_privilege to array failed,err=%d", err);
                }
              }
            }
          }
          break;
        }
      case ObBasicStmt::T_INSERT:
      case ObBasicStmt::T_REPLACE:
        {
          ObInsertStmt *insert_stmt = dynamic_cast<ObInsertStmt*>(stmt);
          if (OB_UNLIKELY(NULL == insert_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            table_privilege.table_id_ = insert_stmt->get_table_id();
            if (!insert_stmt->is_replace())
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_INSERT));
            }
            else
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_REPLACE));
            }
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_UPDATE:
        {
          ObUpdateStmt *update_stmt = dynamic_cast<ObUpdateStmt*>(stmt);
          if (OB_UNLIKELY(NULL == update_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            table_privilege.table_id_ = update_stmt->get_update_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_DELETE:
        {
          ObDeleteStmt *delete_stmt = dynamic_cast<ObDeleteStmt*>(stmt);
          if (OB_UNLIKELY(NULL == delete_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            table_privilege.table_id_ = delete_stmt->get_delete_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_DELETE));
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_GRANT:
        {
          OB_STAT_INC(SQL, SQL_GRANT_PRIVILEGE_COUNT);

          ObGrantStmt *grant_stmt = dynamic_cast<ObGrantStmt*>(stmt);
          if (OB_UNLIKELY(NULL == grant_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            // if grant priv_xx* on * then table_id == 0
            // if grant priv_xx* on table_name
            table_privilege.table_id_ = grant_stmt->get_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_GRANT_OPTION));
            const common::ObArray<ObPrivilegeType> *privileges = grant_stmt->get_privileges();
            int i = 0;
            for (i = 0;i < privileges->count();++i)
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(privileges->at(i)));
            }
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_REVOKE:
        {
          OB_STAT_INC(SQL, SQL_REVOKE_PRIVILEGE_COUNT);

          ObRevokeStmt *revoke_stmt = dynamic_cast<ObRevokeStmt*>(stmt);
          if (OB_UNLIKELY(NULL == revoke_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            ObPrivilege::TablePrivilege table_privilege;
            // if revoke priv_xx* on * from user, then table_id == 0
            // elif revoke priv_xx* on table_name from user, then table_id != 0 && table_id != OB_INVALID_ID
            // elif revoke ALL PRIVILEGES, GRANT OPTION from user, then table_id == OB_INVALID_ID
            table_privilege.table_id_ = revoke_stmt->get_table_id();
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_GRANT_OPTION));
            const common::ObArray<ObPrivilegeType> *privileges = revoke_stmt->get_privileges();
            int i = 0;
            for (i = 0;i < privileges->count();++i)
            {
              OB_ASSERT(true == table_privilege.privileges_.add_member(privileges->at(i)));
            }
            err = table_privileges.push_back(table_privilege);
            if (OB_UNLIKELY(OB_SUCCESS != err))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          break;
        }
      case ObBasicStmt::T_CREATE_USER:
      case ObBasicStmt::T_DROP_USER:
        {
          ObPrivilege::TablePrivilege table_privilege;
          if (ObBasicStmt::T_CREATE_USER == stmt->get_stmt_type())
            OB_STAT_INC(SQL, SQL_CREATE_USER_COUNT);
          else if (ObBasicStmt::T_DROP_USER == stmt->get_stmt_type())
            OB_STAT_INC(SQL, SQL_DROP_USER_COUNT);

          // create user是全局权限
          table_privilege.table_id_ = OB_NOT_EXIST_TABLE_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_CREATE_USER));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      case ObBasicStmt::T_LOCK_USER:
        {
          ObPrivilege::TablePrivilege table_privilege;
          OB_STAT_INC(SQL, SQL_LOCK_USER_COUNT);

          table_privilege.table_id_ = OB_USERS_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      case ObBasicStmt::T_SET_PASSWORD:
        {
          OB_STAT_INC(SQL, SQL_SET_PASSWORD_COUNT);

          ObSetPasswordStmt *set_pass_stmt = dynamic_cast<ObSetPasswordStmt*>(stmt);
          if (OB_UNLIKELY(NULL == set_pass_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            const common::ObStrings* user_pass = set_pass_stmt->get_user_password();
            ObString user;
            err  = user_pass->get_string(0,user);
            OB_ASSERT(OB_SUCCESS == err);
            if (user.length() == 0)
            {
              TBSYS_LOG(DEBUG, "EMPTY");
              // do nothing
            }
            else
            {
              ObPrivilege::TablePrivilege table_privilege;
              table_privilege.table_id_ = OB_USERS_TID;
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
              err = table_privileges.push_back(table_privilege);
              if (OB_UNLIKELY(OB_SUCCESS != err))
              {
                TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
              }
            }
          }
          break;
        }
      case  ObBasicStmt::T_RENAME_USER:
        {
          ObPrivilege::TablePrivilege table_privilege;

          OB_STAT_INC(SQL, SQL_RENAME_USER_COUNT);
          table_privilege.table_id_ = OB_USERS_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_UPDATE));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      case ObBasicStmt::T_CREATE_TABLE:
        {
          ObPrivilege::TablePrivilege table_privilege;
          OB_STAT_INC(SQL, SQL_CREATE_TABLE_COUNT);
          // create table 是全局权限
          table_privilege.table_id_ = OB_NOT_EXIST_TABLE_TID;
          OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_CREATE));
          err = table_privileges.push_back(table_privilege);
          if (OB_UNLIKELY(OB_SUCCESS != err))
          {
            TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
          }
          break;
        }
      case ObBasicStmt::T_DROP_TABLE:
        {
          OB_STAT_INC(SQL, SQL_DROP_TABLE_COUNT);
          ObDropTableStmt *drop_table_stmt = dynamic_cast<ObDropTableStmt*>(stmt);
          if (OB_UNLIKELY(NULL == drop_table_stmt))
          {
            err = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "dynamic cast failed,err=%d", err);
          }
          else
          {
            int64_t i = 0;
            for (i = 0;i < drop_table_stmt->get_table_count();++i)
            {
              ObPrivilege::TablePrivilege table_privilege;
              // drop table 不是全局权限
              table_privilege.table_id_ = drop_table_stmt->get_table_id(i);
              OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_DROP));
              err = table_privileges.push_back(table_privilege);
              if (OB_UNLIKELY(OB_SUCCESS != err))
              {
                TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
              }
            }
          }
          break;
        }
      case ObBasicStmt::T_SHOW_GRANTS:
        {
          ObShowStmt *show_grant_stmt = dynamic_cast<ObShowStmt*>(stmt);
          ObString user_name = show_grant_stmt->get_user_name();
          ObPrivilege::TablePrivilege table_privilege;
          ObPrivilege::TablePrivilege table_privilege2;
          OB_STAT_INC(SQL, SQL_SHOW_GRANTS_COUNT);
          // show grants for user
          if (user_name.length() > 0)
          {
            table_privilege.table_id_ = OB_USERS_TID;
            OB_ASSERT(true == table_privilege.privileges_.add_member(OB_PRIV_SELECT));
            table_privilege2.table_id_ = OB_TABLE_PRIVILEGES_TID;
            OB_ASSERT(true == table_privilege2.privileges_.add_member(OB_PRIV_SELECT));
            if (OB_SUCCESS != (err = table_privileges.push_back(table_privilege)))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
            else if (OB_SUCCESS != (err = table_privileges.push_back(table_privilege2)))
            {
              TBSYS_LOG(WARN, "push table_privilege to array failed, err=%d", err);
            }
          }
          else
          {
            // show grants 当前用户,不需要权限
          }
          break;
        }
      default:
        err = OB_ERR_NO_PRIVILEGE;
        break;
    }
  }
  err = (*pp_privilege)->has_needed_privileges(username, table_privileges);
  if (OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN, "username %.*s don't have enough privilege,err=%d", username.length(), username.ptr(), err);
  }
  else
  {
    TBSYS_LOG(DEBUG, "%.*s do privilege check success", username.length(), username.ptr());
  }
  return err;
}

bool ObSql::no_enough_memory()
{
  static const int64_t reserved_mem_size = 512*1024*1024LL; // 512MB
  bool ret = (ob_get_memory_size_limit() > reserved_mem_size)
    && (ob_get_memory_size_limit() - ob_get_memory_size_handled() < reserved_mem_size);
  if (OB_UNLIKELY(ret))
  {
    TBSYS_LOG(WARN, "not enough memory, limit=%ld used=%ld",
              ob_get_memory_size_limit(), ob_get_memory_size_handled());
  }
  return ret;
}
