/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_create_index_stmt.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_create_index_stmt.h"
#include "ob_schema_checker.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObCreateIndexStmt::ObCreateIndexStmt(ObStringBuf* name_pool)
  : ObBasicStmt(ObBasicStmt::T_CREATE_INDEX), name_pool_(name_pool), unique_(false)
{
  unique_ = false;
}

ObCreateIndexStmt::ObCreateIndexStmt()
  : ObBasicStmt(ObBasicStmt::T_CREATE_INDEX), name_pool_(NULL), unique_(false)
{
}

ObCreateIndexStmt::~ObCreateIndexStmt()
{
}

int ObCreateIndexStmt::set_table_name(ResultPlan& result_plan, const ObString& table_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  const ObTableSchema* table_schema = NULL;
  if ((schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)) == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }
  else if ((table_schema = schema_checker->get_table_schema(table_name)) == NULL)
  {
    ret = OB_ERR_TABLE_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Table '%.*s' doesn't exist", table_name.length(), table_name.ptr());
  }
  else if ((ret = ob_write_string(*name_pool_, table_name, table_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Allocate memory for table name failed");
  }
  return ret;
}

int ObCreateIndexStmt::set_index_name(ResultPlan& result_plan, const ObString& index_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  const ObTableSchema* idx_schema = NULL;
  if ((schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)) == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }
  else if ((ret = ob_write_index_name(*name_pool_, index_name, index_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Allocate memory for index name failed");
  }
  else if ((idx_schema = schema_checker->get_table_schema(index_name_)) != NULL)
  {
    ret = OB_ERR_TABLE_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Index '%.*s' has already existed", index_name.length(), index_name.ptr());
  }
  return ret;
}

int ObCreateIndexStmt::set_index_option(ResultPlan& result_plan, const ObTableOption& option)
{
  int ret = OB_SUCCESS;
  index_option_ = option;
  if ((ret = ob_write_string(*name_pool_, option.expire_info_, index_option_.expire_info_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Allocate memory for expire_info failed");
  }
  else if ((ret = ob_write_string(*name_pool_,
                                  option.compress_method_,
                                  index_option_.compress_method_)
                                  ) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Allocate memory for compress_method failed");
  }
  return ret;
}

int ObCreateIndexStmt::add_sort_column(ResultPlan& result_plan, const ObColumnSortItem& sort_column)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  ObColumnSortItem col;
  col.order_type_ = sort_column.order_type_;
  if (table_name_.length() <= 0)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Please set table name before add column(s)");
  }
  else if ((schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)) == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }
  else if ((column_schema = schema_checker->get_column_schema(table_name_, sort_column.column_name_)) == NULL)
  {
    ret = OB_ERR_TABLE_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Column '%.*s' does not exist", sort_column.column_name_.length(), sort_column.column_name_.ptr());
  }
  else if ((ret = ob_write_string(*name_pool_, sort_column.column_name_, col.column_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Allocate memory for column name failed");
  }
  else if ((ret = sort_columns_.push_back(col)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Add index column %.*s failed", sort_column.column_name_.length(), sort_column.column_name_.ptr());
  }
  return ret;
}

int ObCreateIndexStmt::add_storing_column(ResultPlan& result_plan, const ObString& column_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  ObString col;
  if (table_name_.length() <= 0)
  {
    ret = OB_NOT_INIT;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Please set table name before add column(s)");
  }
  else if ((schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)) == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }
  else if ((column_schema = schema_checker->get_column_schema(table_name_, column_name)) == NULL)
  {
    ret = OB_ERR_TABLE_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Column '%.*s' does not exist", column_name.length(), column_name.ptr());
  }
  else if ((ret = ob_write_string(*name_pool_, column_name, col)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Allocate memory for column name failed");
  }
  else if ((ret = storing_columns_.push_back(col)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Add storing column %.*s failed", column_name.length(), column_name.ptr());
  }
  return ret;
}

void ObCreateIndexStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "ObCreateIndexStmt %d Begin\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "Index Name = %.*s\n", index_name_.length(), index_name_.ptr());
  if (unique_)
  {
    print_indentation(fp, level + 1);
    fprintf(fp, "Unique\n");
  }
  print_indentation(fp, level + 1);
  fprintf(fp, "Table Name = %.*s\n", table_name_.length(), table_name_.ptr());
  print_indentation(fp, level + 1);
  fprintf(fp, "Table Name ::= %.*s\n", table_name_.length(), table_name_.ptr());
  print_indentation(fp, level + 1);
  fprintf(fp, "INDEX COLUMN(s) ::= (");
  for (int32_t i = 0; i < sort_columns_.count(); i++)
  {
    ObColumnSortItem& col = sort_columns_.at(i);
    fprintf(fp, "%.*s %s", col.column_name_.length(), col.column_name_.ptr(),
            col.order_type_ == ObColumnSortItem::ASC ? "ASC" : "DESC");
    if (i <= sort_columns_.count() - 1)
      fprintf(fp, ", ");
    else
      fprintf(fp, ")\n");
  }
  print_indentation(fp, level + 1);
  fprintf(fp, "STORING COLUMN(s) ::= (");
  for (int32_t i = 0; i < storing_columns_.count(); i++)
  {
    ObString& col = storing_columns_.at(i);
    fprintf(fp, "%.*s\n", col.length(), col.ptr());
    if (i <= storing_columns_.count() - 1)
      fprintf(fp, ", ");
    else
      fprintf(fp, ")\n");
  }
  index_option_.print(fp, level + 1);
  print_indentation(fp, level);
  fprintf(fp, "ObCreateIndexStmt %d End\n", index);
}


