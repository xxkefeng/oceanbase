/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_create_table_stmt.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_create_table_stmt.h"
#include "ob_schema_checker.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObCreateTableStmt::ObCreateTableStmt(ObStringBuf* name_pool)
  : ObBasicStmt(ObBasicStmt::T_CREATE_TABLE)
{
  name_pool_ = name_pool;
  next_column_id_ = OB_APP_MIN_COLUMN_ID;
  if_not_exists_ = false;
}

ObCreateTableStmt::~ObCreateTableStmt()
{
}

int ObCreateTableStmt::set_table_name(ResultPlan& result_plan, const ObString& table_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;

  ObSchemaChecker* schema_checker = NULL;
  schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
  if (schema_checker == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "schema(s) are not set");
  }
  if (ret == OB_SUCCESS && !if_not_exists_)
  {
    uint64_t table_id = OB_INVALID_ID;
    if ((table_id = schema_checker->get_table_id(table_name)) != OB_INVALID_ID)
    {
      ret = OB_ERR_ALREADY_EXISTS;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "table '%.*s' already exists", table_name.length(), table_name.ptr());
    }
  }

  if (ret == OB_SUCCESS && (ret = ob_write_string(*name_pool_, table_name, table_name_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "allocate memory for table name failed");
  }
  return ret;
}

int ObCreateTableStmt::set_table_option(ResultPlan& result_plan, const ObTableOption& option)
{
  int ret = OB_SUCCESS;
  table_option_ = option;
  if ((ret = ob_write_string(*name_pool_, option.expire_info_, table_option_.expire_info_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Allocate memory for expire_info failed");
  }
  else if ((ret = ob_write_string(*name_pool_,
                                  option.compress_method_,
                                  table_option_.compress_method_)
                                  ) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Allocate memory for compress_method failed");
  }
  return ret;
}

int ObCreateTableStmt::add_primary_key_part(ResultPlan& result_plan, const ObString& column_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObColumnDef *col = NULL;
  for (int64_t i = 0; i < columns_.count(); i++)
  {
    ObColumnDef& cur_col = columns_.at(i);
    if (cur_col.column_name_ == column_name)
    {
      col = &cur_col;
      break;
    }
  }
  if (col == NULL)
  {
    ret = OB_ERR_COLUMN_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "key column '%.*s' does not exist in table", column_name.length(), column_name.ptr());
  }
  else if (col->primary_key_id_ > 0)
  {
    ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "duplicate primary key part '%.*s'", column_name.length(), column_name.ptr());
  }
  else if ((ret = primay_keys_.push_back(col->column_id_)) != OB_SUCCESS)
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "can not add primary key part");
  }
  else
  {
    col->primary_key_id_ = primay_keys_.count();
  }

  return ret;
}

int ObCreateTableStmt::add_column_def(ResultPlan& result_plan, const ObColumnDef& column)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  for (int64_t i = 0; ret == OB_SUCCESS && i < columns_.count(); i++)
  {
    if (columns_.at(i).column_name_ == column.column_name_)
    {
      ret = OB_ERR_COLUMN_DUPLICATE;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "duplicate column name '%.*s'", column.column_name_.length(), column.column_name_.ptr());
      break;
    }
    if (columns_.at(i).data_type_ == ObCreateTimeType && column.data_type_ == ObCreateTimeType)
    {
      ret = OB_ERR_CREATETIME_DUPLICATE;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "duplicate CREATETIME defined");
      break;
    }
    if (columns_.at(i).data_type_ == ObModifyTimeType && column.data_type_ == ObModifyTimeType)
    {
      ret = OB_ERR_MODIFYTIME_DUPLICATE;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "duplicate MODIFYTIME defined");
      break;
    }
  }
  if (ret == OB_SUCCESS)
  {
    ObColumnDef col = column;
    ret = ob_write_string(*name_pool_, column.column_name_, col.column_name_);
    if (ret == OB_SUCCESS)
    {
      ObObjType type = column.default_value_.get_type();
      if (type == ObVarcharType || type == ObDecimalType)
      {
        ObString str;
        ObString new_str;
        column.default_value_.get_varchar(str);
        if ((ret = ob_write_string(*name_pool_, str, new_str)) == OB_SUCCESS)
        {
          col.default_value_.set_varchar(new_str);
          col.default_value_.set_type(type);
        }
        else
        {
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "Can not malloc default value string for column name '%.*s'", str.length(), str.ptr());
        }
      }
    }
    else
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "Can not malloc space for column name '%.*s'", column.column_name_.length(), column.column_name_.ptr());
    }

    if (ret == OB_SUCCESS && (columns_.push_back(col)) != OB_SUCCESS)
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "can not add column definition");
  }

  return ret;
}

const ObColumnDef& ObCreateTableStmt::get_column_def(int64_t index) const
{
  OB_ASSERT(0 <= index && index < columns_.count());
  return columns_.at(index);
}

void ObCreateTableStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "ObCreateTableStmt %d Begin\n", index);
  if (if_not_exists_)
  {
    print_indentation(fp, level + 1);
    fprintf(fp, "if_not_exists_ = TRUE\n");
  }
  print_indentation(fp, level + 1);
  fprintf(fp, "Table Name ::= %.*s\n", table_name_.length(), table_name_.ptr());
  print_indentation(fp, level + 1);
  fprintf(fp, "COLUMN DEFINITION(s) ::=\n");
  for (int32_t i = 0; i < columns_.count(); i++)
  {
    ObColumnDef& col = columns_.at(i);
    print_indentation(fp, level + 2);
    fprintf(fp, "Column(%d) ::=\n", i + 1);
    col.print(fp, level + 3);
  }
  table_option_.print(fp, level + 1);
  print_indentation(fp, level);
  fprintf(fp, "ObCreateTableStmt %d End\n", index);
}
