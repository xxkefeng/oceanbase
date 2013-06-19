/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_drop_index_stmt.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_drop_index_stmt.h"
#include "ob_schema_checker.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObDropIndexStmt::ObDropIndexStmt(ObStringBuf* name_pool)
  : ObBasicStmt(ObBasicStmt::T_DROP_INDEX), name_pool_(name_pool)
{
}

ObDropIndexStmt::ObDropIndexStmt()
  : ObBasicStmt(ObBasicStmt::T_DROP_INDEX)
{
}

ObDropIndexStmt::~ObDropIndexStmt()
{
}

int ObDropIndexStmt::set_index_name(ResultPlan& result_plan, const common::ObString& index_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
  if (schema_checker == NULL)
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
  else if (schema_checker->get_table_schema(index_name_) == NULL)
  {
    ret = OB_ERR_TABLE_UNKNOWN;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "index '%.*s' does not exist", index_name.length(), index_name.ptr());
  }
  return ret;
}

void ObDropIndexStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(index);
  print_indentation(fp, level);
  fprintf(fp, "ObDropIndexStmt %d Begin\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "Index Name := %.*s\n", index_name_.length(), index_name_.ptr());
  print_indentation(fp, level);
  fprintf(fp, "ObDropIndexStmt %d End\n", index);
}

