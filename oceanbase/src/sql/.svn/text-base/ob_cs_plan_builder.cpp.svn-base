/*
 * (C) 1999-2013 Alibaba Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_cs_plan_builder.cpp,  04/26/2013 08:51:54 PM Yu Huang Exp $
 * 
 * Author:  
 *   Huang Yu <xiaochu.yh@alipay.com>
 * Description:  
 *   
 * 
 */
#include "ob_cs_plan_builder.h"
#include "common/ob_bitmap.h"

using namespace oceanbase::sql;

int ObCsPlanBuilder::get_basic_column(
  ObSqlPlanParam &plan_param,
  const common::ObSchemaManagerV2 *schema_mgr,
  ObSqlReadSimpleParam &read_param,
  bool &is_plain_query) const
{
  int ret = OB_SUCCESS;
  ObSqlExpression expr;
  uint64_t column_id = OB_INVALID_ID;
  uint64_t expr_table_id = OB_INVALID_ID;
  uint64_t table_id = plan_param.base_table_id_;
  uint64_t renamed_table_id = plan_param.table_id_;
  int64_t matched_rowkey_count = 0;
  int64_t matched_rowkey_index = 0;
  int64_t rowkey_cell_count = 0;
  bool is_cid = false;
  bool is_duplicate = false;
  common::ObBitmap<uint64_t> column_ids(OB_ALL_MAX_COLUMN_ID);
  const ObProject *project = NULL;
  const ObTableSchema* table_schema = NULL;

  column_ids.clear();
  is_plain_query = false;
  
  OB_ASSERT(NULL != schema_mgr);
  if (NULL == schema_mgr)
  {
    TBSYS_LOG(WARN, "schema_mgr=NULL");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == (table_schema = schema_mgr->get_table_schema(table_id)))
  {
    TBSYS_LOG(WARN, "get_basic_column_and_join_info null schema.");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (NULL == (project = plan_param.get_op_project()))
  {
    TBSYS_LOG(WARN, "fail to get project. not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    // add add rowkey columns;
    const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
    rowkey_cell_count = rowkey_info.get_size();
    for (int64_t i = 0; i < rowkey_cell_count; ++i)
    {
      column_id = rowkey_info.get_column(i)->column_id_;
      if (OB_SUCCESS != (ret = add_unique_column(column_ids, column_id, read_param, is_duplicate)))
      {
        TBSYS_LOG(WARN, "failed to add_unique_column, ret=%d", ret);
        break;
      }
    }
    // add project columns, erase duplicate columns include rowkey column;
    for(int64_t i = 0; OB_SUCCESS == ret &&  i < project->get_output_columns().count(); i++)
    {
      if(OB_SUCCESS != (ret = project->get_output_columns().at(i, expr)))
      {
        TBSYS_LOG(WARN, "get expression from out columns fail:ret[%d] i[%ld]", ret, i);
        break;
      }
      if(OB_INVALID_ID != expr.get_table_id()
          && table_id != expr.get_table_id()
          && renamed_table_id != expr.get_table_id())
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "Unknown expression id, table_id[%lu], renamed_table_id[%lu], expr_table_id[%lu]", 
            table_id, renamed_table_id, expr.get_table_id());
      }
      else
      {
        column_id = expr.get_column_id();
        if ( (OB_SUCCESS == (ret = expr.get_column_index_expr(expr_table_id, column_id, is_cid))) 
            && is_cid && common::OB_ACTION_FLAG_COLUMN_ID != column_id)
        {
          // check if is rowkey column? 
          if (OB_SUCCESS == table_schema->get_rowkey_info().get_index(column_id, matched_rowkey_index))
          {
            if (matched_rowkey_count >= 0 && matched_rowkey_count < rowkey_cell_count)
            {
              if (matched_rowkey_index == i)
              {
                ++matched_rowkey_count;
              }
              else
              {
                matched_rowkey_count = -1;
              }
            }
          }
          if (OB_SUCCESS != (ret = add_unique_column(column_ids, column_id, read_param, is_duplicate)))
          {
            TBSYS_LOG(WARN, "failed to add_unique_column, ret=%d", ret);
            break;
          }
          else if (is_duplicate)
          {
            TBSYS_LOG(DEBUG, "duplicate column[%ld,%ld]", table_id, column_id);
            continue;
          }
        }
      }
    }/* end for */
  }

  if (OB_SUCCESS == ret) 
  {
    if (read_param.get_column_id_size() == project->get_output_column_size() 
        && matched_rowkey_count == rowkey_cell_count)
    {
      is_plain_query = true;
    }
  }

  return ret;
}

int ObCsPlanBuilder::add_unique_column(
    common::ObBitmap<uint64_t> &column_ids,
    uint64_t column_id, 
    ObSqlReadSimpleParam &read_param,
    bool &is_duplicate) const
{
  int ret = OB_SUCCESS;
  is_duplicate = false;
  if (column_ids.test(column_id))
  {
    is_duplicate = true;
  }
  else if (OB_SUCCESS != (ret = column_ids.set(column_id)))
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "fail to add member[%lu]", column_id);
  }
  else if (OB_SUCCESS != (ret = read_param.add_column(column_id)))
  {
    TBSYS_LOG(WARN, "fail to add column to read param.ret=%d", ret);
  }
  return ret;
}

