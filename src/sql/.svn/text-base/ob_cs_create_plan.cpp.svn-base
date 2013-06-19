/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cs_create_plan.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_cs_create_plan.h"

using namespace oceanbase;
using namespace sql;

int ObCSCreatePlan::add_unique_column( uint64_t column_id, 
    uint64_t *basic_columns, int64_t &basic_column_count, 
    int64_t &pos, bool &is_duplicate)
{
  int ret = OB_SUCCESS;
  is_duplicate = false;
  if (column_ids_.test(column_id))
  {
    is_duplicate = true;
  }
  else if (OB_SUCCESS != (ret = column_ids_.set(column_id)))
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "fail to add member[%lu]", column_id);
  }
  else if (pos >= basic_column_count)
  {
    ret = OB_SIZE_OVERFLOW;
    TBSYS_LOG(WARN, "current count=%ld >= basic_column_count=%ld", pos, basic_column_count);
  }
  else
  {
    basic_columns[pos++] = column_id;
  }
  return ret;
}

int ObCSCreatePlan::get_basic_column_and_join_info(
  const ObSqlReadParam& param,
  const ObSchemaManagerV2& schema_mgr, 
  uint64_t *basic_columns,
  int64_t &basic_column_count,
  int64_t &rowkey_cell_count,
  ObTabletJoin::TableJoinInfo &table_join_info,
  bool &is_plain_query)
{
  int ret = OB_SUCCESS;
  ObSqlExpression expr;
  uint64_t column_id = OB_INVALID_ID;
  uint64_t expr_table_id = OB_INVALID_ID;
  uint64_t table_id = param.get_table_id();
  uint64_t renamed_table_id = param.get_renamed_table_id();
  int64_t count = 0;
  int64_t matched_rowkey_count = 0;
  int64_t matched_rowkey_index = 0;
  const ObColumnSchemaV2 *column_schema = NULL;
  const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
  ObTabletJoin::JoinInfo join_info_item;
  bool is_cid = false;
  bool is_duplicate = false;

  column_ids_.clear();
  is_plain_query = false;

  const ObProject& project = param.get_project();
  const ObTableSchema* table_schema = schema_mgr.get_table_schema(table_id);

  if (OB_UNLIKELY(NULL == table_schema))
  {
    TBSYS_LOG(WARN, "get_basic_column_and_join_info null schema.");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_UNLIKELY(NULL == basic_columns || 0 >= basic_column_count))
  {
    TBSYS_LOG(WARN, "columns[%p] or count[%ld] <= 0", basic_columns, basic_column_count);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    // add add rowkey columns;
    const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
    rowkey_cell_count = rowkey_info.get_size();
    for (int64_t i = 0; i < rowkey_cell_count; ++i)
    {
      column_id = rowkey_info.get_column(i)->column_id_;
      if (OB_SUCCESS != (ret = add_unique_column(column_id, 
              basic_columns, basic_column_count, count, is_duplicate)))
      {
        break;
      }
    }

    // add project columns, erase duplicate columns include rowkey column;
    for(int64_t i = 0; OB_SUCCESS == ret &&  i < project.get_output_columns().count(); i++)
    {
      if(OB_SUCCESS != (ret = project.get_output_columns().at(i, expr)))
      {
        TBSYS_LOG(WARN, "get expression from out columns fail:ret[%d] i[%ld]", ret, i);
        break;
      }
      else if(OB_INVALID_ID != expr.get_table_id()
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
        //如果是普通列, 复合列不交给下层的物理操作符
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

          if (OB_SUCCESS != (ret = add_unique_column(column_id, 
                  basic_columns, basic_column_count, count, is_duplicate)))
          {
            break;
          }
          else if (is_duplicate)
          {
            //TBSYS_LOG(INFO, "duplicate column[%ld,%ld]", table_id, column_id);
            continue;
          }
          else if(NULL == (column_schema = schema_mgr.get_column_schema(table_id, column_id)))
          {
            ret = OB_ERROR;
            TBSYS_LOG(WARN, "get column schema fail:expr_table_id[%lu], column_id[%lu]", expr_table_id, column_id);
          }
          else
          {
            // build join_condition_
            join_info = column_schema->get_join_info();
            if(NULL != join_info)
            {
              if(OB_INVALID_ID == table_join_info.left_table_id_)
              {
                table_join_info.left_table_id_ = table_id;
              }
              else if(table_join_info.left_table_id_ != table_id)
              {
                ret = OB_ERROR;
                TBSYS_LOG(WARN, "left table id cannot change:t1[%lu], t2[%lu]", 
                    table_join_info.left_table_id_, table_id);
              }
              else if(table_join_info.join_condition_.count() == 0)
              {
                uint64_t left_rowkey_column_id = OB_INVALID_ID;
                for(uint64_t left_col_idx=0; left_col_idx < join_info->left_column_count_; left_col_idx++)
                {
                  int64_t col_idx = join_info->left_column_offset_array_[left_col_idx];
                  if(OB_SUCCESS != (ret = table_schema->get_rowkey_info().get_column_id(col_idx, left_rowkey_column_id)))
                  {
                    TBSYS_LOG(WARN, "get left rowkey column id fail:ret[%d], col_idx[%ld]", ret, col_idx);
                  }
                  else if(OB_SUCCESS != (ret = table_join_info.join_condition_.push_back(left_rowkey_column_id)))
                  {
                    TBSYS_LOG(WARN, "push left rowkey column id fail:ret[%d], left_rowkey_column_id[%ld]", 
                        ret, left_rowkey_column_id);
                  }
                }
              }
              else if((uint64_t)table_join_info.join_condition_.count() != join_info->left_column_count_)
              {
                ret = OB_ERROR;
                TBSYS_LOG(WARN, "join info count change");
              }

              if(OB_SUCCESS == ret)
              {
                table_join_info.right_table_id_ = join_info->join_table_;
                join_info_item.left_column_id_ = column_schema->get_id();
                join_info_item.right_column_id_ = join_info->correlated_column_;
                if(OB_SUCCESS != (ret = table_join_info.join_column_.push_back(join_info_item)))
                {
                  TBSYS_LOG(WARN, "add join info item fail:ret[%d]", ret);
                }
              }
            }
          }

        }
      }

    }
  }

  //找到所有的需要join的普通列
  if(OB_SUCCESS == ret && table_join_info.join_column_.count() > 0)
  {
    for(int64_t i=0;OB_SUCCESS == ret && i<table_join_info.join_condition_.count();i++)
    {
      if(OB_SUCCESS != (ret = table_join_info.join_condition_.at(i, column_id)))
      {
        TBSYS_LOG(WARN, "get join cond fail:ret[%d] i[%ld]", ret, i);
      }
      else if (OB_SUCCESS != (ret = add_unique_column(column_id, basic_columns, basic_column_count, count, is_duplicate)))
      {
        TBSYS_LOG(WARN, "fail to add join column[%lu]", column_id);
      }
    }
  }

  if (OB_SUCCESS == ret) 
  {
    basic_column_count = count;
    if (basic_column_count == project.get_output_column_size() 
        && matched_rowkey_count == rowkey_cell_count)
    {
      is_plain_query = true;
    }
  }

  return ret;
}

