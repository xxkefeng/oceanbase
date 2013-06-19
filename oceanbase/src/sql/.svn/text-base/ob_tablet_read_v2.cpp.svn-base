/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_read.cpp 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *   Hu XU <yongle.xh@alipay.com>
 *
 */

#include "ob_tablet_read_v2.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

ObTabletReadV2::ObTabletReadV2(ObPlanContext *plan_context,
    chunkserver::ObTabletManager *tablet_manager,
    common::ObSqlUpsRpcProxy* ups_rpc_proxy, const ObSchemaManagerV2 *schema_mgr)
  :op_root_(NULL),
  is_read_consistency_(true),
  network_timeout_(0),
  join_batch_count_(0),
  cur_rowkey_op_(NULL),
  plan_level_(SSTABLE_DATA),
  column_ids_(OB_ALL_MAX_COLUMN_ID),
  plan_context_(plan_context),
  tablet_manager_(tablet_manager),
  ups_rpc_proxy_(ups_rpc_proxy),
  schema_mgr_(schema_mgr)
{
}

int ObTabletReadV2::open()
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = create_plan()))
  {
    TBSYS_LOG(WARN, "failed to create plan, ret=%d", ret);
  }
  else if (NULL == op_root_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "should create plan before open");
  }
  else
  {
    ret = op_root_->open();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "open tablets scan child operator fail. ret=%d", ret);
    }
  }
  return ret;
}

int ObTabletReadV2::close()
{
  int ret = OB_SUCCESS;
  if(NULL != op_root_)
  {
    if(OB_SUCCESS != (ret = op_root_->close()))
    {
      TBSYS_LOG(WARN, "close op_root fail:ret[%d]", ret);
    }
  }

  return ret;
}


int ObTabletReadV2::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (op_root_ == NULL)
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = op_root_->get_row_desc(row_desc);
  }
  return ret;
}


int ObTabletReadV2::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;

  if(OB_UNLIKELY(NULL == op_root_))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "op root is null");
  }
  else
  {
    ret = op_root_->get_next_row(row);
    if (OB_SUCCESS == ret && NULL != row)
    {
      TBSYS_LOG(DEBUG, "tablet read row[%s]", to_cstring(*row));
    }
    else if (OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObTabletReadV2::get_cur_rowkey(const ObRowkey *&rowkey) const
{
  int ret = OB_SUCCESS;
  if (NULL == cur_rowkey_op_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "last_rowkey_op_ is null");
  }
  else
  {
    ret = cur_rowkey_op_->get_cur_rowkey(rowkey);
    if (OB_SUCCESS != ret || NULL == rowkey)
    {
      TBSYS_LOG(WARN, "failed to get cur rowkey, ret[%d]", ret);
    }
  }
  return ret;
}

int ObTabletReadV2::add_unique_column( uint64_t column_id, 
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

int ObTabletReadV2::get_basic_column_and_join_info(
  const ObSqlReadSimpleParam& param,
  const ObSchemaManagerV2& schema_mgr, 
  uint64_t *basic_columns, // new columns used to query sstable
  int64_t &basic_column_count,
  int64_t &rowkey_cell_count,
  ObTabletJoin::TableJoinInfo &table_join_info)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_INVALID_ID;
  uint64_t expr_table_id = OB_INVALID_ID;
  uint64_t table_id = param.get_table_id();
  int64_t count = 0;
  bool is_duplicate = false;
  int64_t matched_rowkey_count = 0;
  int64_t matched_rowkey_index = 0;
  const ObColumnSchemaV2 *column_schema = NULL;
  const ObColumnSchemaV2::ObJoinInfo *join_info = NULL;
  ObTabletJoin::JoinInfo join_info_item;

  column_ids_.clear();

  const ObTableSchema* table_schema = schema_mgr.get_table_schema(table_id);

  if (OB_UNLIKELY(NULL == table_schema))
  {
    TBSYS_LOG(WARN, "table schema must not null, table_id=%lu", table_id);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_UNLIKELY(NULL == basic_columns || 0 >= basic_column_count))
  {
    TBSYS_LOG(WARN, "table_id=%lu: columns[%p] or count[%ld] <= 0", table_id, basic_columns, basic_column_count);
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

    // generate columns, erase duplicate columns include rowkey column;
    const uint64_t *need_columns = param.get_column_ids();
    const int64_t columns_count = param.get_column_id_size();

    for(int64_t i = 0; OB_SUCCESS == ret &&  i < columns_count; i++)
    {
      {
        column_id = need_columns[i];
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

  if(OB_SUCCESS == ret)
  {
    basic_column_count = count;
  }

  return ret;
}

