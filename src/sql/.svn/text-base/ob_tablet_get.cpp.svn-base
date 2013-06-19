/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_get.cpp 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_tablet_get.h"
#include "common/ob_new_scanner_helper.h"

using namespace oceanbase;
using namespace sql;
using namespace common;

void ObTabletGet::reset()
{
  op_project_.clear();
  op_tablet_join_.reset();
}

int ObTabletGet::create_plan(const ObSchemaManagerV2 &schema_mgr)
{
  int ret = OB_SUCCESS;
  ObTabletJoin::TableJoinInfo table_join_info;
  int64_t basic_column_count = common::OB_MAX_COLUMN_NUMBER;
  uint64_t basic_columns[basic_column_count];
  uint64_t table_id = sql_get_param_->get_table_id();
  ObProject *op_project = NULL;
  int64_t data_version = 0;
  bool is_need_incremental_data = true;
  bool is_plain_query = false;
  ObCellInfo cell_info;
  int64_t rowkey_cell_count = 0;
  
  if (OB_SUCCESS != (ret = get_basic_column_and_join_info(
                               *sql_get_param_, 
                               schema_mgr, 
                               basic_columns, 
                               basic_column_count,
                               rowkey_cell_count,
                               table_join_info,
                               is_plain_query)))
  {
    TBSYS_LOG(WARN, "fail to get basic column and join info:ret[%d]", ret);
  }
  else
  {
    TBSYS_LOG(DEBUG, "cs select basic column ids [%ld], project[%s]", basic_column_count, 
      to_cstring(sql_get_param_->get_project()));
  }

  if (OB_SUCCESS == ret)
  {
    get_param_.reset(true);
    for (int64_t i = 0; OB_SUCCESS == ret && i < sql_get_param_->get_row_size(); i ++)
    {
      cell_info.row_key_ = *(sql_get_param_->operator[](i));
      cell_info.table_id_ = table_id;
      for (int64_t col_id_idx = 0; OB_SUCCESS == ret && col_id_idx < basic_column_count; col_id_idx ++)
      {
        cell_info.column_id_ = basic_columns[col_id_idx];
        if (OB_SUCCESS != (ret = get_param_.add_cell(cell_info)))
        {
          TBSYS_LOG(WARN, "fail to add cell to get param:ret[%d]", ret);
        }
      }
    }
    TBSYS_LOG(DEBUG, "cs get param row size[%ld] sql_get_param row size[%ld]", get_param_.get_row_size(), 
      sql_get_param_->get_row_size());
  }

  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = op_sstable_get_.open_tablet_manager(tablet_manager_,
            sql_get_param_, basic_columns, basic_column_count, rowkey_cell_count)))
    {
      TBSYS_LOG(WARN, "fail to open scan context:ret[%d]", ret);
    }
    else
    {
      plan_level_ = SSTABLE_DATA;
    }
  }

  if (OB_SUCCESS == ret)
  {
    FILL_TRACE_LOG("read only static data[%s]", sql_get_param_->get_is_only_static_data() ? "TRUE":"FALSE");
    if (sql_get_param_->get_is_only_static_data())
    {
      is_need_incremental_data = false;
      cur_rowkey_op_ = &op_sstable_get_;
    }
    else
    {
      op_sstable_get_.get_tablet_data_version(data_version);
      FILL_TRACE_LOG("request data version[%ld], cs serving data version[%ld]", 
        sql_get_param_->get_data_version(), data_version);
      if (sql_get_param_->get_data_version() != OB_NEWEST_DATA_VERSION)
      {
        if (sql_get_param_->get_data_version() == OB_INVALID_VERSION)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "invalid version");
        }
        else if (sql_get_param_->get_data_version() < data_version)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "The request version is not exist: request version[%ld], sstable version[%ld]", sql_get_param_->get_data_version(), data_version);
        }
        else if (sql_get_param_->get_data_version() == data_version)
        {
          is_need_incremental_data = false;
          cur_rowkey_op_ = &op_sstable_get_;
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (is_need_incremental_data)
    {
      const ObRowkeyInfo *rowkey_info = NULL;
      if (table_join_info.join_column_.count() > 0)
      {
        const ObTableSchema *tschema = schema_mgr.get_table_schema(table_join_info.right_table_id_);
        if (NULL == tschema)
        {
          ret = OB_SCHEMA_ERROR;
          TBSYS_LOG(WARN, "fail to get right table id schema[%lu]", table_join_info.right_table_id_);
        }
        else
        {
          rowkey_info = &(tschema->get_rowkey_info());
        }
      }

      if (OB_SUCCESS != (ret = need_incremental_data(
                             basic_columns, 
                             basic_column_count,
                             table_join_info, 
                             rowkey_info,
                             data_version, 
                             sql_get_param_->get_data_version(),
                             rowkey_cell_count)))
      {
        TBSYS_LOG(WARN, "fail to add ups operator:ret[%d]", ret);
      }
    }
    else
    {
      op_root_ = &op_sstable_get_;
    }
  }

  
  if (OB_SUCCESS == ret && sql_get_param_->has_project())
  {
    op_project = &op_project_;
    if (OB_SUCCESS == ret)
    {
      op_project->assign(sql_get_param_->get_project());
      if (OB_SUCCESS != (ret = op_project->set_child(0, *op_root_)))
      {
        TBSYS_LOG(WARN, "fail to set project child. ret=%d", ret);
      }
      else
      {
        
        TBSYS_LOG(DEBUG, "cs get project desc[%s]", to_cstring(*op_project));
        op_root_ = op_project;
      }
    }
  }

  //release tablet
  if (OB_SUCCESS != ret)
  {
    int err = OB_SUCCESS;
    if (OB_SUCCESS != (err = op_sstable_get_.close()))
    {
      TBSYS_LOG(WARN, "fail to close op sstable get:err[%d]", err);
    }
  }

  return ret;
}

int ObTabletGet::need_incremental_data(
    const uint64_t *basic_columns,
    const uint64_t basic_column_count,
    ObTabletJoin::TableJoinInfo &table_join_info, 
    const ObRowkeyInfo *right_table_rowkey_info,
    int64_t start_data_version, 
    int64_t end_data_version,
    int64_t rowkey_cell_count)

{
  int ret = OB_SUCCESS;
  ObUpsMultiGet *op_ups_multi_get = NULL;
  ObMultipleGetMerge *op_tablet_get_merge = NULL;
  ObTabletJoin *op_tablet_join = NULL;

  uint64_t table_id = sql_get_param_->get_table_id();
  uint64_t renamed_table_id = sql_get_param_->get_renamed_table_id();

  ObVersionRange version_range;
  UNUSED(basic_columns);
  UNUSED(basic_column_count);

  if(OB_SUCCESS == ret)
  {
    version_range.start_version_ = ObVersion(start_data_version + 1);
    version_range.border_flag_.unset_min_value();
    version_range.border_flag_.set_inclusive_start();

    if (end_data_version == OB_NEWEST_DATA_VERSION)
    {
      version_range.border_flag_.set_max_value();
    }
    else
    {
      version_range.end_version_ = ObVersion(end_data_version);
      version_range.end_version_.minor_ = INT16_MAX;
      version_range.border_flag_.unset_max_value();
      version_range.border_flag_.set_inclusive_end();
    }
    get_param_.set_version_range(version_range);
  }

  // init ups get 
  if (OB_SUCCESS == ret)
  {
    op_ups_multi_get = &op_ups_multi_get_;
    ups_mget_row_desc_.reset();
    if(OB_SUCCESS != (ret = op_ups_multi_get->set_rpc_proxy(rpc_proxy_)))
    {
      TBSYS_LOG(WARN, "ups scan set ups rpc stub fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_ups_multi_get->set_network_timeout(network_timeout_)))
    {
      TBSYS_LOG(WARN, "set ups scan timeout fail:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret =
          ObNewScannerHelper::get_row_desc(get_param_, true, rowkey_cell_count, ups_mget_row_desc_)))
    {
      TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
    }
    else
    {
      get_param_.set_is_read_consistency(sql_get_param_->get_is_read_consistency());
      op_ups_multi_get->set_row_desc(ups_mget_row_desc_);
      op_ups_multi_get->set_get_param(get_param_);
    }
  }

  if (OB_SUCCESS == ret)
  {
    op_tablet_get_merge = &op_tablet_get_merge_;
    if (OB_SUCCESS == ret)
    {
      cur_rowkey_op_ = op_tablet_get_merge;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = op_tablet_get_merge->set_child(0, op_sstable_get_)))
    {
      TBSYS_LOG(WARN, "set sstable get fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_tablet_get_merge->set_child(1, *op_ups_multi_get)))
    {
      TBSYS_LOG(WARN, "set ups get fail:ret[%d]", ret);
    }
    else
    {
      plan_level_ = UPS_DATA;
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(table_join_info.join_column_.count() > 0)
    {
      op_tablet_join = &op_tablet_join_;
      if (OB_SUCCESS == ret)
      {
        op_tablet_join->set_version_range(version_range);
        op_tablet_join->set_table_join_info(table_join_info);
        op_tablet_join->set_right_table_rowkey_info(right_table_rowkey_info);
        op_tablet_join->set_batch_count(join_batch_count_);
        op_tablet_join->set_is_read_consistency(is_read_consistency_);
        op_tablet_join->set_child(0, *op_tablet_get_merge);
        op_tablet_join->set_network_timeout(network_timeout_);
        if (OB_SUCCESS != (ret = op_tablet_join->set_rpc_proxy(rpc_proxy_) ))
        {
          TBSYS_LOG(WARN, "fail to set rpc proxy:ret[%d]", ret);
        }
        else
        {
          op_root_ = op_tablet_join;
          plan_level_ = JOIN_DATA;
        }
      }
    }
    else
    {
      op_root_ = op_tablet_get_merge;
    }
  }

  if(OB_SUCCESS == ret && renamed_table_id != table_id)
  {
    if (OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = op_rename_.set_table(renamed_table_id, table_id)))
      {
        TBSYS_LOG(WARN, "op_rename set table fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = op_rename_.set_child(0, *op_root_)))
      {
        TBSYS_LOG(WARN, "op_rename set child fail:ret[%d]", ret);
      }
      else
      {
        op_root_ = &op_rename_; 
      }
    }
  }
  
  return ret;
}

bool ObTabletGet::check_inner_stat() const
{
  int ret = true;
  if (NULL == tablet_manager_ ||
    NULL == sql_get_param_)
  {
    ret = false;
    TBSYS_LOG(WARN, "tablet_manager_ [%p], sql_get_param_[%p]", 
      tablet_manager_, sql_get_param_);
  }
  return ret;
}

int ObTabletGet::set_tablet_manager(chunkserver::ObTabletManager *tablet_manager)
{
  int ret = OB_SUCCESS;
  if (NULL == tablet_manager)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "tablet_manager is null");
  }
  else
  {
    tablet_manager_ = tablet_manager;
  }
  return ret;
}

int64_t ObTabletGet::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != op_root_)
  {
    op_root_->to_string(buf, buf_len);
  }
  return pos;
}


