/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_get_v2.cpp 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *   Hu XU  <yongle.xh@alipay.com>
 *
 */

#include "ob_tablet_get_v2.h"
#include "common/ob_new_scanner_helper.h"

using namespace oceanbase;
using namespace sql;
using namespace common;

ObTabletGetV2::ObTabletGetV2(ObPlanContext *plan_context,
    chunkserver::ObTabletManager *tablet_manager,
    common::ObSqlUpsRpcProxy* ups_rpc_proxy,
    const ObSchemaManagerV2 *schema_mgr):
      ObTabletReadV2(plan_context,tablet_manager,ups_rpc_proxy,schema_mgr)
{
  op_tablet_get_merge_.set_is_ups_row(false);
}

ObTabletGetV2::~ObTabletGetV2()
{
}

void ObTabletGetV2::reset()
{
  op_tablet_join_.reset();
  if (NULL != plan_context_)
  {
    plan_context_->reset();
  }
}

int ObTabletGetV2::create_plan()
{
  int ret = OB_SUCCESS;

  ObTabletJoin::TableJoinInfo table_join_info;
  int64_t basic_column_count = common::OB_MAX_COLUMN_NUMBER;
  uint64_t basic_columns[basic_column_count];
  uint64_t table_id = OB_INVALID_ID;
  int64_t data_version = 0;
  bool is_need_incremental_data = true;
  ObCellInfo cell_info;
  int64_t rowkey_cell_count = 0;
  ObSqlGetSimpleParam& sql_get_param = husk_get_.get_get_param();
  network_timeout_ = sql_get_param.get_request_timeout();

  if (NULL == plan_context_ || NULL == tablet_manager_ || NULL == ups_rpc_proxy_ || NULL == schema_mgr_)
  {
    TBSYS_LOG(ERROR, "plan_context=%p tablet_manager=%p ups_rpc_proxy=%p schema_mgr=%p",
        plan_context_, tablet_manager_, ups_rpc_proxy_, schema_mgr_);
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = get_basic_column_and_join_info(
                               sql_get_param, 
                               *schema_mgr_, 
                               basic_columns, 
                               basic_column_count,
                               rowkey_cell_count,
                               table_join_info)))
  {
    TBSYS_LOG(WARN, "fail to get basic column and join info:ret[%d]", ret);
  }
  else
  {
    table_id = husk_get_.get_get_param().get_table_id();
  }

  if (OB_SUCCESS == ret)
  {
    ups_get_param_.reset(true);
    for (int64_t i = 0; OB_SUCCESS == ret && i < sql_get_param.get_row_size(); i ++)
    {
      cell_info.row_key_ = *(sql_get_param[i]);
      cell_info.table_id_ = table_id;
      for (int64_t col_id_idx = 0; OB_SUCCESS == ret && col_id_idx < basic_column_count; col_id_idx ++)
      {
        cell_info.column_id_ = basic_columns[col_id_idx];
        if (OB_SUCCESS != (ret = ups_get_param_.add_cell(cell_info)))
        {
          TBSYS_LOG(WARN, "fail to add cell to get param:ret[%d]", ret);
        }
      }
    }
    //FIXME: bugfix, Same rowkey for 2 rows, should return OB_ERR_PRIMARY_KEY_DUPLICATE
    if ((OB_SUCCESS == ret) && (ups_get_param_.get_row_size() != sql_get_param.get_row_size()))
    {
      // TODO: CS can't send user_error message to ms. need some code
      TBSYS_LOG(USER_ERROR, "Duplicate entry for key 'PRIMARY'");
      ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = op_sstable_get_.open_tablet_manager(tablet_manager_,
            &sql_get_param, basic_columns, basic_column_count, rowkey_cell_count)))
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
    FILL_TRACE_LOG("read only static data[%s]", sql_get_param.get_is_only_static_data() ? "TRUE":"FALSE");
    if (sql_get_param.get_is_only_static_data())
    {
      is_need_incremental_data = false;
      cur_rowkey_op_ = &op_sstable_get_;
    }
    else
    {
      op_sstable_get_.get_tablet_data_version(data_version);
      FILL_TRACE_LOG("request data version[%ld], cs serving data version[%ld]", 
        sql_get_param.get_data_version(), data_version);
      if (sql_get_param.get_data_version() != OB_NEWEST_DATA_VERSION)
      {
        if (sql_get_param.get_data_version() == OB_INVALID_VERSION)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "invalid version");
        }
        else if (sql_get_param.get_data_version() < data_version)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "The request version is not exist: request version[%ld], sstable version[%ld]", sql_get_param.get_data_version(), data_version);
        }
        else if (sql_get_param.get_data_version() == data_version)
        {
          is_need_incremental_data = false;
          cur_rowkey_op_ = &op_sstable_get_;
        }
      }
    }
  }
  // TBSYS_LOG(INFO, "get_req_version=%ld, sstable_version=%ld", sql_get_param.get_data_version(), data_version); // TODO: remove
  if (OB_SUCCESS == ret)
  {
    if (is_need_incremental_data)
    {
      if (OB_SUCCESS != (ret = need_incremental_data(
                             basic_columns, 
                             basic_column_count,
                             table_join_info, 
                             data_version, 
                             sql_get_param.get_data_version(),
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

int ObTabletGetV2::need_incremental_data(
    const uint64_t *basic_columns,
    const uint64_t basic_column_count,
    ObTabletJoin::TableJoinInfo &table_join_info, 
    int64_t start_data_version, 
    int64_t end_data_version,
    int64_t rowkey_cell_count)

{
  int ret = OB_SUCCESS;
  ObUpsMultiGet *op_ups_multi_get = NULL;
  ObMultipleGetMerge *op_tablet_get_merge = NULL;
  ObTabletJoin *op_tablet_join = NULL;
  ObSqlGetSimpleParam& sql_get_param = husk_get_.get_get_param();

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
      version_range.border_flag_.unset_max_value();
      version_range.border_flag_.set_inclusive_end();
    }
    ups_get_param_.set_version_range(version_range);
  }

  // init ups get 
  if (OB_SUCCESS == ret)
  {
    op_ups_multi_get = &op_ups_multi_get_;
    ups_mget_row_desc_.reset();
    if(OB_SUCCESS != (ret = op_ups_multi_get->set_rpc_proxy(ups_rpc_proxy_)))
    {
      TBSYS_LOG(WARN, "ups scan set ups rpc stub fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_ups_multi_get->set_network_timeout(network_timeout_)))
    {
      TBSYS_LOG(WARN, "set ups scan timeout fail:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret =
          ObNewScannerHelper::get_row_desc(ups_get_param_, true, rowkey_cell_count, ups_mget_row_desc_)))
    {
      TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
    }
    else
    {
      ups_get_param_.set_is_read_consistency(sql_get_param.get_is_read_consistency());
      op_ups_multi_get->set_row_desc(ups_mget_row_desc_);
      op_ups_multi_get->set_get_param(ups_get_param_);
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
        op_tablet_join->set_batch_count(join_batch_count_);
        op_tablet_join->set_is_read_consistency(is_read_consistency_);
        op_tablet_join->set_child(0, *op_tablet_get_merge);
        op_tablet_join->set_network_timeout(network_timeout_);
        if (OB_SUCCESS != (ret = op_tablet_join->set_rpc_proxy(ups_rpc_proxy_) ))
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

  return ret;
}

bool ObTabletGetV2::check_inner_stat() const
{
  int ret = true;
  if (NULL == plan_context_ || NULL == tablet_manager_ || NULL == ups_rpc_proxy_ || NULL == schema_mgr_)
  {
    TBSYS_LOG(ERROR, "plan_context=%p tablet_manager=%p ups_rpc_proxy=%p schema_mgr=%p",
        plan_context_, tablet_manager_, ups_rpc_proxy_, schema_mgr_);
    ret = false;
  }
  return ret;
}

int64_t ObTabletGetV2::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != op_root_)
  {
    databuff_printf(buf, buf_len, pos, "TabletGetV2(%s)\n", to_cstring(*op_root_));
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "TabletGetV2()\n");
  }
  return pos;
}

DEFINE_DESERIALIZE(ObTabletGetV2)
{
  int ret = OB_SUCCESS;
  if (NULL == plan_context_)
  {
    TBSYS_LOG(ERROR, "plan_context_ must not null");
    ret = common::OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = husk_get_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "failed to deserialize husk scan, ret=%d", ret);
  }
  else
  {
    const ObSqlGetSimpleParam& get_param = husk_get_.get_get_param();
    plan_context_->table_id_ = get_param.get_table_id();
    plan_context_->cur_rowkey_op_ = this;
    if (OB_SUCCESS != (ret = plan_context_->set_type(GET)))
    {
      TBSYS_LOG(ERROR, "failed to set plan type, ret=%d", ret);
    }
  }
  return ret;
}



