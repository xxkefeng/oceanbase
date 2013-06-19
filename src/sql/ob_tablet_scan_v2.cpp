/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_scan.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_tablet_scan_v2.h"
#include "common/serialization.h"
#include "common/utility.h"
#include "common/ob_cur_time.h"
#include "common/ob_profile_log.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

ObTabletScanV2::ObTabletScanV2(ObPlanContext *plan_context,
  chunkserver::ObTabletManager *tablet_manager,
  common::ObSqlUpsRpcProxy* ups_rpc_proxy,
  const ObSchemaManagerV2 *schema_mgr):
    ObTabletReadV2(plan_context, tablet_manager, ups_rpc_proxy, schema_mgr)
{
  reset();//TODO: need reset?
  op_tablet_scan_merge_.set_is_ups_row(false);
}

void ObTabletScanV2::reset(void)
{
  op_root_ = NULL;
  op_ups_scan_.reset();
  if (op_flag_.has_join_)
  {
    op_tablet_join_.reset();
  }
  memset(&op_flag_, 0, sizeof(op_flag_));
  if (NULL != plan_context_)
  {
    plan_context_->reset();
  }
}

ObTabletScanV2::~ObTabletScanV2()
{
}

int ObTabletScanV2::build_scan_context()
{
  int ret = OB_SUCCESS;
  if (NULL == tablet_manager_)
  {
    TBSYS_LOG(ERROR, "tablet_manager_ must not NULL");
    ret = OB_ERR_UNEXPECTED;
  }
  else
  {
    tablet_manager_->build_scan_context(scan_context_);
  }
  return ret;
}


bool ObTabletScanV2::has_incremental_data() const
{
  bool ret = false;
  switch (plan_level_)
  {
    case SSTABLE_DATA:
      ret = false;
      break;
    case UPS_DATA:
      ret = !op_ups_scan_.is_result_empty();
      break;
    case JOIN_DATA:
      ret = true;
      break;
  }
  return ret;
}

int ObTabletScanV2::need_incremental_data(
    const uint64_t *basic_columns,
    const uint64_t basic_column_count,
    ObTabletJoin::TableJoinInfo &table_join_info, 
    int64_t start_data_version, 
    int64_t end_data_version,
    int64_t rowkey_cell_count)

{
  int ret = OB_SUCCESS;
  ObUpsScan *op_ups_scan = NULL;
  ObMultipleScanMerge *op_tablet_scan_merge = NULL;
  ObTabletJoin *op_tablet_join = NULL;
  ObVersionRange version_range;
  const ObSqlScanSimpleParam& scan_param = husk_scan_.get_scan_param();

  if(OB_SUCCESS == ret)
  {
    op_ups_scan = &op_ups_scan_;
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
    op_ups_scan->set_version_range(version_range);
  }

  // init ups scan
  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = op_ups_scan->set_ups_rpc_proxy(ups_rpc_proxy_)))
    {
      TBSYS_LOG(WARN, "ups scan set ups rpc stub fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_ups_scan->set_network_timeout(network_timeout_)))
    {
      TBSYS_LOG(WARN, "set ups scan timeout fail:ret[%d]", ret);
    }
    else
    {
      op_ups_scan->set_is_read_consistency(scan_param.get_is_read_consistency());
    }
  }

  if(OB_SUCCESS == ret)
  {
    op_ups_scan->set_rowkey_cell_count(rowkey_cell_count);
    if (OB_SUCCESS != (ret = op_ups_scan->set_range(scan_param.get_range())))
    {
      TBSYS_LOG(WARN, "op ups scan set range failed:ret[%d]", ret);
    }
    else
    {
      for (uint64_t i=0;OB_SUCCESS == ret && i < basic_column_count;i++)
      {
        if(OB_SUCCESS != (ret = op_ups_scan->add_column(basic_columns[i])))
        {
          TBSYS_LOG(WARN, "op ups scan add column fail:ret[%d]", ret);
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    op_tablet_scan_merge = &op_tablet_scan_merge_;
    if (OB_SUCCESS == ret)
    {
      cur_rowkey_op_ = op_tablet_scan_merge;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = op_tablet_scan_merge->set_child(0, op_sstable_scan_)))
    {
      TBSYS_LOG(WARN, "set sstable scan fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_tablet_scan_merge->set_child(1, *op_ups_scan)))
    {
      TBSYS_LOG(WARN, "set ups scan fail:ret[%d]", ret);
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
      op_tablet_join->set_version_range(version_range);
      op_tablet_join->set_table_join_info(table_join_info);
      op_tablet_join->set_batch_count(join_batch_count_);
      op_tablet_join->set_is_read_consistency(is_read_consistency_);
      op_tablet_join->set_child(0, *op_tablet_scan_merge);
      op_tablet_join->set_network_timeout(network_timeout_);
      if (OB_SUCCESS != (ret = op_tablet_join->set_rpc_proxy(ups_rpc_proxy_) ))
      {
        TBSYS_LOG(WARN, "fail to set rpc proxy:ret[%d]", ret);
      }
      else
      {
        op_root_ = op_tablet_join;
        plan_level_ = JOIN_DATA;
        op_flag_.has_join_ = true;
      }
    }
    else
    {
      op_root_ = op_tablet_scan_merge;
    }
  }
  return ret;
}

bool ObTabletScanV2::check_inner_stat() const
{
  bool ret = false;

  ret = join_batch_count_ > 0 
  && network_timeout_ > 0
  && NULL != ups_rpc_proxy_
  && NULL != tablet_manager_;
 
  if (!ret)
  {
    TBSYS_LOG(WARN, "join_batch_count_[%ld], "
    "network_timeout_[%ld], "
    "rpc_proxy_[%p]"
    "tablet_manager_[%p]",
    join_batch_count_, 
    network_timeout_,
    ups_rpc_proxy_,
    tablet_manager_);
  }

  return ret;
}

int64_t ObTabletScanV2::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != op_root_)
  {
    databuff_printf(buf, buf_len, pos, "TabletScanV2(%s)\n", to_cstring(*op_root_));
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "TabletScanV2()\n");
  }
  return pos;
}

int ObTabletScanV2::build_sstable_scan_param(
    const uint64_t *basic_columns, const uint64_t count, 
    const int64_t rowkey_cell_count, const ObSqlScanSimpleParam &sql_scan_param, 
    sstable::ObSSTableScanParam &sstable_scan_param) const
{
  int ret = OB_SUCCESS;

  sstable_scan_param.set_range(sql_scan_param.get_range());
  sstable_scan_param.set_is_result_cached(sql_scan_param.get_is_result_cached());
  sstable_scan_param.set_not_exit_col_ret_nop(false);
  sstable_scan_param.set_scan_flag(sql_scan_param.get_scan_flag());
  if (rowkey_cell_count > OB_MAX_ROWKEY_COLUMN_NUMBER)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "rowkey cell count[%ld] should not large than OB_MAX_ROWKEY_COLUMN_NUMBER[%ld]",
        rowkey_cell_count, OB_MAX_ROWKEY_COLUMN_NUMBER);
  }
  else
  {
    sstable_scan_param.set_rowkey_column_count(static_cast<int16_t>(rowkey_cell_count));
  }

  for (uint64_t i=0; OB_SUCCESS == ret && i < count; i++)
  {
    if(OB_SUCCESS != (ret = sstable_scan_param.add_column(basic_columns[i])))
    {
      TBSYS_LOG(WARN, "scan param add column fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObTabletScanV2::create_plan()
{
  int ret = OB_SUCCESS;
  sstable::ObSSTableScanParam sstable_scan_param;
  ObTabletJoin::TableJoinInfo table_join_info;
  int64_t basic_column_count = common::OB_MAX_COLUMN_NUMBER;
  uint64_t basic_columns[basic_column_count];
  int64_t data_version;
  bool is_need_incremental_data = true;
  int64_t rowkey_cell_count = 0;
  const ObSqlScanSimpleParam& scan_param = husk_scan_.get_scan_param();
  network_timeout_ = scan_param.get_request_timeout(); 
  INIT_PROFILE_LOG_TIMER();
  
  if (NULL == plan_context_ || NULL == tablet_manager_ || NULL == ups_rpc_proxy_ || NULL == schema_mgr_)
  {
    TBSYS_LOG(ERROR, "plan_context=%p tablet_manager=%p ups_rpc_proxy=%p schema_mgr=%p",
        plan_context_, tablet_manager_, ups_rpc_proxy_, schema_mgr_);
    ret = OB_NOT_INIT;
  }

  if (OB_SUCCESS == ret)
  {
    tablet_manager_->build_scan_context(scan_context_);
    if (OB_SUCCESS != (ret = get_basic_column_and_join_info(
            scan_param, 
            *schema_mgr_, 
            basic_columns, 
            basic_column_count,
            rowkey_cell_count,
            table_join_info)))
    {
      TBSYS_LOG(WARN, "fail to get basic column and join info:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = build_sstable_scan_param(basic_columns, basic_column_count, 
            rowkey_cell_count, scan_param, sstable_scan_param)))
    {
      TBSYS_LOG(WARN, "build_sstable_scan_param ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = op_sstable_scan_.open_scan_context(sstable_scan_param, scan_context_)))
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
    if (scan_param.get_is_only_static_data())
    {
      is_need_incremental_data = false;
      cur_rowkey_op_ = &op_sstable_scan_;
    }
    else
    {
      op_sstable_scan_.get_tablet_data_version(data_version);
      PROFILE_LOG_TIME(DEBUG, "op_sstable_scan_ open context complete, data version[%ld] , range=%s", 
          data_version, to_cstring(scan_param.get_range()));
      if (scan_param.get_data_version() != OB_NEWEST_DATA_VERSION)
      {
        if (scan_param.get_data_version() == OB_INVALID_VERSION)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "invalid version");
        }
        else if (scan_param.get_data_version() < data_version)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "The request version is not exist: request version[%ld], sstable version[%ld]", scan_param.get_data_version(), data_version);
        }
        else if (scan_param.get_data_version() == data_version)
        {
          is_need_incremental_data = false;
          cur_rowkey_op_ = &op_sstable_scan_;
        }
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (is_need_incremental_data)
    {
      if (OB_SUCCESS != (ret = need_incremental_data(
                             basic_columns, 
                             basic_column_count,
                             table_join_info, 
                             data_version, 
                             scan_param.get_data_version(),
                             rowkey_cell_count)))
      {
        TBSYS_LOG(WARN, "fail to add ups operator:ret[%d]", ret);
      }
    }
    else
    {
      op_root_ = &op_sstable_scan_;
    }
  }

  //release tablet
  if (OB_SUCCESS != ret)
  {
    int err = OB_SUCCESS;
    if (OB_SUCCESS != (err = op_sstable_scan_.close()))
    {
      TBSYS_LOG(WARN, "fail to close op sstable scan:ret[%d]", err);
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObTabletScanV2)
{
  int ret = OB_SUCCESS;
  if (NULL == plan_context_)
  {
    TBSYS_LOG(ERROR, "plan_context_ must not null");
    ret = common::OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = husk_scan_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "failed to deserialize husk scan, ret=%d", ret);
  }
  else
  {
    const ObSqlScanSimpleParam& scan_param = husk_scan_.get_scan_param();
    plan_context_->table_id_ = scan_param.get_table_id();
    plan_context_->cur_rowkey_op_ = this;
    plan_context_->data_range_ = scan_param.get_range();
    if (OB_SUCCESS != (ret = plan_context_->set_type(SCAN)))
    {
      TBSYS_LOG(ERROR, "failed to set plan type, ret=%d", ret);
    }
  }
  return ret;
}



