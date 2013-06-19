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
#include "ob_tablet_scan.h"
#include "common/serialization.h"
#include "common/utility.h"
#include "common/ob_cur_time.h"
#include "common/ob_profile_log.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

ObTabletScan::ObTabletScan()
{
  reset();
  op_tablet_scan_merge_.set_is_ups_row(false);
  server_type_ = common::MERGE_SERVER;
}

void ObTabletScan::reset(void)
{
  op_root_ = NULL;
  op_ups_scan_.reset();
  if (op_flag_.has_join_) op_tablet_join_.reset();
  if (op_flag_.has_rename_) op_rename_.clear();
  if (op_flag_.has_filter_) op_filter_.clear();
  if (op_flag_.has_project_) op_project_.clear();
  if (op_flag_.has_scalar_agg_) op_scalar_agg_.reset();
  if (op_flag_.has_group_)
  {
    op_group_columns_sort_.reset();
    op_group_.reset();
  }
  if (op_flag_.has_limit_) op_limit_.clear();
  memset(&op_flag_, 0, sizeof(op_flag_));
}

ObTabletScan::~ObTabletScan()
{
}

bool ObTabletScan::has_incremental_data() const
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

int ObTabletScan::need_incremental_data(
    const uint64_t *basic_columns,
    const uint64_t basic_column_count,
    ObTabletJoin::TableJoinInfo &table_join_info, 
    const ObRowkeyInfo *right_table_rowkey_info,
    int64_t start_data_version, 
    int64_t end_data_version,
    int64_t rowkey_cell_count)

{
  int ret = OB_SUCCESS;
  ObUpsScan *op_ups_scan = NULL;
  ObMultipleScanMerge *op_tablet_scan_merge = NULL;
  ObTabletJoin *op_tablet_join = NULL;
  ObTableRename *op_rename = NULL;
  uint64_t table_id = sql_scan_param_->get_table_id();
  uint64_t renamed_table_id = sql_scan_param_->get_renamed_table_id();
  ObVersionRange version_range;

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
      version_range.end_version_.minor_ = INT16_MAX;
      version_range.border_flag_.unset_max_value();
      version_range.border_flag_.set_inclusive_end();
    }
    op_ups_scan->set_version_range(version_range);
  }

  // init ups scan
  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = op_ups_scan->set_ups_rpc_proxy(rpc_proxy_)))
    {
      TBSYS_LOG(WARN, "ups scan set ups rpc stub fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_ups_scan->set_network_timeout(network_timeout_)))
    {
      TBSYS_LOG(WARN, "set ups scan timeout fail:ret[%d]", ret);
    }
    else
    {
      op_ups_scan->set_is_read_consistency(sql_scan_param_->get_is_read_consistency());
      op_ups_scan->set_server_type(server_type_);
    }
  }

  if(OB_SUCCESS == ret)
  {
    op_ups_scan->set_rowkey_cell_count(rowkey_cell_count);
    if (OB_SUCCESS != (ret = op_ups_scan->set_range(*(sql_scan_param_->get_range()))))
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
      op_tablet_join->set_right_table_rowkey_info(right_table_rowkey_info);
      op_tablet_join->set_batch_count(join_batch_count_);
      op_tablet_join->set_is_read_consistency(is_read_consistency_);
      op_tablet_join->set_child(0, *op_tablet_scan_merge);
      op_tablet_join->set_network_timeout(network_timeout_);
      if (OB_SUCCESS != (ret = op_tablet_join->set_rpc_proxy(rpc_proxy_) ))
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

  if(OB_SUCCESS == ret && renamed_table_id != table_id)
  {
    op_rename = &op_rename_;
    if(OB_SUCCESS != (ret = op_rename->set_table(renamed_table_id, table_id)))
    {
      TBSYS_LOG(WARN, "op_rename set table fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = op_rename->set_child(0, *op_root_)))
    {
      TBSYS_LOG(WARN, "op_rename set child fail:ret[%d]", ret);
    }
    else
    {
      op_root_ = op_rename; 
      op_flag_.has_rename_ = true;
    }
  }
  return ret;
}

bool ObTabletScan::check_inner_stat() const
{
  bool ret = false;

  ret = join_batch_count_ > 0 
  && NULL != sql_scan_param_
  && network_timeout_ > 0
  && NULL != rpc_proxy_;
 
  if (!ret)
  {
    TBSYS_LOG(WARN, "join_batch_count_[%ld], "
    "sql_scan_param_[%p], "
    "network_timeout_[%ld], "
    "rpc_proxy_[%p]",
    join_batch_count_, 
    sql_scan_param_,
    network_timeout_,
    rpc_proxy_);
  }

  return ret;
}

int64_t ObTabletScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != op_root_)
  {
    op_root_->to_string(buf, buf_len);
  }
  return pos;
}

int ObTabletScan::build_sstable_scan_param(
    const uint64_t *basic_columns, const uint64_t count, 
    const int64_t rowkey_cell_count, const ObSqlScanParam &sql_scan_param, 
    sstable::ObSSTableScanParam &sstable_scan_param) const
{
  int ret = OB_SUCCESS;

  sstable_scan_param.set_range(*sql_scan_param.get_range());
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

int ObTabletScan::create_plan(const ObSchemaManagerV2 &schema_mgr)
{
  int ret = OB_SUCCESS;
  sstable::ObSSTableScanParam sstable_scan_param;
  ObTabletJoin::TableJoinInfo table_join_info;
  int64_t basic_column_count = common::OB_MAX_COLUMN_NUMBER;
  uint64_t basic_columns[basic_column_count];
  ObProject *op_project = NULL;
  ObLimit *op_limit = NULL;
  ObFilter *op_filter = NULL;
  int64_t data_version;
  bool is_need_incremental_data = true;
  bool is_plain_query = false;
  int64_t rowkey_cell_count = 0;
  
  if (OB_SUCCESS != (ret = get_basic_column_and_join_info(
                               *sql_scan_param_, 
                               schema_mgr, 
                               basic_columns, 
                               basic_column_count,
                               rowkey_cell_count,
                               table_join_info,
                               is_plain_query)))
  {
    TBSYS_LOG(WARN, "fail to get basic column and join info:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = build_sstable_scan_param(basic_columns, basic_column_count, 
            rowkey_cell_count, *sql_scan_param_, sstable_scan_param)))
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
    if (sql_scan_param_->get_is_only_static_data())
    {
      is_need_incremental_data = false;
      cur_rowkey_op_ = &op_sstable_scan_;
    }
    else
    {
      op_sstable_scan_.get_tablet_data_version(data_version);
      FILL_TRACE_LOG("op_sstable_scan_ open context complete, data version[%ld] , range=%s", 
          data_version, to_cstring(*sql_scan_param_->get_range()));
      if (sql_scan_param_->get_data_version() != OB_NEWEST_DATA_VERSION)
      {
        if (sql_scan_param_->get_data_version() == OB_INVALID_VERSION)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "invalid version");
        }
        else if (sql_scan_param_->get_data_version() < data_version)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "The request version is not exist: request version[%ld], sstable version[%ld]", sql_scan_param_->get_data_version(), data_version);
        }
        else if (sql_scan_param_->get_data_version() == data_version)
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

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = need_incremental_data(
                               basic_columns, 
                               basic_column_count,
                               table_join_info, 
                               rowkey_info,
                               data_version, 
                               sql_scan_param_->get_data_version(),
                               rowkey_cell_count)))
        {
          TBSYS_LOG(WARN, "fail to add ups operator:ret[%d]", ret);
        }
      }
    }
    else
    {
      op_root_ = &op_sstable_scan_;
    }
  }

  if (OB_SUCCESS == ret && sql_scan_param_->has_filter())
  {
    op_filter = &op_filter_;
    op_filter->assign(sql_scan_param_->get_filter());
    if (OB_SUCCESS != (ret = op_filter->set_child(0, *op_root_)))
    {
      TBSYS_LOG(WARN, "fail to set filter child. ret=%d", ret);
    }
    else
    {
      op_root_ = op_filter;
      op_flag_.has_filter_ = true;
    }
  }

  // ObProject is container of query columns and set 
  // ObSSTableScanParam put into ObSSTableScan.
  if (OB_SUCCESS == ret && sql_scan_param_->has_project() && (!is_plain_query))
  {
    op_project = &op_project_;
    op_project->assign(sql_scan_param_->get_project());
    if (OB_SUCCESS != (ret = op_project->set_child(0, *op_root_)))
    {
      TBSYS_LOG(WARN, "fail to set project child. ret=%d", ret);
    }
    else
    {
      op_root_ = op_project;
      op_flag_.has_project_ = true;
    }
  }
  if (OB_SUCCESS == ret 
      && (sql_scan_param_->has_scalar_agg() || sql_scan_param_->has_group()))
  {
    if (sql_scan_param_->has_scalar_agg() && sql_scan_param_->has_group())
    {
      ret = OB_ERR_GEN_PLAN;
      TBSYS_LOG(WARN, "Group operator and scalar aggregate operator"
          " can not appear in TabletScan at the same time. ret=%d", ret);
    }
    else if (sql_scan_param_->has_scalar_agg())
    {
      op_scalar_agg_.assign(sql_scan_param_->get_scalar_agg());
      // add scalar aggregation
      if (OB_SUCCESS != (ret = op_scalar_agg_.set_child(0, *op_root_)))
      {
        TBSYS_LOG(WARN, "Fail to set child of scalar aggregate operator. ret=%d", ret);
      }
      else
      {
        op_root_ = &op_scalar_agg_;
        op_flag_.has_scalar_agg_ = true;
      }
    }
    else if (sql_scan_param_->has_group())
    {
      // add group by
      if (!sql_scan_param_->has_group_columns_sort())
      {
        ret = OB_ERR_GEN_PLAN;
        TBSYS_LOG(WARN, "Physical plan error, group need a sort operator. ret=%d", ret);
      }
      else
      {
        op_group_columns_sort_.assign(sql_scan_param_->get_group_columns_sort());
        op_group_.assign(sql_scan_param_->get_group());
      }
      if (OB_UNLIKELY(OB_SUCCESS != ret))
      {
      }
      else if (OB_SUCCESS != (ret = op_group_columns_sort_.set_child(0, *op_root_)))
      {
        TBSYS_LOG(WARN, "Fail to set child of sort operator. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = op_group_.set_child(0, op_group_columns_sort_)))
      {
        TBSYS_LOG(WARN, "Fail to set child of group operator. ret=%d", ret);
      }
      else
      {
        op_root_ = &op_group_;
        op_flag_.has_group_ = true;
      }
    }
  }
  if (OB_SUCCESS == ret && sql_scan_param_->has_limit())
  {
    op_limit = &op_limit_;
    op_limit->assign(sql_scan_param_->get_limit());
    if (OB_SUCCESS != (ret = op_limit->set_child(0, *op_root_)))
    {
      TBSYS_LOG(WARN, "fail to set limit child. ret=%d", ret);
    }
    else
    {
      op_root_ = op_limit;
      op_flag_.has_limit_ = true;
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



