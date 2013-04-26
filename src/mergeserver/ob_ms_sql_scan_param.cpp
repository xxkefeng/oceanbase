/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_ms_sql_scan_param.cpp for 
 *
 * Authors:
 *    xiaochu.yh@taobao.com
 */

#include "ob_ms_sql_scan_param.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;


int oceanbase::mergeserver::ObSqlMergerScanParam::set_param(ObSqlScanParam & param)
{
  int ret = OB_SUCCESS;
  decoded_org_param_ = &param;
  
  // implement  ms_scan_param_ = param;
  ms_scan_param_.reset();
  if (OB_SUCCESS != (ret = ms_scan_param_.set_table_id(param.get_renamed_table_id(), param.get_table_id())))
  {
    TBSYS_LOG(WARN, "fail to set table id to ms_scan_param_. ret=%d", ret);
  }
  // deep copy
  else if (OB_SUCCESS != (ret = ms_scan_param_.set_range(*param.get_range(), true)))
  {
    TBSYS_LOG(WARN, "fail to set range to ms_scan_param_. ret=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    ms_scan_param_.set_project(param.get_project());
    ms_scan_param_.set_limit(param.get_limit());
    ms_scan_param_.set_filter(param.get_filter());
    ms_scan_param_.set_scalar_agg(param.get_scalar_agg());
    ms_scan_param_.set_group(param.get_group());
    ms_scan_param_.set_group_columns_sort(param.get_group_columns_sort());
    ms_scan_param_.set_is_read_consistency(param.get_is_read_consistency());
    ms_scan_param_.set_scan_flag(param.get_scan_flag());
  }

  return ret;
}
