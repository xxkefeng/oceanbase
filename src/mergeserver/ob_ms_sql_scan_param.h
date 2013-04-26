/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_ms_sql_scan_param.h for define oceanbase mergeserver scan param operations
 *
 * Authors:
 *   xiaochu.yh@taobao.com
 *
 */
#ifndef MERGESERVER_OB_MS_SQL_SCAN_PARAM_H_ 
#define MERGESERVER_OB_MS_SQL_SCAN_PARAM_H_
#include "sql/ob_sql_scan_param.h"
#include "common/ob_define.h"
namespace oceanbase
{
  namespace mergeserver
  {
    class ObSqlMergerScanParam
    {
    public:
      ObSqlMergerScanParam()
      {
        decoded_org_param_ = NULL;
      }
      ~ObSqlMergerScanParam()
      {
        reset();
      }
      inline oceanbase::sql::ObSqlScanParam * get_cs_param(void)
      {
        return decoded_org_param_;
      }
      const oceanbase::sql::ObSqlScanParam * get_ms_param(void)const
      {
        return &ms_scan_param_;
      }
      inline void reset(void)
      {
        decoded_org_param_ = NULL;
        ms_scan_param_.reset();
      }

      int set_param(oceanbase::sql::ObSqlScanParam & param);
    private:
      oceanbase::sql::ObSqlScanParam  *decoded_org_param_;
      oceanbase::sql::ObSqlScanParam  ms_scan_param_;

    };
  }
}  
#endif /* MERGESERVER_OB_MS_SQL_SCAN_PARAM_H_ */
