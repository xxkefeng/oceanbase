/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_scan.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_SCAN_H
#define _OB_UPS_SCAN_H 1

#include "ob_rowkey_phy_operator.h"
#include "common/ob_string.h"
#include "common/ob_sql_ups_rpc_proxy.h"
#include "common/ob_scan_param.h"
#include "common/ob_range.h"


namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    namespace test
    {
      class ObTabletScanTest_create_plan_not_join_Test;
      class ObTabletScanTest_create_plan_join_Test;
      class ObFakeUpsMultiGet;
    }

    // 用于CS从UPS扫描一批动态数据
    class ObUpsScan: public ObRowkeyPhyOperator
    {
      friend class test::ObTabletScanTest_create_plan_not_join_Test;
      friend class test::ObTabletScanTest_create_plan_join_Test;
      friend class test::ObFakeUpsMultiGet;

      public:
        ObUpsScan();
        virtual ~ObUpsScan();

        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        int set_ups_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy);
        virtual int add_column(const uint64_t &column_id);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        /**
         * 设置要扫描的range
         */
        virtual int set_range(const ObNewRange &range);
        void set_version_range(const ObVersionRange &version_range);
        virtual void reset();

        bool is_result_empty() const;

        inline int set_network_timeout(int64_t network_timeout);
        inline void set_is_read_consistency(bool is_read_consistency)
        {
          is_read_consistency_ = is_read_consistency;
        }

      private:
        // disallow copy
        ObUpsScan(const ObUpsScan &other);
        ObUpsScan& operator=(const ObUpsScan &other);

        int get_next_scan_param(const ObRowkey &last_rowkey, ObScanParam &scan_param);
        int fetch_next(bool first_scan);
        bool check_inner_stat();

      protected:
        // data members
        ObNewRange range_;
        ObNewScanner cur_new_scanner_;
        ObScanParam cur_scan_param_;
        ObUpsRow cur_ups_row_;
        ObRowDesc row_desc_;
        ObStringBuf range_str_buf_;
        ObSqlUpsRpcProxy *rpc_proxy_;
        int64_t network_timeout_;
        int64_t row_counter_;
        bool is_read_consistency_;
    };

    int ObUpsScan::set_network_timeout(int64_t network_timeout)
    {
      int ret = OB_SUCCESS;
      if(network_timeout <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "network_timeout should be positive[%ld]", network_timeout);
      }
      else
      {
        network_timeout_ = network_timeout;
      }
      return ret;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPS_SCAN_H */
