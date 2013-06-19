/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_scan_v2.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_SCAN_V2_H
#define _OB_TABLET_SCAN_V2_H 1

#include "common/ob_schema.h"
#include "common/hash/ob_hashset.h"
#include "chunkserver/ob_tablet_manager.h"
#include "ob_sstable_scan.h"
#include "ob_ups_scan.h"
#include "ob_multiple_scan_merge.h"
#include "ob_tablet_direct_join.h"
#include "ob_tablet_cache_join.h"
#include "ob_sql_expression.h"
#include "ob_sql_scan_simple_param.h"
#include "ob_tablet_read_v2.h"
#include "ob_husk_tablet_scan_v2.h"

//TODO: move to chunkserver/...

namespace oceanbase
{
  namespace sql
  {
    class ObTabletScanV2: public ObTabletReadV2
    {
      struct OpFlag
      {
        uint64_t has_join_ : 1;
        uint64_t reserved_ : 63;
      };

      public:
        explicit ObTabletScanV2(ObPlanContext *plan_context,
          chunkserver::ObTabletManager *tablet_manager,
          common::ObSqlUpsRpcProxy* ups_rpc_proxy,
          const ObSchemaManagerV2 *schema_mgr);
        virtual ~ObTabletScanV2();
        void reset(void);
        inline int get_tablet_range(ObNewRange& range);
        virtual int create_plan();
        bool has_incremental_data() const;
        inline void set_sql_scan_param(const ObSqlScanSimpleParam &sql_scan_param);
        int build_scan_context(void);

        int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual enum ObPhyOperatorType get_type() const
        {
          return PHY_TABLET_SCAN_V2;
        }
      private:
        // disallow copy
        ObTabletScanV2(const ObTabletScanV2 &other);
        ObTabletScanV2& operator=(const ObTabletScanV2 &other);

        bool check_inner_stat() const;

        int need_incremental_data(
            const uint64_t *basic_columns,
            const uint64_t basic_column_count,
            ObTabletJoin::TableJoinInfo &table_join_info, 
            int64_t start_data_version, 
            int64_t end_data_version,
            int64_t rowkey_cell_count);

        int build_sstable_scan_param(
            const uint64_t *basic_columns, 
            const uint64_t count,  
            const int64_t  rowkey_cell_count,
            const ObSqlScanSimpleParam &sql_scan_param, 
            sstable::ObSSTableScanParam &sstable_scan_param) const;
        virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos);

      private:
        ObHuskTabletScanV2 husk_scan_;
        // data members
        ScanContext scan_context_;
        ObSSTableScan op_sstable_scan_;

        /* operator maybe used */
        ObUpsScan op_ups_scan_;
        ObMultipleScanMerge op_tablet_scan_merge_;
        ObTabletDirectJoin op_tablet_join_;

        OpFlag op_flag_;
    };

    int ObTabletScanV2::get_tablet_range(ObNewRange& range) 
    { 
      int ret = OB_SUCCESS;
      ret = op_sstable_scan_.get_tablet_range(range);
      return ret;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLET_SCAN_V2_H */
