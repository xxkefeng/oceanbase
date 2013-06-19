/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_get_v2.h 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_TABLET_GET_V2_H
#define _OB_TABLET_GET_V2_H

#include "ob_tablet_read_v2.h"
#include "ob_sstable_get.h"
#include "sql/ob_multiple_get_merge.h"
#include "chunkserver/ob_tablet_manager.h"
#include "ob_tablet_direct_join.h"
#include "ob_husk_tablet_get_v2.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTabletGetV2 : public ObTabletReadV2
    {
      public:
        explicit ObTabletGetV2(ObPlanContext *plan_context,
          chunkserver::ObTabletManager *tablet_manager,
          common::ObSqlUpsRpcProxy* ups_rpc_proxy,
          const ObSchemaManagerV2 *schema_mgr);
        virtual ~ObTabletGetV2();

        int create_plan();
    
        void reset();
        int64_t to_string(char* buf, const int64_t buf_len) const;

        virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
      private:
        int need_incremental_data(const uint64_t *basic_columns, const uint64_t count,
                                  ObTabletJoin::TableJoinInfo &table_join_info, 
                                  int64_t start_data_version, 
                                  int64_t end_data_version,
                                  int64_t rowkey_cell_count);
        bool check_inner_stat() const;

        virtual enum ObPhyOperatorType get_type() const
        {
          return PHY_TABLET_GET_V2;
        }

      private:
        ObHuskTabletGetV2 husk_get_;
        ObSSTableGet op_sstable_get_;
        ObRowDesc ups_mget_row_desc_;

        ObUpsMultiGet op_ups_multi_get_;
        ObMultipleGetMerge op_tablet_get_merge_;
        ObTabletDirectJoin op_tablet_join_;
        ObGetParam ups_get_param_;
    };
  }
}

#endif /* _OB_TABLET_GET_V2_H */
  

