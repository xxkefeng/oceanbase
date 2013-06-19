/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_read.h 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *   Hu XU<yongle.xh@alipay.com>
 *
 */

#ifndef _OB_TABLET_READ_V2_H
#define _OB_TABLET_READ_V2_H 1

#include "common/ob_row.h"
#include "common/ob_bitmap.h"
#include "common/ob_sql_ups_rpc_proxy.h"
#include "common/ob_schema.h"
#include "chunkserver/ob_tablet_manager.h"
#include "ob_no_children_phy_operator.h"
#include "ob_cur_rowkey_interface.h"
#include "ob_sql_read_simple_param.h"
#include "ob_tablet_join.h"
#include "ob_plan_context.h"
namespace oceanbase
{
  namespace sql
  {
    class ObTabletReadV2: public ObNoChildrenPhyOperator, public ObCurRowkeyInterface
    {
      protected:
        enum PlanLevel 
        {
          SSTABLE_DATA,
          UPS_DATA,
          JOIN_DATA
        };

      public:
        explicit ObTabletReadV2(ObPlanContext *plan_context,chunkserver::ObTabletManager *tablet_manager,
            common::ObSqlUpsRpcProxy* ups_rpc_proxy, const ObSchemaManagerV2 *schema_mgr);
        virtual ~ObTabletReadV2() {}

        int open();
        int close();
        int get_next_row(const common::ObRow *&row);

        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int create_plan() = 0;

        /// 设置批量做join的个数，需要做join时才生效
        void set_join_batch_count(int64_t join_batch_count)
        {
          join_batch_count_ = join_batch_count;
        }

        void set_network_timeout(int64_t network_timeout)
        {
          network_timeout_ = network_timeout;
        }

        inline void set_is_read_consistency(bool is_read_consistency);
        int get_cur_rowkey(const common::ObRowkey *&rowkey) const;
      protected:
        int get_basic_column_and_join_info(
            const ObSqlReadSimpleParam& param, 
            const common::ObSchemaManagerV2& schema_mgr, 
            uint64_t *basic_columns, 
            int64_t &basic_column_count,
            int64_t &rowkey_cell_count,
            ObTabletJoin::TableJoinInfo &table_join_info);

        int add_unique_column( uint64_t column_id, 
            uint64_t *basic_columns, int64_t &basic_column_count, 
            int64_t &pos, bool &is_duplicate);

      protected:
        ObPhyOperator *op_root_;
        bool is_read_consistency_;
        int64_t network_timeout_;
        int64_t join_batch_count_;
        ObCurRowkeyInterface *cur_rowkey_op_;
        enum PlanLevel plan_level_;
        //max column id is OB_ALL_MAX_COLUMN_ID 
        common::ObBitmap<uint64_t> column_ids_;

        ObPlanContext *plan_context_;
        chunkserver::ObTabletManager *tablet_manager_;
        common::ObSqlUpsRpcProxy *ups_rpc_proxy_;
        const ObSchemaManagerV2 *schema_mgr_;
    };

    inline void ObTabletReadV2::set_is_read_consistency(bool is_read_consistency)
    {
      is_read_consistency_ = is_read_consistency;
    }
  }
}

#endif /* _OB_TABLET_READ_V2_H */
  

