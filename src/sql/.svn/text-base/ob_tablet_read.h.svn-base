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
 *
 */

#ifndef _OB_TABLET_READ_H
#define _OB_TABLET_READ_H 1

#include "ob_no_children_phy_operator.h"
#include "ob_cs_create_plan.h"
#include "ob_cur_rowkey_interface.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTabletRead: public ObNoChildrenPhyOperator,
        public ObCSCreatePlan, public ObCurRowkeyInterface
    {
      protected:
        enum PlanLevel 
        {
          SSTABLE_DATA,
          UPS_DATA,
          JOIN_DATA
        };

      public:
        ObTabletRead();
        virtual ~ObTabletRead() {}

        int open();
        int close();
        int get_next_row(const ObRow *&row);

        int get_row_desc(const common::ObRowDesc *&row_desc) const {row_desc=NULL;return OB_NOT_IMPLEMENT;}

        /// 设置批量做join的个数，需要做join时才生效
        void set_join_batch_count(int64_t join_batch_count)
        {
          join_batch_count_ = join_batch_count;
        }
        void set_network_timeout(int64_t network_timeout)
        {
          network_timeout_ = network_timeout;
        }
        int set_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy);
        inline void set_is_read_consistency(bool is_read_consistency);
        int get_cur_rowkey(const ObRowkey *&rowkey) const;

      protected:
        ObPhyOperator *op_root_;
        bool is_read_consistency_;
        ObSqlUpsRpcProxy *rpc_proxy_;
        int64_t network_timeout_;
        int64_t join_batch_count_;
        ObCurRowkeyInterface *cur_rowkey_op_;
        enum PlanLevel plan_level_;
    };

    inline void ObTabletRead::set_is_read_consistency(bool is_read_consistency)
    {
      is_read_consistency_ = is_read_consistency;
    }
  }
}

#endif /* _OB_TABLET_READ_H */
  

