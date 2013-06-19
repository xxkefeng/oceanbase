/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_start_trans.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_START_TRANS_H
#define _OB_START_TRANS_H 1
#include "sql/ob_no_children_phy_operator.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "sql/ob_sql_session_info.h"
namespace oceanbase
{
  namespace sql
  {
    class ObStartTrans: public ObNoChildrenPhyOperator
    {
      public:
        ObStartTrans();
        virtual ~ObStartTrans(){};

        void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc){rpc_ = rpc;}
        void set_trans_param(TransType type) {req_.type_ = type;};
        /// execute the insert statement
        virtual int open();
        virtual int close() {return common::OB_SUCCESS;}
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        virtual int get_next_row(const common::ObRow *&row) {UNUSED(row); return common::OB_NOT_SUPPORTED;}
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const {UNUSED(row_desc); return common::OB_NOT_SUPPORTED;}
      private:
        // disallow copy
        ObStartTrans(const ObStartTrans &other);
        ObStartTrans& operator=(const ObStartTrans &other);
        // function members
      private:
        // data members
        mergeserver::ObMergerRpcProxy* rpc_;
        common::ObTransReq req_;
    };

    inline ObStartTrans::ObStartTrans()
      :rpc_(NULL)
    {
    }

    inline int64_t ObStartTrans::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "StartTrans(type=%d, isolation=%d)\n",
                      req_.type_, req_.isolation_);
      return pos;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_START_TRANS_H */
