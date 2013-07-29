/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_end_trans.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_END_TRANS_H
#define _OB_END_TRANS_H 1
#include "sql/ob_no_children_phy_operator.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "common/ob_transaction.h"
namespace oceanbase
{
  namespace sql
  {
    class ObEndTrans: public ObNoChildrenPhyOperator
    {
      public:
        ObEndTrans();
        virtual ~ObEndTrans(){};

        void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc){rpc_ = rpc;}
        void set_trans_param(const common::ObTransID &trans_id, bool is_rollback);

        /// execute the insert statement
        virtual int open();
        virtual int close() {return common::OB_SUCCESS;}
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        virtual int get_next_row(const common::ObRow *&row) {UNUSED(row); return common::OB_NOT_SUPPORTED;}
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const {UNUSED(row_desc); return common::OB_NOT_SUPPORTED;}
      private:
        // types and constants
      private:
        // disallow copy
        ObEndTrans(const ObEndTrans &other);
        ObEndTrans& operator=(const ObEndTrans &other);
        // function members
      private:
        // data members
        mergeserver::ObMergerRpcProxy* rpc_;
        ObEndTransReq req_;
    };

    inline ObEndTrans::ObEndTrans()
      :rpc_(NULL)
    {
    }

    inline void ObEndTrans::set_trans_param(const ObTransID &trans_id, bool is_rollback)
    {
      req_.trans_id_ = trans_id;
      req_.rollback_ = is_rollback;
    }

    inline int ObEndTrans::open()
    {
      int ret = OB_SUCCESS;
      req_.trans_id_ = my_phy_plan_->get_result_set()->get_session()->get_trans_id(); // get trans id at runtime to support prepare commit/rollback
      if (!req_.trans_id_.is_valid())
      {
        TBSYS_LOG(WARN, "not in transaction");
      }
      else if (OB_SUCCESS != (ret = rpc_->ups_end_trans(req_)))
      {
        TBSYS_LOG(WARN, "failed to end ups transaction, err=%d trans=%s",
                  ret, to_cstring(req_));
        if (OB_TRANS_ROLLBACKED == ret)
        {
          TBSYS_LOG(USER_ERROR, "transaction is rolled back");
        }
        // reset transaction id
        ObTransID invalid_trans;
        my_phy_plan_->get_result_set()->get_session()->set_trans_id(invalid_trans);
      }
      else
      {
        // reset transaction id
        ObTransID invalid_trans;
        my_phy_plan_->get_result_set()->get_session()->set_trans_id(invalid_trans);
      }
      return ret;
    }

    inline int64_t ObEndTrans::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "EndTrans(trans_id=%s, rollback=%c)\n",
                      to_cstring(req_.trans_id_), req_.rollback_?'Y':'N');
      return pos;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_END_TRANS_H */
