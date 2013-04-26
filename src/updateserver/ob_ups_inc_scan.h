/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_UPDATESERVER_OB_INC_SCAN_IMPL_H__
#define __OB_UPDATESERVER_OB_INC_SCAN_IMPL_H__
#include "sql/ob_inc_scan.h"
#include "ob_table_list_query.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    class ObRowDescPrepare : public RowkeyInfoCache
    {
      public:
        ObRowDescPrepare() {};
        virtual ~ObRowDescPrepare() {};
      protected:
        int set_rowkey_size(ObUpsTableMgr* table_mgr, ObRowDesc* row_desc);
    };

    class ObIncGetIter:  public sql::ObPhyOperator, public ObTableListQuery, public ObRowDescPrepare
    {
      public:
        ObIncGetIter(): lock_flag_(sql::LF_NONE), get_param_(NULL), result_(NULL), last_cell_idx_(0)
        {}
      public:
        int open(BaseSessionCtx* session_ctx, ObUpsTableMgr* table_mgr,
                 const common::ObGetParam* get_param, const sql::ObLockFlag lock_flag, sql::ObPhyOperator*& result);
        int open(){ return common::OB_SUCCESS; }
        int close();
        int set_child(int32_t child_idx, sql::ObPhyOperator &child_operator)
        {
          int err = OB_NOT_IMPLEMENT;
          UNUSED(child_idx);
          UNUSED(child_operator);
          return err;
        }
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const { return snprintf(buf, buf_len, "%s", "inc_get_iter"); }
      private:
        common::ObRowDesc row_desc_;
        sql::ObLockFlag lock_flag_;
        const common::ObGetParam* get_param_;
        ObPhyOperator* result_;
        int64_t last_cell_idx_;
        ObSingleTableGetQuery get_query_;
    };

    class ObIncScanIter:  public ObTableListQuery, public ObRowDescPrepare
    {
      public:
        int open(BaseSessionCtx* session_ctx, ObUpsTableMgr* table_mgr,
                 const common::ObScanParam* scan_param, sql::ObPhyOperator*& result);
      private:
        common::ObRowDesc row_desc_;
        ObSingleTableScanQuery scan_query_;
    };

    class ObUpsIncScan: public sql::ObIncScan
    {
      public:
        ObUpsIncScan(BaseSessionCtx& session_ctx): session_ctx_(session_ctx), result_(NULL)
        {}
        virtual ~ObUpsIncScan() {};
      public:
        int open();
        int close();
        int64_t to_string(char* buf, const int64_t buf_len) const;
      public:
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
      protected:
        virtual ObUpsTableMgr* get_table_mgr(); // for test
      private:
        BaseSessionCtx& session_ctx_;
        sql::ObPhyOperator* result_;
        ObIncGetIter get_iter_;
        ObIncScanIter scan_iter_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_INC_SCAN_IMPL_H__ */
