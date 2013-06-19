/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rpc_scan.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_RPC_SCAN_H
#define _OB_RPC_SCAN_H 1
#include "ob_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/ob_row.h"
#include "common/ob_hint.h"
#include "common/ob_schema.h"
#include "common/location/ob_tablet_location_cache_proxy.h"
#include "common/ob_string_buf.h"
#include "sql/ob_sql_scan_param.h"
#include "sql/ob_sql_get_param.h"
#include "sql/ob_sql_context.h"
#include "mergeserver/ob_ms_scan_param.h"
#include "mergeserver/ob_ms_sql_scan_request.h"
#include "mergeserver/ob_ms_sql_get_request.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "mergeserver/ob_merge_server_service.h"
#include "sql/ob_sql_read_strategy.h"
#include "sql/ob_sql_plan_param.h"

namespace oceanbase
{
  namespace sql
  {
    // 用于MS进行全表扫描
    class ObRpcScan : public ObPhyOperator
    {
      public:
        ObRpcScan();
        virtual ~ObRpcScan();

        int set_child(int32_t child_idx, ObPhyOperator &child_operator)
        {
          UNUSED(child_idx);
          UNUSED(child_operator);
          return OB_ERROR;
        }
        int init(ObSqlContext *context);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        void set_hint(const common::ObRpcScanHint &hint);
        int create_plan();
        /**
         * 添加一个需输出的column
         *
         * @note 只有通过复合列结算新生成的列才需要new_column_id
         * @param expr [in] 需输出的列（这个列可能是个复合列的结果）
         *
         * @return OB_SUCCESS或错误码
         *
         * NOTE: 如果传入的expr是一个条件表达式，本函数将不做检查，需要调用者保证
         */
        int add_output_column(const ObSqlExpression& expr);

        /**
         * 设置table_id
         * @param table_id [in] 被访问表的id
         *
         * @return OB_SUCCESS或错误码
         */
        int set_table(const uint64_t table_id, const uint64_t base_table_id);
        /**
         * 添加一个filter
         *
         * @param expr [in] 过滤表达式
         *
         * @return OB_SUCCESS或错误码
         */
        int add_filter(const ObSqlExpression& expr);
        int add_group_column(const uint64_t tid, const uint64_t cid);
        int add_aggr_column(const ObSqlExpression& expr);

        /**
         * 指定limit/offset
         *
         * @param limit [in]
         * @param offset [in]
         *
         * @return OB_SUCCESS或错误码
         */
        int set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset);

        void set_data_version(int64_t data_version)
        {
          plan_param_.data_version_ = data_version;
        }

        void set_rowkey_cell_count(const int64_t rowkey_cell_count)
        {
          cur_row_desc_.set_rowkey_cell_count(rowkey_cell_count);
        }

        inline void set_is_skip_empty_row(bool is_skip_empty_row)
        {
          plan_param_.is_skip_empty_row_ = is_skip_empty_row;
        }

        inline void set_read_method(int32_t read_method)
        {
          plan_param_.read_method_ = read_method;
        }

        int64_t to_string(char* buf, const int64_t buf_len) const;
      private:
        // disallow copy
        ObRpcScan(const ObRpcScan &other);
        ObRpcScan& operator=(const ObRpcScan &other);
        // member method
        int get_next_compact_row(const common::ObRow*& row);
        // A plan carried most info param info, however, not all info.
        // dynamic info such as Scan Range, Get Rowkeys is 
        // injected to plan at runtime
        // class member layout:
        // ObSqlScanParam {
        //    ObPhysicalPlan *plan_;
        //    ObHuskSSTableScan *sstable_op_ptr_;
        //    ObNewRange whole_scan_range_;
        // };
        //int create_scan_plan(ObSqlScanParam &scan_param);
        //int create_get_plan(ObSqlGetParam &get_param);
      private:
        int cons_row_desc(ObRowDesc &row_desc);
        int cons_get_rows(ObSqlGetSimpleParam &get_param);
        int cons_scan_range(ObSqlScanSimpleParam &scan_param);
      private:
        static const int64_t REQUEST_EVENT_QUEUE_SIZE = 8192;
        ObSqlPlanParam plan_param_;
        ObSqlPlanContext plan_context_;
        mergeserver::ObMsSqlScanRequest sql_scan_request_;
        mergeserver::ObMsSqlGetRequest sql_get_request_;
        ObRow cur_row_;
        // cur_row_desc_ contains selected columns
        // get_row_desc_ contains selected columns and a special column
        // for scan operation, outer and internal can share same row desc
        // but for get operation, can not share them.
        // get has a special column due to GET protocol requirement
        ObRowDesc cur_row_desc_;
        ObRowDesc get_row_desc_;
        int64_t timeout_us_;        
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_SCAN_H */
