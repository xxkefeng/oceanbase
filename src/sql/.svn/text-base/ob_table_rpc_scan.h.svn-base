/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_rpc_scan.h
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#ifndef _OB_TABLE_RPC_SCAN_H
#define _OB_TABLE_RPC_SCAN_H 1
#include "ob_table_scan.h"
#include "ob_rpc_scan.h"
#include "ob_sql_expression.h"
#include "ob_table_rename.h"
#include "ob_project.h"
#include "ob_filter.h"
#include "ob_scalar_aggregate.h"
#include "ob_merge_groupby.h"
#include "ob_sort.h"
#include "ob_limit.h"
#include "ob_empty_row_filter.h"
#include "ob_sql_context.h"
#include "common/ob_row.h"
#include "common/ob_hint.h"
namespace oceanbase
{
  namespace sql
  {
    class ObTableRpcScan: public ObTableScan
    {
      public:
        ObTableRpcScan();
        virtual ~ObTableRpcScan();
        void set_phy_plan(ObPhysicalPlan *the_plan);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual ObPhyOperatorType get_type() const;

        int init(ObSqlContext *context);

        /**
         * 添加扫描策略建议
         *
         * 用于控制Scan扫描时的参数，如最大并发数、最大内存使用量等，
         * 参考struct ObRpcScanHint
         */
        void set_hint(const common::ObRpcScanHint &hint);

        /**
         * 添加一个需输出的column
         *
         * @note 只有通过复合列结算新生成的列才需要new_column_id
         * @param expr [in] 需输出的列（这个列可能是个复合列的结果）
         *
         * @return OB_SUCCESS或错误码
         */
        int add_output_column(const ObSqlExpression& expr);

        /**
         * 设置table_id
         * @note 只有基本表被重命名的情况才会使两个不相同id，其实两者相同时base_table_id可以给个默认值。
         * @param table_id [in] 输出的table_id
         * @param base_table_id [in] 被访问表的id
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
        int64_t to_string(char* buf, const int64_t buf_len) const;

        void set_rowkey_cell_count(const int64_t rowkey_cell_count)
        {
          rpc_scan_.set_rowkey_cell_count(rowkey_cell_count);
        }

        inline void set_is_skip_empty_row(bool is_skip_empty_row)
        {
          rpc_scan_.set_is_skip_empty_row(is_skip_empty_row);
          is_skip_empty_row_ = is_skip_empty_row;
        }

        inline void set_read_method(int32_t read_method)
        {
          rpc_scan_.set_read_method(read_method);
          read_method_ = read_method;
        }

        inline int create_plan()
        {
          return rpc_scan_.create_plan();
        }

        NEED_SERIALIZE_AND_DESERIALIZE;

      private:
        // disallow copy
        ObTableRpcScan(const ObTableRpcScan &other);
        ObTableRpcScan& operator=(const ObTableRpcScan &other);
      private:
        // data members
        ObRpcScan rpc_scan_;
        ObFilter select_get_filter_;
        ObScalarAggregate scalar_agg_;
        ObMergeGroupBy group_;
        ObSort group_columns_sort_;
        ObLimit limit_;
        ObEmptyRowFilter empty_row_filter_;
        bool has_rpc_;
        bool has_scalar_agg_;
        bool has_group_;
        bool has_group_columns_sort_;
        bool has_limit_;
        bool is_skip_empty_row_;
        int32_t read_method_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLE_RPC_SCAN_H */
