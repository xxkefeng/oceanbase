/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_values.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_EXPR_VALUES_H
#define _OB_EXPR_VALUES_H 1
#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_row_store.h"
#include "sql/ob_sql_expression.h"
#include "common/ob_array.h"
#include "common/ob_row_desc_ext.h"
namespace oceanbase
{
  namespace sql
  {
    class ObExprValues: public ObNoChildrenPhyOperator
    {
      public:
        ObExprValues();
        virtual ~ObExprValues();

        int set_row_desc(const common::ObRowDesc &row_desc, const common::ObRowDescExt &row_desc_ext);
        int add_value(const ObSqlExpression &v);
        void reserve_values(int64_t num) {values_.reserve(num);};
        common::ObArray<ObSqlExpression> &get_values() {return values_;};

        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const {return PHY_EXPR_VALUES;}
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        // types and constants
      private:
        // disallow copy
        ObExprValues(const ObExprValues &other);
        ObExprValues& operator=(const ObExprValues &other);
        // function members
        int eval();
      private:
        // data members
        common::ObArray<ObSqlExpression> values_;
        common::ObRowStore row_store_;
        common::ObRowDesc row_desc_;
        common::ObRowDescExt row_desc_ext_;
        common::ObRow row_;
        bool from_deserialize_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_EXPR_VALUES_H */
