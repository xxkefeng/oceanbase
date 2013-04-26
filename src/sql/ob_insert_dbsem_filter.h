/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_insert_dbsem_filter.h
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#ifndef _OB_INSERT_DBSEM_FILTER_H
#define _OB_INSERT_DBSEM_FILTER_H 1

#include "ob_single_child_phy_operator.h"
#include "ob_expr_values.h"

namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    namespace test
    {
    }

    // Insert语义过滤器 如果insert的rowkey中有已经存在的 则不能迭代出结果
    class ObInsertDBSemFilter: public ObSingleChildPhyOperator
    {
      public:
        ObInsertDBSemFilter();
        ~ObInsertDBSemFilter();
      public:
        int open();
        int close();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const{return PHY_INSERT_DB_SEM_FILTER;}
        NEED_SERIALIZE_AND_DESERIALIZE;
      public:
        inline ObExprValues &get_values() {return insert_values_;};
      private:
        bool could_insert_;
        ObExprValues insert_values_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_INSERT_DBSEM_FILTER_H */
