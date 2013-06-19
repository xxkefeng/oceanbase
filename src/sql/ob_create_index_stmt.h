/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_create_index_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_CREATE_INDEX_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_INDEX_STMT_H_

#include "common/ob_se_array.h"
#include "common/ob_object.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "sql/ob_basic_stmt.h"
#include "ob_table_option.h"
#include "parse_node.h"

namespace oceanbase
{
  namespace sql
  {
    struct ObColumnSortItem
    {
      enum OrderType
      {
        ASC,
        DESC
      };

      ObColumnSortItem()
      {
        order_type_ = ASC;
      }
      common::ObString column_name_;
      OrderType  order_type_;
    };

    class ObCreateIndexStmt : public ObBasicStmt
    {
    public:
      explicit ObCreateIndexStmt(common::ObStringBuf* name_pool);
      ObCreateIndexStmt();
      virtual ~ObCreateIndexStmt();

      void set_name_pool(common::ObStringBuf* name_pool);
      void set_unique(bool unique);
      int set_table_name(ResultPlan& result_plan, const common::ObString& table_name);
      int set_index_name(ResultPlan& result_plan, const common::ObString& index_name);
      int set_index_option(ResultPlan& result_plan, const ObTableOption& option);
      int add_sort_column(ResultPlan& result_plan, const ObColumnSortItem& sort_column);
      int add_storing_column(ResultPlan& result_plan, const common::ObString& column_name);
      int64_t get_index_column_size() const;
      int64_t get_storing_column_size() const;
      const ObColumnSortItem& get_index_column(int64_t index) const;
      const common::ObString& get_storing_column(int64_t index) const;
      const common::ObString& get_table_name() const;
      const common::ObString& get_index_name() const;
      const ObTableOption& get_index_option() const;
      bool is_unique() const;

      virtual void print(FILE* fp, int32_t level, int32_t index = 0);

    protected:
      common::ObStringBuf*        name_pool_;

    private:
      common::ObString            table_name_;
      common::ObString            index_name_;
      bool                        unique_;
      common::ObSEArray<ObColumnSortItem, common::OB_PREALLOCATED_NUM>  sort_columns_;
      common::ObSEArray<common::ObString, common::OB_PREALLOCATED_NUM>  storing_columns_;

      ObTableOption               index_option_;
    };

    inline void ObCreateIndexStmt::set_name_pool(common::ObStringBuf* name_pool)
    {
      name_pool_ = name_pool;
    }
    inline void ObCreateIndexStmt::set_unique(bool unique)
    {
      unique_ = unique;
    }
    inline const ObTableOption& ObCreateIndexStmt::get_index_option() const
    {
      return index_option_;
    }
    inline bool ObCreateIndexStmt::is_unique() const
    {
      return unique_;
    }
    inline int64_t ObCreateIndexStmt::get_index_column_size() const
    {
      return sort_columns_.count();
    }
    inline int64_t ObCreateIndexStmt::get_storing_column_size() const
    {
      return storing_columns_.count();
    }
    inline const ObColumnSortItem& ObCreateIndexStmt::get_index_column(int64_t index) const
    {
      return sort_columns_.at(index);
    }
    inline const common::ObString& ObCreateIndexStmt::get_storing_column(int64_t index) const
    {
      return storing_columns_.at(index);
    }
    inline const common::ObString& ObCreateIndexStmt::get_table_name() const
    {
      return table_name_;
    }
    inline const common::ObString& ObCreateIndexStmt::get_index_name() const
    {
      return index_name_;
    }
  }
}

#endif //OCEANBASE_SQL_OB_CREATE_INDEX_STMT_H_

