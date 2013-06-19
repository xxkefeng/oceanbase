/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_drop_index_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_DROP_INDEX_STMT_H_
#define OCEANBASE_SQL_OB_DROP_INDEX_STMT_H_

#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "sql/ob_basic_stmt.h"
#include "parse_node.h"

namespace oceanbase
{
  namespace sql
  {
    class ObDropIndexStmt : public ObBasicStmt
    {
    public:
      explicit ObDropIndexStmt(common::ObStringBuf* name_pool);
      ObDropIndexStmt();
      virtual ~ObDropIndexStmt();

      void set_name_pool(common::ObStringBuf* name_pool);
      int set_index_name(ResultPlan& result_plan, const common::ObString& index_name);
      const common::ObString& get_index_name() const;
      void print(FILE* fp, int32_t level, int32_t index = 0);

    protected:
      common::ObStringBuf*        name_pool_;

    private:
      common::ObString   index_name_;
    };

    inline void ObDropIndexStmt::set_name_pool(common::ObStringBuf* name_pool)
    {
      name_pool_ = name_pool;
    }
    inline const common::ObString& ObDropIndexStmt::get_index_name() const
    {
      return index_name_;
    }
  }
}

#endif //OCEANBASE_SQL_OB_DROP_INDEX_STMT_H_



