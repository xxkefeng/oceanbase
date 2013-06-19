/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_create_index.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_CREATE_INDEX_H_
#define OCEANBASE_SQL_OB_CREATE_INDEX_H_

#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_schema_service.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRootRpcProxy;
  } // end namespace mergeserver

  namespace sql
  {
    class ObCreateIndex: public ObNoChildrenPhyOperator
    {
      public:
        ObCreateIndex();
        virtual ~ObCreateIndex();
        // init
        void set_rpc_stub(mergeserver::ObMergerRootRpcProxy* rpc);
        common::TableSchema& get_table_schema();

        /// execute the create table statement
        virtual int open();
        virtual int close();
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      private:
        void grant_owner_privilege();
        // disallow copy
        ObCreateIndex(const ObCreateIndex &other);
        ObCreateIndex& operator=(const ObCreateIndex &other);
      private:
        // data members
        // bool  unique_;
        common::TableSchema   table_schema_;
        mergeserver::ObMergerRootRpcProxy* rpc_;
    };

    inline void ObCreateIndex::set_rpc_stub(mergeserver::ObMergerRootRpcProxy* rpc)
    {
      rpc_ = rpc;
    }
    inline common::TableSchema& ObCreateIndex::get_table_schema()
    {
      return table_schema_;
    }
    inline int ObCreateIndex::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }
    inline int ObCreateIndex::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_CREATE_INDEX_H_ */

