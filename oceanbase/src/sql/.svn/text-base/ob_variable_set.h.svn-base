/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_variable_set.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_VARIABLE_SET_H_
#define OCEANBASE_SQL_OB_VARIABLE_SET_H_
#include "sql/ob_no_children_phy_operator.h"
#include "ob_sql_session_info.h"
#include "common/ob_mutator.h"
#include "common/ob_schema.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRpcProxy;
  } // end namespace mergeserver
  namespace sql
  {
    class ObVariableSet: public ObNoChildrenPhyOperator
    {
      public:
        struct VariableSetNode
        {
          VariableSetNode()
          {
            is_system_variable_ = false;
            is_global_ = false;
          }
          common::ObString variable_name_;
          common::ObObj variable_value_;
          bool is_system_variable_;
          bool is_global_;
        };
        ObVariableSet();
        virtual ~ObVariableSet();

        void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc);
        void set_rowkey_info(const common::ObRowkeyInfo &rowkey_info);
        void set_table_id(const uint64_t& id);
        void set_name_cid(const uint64_t& id);
        void set_type_column(const uint64_t& id, const common::ObObjType& type);
        void set_value_column(const uint64_t& id, const common::ObObjType& type);
        int add_variable_node(const VariableSetNode& node);

        /// execute the prepare statement
        virtual int open();
        virtual int close();
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        /// @note always return OB_ITER_END
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

      private:
        // disallow copy
        ObVariableSet(const ObVariableSet &other);
        ObVariableSet& operator=(const ObVariableSet &other);
        // function members
        int process_variables_set();
        int set_autocommit();
        int clear_autocommit();
      private:
        // data members
        common::ObArray<VariableSetNode> variable_nodes_;
        mergeserver::ObMergerRpcProxy* rpc_;
        uint64_t table_id_;
        // rowkeys of __all_sys_params
        common::ObRowkeyInfo rowkey_info_;
        uint64_t name_cid_;
        uint64_t type_cid_;
        common::ObObjType type_type_;
        uint64_t value_cid_;
        common::ObObjType value_type_;
        common::ObMutator mutator_;
    };

    inline int ObVariableSet::add_variable_node(const VariableSetNode& node)
    {
      return variable_nodes_.push_back(node);
    }
    inline void ObVariableSet::set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc)
    {
      rpc_ = rpc;
    }
    inline void ObVariableSet::set_rowkey_info(const common::ObRowkeyInfo &rowkey_info)
    {
      rowkey_info_ = rowkey_info;
    }
    inline void ObVariableSet::set_table_id(const uint64_t& id)
    {
      table_id_ = id;
    }
    inline void ObVariableSet::set_name_cid(const uint64_t& id)
    {
      name_cid_ = id;
    }
    inline void ObVariableSet::set_type_column(const uint64_t& id, const common::ObObjType& type)
    {
      type_cid_ = id;
      type_type_ = type;
    }
    inline void ObVariableSet::set_value_column(const uint64_t& id, const common::ObObjType& type)
    {
      value_cid_ = id;
      value_type_ = type;
    }
    inline int ObVariableSet::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }
    inline int ObVariableSet::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_VARIABLE_SET_H_ */
