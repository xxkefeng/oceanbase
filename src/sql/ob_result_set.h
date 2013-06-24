/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_result_set.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RESULT_SET_H
#define _OB_RESULT_SET_H 1
#include "common/ob_row.h"
#include "common/ob_array.h"
#include "common/ob_string_buf.h"
#include "sql/ob_phy_operator.h"
#include "sql/ob_physical_plan.h"
#include "common/utility.h"
#include "common/ob_stack_allocator.h"
#include "sql/ob_basic_stmt.h"
#include "obmysql/ob_mysql_global.h" // for EMySQLFieldType
#include "common/page_arena.h"
namespace oceanbase
{
  namespace sql
  {
    class ObSQLSessionInfo;
    // query result set
    class ObResultSet
    {
      public:
        struct Field
        {
          common::ObString tname_; // table name for display
          common::ObString org_tname_; // original table name
          common::ObString cname_;     // column name for display
          common::ObString org_cname_; // original column name
          common::ObObj type_;      // value type
          int64_t to_string(char *buffer, int64_t length) const;
        };
      public:
        ObResultSet();
        ~ObResultSet();
        /// open and execute the execution plan
        /// @note SHOULD be called for all statement even if there is no result rows
        int open();
        /// get the next result row
        /// @return OB_ITER_END when no more data available
        int get_next_row(const common::ObRow *&row);
        /// close the result set after get all the rows
        int close();
        /// get number of rows affected by INSERT/UPDATE/DELETE
        int64_t get_affected_rows() const;
        /// get warning count during the execution
        int64_t get_warning_count() const;
        /// get statement id
        uint64_t get_statement_id() const;
        /// get the server's error message
        const char* get_message() const;
        /// get the server's error code
        const int get_errcode() const;
        /**
         * get the row description
         * the row desc should have been valid after open() and before close()
         * @pre call open() first
         */
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        /// get the field columns
        const common::ObArray<Field> & get_field_columns() const;
        /// get the param columns
        const common::ObArray<Field> & get_param_columns() const;
        /// get the placeholder of params
        common::ObArray<common::ObObj*> & get_params();
        const common::ObArray<obmysql::EMySQLFieldType> & get_params_type() const;
        /// whether the result is with rows (true for SELECT statement)
        bool is_with_rows() const;
        /// get physical plan
        ObPhysicalPlan* get_physical_plan();
        /// to string
        int64_t to_string(char* buf, const int64_t buf_len) const;
        /// whether the statement is a prepared statment
        bool is_prepare_stmt() const;
        /// whether the statement is SHOW WARNINGS
        bool is_show_warnings() const;
        ObBasicStmt::StmtType get_stmt_type() const;
        ObBasicStmt::StmtType get_inner_stmt_type() const;
        ////////////////////////////////////////////////////////////////
        // the following methods are used by the ob_sql module internally
        /// add a field columns
        int init();
        int reset();
        int add_field_column(const Field & field);
        int add_param_column(const Field & field);
        int pre_assign_params_room(const int64_t& size, common::StackAllocator &alloc);
        int fill_params(const common::ObArray<obmysql::EMySQLFieldType>& types,
                        const common::ObArray<common::ObObj>& values);
        int from_prepared(const ObResultSet& stored_result_set);
        int to_prepare(ObResultSet& other);
        const common::ObString& get_statement_name() const;
        void set_statement_id(const uint64_t stmt_id);
        void set_statement_name(const common::ObString name);
        void set_message(const char* message);
        void set_errcode(int code);
        void set_affected_rows(const int64_t& affected_rows);
        void set_warning_count(const int64_t& warning_count);
        void set_physical_plan(ObPhysicalPlan *physical_plan, bool did_own);
        void fileds_clear();
        void set_stmt_type(ObBasicStmt::StmtType stmt_type);
        void set_inner_stmt_type(ObBasicStmt::StmtType stmt_type);
        int get_param_idx(int64_t param_addr, int64_t &idx);
        void set_session(ObSQLSessionInfo *s);
        ObSQLSessionInfo* get_session();
        void set_ps_transformer_allocator(common::ObArenaAllocator *allocator);
      private:
        // types and constants
        static const int64_t MSG_SIZE = 512;
        static const int64_t SMALL_BLOCK_SIZE = 8*1024LL;
      private:
        // disallow copy
        ObResultSet(const ObResultSet &other);
        ObResultSet& operator=(const ObResultSet &other);
        // function members
      private:
        // data members
        uint64_t statement_id_;
        int64_t affected_rows_; // number of rows affected by INSERT/UPDATE/DELETE
        int64_t warning_count_;
        common::ObString statement_name_;
        char message_[MSG_SIZE]; // null terminated message string
        common::ObArenaAllocator block_allocator_;
        common::ObArray<Field> field_columns_;
        common::ObArray<Field> param_columns_;
        common::ObArray<common::ObObj*> params_;
        common::ObArray<obmysql::EMySQLFieldType> params_type_;
        ObPhysicalPlan *physical_plan_;
        bool own_physical_plan_; // whether the physical plan is mine
        ObBasicStmt::StmtType stmt_type_;
        // for a prepared SELECT, stmt_type_ is T_PREPARE
        // but in perf stat we want inner info, i.e. SELECT.
        ObBasicStmt::StmtType inner_stmt_type_;
        int errcode_;
        ObSQLSessionInfo *my_session_; // The session who owns this result set
        common::ObArenaAllocator *ps_trans_allocator_;
    };

    inline int64_t ObResultSet::Field::to_string(char *buffer, int64_t len) const
    {
      int64_t pos;
      pos = snprintf(buffer, len, "tname: %.*s, org_tname: %.*s, "
                     "cname: %.*s, org_cname, %.*s, type: %s",
                     tname_.length(), tname_.ptr(),
                     org_tname_.length(), org_tname_.ptr(),
                     cname_.length(), cname_.ptr(),
                     org_cname_.length(), org_cname_.ptr(), to_cstring(type_));
      return pos;
    }

    inline ObResultSet::ObResultSet()
      :statement_id_(common::OB_INVALID_ID),
       affected_rows_(0), warning_count_(0),
       block_allocator_(common::ObModIds::OB_SQL_RESULT_SET_DYN),
       field_columns_(SMALL_BLOCK_SIZE, common::ModulePageAllocator(block_allocator_)),
       param_columns_(SMALL_BLOCK_SIZE, common::ModulePageAllocator(block_allocator_)),
       params_(SMALL_BLOCK_SIZE, common::ModulePageAllocator(block_allocator_)),
       params_type_(SMALL_BLOCK_SIZE, common::ModulePageAllocator(block_allocator_)),
       physical_plan_(NULL),
       own_physical_plan_(false),
       stmt_type_(ObBasicStmt::T_NONE),
       errcode_(0),
       my_session_(NULL),
       ps_trans_allocator_(NULL)
    {
      memset(message_, 0, sizeof(message_));
    }

    inline int64_t ObResultSet::get_affected_rows() const
    {
      return affected_rows_;
    }

    inline int64_t ObResultSet::get_warning_count() const
    {
      return warning_count_;
    }

    inline uint64_t ObResultSet::get_statement_id() const
    {
      return statement_id_;
    }

    inline const common::ObString& ObResultSet::get_statement_name() const
    {
      return statement_name_;
    }

    inline const char* ObResultSet::get_message() const
    {
      return message_;
    }

    inline const int ObResultSet::get_errcode() const
    {
      return errcode_;
    }

    inline ObPhysicalPlan* ObResultSet::get_physical_plan()
    {
      return physical_plan_;
    }

    inline void ObResultSet::set_statement_id(const uint64_t stmt_id)
    {
      statement_id_ = stmt_id;
    }

    inline void ObResultSet::set_message(const char* message)
    {
      snprintf(message_, MSG_SIZE, "%s", message);
    }

    inline void ObResultSet::set_errcode(int code)
    {
      errcode_ = code;
    }

    inline int ObResultSet::add_field_column(const ObResultSet::Field & field)
    {
      return field_columns_.push_back(field);
    }

    inline int ObResultSet::add_param_column(const ObResultSet::Field & field)
    {
      return param_columns_.push_back(field);
    }

    inline const common::ObArray<ObResultSet::Field> & ObResultSet::get_field_columns() const
    {
      return field_columns_;
    }

    inline const common::ObArray<ObResultSet::Field> & ObResultSet::get_param_columns() const
    {
      return param_columns_;
    }

    inline common::ObArray<common::ObObj*> & ObResultSet::get_params()
    {
      return params_;
    }

    inline const common::ObArray<obmysql::EMySQLFieldType> & ObResultSet::get_params_type() const
    {
      return params_type_;
    }

    inline bool ObResultSet::is_with_rows() const
    {
      return (field_columns_.count() > 0 && !is_prepare_stmt());
    }

    inline void ObResultSet::set_affected_rows(const int64_t& affected_rows)
    {
      affected_rows_ = affected_rows;
    }

    inline void ObResultSet::set_warning_count(const int64_t& warning_count)
    {
      warning_count_ = warning_count;
    }

    inline int64_t ObResultSet::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      common::databuff_printf(buf, buf_len, pos, "stmt_type=%d ",
                              stmt_type_);
      common::databuff_printf(buf, buf_len, pos, "is_with_rows=%c ",
                              this->is_with_rows()?'Y':'N');
      common::databuff_printf(buf, buf_len, pos, "affected_rows=%ld ",
                              affected_rows_);
      common::databuff_printf(buf, buf_len, pos, "warning_count=%ld ",
                              warning_count_);
      common::databuff_printf(buf, buf_len, pos, "field_count=%ld ",
                              field_columns_.count());
      common::databuff_printf(buf, buf_len, pos, "message=%s ",
                              message_);
      common::databuff_printf(buf, buf_len, pos, "prepared_field_count=%ld ",
                              param_columns_.count());
      common::databuff_printf(buf, buf_len, pos, "prepared_param_count=%ld ",
                              params_.count());
      common::databuff_printf(buf, buf_len, pos, "stmt_id=%lu ",
                              statement_id_);
      common::databuff_printf(buf, buf_len, pos, "stmt_name=%.*s",
                              statement_name_.length(), statement_name_.ptr());
      return pos;
    }

    inline int ObResultSet::get_next_row(const common::ObRow *&row)
    {
      OB_ASSERT(physical_plan_);
      errcode_ = physical_plan_->get_main_query()->get_next_row(row);
      return errcode_;
    }

    inline int ObResultSet::close()
    {
      int ret = common::OB_SUCCESS;
      if (OB_UNLIKELY(NULL == physical_plan_))
      {
        if (ObBasicStmt::T_PREPARE != stmt_type_)
        {
          TBSYS_LOG(WARN, "physical_plan not init, stmt_type=%d", stmt_type_);
          ret = common::OB_NOT_INIT;
        }
      }
      else
      {
        ret = physical_plan_->get_main_query()->close();
      }
      set_errcode(ret);
      return ret;
    }
    inline void ObResultSet::set_physical_plan(ObPhysicalPlan *physical_plan, bool did_own)
    {
      physical_plan_ = physical_plan;
      own_physical_plan_ = did_own;
    }

    inline void ObResultSet::fileds_clear()
    {
      affected_rows_ = 0;
      warning_count_ = 0;
      message_[0] = '\0';
      field_columns_.clear();
      param_columns_.clear();
    }

    inline int ObResultSet::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      return physical_plan_->get_main_query()->get_row_desc(row_desc);
    }

    inline bool ObResultSet::is_prepare_stmt() const
    {
      return ObBasicStmt::T_PREPARE == stmt_type_;
    }

    inline void ObResultSet::set_stmt_type(ObBasicStmt::StmtType stmt_type)
    {
      stmt_type_ = stmt_type;
    }

    inline void ObResultSet::set_inner_stmt_type(ObBasicStmt::StmtType stmt_type)
    {
      inner_stmt_type_ = stmt_type;
    }

    inline bool ObResultSet::is_show_warnings() const
    {
      return ObBasicStmt::T_SHOW_WARNINGS == stmt_type_;
    }

    inline void ObResultSet::set_session(ObSQLSessionInfo *s)
    {
      my_session_ = s;
    }

    inline ObSQLSessionInfo* ObResultSet::get_session()
    {
      return my_session_;
    }

    inline ObBasicStmt::StmtType ObResultSet::get_stmt_type() const
    {
      return stmt_type_;
    }

    inline ObBasicStmt::StmtType ObResultSet::get_inner_stmt_type() const
    {
      return inner_stmt_type_;
    }

    inline void ObResultSet::set_ps_transformer_allocator(common::ObArenaAllocator *allocator)
    {
      ps_trans_allocator_ = allocator;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RESULT_SET_H */
