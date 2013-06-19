/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_create_table_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_CREATE_TABLE_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_TABLE_STMT_H_

#include "common/ob_array.h"
#include "common/ob_object.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "sql/ob_basic_stmt.h"
#include "sql/ob_column_def.h"
#include "sql/ob_table_option.h"
#include "parse_node.h"

namespace oceanbase
{
  namespace sql
  {
    class ObCreateTableStmt : public ObBasicStmt
    {
    public:
      explicit ObCreateTableStmt(common::ObStringBuf* name_pool);
      virtual ~ObCreateTableStmt();

      uint64_t gen_column_id();
      void set_if_not_exists(bool if_not_exists);
      int set_table_name(ResultPlan& result_plan, const common::ObString& table_name);
      int set_table_option(ResultPlan& result_plan, const ObTableOption& option);
      int add_primary_key_part(uint64_t primary_key_part);
      int add_primary_key_part(ResultPlan& result_plan, const common::ObString& column_name);
      int add_column_def(ResultPlan& result_plan, const ObColumnDef& column);

      int64_t get_tablet_max_size() const;
      int64_t get_tablet_block_size() const;
      int32_t get_replica_num() const;
      int64_t get_primary_key_size() const;
      int64_t get_column_size() const;
      int32_t get_charset_number() const;
      bool use_bloom_filter() const;
      bool read_static() const;
      bool get_if_not_exists() const;
      const common::ObString& get_table_name() const;
      const common::ObString& get_expire_info() const;
      const common::ObString& get_compress_method() const;
      const common::ObArray<uint64_t>& get_primary_key() const;
      const ObColumnDef& get_column_def(int64_t index) const;
      const ObTableOption& get_table_option();

      virtual void print(FILE* fp, int32_t level, int32_t index = 0);

    protected:
      common::ObStringBuf*        name_pool_;

    private:
      uint64_t                    next_column_id_;
      common::ObString            table_name_;
      common::ObArray<uint64_t>   primay_keys_;
      common::ObArray<ObColumnDef>  columns_;
      bool                        if_not_exists_;
      ObTableOption               table_option_;
      // for future use: create table xxx as select ......
      //ObSelectStmt                *select_clause;
      // create table xxx as already_exist_table, pay attention to whether data are need
    };

    inline uint64_t ObCreateTableStmt::gen_column_id()
    {
      return next_column_id_++;
    }
    inline void ObCreateTableStmt::set_if_not_exists(bool if_not_exists)
    {
      if_not_exists_ = if_not_exists;
    }
    inline  int ObCreateTableStmt::add_primary_key_part(uint64_t primary_key_part)
    {
      return primay_keys_.push_back(primary_key_part);
    }
    inline int64_t ObCreateTableStmt::get_tablet_max_size() const
    {
      return table_option_.tablet_max_size_;
    }
    inline int64_t ObCreateTableStmt::get_tablet_block_size() const
    {
      return table_option_.tablet_block_size_;
    }
    inline int32_t ObCreateTableStmt::get_charset_number() const
    {
      return table_option_.character_set_;
    }
    inline int64_t ObCreateTableStmt::get_primary_key_size() const
    {
      return primay_keys_.count();
    }
    inline int64_t ObCreateTableStmt::get_column_size() const
    {
      return columns_.count();
    }
    inline int32_t ObCreateTableStmt::get_replica_num() const
    {
      return table_option_.replica_num_;
    }
    inline bool ObCreateTableStmt::use_bloom_filter() const
    {
      return table_option_.use_bloom_filter_;
    }
    inline bool ObCreateTableStmt::read_static() const
    {
      return table_option_.read_static_;
    }
    inline bool ObCreateTableStmt::get_if_not_exists() const
    {
      return if_not_exists_;
    }
    inline const common::ObString& ObCreateTableStmt::get_table_name() const
    {
      return table_name_;
    }
    inline const common::ObString& ObCreateTableStmt::get_expire_info() const
    {
      return table_option_.expire_info_;
    }
    inline const common::ObString& ObCreateTableStmt::get_compress_method() const
    {
      return table_option_.compress_method_;
    }
    inline const common::ObArray<uint64_t>& ObCreateTableStmt::get_primary_key() const
    {
      return primay_keys_;
    }
    inline const ObTableOption& ObCreateTableStmt::get_table_option()
    {
      return table_option_;
    }
  }
}

#endif //OCEANBASE_SQL_OB_CREATE_TABLE_STMT_H_

