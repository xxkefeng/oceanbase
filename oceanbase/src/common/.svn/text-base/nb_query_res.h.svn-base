/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * nb_query_res.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _NB_QUERY_RES_H
#define _NB_QUERY_RES_H 1

#include "common/ob_scanner.h"
#include "common/ob_scan_param.h"
#include "common/ob_get_param.h"
#include "common/ob_object.h"
#include "common/ob_rowkey.h"
#include "common/ob_simple_condition.h"
#include "common/ob_easy_array.h"
#include "sql/ob_sql_result_set.h"

namespace oceanbase
{
  namespace common
  {

#define EXTRACT_INT_FIELD(reader, row, column, field, type) \
    if(OB_SUCCESS == ret) \
    { \
      const ObObj * value = NULL; \
      int64_t int_value = 0; \
      ObString column_str; \
      column_str.assign_ptr(const_cast<char*>(column), static_cast<int32_t>(strlen(column))); \
      if (OB_SUCCESS != (ret = reader.get_column(row, column_str, value))  \
          || NULL == value )\
      { \
        TBSYS_LOG(WARN, "get column [%s] in row [%s] ret=%d", \
            column, to_cstring(row), ret); \
      } \
      else if (value->get_type() != ObIntType && value->get_type() != ObNullType ) \
      { \
        TBSYS_LOG(WARN, "get Int value [%s] type not match.", to_cstring(*value)); \
        ret = OB_ERR_UNEXPECTED ; \
      } \
      else if(value->get_type() == ObIntType) \
      { \
        value->get_int(int_value); \
        field = static_cast<type>(int_value); \
        TBSYS_LOG(DEBUG, "get cell info:column[%s], value[%ld]", column, int_value); \
      } \
      else if (value->get_type() == ObNullType) \
      { \
        field = static_cast<type>(0); \
        TBSYS_LOG(WARN, "get cell value null:column[%s]", column); \
      } \
    }

#define EXTRACT_VARCHAR_FIELD(reader, row, column, str_value) \
    if(OB_SUCCESS == ret) \
    { \
      const ObObj * value = NULL; \
      ObString column_str; \
      column_str.assign_ptr(const_cast<char*>(column), static_cast<int32_t>(strlen(column))); \
      if (OB_SUCCESS != (ret = reader.get_column(row, column_str, value))  \
          || NULL == value )\
      { \
        TBSYS_LOG(WARN, "get column [%s] in row [%s] ret=%d", \
            column, to_cstring(row), ret); \
      } \
      else if (value->get_type() != ObVarcharType) \
      { \
        TBSYS_LOG(WARN, "get Varchar value [%s] type not match.", to_cstring(*value)); \
        ret = OB_ERR_UNEXPECTED; \
      } \
      else  \
      { \
        value->get_varchar(str_value); \
      } \
    }

#define EXTRACT_STRBUF_FIELD(reader, row, column, field, max_length) \
    if(OB_SUCCESS == ret) \
    { \
      ObString str_value; \
      EXTRACT_VARCHAR_FIELD(reader, row, column, str_value); \
      if (OB_SUCCESS == ret) \
      { \
        if(str_value.length() >= max_length) \
        { \
          ret = OB_SIZE_OVERFLOW; \
          TBSYS_LOG(WARN, "field max length is not enough:max_length[%ld], str length[%d]", \
              max_length, str_value.length()); \
        } \
        else \
        { \
          memcpy(field, str_value.ptr(), str_value.length()); \
          field[str_value.length()] = '\0'; \
        } \
      } \
    }

#define EXTRACT_CREATE_TIME_FIELD(reader, row, column, field, type) \
    if(OB_SUCCESS == ret) \
    { \
      const ObObj * value = NULL; \
      ObCreateTime time_value = 0; \
      ObString column_str; \
      column_str.assign_ptr(const_cast<char*>(column), static_cast<int32_t>(strlen(column))); \
      if (OB_SUCCESS != (ret = reader.get_column(row, column_str, value))  \
          || NULL == value )\
      { \
        TBSYS_LOG(WARN, "get column [%s] in row [%s] ret=%d", \
            column, to_cstring(row), ret); \
      } \
      else if (value->get_type() != ObCreateTimeType) \
      { \
        TBSYS_LOG(WARN, "get CreateTime value [%s] type not match.", to_cstring(*value)); \
        ret = OB_ERR_UNEXPECTED; \
      } \
      else  \
      { \
        value->get_createtime(time_value); \
        field = static_cast<type>(time_value); \
      } \
    }

#define EXTRACT_MODIFY_TIME_FIELD(reader, row, column, field, type) \
    if(OB_SUCCESS == ret) \
    { \
      const ObObj * value = NULL; \
      ObModifyTime time_value = 0; \
      ObString column_str; \
      column_str.assign_ptr(const_cast<char*>(column), static_cast<int32_t>(strlen(column))); \
      if (OB_SUCCESS != (ret = reader.get_column(row, column_str, value))  \
          || NULL == value )\
      { \
        TBSYS_LOG(WARN, "get column [%s] in row [%s] ret=%d", \
            column, to_cstring(row), ret); \
      } \
      else if (value->get_type() != ObModifyTimeType) \
      { \
        TBSYS_LOG(WARN, "get ModifyTime value [%s] type not match.", to_cstring(*value)); \
        ret = OB_ERR_UNEXPECTED; \
      } \
      else  \
      { \
        value->get_modifytime(time_value); \
        field = static_cast<type>(time_value); \
      } \
    }


    class ObScanHelper;
    class ObRow;

    typedef EasyArray<const char*> SC;

    class SQLQueryResultReader
    {
      public:
        SQLQueryResultReader();
        ~SQLQueryResultReader();
      public:
        int reset();
        sql::ObSQLResultSet& get_result_set() { return result_set_; }
        const sql::ObSQLResultSet& get_result_set() const { return result_set_; }

        // call next function must prepare first;
        int get_next_row(common::ObRow &row) ;
        int get_column(const common::ObRow &row, const ObString &column_name, const ObObj* &value) const;
        int query(common::ObScanHelper& proxy, const char* sql);
        int query_one_row(common::ObScanHelper& proxy, const char* sql, common::ObRow &row);
      private:
        int prepare();
      private:
        sql::ObSQLResultSet result_set_; 
        hash::ObHashMap<ObString,int64_t, hash::NoPthreadDefendMode> cell_map_;  // column name -> index maps;
        ObStringBuf string_buf_;
    };


    int build_condition_expr(const char* field_name, const ObObj & value,
        char* buf, const int64_t buf_len, int64_t & pos);
    int build_where_condition(const SC& field_list, const ObObj* values,
        char* buf, const int64_t buf_len, int64_t & pos);
    int build_where_condition(const SC& field_list, const ObRow & values,
        char* buf, const int64_t buf_len, int64_t & pos);
    int build_select_list(const SC& field_list, 
        char* buf, const int64_t buf_len, int64_t & pos);
    int build_select_stmt(
        const ObString& table_name, const SC& field_list, 
        const SC& cond_columns, const ObObj* values,
        char* buf, const int64_t buf_len, int64_t & pos);
    int build_select_stmt(
        const ObString& table_name, const SC& field_list, 
        const SC& cond_columns, const ObRow& values,
        char* buf, const int64_t buf_len, int64_t & pos);
    int build_select_stmt(
        const ObString& table_name, const SC& field_list, 
        const char* single_column_key, const ObObj& value,
        char* buf, const int64_t buf_len, int64_t & pos);
    int build_delete_stmt(
        const ObString& table_name,  
        const SC& cond_columns, const ObRow& values,
        char* buf, const int64_t buf_len, int64_t & pos);

  }
}

#endif /* _NB_QUERY_RES_H */

