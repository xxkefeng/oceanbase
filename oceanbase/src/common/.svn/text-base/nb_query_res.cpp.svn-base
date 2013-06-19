/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * nb_query_res.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "nb_query_res.h"
#include "common/utility.h"
#include "common/roottable/ob_scan_helper.h"

#define COLUMN_NAME_MAP_BUCKET_NUM 100

using namespace oceanbase;
using namespace common;

namespace oceanbase
{
  namespace common
  {
    //--------------------------------------------------------------
    // class SQLQueryResultReader
    SQLQueryResultReader::SQLQueryResultReader()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = cell_map_.create(COLUMN_NAME_MAP_BUCKET_NUM)))
      {
        TBSYS_LOG(WARN, "create hash map fail:ret[%d]", ret);
      }
    }

    SQLQueryResultReader::~SQLQueryResultReader()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = cell_map_.destroy()))
      {
        TBSYS_LOG(WARN, "destroy hash map fail:ret[%d]", ret);
      }
    }

    int SQLQueryResultReader::prepare()
    {
      int ret = OB_SUCCESS;
      ObString tmp_str;
      if (OB_SUCCESS != (ret = cell_map_.clear()))
      {
        TBSYS_LOG(WARN, "clear hash map fail:ret[%d]", ret);
      }
      for (int64_t i = 0; i < result_set_.get_fields().count() && OB_SUCCESS == ret; ++i)
      {
        const sql::ObResultSet::Field & fd = result_set_.get_fields().at(i);
        if (OB_SUCCESS != (ret = string_buf_.write_string(fd.org_cname_, &tmp_str)))
        {
          TBSYS_LOG(WARN, "write_string (%s) ret=%d", to_cstring(fd.org_cname_), ret);
          break;
        }
        else
        {
          ret = cell_map_.set(tmp_str, i);
          if (hash::HASH_EXIST == ret || hash::HASH_INSERT_SUCC == ret)
          {
            ret = OB_SUCCESS;
          }
          else
          {
            TBSYS_LOG(WARN, "add column name failed, colname:%.*s,ret=%d", 
                fd.org_cname_.length(), fd.org_cname_.ptr(), ret);
            break;
          }
        }
      }
      return ret;
    }

    int SQLQueryResultReader::get_next_row(common::ObRow &row) 
    {
      return result_set_.get_new_scanner().get_next_row(row);
    }

    int SQLQueryResultReader::get_column(const common::ObRow &row, const ObString &column_name, const ObObj* &value) const
    {
      int ret = OB_SUCCESS;
      int64_t index = -1;
      uint64_t table_id = 0;
      uint64_t column_id = 0;
      if (hash::HASH_EXIST != (ret = cell_map_.get(column_name, index)))
      {
        TBSYS_LOG(WARN, "column_name:%.*s not exist in map[%ld].",
            column_name.length(), column_name.ptr(), cell_map_.size());
        ret = OB_ERR_UNEXPECTED;
      }
      else if (OB_SUCCESS != (ret = row.raw_get_cell(index, value, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "column_name:%.*s not exist in row.",
            column_name.length(), column_name.ptr());
      }
      return ret;
    }


    int SQLQueryResultReader::query(common::ObScanHelper& proxy, const char* sql)
    {
      int ret = OB_SUCCESS;
      bool fullfilled = true;
      int64_t num = 0;
      if (OB_SUCCESS != (ret = proxy.query(sql, result_set_)))
      {
        TBSYS_LOG(WARN, "execute %s ret=%d", sql, ret);
      }
      else if (OB_SUCCESS != (ret = prepare()))
      {
        TBSYS_LOG(WARN, "internal prepare error=%d", ret);
      }
      else if (OB_SUCCESS != (ret = result_set_.get_new_scanner().get_is_req_fullfilled(fullfilled, num)))
      {
        // always true
      }
      else if (!fullfilled)
      {
        TBSYS_LOG(WARN, "query sql return data(%ld) more than packet(2MB), not supported now.", num);
        ret = OB_NOT_SUPPORTED;
      }
      return ret;
    }

    // query return one row and only one row.
    int SQLQueryResultReader::query_one_row(common::ObScanHelper& proxy, const char* sql, common::ObRow &row)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = query(proxy, sql)))
      {
        TBSYS_LOG(WARN, "query %s ret=%d", sql, ret);
      }
      else if (OB_SUCCESS != (ret = get_next_row(row)))
      {
        TBSYS_LOG(WARN, "cannot fetch one row ret=%d", ret);
      }
      else if (OB_ITER_END != (ret = get_next_row(row)))
      {
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        ret = OB_SUCCESS;
      }
      return ret;
    }




    int build_condition_expr(
        const char* field_name, const ObObj & value, 
        char* buf, const int64_t buf_len, int64_t & pos)
    {
      int ret = OB_SUCCESS;

      int64_t int_value = 0;
      ObString str_val;
      bool is_add = false;

      switch (value.get_type())
      {
        case ObNullType:
          databuff_printf(buf, buf_len, pos, " %s is null ", field_name);
          break;
        case ObIntType:
          value.get_int(int_value, is_add);
          databuff_printf(buf, buf_len, pos, " %s = %ld ", field_name, int_value);
          break;
        case ObVarcharType:
          value.get_varchar(str_val);
          databuff_printf(buf, buf_len, pos, " %s = '%.*s' ", 
              field_name, str_val.length(), str_val.ptr());
          break;
        case ObFloatType:
        case ObDoubleType:
        case ObDateTimeType:
        case ObPreciseDateTimeType:
        case ObUnknownType:
        case ObCreateTimeType:
        case ObModifyTimeType:
        case ObBoolType:
        case ObExtendType:
          ret = OB_NOT_SUPPORTED;
          TBSYS_LOG(WARN, "not support type [%s] now", to_cstring(value));
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          TBSYS_LOG(WARN, "unknown type [%d] now", value.get_type());
          break;
      }
      return ret;
    }

    int build_where_condition(const SC& field_list, const ObRow & row,
        char* buf, const int64_t buf_len, int64_t & pos)
    {

      int ret = OB_SUCCESS;
      const char* field = NULL;
      const ObObj* value = NULL;
      int64_t count = field_list.count();
      for (int i = 0; i < count && OB_SUCCESS == ret; ++i)
      {
        if (OB_SUCCESS != (ret = field_list.at(i, field)) || NULL == field)
        {
          TBSYS_LOG(WARN, "cannot get [%d] field name, ret=%d", i, ret);
        }
        else if (OB_SUCCESS != (ret = row.raw_get_cell(i, value)) || NULL == value)
        {
          TBSYS_LOG(WARN, "cannot get [%d] cell, row:[%s], ret=%d", i, to_cstring(row), ret);
        }
        else if (OB_SUCCESS != (ret = build_condition_expr(field, *value, buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "build condition expr [%d] ret=%d", i, ret);
        }
        else if (i  < count - 1)
        {
          ret = databuff_printf(buf, buf_len, pos, " %s ", "and");
        }
      }
      return ret;
    }

    int build_where_condition(const SC& field_list, const ObObj* values,
        char* buf, const int64_t buf_len, int64_t & pos)
    {
      int ret = OB_SUCCESS;
      const char* field = NULL;
      int64_t count = field_list.count();
      for (int i = 0; i < count && OB_SUCCESS == ret; ++i)
      {
        if (OB_SUCCESS != (ret = field_list.at(i, field)) || NULL == field)
        {
          TBSYS_LOG(WARN, "cannot get [%d] field name, ret=%d", i, ret);
        }
        else if (OB_SUCCESS != (ret = build_condition_expr(field, values[i], buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "build condition expr [%d] ret=%d", i, ret);
        }
        else if (i  < count - 1)
        {
          ret = databuff_printf(buf, buf_len, pos, " %s ", "and");
        }
      }
      return ret;
    }

    int build_select_list(const SC& field_list, 
        char* buf, const int64_t buf_len, int64_t & pos)
    {
      int ret = OB_SUCCESS;
      int64_t count = field_list.count();
      const char* field = NULL;
      for (int i = 0; i < count && OB_SUCCESS == ret; ++i)
      {
        if (OB_SUCCESS != (ret = field_list.at(i, field)) || NULL == field)
        {
          TBSYS_LOG(WARN, "cannot get [%d] field name, ret=%d", i, ret);
        }
        else if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, "%s", field)))
        {
          TBSYS_LOG(WARN, "build select list [%d] ret=%d", i, ret);
        }
        else if (i < count - 1)
        {
          ret = databuff_printf(buf, buf_len, pos, "%s", ",");
        }
      }
      return ret;
    }

    int build_select_stmt(
        const ObString& table_name, const SC& field_list, 
        const SC& cond_columns, const ObRow& values,
        char* buf, const int64_t buf_len, int64_t & pos)
    {
      int ret = 0;
      if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, "%s ", "select")))
      {
        TBSYS_LOG(WARN, "add select keyword ret = %d", ret);
      }
      else if (OB_SUCCESS != (ret = build_select_list(field_list, buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "build_select_list ret = %d", ret);
      }
      else if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, " from %.*s ", 
              table_name.length(), table_name.ptr())))
      {
        TBSYS_LOG(WARN, "build from clause ret = %d", ret);
      }
      else if (cond_columns.count() != 0) 
      {
        if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, " %s ", "where")))
        {
          TBSYS_LOG(WARN, "add where keyword ret = %d", ret);
        }
        else if (OB_SUCCESS != (ret = build_where_condition(cond_columns, values, buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "build where clause ret = %d", ret);
        }
      }
      return ret;
    }

    int build_select_stmt(
        const ObString& table_name, const SC& field_list, 
        const SC& cond_columns, const ObObj* values,
        char* buf, const int64_t buf_len, int64_t & pos)
    {
      int ret = 0;
      if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, "%s ", "select")))
      {
        TBSYS_LOG(WARN, "add select keyword ret = %d", ret);
      }
      else if (OB_SUCCESS != (ret = build_select_list(field_list, buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "build_select_list ret = %d", ret);
      }
      else if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, " from %.*s ", 
              table_name.length(), table_name.ptr())))
      {
        TBSYS_LOG(WARN, "build from clause ret = %d", ret);
      }
      else if (cond_columns.count() != 0) 
      {
        if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, " %s ", "where")))
        {
          TBSYS_LOG(WARN, "add where keyword ret = %d", ret);
        }
        else if (OB_SUCCESS != (ret = build_where_condition(cond_columns, values, buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "build where clause ret = %d", ret);
        }
      }
      return ret;
    }

    int build_select_stmt(
        const ObString& table_name, const SC& field_list, 
        const char* single_column_key, const ObObj& value,
        char* buf, const int64_t buf_len, int64_t & pos)
    {
      int ret = 0;
      if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, "%s ", "select")))
      {
        TBSYS_LOG(WARN, "add select keyword ret = %d", ret);
      }
      else if (OB_SUCCESS != (ret = build_select_list(field_list, buf, buf_len, pos)))
      {
        TBSYS_LOG(WARN, "build_select_list ret = %d", ret);
      }
      else if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, " from %.*s ", 
              table_name.length(), table_name.ptr())))
      {
        TBSYS_LOG(WARN, "build from clause ret = %d", ret);
      }
      else if (NULL != single_column_key) 
      {
        if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, " %s ", "where")))
        {
          TBSYS_LOG(WARN, "add where keyword ret = %d", ret);
        }
        else if (OB_SUCCESS != (ret = build_condition_expr(single_column_key, value, buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "build where clause ret = %d", ret);
        }
      }
      return ret;
    }

    int build_delete_stmt(
        const ObString& table_name,  
        const SC& cond_columns, const ObRow& values,
        char* buf, const int64_t buf_len, int64_t & pos)
    {
      int ret = 0;
      if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, "delete from %.*s ", 
              table_name.length(), table_name.ptr())))
      {
        TBSYS_LOG(WARN, "add delete keyword ret = %d", ret);
      }
      else if (cond_columns.count() != 0) 
      {
        if (OB_SUCCESS != (ret = databuff_printf(buf, buf_len, pos, " %s ", "where")))
        {
          TBSYS_LOG(WARN, "add where keyword ret = %d", ret);
        }
        else if (OB_SUCCESS != (ret = build_where_condition(cond_columns, values, buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "build where clause ret = %d", ret);
        }
      }
      return ret;
    }

  }
}
