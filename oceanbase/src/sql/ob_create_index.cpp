/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_create_index.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "common/ob_privilege.h"
#include "ob_create_index.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObCreateIndex::ObCreateIndex()
{
}

ObCreateIndex::~ObCreateIndex()
{
}

int ObCreateIndex::open()
{
  int ret = OB_SUCCESS;
  if (NULL == rpc_ || 0 >= strlen(table_schema_.table_name_))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "not init, rpc_=%p", rpc_);
  }
  // FIX ME, show call create_index
  else if (OB_SUCCESS != (ret = rpc_->create_table(false, table_schema_)))
  {
    TBSYS_LOG(WARN, "failed to create index, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "create index succ, index_name=%s", table_schema_.table_name_);
  }
  return ret;
}

int ObCreateIndex::close()
{
  return OB_SUCCESS;
}

int64_t ObCreateIndex::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "CreateIndex(");
  databuff_printf(buf, buf_len, pos, "index_name=%s, ", table_schema_.table_name_);
  databuff_printf(buf, buf_len, pos, "compress_func_name=%s, ", table_schema_.compress_func_name_);
  databuff_printf(buf, buf_len, pos, "table_id=%lu, ", table_schema_.table_id_);
  // we didn't set these type, so just show the type number here
  databuff_printf(buf, buf_len, pos, "table_type=%d, ", table_schema_.table_type_);
  databuff_printf(buf, buf_len, pos, "load_type=%d, ", table_schema_.load_type_);
  databuff_printf(buf, buf_len, pos, "table_def_type=%d, ", table_schema_.table_def_type_);
  databuff_printf(buf, buf_len, pos, "is_use_bloomfilter=%s, ", table_schema_.is_use_bloomfilter_ ? "TRUE" : "FALSE");
  databuff_printf(buf, buf_len, pos, "write_sstable_version=%ld, ", table_schema_.merge_write_sstable_version_);
  databuff_printf(buf, buf_len, pos, "is_pure_update_table=%s, ", table_schema_.is_pure_update_table_ ? "TRUE" : "FALSE");
  databuff_printf(buf, buf_len, pos, "rowkey_split=%ld, ", table_schema_.rowkey_split_);
  databuff_printf(buf, buf_len, pos, "rowkey_column_num=%d, ", table_schema_.rowkey_column_num_);
  databuff_printf(buf, buf_len, pos, "replica_num=%d, ", table_schema_.replica_num_);
  databuff_printf(buf, buf_len, pos, "max_used_column_id=%ld, ", table_schema_.max_used_column_id_);
  databuff_printf(buf, buf_len, pos, "create_mem_version=%ld, ", table_schema_.create_mem_version_);
  databuff_printf(buf, buf_len, pos, "tablet_max_size=%ld, ", table_schema_.tablet_max_size_);
  databuff_printf(buf, buf_len, pos, "charset_number=%d, ", table_schema_.charset_number_);
  databuff_printf(buf, buf_len, pos, "tablet_block_size_=%ld, ", table_schema_.tablet_block_size_);
  databuff_printf(buf, buf_len, pos, "max_rowkey_length=%ld, ", table_schema_.max_rowkey_length_);
  databuff_printf(buf, buf_len, pos, "create_time_column_id=%lu, ", table_schema_.create_time_column_id_);
  databuff_printf(buf, buf_len, pos, "modify_time_column_id_=%lu, ", table_schema_.modify_time_column_id_);

  databuff_printf(buf, buf_len, pos, "columns=[");
  for (int64_t i = 0; i < table_schema_.columns_.count(); ++i)
  {
    const ColumnSchema& col = table_schema_.columns_.at(i);
    databuff_printf(buf, buf_len, pos, "(column_name=%s, ", col.column_name_);
    databuff_printf(buf, buf_len, pos, "column_id_=%lu, ", col.column_id_);
    databuff_printf(buf, buf_len, pos, "column_group_id=%lu, ", col.column_group_id_);
    databuff_printf(buf, buf_len, pos, "rowkey_id=%ld, ", col.rowkey_id_);
    databuff_printf(buf, buf_len, pos, "join_table_id=%lu, ", col.join_table_id_);
    databuff_printf(buf, buf_len, pos, "join_column_id=%lu, ", col.join_column_id_);
    databuff_printf(buf, buf_len, pos, "data_type=%d, ", col.data_type_);
    databuff_printf(buf, buf_len, pos, "data_length_=%ld, ", col.data_length_);
    databuff_printf(buf, buf_len, pos, "data_precision=%ld, ", col.data_precision_);
    databuff_printf(buf, buf_len, pos, "nullable=%s, ", col.nullable_ ? "TRUE" : "FALSE");
    databuff_printf(buf, buf_len, pos, "length_in_rowkey=%ld, ", col.length_in_rowkey_);
    databuff_printf(buf, buf_len, pos, "gm_create=%ld, ", col.gm_create_);
    databuff_printf(buf, buf_len, pos, "gm_modify=%ld)", col.gm_modify_);
    if (i != table_schema_.columns_.count())
      databuff_printf(buf, buf_len, pos, ", ");
  } // end for

  // No JoinInfo here
  databuff_printf(buf, buf_len, pos, "])\n");

  return pos;
}

