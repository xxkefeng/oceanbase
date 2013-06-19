/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/

#include "ob_simple_column_indexes_builder.h"
#include "ob_scan_column_indexes.h"

#include "common/ob_define.h"
#include "ob_sstable_schema.h"
#include "common/ob_schema.h"
#include "common/ob_row_desc.h"

using namespace oceanbase;
using namespace common;
using namespace sstable;

inline __attribute__((always_inline)) int ObSimpleColumnIndexesBuilder::add_rowkey_columns(
      ObSimpleColumnIndexes &column_indexes)
{
  int iret = OB_SUCCESS;
  int64_t column_size = 0;
  int64_t rowkey_count = 0;
  const ObSSTableSchemaColumnDef* def_array = NULL; 

  if (schema_.is_binary_rowkey_format(table_id_))
  {
    if (NULL == rowkey_info_)
    {
      TBSYS_LOG(ERROR, "binary rowkey sstable NOT set rowkey info, table=%ld", table_id_);
      iret = OB_ERROR;
    }
    else
    {
      rowkey_count = rowkey_info_->get_size();
      for (int32_t i = 0; i < rowkey_count && OB_SUCCESS == iret; ++i)
      {
        // indexs[offset_in_row] = row_desc_offset;
        iret = add_column_offset(column_indexes, rowkey_info_->get_column(i)->column_id_, i);
      }
    }
  }
  else if ( NULL == ( def_array = schema_.get_group_schema(table_id_,
          ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID, column_size)))
  {
    iret = OB_ERROR;
    TBSYS_LOG(INFO, "rowkey column group not exist:table:%ld", table_id_);
  }
  else
  {
    rowkey_count = column_size;
    for (int32_t i = 0; i < column_size && OB_SUCCESS == iret; ++i)
    {
      iret = add_column_offset(column_indexes, def_array[i].column_name_id_,
          def_array[i].rowkey_seq_ - 1);
    }
  }

  return iret;
}

int ObSimpleColumnIndexesBuilder::build_full_row_index(ObSimpleColumnIndexes &column_indexes,
    const GroupIdDesc &group)
{
  // whole row, query whole column group;
  int iret = OB_SUCCESS;
  int64_t column_size = 0;
  int64_t rowkey_count = 0;
  uint64_t column_group_id = OB_INVALID_ID;
  const ObSSTableSchemaColumnDef* def_array = NULL; 

  if (group.seq_ == 0)
  {
    // add rowkey columns in first column group;
    add_rowkey_columns(column_indexes);
  }

  if (OB_SUCCESS != iret)
  {
    TBSYS_LOG(ERROR, "add rowkey columns error, iret=%d", iret);
  }
  else if (NULL == (def_array = schema_.get_group_schema(table_id_, group.id_, column_size)))
  {
    iret = OB_ERROR;
    TBSYS_LOG(ERROR, "find column group def array error.");
  }
  else
  {
    // add every column id in this column group.
    for (int32_t i = 0; i < column_size && OB_SUCCESS == iret; ++i)
    {
      /**
       * if one column belongs to several column group, only the first 
       * column group will handle it. except there is only one column 
       * group. 
       */
      if (group.size_ > 1)
      {
        schema_.find_offset_first_column_group_schema(
            table_id_, def_array[i].column_name_id_, column_group_id);
      }
      else 
      {
        column_group_id = group.id_;
      }
      if (column_group_id == group.id_)
      {
        iret = add_column_offset(column_indexes, def_array[i].column_name_id_, i + rowkey_count);
      }
    }
  }
  return iret;
}

int ObSimpleColumnIndexesBuilder::build_input_col_index(ObSimpleColumnIndexes &column_indexes,
    const GroupIdDesc &group,
    const uint64_t *const column_id_begin, const int64_t column_id_size)
{
  int iret = OB_SUCCESS;
  int64_t index = 0;
  int64_t rowkey_column_count = 0;
  uint64_t current_column_id = OB_INVALID_ID;
  uint64_t column_group_id = OB_INVALID_ID;
  ObRowkeyColumn column;

  if (schema_.is_binary_rowkey_format(table_id_) && NULL != rowkey_info_)
  {
    rowkey_column_count = rowkey_info_->get_size();;
  }
  else if (OB_SUCCESS != (iret = schema_.get_rowkey_column_count(table_id_, rowkey_column_count)))
  {
    TBSYS_LOG(WARN, "get_rowkey_column_count ret=%d, table_id=%ld", iret, table_id_);
  }

  // query columns in current group
  for (int32_t i = 0; i < column_id_size && OB_SUCCESS == iret; ++i)
  {
    current_column_id = column_id_begin[i];
    if (0 == current_column_id || OB_INVALID_ID == current_column_id)
    {
      TBSYS_LOG(ERROR, "input column id =%ld (i=%d) is invalid.", 
          current_column_id, i);
      iret = OB_INVALID_ARGUMENT;
    }
    else if ( schema_.is_binary_rowkey_format(table_id_) && NULL != rowkey_info_
        && OB_SUCCESS == rowkey_info_->get_index(current_column_id, index, column))
    {
      // is binary rowkey column?
      if (0 == group.seq_)
      {
        iret = add_column_offset(column_indexes, current_column_id, index);
      }
    }
    else if ( (index = schema_.find_offset_column_group_schema(
            table_id_, ObSSTableSchemaColumnDef::ROWKEY_COLUMN_GROUP_ID, current_column_id)) >= 0 )
    {
      // is rowkey column?
      if (0 == group.seq_)
      {
        iret = add_column_offset(column_indexes, current_column_id, index);
      }
    }
    else if (!schema_.is_column_exist(table_id_, current_column_id))
    {
      // column id not exist in schema, set to NOT_EXIST_COLUMN
      // return NullType .
      // ATTENTION! set index to -1 presents nonexist.
      if (0 == group.seq_)
      {
        // no need add NotExist column in %current_scan_column_indexes_
        // default is not exist;
        iret = add_column_offset(column_indexes, current_column_id, -1);
      }
    }
    else
    {
      /**
       * if one column belongs to several column group, only the first 
       * column group will handle it. except there is only one column 
       * group. 
       */
      if (group.size_ > 1)
      {
        index = schema_.find_offset_first_column_group_schema(
            table_id_, current_column_id, column_group_id);
      }
      else 
      {
        index = schema_.find_offset_column_group_schema(
            table_id_, group.id_, current_column_id);
        column_group_id = group.id_;
      }
      if (index >= 0 && column_group_id == group.id_)
      {
        iret = add_column_offset(column_indexes, current_column_id, index + rowkey_column_count);
      }
    }

  }
  return iret;
}

int ObSimpleColumnIndexesBuilder::add_column_offset(ObSimpleColumnIndexes &column_indexes,
    const uint64_t column_id, const int64_t offset)
{
  int ret = OB_SUCCESS;
  if (row_desc_ && OB_SUCCESS != (ret = row_desc_->add_column_desc(table_id_, column_id)))
  {
    TBSYS_LOG(WARN, "add_column_desc ret=%d, table_id=%ld, column_id=%ld, offset=%ld",
        ret, table_id_, column_id, offset);
  }
  else if (OB_SUCCESS != (ret = column_indexes.add_column(offset)))
  {
    TBSYS_LOG(WARN, "add_column_offset ret=%d, table_id=%ld, column_id=%ld, offset=%ld",
        ret, table_id_, column_id, offset);
  }
  return ret;
}


