/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_new_scanner_helper.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_new_scanner_helper.h"
#include "utility.h"

using namespace oceanbase;
using namespace common;

int ObNewScannerHelper::print_new_scanner(ObNewScanner &new_scanner, ObRow &row)
{
  int ret = OB_SUCCESS;

  while (OB_SUCCESS == ret)
  {
    ret = new_scanner.get_next_row(row);

    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(INFO, "new scanner row:%s", to_cstring(row));
    }
    else
    {
      TBSYS_LOG(WARN, "new scanner get next row fail");
    }
  }

  return ret;
}

int ObNewScannerHelper::add_cell(ObRow &row, const ObCellInfo &cell, bool is_ups_row)
{ // must make sure action_flag_cell cell of is the last one in the cells of the same row
  int ret = OB_SUCCESS;
  ObObj *action_flag_cell = NULL;
  ObObj *value = NULL;
  
  if (OB_SUCCESS != (ret = row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, action_flag_cell) ))
  {
    TBSYS_LOG(WARN, "fail to get action flag cell:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    if (!is_ups_row) //normal row
    {
      if (ObExtendType == cell.value_.get_type())
      {
        action_flag_cell->set_ext(cell.value_.get_ext());
        switch(cell.value_.get_ext())
        {
          case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
          case ObActionFlag::OP_DEL_ROW:
            if (OB_SUCCESS != (ret = put_rowkey_to_row(row, cell)))
            {
              TBSYS_LOG(WARN, "fail to put rowkey to row:ret[%d]", ret);
            }
            break;
          case ObActionFlag::OP_VALID:
          case ObActionFlag::OP_NEW_ADD:
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            TBSYS_LOG(WARN, "unsupported ext type[%ld]", cell.value_.get_ext());
        }
      }
      else
      {
        if (OB_SUCCESS != (ret = row.get_cell(cell.table_id_, cell.column_id_, value) ))
        {
          TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = value->apply(cell.value_) ))
        {
          TBSYS_LOG(WARN, "fail to apply cell to value:ret[%d]", ret);
        }
        else
        {
          action_flag_cell->set_ext(ObActionFlag::OP_VALID);
        }
      }
    }
    else //ups row
    {
      if (OB_SUCCESS == ret)
      {
        if (ObExtendType != cell.value_.get_type() || ObActionFlag::OP_NOP == cell.value_.get_ext())
        {
          if (action_flag_cell->get_ext() == ObActionFlag::OP_DEL_ROW 
            || action_flag_cell->get_ext() == ObActionFlag::OP_NEW_ADD)
          {
            action_flag_cell->set_ext(ObActionFlag::OP_NEW_ADD);
          }
          else
          {
            action_flag_cell->set_ext(ObActionFlag::OP_VALID);
          }

          if (ObExtendType != cell.value_.get_type())
          {
            if (OB_SUCCESS != (ret = row.get_cell(cell.table_id_, cell.column_id_, value) ))
            {
              TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
            }
            else if (OB_SUCCESS != (ret = value->apply(cell.value_) ))
            {
              TBSYS_LOG(WARN, "fail to apply cell to value:ret[%d]", ret);
            }
          }
        }
        else if (ObActionFlag::OP_DEL_ROW == cell.value_.get_ext())
        {
          action_flag_cell->set_ext(ObActionFlag::OP_DEL_ROW);
          if (OB_SUCCESS != (ret = put_rowkey_to_row(row, cell) ))
          {
            TBSYS_LOG(WARN, "fail to put rowkey to row:ret[%d]", ret);
          }
        }
        else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell.value_.get_ext())
        {
          if (OB_SUCCESS != (ret = put_rowkey_to_row(row, cell) ))
          {
            TBSYS_LOG(WARN, "fail to put rowkey to row:ret[%d]", ret);
          }
        }
        else
        {
          ret = OB_NOT_SUPPORTED;
          TBSYS_LOG(WARN, "unsupport ext type[%ld]", cell.value_.get_ext());
        }
      }
    }
  }

  return ret;
}

int ObNewScannerHelper::put_rowkey_to_row(ObRow &row, const ObCellInfo &cell)
{
  int ret = OB_SUCCESS;
  // put rowkey to row
  const ObRowDesc *row_desc = NULL;
  row_desc = row.get_row_desc();
  if (OB_UNLIKELY(NULL == row_desc))
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "row_desc is null");
  }
  else
  {
    const int64_t rowkey_cell_count = row_desc->get_rowkey_cell_count();
    const int64_t cell_row_key_length = cell.row_key_.length();
    if((rowkey_cell_count > 0 && rowkey_cell_count == cell_row_key_length) ||
       (0 == rowkey_cell_count && cell_row_key_length <= row_desc->get_column_num()))
    {
      for (int64_t i = 0; i < cell_row_key_length; i ++)
      {
        row.raw_set_cell(i, cell.row_key_.ptr()[i]);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "unexpected error: rowkey cell count of row desc: %ld, cell row key length: %ld row desc column num: %ld",
          rowkey_cell_count, cell_row_key_length, row_desc->get_column_num());
    }
  }
  return ret;
}

int ObNewScannerHelper::get_row_desc(const ObScanParam &scan_param, ObRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = scan_param.get_table_id();

  for(int64_t i=0;OB_SUCCESS == ret && i<scan_param.get_column_id_size();i++)
  {
    if(OB_SUCCESS != (ret = row_desc.add_column_desc(table_id, scan_param.get_column_id()[i])))
    {
      TBSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID) ))
    {
      TBSYS_LOG(WARN, "fail to add OB_ACTION_FLAG_COLUMN_ID:ret[%d]", ret);
    }
  }

  //todo 添加其他类型的列

  return ret;
}

int ObNewScannerHelper::get_row_desc(const ObGetParam &get_param, const bool has_flag_column,
     const int64_t rowkey_cell_count, ObRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  ObCellInfo *cell = NULL;

  if(get_param.get_row_size() <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "get param row should be positive:actual[%ld]", get_param.get_row_size());
  }

  if(OB_SUCCESS == ret)
  {
    ObGetParam::ObRowIndex row_index = get_param.get_row_index()[0];
    for(int64_t i=0;OB_SUCCESS == ret && i<row_index.size_;i++)
    {
      cell = get_param[row_index.offset_ + i];
      if(OB_SUCCESS != (ret = row_desc.add_column_desc(cell->table_id_, cell->column_id_)))
      {
        TBSYS_LOG(WARN, "add column desc fail:ret[%d], row_index.size_[%d], offset=%ld, row_num=%ld",
                  ret, row_index.size_, row_index.offset_ + i, get_param.get_row_size());
      }
    }
  }

  if (OB_SUCCESS == ret && has_flag_column)
  {
    if (OB_SUCCESS != (ret = row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID) ))
    {
      TBSYS_LOG(WARN, "fail to add OB_ACTION_FLAG_COLUMN_ID:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (rowkey_cell_count < 0)
    {
      ret = OB_INVALID_ARGUMENT;
      TBSYS_LOG(ERROR, "rowkey cell count must > 0, rowkey_cell_count=%ld", rowkey_cell_count);
    }
    else
    {
      row_desc.set_rowkey_cell_count(rowkey_cell_count);
    }
  }

  return ret;
}

