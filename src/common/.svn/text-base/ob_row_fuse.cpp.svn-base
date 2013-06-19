/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_fuse.c
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_row_fuse.h"

using namespace oceanbase::common;

int ObRowFuse::join_row(const ObRow *incr_row, const ObRow *sstable_row, ObRow *result)
{
  int ret = OB_SUCCESS;
  const ObObj *ups_cell = NULL;
  ObObj *result_cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *action_flag_cell = NULL;
  int64_t incr_action_flag = 0;

  if(NULL == incr_row || NULL == sstable_row || NULL == result)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "incr_row[%p], sstable_row[%p], result[%p]", incr_row, sstable_row, result);
  }

  if (OB_SUCCESS != (ret = incr_row->get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, action_flag_cell) ))
  {
    TBSYS_LOG(WARN, "fail to get action flag cell:ret[%d]", ret);
  }
  else if (ObExtendType != action_flag_cell->get_type())
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "action_flag_cell_type[%d]", action_flag_cell->get_type());
  }
  else
  {
    incr_action_flag = action_flag_cell->get_ext();
  }

  if (OB_SUCCESS == ret)
  {
    result->assign(*sstable_row);
    for (int64_t i = 0; OB_SUCCESS == ret && i < incr_row->get_column_num(); ++i)
    {
      if (OB_SUCCESS != (ret = incr_row->raw_get_cell(i, ups_cell, table_id, column_id)))
      {
        TBSYS_LOG(WARN, "fail to get ups cell:ret[%d]", ret);
      }
      
      if (OB_SUCCESS == ret && column_id != OB_ACTION_FLAG_COLUMN_ID)
      {
        if (OB_SUCCESS != (ret = result->get_cell(table_id, column_id, result_cell)))
        {
          TBSYS_LOG(WARN, "fail to get result cell:ret[%d]", ret);
        }
        else
        {
          if (ObActionFlag::OP_DEL_ROW == incr_action_flag)
          {
            result_cell->set_null();
          }
          else if (ObActionFlag::OP_ROW_DOES_NOT_EXIST == incr_action_flag)
          {
            //do nothing
          }
          else if (ObActionFlag::OP_NEW_ADD == incr_action_flag)
          {
            result_cell->set_null();
            if (OB_SUCCESS != (ret = result_cell->apply(*ups_cell)))
            {
              TBSYS_LOG(WARN, "fail to apply ups cell to result cell:ret[%d]", ret);
            }
          }
          else if (ObActionFlag::OP_VALID == incr_action_flag)
          {
            if (OB_SUCCESS != (ret = result_cell->apply(*ups_cell)))
            {
              TBSYS_LOG(WARN, "fail to apply ups cell to result cell:ret[%d]", ret);
            }
          }
          else
          {
            ret = OB_NOT_SUPPORTED;
            TBSYS_LOG(WARN, "unsupport incr_action_flag [%ld]", incr_action_flag);
          }
        }
      }
    }
  }
  return ret;
}

int ObRowFuse::apply_row(const ObRow &row, ObRow &result_row, bool copy)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  ObObj *result_cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObRowDesc *row_desc = NULL;

  if (NULL == (row_desc = row.get_row_desc()))
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "row_desc is null");
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; OB_SUCCESS == ret && i < row.get_column_num(); i ++)
    {
      if (OB_SUCCESS != (ret = row.raw_get_cell(i, cell, table_id, column_id) ))
      {
        TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
      }
      else if (OB_ACTION_FLAG_COLUMN_ID != column_id)
      {
        if (OB_SUCCESS != (ret = result_row.get_cell(table_id, column_id, result_cell) ))
        {
          TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
        }
        else
        {
          if (copy)
          {
            *result_cell = *cell;
          }
          else
          {
            ret = result_cell->apply(*cell);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "fail to apply cell:ret[%d]", ret);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRowFuse::fuse_row(const ObRow &row, ObRow &result_row, bool &is_row_empty, bool is_ups_row)
{
  int ret = OB_SUCCESS;
  if (!is_ups_row)
  {
    ret = fuse_sstable_row(row, result_row, is_row_empty);
  }
  else
  {
    ret = fuse_ups_row(row, result_row, is_row_empty);
  }
  return ret;
}

int ObRowFuse::fuse_sstable_row(const ObRow &row, ObRow &result_row, bool &is_row_empty)
{
  int ret = OB_SUCCESS;
  const ObObj *action_flag_cell = NULL;
  ObObj *result_action_flag_cell = NULL;
  int64_t row_action_flag = 0;
  int64_t idx = 0;
  int64_t result_idx = 0;

  TBSYS_LOG(DEBUG, "before fuse row: %s, result_row: %s", to_cstring(row), to_cstring(result_row));

  if (NULL == row.get_row_desc() || NULL == result_row.get_row_desc())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "row desc[%p], result row desc[%p]", 
      row.get_row_desc(), result_row.get_row_desc());
  }
  else
  {
    idx = row.get_row_desc()->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
    result_idx = result_row.get_row_desc()->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
    if (OB_INVALID_INDEX != result_idx)
    {
      if (OB_SUCCESS != (ret = result_row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, result_action_flag_cell)))
      {
        TBSYS_LOG(WARN, "fail to get cell:ret[%d]", ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_INVALID_INDEX == idx)
    {
      if (true == is_row_empty)
      {
        result_row.assign(row);
        is_row_empty = false;
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "not action flag found for row");
      }
    }
    else
    {
      if (OB_SUCCESS != (ret = row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, action_flag_cell) ))
      {
        TBSYS_LOG(WARN, "fail to get action flag cell:ret[%d]", ret);
      }
      else if (ObExtendType != action_flag_cell->get_type())
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "action_flag_cell_type[%d]", action_flag_cell->get_type());
      }
      else
      {
        row_action_flag = action_flag_cell->get_ext();
      }

      if (OB_SUCCESS == ret)
      {
        switch (row_action_flag)
        {
          case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
            // do nothing
            break;
          case ObActionFlag::OP_DEL_ROW:
            is_row_empty = true;
            if (OB_SUCCESS != (ret = result_row.reset(true, ObRow::DEFAULT_NULL) ))
            {
              TBSYS_LOG(WARN, "reset result fail:ret[%d]", ret);
            }
            else if (NULL != result_action_flag_cell)
            {
              result_action_flag_cell->set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
            }
            break;
          case ObActionFlag::OP_NEW_ADD:
            is_row_empty = false;
            if (OB_SUCCESS != (ret = result_row.reset(true, ObRow::DEFAULT_NULL) )) // set to all null
            {
              TBSYS_LOG(WARN, "fail to reset result_row:ret[%d]", ret);
            }
            else if (OB_SUCCESS != (ret = apply_row(row, result_row, false) ))
            {
              TBSYS_LOG(WARN, "fail to apply row:ret[%d]", ret);
            }
            else if (NULL != result_action_flag_cell)
            {
              result_action_flag_cell->set_ext(ObActionFlag::OP_VALID);
            }
            break;
          case ObActionFlag::OP_VALID:
            is_row_empty = false;
            if (OB_SUCCESS != (ret = apply_row(row, result_row, false) ))
            {
              TBSYS_LOG(WARN, "fail to apply row:ret[%d]", ret);
            }
            else if (NULL != result_action_flag_cell)
            {
              result_action_flag_cell->set_ext(ObActionFlag::OP_VALID);
            }
            break;
          default:
            ret = OB_NOT_SUPPORTED;
            TBSYS_LOG(WARN, "unsupported ext type[%ld]", row_action_flag);
        }
      }
    }
  }
  TBSYS_LOG(DEBUG, "after fuse row: %s, result_row: %s", to_cstring(row), to_cstring(result_row));
  return ret;
}

int ObRowFuse::fuse_ups_row(const ObRow &row, ObRow &result_row, bool &is_row_empty)
{
  int ret = OB_SUCCESS;
  const ObObj *action_flag_cell = NULL;
  ObObj *result_action_flag_cell = NULL;
  bool copy = false;
  int64_t row_action_flag = 0;
  int64_t result_row_action_flag = 0;

  TBSYS_LOG(DEBUG, "before fuse row: %s, result_row: %s", to_cstring(row), to_cstring(result_row));
  if (OB_SUCCESS != (ret = row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, action_flag_cell) ))
  {
    TBSYS_LOG(WARN, "fail to get action flag cell:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = result_row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, result_action_flag_cell) ))
  {
    TBSYS_LOG(WARN, "fail to get result action flag cell:ret[%d]", ret);
  }
  else if (ObExtendType != action_flag_cell->get_type() || ObExtendType != result_action_flag_cell->get_type())
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "action_flag_cell_type[%d], result_row_action_flag_type[%d]", action_flag_cell->get_type(),
      result_action_flag_cell->get_type());
  }
  else
  {
    row_action_flag = action_flag_cell->get_ext();
    result_row_action_flag = result_action_flag_cell->get_ext();
  }

  if (OB_SUCCESS == ret)
  {
    switch (row_action_flag)
    {
      case ObActionFlag::OP_NEW_ADD:
        result_action_flag_cell->set_ext(ObActionFlag::OP_NEW_ADD);
        if (OB_SUCCESS != (ret = apply_row(row, result_row, true) ))
        {
          TBSYS_LOG(WARN, "fail to apply row:ret[%d]", ret);
        }
        break;
      case ObActionFlag::OP_DEL_ROW:
        result_action_flag_cell->set_ext(ObActionFlag::OP_DEL_ROW);
        break;
      case ObActionFlag::OP_VALID:
        if (result_row_action_flag == ObActionFlag::OP_VALID
          || result_row_action_flag == ObActionFlag::OP_ROW_DOES_NOT_EXIST)
        {
          result_action_flag_cell->set_ext(ObActionFlag::OP_VALID);
        }
        else
        {
          result_action_flag_cell->set_ext(ObActionFlag::OP_NEW_ADD);
        }
        copy = result_row_action_flag == ObActionFlag::OP_DEL_ROW
          || result_row_action_flag == ObActionFlag::OP_ROW_DOES_NOT_EXIST;
        if (OB_SUCCESS != (ret = apply_row(row, result_row, copy) ))
        {
          TBSYS_LOG(WARN, "fail to apply row:ret[%d]", ret);
        }
        break;
      case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
        // do nothing
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        TBSYS_LOG(WARN, "unsupported ext type[%ld]", row_action_flag);
    }
  }

  if (OB_SUCCESS == ret)
  {
    is_row_empty = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == result_action_flag_cell->get_ext());
  }
  
  TBSYS_LOG(DEBUG, "after fuse row: %s, result_row: %s", to_cstring(row), to_cstring(result_row));
  return ret;
}

