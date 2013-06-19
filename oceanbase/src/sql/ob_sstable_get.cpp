/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sstable_get.cpp 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_sstable_get.h"
#include "common/ob_new_scanner_helper.h"

ObSSTableGet::ObSSTableGet()
  :tablet_manager_(NULL),
  tablet_version_(0),
  getter_(NULL),
  cur_rowkey_(NULL)
{
}

int ObSSTableGet::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;

  if(OB_UNLIKELY(NULL == getter_))
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = getter_->get_row_desc(row_desc);
  }
  return ret;
}

int ObSSTableGet::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  return ret;
}


int ObSSTableGet::open()
{
  int ret = OB_SUCCESS;
  if (NULL == tablet_manager_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "tablet_manager_ is null");
  }

  return ret;
}

int ObSSTableGet::close()
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  if (NULL != tablet_manager_)
  {
    if (OB_SUCCESS != (err = tablet_manager_->end_get()))
    {
      TBSYS_LOG(WARN, "fail to end get :err[%d]", err);
      ret = err;
    }
  }
  return ret;
}

int ObSSTableGet::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  
  if(OB_UNLIKELY(NULL == getter_))
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = getter_->get_next_row(row);
    FILL_TRACE_LOG("sstable_get_next_row_done");
    if (OB_SUCCESS != ret && OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "fail to get next row:ret[%d]", ret);
    }
    else if (OB_SUCCESS == ret)
    {
      TBSYS_LOG(DEBUG, "sstable get row[%s]", to_cstring(*row));
      if (OB_SUCCESS != (ret = row->get_rowkey(cur_rowkey_)))
      {
        TBSYS_LOG(WARN, "fail to get cur rowkey, ret[%d]", ret);
      }
    }
  }

  return ret;
}

int64_t ObSSTableGet::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObSSTableGet");
  return pos;
}

int ObSSTableGet::get_tablet_data_version(int64_t &data_version)
{
  data_version = tablet_version_;
  return OB_SUCCESS;
}

int ObSSTableGet::get_cur_rowkey(const common::ObRowkey *&rowkey) const
{
  int ret = OB_SUCCESS;
  if (NULL == cur_rowkey_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "fail to get cur rowkey, ret[%d]", ret);
  }
  else
  {
    rowkey = cur_rowkey_;
  }
  return OB_SUCCESS;
}



