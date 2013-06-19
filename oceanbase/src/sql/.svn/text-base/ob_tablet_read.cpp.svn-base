/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_read.cpp 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_tablet_read.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

ObTabletRead::ObTabletRead()
  :op_root_(NULL),
  is_read_consistency_(true),
  rpc_proxy_(NULL),
  network_timeout_(0),
  join_batch_count_(0),
  cur_rowkey_op_(NULL),
  plan_level_(SSTABLE_DATA)
{
}

int ObTabletRead::open()
{
  int ret = OB_SUCCESS;
  if (NULL == op_root_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "should create plan before open");
  }
  
  if (OB_SUCCESS == ret)
  {
    ret = op_root_->open();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "open tablets scan child operator fail. ret=%d", ret);
    }
  }
  return ret;
}

int ObTabletRead::close()
{
  int ret = OB_SUCCESS;
  //释放内存等;
  if(NULL != op_root_)
  {
    if(OB_SUCCESS != (ret = op_root_->close()))
    {
      TBSYS_LOG(WARN, "close op_root fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObTabletRead::get_next_row(const ObRow *&row)
{
  int ret = OB_SUCCESS;

  if(OB_UNLIKELY(NULL == op_root_))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "op root is null");
  }
  else
  {
    ret = op_root_->get_next_row(row);
    if (OB_SUCCESS == ret && NULL != row)
    {
      TBSYS_LOG(DEBUG, "tablet read row[%s]", to_cstring(*row));
    }
    else if (OB_ITER_END != ret)
    {
      TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
    }
  }
  return ret;
}

int ObTabletRead::set_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (NULL == rpc_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "rpc_proxy is null");
  }
  else
  {
    rpc_proxy_ = rpc_proxy;
  }
  return ret;
}

int ObTabletRead::get_cur_rowkey(const ObRowkey *&rowkey) const
{
  int ret = OB_SUCCESS;
  if (NULL == cur_rowkey_op_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "last_rowkey_op_ is null");
  }
  else
  {
    ret = cur_rowkey_op_->get_cur_rowkey(rowkey);
    if (OB_SUCCESS != ret || NULL == rowkey)
    {
      TBSYS_LOG(WARN, "failed to get cur rowkey, ret[%d]", ret);
    }
  }
  return ret;
}

