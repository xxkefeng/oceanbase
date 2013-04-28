/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_mem_sstable_scan.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_mem_sstable_scan.h"
#include "common/ob_row_store.h"

using namespace oceanbase;
using namespace common;
using namespace sql;

ObMemSSTableScan::ObMemSSTableScan()
  :from_deserialize_(false), tmp_table_(NULL)
{
}

ObMemSSTableScan::~ObMemSSTableScan()
{
}

int ObMemSSTableScan::open()
{
  int ret = OB_SUCCESS;
  if (!from_deserialize_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "this operator can only open after deserialize");
  }
  else
  {
    cur_row_.set_row_desc(cur_row_desc_);
  }
  return ret;
}

int ObMemSSTableScan::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  ret = row_store_.get_next_row(cur_row_);
  if (OB_ITER_END == ret)
  {
    TBSYS_LOG(DEBUG, "end of iteration");
  }
  else if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get next row from row store:ret[%d]", ret);
  }
  else
  {
    row = &cur_row_;
    TBSYS_LOG(DEBUG, "[MemSSTableScan] %s", to_cstring(cur_row_));
  }
  return ret;
}

int ObMemSSTableScan::close()
{
  return OB_SUCCESS;
}

int ObMemSSTableScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  UNUSED(child_idx);
  UNUSED(child_operator);
  return OB_NOT_IMPLEMENT;
}

int64_t ObMemSSTableScan::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "MemSSTableScan(static_data=");
  if (NULL != tmp_table_)
  {
    pos += tmp_table_->to_string(buf+pos, buf_len-pos);
  }
  databuff_printf(buf, buf_len, pos, ", row_desc=");
  pos += cur_row_desc_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ", row_store=");
  pos += row_store_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ", from_deserialize=%c", from_deserialize_?'Y':'N');
  databuff_printf(buf, buf_len, pos, ")\n");
  return pos;
}

int ObMemSSTableScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (cur_row_desc_.get_column_num() <= 0)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "cur_row_desc_ is empty");
  }
  else
  {
    row_desc = &cur_row_desc_;
  }
  return ret;
}

ObPhyOperatorType ObMemSSTableScan::get_type() const
{
  return PHY_MEM_SSTABLE_SCAN;
}

DEFINE_SERIALIZE(ObMemSSTableScan)
{
  int ret = OB_SUCCESS;
  const common::ObRowDesc *row_desc = NULL;
  if (NULL == tmp_table_)
  {
    TBSYS_LOG(WARN, "tmp_table_ is NULL");
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = tmp_table_->get_row_desc(row_desc)))
  {
    TBSYS_LOG(WARN, "failed to get row desc, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = row_desc->serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to serialize row desc:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = tmp_table_->get_row_store().serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to serialize row store:ret[%d]", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObMemSSTableScan)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = cur_row_desc_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to deserialize row desc:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = row_store_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "fail to deserialize row_store:ret[%d]", ret);
  }
  else
  {
    from_deserialize_ = true;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObMemSSTableScan)
{
  int64_t size = 0;
  size += cur_row_desc_.get_serialize_size();
  size += row_store_.get_serialize_size();
  return size;
}
