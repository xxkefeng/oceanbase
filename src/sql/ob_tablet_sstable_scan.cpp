/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* ob_tablet_sstable_scan.cpp is for ...
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/

#include "ob_tablet_sstable_scan.h"
#include "common/ob_schema.h"
#include "common/utility.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObTabletSSTableScan::ObTabletSSTableScan()
{
  reset();
}

ObTabletSSTableScan::~ObTabletSSTableScan()
{
}

void ObTabletSSTableScan::reset()
{
  op_root_ = NULL;
  sstable_scan_param_ = NULL;
}

int ObTabletSSTableScan::create_plan(const ObSchemaManagerV2 &schema_mgr)
{
  // Caution: we pass *(const ObSchemaManagerV2 *)NULL to ObTabletSSTableScan::create_plan
  UNUSED(schema_mgr);

  int rc = OB_SUCCESS;
  if (!sstable_scan_param_)
  {
    TBSYS_LOG(ERROR, "sstable scan param is NULL while creating plan");
    rc = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == rc)
  {
    rc = sstable_scan_.open_scan_context(*sstable_scan_param_, scan_context_);
    if (OB_SUCCESS != rc)
    {
      TBSYS_LOG(ERROR,"sstable scan open scan context failed, rc %d", rc);
    }
    else
    {
      op_root_ = &sstable_scan_;
      cur_rowkey_op_ = &sstable_scan_;
    }
  }

  return rc;
}

int64_t ObTabletSSTableScan::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != op_root_)
  {
    pos = op_root_->to_string(buf, buf_len);
  }

  return pos;
}

