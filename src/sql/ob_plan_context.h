/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_plan_context.h
 *
 * Authors:
 *  yongle.xh<yongle.xh@alipay.com>
 *
 */
#ifndef _OB_PHYSICAL_PLAN_CONTEXT_H
#define _OB_PHYSICAL_PLAN_CONTEXT_H

#include "common/ob_range2.h"
namespace oceanbase
{
  namespace sql
  {
    class ObCurRowkeyInterface;

    enum ObPlanType{
      SCAN,
      GET,
      INIT,
    };

    struct ObPlanContext
    {
      ObPlanContext():table_id_(common::OB_INVALID_ID), type_(INIT),
        cur_rowkey_op_(NULL), data_range_() {}
      uint64_t table_id_;
      ObPlanType type_; // this field is set by sstable scan/get or ups scan/get
      const ObCurRowkeyInterface *cur_rowkey_op_;// only could be scan_merge_/sstable_scan_/get_merge_/sstable_get_ now
      ObNewRange data_range_;

      inline int set_type(ObPlanType type)
      {
        int ret = common::OB_SUCCESS;
        if (INIT == type_)
        {
          type_ = type;
        }
        else if (type != type_)
        {
          TBSYS_LOG(WARN, "scan and get operation should not be in one plan, type=%d type_=%d", type, type_);
          ret = common::OB_ERR_UNEXPECTED;
        }
        return ret;
      }

      void reset()
      {
        table_id_ = common::OB_INVALID_ID;
        type_ = INIT;
        cur_rowkey_op_ = NULL;
        data_range_.reset();
      }
    };
  }
}

#endif // _OB_PHYSICAL_PLAN_CONTEXT_H
