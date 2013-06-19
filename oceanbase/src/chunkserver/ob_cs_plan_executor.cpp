/** 
 * (C) 2010-2013
 * Alibaba Group Holding Limited.  
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_plan_executor.cpp
 *
 * Authors:
 *  yongle.xh<yongle.xh@alipay.com>
 *
 */

#include "ob_cs_plan_executor.h"
#include "common/ob_profile_log.h"
#include "common/ob_schema_manager.h"

using namespace oceanbase;
using namespace chunkserver;
using namespace common;

ObCsPlanExecutor::ObCsPlanExecutor() :
  timeout_us_(0),
  cur_rowkey_(NULL),
  cur_row_(NULL),
  root_op_(NULL)
{
}

ObCsPlanExecutor::~ObCsPlanExecutor()
{
}

void ObCsPlanExecutor::reset()
{
  timeout_us_ = 0;
  rowkey_allocator_.reuse();
  cur_rowkey_ = NULL;
  cur_row_ = NULL;
  root_op_ = NULL;
}

int ObCsPlanExecutor::open(const sql::ObPhysicalPlan &plan, ObPlanContext& context)
{
  int ret = OB_SUCCESS;
  context_ = &context;

  if (NULL == (root_op_ = plan.get_main_query()))
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "root_op_ must not null");
  }
  else if (NULL == context.cur_rowkey_op_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "cur_rowkey_op must not null");
  }
  else if (OB_SUCCESS != (ret = root_op_->open()))
  {
    TBSYS_LOG(ERROR, "failed to open rppt op, ret=%d", ret);
  }
  return ret;
}

int ObCsPlanExecutor::fill_scan_data(ObNewScanner &new_scanner)
{
  int ret = OB_SUCCESS;
  int64_t fullfilled_row_num = 0;
  int64_t start_time = 0;
  int64_t timeout = timeout_us_;

  INIT_PROFILE_LOG_TIMER();

  if(OB_SUCCESS == ret)
  {
    new_scanner.reuse();
    start_time = tbsys::CTimeUtil::getTime();
  }

  if (NULL == context_ || NULL == context_->cur_rowkey_op_)
  {
    TBSYS_LOG(ERROR, "context_=%p and context_.cur_rowkey_op_=%p must not null", context_, context_->cur_rowkey_op_);
    ret = OB_NOT_INIT;
  }

  while(OB_SUCCESS == ret)
  {
    if(NULL == cur_row_)
    {
      ret = root_op_->get_next_row(cur_row_);
      if(OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(true, fullfilled_row_num)))
        {
          TBSYS_LOG(WARN, "new scanner set is req fullfilled fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = new_scanner.set_last_row_key(last_rowkey_)))
        {
          TBSYS_LOG(WARN, "new scanner set last row key fail:ret[%d]", ret);
        }
        break;
      }
      else if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
      }

      if(OB_SUCCESS == ret)
      {
        if(NULL != cur_rowkey_)
        {
          rowkey_allocator_.reuse();
          if(OB_SUCCESS != (ret = cur_rowkey_->deep_copy(last_rowkey_, rowkey_allocator_)))
          {
            TBSYS_LOG(WARN, "deep copy rowkey fail:ret[%d]", ret);
          }
        }

        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = context_->cur_rowkey_op_->get_cur_rowkey(cur_rowkey_)))
          {
            TBSYS_LOG(WARN, "get last rowkey fail:ret[%d]", ret);
          }
        }
      }
    }

    int64_t max_time = timeout - std::min(static_cast<int64_t>((static_cast<double>(timeout) * 0.3)),
                                          static_cast<int64_t>(500 * 1000) /* 500ms */);

    if (OB_SUCCESS == ret)
    {
      // timeout
      if ((start_time + max_time) < g_cur_time)
      {
        if (fullfilled_row_num > 0)
        {
          // at least get one row
          TBSYS_LOG(WARN, "get or scan too long time, start_time=%ld timeout=%ld timeu=%ld row_count=%ld",
              start_time, timeout, g_cur_time - start_time, fullfilled_row_num);
          if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(false, fullfilled_row_num)))
          {
            TBSYS_LOG(WARN, "new scanner set is req fullfilled fail:ret[%d]", ret);
          }
          else if(OB_SUCCESS != (ret = new_scanner.set_last_row_key(last_rowkey_)))
          {
            TBSYS_LOG(WARN, "new scanner set last row key fail:ret[%d]", ret);
          }
          //else
          //{
          //  ret = OB_FORCE_TIME_OUT;
          //}
        }
        else
        {
          TBSYS_LOG(ERROR, "can't get any row, start_time=%ld timeout=%ld timeu=%ld",
              start_time, timeout, g_cur_time - start_time);
          ret = OB_RESPONSE_TIME_OUT;
        }
        break;
      }
    }

    if(OB_SUCCESS == ret)
    {
      ret = new_scanner.add_row(*cur_row_);
      if(OB_SIZE_OVERFLOW == ret)
      {
        ret = OB_SUCCESS;
        if(OB_SUCCESS != (ret = new_scanner.set_is_req_fullfilled(false, fullfilled_row_num)))
        {
          TBSYS_LOG(WARN, "new scanner set is req fullfilled fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = new_scanner.set_last_row_key(last_rowkey_)))
        {
          TBSYS_LOG(WARN, "new scanner set last row key fail:ret[%d]", ret);
        }
        break;
      }
      else if(OB_SUCCESS == ret)
      {
        cur_row_ = NULL;
        fullfilled_row_num ++;

        if (fullfilled_row_num % 1000 == 0)
        {
          PROFILE_LOG_TIME(DEBUG, "fill next 1000 row, fullfilled_row_num=%ld", fullfilled_row_num);
        }
      }
      else
      {
        TBSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
      }
    }
  }

  if ((OB_SUCCESS == ret || OB_FORCE_TIME_OUT == ret)
        && NULL != context_ && SCAN == context_->type_  )
  {
    if (OB_SUCCESS != (ret = new_scanner.set_range(context_->data_range_)))
    {
      TBSYS_LOG(WARN, "failed to set range for result of scan, ret=%d", ret);
    }
  }

  return ret;
}

int ObCsPlanExecutor::close()
{
  int ret = OB_SUCCESS;
  if (NULL != root_op_ && OB_SUCCESS != (ret = root_op_->close()))
  {
    TBSYS_LOG(WARN, "close root_op_ fail:err[%d]", ret);
  }
  return ret;
}
