/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 *
 * Authors:
 *   huating <huating.zmq@alipay.com>
 *
 */
#include "ob_interval_sample.h"

namespace oceanbase
{
  namespace sql
  {
    using namespace oceanbase;
    using namespace common;

    ObIntervalSample::ObIntervalSample()
      :row_interval_(0)
    {
    }

    ObIntervalSample::~ObIntervalSample()
    {
    }

    int ObIntervalSample::set_row_interval(const int64_t row_interval)
    {
      int ret = OB_SUCCESS;

      if (row_interval <= 0)
      {
        TBSYS_LOG(WARN, "row interval must be > 0, row_interval=%ld",
                  row_interval);
        ret = OB_INVALID_ARGUMENT;
      }
      else 
      {
        row_interval_ = row_interval;
      }
      
      return ret;
    }

    int64_t ObIntervalSample::get_row_interval() const
    {
      return row_interval_; 
    }

    int ObIntervalSample::open()
    {
      int ret = OB_SUCCESS;

      if ((ret = ObSingleChildPhyOperator::open()) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Failed to open child_op, ret=%d", ret);
      }
      else if (row_interval_ <= 0)
      {
        TBSYS_LOG(WARN, "invalid row interval, row_interval_=%ld", row_interval_);
      }

      return ret;
    }

    int ObIntervalSample::close()
    {
      return ObSingleChildPhyOperator::close();
    }

    int ObIntervalSample::get_row_desc(const ObRowDesc*& row_desc) const
    {
      int ret = OB_SUCCESS;

      if (OB_UNLIKELY(NULL == child_op_ || row_interval_ <= 0))
      {
        TBSYS_LOG(ERROR, "child_op_ is NULL or invalid row_interval_=%ld", 
                  row_interval_);
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = child_op_->get_row_desc(row_desc);
      }

      return ret;
    }

    int ObIntervalSample::get_next_row(const ObRow*& row)
    {
      int ret = OB_SUCCESS;
      const ObRow* input_row = NULL;
      int64_t skip_row_count = 0;

      if (OB_UNLIKELY(NULL == child_op_ || row_interval_ <= 0))
      {
        TBSYS_LOG(ERROR, "child_op_ is NULL or invalid row_interval_=%ld", 
                  row_interval_);
        ret = OB_NOT_INIT;
      }
      else
      {
        while (skip_row_count < row_interval_)
        {
          if (OB_SUCCESS != (ret = child_op_->get_next_row(input_row)))
          {
            if (OB_ITER_END != ret)
            {
              TBSYS_LOG(WARN, "child_op failed to get next row, ret=%d", ret);
            }
            break;
          }
          else
          {
            ++skip_row_count;
          }
        } // end while

        if (OB_SUCCESS == ret)
        {
          row = input_row;
        }
      }

      return ret;
    }

    int64_t ObIntervalSample::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;

      if (NULL != buf && buf_len > 0)
      {
        databuff_printf(buf, buf_len, pos, "row_interval=%ld", row_interval_);
        if (NULL != child_op_)
        {
          int64_t pos2 = child_op_->to_string(buf + pos, buf_len - pos);
          pos += pos2;
        }
      }

      return pos;
    }
  } /* chunkserver */
} /* oceanbase */
