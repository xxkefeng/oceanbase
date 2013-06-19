/**
 * (C) 2010-2013 Taobao Inc.
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
#ifndef OCEANBASE_SQL_INTERVAL_SAMPLE_H_
#define OCEANBASE_SQL_INTERVAL_SAMPLE_H_

#include "ob_single_child_phy_operator.h"

namespace oceanbase
{
  namespace sql
  {
    class ObIntervalSample : public sql::ObSingleChildPhyOperator
    {
      public:
        ObIntervalSample();
        virtual ~ObIntervalSample();

        int set_row_interval(const int64_t row_interval);
        int64_t get_row_interval() const;
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow*& row);
        virtual int get_row_desc(const common::ObRowDesc*& row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObIntervalSample);

        int64_t row_interval_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_SQL_INTERVAL_SAMPLE_H_
