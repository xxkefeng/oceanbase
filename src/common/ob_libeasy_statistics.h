/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@taobao.com>
 */

#ifndef OB_LIBEASY_STATISTICS_H
#define OB_LIBEASY_STATISTICS_H

#include "easy_io_struct.h"
#include "easy_io.h"
#include "common/ob_timer.h"

#define FD_BUFFER_SIZE 1024 * 1024
namespace oceanbase
{
  namespace common
  {
    class ObLibEasyStatistics : public common::ObTimerTask
    {
      public:
        ObLibEasyStatistics();
        ~ObLibEasyStatistics();
        void runTimerTask();
        void init(easy_io_t *eio);
      private:
        easy_io_t *eio_;
        char buffer_[FD_BUFFER_SIZE];
    };
  }
}
#endif // OB_LIBEASY_STATISTICS_H
