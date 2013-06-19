/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#ifndef _OB_DAILY_MERGE_CHECKER_H_
#define _OB_DAILY_MERGE_CHECKER_H_

#include "tbsys.h"

namespace oceanbase
{
  namespace rootserver
  {
    // this runnable checker is a runnable thread for check root table's healthy
    // 1. at first check the daily merge whether all tablet merge finished
    // 2. if daily merge not finished, check the root table contains dropped tables
    // if exist, clear the tables and in the next round recheck it
    class ObRootServer2;
    class ObDailyMergeChecker : public tbsys::CDefaultRunnable
    {
    public:
      ObDailyMergeChecker(ObRootServer2 * root_server);
      virtual ~ObDailyMergeChecker();
    public:
      void run(tbsys::CThread * thread, void * arg);
    private:
      const static int64_t CHECK_DROP_INTERVAL = 600 * 1000 * 1000;
      const static int64_t CHECK_INTERVAL_SECOND = 5;
    private:
      ObRootServer2 * root_server_;
    };
  }
}

#endif // _OB_DAILY_MERGE_CHECKER_H_
