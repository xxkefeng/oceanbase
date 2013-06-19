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

#include "ob_root_server2.h"
#include "ob_daily_merge_checker.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

ObDailyMergeChecker::ObDailyMergeChecker(ObRootServer2 * root_server) : root_server_(root_server)
{
  OB_ASSERT(root_server != NULL);
}

ObDailyMergeChecker::~ObDailyMergeChecker()
{
}

void ObDailyMergeChecker::run(tbsys::CThread * thread, void * arg)
{
  UNUSED(thread);
  UNUSED(arg);
  int err = OB_SUCCESS;
  TBSYS_LOG(INFO, "[NOTICE] merge checker thread start");
  bool did_get_last_frozen_version = true;
  int64_t last_check_timestamp = 0;
  while (!_stop)
  {
    if (did_get_last_frozen_version)
    {
      ObServer ups;
      root_server_->get_update_server_info(false, ups);
      if (0 == ups.get_ipv4())
      {
        TBSYS_LOG(INFO, "no ups right now, sleep for next round");
        sleep(1);
        continue;
      }
      else if (OB_SUCCESS != (err = root_server_->get_last_frozen_version_from_ups(false)))
      {
        TBSYS_LOG(WARN, "failed to get frozen version, err=%d ups=%s", err, ups.to_cstring());
      }
      else
      {
        did_get_last_frozen_version = false;
      }
    }
    if (root_server_->is_master() && root_server_->is_daily_merge_tablet_error())
    {
      TBSYS_LOG(ERROR, "merge process alreay have some error, check it");
    }
    int64_t now = tbsys::CTimeUtil::getMonotonicTime();
    int64_t max_merge_duration_us = root_server_->config_.max_merge_duration_time;
    if (root_server_->is_master() && (root_server_->last_frozen_time_ > 0))
    {
      // check all tablet merged finish and root table is integrated
      if (!root_server_->check_all_tablet_merged())
      {
        if (now > last_check_timestamp + CHECK_DROP_INTERVAL)
        {
          // delete the dropped tablet from root table
          int64_t count = 0;
          err = root_server_->delete_dropped_tables(count);
          if (count > 0)
          {
            TBSYS_LOG(WARN, "delete dropped tables' tablets in root table:count[%ld], err[%d]", count, err);
          }
          last_check_timestamp = now;
        }
        // the first time not write this error log
        if ((root_server_->last_frozen_time_ != 1)
            && (max_merge_duration_us + root_server_->last_frozen_time_) < now)
        {
          TBSYS_LOG(ERROR, "merge is too slow,start at:%ld,now:%ld,max_merge_duration_:%ld",
              root_server_->last_frozen_time_, now, max_merge_duration_us);
        }
      }
      else
      {
        // write the log for qa & dba
        TBSYS_LOG(INFO, "build new root table ok");
        root_server_->last_frozen_time_ = 0;
        // checkpointing after done merge
        root_server_->make_checkpointing();
        root_server_->start_build_index_if_necessary();
      }
    }
    sleep(CHECK_INTERVAL_SECOND);
  }
  TBSYS_LOG(INFO, "[NOTICE] merge checker thread exit");
}

