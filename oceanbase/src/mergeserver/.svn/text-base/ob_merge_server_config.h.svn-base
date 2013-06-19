/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Filename: ob_merge_server_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#ifndef _OB_MERGE_SERVER_CONFIG_H_
#define _OB_MERGE_SERVER_CONFIG_H_

#include <stdint.h>
#include "tbsys.h"
#include "common/ob_config.h"
#include "common/ob_object.h"
#include "common/ob_server_config.h"
#include "common/ob_system_config.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace mergeserver
  {
    union UValue
    {
        int32_t i32;
        int64_t i64;
    };

    class ObMergeServerConfig
          : public common::ObServerConfig
    {
      protected:
        common::ObRole get_server_type() const
        { return OB_MERGESERVER; }

      public:
        DEF_INT(root_server_port, "0", "(1024,65535)", "root server listen port");
        DEF_TIME(task_left_time, "100ms", "task left time for drop ahead");
        DEF_INT(task_queue_size, "10000", "[1,]", "task queue size");
        DEF_INT(io_thread_count, "1", "[1,]", "io thread count for libeasy");
        DEF_INT(task_thread_count, "10", "task thread number");
        DEF_INT(log_interval_count, "100", "legacy param, used for OB0.3 Only");
        DEF_TIME(network_timeout, "2s", "timeout when communication with other server");
        DEF_TIME(frozen_version_timeout, "600s", "ups frozen version cache tiemout");
        DEF_TIME(monitor_interval, "600s", "execute monitor task once every monitor_interval");
        DEF_TIME(lease_check_interval, "6s", "lease check interval");
        DEF_CAP(location_cache_size, "32MB", "location cache size");
        DEF_TIME(location_cache_timeout, "600s", "location cache timeout");
        DEF_CAP(intermediate_buffer_size, "8MB", "intermediate buffer size to store one packet, 4 times network packet size (2M)");
        DEF_INT(memory_size_limit_percentage, "40", "(0,100]", "max percentage of totoal physical memory ms can use");
        DEF_INT(max_parellel_count, "16", "[1,]", "max parellel sub request to chunkservers for one request");
        DEF_INT(max_get_rows_per_subreq, "20", "[0,]", "row count to split to cs when using multi-get, 0 means no split");
        DEF_TIME(max_req_process_time, "15s", "max process time for each request");
        DEF_BOOL(use_new_balance_method, "True", "use new balance method");
        DEF_INT(reserve_get_param_count, "3", "[1,]", "legacy param, used for OB0.3 Only");
        DEF_INT(timeout_percent, "70", "[10,80]", "max cs timeout to ms timeout, used by ms retry");
        DEF_BOOL(allow_return_uncomplete_result, "False", "allow return uncomplete result");
        DEF_TIME(slow_query_threshold, "100ms", "query time beyond this value will be treat as slow query");
        DEF_CAP(query_cache_size, "0", "[0,]", "query cache size, 0 means disabled");
        //param for obmysql
        DEF_INT(obmysql_port, "3100", "(1024,65536)", "obmysql listen port");
        DEF_INT(obmysql_io_thread_count, "4", "[1,]", "obmysql io thread count for libeasy");
        DEF_INT(obmysql_work_thread_count, "50", "[1,]", "obmysql io thread count for doing sql task");
        DEF_CAP(obmysql_task_queue_size, "10000", "[1,]", "obmysql task queue size");
    };
  } /* mergeserver */
} /* oceanbase */

#endif /* _OB_MERGE_SERVER_CONFIG_H_ */
