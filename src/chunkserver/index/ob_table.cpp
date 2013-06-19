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
#include "common/file_directory_utils.h"
#include "ob_tablet_sstable.h"
#include "ob_disk_manager.h"
#include "ob_tablet_manager.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace common;

    bool ObRpcOptions::is_valid() const
    {
      bool ret = true;

      if (NULL == rpc_stub_ || NULL == sql_rpc_stub_ || 0 == server_.get_ipv4() 
          || retry_times_ < 0 || timeout_ < 0)
      {
        TBSYS_LOG(WARN, "invalid rpc options, %s", to_cstring(*this));
        ret = false;
      }

      return ret;
    }

    int64_t ObRpcOptions::to_string(char* buffer, const int64_t size) const
    {
      int64_t pos = 0;

      if (NULL != buffer && size > 0)
      {
        databuff_printf(buffer, size, pos, 
                        "rpc_stub_=%p, sql_rpc_stub_=%p, "
                        "retry_times_=%ld, timeout_=%ld, server_ip=",
                        rpc_stub_, sql_rpc_stub_, retry_times_, timeout_);
        pos += server_.to_string(buffer + pos, size - pos);
      }

      return pos;
    }

    bool ObScanOptions::is_valid() const
    {
      bool ret = true;

      if (NULL == schema_ || NULL == data_tablet_range_
          || (scan_local_ && NULL == scan_context_) 
          || (!scan_local_ && !rpc_option_.is_valid())
          || OB_INVALID_ID == index_table_id_)
      {
        TBSYS_LOG(WARN, "invalid scan options, %s", to_cstring(*this));
        ret = false;
      }

      return ret;
    }

    int64_t ObScanOptions::to_string(char* buffer, const int64_t size) const
    {
      int64_t pos = 0;

      if (NULL != buffer && size > 0)
      {
        databuff_printf(buffer, size, pos, 
                        "index_table_id_=%lu, scan_local=%d, is_result_cached_=%d, "
                        "scan_only_rowkey_=%d, need_sort=%d, row_interval_=%ld, "
                        "schema_=%p, data_tablet_range_=%p, index_tablet_range_=%p, "
                        "scan_param_=%p, scan_context_=%p, rpc_option_=%s",
                        index_table_id_, scan_local_, is_result_cached_, 
                        scan_only_rowkey_, need_sort_, row_interval_, 
                        schema_, data_tablet_range_, index_tablet_range_, 
                        scan_param_, scan_context_, to_cstring(rpc_option_));
      }

      return pos;
    }
  } /* chunkserver */
} /* oceanbase */
