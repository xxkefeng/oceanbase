/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_ms_provider.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_ms_provider.h"
#include "common/ob_scan_param.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootMsProvider::ObRootMsProvider(const ObChunkServerManager & server_manager)
  :server_manager_(server_manager)
{
}

ObRootMsProvider::~ObRootMsProvider()
{
}

int ObRootMsProvider::get_ms(ObServer & server)
{
  static __thread int32_t last_index = -1;
  last_index = (last_index + 1) % (int32_t)server_manager_.size();
  int ret = server_manager_.get_next_alive_ms(last_index, server);
  if (ret != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "get next alive ms failed:index[%d], ret[%d]", last_index, ret);
  }
  else
  {
    TBSYS_LOG(TRACE, "get alive ms succ:index[%d], server[%s]", last_index, server.to_cstring());
  }
  return ret;
}

