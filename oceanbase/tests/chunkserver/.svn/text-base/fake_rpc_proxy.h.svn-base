/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* fake_rpc_proxy.h is for ...
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/
#ifndef OCEANBASE_CHUNKSERVER_FAKE_RPC_PROXY_H_
#define OCEANBASE_CHUNKSERVER_FAKE_RPC_PROXY_H_

#include "ob_rpc_proxy.h"

namespace oceanbase
{
namespace tests
{
namespace chunkserver
{

namespace ob = oceanbase;
namespace ob_cs = oceanbase::chunkserver;

class FakeRpcProxy : public ob_cs::ObMergerRpcProxy
{
public:
  FakeRpcProxy(const int64_t retry_count, const int64_t timeout,
      const ob::common::ObServer &root_server)
    : ObMergerRpcProxy(retry_count, timeout, root_server)
  {
  }

  virtual int get_update_server(const bool, ob::common::ObServer &server, bool)
  {
    server.set_ipv4_addr("127.0.0.1", 8968);
    return OB_SUCCESS;
  }

  virtual int fetch_update_server_list(int32_t &count)
  {
    count = 1;
    return OB_SUCCESS;
  }
};

} // end namespace chunkserver
} // end namespace tests
} // end namespace oceanbase
#endif // OCEANBASE_CHUNKSERVER_FAKE_RPC_PROXY_H_
