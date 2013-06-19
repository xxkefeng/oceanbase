/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_rpc_stub.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_sql_rpc_stub.h"
#include "common/ob_client_manager.h"
#include "common/ob_server.h"
#include "common/ob_result.h"
#include "common/ob_operate_result.h"
#include "common/thread_buffer.h"
#include "common/ob_schema.h"
#include "common/ob_tablet_info.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_trace_log.h"
#include "common/utility.h"
#include "sstable/ob_sstable_scan_param.h"


using namespace oceanbase;
using namespace chunkserver;
using namespace common;


ObSqlRpcStub::ObSqlRpcStub()
{
}

ObSqlRpcStub::~ObSqlRpcStub()
{
}

int ObSqlRpcStub::get(const int64_t timeout, const ObServer & server, const ObGetParam & get_param, ObNewScanner & new_scanner) const
{
  return send_1_return_1(server, timeout, OB_NEW_GET_REQUEST, DEFAULT_VERSION, get_param, new_scanner);
}

int ObSqlRpcStub::scan(const int64_t timeout, const ObServer & server, const ObScanParam & scan_param, ObNewScanner & new_scanner) const
{
  return send_1_return_1(server, timeout, OB_NEW_SCAN_REQUEST, DEFAULT_VERSION, scan_param, new_scanner);
}

int ObSqlRpcStub::sstable_scan(const int64_t timeout, const ObServer &server,
    const sstable::ObSSTableScanParam &param,
    ObNewScanner &new_scanner, int64_t &session_id) const
{
  int rc = OB_SUCCESS;
  ObDataBuffer buffer;
  ObResultCode result_code;
  int64_t pos = 0;

  new_scanner.reuse();
  if (OB_SUCCESS != (rc = get_rpc_buffer(buffer)))
  {
    TBSYS_LOG(ERROR, "get rpc buffer failed, rc %d", rc);
  }
  else if (OB_SUCCESS != (rc = param.serialize(buffer.get_data(), buffer.get_capacity(),
          buffer.get_position())))
  {
    TBSYS_LOG(ERROR, "serialize sstable scan param failed, rc %d", rc);
  }
  else if (OB_SUCCESS != (rc = rpc_frame_->send_request(server, OB_SSTABLE_SCAN_REQUEST,
          DEFAULT_VERSION, timeout, buffer, session_id)))
  {
    TBSYS_LOG(ERROR, "send sstable scan request failed, rc %d", rc);
  }
  else if (OB_SUCCESS != (rc = deserialize_result_1(buffer, pos, result_code, new_scanner)))
  {
    TBSYS_LOG(ERROR, "deserialize result failed %d", rc);
  }

  return rc;
}

int ObSqlRpcStub::get_next_session_scanner(const int64_t timeout, const ObServer &server,
    const int64_t session_id, ObNewScanner &new_scanner) const
{
  int rc = OB_SUCCESS;
  ObResultCode result_code;
  ObDataBuffer in_buffer;
  ObDataBuffer out_buffer;
  int64_t pos = 0;

  new_scanner.reuse();
  if (OB_SUCCESS != (rc = get_rpc_buffer(out_buffer)))
  {
    TBSYS_LOG(ERROR, "get rpc buffer failed, rc %d", rc);
  }
  else if (OB_SUCCESS != (rc = rpc_frame_->get_next(server, session_id, timeout,
          in_buffer, out_buffer)))
  {
    TBSYS_LOG(ERROR, "send next request failed, rc %d", rc);
  }
  else if (OB_SUCCESS != (rc = deserialize_result_1(out_buffer, pos, result_code, new_scanner)))
  {
    TBSYS_LOG(ERROR, "deserialize result failed %d", rc);
  }

  return rc;
}
int ObSqlRpcStub::end_next_session(const int64_t timeout, ObServer &server,
    const int64_t session_id) const
{
  ObDataBuffer buffer;

  return rpc_frame_->post_end_next(server,session_id, timeout, buffer, NULL, NULL);
}

