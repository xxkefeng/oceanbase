/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ups_rpc_stub.h,v 0.1 2010/09/27 16:59:49 chuanhui Exp $
 *
 * Authors:
 *   rizhao <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */

#include "ob_common_rpc_stub.h"
#include "common/utility.h"
#include "sql/ob_sql_result_set.h"
using namespace oceanbase::common;

const int32_t ObCommonRpcStub :: DEFAULT_VERSION = 1;
const int64_t ObCommonRpcStub :: DEFAULT_RPC_TIMEOUT_US = 1 * 1000 * 1000;
ObCommonRpcStub :: ObCommonRpcStub()
{
}

ObCommonRpcStub :: ~ObCommonRpcStub()
{
}

int ObCommonRpcStub :: init(const ObClientManager* client_mgr)
{
  int err = OB_SUCCESS;

  if (NULL == client_mgr)
  {
    TBSYS_LOG(WARN, "invalid param, client_mgr=%p", client_mgr);
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = ObRpcStub::init(&thread_buffer_, client_mgr)))
  {
    TBSYS_LOG(WARN, "base init err=%d", err);
  }

  return err;
}

int ObCommonRpcStub :: ups_report_slave_failure(const common::ObServer &slave_add, const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  UNUSED(slave_add);
  UNUSED(timeout_us);
  return err;
}

int ObCommonRpcStub :: send_log(const ObServer& ups_slave, ObDataBuffer& log_data,
    const int64_t timeout_us)
{
  int err = OB_SUCCESS;
  ObDataBuffer out_buff;

  err = get_rpc_buffer(out_buff);

  // step 1. send log data to slave
  if (OB_SUCCESS == err)
  {
    err = rpc_frame_->send_request(ups_slave,
        OB_SEND_LOG, DEFAULT_VERSION, timeout_us, log_data, out_buff);
    if (err != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "send log data to slave failed "
          "data_len[%ld] err[%d].", log_data.get_position(), err);
    }
  }

  // step 2. deserialize the response code
  int64_t pos = 0;
  if (OB_SUCCESS == err)
  {
    ObResultCode result_code;
    err = result_code.deserialize(out_buff.get_data(), out_buff.get_position(), pos);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, err);
    }
    else
    {
      err = result_code.result_code_;
    }
  }

  return err;
}

int ObCommonRpcStub :: renew_lease(const common::ObServer& master,
    const common::ObServer& slave_addr, const int64_t timeout_us)
{
  return send_1_return_0(master, timeout_us, OB_RENEW_LEASE_REQUEST, DEFAULT_VERSION, slave_addr);
}

int ObCommonRpcStub :: grant_lease(const common::ObServer& slave,
    const ObLease& lease, const int64_t timeout_us)
{
  return send_1_return_0(slave, timeout_us, OB_GRANT_LEASE_REQUEST, DEFAULT_VERSION, lease);
}

int ObCommonRpcStub::slave_quit(const common::ObServer& master, const common::ObServer& slave_addr,
    const int64_t timeout_us)
{
  return send_1_return_0(master, timeout_us, OB_SLAVE_QUIT, DEFAULT_VERSION, slave_addr);
}

int ObCommonRpcStub :: get_master_ups_info(const ObServer& rs, ObServer &master_ups, const int64_t timeout_us)
{
  return send_0_return_1(rs, timeout_us, OB_GET_UPDATE_SERVER_INFO, DEFAULT_VERSION, master_ups);
}

int ObCommonRpcStub :: get_obi_role(const common::ObServer& rs, common::ObiRole &obi_role, const int64_t timeout_us)
{
  return send_0_return_1(rs, timeout_us, OB_GET_OBI_ROLE, DEFAULT_VERSION, obi_role);
}

int ObCommonRpcStub :: send_obi_role(const common::ObServer& slave, const common::ObiRole obi_role)
{
  return send_1_return_0(slave, DEFAULT_RPC_TIMEOUT_US, OB_SET_OBI_ROLE_TO_SLAVE, DEFAULT_VERSION, obi_role);
}

int ObCommonRpcStub :: send_keep_alive(const common::ObServer &slave)
{
  UNUSED(slave);
  return OB_SUCCESS;
}

// copy from ob_client_helper
int ObCommonRpcStub::scan(const ObServer& ms, const ObScanParam& scan_param, ObScanner& scanner, const int64_t timeout)
{
  return send_1_return_1(ms, timeout, OB_SCAN_REQUEST, DEFAULT_VERSION, scan_param, scanner);
}

int ObCommonRpcStub::mutate(const ObServer& update_server, const ObMutator& mutator, const int64_t timeout)
{
  return send_1_return_0(update_server, timeout, OB_WRITE, DEFAULT_VERSION, mutator);
}

int ObCommonRpcStub :: renew_lease(const common::ObServer &rootserver)
{
  UNUSED(rootserver);
  return OB_SUCCESS;
}

int ObCommonRpcStub::execute_sql( const ObServer & ms, const ObString &sql_str, 
    sql::ObSQLResultSet& result, const int64_t timeout) const
{
  return send_1_return_1(ms, timeout, OB_SQL_EXECUTE, DEFAULT_VERSION, sql_str, result);
}

int ObCommonRpcStub::execute_sql(const common::ObServer& ms, 
    const common::ObString sql, const int64_t timeout) const
{
  return send_1_return_0(ms, timeout, OB_SQL_EXECUTE, DEFAULT_VERSION, sql);
}

const ObClientManager* ObCommonRpcStub::get_client_mgr() const
{
  return rpc_frame_;
}
