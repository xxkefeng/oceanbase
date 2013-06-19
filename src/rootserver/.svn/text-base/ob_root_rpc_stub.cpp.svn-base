#include "rootserver/ob_root_worker.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "common/ob_schema.h"
#include "common/ob_define.h"
#include "common/ob_tbnet_callback.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootRpcStub::ObRootRpcStub()
{
}

ObRootRpcStub::~ObRootRpcStub()
{
}

int ObRootRpcStub::init(const ObClientManager *client_mgr, common::ThreadSpecificBuffer* tsbuffer)
{
  UNUSED(tsbuffer);
  ObCommonRpcStub::init(client_mgr);
  return OB_SUCCESS;
}

int ObRootRpcStub::slave_register(const ObServer& master, const ObServer& slave_addr,
    ObFetchParam& fetch_param, const int64_t timeout)
{
  return send_1_return_1(master, timeout, OB_SLAVE_REG, DEFAULT_VERSION, slave_addr, fetch_param);
}


int ObRootRpcStub::set_obi_role(const common::ObServer& ups, const common::ObiRole& role, const int64_t timeout_us)
{
  return send_1_return_0(ups, timeout_us, OB_SET_OBI_ROLE, DEFAULT_VERSION, role);
}

int ObRootRpcStub::switch_schema(const common::ObServer& server, const common::ObSchemaManagerV2& schema_manager, const int64_t timeout_us)
{
  return send_1_return_0(server, timeout_us, OB_SWITCH_SCHEMA, DEFAULT_VERSION, schema_manager);
}

int ObRootRpcStub::migrate_tablet(const common::ObServer& src_cs, const common::ObServer& dest_cs, const common::ObNewRange& range, bool keep_src, const int64_t timeout_us)
{
  return send_3_return_0(src_cs, timeout_us, OB_CS_MIGRATE, DEFAULT_VERSION, range, dest_cs, keep_src);
}

int ObRootRpcStub::create_tablet(const common::ObServer& cs, const common::ObNewRange& range, const int64_t mem_version, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = send_2_return_0(cs, timeout_us, OB_CS_CREATE_TABLE, DEFAULT_VERSION, range, mem_version)))
  {
    TBSYS_LOG(WARN, "failed to create tablet, err=%d, cs=%s, range=%s", ret, cs.to_cstring(), to_cstring(range));
  }
  return ret;
}

int ObRootRpcStub::delete_tablets(const common::ObServer& cs, const common::ObTabletReportInfoList &tablets, const int64_t timeout_us)
{
  return send_1_return_0(cs, timeout_us, OB_CS_DELETE_TABLETS, DEFAULT_VERSION, tablets);
}

int ObRootRpcStub::import_tablets(const common::ObServer& cs, const uint64_t table_id, const int64_t version, const int64_t timeout_us)
{
  return send_2_return_0(cs, timeout_us, OB_CS_IMPORT_TABLETS, DEFAULT_VERSION, (int64_t)table_id, version);
}

int ObRootRpcStub::get_last_frozen_version(const common::ObServer& ups, const int64_t timeout_us, int64_t &frozen_version)
{
  return send_0_return_1(ups, timeout_us, OB_UPS_GET_LAST_FROZEN_VERSION, DEFAULT_VERSION, frozen_version);
}

int ObRootRpcStub::get_obi_role(const common::ObServer& master, const int64_t timeout_us, common::ObiRole &obi_role)
{
  return send_0_return_1(master, timeout_us, OB_GET_OBI_ROLE, DEFAULT_VERSION, obi_role);
}

int ObRootRpcStub::heartbeat_to_cs(const common::ObServer& cs,
    const int64_t lease_time,
    const int64_t frozen_mem_version,
    const int64_t schema_version,
    const int64_t config_version)
{
  static const int MY_VERSION = 3;
  return post_request_4(cs, DEFAULT_RPC_TIMEOUT_US,
      OB_REQUIRE_HEARTBEAT, MY_VERSION,
      ObTbnetCallback::default_callback, NULL,
      lease_time, frozen_mem_version, schema_version, config_version);
}

int ObRootRpcStub::heartbeat_to_ms(const common::ObServer& ms,
    const int64_t lease_time,
    const int64_t frozen_mem_version,
    const int64_t schema_version,
    const common::ObiRole &role,
    const int64_t privilege_version,
    const int64_t config_version)
{
  static const int MY_VERSION = 4;
  return post_request_6(ms, DEFAULT_RPC_TIMEOUT_US,
      OB_REQUIRE_HEARTBEAT, MY_VERSION,
      ObTbnetCallback::default_callback, NULL,
      lease_time, frozen_mem_version, schema_version,
      role, privilege_version, config_version);
}

int ObRootRpcStub::grant_lease_to_ups(const common::ObServer& ups, ObMsgUpsHeartbeat &msg)
{
  return post_request_1(ups, DEFAULT_RPC_TIMEOUT_US, OB_RS_UPS_HEARTBEAT,
      msg.MY_VERSION, ObTbnetCallback::default_callback, NULL, msg);
}

int ObRootRpcStub::request_report_tablet(const common::ObServer& chunkserver)
{
  int64_t dummy = 0;
  return post_request_1(chunkserver, DEFAULT_RPC_TIMEOUT_US, OB_RS_REQUEST_REPORT_TABLET,
      DEFAULT_VERSION, ObTbnetCallback::default_callback, NULL, dummy);
}

int ObRootRpcStub::revoke_ups_lease(const common::ObServer& ups, const int64_t lease, const common::ObServer& master, const int64_t timeout_us)
{
  ObMsgRevokeLease msg;
  msg.lease_ = lease;
  msg.ups_master_ = master;
  return send_1_return_0(ups, timeout_us, OB_RS_UPS_REVOKE_LEASE, msg.MY_VERSION, msg);
}

int ObRootRpcStub::get_ups_max_log_seq(const common::ObServer& ups, uint64_t &max_log_seq, const int64_t timeout_us)
{
  int64_t log_seq = 0;
  int ret = send_0_return_1(ups, timeout_us, OB_RS_GET_MAX_LOG_SEQ, DEFAULT_VERSION, log_seq);
  if (OB_SUCCESS != ret) max_log_seq = log_seq;
  return ret;
}

int ObRootRpcStub::shutdown_cs(const common::ObServer& cs, bool is_restart, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObDataBuffer msgbuf;
  if (NULL == rpc_frame_)
  {
    TBSYS_LOG(ERROR, "rpc_frame_=NULL");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = get_rpc_buffer(msgbuf)))
  {
    TBSYS_LOG(ERROR, "failed to get thread buffer, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), is_restart ? 1 : 0)))
  {
    TBSYS_LOG(ERROR, "encode is_restart fail:ret[%d], is_restart[%d]", ret, is_restart ? 1 : 0);
  }
  else if (OB_SUCCESS != (ret = rpc_frame_->send_request(cs, OB_STOP_SERVER, DEFAULT_VERSION, timeout_us, msgbuf)))
  {
    TBSYS_LOG(WARN, "failed to send request, err=%d", ret);
  }
  else
  {
    // success
    ObResultCode result;
    int64_t pos = 0;
    if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
    {
      TBSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != result.result_code_)
    {
      TBSYS_LOG(WARN, "failed to restart, err=%d server=%s", result.result_code_, cs.to_cstring());
      ret = result.result_code_;
    }
  }
  return ret;
}
int ObRootRpcStub::get_split_range(const common::ObServer& ups, const int64_t timeout_us,
    const uint64_t table_id, const int64_t frozen_version, ObTabletInfoList &tablets)
{
  return send_2_return_1(ups, timeout_us, OB_RS_FETCH_SPLIT_RANGE, DEFAULT_VERSION,
      frozen_version, (int64_t)table_id, tablets);
}
int ObRootRpcStub::table_exist_in_cs(const ObServer &cs, const int64_t timeout_us,
    const uint64_t table_id, bool &is_exist_in_cs)
{
  int ret = OB_SUCCESS;
  ret = send_1_return_0(cs, timeout_us, OB_CS_CHECK_TABLET, DEFAULT_VERSION, (int64_t)table_id);
  if (OB_CS_TABLET_NOT_EXIST == ret)
  {
    ret = OB_SUCCESS;
    is_exist_in_cs = false;
  }
  else if (OB_SUCCESS == ret)
  {
    is_exist_in_cs = true;
  }
  else
  {
    TBSYS_LOG(WARN, "fail to check cs tablet. table_id=%lu, cs_addr=%s, err=%d",
        table_id, cs.to_cstring(), ret);
  }
  return ret;
}

int ObRootRpcStub::request_cs_load_bypass_tablet(const common::ObServer& chunkserver,
    const common::ObTableImportInfoList &import_info, const int64_t timeout_us)
{
  return send_1_return_0(chunkserver, timeout_us, OB_CS_LOAD_BYPASS_SSTABLE, DEFAULT_VERSION, import_info);
}
int ObRootRpcStub::request_cs_delete_table(const common::ObServer& chunkserver,
    const uint64_t table_id, const int64_t timeout_us)
{
  return send_1_return_0(chunkserver, timeout_us, OB_CS_DELETE_TABLE, DEFAULT_VERSION, (int64_t)table_id);
}

int ObRootRpcStub::request_cs_build_sample(
    const ObServer& chunkserver, const int64_t timeout_us,
    const int64_t frozen_version, const uint64_t data_table_id, 
    const uint64_t index_table_id, const int64_t sample_count)
{
  return send_4_return_0(chunkserver, timeout_us, OB_CS_SAMPLE_TABLE, DEFAULT_VERSION, 
                         frozen_version, (int64_t)data_table_id, (int64_t)index_table_id, sample_count);
}

int ObRootRpcStub::request_cs_build_index(
    const ObServer& chunkserver, const int64_t timeout_us,
    const int64_t frozen_version, const uint64_t data_table_id, const uint64_t index_table_id)
{
  return send_3_return_0(chunkserver, timeout_us, OB_CS_BUILD_INDEX, DEFAULT_VERSION, 
                         frozen_version, (int64_t)data_table_id, (int64_t)index_table_id);
}
