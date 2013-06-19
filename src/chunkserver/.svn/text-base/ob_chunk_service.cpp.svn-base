/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     Author Name <email address>
 *        - some work details if you want
 */

#include "ob_chunk_service.h"
#include "ob_chunk_server.h"
#include "common/ob_atomic.h"
#include "common/utility.h"
#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_packet.h"
#include "common/ob_read_common_data.h"
#include "common/ob_scanner.h"
#include "common/ob_new_scanner.h"
#include "common/ob_range2.h"
#include "common/ob_result.h"
#include "common/file_directory_utils.h"
#include "common/ob_trace_log.h"
#include "common/ob_schema_manager.h"
#include "common/ob_version.h"
#include "common/ob_profile_log.h"
#include "sql/ob_sql_scan_param.h"
#include "sstable/ob_disk_path.h"
#include "sstable/ob_aio_buffer_mgr.h"
#include "ob_tablet.h"
#include "ob_chunk_server_main.h"
#include "ob_query_service.h"
#include "ob_sql_query_service.h"
#include "ob_tablet_service.h"
#include "ob_sql_rpc_stub.h"
#include "ob_cs_plan_executor.h"
#include "ob_cs_phy_operator_factory.h"
#include "common/ob_common_stat.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::compactsstable;

namespace oceanbase
{
  namespace chunkserver
  {
    ObChunkService::ObChunkService()
    : chunk_server_(NULL), inited_(false),
      service_started_(false), in_register_process_(false),
      service_expired_time_(0),
      migrate_task_count_(0), lease_checker_(this), merge_task_(this),
      fetch_ups_task_(this)

    {
    }

    ObChunkService::~ObChunkService()
    {
      destroy();
    }

    /**
     * use ObChunkService after initialized.
     */
    int ObChunkService::initialize(ObChunkServer* chunk_server)
    {
      int ret = OB_SUCCESS;

      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (NULL == chunk_server)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = timer_.init()))
      {
        TBSYS_LOG(ERROR, "init timer fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = ms_list_task_.init(
                                chunk_server->get_config().get_root_server(),
                                &chunk_server->get_client_manager(),
                                false)))
      {
        TBSYS_LOG(ERROR, "init ms list failt, ret: [%d]", ret);
      }
      else if (OB_SUCCESS !=
               (ret = chunk_server->get_config_mgr().init(
                 ms_list_task_, chunk_server->get_client_manager(), timer_)))
      {
        TBSYS_LOG(ERROR, "init chunk server config error, ret: [%d]", ret);
      }
      else
      {
        chunk_server_ = chunk_server;
        inited_ = true;
      }

      return ret;
    }

    /*
     * stop service, before chunkserver stop working thread.
     */
    int ObChunkService::destroy()
    {
      int rc = OB_SUCCESS;
      if (inited_)
      {
        inited_ = false;
        timer_.destroy();
        service_started_ = false;
        in_register_process_ = false;
        chunk_server_ = NULL;
      }
      else
      {
        rc = OB_NOT_INIT;
      }

      return rc;
    }

    /**
     * ChunkServer must fetch schema from RootServer first.
     * then provide service.
     */
    int ObChunkService::start()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        rc = OB_NOT_INIT;
      }
      else
      {
        rc = chunk_server_->init_merge_join_rpc();
        if (rc != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "init merge join rpc failed.");
        }
      }

      if (OB_SUCCESS == rc)
      {
        rc = check_compress_lib(chunk_server_->get_config().check_compress_lib);
        if (OB_SUCCESS != rc)
        {
          TBSYS_LOG(ERROR, "the compress lib in the list is not exist: rc=[%d]", rc);
        }
      }

      if (OB_SUCCESS == rc)
      {
        rc = load_tablets();
        if (OB_SUCCESS != rc)
        {
          TBSYS_LOG(ERROR, "load local tablets error, rc=%d", rc);
        }
      }

      if (OB_SUCCESS == rc)
      {
        rc = register_self();
      }

      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(lease_checker_,
            chunk_server_->get_config().lease_check_interval, false);
      }

      if (OB_SUCCESS == rc)
      {
        //for the sake of simple,just update the stat per second
        rc = timer_.schedule(stat_updater_,1000000,true);
      }

      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(fetch_ups_task_,
          chunk_server_->get_config().fetch_ups_interval, false);
      }

      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(time_update_duty_, TimeUpdateDuty::SCHEDULE_PERIOD, true);
      }

      if (OB_SUCCESS == rc)
      {
        rc = timer_.schedule(ms_list_task_, MsList::SCHEDULE_PERIOD, true);
      }

      if (OB_SUCCESS == rc)
      {
        ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
        rc = tablet_manager.start_merge_thread();
        if (rc != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR,"start merge thread failed.");
        }

        int64_t bypass_loader_threads = chunk_server_->get_config().bypass_sstable_loader_thread_num;
        if (OB_SUCCESS == rc && bypass_loader_threads > 0)
        {
          if (OB_SUCCESS != (rc = tablet_manager.start_bypass_loader_thread()))
          {
            TBSYS_LOG(ERROR, "start bypass sstable loader threads failed, ret=%d", rc);
          }
        }

        int64_t build_index_threads = chunk_server_->get_config().index_builder_thread_num;
        if (OB_SUCCESS == rc && build_index_threads > 0)
        {
          if (OB_SUCCESS != (rc = tablet_manager.start_build_index_thread()))
          {
            TBSYS_LOG(ERROR, "start build index threads failed, ret=%d", rc);
          }
        }
      }

      return rc;
    }

    int ObChunkService::check_compress_lib(const char* compress_name_buf)
    {
      int rc = OB_SUCCESS;
      char temp_buf[OB_MAX_VARCHAR_LENGTH];
      ObCompressor* compressor = NULL;
      char* ptr = NULL;
      
      if (NULL == compress_name_buf)
      {
        TBSYS_LOG(ERROR, "compress name buf is NULL");
        rc = OB_INVALID_ARGUMENT;
      }
      else if (static_cast<int64_t>(strlen(compress_name_buf)) >= OB_MAX_VARCHAR_LENGTH)
      {
        TBSYS_LOG(ERROR, "compress name length >= OB_MAX_VARCHAR_LENGTH: compress name length=[%d]", static_cast<int>(strlen(compress_name_buf)));
        rc = OB_INVALID_ARGUMENT;
      }
      else
      { 
        memcpy(temp_buf, compress_name_buf, strlen(compress_name_buf) + 1);
      }

      if (OB_SUCCESS == rc)
      {
        char* save_ptr = NULL;
        ptr = strtok_r(temp_buf, ":", &save_ptr);
        while (NULL != ptr)
        {
          if (NULL == (compressor = create_compressor(ptr)))
          {
            TBSYS_LOG(ERROR, "create compressor error: ptr=[%s]", ptr);
            rc = OB_ERROR;
            break;
          }
          else
          {
            destroy_compressor(compressor);
            TBSYS_LOG(INFO, "create compressor success: ptr=[%s]", ptr);
            ptr = strtok_r(NULL, ":", &save_ptr);
          }
        }
      }

      return rc;
    }

    int ObChunkService::load_tablets()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        rc = OB_NOT_INIT;
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();

      // load tablets;
      if (OB_SUCCESS == rc)
      {
        rc = tablet_manager.load_tablets();
      }

      return rc;
    }

    int ObChunkService::register_self_busy_wait(int32_t &status)
    {
      int rc = OB_SUCCESS;
      status = 0;
      char server_version[OB_SERVER_VERSION_LENGTH] = "";
      get_package_and_svn(server_version, sizeof(server_version));

      while (inited_)
      {
        rc = CS_RPC_CALL_RS(register_server, chunk_server_->get_self(), false, status, server_version);
        if (OB_SUCCESS == rc) break;
        if (OB_RESPONSE_TIME_OUT != rc && OB_NOT_INIT != rc)
        {
          TBSYS_LOG(ERROR, "register self to rootserver failed, rc=%d", rc);
          break;
        }
        usleep(static_cast<useconds_t>(chunk_server_->get_config().network_timeout));
      }
      return rc;
    }

    int ObChunkService::report_tablets_busy_wait()
    {
      int rc = OB_SUCCESS;
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      while (inited_ && (service_started_ || (in_register_process_ && !service_started_)))
      {
        rc = tablet_manager.report_tablets();
        if (OB_SUCCESS == rc) break;
        if (OB_RESPONSE_TIME_OUT != rc && OB_CS_EAGAIN != rc)
        {
          TBSYS_LOG(ERROR, "report tablets to rootserver failed, rc=%d", rc);
          break;
        }
        usleep(static_cast<useconds_t>(chunk_server_->get_config().network_timeout));
      }
      return rc;
    }

    int ObChunkService::fetch_schema_busy_wait(ObSchemaManagerV2 *schema)
    {
      int rc = OB_SUCCESS;
      if (NULL == schema)
      {
        TBSYS_LOG(ERROR,"invalid argument,sceham is null");
        rc = OB_INVALID_ARGUMENT;
      }
      else
      {
        while (inited_)
        {
          CS_RPC_CALL_RS(fetch_schema, 0, false, *schema);
          if (OB_SUCCESS == rc) break;
          if (OB_RESPONSE_TIME_OUT != rc)
          {
            TBSYS_LOG(ERROR, "report tablets to rootserver failed, rc=%d", rc);
            break;
          }
          usleep(static_cast<useconds_t>(chunk_server_->get_config().network_timeout));
        }
      }
      return rc;
    }

    int ObChunkService::register_self()
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot register_self.");
        rc = OB_NOT_INIT;
      }

      if (in_register_process_)
      {
        TBSYS_LOG(ERROR, "another thread is registering.");
        rc = OB_ERROR;
      }
      else
      {
        in_register_process_ = true;
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      //const ObChunkServerParam & param = chunk_server_->get_config();
      //ObRootServerRpcStub & rs_rpc_stub = chunk_server_->get_rs_rpc_stub();
      int32_t status = 0;
      // register self to rootserver until success.
      if (OB_SUCCESS == rc)
      {
        rc = register_self_busy_wait(status);
      }

      if (OB_SUCCESS == rc)
      {
        // TODO init lease 10s for first startup.
        service_expired_time_ = tbsys::CTimeUtil::getTime() + 10000000;
        int64_t current_data_version = tablet_manager.get_last_not_merged_version();
        if (0 == status)
        {
          TBSYS_LOG(INFO, "system startup on first time, wait rootserver start new schema,"
              "current data version=%ld", current_data_version);
          // start chunkserver on very first time, do nothing, wait rootserver
          // launch the start_new_schema process.
          //service_started_ = true;
        }
        else
        {
          TBSYS_LOG(INFO, "chunk service start, current data version: %ld", current_data_version);
          rc = report_tablets_busy_wait();
          if (OB_SUCCESS == rc)
          {
            tablet_manager.report_capacity_info();
            service_started_ = true;
          }
        }
      }

      in_register_process_ = false;

      return rc;
    }

    /*
     * after initialize() & start(), then can handle the request.
     */
    int ObChunkService::do_request(
        const int64_t receive_time,
        const int32_t packet_code,
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      UNUSED(receive_time);
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "service not initialized, cannot accept any message.");
        rc = OB_NOT_INIT;
      }

      if (OB_SUCCESS == rc)
      {
        if (!service_started_
            && packet_code != OB_START_MERGE
            && packet_code != OB_REQUIRE_HEARTBEAT)
        {
          TBSYS_LOG(ERROR, "service not started, only accept "
              "start schema message or heatbeat from rootserver.");
          rc = OB_CS_SERVICE_NOT_STARTED;
        }
      }
      if (OB_SUCCESS == rc)
      {
        //check lease valid.
        if (!is_valid_lease()
            && (!in_register_process_)
            && packet_code != OB_REQUIRE_HEARTBEAT)
        {
          // TODO re-register self??
          TBSYS_LOG(WARN, "lease expired, wait timer schedule re-register self to rootserver.");
        }
      }

      if (OB_SUCCESS != rc)
      {
        TBSYS_LOG(ERROR, "call func error packet_code is %d return code is %d",
            packet_code, rc);
        common::ObResultCode result;
        result.result_code_ = rc;
        // send response.
        int serialize_ret = result.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
        }
        else
        {
          chunk_server_->send_response(
              packet_code + 1, version,
              out_buffer, req, channel_id);
        }
      }
      else
      {
        switch(packet_code)
        {
          case OB_SQL_GET_REQUEST:
            rc = cs_sql_get(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_SQL_SCAN_REQUEST:
            rc = cs_sql_scan(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_PHY_PLAN_EXECUTE:
            rc = cs_plan_execute(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_GET_REQUEST:
          case OB_BATCH_GET_REQUEST:
          case OB_SCAN_REQUEST:
            rc = OB_NOT_SUPPORTED;
            break;
          case OB_DROP_OLD_TABLETS:
            rc = cs_drop_old_tablets(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_REQUIRE_HEARTBEAT:
            rc = cs_heart_beat(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_MIGRATE:
            rc = cs_migrate_tablet(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_MIGRATE_OVER:
            rc = cs_load_tablet(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_DELETE_TABLETS:
            rc = cs_delete_tablets(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_CREATE_TABLE:
            rc = cs_create_tablet(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_SWITCH_SCHEMA:
            rc = cs_accept_schema(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_GET_MIGRATE_DEST_LOC:
            rc = cs_get_migrate_dest_loc(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_DUMP_TABLET_IMAGE:
            rc = cs_dump_tablet_image(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_FETCH_STATS:
            rc = cs_fetch_stats(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_START_GC:
            rc = cs_start_gc(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_UPS_RELOAD_CONF:
            rc = cs_reload_conf(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_SHOW_PARAM:
            rc = cs_show_param(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_STOP_SERVER:
            rc = cs_stop_server(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CHANGE_LOG_LEVEL:
            rc = cs_change_log_level(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_RS_REQUEST_REPORT_TABLET:
            rc = cs_force_to_report_tablet(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_CHECK_TABLET:
            rc = cs_check_tablet(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_MERGE_TABLETS:
            rc = cs_merge_tablets(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_SEND_FILE_REQUEST:
            rc = cs_send_file(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_SYNC_ALL_IMAGES:
            rc = cs_sync_all_images(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_LOAD_BYPASS_SSTABLE:
            rc = cs_load_bypass_sstables(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_DELETE_TABLE:
            rc = cs_delete_table(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_SAMPLE_TABLE:
            rc = cs_build_sample(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_BUILD_INDEX:
            rc = cs_build_index(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_STOP_BUILD_INDEX:
            rc = cs_stop_build_index(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_CS_FETCH_SSTABLE_DIST:
            rc = cs_fetch_sstable_dist(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_SET_CONFIG:
            rc = cs_set_config(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_GET_CONFIG:
            rc = cs_get_config(version, channel_id, req, in_buffer, out_buffer);
            break;
          case OB_SSTABLE_SCAN_REQUEST:
            rc = cs_sstable_scan(version, channel_id, req, in_buffer, out_buffer, timeout_time);
            break;
          case OB_CS_DISK_MAINTAIN:
            rc = cs_disk_maintain(version, channel_id, req, in_buffer, out_buffer);
            break;
          default:
            rc = OB_ERROR;
            TBSYS_LOG(WARN, "not support packet code[%d]", packet_code);
            break;
        }
      }
      return rc;
    }

    int ObChunkService::cs_send_file(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      return chunk_server_->get_file_service().handle_send_file_request(
          version, channel_id, req, in_buffer, out_buffer);
    }

    int ObChunkService::get_sql_query_service(ObSqlQueryService *&service)
    {
      int ret = OB_SUCCESS;
      service = GET_TSI_ARGS(ObTabletService, TSI_CS_TABLET_SERVICE_1, *chunk_server_);
      if (NULL == service)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "get thread specific ObSqlQueryService fail:ret[%d]", ret);
      }

      return ret;
    }

    int ObChunkService::reset_internal_status()
    {
      int ret = OB_SUCCESS;

      ret = reset_query_thread_local_buffer();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to reset query thread local buffer");
      }

      ret = wait_aio_buffer();
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to wait aio buffer free");
      }

      return ret;
    }

    int ObChunkService::cs_plan_execute(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    { 
      // TODO: need to get tableid, range, and type(scan or get)
      int CS_PLAN_EXECUTE_VERSION = 1;
      int ret = OB_SUCCESS;
      if (CS_PLAN_EXECUTE_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      UNUSED(req);
      UNUSED(in_buffer);
      UNUSED(out_buffer);
      UNUSED(timeout_time);

      common::ModulePageAllocator mod(common::ObModIds::OB_CS_PLAN_EXECUTE); // TODO
      common::ModuleArena allocator(2 * OB_MAX_PACKET_LENGTH, mod);// TODO
      sql::ObPhysicalPlan phy_plan;
      ObPlanContext context;
      ObCsPhyOperatorFactory phy_operator_factory; //TODO
      const ObCurRowkeyInterface *cur_rowkey_op = NULL;
      const ObSchemaManagerV2 *schema_mgr = NULL;
      common::ObMergerSchemaManager *merger_schema_mgr = NULL;

      if (OB_SUCCESS == ret)
      {
        if(NULL == (merger_schema_mgr = chunk_server_->get_schema_manager()))
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "chunk server get schema manager fail:ret[%d]", ret);
        }
        else if(NULL == (schema_mgr = merger_schema_mgr->get_schema((int64_t)0)))
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "get user schema mgr fail:ret[%d]", ret);
        }

        ret = phy_operator_factory.set_param(&context, &chunk_server_->get_tablet_manager(),
                              chunk_server_->get_rpc_proxy(), schema_mgr);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to set phy_operator_factory param, ret=%d", ret);
        }
      }

      // rebuild physical plan
      if (OB_SUCCESS == ret)
      {
        allocator.reuse();
        phy_plan.clear();
        phy_plan.set_allocator(&allocator);
        phy_plan.set_operator_factory(&phy_operator_factory);
        ret = phy_plan.deserialize(in_buffer.get_data(), in_buffer.get_capacity(), in_buffer.get_position());

        if (OB_SUCCESS == ret)
        {
          cur_rowkey_op = context.cur_rowkey_op_;
          if (NULL == cur_rowkey_op)
          {
            TBSYS_LOG(ERROR, "cur_rowkey_op must not null");
            ret = OB_ERR_UNEXPECTED;
          }
        }
        else
        {
          TBSYS_LOG(WARN, "failed to deserialize phy pan, ret=%d", ret);
        }
      }

      FILL_TRACE_LOG("deserialize_phy_plan_done");
      
      // execute
      int64_t start_time = tbsys::CTimeUtil::getTime();
      uint64_t  table_id = OB_INVALID_ID; //for stat
      bool is_scan = true; //for stat
      ObNewScanner* new_scanner = GET_TSI_MULT(ObNewScanner, TSI_CS_NEW_SCANNER_1);
      ObCsPlanExecutor* plan_executor= GET_TSI_MULT(ObCsPlanExecutor, TSI_CS_PLAN_EXECUTOR_1);
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      int64_t packet_cnt = 0;
      bool is_last_packet = false;
      bool is_fullfilled = true;
      int64_t fullfilled_num = 0;
      int64_t ups_data_version = 0;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread = chunk_server_->get_default_task_queue_thread();

      INIT_PROFILE_LOG_TIMER();
      // set page size to 2MB in begining..
      reset_query_thread_local_buffer();

      if (NULL == new_scanner || NULL == plan_executor)
      {
        TBSYS_LOG(ERROR, "failed to get thread local plan_executor or new scanner,"
            "plan_executor=%p new_scanner=%p", plan_executor, new_scanner);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        new_scanner->reuse();
        plan_executor->reset();
      }

      if (OB_SUCCESS == ret)
      {
        plan_executor->set_timeout_us(timeout_time);
        ret = plan_executor->open(phy_plan, context);
        PROFILE_LOG_TIME(DEBUG, "open plan_executor complete, rc=%d", ret);

        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "open plan_executor fail:err[%d]", ret);
        }
        FILL_TRACE_LOG("execute_cs_plan_done");

        if(OB_SUCCESS == ret)
        {
          ret = plan_executor->fill_scan_data(*new_scanner);
          if (OB_ITER_END == ret)
          {
            is_last_packet = true;
            ret = OB_SUCCESS;
          }
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to fill_scan_date:err[%d]", ret);
          }
          FILL_TRACE_LOG("fill_data_done");
        }
      }

      if (OB_SUCCESS == ret)
      {
        new_scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
        if (!is_fullfilled && !is_last_packet)
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      PROFILE_LOG_TIME(DEBUG, "first fill scan data, #row=%ld,  is_last_packet=%d, "
          "is_fullfilled=%ld, ret=%d, session_id=%ld.", fullfilled_num,  
          is_last_packet, is_fullfilled, ret, session_id);

      do
      {
        if (OB_SUCCESS == ret && !is_fullfilled && !is_last_packet)
        {
          ret = queue_thread.prepare_for_next_request(session_id);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to prepare_for_next_request:err[%d]", ret);
          }
        }
        // send response.
        out_buffer.get_position() = 0;
        common::ObResultCode rc;
        rc.result_code_ = ret;
        int serialize_ret = rc.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(WARN, "serialize result code object failed.");
          break ;
        }

        // if scan return success , we can return scanner.
        if (OB_SUCCESS == ret && OB_SUCCESS == serialize_ret)
        {
          serialize_ret = new_scanner->serialize(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position());
          ups_data_version = new_scanner->get_data_version();
          if (OB_SUCCESS != serialize_ret)
          {
            TBSYS_LOG(WARN, "serialize ObScanner failed, ret=%d", serialize_ret);
            break;
          }
        }

        if (OB_SUCCESS == serialize_ret)
        {
          //if (is_scan)
          //{
          //  OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_BYTES, out_buffer.get_position());
          //}
          //else
          //{
          //  OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_BYTES, out_buffer.get_position());
          //}
          chunk_server_->send_response(
              is_last_packet ? OB_SESSION_END : OB_PHY_PLAN_EXECUTE_RESPONSE, CS_PLAN_EXECUTE_VERSION,
              out_buffer, req, response_cid, session_id);
          PROFILE_LOG_TIME(DEBUG, "send response, #packet=%ld, is_last_packet=%d, session_id=%ld", 
              packet_cnt, is_last_packet, session_id);
          packet_cnt++;
        }

        if (OB_SUCCESS == ret && !is_fullfilled && !is_last_packet)
        {
          new_scanner->reuse();
          ret = queue_thread.wait_for_next_request(
            session_id, next_request, timeout_time - tbsys::CTimeUtil::getTime());
          if (OB_NET_SESSION_END == ret)
          {
            //merge server end this session
            ret = OB_SUCCESS;
            if (NULL != next_request)
            {
              req = next_request->get_request();
              easy_request_wakeup(req);
            }
            break;
          }
          else if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout, ret=%d", ret);
            break;
          }
          else
          {
            response_cid = next_request->get_channel_id();
            req = next_request->get_request();
            ret = plan_executor->fill_scan_data(*new_scanner);
            if (OB_ITER_END == ret)
            {
              is_last_packet = true;
              ret = OB_SUCCESS;
            }
            new_scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
            PROFILE_LOG_TIME(DEBUG, "next fill scan data, #row=%ld,  is_last_packet=%d, "
                "is_fullfilled=%ld, ret=%d, session_id=%ld.", fullfilled_num,  
                is_last_packet, is_fullfilled, ret, session_id);
          }
        }
        else
        {
          //error happen or fullfilled or sent last packet
          break;
        }
      } while (true);

      FILL_TRACE_LOG("send response, packet_cnt=%ld, session_id=%ld, io stat: %s, ret=%d",
          packet_cnt, session_id, get_io_stat_str(), ret);

      int64_t consume_time = tbsys::CTimeUtil::getTime() - start_time;
      if (is_scan)
      {
        OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_TIME, consume_time);
      }
      else
      {
        OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_TIME, consume_time);
      }

      //reset initernal status for next scan operator
      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }

      if(NULL != plan_executor)
      {
        plan_executor->close();
      }
      if (NULL != merger_schema_mgr && NULL != schema_mgr)
      {
        if (OB_SUCCESS != (ret = merger_schema_mgr->release_schema(schema_mgr)))
        {
          TBSYS_LOG(WARN, "fail to release schema. ret=%d, version=%ld", ret, schema_mgr->get_version());
        }
      }
      reset_internal_status();
      FILL_TRACE_LOG("end");
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();

      return ret;
    }

    int ObChunkService::cs_sql_scan(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      sql::ObSqlScanParam *sql_scan_param_ptr = GET_TSI_MULT(sql::ObSqlScanParam, TSI_CS_SQL_SCAN_PARAM_1);
      FILL_TRACE_LOG("start_cs_sql_scan");
      return cs_sql_read(version, channel_id, req, in_buffer, out_buffer, timeout_time, sql_scan_param_ptr);
    }

    int ObChunkService::cs_sql_get(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time)
    {
      sql::ObSqlGetParam *sql_get_param_ptr = GET_TSI_MULT(sql::ObSqlGetParam, TSI_CS_SQL_GET_PARAM_1);
      FILL_TRACE_LOG("start_cs_sql_get");
      return cs_sql_read(version, channel_id, req, in_buffer, out_buffer, timeout_time, sql_get_param_ptr);
    }

    int ObChunkService::cs_sql_read(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer,
        const int64_t timeout_time,
        sql::ObSqlReadParam *sql_read_param_ptr)
    {
      const int32_t CS_SCAN_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int64_t start_time = tbsys::CTimeUtil::getTime();
      uint64_t  table_id = OB_INVALID_ID; //for stat
      bool is_scan = true; //for stat
      ObNewScanner* new_scanner = GET_TSI_MULT(ObNewScanner, TSI_CS_NEW_SCANNER_1);
      int64_t session_id = 0;
      int32_t response_cid = channel_id;
      int64_t packet_cnt = 0;
      bool is_last_packet = false;
      bool is_fullfilled = true;
      int64_t fullfilled_num = 0;
      int64_t ups_data_version = 0;
      ObPacket* next_request = NULL;
      ObPacketQueueThread& queue_thread =
        chunk_server_->get_default_task_queue_thread();
      ObSqlQueryService *sql_query_service = NULL;

      // set page size to 2MB in begining..
      reset_query_thread_local_buffer();

      if (version != CS_SCAN_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == new_scanner || NULL == sql_read_param_ptr)
      {
        TBSYS_LOG(ERROR, "failed to get thread local scan_param or new scanner, "
            "new_scanner=%p, sql_read_param_ptr=%p", new_scanner, sql_read_param_ptr);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        new_scanner->reuse();
        sql_read_param_ptr->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = sql_read_param_ptr->deserialize(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_sql_scan input scan param error. ret=%d", rc.result_code_);
        }
      }
      FILL_TRACE_LOG("deserialize_param_done");

      if (OB_SUCCESS == rc.result_code_)
      {
        const ObSqlScanParam *sql_scan_param_ptr = dynamic_cast<const ObSqlScanParam *>(sql_read_param_ptr);
        is_scan = (NULL != sql_scan_param_ptr);
        table_id = sql_read_param_ptr->get_table_id();
        if (table_id == OB_ALL_SERVER_STAT_TID)
        {
          if (NULL == sql_scan_param_ptr)
          {
            rc.result_code_ = OB_NOT_SUPPORTED;
            TBSYS_LOG(WARN, "get method is not supported for all server stat table ");
          }
          else
          {
            is_last_packet = true;
            new_scanner->set_range(*sql_scan_param_ptr->get_range());
            ObStatSingleton::get_instance()->get_scanner(*new_scanner);
          }
          FILL_TRACE_LOG("get_server_stat_done");
        }
        else
        {
          if (OB_SUCCESS == rc.result_code_)
          {
            if(OB_SUCCESS != (rc.result_code_ = get_sql_query_service(sql_query_service)))
            {
              TBSYS_LOG(WARN, "get sql query service fail:ret[%d]", rc.result_code_);
            }
            else
            {
              sql_query_service->reset();
            }
          }

          if (OB_SUCCESS == rc.result_code_)
          {
            sql_query_service->set_timeout_us(timeout_time);
            table_id = sql_read_param_ptr->get_table_id();

            if (is_scan)
            {
              OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_COUNT);
            }
            else
            {
              OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_COUNT);
            }

            rc.result_code_ = sql_query_service->open(*sql_read_param_ptr);

            if(OB_SUCCESS != rc.result_code_)
            {
              TBSYS_LOG(WARN, "open query service fail:err[%d]", rc.result_code_);
            }
            FILL_TRACE_LOG("sql_query_done");
          }

          if(OB_SUCCESS == rc.result_code_)
          {
            rc.result_code_ = sql_query_service->fill_scan_data(*new_scanner);
            if (OB_ITER_END == rc.result_code_)
            {
              is_last_packet = true;
              rc.result_code_ = OB_SUCCESS;
            }
            if (OB_SUCCESS != rc.result_code_)
            {
              TBSYS_LOG(WARN, "failed to fill_scan_date:err[%d]", rc.result_code_);
            }
            FILL_TRACE_LOG("fill_data_done");
          }
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        new_scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
        if (!is_fullfilled && !is_last_packet)
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      FILL_TRACE_LOG("first fill scan data, #row=%ld,  is_last_packet=%d, "
          "is_fullfilled=%ld, ret=%d, session_id=%ld.", fullfilled_num,  
          is_last_packet, is_fullfilled, rc.result_code_, session_id);

      do
      {
        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled && !is_last_packet)
        {
          rc.result_code_ = queue_thread.prepare_for_next_request(session_id);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to prepare_for_next_request:err[%d]", rc.result_code_);
          }
        }
        // send response.
        out_buffer.get_position() = 0;
        int serialize_ret = rc.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize result code object failed.");
          break ;
        }

        // if scan return success , we can return scanner.
        if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
        {
          serialize_ret = new_scanner->serialize(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position());
          ups_data_version = new_scanner->get_data_version();
          if (OB_SUCCESS != serialize_ret)
          {
            TBSYS_LOG(ERROR, "serialize ObScanner failed.");
            break;
          }
        }

        if (OB_SUCCESS == serialize_ret)
        {
          if (is_scan)
          {
            OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_BYTES, out_buffer.get_position());
          }
          else
          {
            OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_BYTES, out_buffer.get_position());
          }
          chunk_server_->send_response(
              is_last_packet ? OB_SESSION_END : OB_SQL_SCAN_RESPONSE, CS_SCAN_VERSION,
              out_buffer, req, response_cid, session_id);
          FILL_TRACE_LOG("send response, #packet=%ld, is_last_packet=%d, session_id=%ld", 
              packet_cnt, is_last_packet, session_id);
          packet_cnt++;
        }

        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled && !is_last_packet)
        {
          new_scanner->reuse();
          rc.result_code_ = queue_thread.wait_for_next_request(
            session_id, next_request, timeout_time - tbsys::CTimeUtil::getTime());
          if (OB_NET_SESSION_END == rc.result_code_)
          {
            //merge server end this session
            rc.result_code_ = OB_SUCCESS;
            if (NULL != next_request)
            {
              req = next_request->get_request();
              easy_request_wakeup(req);
            }
            break;
          }
          else if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to wait for next reques timeout, ret=%d",
              rc.result_code_);
            break;
          }
          else
          {
            response_cid = next_request->get_channel_id();
            req = next_request->get_request();
            rc.result_code_ = sql_query_service->fill_scan_data(*new_scanner);
            if (OB_ITER_END == rc.result_code_)
            {
              /**
               * the last packet is not always with true fullfilled flag,
               * maybe there is not enough memory to query the normal scan
               * request, we just return part of result, user scan the next
               * data if necessary. it's order to be compatible with 0.2
               * version.
               */
              is_last_packet = true;
              rc.result_code_ = OB_SUCCESS;
            }
            new_scanner->get_is_req_fullfilled(is_fullfilled, fullfilled_num);
            FILL_TRACE_LOG("next fill scan data, #row=%ld,  is_last_packet=%d, "
                "is_fullfilled=%ld, ret=%d, session_id=%ld.", fullfilled_num,  
                is_last_packet, is_fullfilled, rc.result_code_, session_id);
          }
        }
        else
        {
          //error happen or fullfilled or sent last packet
          break;
        }
      } while (true);

      FILL_TRACE_LOG("scan over, packet_cnt=%ld, session_id=%ld, io stat: %s, ret=%d",
          packet_cnt, session_id, get_io_stat_str(), rc.result_code_);

      int64_t consume_time = tbsys::CTimeUtil::getTime() - start_time;
      if (is_scan)
      {
        OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_SCAN_TIME, consume_time);
      }
      else
      {
        OB_STAT_TABLE_INC(CHUNKSERVER, table_id, INDEX_GET_TIME, consume_time);
      }


      //reset initernal status for next scan operator
      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }

      if(NULL != sql_query_service)
      {
        sql_query_service->close();
      }
      reset_internal_status();

      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();

      return rc.result_code_;
    }

    int ObChunkService::cs_sstable_scan(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t *req,
        common::ObDataBuffer &in_buffer,
        common::ObDataBuffer &out_buffer,
        const int64_t timeout_time)
    {
      const int32_t CS_SSTABLE_SCAN_VERSION = 1;
      ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObSSTableScanParam *scan_param = GET_TSI_MULT(ObSSTableScanParam, TSI_CS_SSTABLE_SCAN_PARAM_1);
      ObNewScanner *new_scanner = GET_TSI_MULT(ObNewScanner, TSI_CS_NEW_SCANNER_1);

      reset_query_thread_local_buffer();

      if (CS_SSTABLE_SCAN_VERSION != version)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == new_scanner || NULL == scan_param)
      {
        TBSYS_LOG(ERROR, "failed to get thread local obj, new_scanner %p, scan_param %p",
            new_scanner, scan_param);
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        new_scanner->reuse();
        scan_param->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = scan_param->deserialize(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "deserialize sstable scan param failed, rc %d", rc.result_code_);
        }
      }

      ObSqlQueryService *query_service = NULL;
      if (OB_SUCCESS != (rc.result_code_ = get_sql_query_service(query_service)))
      {
        TBSYS_LOG(ERROR, "get query service failed");
      }
      else
      {
        if (OB_SUCCESS != (rc.result_code_ = query_service->open(*scan_param, timeout_time)))
        {
          TBSYS_LOG(ERROR, "open query service fail, rc %d", rc.result_code_);
        }
        else
        {
          rc.result_code_ = query_service->fill_scan_data(*new_scanner);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "fill scan data failed, rc %d", rc.result_code_);
          }
        }
      }

      bool is_fullfilled = true;
      int64_t fullfill_num = 0;
      int64_t session_id = 0;
      ObPacketQueueThread &queue_thread = chunk_server_->get_default_task_queue_thread();
      if (OB_SUCCESS == rc.result_code_)
      {
        new_scanner->get_is_req_fullfilled(is_fullfilled, fullfill_num);
        if (!is_fullfilled)
        {
          session_id = queue_thread.generate_session_id();
        }
      }

      int32_t cid = channel_id;
      while (true)
      {
        if (!is_fullfilled)
        {
          rc.result_code_ = queue_thread.prepare_for_next_request(session_id);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "failed to prepare_for_next_request, rc %d", rc.result_code_);
          }
        }

        out_buffer.get_position() = 0;
        int serialize_rc = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
            out_buffer.get_position());
        if (OB_SUCCESS != serialize_rc)
        {
          TBSYS_LOG(ERROR, "serialize result code obj failed, rc %d", serialize_rc);
          break;
        }
        if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_rc)
        {
          serialize_rc = new_scanner->serialize(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position());
          if (OB_SUCCESS != serialize_rc)
          {
            TBSYS_LOG(ERROR, "serialize new scanner failed, rc %d", serialize_rc);
            break;
          }
        }

        if (OB_SUCCESS == serialize_rc)
        {
          chunk_server_->send_response(is_fullfilled ? OB_SESSION_END : OB_SSTABLE_SCAN_RESPONSE,
              CS_SSTABLE_SCAN_VERSION, out_buffer, req, cid, session_id);
        }

        ObPacket *next_request = NULL;
        if (OB_SUCCESS == rc.result_code_ && !is_fullfilled)
        {
          new_scanner->reuse();
          rc.result_code_ = queue_thread.wait_for_next_request(
              session_id, next_request, timeout_time - tbsys::CTimeUtil::getTime());
          if (OB_NET_SESSION_END == rc.result_code_)
          {
            // peer end this session
            rc.result_code_ = OB_SUCCESS;
            if (next_request)
            {
              req = next_request->get_request();
              easy_request_wakeup(req);
            }
            break;
          }
          else if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(WARN, "failed to wait for next request timeout, rc %d", rc.result_code_);
            break;
          }
          else
          {
            cid = next_request->get_channel_id();
            req = next_request->get_request();
            rc.result_code_ = query_service->fill_scan_data(*new_scanner);
            new_scanner->get_is_req_fullfilled(is_fullfilled, fullfill_num);
          }
        }
        else
        {
          // error or fullfilled or sent last packet.
          break;
        }
      }

      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }

      if (query_service)
      {
        query_service->close();
      }

      reset_internal_status();

      return rc.result_code_;
    }
        
    int ObChunkService::cs_drop_old_tablets(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DROP_OLD_TABLES_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      //char msg_buff[24];
      //rc.message_.assign(msg_buff, 24);
      if (version != CS_DROP_OLD_TABLES_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      int64_t memtable_frozen_version = 0;

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = common::serialization::decode_vi64(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position(), &memtable_frozen_version);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse drop_old_tablets input memtable_frozen_version param error.");
        }
      }

      TBSYS_LOG(INFO, "drop_old_tablets: memtable_frozen_version:%ld", memtable_frozen_version);

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_DROP_OLD_TABLETS_RESPONSE,
            CS_DROP_OLD_TABLES_VERSION,
            out_buffer, req, channel_id);
      }


      // call tablet_manager_ drop tablets.
      //ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      //rc.result_code_ = tablet_manager.drop_tablets(memtable_frozen_version);

      return rc.result_code_;
    }

    /*
     * int cs_heart_beat(const int64_t lease_duration);
     */
    int ObChunkService::cs_heart_beat(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_HEART_BEAT_VERSION = 2;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      //char msg_buff[24];
      //rc.message_.assign(msg_buff, 24);
      UNUSED(channel_id);
      UNUSED(req);
      UNUSED(out_buffer);
      //if (version != CS_HEART_BEAT_VERSION)
      //{
      //  rc.result_code_ = OB_ERROR_FUNC_VERSION;
      //}

      // send heartbeat request to root_server first
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = CS_RPC_CALL_RS(heartbeat_server, chunk_server_->get_self(), OB_CHUNKSERVER);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "failed to async_heartbeat, ret=%d", rc.result_code_);
        }
      }

      // ignore the returned code of async_heartbeat
      int64_t lease_duration = 0;
      rc.result_code_ = common::serialization::decode_vi64(
          in_buffer.get_data(), in_buffer.get_capacity(),
          in_buffer.get_position(), &lease_duration);
      if (OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(ERROR, "parse cs_heart_beat input lease_duration param error.");
      }
      else
      {
        service_expired_time_ = tbsys::CTimeUtil::getTime() + lease_duration;
        TBSYS_LOG(DEBUG, "cs_heart_beat: lease_duration=%ld", lease_duration);
      }

      TBSYS_LOG(DEBUG,"cs_heart_beat,version:%d,CS_HEART_BEAT_VERSION:%d",version,CS_HEART_BEAT_VERSION);

      if (version >= CS_HEART_BEAT_VERSION)
      {
        int64_t frozen_version = 0;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = common::serialization::decode_vi64(
              in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(),&frozen_version);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse cs_heart_beat input frozen_version param error.");
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs_heart_beat: frozen_version=%ld", frozen_version);
          }
        }

        if (OB_SUCCESS == rc.result_code_ && service_started_)
        {
          int64_t wait_time = 0;
          ObTabletManager &tablet_manager = chunk_server_->get_tablet_manager();
          if ( frozen_version > chunk_server_->get_tablet_manager().get_last_not_merged_version() )
          {
            if (frozen_version > merge_task_.get_last_frozen_version())
            {
              TBSYS_LOG(INFO,"pending a new frozen version need merge:%ld,last:%ld",
                  frozen_version, merge_task_.get_last_frozen_version() );
              merge_task_.set_frozen_version(frozen_version);
            }
            if (!merge_task_.is_scheduled()
                && tablet_manager.get_chunk_merge().can_launch_next_round(frozen_version))
            {
              srand(static_cast<int32_t>(tbsys::CTimeUtil::getTime()));
              int64_t merge_delay_interval = chunk_server_->get_config().merge_delay_interval;
              if (merge_delay_interval > 0)
              {
                wait_time = random() % merge_delay_interval;
              }
              // wait one more minute for ensure slave updateservers sync frozen version.
              wait_time += chunk_server_->get_config().merge_delay_for_lsync;
              TBSYS_LOG(INFO, "launch a new merge process after wait %ld us.", wait_time);
              timer_.schedule(merge_task_, wait_time, false);  //async
              merge_task_.set_scheduled();
            }
          }
        }
      }

      if (version > CS_HEART_BEAT_VERSION)
      {
        int64_t local_version = 0;
        int64_t schema_version = 0;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
                                                       in_buffer.get_capacity(),
                                                       in_buffer.get_position(),
                                                       &schema_version);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse heartbeat schema version failed:ret[%d]",
                      rc.result_code_);
          }
        }

        // fetch new schema in a temp timer task
        if ((OB_SUCCESS == rc.result_code_) && service_started_)
        {
          local_version = chunk_server_->get_schema_manager()->get_latest_version();
          if ((local_version > schema_version) && (schema_version != 0))
          {
            rc.result_code_ = OB_ERROR;
            TBSYS_LOG(ERROR, "check schema local version gt than new version:"
                "local[%ld], new[%ld]", local_version, schema_version);
          }
          else if (!fetch_schema_task_.is_scheduled()
                   && local_version < schema_version)
          {
            fetch_schema_task_.init(chunk_server_->get_root_server(), 
                &chunk_server_->get_rpc_stub(), chunk_server_->get_schema_manager());
            fetch_schema_task_.set_version(local_version, schema_version);
            srand(static_cast<int32_t>(tbsys::CTimeUtil::getTime()));
            timer_.schedule(fetch_schema_task_,
                            random() % FETCH_SCHEMA_INTERVAL, false);
            fetch_schema_task_.set_scheduled();
          }
        }
      }

      if (version > CS_HEART_BEAT_VERSION)
      {
        int64_t config_version;
        ObConfigManager &config_mgr = chunk_server_->get_config_mgr();
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
                                                       in_buffer.get_capacity(),
                                                       in_buffer.get_position(),
                                                       &config_version);
          if (OB_SUCCESS != (rc.result_code_))
          {
            TBSYS_LOG(ERROR, "parse heartbeat config version failed: ret[%d]",
                      rc.result_code_);
          }
          else if (OB_SUCCESS !=
                   (rc.result_code_ = config_mgr.got_version(config_version)))
          {
            TBSYS_LOG(WARN, "Process config failed, ret: [%d]", rc.result_code_);
          }
        }
      }
      /*
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_HEARTBEAT_RESPONSE,
            CS_HEART_BEAT_VERSION,
            out_buffer, connection, channel_id);
      }
      */
      return rc.result_code_;
    }

    int ObChunkService::cs_accept_schema(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_ACCEPT_SCHEMA_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int &err = rc.result_code_;
      ObSchemaManagerV2 *schema = NULL;
      ObMergerSchemaManager *schema_manager = NULL;
      int64_t schema_version = 0;
      int ret = OB_SUCCESS;

      if (version != CS_ACCEPT_SCHEMA_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == err)
      {
        if (NULL == (schema = OB_NEW(ObSchemaManagerV2, ObModIds::OB_CS_SERVICE_FUNC)))
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
          TBSYS_LOG(WARN, "fail to new ObSchemaManagerV2");
        }
      }

      if (OB_SUCCESS == err)
      {
        if (NULL == chunk_server_ || NULL == (schema_manager = chunk_server_->get_schema_manager()))
        {
          err = OB_NOT_INIT;
          TBSYS_LOG(WARN, "not init:chunk_server_[%p], schema_manager[%p]", chunk_server_, schema_manager);
        }
      }

      if (OB_SUCCESS == err)
      {
        err = schema->deserialize(
              in_buffer.get_data(), in_buffer.get_capacity(),
              in_buffer.get_position());
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to deserialize schema:err[%d]", err);
        }
        else
        {
          schema_version = schema->get_version();
        }
      }

      if (OB_SUCCESS == err)
      {
        if (schema_version <= schema_manager->get_latest_version())
        {
          TBSYS_LOG(WARN, "schema version too old. old=%ld, latest=%ld",
              schema_version, schema_manager->get_latest_version());
          err = OB_OLD_SCHEMA_VERSION;
        }
        else if (OB_SUCCESS != (err = schema_manager->add_schema(*schema)))
        {
          TBSYS_LOG(WARN, "fail to add schema :err[%d]", err);
        }
      }

      OB_DELETE(ObSchemaManagerV2, ObModIds::OB_CS_SERVICE_FUNC, schema);

      ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "fail to serialize: ret[%d]", ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = chunk_server_->send_response(
            OB_SWITCH_SCHEMA_RESPONSE,
            CS_ACCEPT_SCHEMA_VERSION,
            out_buffer, req, channel_id);
      }
      return ret;
    }

    int ObChunkService::cs_create_tablet(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_CREATE_TABLE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      if (version != CS_CREATE_TABLE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
          TBSYS_LOG(WARN, "load bypass sstable is running, cannot create tablet");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      ObNewRange range;
      range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = range.deserialize(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_create_tablet input range param error.");
        }
      }

      TBSYS_LOG(INFO, "cs_create_tablet, dump input range %s", to_cstring(range));

      // get last frozen memtable version for update
      int64_t last_frozen_memtable_version = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(
            in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position(), &last_frozen_memtable_version);
        /*
        ObServer update_server;
        rc.result_code_ = chunk_server_->get_rs_rpc_stub().get_update_server(update_server);
        ObRootServerRpcStub update_stub;
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = update_stub.init(update_server, &chunk_server_->get_client_manager());
        }
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = update_stub.get_last_frozen_memtable_version(last_frozen_memtable_version);
        }
        */
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        TBSYS_LOG(DEBUG, "create tablet, last_frozen_memtable_version=%ld",
            last_frozen_memtable_version);
        rc.result_code_ = chunk_server_->get_tablet_manager().create_tablet(
            range, last_frozen_memtable_version);
      }

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "cs_create_tablet rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_CREATE_TABLE_RESPONSE,
            CS_CREATE_TABLE_VERSION,
            out_buffer, req, channel_id);
      }

      if( OB_SUCCESS != (rc.result_code_ = chunk_server_->get_tablet_manager().get_serving_tablet_image().flush_log(OB_LOG_CS_CREATE_TABLET, OB_SUCCESS == rc.result_code_)))
      {
        TBSYS_LOG(ERROR, "flush log error, rc=%d", rc.result_code_);
      }

      // call tablet_manager_ drop tablets.

      return rc.result_code_;
    }


    int ObChunkService::cs_load_tablet(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_LOAD_TABLET_VERSION = 2;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      if (version <= 0 || version > CS_LOAD_TABLET_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_config().merge_migrate_concurrency
               && (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped()))
      {
        TBSYS_LOG(WARN, "merge running, cannot migrate in.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot migrate in");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_build_index_thread().is_finish_build())
      {
        TBSYS_LOG(WARN, "build index is running, cannot migrate in");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      ObNewRange range;
      range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
      int64_t num_file = 0;
      //deserialize ObRange
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = range.deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet range param error.");
        }
        else
        {
          char range_buf[OB_RANGE_STR_BUFSIZ];
          range.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          TBSYS_LOG(DEBUG,"cs_load_tablet dump range <%s>", range_buf);
        }
      }

      int32_t dest_disk_no = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi32(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position(), &dest_disk_no);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse dest_disk_no range param error.");
        }
        else
        {
          TBSYS_LOG(INFO, "cs_load_tablet dest_disk_no=%d ", dest_disk_no);
        }
      }

      // deserialize tablet_version;
      int64_t tablet_version = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &tablet_version);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet tablet_version param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet tablet_version = %ld ", tablet_version);
        }
      }

      uint64_t crc_sum = 0;
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), (int64_t*)(&crc_sum));
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet crc_sum param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet crc_sum = %lu ", crc_sum);
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = common::serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(),&num_file);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet number of sstable  param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet num_file = %ld ", num_file);
        }
      }

      char (*path)[OB_MAX_FILE_NAME_LENGTH] = NULL;
      char * path_buf = NULL;
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      if (OB_SUCCESS == rc.result_code_ && num_file > 0)
      {
        path_buf = static_cast<char*>(ob_malloc(num_file*OB_MAX_FILE_NAME_LENGTH));
        if ( NULL == path_buf )
        {
          TBSYS_LOG(ERROR, "failed to allocate memory for path array.");
          rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          path = new(path_buf)char[num_file][OB_MAX_FILE_NAME_LENGTH];
        }

        int64_t len = 0;
        if (OB_SUCCESS == rc.result_code_)
        {
          for( int64_t idx =0; idx < num_file; idx++)
          {
            if(NULL == common::serialization::decode_vstr(in_buffer.get_data(),
                  in_buffer.get_capacity(), in_buffer.get_position(),
                  path[idx], OB_MAX_FILE_NAME_LENGTH, &len))
            {
              rc.result_code_ = OB_ERROR;
              TBSYS_LOG(ERROR, "parse cs_load_tablet dest_path param error.");
              break;
            }
            else
            {
              TBSYS_LOG(INFO, "parse cs_load_tablet dest_path [%ld] = %s", idx, path[idx]);
            }
          }
        }
      }

      // deserialize tablet_seq_num
      int64_t tablet_seq_num = 0;
      if (OB_SUCCESS == rc.result_code_ && version  > 1)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &tablet_seq_num);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_load_tablet tablet_seq_num param error.");
        }
        else
        {
          TBSYS_LOG(INFO,"cs_load_tablet tablet_seq_num = %ld ", tablet_seq_num);
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_manager.dest_load_tablet(range, path, num_file,
          tablet_version, tablet_seq_num, dest_disk_no, crc_sum);
        if (OB_SUCCESS != rc.result_code_ && OB_CS_MIGRATE_IN_EXIST != rc.result_code_)
        {
          TBSYS_LOG(WARN, "ObTabletManager::dest_load_tablet error, rc=%d", rc.result_code_);
        }
      }

      if ( NULL != path_buf )
      {
        ob_free(path_buf);
      }

      //send response to src chunkserver
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_MIGRATE_RESPONSE,
            CS_LOAD_TABLET_VERSION,
            out_buffer, req, channel_id);
      }

      if( OB_SUCCESS != (rc.result_code_ = tablet_manager.get_serving_tablet_image().flush_log(OB_LOG_CS_DEST_LOAD_TABLET, OB_SUCCESS == rc.result_code_)))
      {
        TBSYS_LOG(ERROR, "flush log error, rc=%d", rc.result_code_);
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_delete_tablets(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DELETE_TABLETS_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObTabletReportInfoList *delete_tablet_list = NULL;
      bool is_force = false;

      if (version != CS_DELETE_TABLETS_VERSION
          && version != CS_DELETE_TABLETS_VERSION + 1)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot remove tablets.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (NULL == (delete_tablet_list = GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_1)))
      {
        TBSYS_LOG(ERROR, "cannot get ObTabletReportInfoList object.");
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        delete_tablet_list->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = delete_tablet_list->deserialize(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_delete_tablets tablet info list param error.");
        }
      }

      if (version > CS_DELETE_TABLETS_VERSION)
      {
        if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
        {
          TBSYS_LOG(WARN, "load bypass sstable is running, cannot delete tablets");
          rc.result_code_ = OB_CS_EAGAIN;
        }
        if (OB_SUCCESS == rc.result_code_)
        {
          rc.result_code_ = common::serialization::decode_bool(
              in_buffer.get_data(),in_buffer.get_capacity(),
              in_buffer.get_position(),&is_force);
          if (OB_SUCCESS != rc.result_code_)
          {
            TBSYS_LOG(ERROR, "parse cs_delete_tablets input is_force param error.");
          }
          else
          {
            TBSYS_LOG(DEBUG, "cs_heart_beat: is_force=%d", is_force);
          }
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
        ObMultiVersionTabletImage & tablet_image = tablet_manager.get_serving_tablet_image();

        char range_buf[OB_RANGE_STR_BUFSIZ];
        int64_t size = delete_tablet_list->get_tablet_size();
        int64_t version = 0;
        const ObTabletReportInfo* const tablet_info_array = delete_tablet_list->get_tablet();
        ObTablet *src_tablet = NULL;
        int32_t disk_no = -1;

        for (int64_t i = 0; i < size ;  ++i)
        {
          version = tablet_info_array[i].tablet_location_.tablet_version_;
          tablet_info_array[i].tablet_info_.range_.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
          rc.result_code_ = tablet_image.acquire_tablet(
              tablet_info_array[i].tablet_info_.range_,
              ObMultiVersionTabletImage::SCAN_FORWARD, version, src_tablet);

          if (OB_SUCCESS == rc.result_code_ && NULL != src_tablet
              && src_tablet->get_data_version() == version)
          {
            TBSYS_LOG(INFO, "delete tablet, version=%ld, disk=%d, range=<%s>",
                src_tablet->get_data_version(), src_tablet->get_disk_no(), range_buf);

            if(OB_SUCCESS != (rc.result_code_ = tablet_image.set_tablet_merged(src_tablet)))
            {
              TBSYS_LOG(WARN, "failed to set tablet merged, ret=%d", rc.result_code_);
            }
            else if(OB_SUCCESS != (rc.result_code_ = tablet_image.remove_tablet(
                    tablet_info_array[i].tablet_info_.range_, version, disk_no)))
            {
              TBSYS_LOG(WARN, "failed to remove tablet from tablet image, "
                  "version=%ld, disk=%d, range=%s",
                  src_tablet->get_data_version(), src_tablet->get_disk_no(),
                  to_cstring(src_tablet->get_range()));
            }
          }
          else
          {
            TBSYS_LOG(INFO, "cannot find tablet, version=%ld, range=<%s>",
                version, range_buf);
          }

          if (NULL != src_tablet)
          {
            tablet_image.release_tablet(src_tablet);
          }
        }

      }

      //send response to src chunkserver
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_DELETE_TABLETS_RESPONSE,
            CS_DELETE_TABLETS_VERSION,
            out_buffer, req, channel_id);
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      ObMultiVersionTabletImage & tablet_image = tablet_manager.get_serving_tablet_image();

      if(OB_SUCCESS != (rc.result_code_ = tablet_image.flush_log(OB_LOG_CS_DEL_TABLET, OB_SUCCESS == rc.result_code_)))
      {
        TBSYS_LOG(WARN, "flush log error, ret=%d", rc.result_code_);
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_merge_tablets(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_MERGE_TABLETS_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObTabletReportInfoList *merge_tablet_list = NULL;

      if (version != CS_MERGE_TABLETS_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_reported())
      {
        TBSYS_LOG(WARN, "merge running, cannot merge tablets.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstables is running, cannot merge tablets.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_build_index_thread().is_finish_build())
      {
        TBSYS_LOG(WARN, "build index is running, cannot merge tablets");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (NULL == (merge_tablet_list = GET_TSI_MULT(ObTabletReportInfoList, TSI_CS_TABLET_REPORT_INFO_LIST_1)))
      {
        TBSYS_LOG(ERROR, "cannot get ObTabletReportInfoList object.");
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        merge_tablet_list->reset();
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = merge_tablet_list->deserialize(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_merge_tablets tablet info list param error.");
        }
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "merge_tablets rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_MERGE_TABLETS_RESPONSE,
            CS_MERGE_TABLETS_VERSION,
            out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = chunk_server_->get_tablet_manager().merge_multi_tablets(*merge_tablet_list);
      }

      bool is_merge_succ = (OB_SUCCESS == rc.result_code_);
      //report merge tablets info to root server
      rc.result_code_ = CS_RPC_CALL_RS(merge_tablets_over, *merge_tablet_list, is_merge_succ);
      if (OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(WARN, "report merge tablets over error, is_merge_succ=%d, seq_num=%ld",
            is_merge_succ, is_merge_succ ? merge_tablet_list->get_tablet()[0].tablet_location_.tablet_seq_ : -1);
      }
      else if (is_merge_succ)
      {
        TBSYS_LOG(INFO, "merge tablets <%s> over and success, seq_num=%ld",
            to_cstring(merge_tablet_list->get_tablet()[0].tablet_info_.range_),
            merge_tablet_list->get_tablet()[0].tablet_location_.tablet_seq_);
      }


      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      ObMultiVersionTabletImage & tablet_image = tablet_manager.get_serving_tablet_image();
      if(OB_SUCCESS != (rc.result_code_ = tablet_image.flush_log(OB_LOG_CS_MERGE_TABLET, OB_SUCCESS == rc.result_code_)))
      {
        TBSYS_LOG(WARN, "flush log error, ret=%d", rc.result_code_);
      }

      return rc.result_code_;
    }


    int ObChunkService::cs_migrate_tablet(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_MIGRATE_TABLET_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int32_t is_inc_migrate_task_count = 0;

      if (version != CS_MIGRATE_TABLET_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }


      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      ObNewRange range;
      range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
      ObServer dest_server;
      bool keep_src = false;

      //deserialize ObRange
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = range.deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_migrate_tablet range param error.");
        }
      }

      //deserialize destination chunkserver
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = dest_server.deserialize(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position());
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_migrate_tablet dest_server param error.");
        }
      }

      //deserialize migrate type(copy or move)
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = common::serialization::decode_bool(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position(),&keep_src);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_migrate_tablet keep_src param error.");
        }
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        TBSYS_LOG(INFO, "begin migrate_tablet %s, dest_server=%s, keep_src=%d",
            to_cstring(range), to_cstring(dest_server), keep_src);
      }

      if (!chunk_server_->get_config().merge_migrate_concurrency
          && (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped()))
      {
        TBSYS_LOG(WARN, "merge running, cannot migrate.");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot migrate");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_build_index_thread().is_finish_build())
      {
        TBSYS_LOG(WARN, "build index is running, cannot migrate");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      int64_t max_migrate_task_count = chunk_server_->get_config().max_migrate_task_count;
      if (OB_SUCCESS == rc.result_code_ )
      {
        uint32_t old_migrate_task_count = migrate_task_count_;
        while(old_migrate_task_count < max_migrate_task_count)
        {
          uint32_t tmp = atomic_compare_exchange(&migrate_task_count_, old_migrate_task_count+1, old_migrate_task_count);
          if (tmp == old_migrate_task_count)
          {
            is_inc_migrate_task_count = 1;
            break;
          }
          old_migrate_task_count = migrate_task_count_;
        }

        if (0 == is_inc_migrate_task_count)
        {
          TBSYS_LOG(WARN, "current migrate task count = %u, exceeded max = %ld",
              old_migrate_task_count, max_migrate_task_count);
          rc.result_code_ = OB_CS_EAGAIN;
        }
      }


      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "migrate_tablet rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_MIGRATE_OVER_RESPONSE,
            CS_MIGRATE_TABLET_VERSION,
            out_buffer, req, channel_id);
      }

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      char (*dest_path)[OB_MAX_FILE_NAME_LENGTH] = NULL;
      char (*src_path)[OB_MAX_FILE_NAME_LENGTH] = NULL;
      char *dest_path_buf = static_cast<char*>(ob_malloc(ObTablet::MAX_SSTABLE_PER_TABLET*OB_MAX_FILE_NAME_LENGTH));
      char *src_path_buf  = static_cast<char*>(ob_malloc(ObTablet::MAX_SSTABLE_PER_TABLET*OB_MAX_FILE_NAME_LENGTH));
      if ( NULL == src_path_buf || NULL == dest_path_buf)
      {
        TBSYS_LOG(ERROR, "migrate_tablet failed to allocate memory for path array.");
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        src_path = new(src_path_buf)char[ObTablet::MAX_SSTABLE_PER_TABLET][OB_MAX_FILE_NAME_LENGTH];
        dest_path = new(dest_path_buf)char[ObTablet::MAX_SSTABLE_PER_TABLET][OB_MAX_FILE_NAME_LENGTH];
      }

      int64_t num_file = 0;
      int64_t tablet_version = 0;
      int32_t dest_disk_no = 0;
      uint64_t crc_sum = 0;
      int64_t tablet_seq_num = 0;

      ObMultiVersionTabletImage & tablet_image = tablet_manager.get_serving_tablet_image();
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_manager.migrate_tablet(range,
            dest_server, src_path, dest_path, num_file, tablet_version,
            tablet_seq_num, dest_disk_no, crc_sum);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "ObTabletManager::migrate_tablet <%s> error, rc.result_code_=%d",
              to_cstring(range), rc.result_code_);
        }
      }


      //send request to load sstable
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = CS_RPC_CALL(dest_load_tablet, dest_server,
            range, dest_disk_no, tablet_version, tablet_seq_num, crc_sum, num_file, dest_path);
        if (OB_CS_MIGRATE_IN_EXIST == rc.result_code_)
        {
          TBSYS_LOG(INFO, "dest server already hold this tablet, consider it done.");
          rc.result_code_ = OB_SUCCESS;
        }
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "dest server load tablet <%s> error ,rc.code=%d",
              to_cstring(range), rc.result_code_);
        }
      }

      /**
       * it's better to decrease the migrate task counter before
       * calling migrate_over(), because rootserver keeps the same
       * migrate task counter of each chunkserver, if migrate_over()
       * returned, rootserver has decreased the migrate task counter
       * and send a new migrate task to this chunkserver immediately,
       * if this chunkserver hasn't decreased the migrate task counter
       * at this time, the chunkserver will return -1010 error. if
       * rootserver doesn't send migrate message successfully, it
       * doesn't increase the migrate task counter, so it sends
       * migrate message continiously and it receives a lot of error
       * -1010.
       */
      if (0 != is_inc_migrate_task_count)
      {
        atomic_dec(&migrate_task_count_);
      }

      //report migrate info to root server
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_= CS_RPC_CALL_RS(migrate_over, range,
            chunk_server_->get_self(), dest_server, keep_src, tablet_version, tablet_seq_num);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "report migrate tablet <%s> over error, rc.code=%d",
              to_cstring(range), rc.result_code_);
        }
        else
        {
          TBSYS_LOG(INFO,"report migrate tablet <%s> over to rootserver success.",
              to_cstring(range));
        }
      }

      if( OB_SUCCESS == rc.result_code_ && false == keep_src)
      {
        // migrate/move , set local tablet merged,
        // will be discarded in next time merge process .
        ObTablet *src_tablet = NULL;
        int32_t disk_no = 0;
        rc.result_code_ = tablet_image.acquire_tablet(range,
            ObMultiVersionTabletImage::SCAN_FORWARD, 0, src_tablet);
        if (OB_SUCCESS == rc.result_code_ && NULL != src_tablet)
        {
          TBSYS_LOG(INFO, "src tablet set merged, version=%ld, seq_num=%ld, disk=%d, range:%s",
              src_tablet->get_data_version(), src_tablet->get_sequence_num(),
              src_tablet->get_disk_no(), to_cstring(src_tablet->get_range()));

          if(OB_SUCCESS != (rc.result_code_ = tablet_image.set_tablet_merged(src_tablet)))
          {
            TBSYS_LOG(WARN, "failed to set tablet merged, ret=%d", rc.result_code_);
          }
          else if(OB_SUCCESS != (rc.result_code_ = tablet_image.remove_tablet(
                  src_tablet->get_range(), src_tablet->get_data_version(), disk_no)))
          {
            TBSYS_LOG(WARN, "failed to remove tablet from tablet image, "
                "version=%ld, disk=%d, range=%s",
                src_tablet->get_data_version(), src_tablet->get_disk_no(),
                to_cstring(src_tablet->get_range()));
          }

        }

        if (NULL != src_tablet)
        {
          tablet_image.release_tablet(src_tablet);
        }

      }

      if ( NULL != src_path_buf )
      {
        ob_free(src_path_buf);
      }
      if ( NULL != dest_path_buf )
      {
        ob_free(dest_path_buf);
      }
      TBSYS_LOG(INFO, "migrate_tablet finish rc.code=%d",rc.result_code_);

      if(OB_SUCCESS != (rc.result_code_ = tablet_image.flush_log(OB_LOG_CS_SRC_MIGRATE_TABLET, OB_SUCCESS == rc.result_code_)))
      {
        TBSYS_LOG(WARN, "flush log error, ret=%d", rc.result_code_);
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_get_migrate_dest_loc(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_GET_MIGRATE_DEST_LOC_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_GET_MIGRATE_DEST_LOC_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_config().merge_migrate_concurrency
          && (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped())
          && (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
          && (!chunk_server_->get_tablet_manager().get_build_index_thread().is_finish_build()))
      {
        TBSYS_LOG(WARN, "merge or load bypass sstables running, cannot migrate in.");
        rc.result_code_ = OB_CS_EAGAIN;
      }


      int64_t occupy_size = 0;
      //deserialize occupy_size
      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &occupy_size);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(ERROR, "parse cs_get_migrate_dest_loc occupy_size param error.");
        }
        else
        {
          TBSYS_LOG(INFO, "cs_get_migrate_dest_loc occupy_size =%ld", occupy_size);
        }
      }

      int32_t disk_no = 0;
      char dest_directory[OB_MAX_FILE_NAME_LENGTH];

      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      disk_no = tablet_manager.get_disk_manager().get_disk_for_migrate();
      if (disk_no <= 0)
      {
        TBSYS_LOG(ERROR, "get wrong disk no =%d", disk_no);
        rc.result_code_ = OB_ERROR;
      }
      else
      {
        rc.result_code_ = get_sstable_directory(disk_no, dest_directory, OB_MAX_FILE_NAME_LENGTH);
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      // ifreturn success , we can return disk_no & dest_directory.
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        serialize_ret = serialization::encode_vi32(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position(), disk_no);
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize disk_no failed.");
        }
      }

      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        ObString dest_string(OB_MAX_FILE_NAME_LENGTH,
            static_cast<int32_t>(strlen(dest_directory)), dest_directory);
        serialize_ret = dest_string.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize dest_directory failed.");
        }
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_GET_MIGRATE_DEST_LOC_RESPONSE,
            CS_GET_MIGRATE_DEST_LOC_VERSION,
            out_buffer, req, channel_id);
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_disk_maintain(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
  {
      common::ObResultCode rc;
      const int32_t CS_DISK_MAINTAIN_VERSION = 1;
      rc.result_code_ = OB_SUCCESS;

      if(version != CS_DISK_MAINTAIN_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if(chunk_server_->get_tablet_manager().is_disk_maintain())
      {
        TBSYS_LOG(WARN, "disk is maintaining");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot do disk maintain");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot do disk maintain");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      int8_t is_install = -1;
      int32_t disk_no = -1;

      if(OB_SUCCESS != rc.result_code_)
      {
      }
      else if (OB_SUCCESS != ( rc.result_code_ =
            serialization::decode_i8(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(), &is_install)))
      {
        TBSYS_LOG(WARN, "parse disk maintain param error.");
      }
      else if (OB_SUCCESS != ( rc.result_code_ =
            serialization::decode_vi32(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(), &disk_no)))
      {
        TBSYS_LOG(WARN, "parse disk maintain param error.");
      }
      else
      {
        if(1 == is_install)
        {
          rc.result_code_ = chunk_server_->get_tablet_manager().install_disk(disk_no);
        }
        else if(0 == is_install)
        {
          rc.result_code_ = chunk_server_->get_tablet_manager().uninstall_disk(disk_no);
        }
        else
        {
          TBSYS_LOG(ERROR, "is install flag invalid is_install: %d", is_install);
        }
      }

    int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "cs_create_tablet rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_DISK_MAINTAIN_RESPONSE,
            CS_DISK_MAINTAIN_VERSION,
            out_buffer, req, channel_id);
    }

    return rc.result_code_;
  }


    int ObChunkService::cs_dump_tablet_image(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DUMP_TABLET_IMAGE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      int32_t index = 0;
      int32_t disk_no = 0;

      char *dump_buf = NULL;
      const int64_t dump_size = OB_MAX_PACKET_LENGTH - 1024;

      int64_t pos = 0;
      ObTabletImage * tablet_image = NULL;

      if (version != CS_DUMP_TABLET_IMAGE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != ( rc.result_code_ =
            serialization::decode_vi32(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &index)))
      {
        TBSYS_LOG(WARN, "parse cs_dump_tablet_image index param error.");
      }
      else if (OB_SUCCESS != ( rc.result_code_ =
            serialization::decode_vi32(in_buffer.get_data(),
            in_buffer.get_capacity(), in_buffer.get_position(), &disk_no)))
      {
        TBSYS_LOG(WARN, "parse cs_dump_tablet_image disk_no param error.");
      }
      else if (disk_no <= 0)
      {
        TBSYS_LOG(WARN, "cs_dump_tablet_image input param error, "
            "disk_no=%d", disk_no);
        rc.result_code_ = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (dump_buf = static_cast<char*>(ob_malloc(dump_size))))
      {
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR, "allocate memory for serialization failed.");
      }
      else if (OB_SUCCESS != (rc.result_code_ =
            chunk_server_->get_tablet_manager().get_serving_tablet_image().
            serialize(index, disk_no, dump_buf, dump_size, pos)))
      {
        TBSYS_LOG(WARN, "serialize tablet image failed. disk_no=%d", disk_no);
      }

      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      // ifreturn success , we can return dump_buf
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == serialize_ret)
      {
        ObString return_dump_obj(static_cast<int32_t>(pos), static_cast<int32_t>(pos), dump_buf);
        serialize_ret = return_dump_obj.serialize(out_buffer.get_data(),
            out_buffer.get_capacity(), out_buffer.get_position());
        if (OB_SUCCESS != serialize_ret)
        {
          TBSYS_LOG(ERROR, "serialize return_dump_obj failed.");
        }
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_DUMP_TABLET_IMAGE_RESPONSE,
            CS_DUMP_TABLET_IMAGE_VERSION,
            out_buffer, req, channel_id);
      }

      if (NULL != dump_buf)
      {
        ob_free(dump_buf);
        dump_buf = NULL;
      }
      if (NULL != tablet_image)
      {
        delete(tablet_image);
        tablet_image = NULL;
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_fetch_stats(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      const int32_t CS_FETCH_STATS_VERSION = 1;
      int ret = OB_SUCCESS;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_FETCH_STATS_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      // ifreturn success , we can return dump_buf
      if (OB_SUCCESS == rc.result_code_ && OB_SUCCESS == ret)
      {
        ret = chunk_server_->get_stat_manager().serialize(
            out_buffer.get_data(),out_buffer.get_capacity(),out_buffer.get_position());
      }

      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_FETCH_STATS_RESPONSE,
            CS_FETCH_STATS_VERSION,
            out_buffer, req, channel_id);
      }
      return ret;
    }

    int ObChunkService::cs_start_gc(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_START_GC_VERSION = 1;
      int ret = OB_SUCCESS;
      int64_t recycle_version = 0;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_START_GC_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!chunk_server_->get_tablet_manager().get_build_index_thread().is_finish_build())
      {
        TBSYS_LOG(WARN, "build index is running, cannot start gc");
        rc.result_code_ = OB_CS_EAGAIN;
      }

      if (OB_SUCCESS == ret && OB_SUCCESS == rc.result_code_ )
      {
        rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(), in_buffer.get_capacity(),
            in_buffer.get_position(), &recycle_version);
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_RESULT,
            CS_START_GC_VERSION,
            out_buffer, req, channel_id);
      }
      chunk_server_->get_tablet_manager().start_gc(recycle_version);
      return ret;
    }

    int ObChunkService::cs_check_tablet(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t DEFAULT_VERSION = 1;
      int ret = OB_SUCCESS;
      int64_t table_id = 0;
      ObNewRange range;
      ObTablet* tablet = NULL;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != DEFAULT_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      //if (OB_SUCCESS == ret && OB_SUCCESS == rc.result_code_ )
      else if (OB_SUCCESS !=
          (rc.result_code_ = serialization::decode_vi64(in_buffer.get_data(),
             in_buffer.get_capacity(), in_buffer.get_position(), &table_id)))
      {
        TBSYS_LOG(WARN, "deserialize table id error. pos=%ld, cap=%ld",
            in_buffer.get_position(), in_buffer.get_capacity());
        rc.result_code_ = OB_INVALID_ARGUMENT;
      }
      else
      {
        range.table_id_ = table_id;
        range.start_key_.set_min_row();
        range.end_key_.set_max_row();
        rc.result_code_ = chunk_server_->get_tablet_manager().get_serving_tablet_image().acquire_tablet(
            range, ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
        if (NULL != tablet)
        {
          chunk_server_->get_tablet_manager().get_serving_tablet_image().release_tablet(tablet);
        }
      }

      if (OB_SUCCESS != (ret = rc.serialize(out_buffer.get_data(),
              out_buffer.get_capacity(), out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }
      else
      {
        chunk_server_->send_response(
            OB_CS_CHECK_TABLET_RESPONSE,
            DEFAULT_VERSION,
            out_buffer, req, channel_id);
      }
      return ret;
    }

    int ObChunkService::cs_reload_conf(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      int ret = OB_SUCCESS;
      const int32_t CS_RELOAD_CONF_VERSION = 1;

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_RELOAD_CONF_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == chunk_server_)
      {
        rc.result_code_ = OB_NOT_INIT;
      }
      else
      {
        rc.result_code_ = chunk_server_->get_config_mgr().reload_config();
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }
      else
      {
        chunk_server_->send_response(OB_UPS_RELOAD_CONF_RESPONSE,
                                     CS_RELOAD_CONF_VERSION, out_buffer, req, channel_id);
      }

      return ret;
    }

    int ObChunkService::cs_show_param(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_SHOW_PARAM_VERSION = 1;
      int ret = OB_SUCCESS;
      UNUSED(in_buffer);

      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (version != CS_SHOW_PARAM_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_RESULT,
            CS_SHOW_PARAM_VERSION,
            out_buffer, req, channel_id);
      }
      chunk_server_->get_config().print();
      ob_print_mod_memory_usage();
      return ret;
    }

    int ObChunkService::cs_stop_server(
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      UNUSED(in_buffer);
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      //int64_t server_id = chunk_server_->get_root_server().get_ipv4_server_id();
      int64_t peer_id = convert_addr_to_server(req->ms->c->addr);

      /*
      if (server_id != peer_id)
      {
        TBSYS_LOG(WARN, "*stop server* WARNNING coz packet from unrecongnized address "
                  "which is [%lld], should be [%lld] as rootserver.", peer_id, server_id);
        // comment follow line not to strict packet from rs.
        // rc.result_code_ = OB_ERROR;
      }
      */

      int32_t restart = 0;
      rc.result_code_ = serialization::decode_i32(in_buffer.get_data(), in_buffer.get_capacity(),
                                                  in_buffer.get_position(), &restart);

      //int64_t pos = 0;
      //rc.result_code_ = serialization::decode_i32(in_buffer.get_data(), in_buffer.get_position(), pos, &restart);

      if (restart != 0)
      {
        BaseMain::set_restart_flag();
        TBSYS_LOG(INFO, "receive *restart server* packet from: [%ld]", peer_id);
      }
      else
      {
        TBSYS_LOG(INFO, "receive *stop server* packet from: [%ld]", peer_id);
      }

      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
          OB_STOP_SERVER_RESPONSE,
          version,
          out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {

        chunk_server_->stop();
      }
      return rc.result_code_;
    }

    int ObChunkService::cs_force_to_report_tablet(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      int ret = OB_SUCCESS;
      UNUSED(channel_id);
      UNUSED(req);
      UNUSED(in_buffer);
      UNUSED(out_buffer);
      static const int MY_VERSION = 1;
      TBSYS_LOG(INFO, "cs receive force_report_tablet to rs. maybe have some network trouble");
      if (MY_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
        TBSYS_LOG(WARN, "force to report tablet verion not equal. my_version=%d, receive_version=%d", MY_VERSION, version);
      }
      ObTabletManager & tablet_manager = chunk_server_->get_tablet_manager();
      if (OB_SUCCESS == ret)
      {
        if (in_register_process_ || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
            || tablet_manager.get_bypass_sstable_loader().is_pending_upgrade()
            || 0 != atomic_compare_exchange(&scan_tablet_image_count_, 1, 0))
        {
          TBSYS_LOG(WARN, "someone else is reporting. give up this process");
        }
        else
        {
          ret = report_tablets_busy_wait();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to report tablets. err = %d", ret);
          }
          else
          {
            tablet_manager.report_capacity_info();
          }
          atomic_exchange(&scan_tablet_image_count_, 0);
        }
      }
      easy_request_wakeup(req);
      return ret;
    }

    int ObChunkService::cs_change_log_level(
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      UNUSED(version);
      common::ObResultCode result;
      result.result_code_ = OB_SUCCESS;
      int32_t log_level = -1;
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buffer.get_data(),
                                                          in_buffer.get_capacity(),
                                                          in_buffer.get_position(),
                                                          &log_level)))
      {
        TBSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        if (TBSYS_LOG_LEVEL_ERROR <= log_level
            && TBSYS_LOG_LEVEL_DEBUG >= log_level)
        {
          TBSYS_LOG(INFO, "change log level. From: %d, To: %d", TBSYS_LOGGER._level, log_level);
          TBSYS_LOGGER._level = log_level;
        }
        else
        {
          TBSYS_LOG(WARN, "invalid log level, level=%d", log_level);
          result.result_code_ = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS != (ret = result.serialize(out_buffer.get_data(),
                                                  out_buffer.get_capacity(),
                                                  out_buffer.get_position())))
        {
          TBSYS_LOG(WARN, "serialize error, err=%d", ret);
        }
        else
        {
          ret = chunk_server_->send_response(OB_RS_ADMIN_RESPONSE, MY_VERSION, out_buffer, req, channel_id);
        }
      }
      return ret;
    }

    int ObChunkService::cs_sync_all_images(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_SYNC_ALL_IMAGES_VERSION = 1;
      int ret = OB_SUCCESS;
      UNUSED(in_buffer);

      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      TBSYS_LOG(INFO, "chunkserver start sync all tablet images");
      if (version != CS_SYNC_ALL_IMAGES_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      ret = rc.serialize(out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "rc.serialize error");
      }

      if (OB_SUCCESS == ret)
      {
        chunk_server_->send_response(
            OB_RESULT,
            CS_SYNC_ALL_IMAGES_VERSION,
            out_buffer, req, channel_id);
      }

      if (inited_ && service_started_
          && !tablet_manager.get_chunk_merge().is_pending_in_upgrade())
      {
        rc.result_code_ = tablet_manager.sync_all_tablet_images();
      }
      else
      {
        TBSYS_LOG(WARN, "can't sync tablet images now, please try again later");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      TBSYS_LOG(INFO, "finish sync all tablet images, ret=%d", rc.result_code_);

      return ret;
    }

    int ObChunkService::cs_load_bypass_sstables(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_LOAD_BYPASS_SSTABLE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
      int64_t serving_version = tablet_manager.get_last_not_merged_version();
      ObTableImportInfoList* table_list =
        GET_TSI_MULT(ObTableImportInfoList, TSI_CS_TABLE_IMPORT_INFO_1);

      if (version != CS_LOAD_BYPASS_SSTABLE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == table_list)
      {
        TBSYS_LOG(WARN, "failed to allocate memory for import table list info");
        rc.result_code_ = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (!tablet_manager.get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot load bypass sstables");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!tablet_manager.get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot load bypass "
                        "sstables concurrency");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (OB_SUCCESS != (rc.result_code_ = table_list->deserialize(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position())))
      {
        TBSYS_LOG(WARN, "deserialize import table list info error, "
                        "load_version=%ld, err=%d",
          table_list->tablet_version_, rc.result_code_);
      }
      else if (serving_version < 0 || table_list->tablet_version_ < 1)
      {
        TBSYS_LOG(WARN, "invalid serving_version=%ld, load_version=%ld",
          serving_version, table_list->tablet_version_);
        rc.result_code_ = OB_ERROR;
      }
      else if (0 != serving_version && table_list->tablet_version_ != serving_version)
      {
        TBSYS_LOG(WARN, "load version is different from local serving verion, "
                        "load_verion=%ld, serving_version=%ld",
          table_list->tablet_version_, serving_version);
        rc.result_code_ = OB_ERROR;
      }

      TBSYS_LOG(INFO, "received load bypass sstable command, ret=%d", rc.result_code_);
      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "cs_load_bypass_sstables rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_LOAD_BYPASS_SSTABLE_RESPONSE,
            CS_LOAD_BYPASS_SSTABLE_VERSION,
            out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_manager.load_bypass_sstables(*table_list);
      }

      bool is_start_load_succ = (OB_SUCCESS == rc.result_code_);
      if (!is_start_load_succ && NULL != table_list
          && table_list->response_rootserver_)
      {
        //report load bypass sstables over info to root server
        rc.result_code_= CS_RPC_CALL_RS(load_bypass_sstables_over,
            chunk_server_->get_self(), *table_list,
            is_start_load_succ);
        if (OB_SUCCESS != rc.result_code_)
        {
          TBSYS_LOG(WARN, "report load bypass sstables over error, is_start_load_succ=%d",
              is_start_load_succ);
        }
      }
      TBSYS_LOG(INFO, "start load bypass sstables, load_version=%ld, "
                      "is_response_rootserver=%d, is_start_load_succ=%d, ret=%d",
        table_list->tablet_version_, table_list->response_rootserver_,
        is_start_load_succ, rc.result_code_);

      return rc.result_code_;
    }

    int ObChunkService::cs_delete_table(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_DELETE_TABLE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      uint64_t table_id = 0;
      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();

      if (version != CS_DELETE_TABLE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!tablet_manager.get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot delete table");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!tablet_manager.get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot delete table");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (in_register_process_ || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
            || tablet_manager.get_bypass_sstable_loader().is_pending_upgrade())
      {
        TBSYS_LOG(WARN, "other thread is scanning tablet images, "
                        "can't iterate it concurrency");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (OB_SUCCESS != (rc.result_code_ = serialization::decode_vi64(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position(), reinterpret_cast<int64_t*>(&table_id))))
      {
        TBSYS_LOG(WARN, "deserialize table_id error, err=%d", rc.result_code_);
      }

      TBSYS_LOG(INFO, "received delete table command, table_id=%lu, ret=%d",
        table_id, rc.result_code_);
      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "cs_load_bypass_sstables rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_DELETE_TABLE_RESPONSE,
            CS_DELETE_TABLE_VERSION,
            out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_manager.delete_table(table_id);
      }

      bool is_delete_succ = (OB_SUCCESS == rc.result_code_);
      //report load bypass sstables over info to root server
      rc.result_code_= CS_RPC_CALL_RS(delete_table_over,
          chunk_server_->get_self(), table_id, is_delete_succ);
      if (OB_SUCCESS != rc.result_code_)
      {
        TBSYS_LOG(WARN, "report delete table over error, is_delete_succ=%d",
            is_delete_succ);
      }

      if( OB_SUCCESS != (rc.result_code_ = tablet_manager.get_serving_tablet_image().flush_log(OB_LOG_CS_DEL_TABLE, OB_SUCCESS == rc.result_code_)))
      {
        TBSYS_LOG(ERROR, "flush log error, rc=%d", rc.result_code_);
      }

      TBSYS_LOG(INFO, "load delete table over, table_id=%lu, "
                      "is_delete_succ=%d, ret=%d",
          table_id, is_delete_succ, rc.result_code_);

      return rc.result_code_;
    }

    int ObChunkService::cs_build_sample(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_BUILD_SAMPLE_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int64_t tablet_version = 0;
      uint64_t data_table_id = OB_INVALID_ID;
      uint64_t index_table_id = OB_INVALID_ID;
      int64_t sample_count = 0;
      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();

      if (version != CS_BUILD_SAMPLE_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!tablet_manager.get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot build sample");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!tablet_manager.get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot build sample");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (in_register_process_ || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
            || tablet_manager.get_bypass_sstable_loader().is_pending_upgrade())
      {
        TBSYS_LOG(WARN, "other thread is scanning tablet images, "
                        "can't iterate it concurrency");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_build_index_thread().is_finish_build())
      {
        TBSYS_LOG(WARN, "build index is running, cannot build sample");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (OB_SUCCESS != (rc.result_code_ = serialization::decode_vi64(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position(), &tablet_version)))
      {
        TBSYS_LOG(WARN, "deserialize tablet_version error, err=%d", rc.result_code_);
      }
      else if (tablet_version != tablet_manager.get_last_not_merged_version())
      {
        TBSYS_LOG(WARN, "build sample tablet_version=%ld, not equal to serving_tablet_version=%ld",
                  tablet_version, tablet_manager.get_last_not_merged_version());
        rc.result_code_ = OB_ERROR;
      }
      else if (OB_SUCCESS != (rc.result_code_ = serialization::decode_vi64(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position(), reinterpret_cast<int64_t*>(&data_table_id))))
      {
        TBSYS_LOG(WARN, "deserialize data_table_id error, err=%d", rc.result_code_);
      }
      else if (OB_SUCCESS != (rc.result_code_ = serialization::decode_vi64(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position(), reinterpret_cast<int64_t*>(&index_table_id))))
      {
        TBSYS_LOG(WARN, "deserialize index_table_id error, err=%d", rc.result_code_);
      }
      else if (OB_SUCCESS != (rc.result_code_ = serialization::decode_vi64(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position(), &sample_count)))
      {
        TBSYS_LOG(WARN, "deserialize sample_count error, err=%d", rc.result_code_);
      }

      TBSYS_LOG(INFO, "received build sample command, data_table_id=%lu, index_table_id=%lu,"
                      "sample_count=%ld, ret=%d",
        data_table_id, index_table_id, sample_count, rc.result_code_);
      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "cs_build_sample rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_REPORT_SAMPLES_RESPONSE,
            CS_BUILD_SAMPLE_VERSION,
            out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_manager.get_build_index_thread().start_build_sample(
           data_table_id, index_table_id, sample_count);
      }

      TBSYS_LOG(INFO, "build sample over, data_table_id=%lu, index_table_id=%lu, "
                      "sample_count=%ld, ret=%d",
          data_table_id, index_table_id, sample_count, rc.result_code_);

      return rc.result_code_;
    }

    int ObChunkService::cs_build_index(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_BUILD_INDEX_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;
      int64_t tablet_version = 0;
      uint64_t data_table_id = OB_INVALID_ID;
      uint64_t index_table_id = OB_INVALID_ID;
      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();

      if (version != CS_BUILD_INDEX_VERSION)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else if (!tablet_manager.get_chunk_merge().is_merge_stoped())
      {
        TBSYS_LOG(WARN, "merge running, cannot build index");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!tablet_manager.get_bypass_sstable_loader().is_loader_stoped())
      {
        TBSYS_LOG(WARN, "load bypass sstable is running, cannot build index");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (in_register_process_ || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
            || tablet_manager.get_bypass_sstable_loader().is_pending_upgrade())
      {
        TBSYS_LOG(WARN, "other thread is scanning tablet images, "
                        "can't iterate it concurrency");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (!chunk_server_->get_tablet_manager().get_build_index_thread().is_finish_build())
      {
        TBSYS_LOG(WARN, "build index is running, cannot build concurrency");
        rc.result_code_ = OB_CS_EAGAIN;
      }
      else if (OB_SUCCESS != (rc.result_code_ = serialization::decode_vi64(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position(), &tablet_version)))
      {
        TBSYS_LOG(WARN, "deserialize tablet_version error, err=%d", rc.result_code_);
      }
      else if (tablet_version != tablet_manager.get_last_not_merged_version())
      {
        TBSYS_LOG(WARN, "build index tablet_version=%ld, not equal to serving_tablet_version=%ld",
                  tablet_version, tablet_manager.get_last_not_merged_version());
        rc.result_code_ = OB_ERROR;
      }
      else if (OB_SUCCESS != (rc.result_code_ = serialization::decode_vi64(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position(), reinterpret_cast<int64_t*>(&data_table_id))))
      {
        TBSYS_LOG(WARN, "deserialize data_table_id error, err=%d", rc.result_code_);
      }
      else if (OB_SUCCESS != (rc.result_code_ = serialization::decode_vi64(
        in_buffer.get_data(), in_buffer.get_capacity(),
        in_buffer.get_position(), reinterpret_cast<int64_t*>(&index_table_id))))
      {
        TBSYS_LOG(WARN, "deserialize index_table_id error, err=%d", rc.result_code_);
      }

      TBSYS_LOG(INFO, "received build index command, data_table_id=%lu, "
                      "index_table_id=%lu, ret=%d",
        data_table_id, index_table_id, rc.result_code_);
      //response to root server first
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (serialize_ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "cs_build_index rc.serialize error");
      }

      if (OB_SUCCESS == serialize_ret)
      {
        chunk_server_->send_response(
            OB_CS_BUILD_INDEX_RESPONSE,
            CS_BUILD_INDEX_VERSION,
            out_buffer, req, channel_id);
      }

      if (OB_SUCCESS == rc.result_code_)
      {
        rc.result_code_ = tablet_manager.get_build_index_thread().start_build_global_index(
           data_table_id, index_table_id);
      }

      TBSYS_LOG(INFO, "build index over, data_table_id=%lu, index_table_id=%lu, ret=%d",
          data_table_id, index_table_id, rc.result_code_);

      return rc.result_code_;
    }

    int ObChunkService::cs_stop_build_index(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t *req,
        common::ObDataBuffer &in_buffer,
        common::ObDataBuffer &out_buffer)
    {
      UNUSED(in_buffer);
      const int32_t CS_STOP_BUILD_INDEX_VERSION = 1;
      common::ObResultCode rc;
      rc.result_code_ = OB_SUCCESS;

      if (CS_STOP_BUILD_INDEX_VERSION != version)
      {
        rc.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        TBSYS_LOG(INFO, "stop build index");
        chunk_server_->get_tablet_manager().get_build_index_thread().stop_all();
      }
      int serialize_ret = rc.serialize(out_buffer.get_data(),
          out_buffer.get_capacity(), out_buffer.get_position());
      if (OB_SUCCESS != serialize_ret)
      {
        TBSYS_LOG(ERROR, "cs_stop_build_index rc.serialize error, ret %d", serialize_ret);
      }
      else
      {
        chunk_server_->send_response(
            OB_CS_STOP_BUILD_INDEX_RESPONSE,
            CS_STOP_BUILD_INDEX_VERSION,
            out_buffer, req, channel_id);
      }

      return rc.result_code_;
    }

    int ObChunkService::cs_fetch_sstable_dist(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      const int32_t CS_FETCH_SSTABLE_DIST_VERSION = 1;
      int ret = OB_SUCCESS;
      int rpc_ret= OB_SUCCESS;
      int64_t table_version = 0;
      int32_t response_cid = channel_id;
      ObTabletManager& tablet_manager = chunk_server_->get_tablet_manager();
      ObPacketQueueThread& queue_thread =
        chunk_server_->get_default_task_queue_thread();
      int64_t session_id = queue_thread.generate_session_id();
      ObPacket* next_request = NULL;
      const int64_t timeout = chunk_server_->get_config().network_timeout;
      ObMultiVersionTabletImage& tablet_image = tablet_manager.get_serving_tablet_image();
      ObMemBuf mem_buf;
      bool is_increased_scan_tablet_image_count = false;
      const int64_t BUF_SIZE = OB_MAX_PACKET_LENGTH - 32*1024; //2MB - 32KB
      int64_t buf_pos = 0;
      char* buf = NULL;
      ObNewRange range;
      ObString sstable_path;
      ObTablet* tablet = NULL;
      int64_t sstable_count = 0;
      char path_buf[OB_MAX_FILE_NAME_LENGTH];

      if (in_register_process_ || tablet_manager.get_chunk_merge().is_pending_in_upgrade()
          || tablet_manager.get_bypass_sstable_loader().is_pending_upgrade()
          || 0 != atomic_compare_exchange(&scan_tablet_image_count_, 1, 0))
      {
        TBSYS_LOG(WARN, "someone else is scanning tablets. give up this process");
      }
      else
      {
        is_increased_scan_tablet_image_count = true;
      }

      if (version != CS_FETCH_SSTABLE_DIST_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
        TBSYS_LOG(WARN, "server version of cs_fetch_sstable_dist is %d, "
            "request version is %d", CS_FETCH_SSTABLE_DIST_VERSION, version);
      }
      else if (OB_SUCCESS != (ret =
            serialization::decode_vi64(in_buffer.get_data(),
              in_buffer.get_capacity(), in_buffer.get_position(), &table_version)))
      {
        TBSYS_LOG(WARN, "failed to decode table_version, cap=%ld pos=%ld ret=%d",
            in_buffer.get_capacity(), in_buffer.get_position(), ret);
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = mem_buf.ensure_space(BUF_SIZE)))
        {
          TBSYS_LOG(WARN, "failed to ensure space, buf size=%ld ret=%d", BUF_SIZE, ret);
        }
        else
        {
          buf = mem_buf.get_buffer();
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (tablet_image.get_last_not_merged_version() != table_version)
        {
          TBSYS_LOG(WARN, "serving version[%ld] of this cs is not same as request[%ld]",
              tablet_image.get_last_not_merged_version(), table_version);
          ret = OB_INVALID_DATA;
        }
        else if (OB_SUCCESS != (ret = tablet_image.begin_scan_tablets()))
        {
          TBSYS_LOG(WARN, "failed to begin scan tablets, ret=%d", ret);
        }
      }

      do
      {
        if (OB_SUCCESS == ret)
        {
          ret = tablet_image.get_next_tablet(tablet);
          if (OB_ITER_END == ret)
          {
            //do nothig
          }
          else if (OB_SUCCESS == ret && NULL != tablet)
          {
            range = tablet->get_range();
            if (range.table_id_ < common::OB_APP_MIN_TABLE_ID)
            {
              TBSYS_LOG(INFO, "skip internal table %lu", range.table_id_);
              continue;
            }

            if (table_version != tablet->get_data_version())
            {
              ret = OB_INVALID_DATA;
              TBSYS_LOG(WARN, "tablet data version[%ld]  is not same as request[%ld]",
                  tablet->get_data_version(), table_version);
            }
            else
            {
              const sstable::ObSSTableId sstable_id = tablet->get_sstable_id();
              if (sstable_id.sstable_file_id_ <= 0)
              {
                ret = OB_INVALID_DATA;
                TBSYS_LOG(WARN, "one tablet should only has one sstable, range: %s", to_cstring(range));
              }
              else
              {
                ret = get_sstable_path(sstable_id, path_buf, sizeof(path_buf));
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "failed to get sstable path with sstable id %ld range %s, ret=%d",
                      sstable_id.sstable_file_id_, to_cstring(range), ret);
                }
                else
                {
                  sstable_path.assign_ptr(path_buf, static_cast<ObString::obstr_size_t>(strlen(path_buf)));
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(WARN, "failed to get next tablet info, ret=%d", ret);
          }
        }

        if (OB_SUCCESS != ret
            || BUF_SIZE - buf_pos < range.get_serialize_size() + sstable_path.get_serialize_size())
        {
          ObResultCode rc;
          rc.result_code_ = ret;
          out_buffer.get_position() = 0;
          if (OB_SUCCESS != (rpc_ret= rc.serialize(out_buffer.get_data(),
                  out_buffer.get_capacity(), out_buffer.get_position())))
          {
            TBSYS_LOG(WARN, "failed to serialize rc, cap=%ld pos=%ld ret=%d",
                out_buffer.get_capacity(), out_buffer.get_position(), rpc_ret);
            break;
          }

          if (OB_SUCCESS == ret || OB_ITER_END == ret)
          {
            rpc_ret = common::serialization::encode_vi64(
                out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position(), sstable_count);
            if (OB_SUCCESS != rpc_ret)
            {
              TBSYS_LOG(WARN, "failed to encode sstable count, cap=%ld pos=%ld ret=%d",
                   out_buffer.get_capacity(), out_buffer.get_position(), rpc_ret);
              break;
            }
            else if (out_buffer.get_remain() < buf_pos)
            {
              TBSYS_LOG(WARN, "failed to write tablet info, buf remain %ld, buf_pos %ld",
                  out_buffer.get_remain(), buf_pos);
              rpc_ret = OB_BUF_NOT_ENOUGH;
              break;
            }
            else
            {
              ::memcpy(out_buffer.get_data() + out_buffer.get_position(), buf, buf_pos);
              out_buffer.get_position() += buf_pos;
            }
          }

          rpc_ret = queue_thread.prepare_for_next_request(session_id);
          if (OB_SUCCESS != rpc_ret)
          {
            TBSYS_LOG(WARN, "failed to prepare next request: session_id=%ld, ret=%d", session_id, rpc_ret);
            break;
          }
          else if (OB_SUCCESS != (rpc_ret =
                chunk_server_->send_response(OB_SUCCESS != ret? OB_NET_SESSION_END: OB_CS_FETCH_SSTABLE_DIST_RESPONSE,
                  CS_FETCH_SSTABLE_DIST_VERSION, out_buffer, req, response_cid, session_id)))
          {
            TBSYS_LOG(WARN, "failed to send response, response_cid=%d, session_id=%ld, ret=%d",
                response_cid, session_id, rpc_ret);
            break;
          }

          if (OB_SUCCESS != ret || OB_SUCCESS != rpc_ret)
          {
            break;
          }

          rpc_ret = queue_thread.wait_for_next_request(session_id, next_request, timeout);
          if (OB_NET_SESSION_END == rpc_ret)
          {
            rpc_ret = OB_SUCCESS;
            TBSYS_LOG(INFO, "client server stop fetching sstable dist");
            break;
          }
          else if (OB_SUCCESS != rpc_ret)
          {
            TBSYS_LOG(WARN, "failed to wait for next request, session_id=%ld timeout=%ld ret=%d",
                session_id, timeout, rpc_ret);
            break;
          }
          response_cid = next_request->get_channel_id();
          req = next_request->get_request();
          sstable_count = 0;
          buf_pos = 0;
        }

        ++sstable_count;
        if (OB_SUCCESS != (ret = range.serialize(buf, BUF_SIZE, buf_pos)))
        {
          TBSYS_LOG(WARN, "failed to serialize range: %s, buf size=%ld buf pos=%ld ret=%d",
              to_cstring(range), BUF_SIZE, buf_pos, ret);
        }
        else if (OB_SUCCESS != (ret = sstable_path.serialize(buf, BUF_SIZE, buf_pos)))
        {
          TBSYS_LOG(WARN, "failed to serialize path: %s, buf size=%ld buf pos=%ld ret=%d",
              path_buf, BUF_SIZE, buf_pos, ret);
        }
      } while(OB_SUCCESS == ret && OB_SUCCESS == rpc_ret);

      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
      if (OB_SUCCESS == ret && OB_SUCCESS != rpc_ret)
      {
        ret = rpc_ret;
      }

      int tmp_ret = tablet_image.end_scan_tablets();
      if (OB_SUCCESS != tmp_ret)
      {
        TBSYS_LOG(WARN, "failed to end scan tablets, ret=%d", tmp_ret);
        if (OB_SUCCESS == ret)
        {
          ret = tmp_ret;
        }
      }
      TBSYS_LOG(INFO, "complete to handle fetch_sstable_dist, ret=%d", ret);

      if (session_id > 0)
      {
        queue_thread.destroy_session(session_id);
      }

      if (is_increased_scan_tablet_image_count)
      {
        atomic_exchange(&scan_tablet_image_count_, 0);
      }

      return ret;
    }

    int ObChunkService::cs_set_config(
        const int32_t version,
        const int32_t channel_id,
        easy_request_t* req,
        common::ObDataBuffer& in_buffer,
        common::ObDataBuffer& out_buffer)
    {
      UNUSED(version);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;
      common::ObString config_str;
      if (OB_SUCCESS != (ret = config_str.deserialize(
                           in_buffer.get_data(),
                           in_buffer.get_capacity(), in_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "Deserialize config string failed! ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = chunk_server_->get_config().add_extra_config(config_str.ptr(), true)))
      {
        TBSYS_LOG(ERROR, "Set config failed! ret: [%d]", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "Set config successfully! str: [%s]", config_str.ptr());
      }

      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buffer.get_data(),
                                             out_buffer.get_capacity(),
                                             out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = chunk_server_->send_response(OB_SET_CONFIG_RESPONSE,
                                                                 MY_VERSION, out_buffer, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }

      return ret;
    }

    int ObChunkService::cs_get_config(
      const int32_t version,
      const int32_t channel_id,
      easy_request_t* req,
      common::ObDataBuffer& in_buffer,
      common::ObDataBuffer& out_buffer)
    {
      UNUSED(version);
      UNUSED(in_buffer);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObResultCode res;

      if (NULL == chunk_server_)
      {
        ret = OB_NOT_INIT;
      }

      res.result_code_ = ret;
      if (OB_SUCCESS != (ret = res.serialize(out_buffer.get_data(),
                                             out_buffer.get_capacity(),
                                             out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize result code fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS == res.result_code_ &&
               OB_SUCCESS !=
               (ret = chunk_server_->get_config().serialize(out_buffer.get_data(),
                                                            out_buffer.get_capacity(),
                                                            out_buffer.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize configuration fail, ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = chunk_server_->send_response(OB_GET_CONFIG_RESPONSE,
                                                                 MY_VERSION, out_buffer, req, channel_id)))
      {
        TBSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }

      return ret;
    }

    bool ObChunkService::is_valid_lease()
    {
      int64_t current_time = tbsys::CTimeUtil::getTime();
      return current_time < service_expired_time_;
    }

    void ObChunkService::LeaseChecker::runTimerTask()
    {
      if (NULL != service_ && service_->inited_)
      {
        if (!service_->is_valid_lease() && !service_->in_register_process_ )
        {
          TBSYS_LOG(INFO, "lease expired, re-register to root_server");
          service_->register_self();
        }


        // reschedule
        service_->timer_.schedule(*this,
            service_->chunk_server_->get_config().lease_check_interval, false);
      }
    }

    void ObChunkService::StatUpdater::runTimerTask()
    {
      ObStat *stat = NULL;
      OB_STAT_GET(CHUNKSERVER, stat);
      if (NULL == stat)
      {
        TBSYS_LOG(DEBUG,"get stat failed");
      }
      else
      {
        int64_t request_count = stat->get_value(INDEX_META_REQUEST_COUNT);
        current_request_count_ = request_count - pre_request_count_;
        stat->set_value(INDEX_META_REQUEST_COUNT_PER_SECOND,current_request_count_);
        pre_request_count_ = request_count;

        // memory usage stats
        ObMultiVersionTabletImage::ObTabletImageStat image_stat;
        ObTabletManager& manager = THE_CHUNK_SERVER.get_tablet_manager();
        manager.get_serving_tablet_image().get_image_stat(image_stat);
        OB_STAT_SET(CHUNKSERVER, INDEX_CS_SERVING_VERSION, manager.get_last_not_merged_version());
        OB_STAT_SET(CHUNKSERVER, INDEX_META_OLD_VER_TABLETS_NUM, image_stat.old_ver_tablets_num_);
        OB_STAT_SET(CHUNKSERVER, INDEX_META_OLD_VER_MERGED_TABLETS_NUM, image_stat.old_ver_merged_tablets_num_);
        OB_STAT_SET(CHUNKSERVER, INDEX_META_NEW_VER_TABLETS_NUM, image_stat.new_ver_tablets_num_);
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_DEFAULT, ob_get_mod_memory_usage(ObModIds::OB_MOD_DEFAULT));
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_NETWORK, ob_get_mod_memory_usage(ObModIds::OB_COMMON_NETWORK));
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_THREAD_BUFFER, ob_get_mod_memory_usage(ObModIds::OB_THREAD_BUFFER));
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_TABLET, ob_get_mod_memory_usage(ObModIds::OB_CS_TABLET_IMAGE));
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_BI_CACHE, manager.get_block_index_cache().get_cache_mem_size());
        OB_STAT_SET(CHUNKSERVER, INDEX_MU_BLOCK_CACHE, manager.get_block_cache().size());
        if (manager.get_join_cache().is_inited())
        {
          OB_STAT_SET(CHUNKSERVER, INDEX_MU_JOIN_CACHE, manager.get_join_cache().get_cache_mem_size());
        }
        if (NULL != manager.get_row_cache())
        {
          OB_STAT_SET(CHUNKSERVER, INDEX_MU_SSTABLE_ROW_CACHE, manager.get_row_cache()->get_cache_mem_size());
        }

        if (run_count_++ % 600 == 0) ob_print_mod_memory_usage();
      }
    }

    void ObChunkService::MergeTask::runTimerTask()
    {
      int err = OB_SUCCESS;
      ObTabletManager & tablet_manager = service_->chunk_server_->get_tablet_manager();
      unset_scheduled();

      if (frozen_version_ <= 0)
      {
        //initialize phase or slave rs switch to master but get frozen version failed
      }
      else if (frozen_version_  < tablet_manager.get_serving_tablet_image().get_newest_version())
      {
        TBSYS_LOG(ERROR,"mem frozen version (%ld) < newest local tablet version (%ld),exit",frozen_version_,
            tablet_manager.get_serving_tablet_image().get_newest_version());
        kill(getpid(),2);
      }
      else if (tablet_manager.get_chunk_merge().can_launch_next_round(frozen_version_))
      {
        err = tablet_manager.merge_tablets(frozen_version_);
        if (err != OB_SUCCESS)
        {
          frozen_version_ = 0; //failed,wait for the next schedule
        }
      }
      else
      {
        tablet_manager.get_chunk_merge().set_newest_frozen_version(frozen_version_);
      }
    }

    int ObChunkService::fetch_update_server_list()
    {
      int err = OB_SUCCESS;

      if (NULL == chunk_server_->get_rpc_proxy())
      {
        TBSYS_LOG(ERROR, "rpc_proxy_ is NULL");
      }
      else
      {
        int32_t count = 0;
        err = chunk_server_->get_rpc_proxy()->fetch_update_server_list(count);
        if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fetch update server list failed:ret[%d]", err);
        }
        else
        {
          TBSYS_LOG(DEBUG, "fetch update server list succ:count[%d]", count);
        }
      }

      return err;
    }

    void ObChunkService::FetchUpsTask::runTimerTask()
    {
      service_->fetch_update_server_list();
      // reschedule fetch updateserver list task with new interval.
      service_->timer_.schedule(*this,
        service_->chunk_server_->get_config().fetch_ups_interval, false);
    }


  } // end namespace chunkserver
} // end namespace oceanbase
