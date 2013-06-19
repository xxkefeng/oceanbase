/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_rpc_stub.cpp for rpc among chunk server, update server and
 * root server.
 *
 * Authors:
 *   qushan <qushan@alipay.com>
 *
 */
#include "ob_general_rpc_stub.h"
#include "ob_client_manager.h"
#include "ob_obi_role.h"
#include "ob_server.h"
#include "ob_result.h"
#include "ob_operate_result.h"
#include "thread_buffer.h"
#include "ob_schema.h"
#include "ob_tablet_info.h"
#include "ob_read_common_data.h"
#include "ob_scanner.h"
#include "ob_trace_log.h"
#include "utility.h"
#include "ob_tbnet_callback.h"
#include "ob_schema_service.h"
#include "ob_strings.h"
#include "ob_mutator.h"
#include "ob_ups_info.h"
#include "location/ob_tablet_location_list.h"
#include "sql/ob_ups_result.h"
#include "sql/ob_physical_plan.h"
#include "sql/ob_sql_result_set.h"

namespace oceanbase
{
  namespace common
  {
    ObGeneralRpcStub::ObGeneralRpcStub()
    {
    }

    ObGeneralRpcStub::~ObGeneralRpcStub()
    {
    }

    int ObGeneralRpcStub::register_server(const int64_t timeout, const ObServer & root_server,
        const ObServer & merge_server, const bool is_merger, int32_t & status, const char* server_version) const
    {
      return send_3_return_1(root_server, timeout, OB_SERVER_REGISTER,
                             DEFAULT_VERSION, merge_server, is_merger, server_version, status);
    }

    int ObGeneralRpcStub::register_merge_server(const int64_t timeout,
                                                const common::ObServer & root_server,
                                                const common::ObServer & server,
                                                const int32_t sql_port, int32_t &status, const char* server_version) const
    {
      return send_3_return_1(root_server, timeout, OB_MERGE_SERVER_REGISTER,
                             DEFAULT_VERSION, server, sql_port, server_version, status);
    }

    // heartbeat rpc through register server interface
    int ObGeneralRpcStub::heartbeat_server(const int64_t timeout, const ObServer & root_server,
        const ObServer & merge_server, const ObRole server_role) const
    {
      return post_request_2(root_server, timeout, OB_HEARTBEAT, NEW_VERSION,
          ObTbnetCallback::default_callback, NULL,
          merge_server, static_cast<int32_t>(server_role));
    }

    int ObGeneralRpcStub::find_server(const int64_t timeout, const ObServer & root_server,
        ObServer & update_server) const
    {
      return send_0_return_1(root_server, timeout,
          OB_GET_UPDATE_SERVER_INFO, DEFAULT_VERSION, update_server);
    }

    int ObGeneralRpcStub::fetch_update_server(
        const int64_t timeout, const ObServer & root_server,
        ObServer & update_server, bool for_merge) const
    {
      return send_0_return_1(root_server, timeout,
          for_merge ? OB_GET_UPDATE_SERVER_INFO_FOR_MERGE : OB_GET_UPDATE_SERVER_INFO,
          DEFAULT_VERSION, update_server);
    }

    int ObGeneralRpcStub::fetch_server_list(const int64_t timeout, const ObServer & root_server,
        ObUpsList & server_list) const
    {
      return send_0_return_1(root_server, timeout, OB_GET_UPS, DEFAULT_VERSION, server_list);
    }


    int ObGeneralRpcStub::fetch_frozen_time(
        const int64_t timeout, ObServer & update_server,
        const int64_t frozen_version, int64_t& frozen_time) const
    {
      UNUSED(frozen_version);
      return send_1_return_1(update_server, timeout, OB_UPS_GET_TABLE_TIME_STAMP,
          DEFAULT_VERSION, frozen_version, frozen_time);
    }

    // fetch schema current version
    int ObGeneralRpcStub::fetch_schema_version(
        const int64_t timeout, const common::ObServer & root_server,
        int64_t & timestamp) const
    {
      return send_0_return_1(root_server, timeout, OB_FETCH_SCHEMA_VERSION, DEFAULT_VERSION, timestamp);
    }

    int ObGeneralRpcStub::fetch_schema(
        const int64_t timeout, const ObServer & root_server,
        const int64_t version, const bool only_core_tables, ObSchemaManagerV2 & schema) const
    {
      return send_2_return_1(root_server, timeout, OB_FETCH_SCHEMA, NEW_VERSION,
          version, only_core_tables, schema);
    }

    int ObGeneralRpcStub::fetch_tablet_location(
        const int64_t timeout, const ObServer & root_server,
        const uint64_t root_table_id, const uint64_t table_id,
        const ObRowkey & row_key, ObScanner & scanner) const
    {
      ObCellInfo cell;
      // cell info not root table id
      UNUSED(root_table_id);
      cell.table_id_ = table_id;
      cell.column_id_ = 0;
      cell.row_key_ = row_key;
      ObGetParam get_param;
      get_param.set_is_result_cached(false);
      get_param.set_is_read_consistency(false);
      int ret = get_param.add_cell(cell);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "add cell to get param failed:ret[%d]", ret);
      }
      else
      {
        ret = ObDataRpcStub::get(timeout, root_server, get_param, scanner);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "scan root server for get chunk server location failed:"
              "table_id[%lu], rowkey[%s], ret[%d]", table_id, to_cstring(row_key), ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "scan root server for get chunk server location succ:"
              "table_id[%lu]", table_id);
        }
      }
      return ret;
    }

    int ObGeneralRpcStub::reload_self_config(const int64_t timeout, const ObServer & merge_server, const char *filename) const
    {
      ObString file_str;
      file_str.assign(const_cast<char*>(filename), static_cast<int32_t>(strlen(filename)));
      return send_1_return_0(merge_server, timeout, OB_UPS_RELOAD_CONF, DEFAULT_VERSION, file_str);
    }


    int ObGeneralRpcStub::create_table(const int64_t timeout, const common::ObServer & root_server,
        bool if_not_exists, const common::TableSchema & tschema) const
    {
      int ret = OB_SUCCESS;
      ObResultCode result_code;
      ret = send_2_return_0(root_server, timeout, OB_CREATE_TABLE, DEFAULT_VERSION,
          result_code, if_not_exists, tschema);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "send_2_return_0 failed ret[%d]", ret);
        TBSYS_LOG(USER_ERROR, "%.*s", result_code.message_.length(), result_code.message_.ptr());
      }
      return ret;
    }

    int ObGeneralRpcStub::drop_table(const int64_t timeout, const common::ObServer & root_server,
        bool if_exists, const common::ObStrings &tables) const
    {
      int ret = OB_SUCCESS;
      ObResultCode result_code;
      ret = send_2_return_0(root_server, timeout, OB_DROP_TABLE, DEFAULT_VERSION,
          result_code, if_exists, tables);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "send_2_return_0 failed: ret[%d]", ret);
        TBSYS_LOG(USER_ERROR, "%.*s", result_code.message_.length(), result_code.message_.ptr());
      }
      return ret;
    }

    int ObGeneralRpcStub::alter_table(const int64_t timeout, const common::ObServer & root_server,
        const common::AlterTableSchema & alter_schema) const
    {
      int ret = OB_SUCCESS;
      ObResultCode result_code;
      ret = send_1_return_0(root_server, timeout, OB_ALTER_TABLE, DEFAULT_VERSION,
          result_code, alter_schema);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "send_1_return_0 failed ret[%d]", ret);
        TBSYS_LOG(USER_ERROR, "%.*s", result_code.message_.length(), result_code.message_.ptr());
      }
      return ret;
    }

    int ObGeneralRpcStub::mutate(const int64_t timeout, const ObServer & server,
        const ObMutator & mutate_param, const bool has_data, ObScanner & scanner) const
    {
      int ret = OB_SUCCESS;
      if (has_data)
      {
        ret = send_1_return_1(server, timeout, OB_MS_MUTATE, DEFAULT_VERSION, mutate_param, scanner);
      }
      else
      {
        ret = send_1_return_0(server, timeout, OB_MS_MUTATE, DEFAULT_VERSION, mutate_param);
      }
      return ret;
    }

    int ObGeneralRpcStub::ups_apply(const int64_t timeout, const ObServer & server, 
        const ObMutator & mutate_param) const
    {
      return send_1_return_0(server, timeout, OB_WRITE, DEFAULT_VERSION, mutate_param);
    }

    int ObGeneralRpcStub::report_tablets(
        const int64_t timeout, const ObServer & root_server,
        const ObServer &client_server, const ObTabletReportInfoList& tablets,
        int64_t time_stamp, bool has_more)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = send_3_return_0(root_server, timeout, OB_REPORT_TABLETS, DEFAULT_VERSION,
              client_server, tablets, time_stamp)))
      {
        TBSYS_LOG(WARN, "send report tablets message failed, ret=%d", ret);
      }
      else if (!has_more)  // at the end of report process
      {
        // reset param buffer, for new remote procedure call process(OB_WAITING_JOB_DONE)
        TBSYS_LOG(INFO, "report tablets over, send OB_WAITING_JOB_DONE message.");
        ret = send_2_return_0(root_server, timeout, OB_WAITING_JOB_DONE, DEFAULT_VERSION,
            client_server, time_stamp);
      }

      return ret;
    }

    int ObGeneralRpcStub::delete_tablets(const int64_t timeout, const ObServer & root_server,
        const ObServer &client_server, const common::ObTabletReportInfoList& tablets)
    {
      return send_2_return_0(root_server, timeout, OB_CS_DELETE_TABLETS, DEFAULT_VERSION,
          client_server, tablets);
    }

    int ObGeneralRpcStub::report_samples(const int64_t timeout, const ObServer & root_server,
        const bool is_succ, const ObServer &client_server, const uint64_t index_table_id, 
        const ObNewScanner& result, bool has_more)
    {
      int ret = OB_SUCCESS;

      if (is_succ && OB_SUCCESS != (ret = send_2_return_0(root_server, timeout, OB_CS_REPORT_SAMPLES, 
            DEFAULT_VERSION, client_server, result)))
      {
        TBSYS_LOG(WARN, "send report samples message failed, ret=%d", ret);
      }
      else if (!has_more)  // at the end of report process
      {
        TBSYS_LOG(INFO, "report samples over, send OB_CS_SAMPLE_TABLE_DONE message.");
        ret = send_3_return_0(root_server, timeout, OB_CS_SAMPLE_TABLE_DONE, DEFAULT_VERSION,
            is_succ, client_server, static_cast<int64_t>(index_table_id));
      }

      return ret;
    }

    int ObGeneralRpcStub::build_index_over(const int64_t timeout, const ObServer & root_server,
        const bool is_succ, const ObServer &client_server, const uint64_t index_table_id)
    {
      return send_3_return_0(root_server, timeout, OB_CS_BUILD_INDEX_DONE, DEFAULT_VERSION,
              is_succ, client_server, static_cast<int64_t>(index_table_id));
    }

    int ObGeneralRpcStub::dest_load_tablet(
        const int64_t timeout,
        const common::ObServer &dest_server,
        const common::ObNewRange &range,
        const int32_t dest_disk_no,
        const int64_t tablet_version,
        const int64_t tablet_seq_num,
        const uint64_t crc_sum,
        const int64_t size,
        const char (*path)[common::OB_MAX_FILE_NAME_LENGTH])
    {
      int ret = OB_SUCCESS;
      const int32_t CS_DEST_LOAD_TABLET_VERSION = 2;
      ObDataBuffer data_buff;
      ret = get_rpc_buffer(data_buff);

      // step 1. serialize range to data_buff
      if (OB_SUCCESS != (ret = range.serialize(data_buff.get_data(),
              data_buff.get_capacity(), data_buff.get_position())))
      {
        TBSYS_LOG(ERROR, "serialize range failed[%d]", ret);
      }
      // dest_disk_no
      else if ( OB_SUCCESS != (ret = serialization::encode_vi32(data_buff.get_data(),
              data_buff.get_capacity(), data_buff.get_position(), dest_disk_no)))
      {
        TBSYS_LOG(ERROR, "serialize dest_disk_no failed[%d]", ret);
      }
      // tablet_version
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(data_buff.get_data(),
              data_buff.get_capacity(), data_buff.get_position(), tablet_version)))
      {
        TBSYS_LOG(ERROR, "serialize tablet_version failed=[%d]", ret);
      }
      // crc checksum
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(data_buff.get_data(),
              data_buff.get_capacity(), data_buff.get_position(), crc_sum)))
      {
        TBSYS_LOG(ERROR, "serialize crc_sum failed=[%d]", ret);
      }
      // number of sstable files
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(data_buff.get_data(),
              data_buff.get_capacity(), data_buff.get_position(), size)))
      {
        TBSYS_LOG(ERROR, "serialize file num failed=[%d]", ret);
      }

      // path
      if (OB_SUCCESS == ret)
      {
        for ( int64_t idx =0 ; idx < size; idx++)
        {
          ret = serialization::encode_vstr(data_buff.get_data(), data_buff.get_capacity(),
              data_buff.get_position(), path[idx]);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "serialize file path failed=[%d]", ret);
            break;
          }
        }
      }

      // tablet sequence number
      if (OB_SUCCESS == ret && CS_DEST_LOAD_TABLET_VERSION > 1)
      {
        ret = serialization::encode_vi64(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position(), tablet_seq_num);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "serialize tablet_seq_num failed=[%d]", ret);
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(dest_server,
            OB_MIGRATE_OVER, CS_DEST_LOAD_TABLET_VERSION, timeout, data_buff);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "send request to chunkserver for dest load tablet"
              ",ret=[%d].", ret);
        }
        else
        {
          TBSYS_LOG(INFO,"send request to destination server success");
        }
      }


      // step 3. deserialize the response code
      int64_t pos = 0;
      if (OB_SUCCESS == ret)
      {
        ObResultCode result_code;
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d].", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
        }
      }
      return ret;
    }

    int ObGeneralRpcStub::migrate_over(
        const int64_t timeout,
        const ObServer & root_server,
        const common::ObNewRange &range,
        const common::ObServer &src_server,
        const common::ObServer &dest_server,
        const bool keep_src,
        const int64_t tablet_version,
        const int64_t tablet_seq_num)
    {
      const int32_t CS_MIGRATE_OVER_VERSION = 2;
      return send_6_return_0(root_server, timeout, OB_MIGRATE_OVER, CS_MIGRATE_OVER_VERSION,
          range, src_server, dest_server, keep_src, tablet_version, tablet_seq_num);
    }

    int ObGeneralRpcStub::report_capacity_info(
        const int64_t timeout, const ObServer & root_server,
        const common::ObServer &server, const int64_t capacity, const int64_t used)
    {
      return send_3_return_0(root_server, timeout, OB_REPORT_CAPACITY_INFO,
          DEFAULT_VERSION, server, capacity, used);
    }

    int ObGeneralRpcStub::get_migrate_dest_location( const int64_t timeout,
        const ObServer & dest_server, const int64_t occupy_size,
        int32_t &dest_disk_no, common::ObString &dest_path)
    {
      return send_1_return_2(dest_server, timeout, OB_CS_GET_MIGRATE_DEST_LOC,
          DEFAULT_VERSION, occupy_size, dest_disk_no, dest_path);
    }

    int ObGeneralRpcStub::get_last_frozen_memtable_version(const int64_t timeout,
        const ObServer & update_server, int64_t &last_version) const
    {
      return send_0_return_1(update_server, timeout, OB_UPS_GET_LAST_FROZEN_VERSION,
          DEFAULT_VERSION, last_version);
    }

    int ObGeneralRpcStub::get_tablet_info(
        const int64_t timeout, const ObServer & root_server,
        const common::ObSchemaManagerV2& schema,
        const uint64_t table_id, const common::ObNewRange& range,
        ObTabletLocation location [], int32_t& size)
    {
      int ret = OB_SUCCESS;
      int32_t index = 0;
      if (OB_INVALID_ID == table_id || size <= 0)
      {
        TBSYS_LOG(ERROR,"invalid table id");
        ret = OB_ERROR;
      }

      ObScanParam param;
      ObScanner scanner;
      ObString table_name;

      const ObTableSchema *table = schema.get_table_schema(table_id);

      if (NULL == table)
      {
        TBSYS_LOG(ERROR,"can not find table %lu",table_id);
        ret = OB_ERROR;
      }
      else
      {
        table_name.assign_ptr(const_cast<char *>(table->get_table_name()),
            static_cast<int32_t>(strlen(table->get_table_name())));
      }

      if (OB_SUCCESS == ret)
      {
        param.set(OB_INVALID_ID,table_name,range); //use table name
        param.set_is_read_consistency(false);
      }

      if ((OB_SUCCESS == ret) && ((ret = scan(timeout, root_server, param, scanner)) != OB_SUCCESS) )
      {
        TBSYS_LOG(ERROR,"get tablet from rootserver failed:[%d]",ret);
      }

      ObServer server;
      ObCellInfo * cell = NULL;
      ObScannerIterator iter;
      bool row_change = false;

      if (OB_SUCCESS == ret)
      {
        int64_t ip = 0;
        int64_t port = 0;
        int64_t version = 0;
        iter = scanner.begin();
        ret = iter.get_cell(&cell, &row_change);
        row_change = false;

        while((OB_SUCCESS == ret) && index < size)
        {
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
          }
          else if (row_change)
          {
            TBSYS_LOG(DEBUG,"row :%s changed,ignore", to_cstring(cell->row_key_));
            break; //just get one row
          }
          else if (cell != NULL)
          {
            if ((cell->column_name_.compare("1_port") == 0)
                || (cell->column_name_.compare("2_port") == 0)
                || (cell->column_name_.compare("3_port") == 0))
            {
              ret = cell->value_.get_int(port);
              TBSYS_LOG(DEBUG,"port is %ld",port);
            }
            else if ((cell->column_name_.compare("1_ipv4") == 0)
                || (cell->column_name_.compare("2_ipv4") == 0)
                || (cell->column_name_.compare("3_ipv4") == 0))
            {
              ret = cell->value_.get_int(ip);
              TBSYS_LOG(DEBUG,"ip is %ld",ip);
            }
            else if (cell->column_name_.compare("1_tablet_version") == 0 ||
                cell->column_name_.compare("2_tablet_version") == 0 ||
                cell->column_name_.compare("3_tablet_version") == 0)
            {
              ret = cell->value_.get_int(version);
              TBSYS_LOG(DEBUG,"tablet_version is %ld, rowkey:%s",version, to_cstring(cell->row_key_));
            }

            if (OB_SUCCESS == ret)
            {
              if (0 != port && 0 != ip && 0 != version)
              {
                server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
                TBSYS_LOG(INFO,"add tablet s:%s,%ld", to_cstring(server), version);
                ObTabletLocation addr(version, server);
                location[index++] = addr;
                ip = port = version = 0;
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
            }

            if (++iter == scanner.end())
              break;
            ret = iter.get_cell(&cell, &row_change);
          }
          else
          {
            //impossible
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        size = index;
        TBSYS_LOG(INFO,"get %d tablets from rootserver",size);
      }
      return ret;
    }

    int ObGeneralRpcStub::merge_tablets_over(const int64_t timeout, const ObServer & root_server,
        const common::ObTabletReportInfoList& tablet_list, const bool is_merge_succ)
    {
      int ret = OB_SUCCESS;
      if (is_merge_succ)
      {
        ret = send_1_return_0(root_server, timeout, OB_CS_MERGE_TABLETS_DONE, DEFAULT_VERSION,
            is_merge_succ);
      }
      else
      {
        ret = send_2_return_0(root_server, timeout, OB_CS_MERGE_TABLETS_DONE, DEFAULT_VERSION,
            is_merge_succ, tablet_list);
      }
      return ret;
    }

    int ObGeneralRpcStub::scan(const int64_t timeout, const ObServer & server,
        const ObScanParam & scan_param, ObScanner & scanner) const
    {
      int ret = OB_SUCCESS;
      ObDataBuffer data_buff;
      ret = get_rpc_buffer(data_buff);
      // step 1. serialize ObScanParam to the data_buff
      if (OB_SUCCESS == ret)
      {
        ret = scan_param.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "serialize scan param failed:ret[%d]", ret);
        }
      }
      // step 2. send request for scan data
      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(server, OB_SCAN_REQUEST, DEFAULT_VERSION,
            timeout, data_buff);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "send scan request to server failed:ret[%d]", ret);
        }
      }
      // step 3. deserialize the response result
      int64_t pos = 0;
      if (OB_SUCCESS == ret)
      {
        ObResultCode result_code;
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
          if (OB_UNLIKELY(OB_SUCCESS != ret))
          {
            TBSYS_LOG(USER_ERROR, "%.*s", result_code.message_.length(), result_code.message_.ptr());
          }
        }
      }

      // step 4. deserialize the scanner
      if (OB_SUCCESS == ret)
      {
        scanner.clear();
        ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
        }
      }

      // write debug log
      bool is_fullfilled = false;
      int64_t fullfilled_item_num = 0;
      int64_t res_size = pos;
      scanner.get_is_req_fullfilled(is_fullfilled,fullfilled_item_num);
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "scan data succ from server:addr[%s]", to_cstring(server));
      }
      else
      {
        TBSYS_LOG(WARN, "scan data failed from server:addr[%s], ret[%d]", to_cstring(server), ret);
      }
      FILL_TRACE_LOG("step 3.* finish server scan:addr[%s], err[%d] fullfill[%d] item_num[%ld] res_size[%ld]",
          to_cstring(server), ret, is_fullfilled, fullfilled_item_num, res_size);
      return ret;
    }

    int ObGeneralRpcStub::get(const int64_t timeout, const ObServer & server,
        const ObGetParam & get_param, ObScanner & scanner) const
    {
      int ret = OB_SUCCESS;
      ObDataBuffer data_buff;
      ret = get_rpc_buffer(data_buff);
      // step 1. serialize ObGetParam to the data_buff
      if (OB_SUCCESS == ret)
      {
        ret = get_param.serialize(data_buff.get_data(), data_buff.get_capacity(),
            data_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "serialize get_param failed:ret[%d]", ret);
        }
      }
      // step 2. send request for get data
      if (OB_SUCCESS == ret)
      {
        ret = rpc_frame_->send_request(server, OB_GET_REQUEST, DEFAULT_VERSION,
            timeout, data_buff);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "send get request to server failed:ret[%d]", ret);
        }
      }
      // step 3. deserialize the response result
      int64_t pos = 0;
      if (OB_SUCCESS == ret)
      {
        ObResultCode result_code;
        ret = result_code.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize result failed:pos[%ld], ret[%d]", pos, ret);
        }
        else
        {
          ret = result_code.result_code_;
          if (OB_UNLIKELY(OB_SUCCESS != ret))
          {
            TBSYS_LOG(USER_ERROR, "%.*s", result_code.message_.length(), result_code.message_.ptr());
          }
        }
      }
      // step 4. deserialize the scanner
      if (OB_SUCCESS == ret)
      {
        scanner.clear();
        ret = scanner.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize scanner from buff failed:pos[%ld], ret[%d]", pos, ret);
        }
      }
      bool is_fullfilled = false;
      int64_t fullfilled_item_num = 0;
      int64_t res_size = pos;
      scanner.get_is_req_fullfilled(is_fullfilled,fullfilled_item_num);
      // write debug log
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(DEBUG, "get data succ from server:addr[%s]", to_cstring(server));
      }
      else
      {
        TBSYS_LOG(WARN, "get data failed from server:addr[%s], ret[%d]", to_cstring(server), ret);
      }
      FILL_TRACE_LOG("step 3.* finish server get:addr[%s], err[%d] fullfill[%d] item_num[%ld] res_size[%ld]",
          to_cstring(server), ret, is_fullfilled, fullfilled_item_num, res_size);
      return ret;
    }



    // server scan
    int ObGeneralRpcStub::scan(const int64_t timeout, ObTabletLocationList & list,
        const ObScanParam & scan_param, ObTabletLocationItem & succ_addr,
        ObScanner & scanner, bool & update_list) const
    {
      int ret = OB_SUCCESS;
      if (0 == list.size())
      {
        TBSYS_LOG(WARN, "%s", "check list size is zero");
        ret = OB_DATA_NOT_SERVE;
      }
      else
      {
        // set all invlaid item to valid status
        if (list.get_valid_count() < 1)
        {
          list.set_item_valid(tbsys::CTimeUtil::getTime());
        }

        ret = OB_CHUNK_SERVER_ERROR;
        for (int32_t i = 0; i < list.size(); ++i)
        {
          if (list[i].err_times_ >= ObTabletLocationItem::MAX_ERR_TIMES)
          {
            TBSYS_LOG(DEBUG, "check server err times too much:times[%ld]", list[i].err_times_);
            continue;
          }
          scanner.clear();
          ret = scan(timeout, list[i].server_.chunkserver_, scan_param, scanner);
          if (OB_CS_TABLET_NOT_EXIST == ret)
          {
            TBSYS_LOG(WARN, "check chunk server position failed:pos[%d], count[%ld], ret[%d]",
                i, list.size(), ret);
            list[i].err_times_ = ObTabletLocationItem::MAX_ERR_TIMES;
            update_list = true;
            continue;
          }
          else if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "scan from chunk server failed:pos[%d], count[%ld], ret[%d]",
                i, list.size(), ret);
            ++list[i].err_times_;
            update_list = true;
            continue;
          }
          else
          {
            TBSYS_LOG(DEBUG, "scan from chunk server succ:pos[%d], count[%ld]", i, list.size());
            if (list[i].err_times_ != 0)
            {
              list[i].err_times_ = 0;
              update_list = true;
            }
            succ_addr = list[i];
            break;
          }
        }
      }
      return ret;
    }

    // server get
    int ObGeneralRpcStub::get(const int64_t timeout, ObTabletLocationList & list,
        const ObGetParam & get_param, ObTabletLocationItem & succ_addr,
        ObScanner & scanner, bool & update_list) const
    {
      int ret = OB_SUCCESS;
      if (0 == list.size())
      {
        TBSYS_LOG(WARN, "%s", "check list size is zero");
        ret = OB_DATA_NOT_SERVE;
      }
      else
      {
        // set all invlaid item to valid status
        if (list.get_valid_count() < 1)
        {
          list.set_item_valid(tbsys::CTimeUtil::getTime());
        }

        ret = OB_CHUNK_SERVER_ERROR;
        for (int32_t i = 0; i < list.size(); ++i)
        {
          if (list[i].err_times_ >= ObTabletLocationItem::MAX_ERR_TIMES)
          {
            TBSYS_LOG(DEBUG, "check server err times too much:times[%ld]", list[i].err_times_);
            continue;
          }
          scanner.clear();
          ret = get(timeout, list[i].server_.chunkserver_, get_param, scanner);
          if (OB_CS_TABLET_NOT_EXIST == ret)
          {
            TBSYS_LOG(WARN, "check chunk server position failed:pos[%d], count[%ld], ret[%d]",
                i, list.size(), ret);
            list[i].err_times_ = ObTabletLocationItem::MAX_ERR_TIMES;
            update_list = true;
            continue;
          }
          else if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "get from chunk server failed:pos[%d], count[%ld], ret[%d]",
                i, list.size(), ret);
            ++list[i].err_times_;
            update_list = true;
            continue;
          }
          else
          {
            TBSYS_LOG(DEBUG, "get from chunk server succ:pos[%d], count[%ld]", i, list.size());
            if (list[i].err_times_ != 0)
            {
              list[i].err_times_ = 0;
              update_list = true;
            }
            succ_addr = list[i];
            break;
          }
        }
      }
      return ret;
    }

    int ObGeneralRpcStub::load_bypass_sstables_over(
      const int64_t timeout, const ObServer & root_server,
      const ObServer& self, const ObTableImportInfoList& table_list, const bool is_load_succ)
    {
      return send_3_return_0(root_server, timeout, OB_CS_LOAD_BYPASS_SSTABLE_DONE, DEFAULT_VERSION,
            self, table_list, is_load_succ);
    }

    int ObGeneralRpcStub::delete_table_over(
      const int64_t timeout, const ObServer & root_server,
      const ObServer& self, const uint64_t table_id, const bool is_delete_succ)
    {
      return send_3_return_0(root_server, timeout, OB_CS_DELETE_TABLE_DONE, DEFAULT_VERSION,
            self, static_cast<int64_t>(table_id), is_delete_succ);
    }

    int ObGeneralRpcStub::get_obi_role(const int64_t timeout_us, const common::ObServer& root_server, common::ObiRole &obi_role) const
    {
      return send_0_return_1(root_server, timeout_us, OB_GET_OBI_ROLE, DEFAULT_VERSION, obi_role);
    }

    int ObGeneralRpcStub::get_master_ups_info(const int64_t timeout_us, const ObServer& root_server, ObServer &master_ups) const
    {
      return send_0_return_1(root_server, timeout_us, OB_GET_UPDATE_SERVER_INFO, DEFAULT_VERSION, master_ups);
    }

    int ObGeneralRpcStub::ups_plan_execute(const int64_t timeout, const ObServer & ups,
                                           const sql::ObPhysicalPlan &plan, sql::ObUpsResult &result) const
    {
      int ret = OB_SUCCESS;
      ObResultCode result_code;
      ret = send_1_return_1(ups, timeout, OB_PHY_PLAN_EXECUTE, DEFAULT_VERSION,
                            result_code, plan, result);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(USER_ERROR, "%.*s", result_code.message_.length(), result_code.message_.ptr());
      }
      return ret;
    }

    int ObGeneralRpcStub::ups_start_trans(const int64_t timeout, const ObServer & ups,
                                          const ObTransReq &req, ObTransID &trans_id) const
    {
      int ret = OB_SUCCESS;
      ObResultCode result_code;
      ret = send_1_return_1(ups, timeout, OB_START_TRANSACTION, DEFAULT_VERSION,
                            result_code, req, trans_id);
      return ret;
    }

    int ObGeneralRpcStub::ups_end_trans(const int64_t timeout, const ObServer & ups,
                                        const ObEndTransReq &req) const
    {
      int ret = OB_SUCCESS;
      ObResultCode result_code;
      ret = send_1_return_0(ups, timeout, OB_END_TRANSACTION, DEFAULT_VERSION,
                            result_code, req);
      return ret;
    }
    int ObGeneralRpcStub::execute_sql(const int64_t timeout, const ObServer & ms, const ObString &sql_str) const
    {
      return send_1_return_0(ms, timeout, OB_SQL_EXECUTE, DEFAULT_VERSION, sql_str);
    }
    int ObGeneralRpcStub::execute_sql(const int64_t timeout, const ObServer & ms, 
        const ObString &sql_str, sql::ObSQLResultSet& result) const
    {
      return send_1_return_1(ms, timeout, OB_SQL_EXECUTE, DEFAULT_VERSION, sql_str, result);
    }
    int ObGeneralRpcStub::get_master_obi_rs(const int64_t timeout,
                                            const ObServer &rootserver,
                                            ObServer &master_obi_rs) const
    {
      int ret = OB_SUCCESS;
      ret = send_0_return_1(rootserver, timeout, OB_GET_MASTER_OBI_RS,
                            DEFAULT_VERSION, master_obi_rs);
      return ret;
    }

  } // end namespace chunkserver
} // end namespace oceanbase
