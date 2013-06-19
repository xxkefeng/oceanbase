/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_rpc_proxy.h for rpc among chunk server, update server and
 * root server.
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_rpc_proxy.h"
#include "common/ob_rpc_stub.h"

#include "common/utility.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_trace_log.h"
#include "common/ob_crc64.h"
#include "common/ob_schema_manager.h"
#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObMergerRpcProxy::ObMergerRpcProxy():ups_list_lock_(tbsys::WRITE_PRIORITY)
    {
      init_ = false;
      rpc_stub_ = NULL;
      rpc_retry_times_ = 0;
      rpc_timeout_ = 0;
      min_fetch_interval_ = 10 * 1000 * 1000L;
      fetch_ups_timestamp_ = 0;
      cur_finger_print_ = 0;
      fail_count_threshold_ = 100;
      black_list_timeout_ = 60 * 1000 * 1000L;
    }

    ObMergerRpcProxy::ObMergerRpcProxy(
        const int64_t retry_times, const int64_t timeout,
        const ObServer & root_server)
    {
      init_ = false;
      rpc_retry_times_ = retry_times;
      rpc_timeout_ = timeout;
      root_server_ = root_server;
      min_fetch_interval_ = 10 * 1000 * 1000L;
      fetch_ups_timestamp_ = 0;
      rpc_stub_ = NULL;
      cur_finger_print_ = 0;
      fail_count_threshold_ = 100;
      black_list_timeout_ = 60 * 1000 * 1000L;
    }

    ObMergerRpcProxy::~ObMergerRpcProxy()
    {
    }

    bool ObMergerRpcProxy::check_inner_stat(void) const
    {
      return(init_ && (NULL != rpc_stub_));
    }

    int ObMergerRpcProxy::init(
        common::ObGeneralRpcStub * rpc_stub, ObSqlRpcStub * sql_rpc_stub)
    {
      int ret = OB_SUCCESS;
      if ((NULL == rpc_stub) || (NULL == sql_rpc_stub))
      {
        TBSYS_LOG(WARN, "check schema or tablet cache failed:"
            "rpc[%p], sql_rpc_stub[%p]", rpc_stub, sql_rpc_stub);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else if (true == init_)
      {
        TBSYS_LOG(WARN, "check already inited");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        rpc_stub_ = rpc_stub;
        sql_rpc_stub_ = sql_rpc_stub;
        init_ = true;
      }
      return ret;
    }

    int ObMergerRpcProxy::fetch_update_server_list(int32_t & count)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat())
      {
        TBSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsList list;
        ret = rpc_stub_->fetch_server_list(rpc_timeout_, root_server_, list);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "fetch server list from root server %s failed:ret[%d]",
              to_cstring(root_server_), ret);
        }
        else
        {
          count = list.ups_count_;
          // if has error modify the list
          modify_ups_list(list);
          // check finger print changed
          uint64_t finger_print = ob_crc64(&list, sizeof(list));
          if (finger_print != cur_finger_print_)
          {
            TBSYS_LOG(INFO, "ups list changed succ:cur[%lu], new[%lu], ups_count[%d]",
                cur_finger_print_, finger_print, count);
            list.print();
            update_ups_info(list);
            tbsys::CWLockGuard lock(ups_list_lock_);
            cur_finger_print_ = finger_print;
            memcpy(&update_server_list_, &list, sizeof(update_server_list_));
            // init update server blacklist fail_count threshold, timeout
            ret = black_list_.init(static_cast<int32_t>(fail_count_threshold_), black_list_timeout_,
              MERGE_SERVER, update_server_list_);
            if (ret != OB_SUCCESS)
            {
              // if failed use old blacklist info
              TBSYS_LOG(ERROR, "init black list failed use old blacklist info:ret[%d]", ret);
            }
            else
            {
              ret = ups_black_list_for_merge_.init(static_cast<int32_t>(fail_count_threshold_), black_list_timeout_,
                CHUNK_SERVER, update_server_list_);
              if (ret != OB_SUCCESS)
              {
                // if failed use old blacklist info
                TBSYS_LOG(ERROR, "init black list failed use old blacklist info:ret[%d]", ret);
              }
            }
          }
          else
          {
            TBSYS_LOG(DEBUG, "ups list not changed:finger[%lu], count[%d]", finger_print, list.ups_count_);
          }
        }
      }
      return ret;
    }

    void ObMergerRpcProxy::update_ups_info(const ObUpsList & list)
    {
      for (int64_t i = 0; i < list.ups_count_; ++i)
      {
        if (UPS_MASTER == list.ups_array_[i].stat_)
        {
          update_server_ = list.ups_array_[i].addr_;
          inconsistency_update_server_.set_ipv4_addr(
            update_server_.get_ipv4(), list.ups_array_[i].inner_port_);
          break;
        }
      }
    }

    int ObMergerRpcProxy::get_frozen_time(
        const int64_t frozen_version, int64_t& forzen_time)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat())
      {
        TBSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        ObServer update_server;
        ret = get_update_server(false, update_server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        }
        else
        {
          ret = rpc_stub_->fetch_frozen_time(rpc_timeout_, update_server,
              frozen_version, forzen_time);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fetch frozen time failed:ret[%d]", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "fetch frozen time succ:frozen version[%ld],"
                "frozen time[%ld]",
                frozen_version, forzen_time);
          }
        }
      }
      return ret;
    }

    int ObMergerRpcProxy::get_frozen_schema(
      const int64_t frozen_version, ObSchemaManagerV2& schema)
    {
      int ret = OB_SUCCESS;

      if (!check_inner_stat())
      {
        TBSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        ObServer update_server;
        ret = get_update_server(false, update_server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        }
        else
        {
          ret = rpc_stub_->fetch_schema(rpc_timeout_, update_server,
              frozen_version, false, schema);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "fetch frozen schema failed:ret[%d]", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "fetch frozen schema succ:frozen version[%ld]",
                frozen_version);
          }
        }
      }

      return ret;
    }

    int ObMergerRpcProxy::get_update_server(const bool renew,
                                            ObServer & server,
                                            bool need_master)
    {
      int ret = OB_SUCCESS;
      bool is_master_addr_invalid = false;
      {
        tbsys::CRLockGuard lock(ups_list_lock_);
        is_master_addr_invalid = (0 == update_server_.get_ipv4());
      }
      if (true == renew || is_master_addr_invalid)
      {
        int64_t timestamp = tbsys::CTimeUtil::getTime();
        if (timestamp - fetch_ups_timestamp_ > min_fetch_interval_ || is_master_addr_invalid)
        {
          int32_t server_count = 0;
          tbsys::CThreadGuard lock(&update_lock_);
          if (timestamp - fetch_ups_timestamp_ > min_fetch_interval_ || is_master_addr_invalid)
          {
            TBSYS_LOG(DEBUG, "renew fetch update server list");
            fetch_ups_timestamp_ = tbsys::CTimeUtil::getTime();
            // renew the udpate server list
            ret = fetch_update_server_list(server_count);
            if (ret != OB_SUCCESS)
            {
              TBSYS_LOG(WARN, "fetch update server list failed:ret[%d]", ret);
            }
            else if (server_count == 0)
            {
              TBSYS_LOG(DEBUG, "fetch update server list empty retry fetch vip update server");
              // using old protocol get update server ip
              ret = rpc_stub_->fetch_update_server(rpc_timeout_, root_server_, server);
              if (ret != OB_SUCCESS)
              {
                TBSYS_LOG(WARN, "find update server vip failed:ret[%d]", ret);
              }
              else
              {
                tbsys::CWLockGuard lock(ups_list_lock_);
                update_server_ = server;
              }

              if (OB_SUCCESS == ret)
              {
                // using old protocol get update server ip for daily merge
                ret = rpc_stub_->fetch_update_server(rpc_timeout_, root_server_,
                    server, true);
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(WARN, "find update server vip for daily merge failed:ret[%d]", ret);
                }
                else
                {
                  tbsys::CWLockGuard lock(ups_list_lock_);
                  inconsistency_update_server_ = server;
                }
              }
            }
          }
          else
          {
            TBSYS_LOG(DEBUG, "fetch update server list by other thread");
          }
        }
      }
      // renew master update server addr
      tbsys::CWLockGuard lock(ups_list_lock_);
      server = need_master ? update_server_ : inconsistency_update_server_;
      return ret;
    }

    bool ObMergerRpcProxy::check_range_param(const ObNewRange & range_param)
    {
      bool bret = true;
      if (((!range_param.start_key_.is_min_row()) && (0 == range_param.start_key_.length()))
          || (!range_param.end_key_.is_max_row() && (0 == range_param.end_key_.length())))
      {
        TBSYS_LOG(WARN, "check range param failed");
        bret = false;
      }
      return bret;
    }

    bool ObMergerRpcProxy::check_scan_param(const ObScanParam & scan_param)
    {
      bool bret = true;
      const ObNewRange * range = scan_param.get_range();
      // the min/max value length is zero
      if (NULL == range)// || (0 == range->start_key_.length()))
      {
        TBSYS_LOG(WARN, "check scan range failed");
        bret = false;
      }
      else
      {
        bret = check_range_param(*range);
      }
      return bret;
    }

    // must be in one chunk server
    int ObMergerRpcProxy::cs_get(
        const ObGetParam & get_param,
        ObScanner & scanner,  ObIterator *it_out[],int64_t& it_size)
    {
      UNUSED(get_param);
      UNUSED(scanner);
      UNUSED(it_out);
      UNUSED(it_size);
      TBSYS_LOG(WARN, "not implement");
      return OB_ERROR;
    }

    // only one communication with chunk server
    int ObMergerRpcProxy::cs_scan(
        const ObScanParam & scan_param,
        ObScanner & scanner, ObIterator *it_out[],int64_t& it_size)
    {
      UNUSED(scan_param);
      UNUSED(scanner);
      UNUSED(it_out);
      UNUSED(it_size);
      TBSYS_LOG(WARN, "not implement");
      return OB_ERROR;
    }

    void ObMergerRpcProxy::modify_ups_list(ObUpsList & list)
    {
      if (0 == list.ups_count_)
      {
        // add vip update server to list
        TBSYS_LOG(DEBUG, "check ups count is zero:count[%d]", list.ups_count_);
        ObUpsInfo info;
        info.addr_ = update_server_;
        // set inner port to update server port
        info.inner_port_ = inconsistency_update_server_.get_port();
        info.ms_read_percentage_ = 100;
        info.cs_read_percentage_ = 100;
        list.ups_count_ = 1;
        list.ups_array_[0] = info;
        list.sum_ms_percentage_ = 100;
        list.sum_cs_percentage_ = 100;
      }
      else
      {
        TBSYS_LOG(DEBUG, "reset the percentage for all servers");
        if (list.get_sum_percentage(MERGE_SERVER) <= 0)
        {
          for (int32_t i = 0; i < list.ups_count_; ++i)
          {
            // reset all ms to equal
            list.ups_array_[i].ms_read_percentage_ = 1;
          }
          // reset all ms sum percentage to count
          list.sum_ms_percentage_ = list.ups_count_;
        }
        if (list.get_sum_percentage(CHUNK_SERVER) <= 0)
        {
          for (int32_t i = 0; i < list.ups_count_; ++i)
          {
            // reset all cs to equal
            list.ups_array_[i].cs_read_percentage_ = 1;
          }
          // reset all cs sum percentage to count
          list.sum_cs_percentage_ = list.ups_count_;
        }
      }
    }

    template<class T, class RpcT>
    int ObMergerRpcProxy::master_ups_get(RpcT *rpc_stub, const ObGetParam & get_param, T & scanner,
        const int64_t time_out)
    {
      int ret = OB_ERROR;
      ObServer update_server;
      for (int64_t i = 0; i <= rpc_retry_times_; ++i)
      {
        ret = get_update_server((OB_NOT_MASTER == ret), update_server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
          break;
        }
        ret = rpc_stub->get((time_out > 0) ? time_out : rpc_timeout_, update_server, get_param, scanner);
        if (OB_INVALID_START_VERSION == ret)
        {
          OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
          TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
          break;
        }
        else if (OB_NOT_MASTER == ret)
        {
          TBSYS_LOG(WARN, "get from update server check role failed:ret[%d]", ret);
        }
        else if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get from update server failed:ret[%d]", ret);
        }
        else
        {
          break;
        }
        usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
      }
      return ret;
    }

    template<class T, class RpcT>
    int ObMergerRpcProxy::slave_ups_get(RpcT *rpc_stub, const ObGetParam & get_param,
      T & scanner, const ObServerType server_type, const int64_t time_out)
    {
      int ret = OB_SUCCESS;
      int32_t retry_times = 0;
      int32_t cur_index = -1;
      int32_t max_count = 0;

      //LOCK BLOCK
      {
        tbsys::CRLockGuard lock(ups_list_lock_);
        int32_t server_count = max_count = update_server_list_.ups_count_;
        if (0 == server_count)
        {
          TBSYS_LOG(INFO, "no ups right now local, updating...");
          if (OB_SUCCESS != (ret = fetch_update_server_list(server_count)))
          {
            TBSYS_LOG(WARN, "fetch update server list fail, ret: [%d]", ret);
          }
          else if (0 == server_count)
          {
            ret = OB_UPS_NOT_EXIST;
            TBSYS_LOG(WARN, "no update server available right now, ret: [%d]", ret);
          }
          else
          {
            TBSYS_LOG(INFO, "update local ups list"
                      " info successfully! Got [%d] ups.", server_count);
          }
        }
        if (OB_SUCCESS == ret)
        {
          max_count = server_count;
          cur_index = ObReadUpsBalance::select_server(update_server_list_, server_type);
          if (cur_index < 0)
          {
            TBSYS_LOG(WARN, "select server failed:count[%d], index[%d]", server_count, cur_index);
            ret = OB_ENTRY_NOT_EXIST;
          }
          else
          {
            ObUpsBlackList& black_list =
              (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
            // bring back to alive no need write lock
            if (black_list.get_valid_count() <= 0)
            {
              TBSYS_LOG(WARN, "check all the update server not invalid:count[%d]",
                        black_list.get_valid_count());
              black_list.reset();
            }
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = OB_ENTRY_NOT_EXIST;
        ObServer update_server;
        for (int32_t i = cur_index; retry_times < max_count; ++i, ++retry_times)
        {
          //LOCK BLOCK
          {
            tbsys::CRLockGuard lock(ups_list_lock_);
            int32_t server_count = update_server_list_.ups_count_;
            if (false == check_server(i % server_count, server_type))
            {
              TBSYS_LOG(WARN, "check update server failed:index[%d]", i%server_count);
              continue;
            }
            update_server = update_server_list_.ups_array_[i%server_count].get_server(server_type);
          }
          TBSYS_LOG(DEBUG, "select slave update server for get:index[%d], ip[%d], port[%d]",
              i, update_server.get_ipv4(), update_server.get_port());
          ret = rpc_stub->get((time_out > 0) ? time_out : rpc_timeout_, update_server, get_param, scanner);
          if (OB_INVALID_START_VERSION == ret)
          {
            OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
            TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
          }
          else if (ret != OB_SUCCESS)
          {
            // inc update server fail counter for blacklist
            //LOCK BLOCK
            {
              tbsys::CRLockGuard lock(ups_list_lock_);
              int32_t server_count = update_server_list_.ups_count_;
              ObUpsBlackList& black_list =
                (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
              black_list.fail(i%server_count, update_server);
              TBSYS_LOG(WARN, "get from update server failed:ip[%d], port[%d], ret[%d]",
                  update_server.get_ipv4(), update_server.get_port(), ret);
            }
          }
          else
          {
            break;
          }
        }
      }
      return ret;
    }

    //
    int ObMergerRpcProxy::ups_get(const ObGetParam & get_param,
      ObScanner & scanner, const ObServerType server_type, const int64_t time_out)
    {
      return ups_get_(rpc_stub_, get_param, scanner, server_type, time_out);
    }

    template<class T, class RpcT>
    int ObMergerRpcProxy::ups_get_(RpcT *rpc_stub, const ObGetParam & get_param,
      T & scanner, const ObServerType server_type, const int64_t time_out)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat())
      {
        TBSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else if (NULL == get_param[0])
      {
        TBSYS_LOG(ERROR, "check first cell failed:cell[%p]", get_param[0]);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else if (true == get_param.get_is_read_consistency())
      {
        ret = master_ups_get(rpc_stub, get_param, scanner, time_out);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get from master ups failed:ret[%d]", ret);
        }
      }
      else
      {
        ret = slave_ups_get(rpc_stub, get_param, scanner, server_type, time_out);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "get from slave ups failed:ret[%d]", ret);
        }
      }

      if ((OB_SUCCESS == ret) && (get_param.get_cell_size() > 0) && scanner.is_empty())
      {
        TBSYS_LOG(WARN, "update server error, response request with zero cell");
        ret = OB_ERR_UNEXPECTED;
      }

      return ret;
    }

    template<class T, class RpcT>
    int ObMergerRpcProxy::master_ups_scan(RpcT *rpc_stub, const ObScanParam & scan_param, T & scanner,
        const int64_t time_out)
    {
      int ret = OB_ERROR;
      ObServer update_server;
      for (int64_t i = 0; i <= rpc_retry_times_; ++i)
      {
        ret = get_update_server((OB_NOT_MASTER == ret), update_server);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
          break;
        }
        TBSYS_LOG(DEBUG, "[%ld]scan master ups: %s", i, to_cstring(update_server));
        ret = rpc_stub->scan((time_out > 0) ? time_out : rpc_timeout_, update_server, scan_param, scanner);
        if (OB_INVALID_START_VERSION == ret)
        {
          OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
          TBSYS_LOG(WARN, "check chunk server %s data version failed:ret[%d]", to_cstring(update_server), ret);
          break;
        }
        else if (OB_NOT_MASTER == ret)
        {
          TBSYS_LOG(WARN, "get from update server %s check role failed:ret[%d]", to_cstring(update_server), ret);
        }
        else if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "get from update server %s failed:ret[%d]", to_cstring(update_server), ret);
        }
        else
        {
          break;
        }
        usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
      }
      return ret;
    }

    int ObMergerRpcProxy::set_rpc_param(const int64_t retry_times, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if ((retry_times < 0) || (timeout <= 0))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "check rpc timeout param failed:retry_times[%ld], timeout[%ld]",
          retry_times, timeout);
      }
      else
      {
        rpc_retry_times_ = retry_times;
        rpc_timeout_ = timeout;
      }
      return ret;
    }

    int ObMergerRpcProxy::set_blacklist_param(
        const int64_t timeout, const int64_t fail_count)
    {
      int ret = OB_SUCCESS;
      if ((timeout <= 0) || (fail_count <= 0))
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "check blacklist param failed:timeout[%ld], threshold[%ld]",
          timeout, fail_count);
      }
      else
      {
        fail_count_threshold_ = fail_count;
        black_list_timeout_ = timeout;
      }
      return ret;
    }

    bool ObMergerRpcProxy::check_server(const int32_t index, const ObServerType server_type)
    {
      bool ret = true;
      ObUpsBlackList& black_list =
        (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;

      // check the read percentage and not in black list, if in list timeout make it to be alive
      if ((false == black_list.check(index))
        || (update_server_list_.ups_array_[index].get_read_percentage(server_type) <= 0))
      {
        ret = false;
      }

      return ret;
    }

    template<class T, class RpcT>
    int ObMergerRpcProxy::slave_ups_scan(RpcT *rpc_stub, const ObScanParam & scan_param,
      T & scanner, const ObServerType server_type, const int64_t time_out)
    {
      int ret = OB_SUCCESS;
      int32_t cur_index = -1;
      int32_t max_count = 0;
      int32_t retry_times = 0;

      //LOCK BLOCK
      {
        tbsys::CRLockGuard lock(ups_list_lock_);
        int32_t server_count = max_count = update_server_list_.ups_count_;
        if (0 == server_count)
        {
          TBSYS_LOG(INFO, "no ups right now local, updating...");
          if (OB_SUCCESS != (ret = fetch_update_server_list(server_count)))
          {
            TBSYS_LOG(WARN, "fetch update server list fail, ret: [%d]", ret);
          }
          else if (0 == server_count)
          {
            ret = OB_UPS_NOT_EXIST;
            TBSYS_LOG(WARN, "no update server available right now, ret: [%d]", ret);
          }
          else
          {
            TBSYS_LOG(INFO, "update local ups list"
                      " info successfully! Got [%d] ups.", server_count);
          }
        }
        if (OB_SUCCESS == ret)
        {
          max_count = server_count;
          cur_index = ObReadUpsBalance::select_server(update_server_list_, server_type);
          TBSYS_LOG(DEBUG, "update_server_list_=%s,server_type=%d,index=%d", 
              to_cstring(update_server_list_), server_type, cur_index);
          if (cur_index < 0)
          {
            TBSYS_LOG(WARN, "select server failed:count[%d], index[%d]", server_count, cur_index);
            ret = OB_ENTRY_NOT_EXIST;
          }
          else
          {
            ObUpsBlackList& black_list =
              (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
            // bring back to alive no need write lock
            if (black_list.get_valid_count() <= 0)
            {
              TBSYS_LOG(WARN, "check all the update server not invalid:count[%d]",
                        black_list.get_valid_count());
              black_list.reset();
            }
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = OB_ENTRY_NOT_EXIST;
        ObServer update_server;
        for (int32_t i = cur_index; retry_times < max_count; ++i, ++retry_times)
        {
          //LOCK BLOCK
          {
            tbsys::CRLockGuard lock(ups_list_lock_);
            int32_t server_count = update_server_list_.ups_count_;
            if (false == check_server(i % server_count, server_type))
            {
              TBSYS_LOG(WARN, "check update server failed:index[%d]", i%server_count);
              continue;
            }
            update_server = update_server_list_.ups_array_[i%server_count].get_server(server_type);
          }
          TBSYS_LOG(INFO, "select slave update server for scan:index[%d], ups:[%s]",
              i, to_cstring(update_server));
          ret = rpc_stub->scan((time_out > 0) ? time_out : rpc_timeout_, update_server, scan_param, scanner);
          if (OB_INVALID_START_VERSION == ret)
          {
            OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
            TBSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
          }
          else if (ret != OB_SUCCESS)
          {
            // inc update server fail counter for blacklist
            //LOCK BLOCK
            {
              tbsys::CRLockGuard lock(ups_list_lock_);
              int32_t server_count = update_server_list_.ups_count_;
              ObUpsBlackList& black_list =
                (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
              black_list.fail(i%server_count, update_server);
              TBSYS_LOG(WARN, "get from update server failed:ups_ip[%s], port[%d], ret[%d]",
                  to_cstring(update_server), update_server.get_port(), ret);
            }
          }
          else
          {
            TBSYS_LOG(DEBUG, "%s", "ups get data succ");
            break;
          }
        }
      }
      return ret;
    }

    int ObMergerRpcProxy::ups_scan(const ObScanParam & scan_param,
      ObScanner & scanner, const ObServerType server_type, const int64_t time_out )
    {
      return ups_scan_(rpc_stub_, scan_param, scanner, server_type, time_out);
    }

    template<class T, class RpcT>
    int ObMergerRpcProxy::ups_scan_(RpcT *rpc_stub, const ObScanParam & scan_param,
      T & scanner, const ObServerType server_type, const int64_t time_out )
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat())
      {
        TBSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else if (!check_scan_param(scan_param))
      {
        TBSYS_LOG(ERROR, "%s", "check scan param failed");
        ret = OB_INPUT_PARAM_ERROR;
      }
      else if (true == scan_param.get_is_read_consistency())
      {
        ret = master_ups_scan(rpc_stub, scan_param, scanner, time_out);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "scan from master ups failed:ret[%d]", ret);
        }
      }
      else
      {
        ret = slave_ups_scan(rpc_stub, scan_param, scanner, server_type, time_out);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "scan from slave ups failed:ret[%d]", ret);
        }
      }

      return ret;
    }

    int ObMergerRpcProxy::sql_ups_get(const common::ObGetParam & get_param,
                    common::ObNewScanner & scanner,
                    common::ObServerType type,
                    const int64_t timeout /* = 0 */)
    {
      return ups_get_(sql_rpc_stub_, get_param, scanner, type, timeout);
    }

    int ObMergerRpcProxy::sql_ups_scan(const common::ObScanParam & scan_param,
                     common::ObNewScanner & scanner,
                     common::ObServerType type,
                     const int64_t timeout /* = 0 */)
    {
      return ups_scan_(sql_rpc_stub_, scan_param, scanner, type, timeout);
    }

  } // end namespace chunkserver
} // end namespace oceanbase
