/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_ms_scan_event.h,v 0.1 2011/09/28 14:28:10 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */
#include "common/ob_trace_log.h"
#include "common/ob_schema_manager.h"
#include "common/ob_common_stat.h"
#include "sql/ob_sql_plan_param.h"
#include "ob_ms_sql_scan_request.h"
#include "ob_ms_async_rpc.h"
#include "ob_read_param_modifier.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace mergeserver
  {
    ObMsSqlScanRequest::ObMsSqlScanRequest()
    {
      total_sub_request_count_ = 0;
      finished_sub_request_count_ = 0;
      merger_operator_.reset();
      ObMsSqlRequest::reset();
      tablet_scan_op_ = NULL;
      cs_result_mem_size_used_ = 0;
      sharding_limit_count_ = 0;
      cur_row_cell_cnt_ = 0;
    }

    ObMsSqlScanRequest::~ObMsSqlScanRequest()
    {
      int32_t size = sub_requests_.size();
      for (int32_t i = 0;i < size; ++i)
      {
        if (sub_requests_[i] != NULL)
        {
          sub_requests_[i]->~ObMsSqlSubScanRequest();
          ob_free(sub_requests_[i]);
          sub_requests_[i] = NULL;
        }
      }
    }

    ObMsSqlSubScanRequest * ObMsSqlScanRequest::alloc_sub_scan_request()
    {
      void *ptr = NULL;
      ObMsSqlSubScanRequest *sub_req = NULL;
      if ((total_sub_request_count_ >= 0)
        && (total_sub_request_count_ < MAX_SUBREQUEST_NUM))
      {
        if (total_sub_request_count_ == 0)
        {
          sub_req = sub_requests_[0];
          total_sub_request_count_++;
        }
        else
        {
          int ret = OB_SUCCESS;
          if ( NULL == (ptr = ob_malloc(sizeof(ObMsSqlSubScanRequest))))
          {
            TBSYS_LOG(WARN, "ob malloc failed, ret=%d", OB_ALLOCATE_MEMORY_FAILED);
          }
          else
          {
            sub_req = new (ptr) ObMsSqlSubScanRequest();
            if (OB_SUCCESS != (ret = sub_requests_.push_back(sub_req)))
            {
              TBSYS_LOG(ERROR, "push ObMsSqlSubScanRequest* to sub_requests failed, ret=%d", ret);
              sub_req->~ObMsSqlSubScanRequest();
              ob_free(sub_req);
              sub_req = NULL;
            }
            else
            {
              total_sub_request_count_++;
            }
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "allocate sub scan request failed. [total_sub_request_count_=%d][finished_sub_request_count_=%d]"
          "[MAX_SUBREQUEST_NUM=%d]", total_sub_request_count_, finished_sub_request_count_,
          MAX_SUBREQUEST_NUM);
      }
      return sub_req;
    }

    int ObMsSqlScanRequest::set_request_param(ObSqlPlanParam &plan_param)
    {
      int err = OB_SUCCESS;
      int64_t sharding_limit_offset = 0;
      const ObNewRange &range = plan_param.get_request_scan_param().get_range();
      ObLimit *op_limit = NULL;
      tablet_scan_op_ = plan_param.get_op_tablet_scan();
      OB_ASSERT(NULL !=  tablet_scan_op_);
      OB_ASSERT(plan_param.max_parallel_count_ > 0);
      OB_ASSERT(plan_param.max_memory_limit_ > 0);
      OB_ASSERT(plan_param.initial_timeout_us_ > 0);
      max_parallel_count_ = plan_param.max_parallel_count_;
      max_cs_result_mem_size_ = plan_param.max_memory_limit_;
      timeout_us_ = plan_param.initial_timeout_us_;
      phy_plan_ = &plan_param.inner_plan_;
      plan_param.get_request_scan_param().set_data_version(plan_param.data_version_); // update every get request
      if (NULL != (op_limit = plan_param.get_op_limit()))
      {
        if(OB_SUCCESS != (err = op_limit->get_limit(sharding_limit_count_, sharding_limit_offset)))
        {
          TBSYS_LOG(WARN, "fail to get limit. err=%d", err);
        }
      }
      else
      {
        sharding_limit_count_ = -1; // unlimited
        sharding_limit_offset = 0;
      }
      
      if ((OB_SUCCESS == err) && (sharding_limit_offset > 0))
      {
        TBSYS_LOG(ERROR, "unexpected error, synmatic error [sharding_limit_offset:%ld]", sharding_limit_offset);
      }
      if (range.start_key_ > range.end_key_)
      {
        // in the case of empty range scan
        set_finish(true);
      }
      else if (OB_SUCCESS != (err = merger_operator_.set_param(range)))
      {
        TBSYS_LOG(WARN, "fail to set param.");
      }
      if ((OB_SUCCESS == err) &&(OB_SUCCESS != (err = org_req_range_iter_.initialize(
                get_cache_proxy(),
                &range,
                ScanFlag::FORWARD,
                &get_buffer_pool()))))
      {
        TBSYS_LOG(WARN,"fail to init table range iterator [err:%d]", err);
      }
      return err;
    }

    int ObMsSqlScanRequest::send_rpc_event(ObMsSqlSubScanRequest * sub_req, const int64_t timeout_us,
      uint64_t * triggered_rpc_event_id)
    {
      int err = OB_SUCCESS;
      const ObMergerAsyncRpcStub * async_rpc_stub = NULL;
      ObMsSqlRpcEvent *rpc_event = NULL;
      ObChunkServerItem selected_server;
      if (OB_SUCCESS != (err = create(&rpc_event)))
      {
        TBSYS_LOG(WARN, "fail to create rpc event. [err=%d]", err);
      }
      else if (OB_SUCCESS != (err = sub_req->add_event(rpc_event, this, selected_server)))
      {
        TBSYS_LOG(WARN, "fail to init rpc event. [err=%d]", err);
      }
      else if (NULL == (async_rpc_stub = get_rpc()))
      {
        TBSYS_LOG(ERROR, "fail to get rpc");
        err = OB_ERR_UNEXPECTED;
      }
      else if (NULL == tablet_scan_op_)
      {
        TBSYS_LOG(WARN, "tablet_scan_op_ not init");
        err = OB_NOT_INIT;
      }

      if (OB_SUCCESS == err)
      {
        FILL_TRACE_LOG("cs=%s,event_id=%ld", to_cstring(selected_server.addr_), rpc_event->get_event_id());
      }

      int64_t fast_retry_timeout_us = static_cast<int64_t>(timeout_us * get_timeout_percent() / 100);
      if (OB_SUCCESS == err)
      {
        /// TODO: construct ObPhysicalPlan
        /// put your code here
        if (OB_SUCCESS != (err = tablet_scan_op_->get_scan_param().set_range(sub_req->get_sub_range())))
        {
          TBSYS_LOG(WARN, "fail to set range to scan param. err=%d", err);
        }
        else if (OB_SUCCESS != (err = async_rpc_stub->scan(
                fast_retry_timeout_us,
                selected_server.addr_,
                /* @note range and offset set in get_scan_param() function */
                *phy_plan_,
                *rpc_event)))
        {
          TBSYS_LOG(WARN, "fail to scan cs %s. event id %ld [err=%d]", 
              to_cstring(selected_server.addr_), rpc_event->get_event_id(), err);
          /// @Exception  scan failed, no failure packet would return to the framework
          ///             so, we need to release rpc_event manually here
          int tmp_err = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_err = ObMsSqlRequest::destroy(rpc_event)))
          {
            TBSYS_LOG(WARN, "fail to destroy rpc_event. [err=%d]", tmp_err);
          }
          else
          {
            TBSYS_LOG(INFO, "rpc_event destroyed.");
          }
        }
        // TBSYS_LOG(INFO, "Plan=%s", to_cstring(*phy_plan_));
      }
      if ((OB_SUCCESS == err) && (NULL != triggered_rpc_event_id))
      {
        *triggered_rpc_event_id = rpc_event->get_event_id();
      }
      return err;
    }


    int ObMsSqlScanRequest::get_session_next(const int32_t sub_req_idx, const ObMsSqlRpcEvent &prev_rpc_event,
        ObNewRange &query_range, const int64_t timeout_us,  const int64_t limit_offset)
    {
      int err = OB_SUCCESS;
      const ObMergerAsyncRpcStub * async_rpc_stub = NULL;
      ObMsSqlRpcEvent *new_rpc_event = NULL;
      ObMsSqlSubScanRequest *sub_req = NULL;
      ObChunkServerItem only_replica;
      only_replica.addr_ = prev_rpc_event.get_server();
      int64_t limit_count = sharding_limit_count_;
      if ((limit_offset > 0) && (limit_count <= 0))
      {
        limit_count = ObMergerSchemaManager::MAX_INT64_VALUE;
      }
      if ((OB_SUCCESS == err) && (NULL == (sub_req = alloc_sub_scan_request())))
      {
        TBSYS_LOG(WARN, "fail to allocate sub scan request");
        err = OB_ERROR;
      }
      if ((OB_SUCCESS == err)
          && (OB_SUCCESS != (err = sub_req->init(
                query_range,
                limit_offset,
                limit_count,
                &only_replica,
                1,
                false,
                &get_buffer_pool()))))
      {
        TBSYS_LOG(WARN, "fail to init SubScanRequest. [err=%d]", err);
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = create(&new_rpc_event))))
      {
        TBSYS_LOG(WARN, "fail to create rpc event. [err=%d]", err);
      }
      else if (OB_SUCCESS == err)
      {
        new_rpc_event->set_session_id(prev_rpc_event.get_session_id());
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = sub_req->add_event(
        new_rpc_event, this, only_replica))))
      {
        TBSYS_LOG(WARN, "fail to init rpc event. [err=%d]", err);
      }
      if ((OB_SUCCESS == err) && (NULL == (async_rpc_stub = get_rpc())))
      {
        TBSYS_LOG(WARN, "fail to get rpc");
        err = OB_ERROR;
      }
      if (OB_SUCCESS == err)
      {
        FILL_TRACE_LOG("snext_cs=%s,sid=%ld", to_cstring(only_replica.addr_), prev_rpc_event.get_session_id());
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = async_rpc_stub->get_session_next(timeout_us,
        only_replica.addr_, prev_rpc_event.get_session_id(), prev_rpc_event.get_req_type(), *new_rpc_event))))
      {
        TBSYS_LOG(WARN,"fail to get session next from %s [err:%d,session_id:%ld,req_id:%lu]", 
            to_cstring(only_replica.addr_), err, prev_rpc_event.get_session_id(),get_request_id());
      }
      if (OB_SUCCESS == err)
      {
        int64_t replica_count = ObTabletLocationList::MAX_REPLICA_COUNT;
        ObChunkServerItem     req_cs_replicas[replica_count];
        if (OB_UNLIKELY(sub_requests_[sub_req_idx] == NULL))
        {
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "sub request is null, sub_req_idx=%d, ret=%d", sub_req_idx, err);
        }
        else
        {
          sub_requests_[sub_req_idx]->get_cs_replicas(req_cs_replicas, replica_count);
          for (int64_t i = 0; i < replica_count; i++)
          {
            if (only_replica.addr_ == req_cs_replicas[i].addr_)
            {
              req_cs_replicas[i] = req_cs_replicas[replica_count - 1];
              replica_count --;
              break;
            }
          }
          if (replica_count > 0)
          {
            sub_req->reset_cs_replicas(static_cast<int32_t>(replica_count), req_cs_replicas);
          }
        }
      }
      TBSYS_LOG(DEBUG, "[session next] timeout_us:%ld, limit_offset:%ld, limit_count:%ld, "
        "total_rpc_event_count:%d, session_id:%ld, reqest_id:%lu,"
        "prev_event_id:%lu,cur_event_id:%lu,cs:%s]",  timeout_us, limit_offset, limit_count,
        total_sub_request_count_, prev_rpc_event.get_session_id(), get_request_id(),
        prev_rpc_event.get_event_id(), (new_rpc_event ? (new_rpc_event->get_event_id()):0),
        to_cstring(only_replica.addr_));
      return err;
    }

    int ObMsSqlScanRequest::do_request(
        const int64_t max_parallel_count,
        ObTabletLocationRangeIterator &range_iter,
        const int64_t timeout_us,
        const int64_t limit_offset)
    {
      int err = OB_SUCCESS;
      ObNewRange query_range;
      ObMsSqlSubScanRequest *sub_req = NULL;
      int64_t limit_count = sharding_limit_count_;
      if ((limit_offset > 0) && (limit_count <= 0))
      {
        limit_count = ObMergerSchemaManager::MAX_INT64_VALUE;
      }

      int32_t replica_count = ObTabletLocationList::MAX_REPLICA_COUNT;
      ObChunkServerItem replicas[ObTabletLocationList::MAX_REPLICA_COUNT];
      int32_t triggered_rpc_event_count = 0;

      int64_t cur_limit_offset = limit_offset;
      int64_t cur_limit_count = limit_count;

      TBSYS_LOG(DEBUG, "[do_request begin] timeout_us:%ld, limit_offset:%ld, limit_count:%ld.",
        timeout_us, limit_offset, limit_count);

      while (OB_SUCCESS == err)
      {
        if (triggered_rpc_event_count > 0)
        {
          cur_limit_offset = 0;
          /// cur_limit_count = 0;
        }

        /// 1. split requested ObNewRange into small ranges
        err = range_iter.next(reinterpret_cast<ObChunkServerItem*>(replicas), replica_count, query_range);
        if (OB_ITER_END == err)
        {
          //TBSYS_LOG(DEBUG, "end of range iteration. break");
          err = OB_SUCCESS;
          break;
        }
        else if (OB_SUCCESS != err)
        {
          TBSYS_LOG(WARN, "fail to get tablet locations.");
        }

        /// 2. allocate a sub scan request
        if (OB_SUCCESS == err)
        {
          if (NULL == (sub_req = alloc_sub_scan_request()))
          {
            TBSYS_LOG(WARN, "fail to allocate sub scan request");
            err = OB_ERROR;
          }
        }

        /// 3. init the sub scan request
        if (OB_SUCCESS == err) 
        {
          if (OB_SUCCESS != (err = sub_req->init(
                  query_range,
                  cur_limit_offset,
                  cur_limit_count, 
                  reinterpret_cast<ObChunkServerItem*>(replicas),
                  replica_count,
                  false,
                  &get_buffer_pool())))
          {
            TBSYS_LOG(WARN, "fail to init SubScanRequest. [err=%d]", err);
          }
        }
        /// 4. send request to cs
        if (OB_SUCCESS == err)
        {
          TBSYS_LOG(DEBUG, "sub query range info: %s", to_cstring(query_range));
          if (OB_SUCCESS != (err = send_rpc_event(sub_req, timeout_us)))
          {
            TBSYS_LOG(WARN,"fail to send rpc event of ObMsSqlSubScanRequest [err:%d]", err);
          }
          else
          {
            triggered_rpc_event_count ++;
            // TBSYS_LOG(DEBUG, "success in sending a async scan request to selected server");
          }
        }

        /// 5. check if need more request
        if ((max_parallel_count > 0)
          && (total_sub_request_count_ - finished_sub_request_count_ >= max_parallel_count))
        {
          break;
        }
      }

      TBSYS_LOG(DEBUG, "[do_request end] timeout_us:%ld, limit_offset:%ld, limit_count:%ld, triggered_rpc_event_count:%d,"
        "total_rpc_event_count:%d", timeout_us, limit_offset, limit_count, triggered_rpc_event_count, total_sub_request_count_);
      return err;
    }


    int ObMsSqlScanRequest::find_sub_scan_request(ObMsSqlRpcEvent * rpc_event,
      bool &belong_to_this, bool &is_first,  int32_t &idx)
    {
      int err = OB_SUCCESS;
      belong_to_this = false;
      is_first = false;
      for (idx = 0; idx < total_sub_request_count_; idx++)
      {
        if (OB_UNLIKELY(sub_requests_[idx] == NULL))
        {
          err = OB_ERR_UNEXPECTED;
          TBSYS_LOG(WARN, "sub request is null, idx = %d, ret=%d", idx, err);
          break;
        }
        else
        {
          if (OB_SUCCESS != (err = sub_requests_[idx]->agent_event_finish(rpc_event, belong_to_this, is_first)))
          {
            TBSYS_LOG(WARN, "fail to determine if agent event finished. [err=%d]", err);
            break;
          }
          /// event not belong to the i-th subrequest, go on seraching
          else if (false == belong_to_this)
          {
            continue;
          }
          else if (false == is_first)
          {
            TBSYS_LOG(INFO, "not the first result of sub_scan[%d]. ignore.", idx);
            break;
          }
          /// Must be: (OB_SUCCESS == err, true == is_first, true == belong_to_this)
          break;
        }
      }
      if (!belong_to_this)
      {
        idx = -1;
      }
      return err;
    }




    int ObMsSqlScanRequest::check_if_need_more_req(const int32_t sub_req_idx,  const int64_t timeout_us, ObMsSqlRpcEvent &prev_rpc_event, bool &is_session_end)
    {
      int err = OB_SUCCESS;
      ObNewScanner *scanner = NULL;
      ObNewRange next_scan_range;
      int64_t next_limit_offset = 0;
      if (OB_UNLIKELY(sub_requests_[sub_req_idx] == NULL))
      {
        err = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "sub request is null, sub_req_idx=%d, ret=%d", sub_req_idx, err);
      }
      else
      {
        if ((NULL == (scanner = sub_requests_[sub_req_idx]->get_scanner())))
        {
          TBSYS_LOG(WARN, "scanner is NULL. no result.");
          err = OB_ERROR;
        }
        bool is_req_fullfilled = false;
        int64_t fullfilled_count = 0;
        is_session_end = false;
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scanner->get_is_req_fullfilled(is_req_fullfilled,fullfilled_count))))
        {
          TBSYS_LOG(WARN,"fail to get fullfilled info from scanner [err:%d]", err);
        }

        /// (a)
        /// check if sub-request fullfilled
        /// if not , create more sub request, old scan range is updated too
        if (OB_SUCCESS == err)
        {
          if (OB_ITER_END == (err = get_next_range(
            sub_requests_[sub_req_idx]->get_sub_range(),
            *scanner,
            sub_requests_[sub_req_idx]->get_limit_offset(),
            next_scan_range,
            next_limit_offset,
            get_buffer_pool()
            )))
          {
            // input scan range fully scanned(matched with range of %scanner)
            // and request fulfilled(all data returned), so this sub request complete
            // no further data need to fetch.
            is_session_end = true;
            TBSYS_LOG(DEBUG, "sub request fullfilled.[next_limit_offset=%ld]", next_limit_offset);
          } 
          else if (OB_SUCCESS != err)
          {
            TBSYS_LOG(WARN, "fail to get next range, err=%d", err);
          }
          else
          {
            if (is_req_fullfilled)
            {
              // input scan range not fully scanned, that means input scan range splited on
              // chunkserver before issue scan request. send new sub request for new split range.
              ObTabletLocationRangeIterator range_iter;
              if (OB_SUCCESS != (err = range_iter.initialize(
                      get_cache_proxy(),
                      &next_scan_range,
                      ScanFlag::FORWARD, 
                      &get_buffer_pool())))
              {
                TBSYS_LOG(WARN,"fail to initialize range iterator [err:%d]", err);
              }
              else if (OB_SUCCESS != (err = do_request(-1, range_iter,  timeout_us, next_limit_offset)))
              {
                TBSYS_LOG(WARN, "fail to issue new scan request [err:%d]", err);
              }
            }
            else if (prev_rpc_event.is_session_end())
            {
              // prev request not fullfilled(data too big to transfer); but session end.
              TBSYS_LOG(INFO, "prev_rpc_event return OB_SESSION_END, end session, no need more request.");
              is_session_end = true;
              err = OB_ITER_END;
            }
            else if (OB_SUCCESS != (err = get_session_next(
                    sub_req_idx,
                    prev_rpc_event,
                    next_scan_range,
                    timeout_us,
                    next_limit_offset)))
            {
              // prev request not fullfilled(data too big to transfer); use stream request.
              TBSYS_LOG(WARN, "fail to get session next [err:%d]", err);
            }
          }

          if (OB_ITER_END == err)
          {
            err = OB_SUCCESS; // nust reset err code
          }
        }
      }
      return err;
    }


    bool ObMsSqlScanRequest::check_if_location_cache_valid_(const ObNewScanner & scanner, const ObNewRange & scan_range)
    {
      int err = OB_SUCCESS;
      bool res = true;
      ObNewRange tablet_range;
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = scanner.get_range(tablet_range))))
      {
        TBSYS_LOG(WARN,"fail to get tablet range from scanner [err:%d]", err);
      }
      TBSYS_LOG(DEBUG, "scan{ start: [%s], end: [%s], bf: [%d] }",
                to_cstring(tablet_range.start_key_),
                to_cstring(tablet_range.end_key_), tablet_range.border_flag_.get_data());
      TBSYS_LOG(DEBUG, "param{ start: [%s], end: [%s], bf: [%d] }",
                to_cstring(scan_range.start_key_),
                to_cstring(scan_range.end_key_),
                scan_range.border_flag_.get_data());
      if ((OB_SUCCESS == err)
        && !(scan_range.start_key_.is_min_row())
        && !(tablet_range.end_key_.is_max_row()))
      {
        if (scan_range.border_flag_.inclusive_start() && (scan_range.start_key_ > tablet_range.end_key_))
        {
          res = false;
        }
        else if (!scan_range.border_flag_.inclusive_start()
          && (scan_range.start_key_ >= tablet_range.end_key_))
        {
          res = false;
        }
      }
      if ((OB_SUCCESS == err)
          && res
        && (!tablet_range.start_key_.is_min_row()))
      {
        if (scan_range.start_key_.is_min_row())
        {
          res = false;
        }
        else if (scan_range.start_key_ < tablet_range.start_key_)
        {
          res = false;
        }
        else if (!tablet_range.border_flag_.inclusive_start()
                 && (scan_range.start_key_ == tablet_range.start_key_)
                 && (scan_range.border_flag_.inclusive_start()))
        {
          res = false;
        }
      }

      if (OB_SUCCESS != err)
      {
        res = false;
      }
      return res;
    }
    
    
    /* @return
     *   OB_SUCCESS: normally successful
     *   OB_ITER_END: fail to retry. no more replica to retry
     */
    int ObMsSqlScanRequest::process_result(const int64_t timeout_us,
      ObMsSqlRpcEvent *rpc_event, bool& finish)
    {
      int32_t sub_req_idx = 0;
      int err = OB_SUCCESS;
      bool belong_to_this = false;
      bool is_first = false;
      ObNewRange next_scan_range;
      ObNewScanner *scanner = NULL;
      ObChunkServerItem selected_server;
      bool can_free_res  = false;
      bool is_session_end = false;
      timeout_us_ = std::min(timeout_us_,timeout_us); // get_next_row() depends on this update
      if (NULL != rpc_event)
      {
        int64_t scan_event_time_cost = tbsys::CTimeUtil::getTime() - rpc_event->get_timestamp();
        OB_STAT_INC(MERGESERVER, SQL_SCAN_EVENT_TIME,  scan_event_time_cost);
        OB_STAT_INC(MERGESERVER, SQL_SCAN_EVENT_COUNT);

        TBSYS_LOG(DEBUG, "rpc_event finised [us_used:%ld,timeout_us:%ld,request_event_id:%lu,rpc_event_id:%lu,"
            "rpc_event_client_id:%lu, server:%s,session_id:%lu, err:%d]",
            rpc_event->get_time_used(),rpc_event->get_timeout_us(), get_request_id(),
            rpc_event->get_event_id(), rpc_event->get_client_id(), to_cstring(rpc_event->get_server()), rpc_event->get_session_id(),
            rpc_event->get_result_code());
      }

      /// param checking before start
      if (true == finish)
      {
        TBSYS_LOG(ERROR, "request finished already. should not call process_result() anymore!");
        err = OB_ERR_UNEXPECTED;
      }
      else if (NULL == rpc_event)
      {
        TBSYS_LOG(WARN, "NULL pointer error. [rpc_event=%p]", rpc_event);
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        /// just to calculate how much time will be used by ms
        FILL_TRACE_LOG("");
      }

      //TBSYS_LOG(DEBUG, "[process result begin]");
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = find_sub_scan_request(rpc_event,
        belong_to_this, is_first,  sub_req_idx))))
      {
        TBSYS_LOG(WARN,"fail to find SubRequest for rpc event [rpc_event:%p,rpc_event_id:%ld,"
          "rpc_event->client_id:%lu,this->event_id:%lu]",  rpc_event, rpc_event->get_event_id(),
          rpc_event->get_client_id(), this->get_request_id());
      }

      if ((OB_SUCCESS == err) && (OB_SUCCESS == rpc_event->get_result_code()))
      {
        if ((OB_SUCCESS == err) && (false == belong_to_this))
        {
          TBSYS_LOG(WARN, "Unexpected. rpc event not found! The framework should deal with this case. not here");
          err = OB_ERR_UNEXPECTED;
        }
        else
        {
          sub_requests_[sub_req_idx]->reset_session();
        }

        if ((OB_SUCCESS == err)
          && (!check_if_location_cache_valid_(rpc_event->get_result(), sub_requests_[sub_req_idx]->get_sub_range())))
        {
          rpc_event->set_result_code(OB_DATA_NOT_SERVE);
          TBSYS_LOG(WARN,"location cache invalid");
        }
        if ((OB_SUCCESS == err) && (true == belong_to_this) && (true == is_first)
            && (OB_SUCCESS == rpc_event->get_result_code()))
        {
          TBSYS_LOG(DEBUG, "got first finished rpc event of [sub_request:%d,us_used:%ld,"
              "request_event_id:%lu,rpc_event_id:%lu,session_id:%ld,server:%s,#rows:%ld]",
              sub_req_idx,
              rpc_event->get_time_used(),
              get_request_id(),
              rpc_event->get_event_id(),
              rpc_event->get_session_id(),
              to_cstring(rpc_event->get_server()),
              rpc_event->get_result().get_row_num());

          cs_result_mem_size_used_ += rpc_event->get_result().get_size();
          if ((OB_SUCCESS == err) && (NULL == (scanner = sub_requests_[sub_req_idx]->get_scanner())))
          {
            TBSYS_LOG(WARN, "scanner is NULL. no result.");
            err = OB_ERROR;
          }
          if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = check_if_need_more_req(
                    sub_req_idx, 
                    timeout_us,
                    *rpc_event, 
                    is_session_end))))
          {
            TBSYS_LOG(WARN,"fail to check if need any more request for subreq [err:%d,sub_req_idx:%d]",
              err, sub_req_idx);
          }

          /// (d) add result to merger operator
          if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = merger_operator_.add_sharding_result(
            *scanner,
            sub_requests_[sub_req_idx]->get_sub_range(),
            finish, 
            get_buffer_pool()))))
          {
            TBSYS_LOG(WARN, "fail to add sharding result. [err=%d]", err);
          }

          /// dump all result that received be send to ob client
          if (OB_SUCCESS == err && NULL != scanner && TBSYS_LOGGER._level >= TBSYS_LOG_LEVEL_DEBUG)
          {
            TBSYS_LOG(DEBUG, "[dump] scanner data that received from chunkserver");
            scanner->dump_all(TBSYS_LOGGER._level);
          }

          if (OB_SUCCESS == err)
          {
            finished_sub_request_count_++;

            // debug purpose
            bool is_fullfill = false;
            ObRowkey last_rowkey;
            rpc_event->get_result().get_last_row_key(last_rowkey);
            ObNewRange scanner_range;
            rpc_event->get_result().get_range(scanner_range);
            int64_t fullfill_num = 0;
            rpc_event->get_result().get_is_req_fullfilled(is_fullfill, fullfill_num);
            FILL_TRACE_LOG("add cs result[#rows:%ld,"
                "sub_req_count_:%d,server:%s,fullfill:%d,fullfill_num:%ld,"
                "last_rowkey:%s, scanner range:%s, local scan_param:%s]",
                rpc_event->get_result().get_row_num(),finished_sub_request_count_,
                to_cstring(rpc_event->get_server()), is_fullfill, fullfill_num, to_cstring(last_rowkey), to_cstring(scanner_range),
                to_cstring(sub_requests_[sub_req_idx]->get_sub_range()));
          }
        }
      }
      if ((OB_SUCCESS == err) && (OB_SUCCESS != rpc_event->get_result_code()) && belong_to_this)
      {
        TBSYS_LOG(WARN, "rpc event return code not success. [err=%d,event_id:%lu]", 
            rpc_event->get_result_code(),
            rpc_event->get_event_id());

        if (OB_SUCCESS != update_location_cache(
              rpc_event->get_server(),
              rpc_event->get_result_code(), 
              sub_requests_[sub_req_idx]->get_sub_range()))
        {
          TBSYS_LOG(WARN,"fail to update location cache");
        }

        if ((OB_SUCCESS == err) && is_first)
        {
          if (OB_SUCCESS != (err = retry(sub_req_idx, rpc_event, timeout_us)))
          {
            TBSYS_LOG(WARN, "this retry not success");
          }
          if (OB_ITER_END == err)
          {
            TBSYS_LOG(WARN, "tried all replica and not successful.");
            err = rpc_event->get_result_code();
          }
        }
      }

      /// valid check
      if (OB_SUCCESS == err)
      {
        TBSYS_LOG(DEBUG, "finished_sub_request_count_ =%d, total_sub_request_count_=%d",
          finished_sub_request_count_ , total_sub_request_count_);
        if (finished_sub_request_count_ > total_sub_request_count_)
        {
          TBSYS_LOG(ERROR, "unexpected sub request finish count. [finished=%d][total=%d]",
            finished_sub_request_count_, total_sub_request_count_);
          err = OB_ERR_UNEXPECTED;
        }
      }

      if (can_free_res && (NULL != scanner))
      {
        scanner->clear();
      }
      if ((OB_SUCCESS != err) || (finish))
      {
        end_sessions_();
      }
      //TBSYS_LOG(DEBUG, "[process result end, finish=%d]", finish);
      return err;
    }


    void ObMsSqlScanRequest::close()
    {
      end_sessions_();
      ObMsSqlRequest::close();
      reset();
    }

    void ObMsSqlScanRequest::end_sessions_()
    {
      for (int32_t i = 0; (i < total_sub_request_count_); i++)
      {
        if (sub_requests_[i]->get_session_id() > ObCommonSqlRpcEvent::INVALID_SESSION_ID)
        {
          if (OB_SUCCESS == terminate_remote_session(sub_requests_[i]->get_session_server(), sub_requests_[i]->get_session_id()))
          {
            TBSYS_LOG(INFO,"end unfinished session [scan_event:%lu,idx:%d,session_id:%lu]", get_request_id(),
              i, sub_requests_[i]->get_session_id());
            sub_requests_[i]->reset_session();
          }
        }
      }
    }

    int ObMsSqlScanRequest::retry(const int32_t sub_req_idx, ObMsSqlRpcEvent *rpc_event, int64_t timeout_us)
    {
      int err = OB_SUCCESS;

      if (NULL == rpc_event)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "null pointer error. [rpc_event=%p][err=%d]", rpc_event, err);
      }

      // see Bug #216902
      if (OB_CS_TABLET_NOT_EXIST == rpc_event->get_result_code())
      {
        sub_requests_[sub_req_idx]->set_tablet_migrate();
      }
      if (sub_requests_[sub_req_idx]->has_tablet_migrate()
          && (sub_requests_[sub_req_idx]->tried_replica_count() >= sub_requests_[sub_req_idx]->total_replica_count()))
      {
        rpc_event->set_result_code(OB_DATA_NOT_SERVE);
      }
      /// update cs replicas if neccery
      if ((OB_SUCCESS == err) && (OB_DATA_NOT_SERVE == rpc_event->get_result_code()))
      {
        ObTabletLocationRangeIterator range_iter;
        ObNewRange query_range;
        int32_t replica_count = ObTabletLocationList::MAX_REPLICA_COUNT;
        ObChunkServerItem replicas[ObTabletLocationList::MAX_REPLICA_COUNT];
        if ((OB_SUCCESS == err)
          && (OB_SUCCESS != (err = range_iter.initialize(
                get_cache_proxy(),
                &sub_requests_[sub_req_idx]->get_sub_range(),
                ScanFlag::FORWARD,
                &get_buffer_pool()))))
        {
          TBSYS_LOG(WARN,"fail to initialize range iterator [err:%d]", err);
        }

        if ((OB_SUCCESS == err)
          && (OB_SUCCESS != (err = range_iter.next(replicas,replica_count,query_range)))
          && (OB_ITER_END != err))
        {
          TBSYS_LOG(WARN,"fail to find replicas [err:%d]", err);
        }

        if (OB_ITER_END == err)
        {
          TBSYS_LOG(WARN,"fail to find replicas, while retry [err:%d]", err);
          err = OB_ERR_UNEXPECTED;
        }

        if ((OB_SUCCESS == err) && (replica_count <= 0))
        {
          TBSYS_LOG(WARN,"fail to find replicas, while retry [err:%d,replica_count:%d]", err, replica_count);
          err = OB_DATA_NOT_SERVE;
        }

        if ((OB_SUCCESS == err)
          && (OB_SUCCESS != (err = sub_requests_[sub_req_idx]->reset_cs_replicas(replica_count, replicas))))
        {
          TBSYS_LOG(WARN,"fail to set sub request's cs replicas [err:%d]", err);;
        }
      }

      if (OB_SUCCESS == err)
      {
        /// @note: only if tried_replica_count < total_replica_count we would retry
        /// to prevent retry storm, set max retry limit regardless reset_cs_replicas,
        /// max_retry_limit = 3 times of total_replica_count
        if (sub_requests_[sub_req_idx]->tried_replica_count() >= sub_requests_[sub_req_idx]->total_replica_count() ||
            sub_requests_[sub_req_idx]->total_tried_replica_count() >= sub_requests_[sub_req_idx]->total_replica_count() * 3)
        {
          TBSYS_LOG(WARN, "exceeds retry times. [total=%d][tried_replica_count=%d][total_replica_count=%d]",
              sub_requests_[sub_req_idx]->total_tried_replica_count(),
              sub_requests_[sub_req_idx]->tried_replica_count(),
              sub_requests_[sub_req_idx]->total_replica_count());
          err = rpc_event->get_result_code();
        }
      }

      if (OB_SUCCESS == err)
      {
        uint64_t retry_event_id = 0;
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = send_rpc_event(sub_requests_[sub_req_idx], timeout_us, &retry_event_id))))
        {
          TBSYS_LOG(WARN,"fail to resend rpc event of ObMsSqlSubScanRequest [idx:%d,err:%d]", sub_req_idx, err);
        }
        else if (OB_SUCCESS == err)
        {
          TBSYS_LOG(INFO, "retry [prev_rpc:%lu,retry_rpc:%lu,request:%lu]", rpc_event->get_event_id(), retry_event_id,
            get_request_id());
        }
      }
      return err;
    }


    int ObMsSqlScanRequest::initialize()
    {
      int ret = OB_SUCCESS;
      void *ptr = NULL;

      // in ps mode, should reset everything first
      reset();

      if (NULL == (ptr = ob_malloc(sizeof(ObMsSqlSubScanRequest))))
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "ob malloc failed, ret=%d", ret);
      }
      else
      {
        ObMsSqlSubScanRequest *sub_scan_req = new (ptr) ObMsSqlSubScanRequest();
        if (OB_SUCCESS != (ret = sub_requests_.push_back(sub_scan_req)))
        {
          TBSYS_LOG(WARN, "push ObMsSqlSubScanRequest* to sub_requests failed, ret=%d", ret);
          sub_scan_req->~ObMsSqlSubScanRequest();
          ob_free(sub_scan_req);
          sub_scan_req = NULL;
        }
      }
      return ret;
    }
    // reset 到 ObMsSqlScanRequest::initialize之前的状态
    // 使用方式：
    // 构造函数，initialize, reset，initialize, reset，initialize, reset
    // 不允许以下使用方式：
    // 构造函数，reset, do other than initialize ...
    void ObMsSqlScanRequest::reset()
    {
      /// reset all finished rpc event request
      /// leave the rest to the framework
      for (int32_t i = 0;i < total_sub_request_count_; ++i)
      {
        if (sub_requests_[i] != NULL)
        {
          sub_requests_[i]->~ObMsSqlSubScanRequest();
          ob_free(sub_requests_[i]);
          sub_requests_[i] = NULL;
        }
      }
      sub_requests_.clear();
      total_sub_request_count_ = 0;
      finished_sub_request_count_ = 0;
      merger_operator_.reset();
      ObMsSqlRequest::reset();
      tablet_scan_op_ = NULL;
      cs_result_mem_size_used_ = 0;
      sharding_limit_count_ = 0;
      cur_row_cell_cnt_ = 0;
    }

    
    int ObMsSqlScanRequest::get_next_row(oceanbase::common::ObRow &row)
    {
      int ret = OB_SUCCESS;
      if (true == is_finish())
      {
        // nop
      }
      else
      {
        // check if need to trigger more request
        if ((total_sub_request_count_ - finished_sub_request_count_ < max_parallel_count_)
            && (merger_operator_.get_mem_size_used() < max_cs_result_mem_size_)
            && false == org_req_range_iter_.end())
        {
          ret = do_request(max_parallel_count_, org_req_range_iter_, timeout_us_);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to trigger more event request. err=%d", ret);
          }
          TBSYS_LOG(DEBUG, "total_sub_request_count_=%d, finished_sub_request_count_=%d, max_parallel_count_=%ld,"
              "merger_operator_.get_mem_size_used()=%ld, max_cs_result_mem_size_=%ld",
              total_sub_request_count_, finished_sub_request_count_, max_parallel_count_,
              merger_operator_.get_mem_size_used(), max_cs_result_mem_size_);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = merger_operator_.get_next_row(row);
      }
      return ret;
    }


    int ObMsSqlScanRequest::get_next_range(
        const ObNewRange &org_scan_range,
        const ObNewScanner &prev_scan_result,
        const int64_t prev_limit_offset,
        ObNewRange &cur_range,
        int64_t & cur_limit_offset,
        ObStringBuf &buf)
    {
      int err = OB_SUCCESS;
      cur_limit_offset = 0;
      cur_range.reset();
      ObNewRange tablet_range;
      if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = prev_scan_result.get_range(tablet_range))))
      {
        TBSYS_LOG(WARN,"fail to get tablet range from prev result [err:%d]", err);
      }
      if ((OB_SUCCESS == err)  && (0 < prev_limit_offset))
      {
        /// check tablet split
        if ((tablet_range.compare_with_startkey2(org_scan_range) > 0) || 
            (tablet_range.compare_with_endkey2(org_scan_range) < 0))
        {
          TBSYS_LOG(WARN,"cs tablet splitted during the whole request, please try this request again");
          err = OB_NEED_RETRY;
        }
      }

      if (OB_SUCCESS == err)
      {
        err = get_next_range_for_trivail_scan(org_scan_range,prev_scan_result,cur_range);
        if ((OB_SUCCESS != err) && (OB_ITER_END != err))
        {
          TBSYS_LOG(WARN,"fail to get next range [err:%d]", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        ObRowkey str;
        str = cur_range.start_key_;
        if ((OB_SUCCESS == err) && (str.length() > 0))
        {
          if (OB_SUCCESS != (err = buf.write_string(str,&(cur_range.start_key_))))
          {
            TBSYS_LOG(WARN,"fail to deep copy cur_range.start_key_ [err:%d]", err);
          }
        }
        str = cur_range.end_key_;
        if ((OB_SUCCESS == err) && (str.length() > 0))
        {
          if (OB_SUCCESS != (err = buf.write_string(str,&(cur_range.end_key_))))
          {
            TBSYS_LOG(WARN,"fail to deep copy cur_range.end_key_ [err:%d]", err);
          }
        }
      }
      return err;
    }

    int ObMsSqlScanRequest::get_next_range_for_trivail_scan(
        const ObNewRange &org_scan_range,
        const ObNewScanner &prev_scan_result,
        ObNewRange &cur_range)
    {
      int err = OB_SUCCESS;
      ObNewRange tablet_range;
      bool request_fullfilled = false;
      int64_t fullfilled_row_num = 0;
      err = prev_scan_result.get_is_req_fullfilled(request_fullfilled,fullfilled_row_num);

      ObRowkey last_row_key;
      if (OB_SUCCESS == err)
      {
        if (request_fullfilled)
        {
          err = prev_scan_result.get_range(tablet_range);
          if (OB_SUCCESS == err)
          {
            if (tablet_range.compare_with_endkey2(org_scan_range) >= 0)
            {
              // finish scan
              err = OB_ITER_END;
            }
            else
            {
              last_row_key = tablet_range.end_key_;
            }
          }
        }
        else
        {
          if (OB_SUCCESS != (err = prev_scan_result.get_last_row_key(last_row_key)))
          {
            TBSYS_LOG(WARN,"fail to get last rowkey from prev result [err:%d]", err);
          }
        }
      }

      if (OB_SUCCESS == err)
      {
        cur_range = org_scan_range;
        // forward
        cur_range.start_key_ = last_row_key;
        cur_range.border_flag_.unset_inclusive_start();
      }
      return err;
    }
    /// namespace
  }
}
