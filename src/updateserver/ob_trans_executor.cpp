////===================================================================
 //
 // ob_trans_executor.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2012 Taobao.com, Inc.
 //
 // Created on 2012-08-30 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "common/ob_common_param.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_type.h"
#include "common/ob_trace_id.h"
#include "sql/ob_lock_filter.h"
#include "sql/ob_inc_scan.h"
#include "sql/ob_ups_modify.h"
#include "ob_ups_lock_filter.h"
#include "ob_ups_inc_scan.h"
#include "ob_memtable_modify.h"
#include "ob_update_server_main.h"
#include "ob_trans_executor.h"
#include "ob_ups_phy_operator_factory.h"
#include "ob_session_guard.h"

#define UPS ObUpdateServerMain::get_instance()->get_update_server()

namespace oceanbase
{
  namespace updateserver
  {
    TransExecutor::TransExecutor(ObUtilInterface &ui) : TransHandlePool(),
                                                        TransCommitThread(),
                                                        ui_(ui),
                                                        allocator_(),
                                                        session_ctx_factory_(),
                                                        session_mgr_(),
                                                        lock_mgr_(),
                                                        uncommited_session_list_(),
                                                        ups_result_buffer_(ups_result_memory_, OB_MAX_PACKET_LENGTH)
    {
      allocator_.set_mod_id(ObModIds::OB_UPS_TRANS_EXECUTOR_TASK);
      memset(ups_result_memory_, 0, OB_MAX_PACKET_LENGTH);

      memset(packet_handler_, 0, sizeof(packet_handler_));
      memset(trans_handler_, 0, sizeof(trans_handler_));
      memset(commit_handler_, 0, sizeof(commit_handler_));
      for (int64_t i = 0; i < OB_PACKET_NUM; i++)
      {
        packet_handler_[i] = phandle_non_impl;
        trans_handler_[i] = thandle_non_impl;
        commit_handler_[i] = chandle_non_impl;
      }

      packet_handler_[OB_FREEZE_MEM_TABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_MINOR_FREEZE_MEMTABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_MINOR_LOAD_BYPASS] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_MAJOR_LOAD_BYPASS] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_CLEAR_ACTIVE_MEMTABLE] = phandle_clear_active_memtable;
      packet_handler_[OB_UPS_ASYNC_CHECK_CUR_VERSION] = phandle_check_cur_version;

      trans_handler_[OB_NEW_SCAN_REQUEST] = thandle_scan_trans;
      trans_handler_[OB_NEW_GET_REQUEST] = thandle_get_trans;
      trans_handler_[OB_SCAN_REQUEST] = thandle_scan_trans;
      trans_handler_[OB_GET_REQUEST] = thandle_get_trans;
      trans_handler_[OB_MS_MUTATE] = thandle_write_trans;
      trans_handler_[OB_WRITE] = thandle_write_trans;
      trans_handler_[OB_PHY_PLAN_EXECUTE] = thandle_write_trans;
      trans_handler_[OB_START_TRANSACTION] = thandle_start_session;
      trans_handler_[OB_UPS_ASYNC_KILL_ZOMBIE] = thandle_kill_zombie;
      trans_handler_[OB_UPS_SHOW_SESSIONS] = thandle_show_sessions;
      trans_handler_[OB_UPS_KILL_SESSION] = thandle_kill_session;
      trans_handler_[OB_END_TRANSACTION] = thandle_end_session;

      commit_handler_[OB_MS_MUTATE] = chandle_write_commit;
      commit_handler_[OB_WRITE] = chandle_write_commit;
      commit_handler_[OB_PHY_PLAN_EXECUTE] = chandle_write_commit;
      commit_handler_[OB_END_TRANSACTION] = chandle_write_commit;
      commit_handler_[OB_SEND_LOG] = chandle_send_log;
      commit_handler_[OB_FAKE_WRITE_FOR_KEEP_ALIVE] = chandle_fake_write_for_keep_alive;
      commit_handler_[OB_SLAVE_REG] = chandle_slave_reg;
      commit_handler_[OB_SWITCH_SCHEMA] = chandle_switch_schema;
      commit_handler_[OB_UPS_FORCE_FETCH_SCHEMA] = chandle_force_fetch_schema;
      commit_handler_[OB_UPS_SWITCH_COMMIT_LOG] = chandle_switch_commit_log;
    }

    TransExecutor::~TransExecutor()
    {
      destroy();
    }

    int TransExecutor::init(const int64_t thread_num)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = allocator_.init(ALLOCATOR_TOTAL_LIMIT, ALLOCATOR_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE)))
      {
        TBSYS_LOG(WARN, "init allocator fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = session_mgr_.init(MAX_RO_NUM, MAX_RP_NUM, MAX_RW_NUM, &session_ctx_factory_)))
      {
        TBSYS_LOG(WARN, "init session mgr fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = TransCommitThread::init(TASK_QUEUE_LIMIT, FINISH_THREAD_IDLE)))
      {
        TBSYS_LOG(WARN, "init TransCommitThread fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = TransHandlePool::init(thread_num, TASK_QUEUE_LIMIT)))
      {
        TBSYS_LOG(WARN, "init TransHandlePool fail ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "TransExecutor init succ");
      }
      return ret;
    }

    void TransExecutor::destroy()
    {
      TransHandlePool::destroy();
      TransCommitThread::destroy();
      session_mgr_.destroy();
      allocator_.destroy();
    }

    void TransExecutor::on_commit_push_fail(void* ptr)
    {
      TBSYS_LOG(ERROR, "commit push fail, will kill self, task=%p", ptr);
      kill(getpid(), SIGTERM);
    }

    void TransExecutor::handle_packet(ObPacket &pkt)
    {
      int ret = OB_SUCCESS;
      int pcode = pkt.get_packet_code();
      TBSYS_LOG(DEBUG, "start handle packet pcode=%d", pcode);
      if (0 > pcode
          || OB_PACKET_NUM <= pcode)
      {
        easy_request_t *req = pkt.get_request();
        TBSYS_LOG(ERROR, "invalid packet code=%d src=%s",
                  pcode, NULL == req ? NULL : get_peer_ip(req));
        ret = OB_UNKNOWN_PACKET;
      }
      else if (!handle_in_situ_(pcode))
      {
        int64_t task_size = sizeof(Task) + pkt.get_buffer()->get_capacity();
        Task *task = (Task*)allocator_.alloc(task_size);
        if (NULL == task)
        {
          ret = OB_MEM_OVERFLOW;
        }
        else
        {
          task->reset();
          task->pkt = pkt;
          task->src_addr = get_easy_addr(pkt.get_request());
          char *data_buffer = (char*)task + sizeof(Task);
          memcpy(data_buffer, pkt.get_buffer()->get_data(), pkt.get_buffer()->get_capacity());
          task->pkt.get_buffer()->set_data(data_buffer, pkt.get_buffer()->get_capacity());
          task->pkt.get_buffer()->get_position() = pkt.get_buffer()->get_position();
          TBSYS_LOG(DEBUG, "task_size=%ld data_size=%ld pos=%ld",
                    task_size, pkt.get_buffer()->get_capacity(), pkt.get_buffer()->get_position());
          (task->pkt).set_receive_ts(tbsys::CTimeUtil::getTime());
          ret = push_task_(*task);
        }
      }
      else
      {
        int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                  UPS.get_param().packet_max_wait_time :
                                  pkt.get_source_timeout();
        // 1.等待正在运行的事务提交
        // 2.等待commitlog缓冲区中的日志刷到磁盘
        ret = session_mgr_.wait_write_session_end_and_lock(packet_timewait);
        if (OB_SUCCESS == ret)
        {
          ObSpinLockGuard guard(write_clog_mutex_);
          ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer_.get_buffer();
          if (NULL == my_buffer)
          {
            TBSYS_LOG(WARN, "get thread specific buffer fail");
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            my_buffer->reset();
            ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
            packet_handler_[pcode](pkt, thread_buff);
          }
          session_mgr_.unlock_write_session();
        }
        else
        {
          TBSYS_LOG(WARN, "wait_write_session_end_and_lock(pkt=%d, timeout=%ld)=>%d", pcode, packet_timewait, ret);
        }
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "handle_pkt fail, ret=%d, pkt=%d", ret, pkt.get_packet_code());
        UPS.response_result(ret, pkt);
      }
    }

    bool TransExecutor::handle_in_situ_(const int pcode)
    {
      bool bret = false;
      if (OB_FREEZE_MEM_TABLE == pcode
          || OB_UPS_MINOR_FREEZE_MEMTABLE == pcode
          || OB_UPS_MINOR_LOAD_BYPASS == pcode
          || OB_UPS_MAJOR_LOAD_BYPASS == pcode
          || OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE == pcode
          || OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE == pcode
          || OB_UPS_CLEAR_ACTIVE_MEMTABLE == pcode
          || OB_UPS_ASYNC_CHECK_CUR_VERSION == pcode)
      {
        bret = true;
      }
      return bret;
    }

    int TransExecutor::push_task_(Task &task)
    {
      int ret = OB_SUCCESS;
      switch (task.pkt.get_packet_code())
      {
        case OB_NEW_SCAN_REQUEST:
        case OB_NEW_GET_REQUEST:
        case OB_SCAN_REQUEST:
        case OB_GET_REQUEST:
        case OB_MS_MUTATE:
        case OB_WRITE:
        case OB_PHY_PLAN_EXECUTE:
        case OB_START_TRANSACTION:
        case OB_UPS_ASYNC_KILL_ZOMBIE:
        case OB_UPS_SHOW_SESSIONS:
        case OB_UPS_KILL_SESSION:
        case OB_END_TRANSACTION:
          ret = TransHandlePool::push(&task, task.pkt.get_req_sign());
          break;
        case OB_SEND_LOG:
        case OB_FAKE_WRITE_FOR_KEEP_ALIVE:
        case OB_SLAVE_REG:
        case OB_SWITCH_SCHEMA:
        case OB_UPS_FORCE_FETCH_SCHEMA:
        case OB_UPS_SWITCH_COMMIT_LOG:
          ret = TransCommitThread::push(&task);
          break;
        default:
          TBSYS_LOG(ERROR, "unknown packet code=%d src=%s",
                    task.pkt.get_packet_code(), inet_ntoa_r(task.src_addr));
          ret = OB_UNKNOWN_PACKET;
          break;
      }
      return ret;
    }

    bool TransExecutor::wait_for_commit_(const int pcode)
    {
      bool bret = true;
      if (OB_MS_MUTATE == pcode
          || OB_WRITE == pcode
          || OB_PHY_PLAN_EXECUTE == pcode
          || OB_END_TRANSACTION == pcode)
      {
        bret = false;
      }
      return bret;
    }

    int TransExecutor::fill_return_rows_(sql::ObPhyOperator &phy_op, ObNewScanner &scanner, sql::ObUpsResult &ups_result)
    {
      int ret = OB_SUCCESS;
      scanner.reuse();
      const ObRow *row = NULL;
      while (OB_SUCCESS == (ret = phy_op.get_next_row(row)))
      {
        if (NULL == row)
        {
          TBSYS_LOG(WARN, "row null pointer, phy_op=%p type=%d", &phy_op, phy_op.get_type());
          ret = OB_ERR_UNEXPECTED;
          break;
        }
        if (OB_SUCCESS != (ret = scanner.add_row(*row)))
        {
          TBSYS_LOG(WARN, "add row to scanner fail, ret=%d %s", ret, to_cstring(*row));
          break;
        }
      }
      if (OB_ITER_END == ret)
      {
        if (OB_SUCCESS != (ret = ups_result.set_scanner(scanner)))
        {
          TBSYS_LOG(WARN, "set scanner to ups_result fail ret=%d", ret);
        }
      }
      return ret;
    }

    void TransExecutor::reset_warning_strings_()
    {
      tbsys::WarningBuffer *warning_buffer = tbsys::get_tsi_warning_buffer();
      if (NULL != warning_buffer)
      {
        warning_buffer->reset();
      }
    }

    void TransExecutor::fill_warning_strings_(sql::ObUpsResult &ups_result)
    {
      tbsys::WarningBuffer *warning_buffer = tbsys::get_tsi_warning_buffer();
      if (NULL != warning_buffer)
      {
        for (uint32_t i = 0; i < warning_buffer->get_total_warning_count(); i++)
        {
          ups_result.add_warning_string(warning_buffer->get_warning(i));
          TBSYS_LOG(DEBUG, "fill warning string idx=%d [%s]", i, warning_buffer->get_warning(i));
        }
        warning_buffer->reset();
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    void TransExecutor::handle_trans(void *ptask, void *pdata)
    {
      int ret = OB_SUCCESS;
      thread_errno() = OB_SUCCESS;
      bool release_task = true;
      Task *task = (Task*)ptask;
      TransParamData *param = (TransParamData*)pdata;
      int64_t packet_timewait = (NULL == task || 0 == task->pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                task->pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      if (NULL == task)
      {
        TBSYS_LOG(WARN, "null pointer task=%p", task);
      }
      else if (NULL == param)
      {
        TBSYS_LOG(WARN, "null pointer param data pdata=%p src=%s",
                  pdata, inet_ntoa_r(task->src_addr));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 > task->pkt.get_packet_code()
              || OB_PACKET_NUM <= task->pkt.get_packet_code())
      {
        TBSYS_LOG(ERROR, "unknown packet code=%d src=%s",
                  task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        ret = OB_UNKNOWN_PACKET;
      }
      else if (process_timeout < (tbsys::CTimeUtil::getTime() - task->pkt.get_receive_ts()))
      {
        OB_STAT_INC(UPDATESERVER, UPS_STAT_PACKET_LONG_WAIT_COUNT, 1);
        TBSYS_LOG(WARN, "process timeout=%ld not enough cur_time=%ld receive_time=%ld packet_code=%d src=%s",
                  process_timeout, tbsys::CTimeUtil::getTime(), task->pkt.get_receive_ts(), task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        ret = OB_PROCESS_TIMEOUT;
      }
      else
      {
        //忽略log自带的前两个字段，trace id和chid，以后续的trace id和chid为准
        PROFILE_LOG(DEBUG, TRACE_ID SOURCE_CHANNEL_ID PCODE WAIT_TIME_US_IN_RW_QUEUE,
                    (task->pkt).get_trace_id(),
                    (task->pkt).get_channel_id(),
                    (task->pkt).get_packet_code(),
                    tbsys::CTimeUtil::getTime() - (task->pkt).get_receive_ts());
        release_task = trans_handler_[task->pkt.get_packet_code()](*this, *task, *param);
      }
      if (NULL != task)
      {
        if ((OB_SUCCESS != ret && !IS_SQL_ERR(ret))
            || (OB_SUCCESS != thread_errno() && !IS_SQL_ERR(thread_errno())))
        {
          TBSYS_LOG(WARN, "process fail ret=%d pcode=%d src=%s",
                    (OB_SUCCESS != ret) ? ret : thread_errno(), task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        }
        if (OB_SUCCESS != ret)
        {
          UPS.response_result(ret, task->pkt);
        }
        if (release_task)
        {
          allocator_.free(task);
          task = NULL;
        }
      }
    }

#define LOG_SESSION(header, session_ctx, task)                     \
  FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), header " packet wait=%ld start_time=%ld timeout=%ld src=%s fd=%d %s ctx=%p", \
                 tbsys::CTimeUtil::getTime() - task.pkt.get_receive_ts(), \
                 task.pkt.get_receive_ts(),                             \
                 task.pkt.get_source_timeout(),                         \
                 inet_ntoa_r(task.src_addr),                            \
                 task.pkt.get_request()->ms->c->fd,                     \
                 to_cstring(task.sid),                                  \
                 session_ctx);

    int TransExecutor::get_session_type(const ObTransID& sid, SessionType& type)
    {
      int ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
      BaseSessionCtx *session_ctx = NULL;
      if (OB_SUCCESS != (ret = session_guard.fetch_session(sid, session_ctx)))
      {
        TBSYS_LOG(WARN, "fetch_session(%s)=>%d", to_cstring(sid), ret);
      }
      else
      {
        type = session_ctx->get_type();
      }
      return ret;
    }

    bool TransExecutor::handle_write_trans_(Task &task, ObMutator &mutator, ObNewScanner &scanner)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      ObTransReq req;
      SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
      RWSessionCtx *session_ctx = NULL;
      ILockInfo *lock_info = NULL;
      int64_t packet_timewait = (0 == task.pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                task.pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      req.start_time_ = task.pkt.get_receive_ts();
      req.timeout_ = process_timeout;
      req.idle_time_ = process_timeout;
      task.sid.reset();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = ui_.ui_deserialize_mutator(*task.pkt.get_buffer(), mutator)))
      {
        TBSYS_LOG(WARN, "deserialize mutator fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = session_guard.start_session(req, task.sid, session_ctx)))
      {
        TBSYS_LOG(ERROR, "start_session()=>%d", ret);
      }
      else
      {
        LOG_SESSION("mutator start", session_ctx, task);
        session_ctx->set_stmt_start_time(task.pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        if (ST_READ_WRITE != session_ctx->get_type())
        {
          ret = OB_TRANS_NOT_MATCH;
          TBSYS_LOG(ERROR, "session.type[%d] is not RW", session_ctx->get_type());
        }
        else if (NULL == (lock_info = session_ctx->get_lock_info()))
        {
          ret = OB_NOT_INIT;
          TBSYS_LOG(ERROR, "lock_info == NULL");
        }
        else if (OB_SUCCESS != (ret = UPS.get_table_mgr().apply(OB_WRITE != task.pkt.get_packet_code(), *session_ctx, *lock_info, mutator)))
        {
          OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_FAIL_COUNT, 1);
          TBSYS_LOG(WARN, "table mgr apply fail ret=%d", ret);
        }
        else
        {
          scanner.reuse();
          if (OB_SUCCESS != (ret = session_ctx->get_ups_result().set_scanner(scanner)))
          {
            TBSYS_LOG(ERROR, "session_ctx.set_scanner()=>%d", ret);
          }
          else if (OB_SUCCESS != (ret = session_ctx->get_ups_mutator().get_mutator().pre_serialize()))
          {
            TBSYS_LOG(ERROR, "session_ctx.mutator.pre_serialize()=>%d", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "precommit end timeu=%ld", tbsys::CTimeUtil::getTime() - task.pkt.get_receive_ts());
            session_guard.revert();
            ret = TransCommitThread::push(&task);
            if (OB_SUCCESS != ret && task.sid.is_valid())
            {
              session_mgr_.end_session(task.sid.descriptor_, true);
            }
          }
        }
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, task.pkt);
      }
      return (OB_SUCCESS != ret);
    }

    void TransExecutor::handle_start_session_(Task &task, ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      ObTransReq req;
      SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
      BaseSessionCtx* session_ctx = NULL;
      task.sid.reset();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = req.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    task.pkt.get_buffer()->get_position())))
      {
        TBSYS_LOG(WARN, "deserialize session_req fail ret=%d", ret);
      }
      else
      {
        req.start_time_ = task.pkt.get_receive_ts();
      }
      if (OB_SUCCESS != ret)
      {}
      else if (OB_SUCCESS != (ret = session_guard.start_session(req, task.sid, session_ctx)))
      {
        TBSYS_LOG(WARN, "begin session fail ret=%d", ret);
      }
      else
      {
        LOG_SESSION("session start", session_ctx, task);
        PRINT_TRACE_BUF(session_ctx->get_tlog_buffer());
      }
      UPS.response_trans_id(ret, task.pkt, task.sid, buffer);
    }

    bool TransExecutor::handle_end_session_(Task &task, ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      bool need_free_task = true;
      ObEndTransReq req;
      SessionType type = ST_READ_ONLY;
      UNUSED(buffer);
      task.sid.reset();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = req.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    task.pkt.get_buffer()->get_position())))
      {
        TBSYS_LOG(WARN, "deserialize session_req fail ret=%d", ret);
      }
      else if (!req.trans_id_.is_valid())
      {
        ret = OB_TRANS_NOT_MATCH;
        TBSYS_LOG(ERROR, "sid[%s] is invalid", to_cstring(task.sid));
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, task.pkt);
      }
      else if (OB_SUCCESS != (ret = get_session_type(req.trans_id_, type)))
      {
        UPS.response_result(req.rollback_? OB_SUCCESS: ret, task.pkt);
        TBSYS_LOG(ERROR, "get_session_type(%s)=>%d", to_cstring(req.trans_id_), ret);
      }
      else
      {
        // sid 正确
        if (ST_READ_WRITE == type && !req.rollback_)
        {
          task.sid = req.trans_id_;
          if (OB_SUCCESS != (ret = TransCommitThread::push(&task)))
          {
            UPS.response_result(ret, task.pkt);
          }
          else
          {
            need_free_task = false;
          }
        }
        else
        {
          ret = session_mgr_.end_session(req.trans_id_.descriptor_, req.rollback_);
          UPS.response_result(req.rollback_? OB_SUCCESS: ret, task.pkt);
        }
      }
      return need_free_task;
    }

    bool TransExecutor::handle_phyplan_trans_(Task &task,
                                             sql::ObPhysicalPlan &phy_plan,
                                             ObNewScanner &new_scanner,
                                             ModuleArena &allocator,
                                             ObDataBuffer& buffer)
    {
      reset_warning_strings_();
      bool need_free_task = true;
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      int end_session_ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, end_session_ret);
      RWSessionCtx* session_ctx = NULL;
      int64_t packet_timewait = (0 == task.pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                task.pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      int64_t pos = task.pkt.get_buffer()->get_position();
      bool with_sid = false;
      task.sid.reset();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        TBSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = phy_plan.deserialize_header(task.pkt.get_buffer()->get_data(),
                                                                task.pkt.get_buffer()->get_capacity(),
                                                                pos)))
      {
        TBSYS_LOG(WARN, "phy_plan.deseiralize_header ret=%d", ret);
      }
      else if ((with_sid = phy_plan.get_trans_id().is_valid()))
      {
        if (OB_SUCCESS != (ret = session_guard.fetch_session(phy_plan.get_trans_id(), session_ctx)))
        {
          TBSYS_LOG(USER_ERROR, "Session has been killed, error %d, \'%s\'", ret, to_cstring(phy_plan.get_trans_id()));
        }
        else
        {
          task.sid = phy_plan.get_trans_id();
          LOG_SESSION("session stmt", session_ctx, task);
        }
      }
      else
      {
        phy_plan.get_trans_req().start_time_ = task.pkt.get_receive_ts();
        if (OB_SUCCESS != (ret = session_guard.start_session(phy_plan.get_trans_req(), task.sid, session_ctx)))
        {
          TBSYS_LOG(ERROR, "start_session()=>%d", ret);
        }
        else
        {
          if (phy_plan.get_start_trans())
          {
            session_ctx->get_ups_result().set_trans_id(task.sid);
          }
          LOG_SESSION("start_trans", session_ctx, task);
        }
      }
      if (OB_SUCCESS != ret)
      {}
      else
      {
        session_ctx->set_stmt_start_time(task.pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        sql::ObPhyOperator *main_op = NULL;
        ObUpsPhyOperatorFactory phy_operator_factory;
        phy_operator_factory.set_session_ctx(session_ctx);
        phy_operator_factory.set_table_mgr(&UPS.get_table_mgr());
        phy_plan.clear();
        allocator.reuse();
        phy_plan.set_allocator(&allocator);
        phy_plan.set_operator_factory(&phy_operator_factory);

        if (OB_SUCCESS != (ret = phy_plan.deserialize(task.pkt.get_buffer()->get_data(),
                                                      task.pkt.get_buffer()->get_capacity(),
                                                      task.pkt.get_buffer()->get_position())))
        {
          TBSYS_LOG(WARN, "deserialize phy_plan fail ret=%d", ret);
        }
        else
        {
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "phyplan allocator used=%ld total=%ld %s",
                        allocator.used(), allocator.total(), to_cstring(phy_plan));
        }
        if (OB_SUCCESS != ret)
        {}
        else if (NULL == (main_op = phy_plan.get_main_query()))
        {
          TBSYS_LOG(WARN, "main query null pointer");
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_SUCCESS != (ret = main_op->open()))
        {
          if (OB_ERR_PRIMARY_KEY_DUPLICATE != ret)
          {
            OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_FAIL_COUNT, 1);
            TBSYS_LOG(WARN, "main_op open fail ret=%d", ret);
          }
        }
        else if (OB_SUCCESS != (ret = fill_return_rows_(*main_op, new_scanner, session_ctx->get_ups_result())))
        {
          main_op->close();
          TBSYS_LOG(WARN, "fill result rows with main_op fail ret=%d", ret);
        }
        else
        {
          main_op->close();
          fill_warning_strings_(session_ctx->get_ups_result());
          TBSYS_LOG(DEBUG, "precommit end timeu=%ld", tbsys::CTimeUtil::getTime() - task.pkt.get_receive_ts());
        }
        if (OB_SUCCESS != ret)
        {}
        else if (with_sid || phy_plan.get_start_trans())
        {
          session_ctx->get_ups_result().serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
          UPS.response_buffer(ret, task.pkt, buffer);
        }
        else
        {
          if (OB_SUCCESS != (ret = session_ctx->get_ups_mutator().get_mutator().pre_serialize()))
          {
            TBSYS_LOG(ERROR, "session_ctx.mutator.pre_serialize()=>%d", ret);
          }
          else
          {
            session_guard.revert();
            if (OB_SUCCESS != (ret = TransCommitThread::push(&task)))
            {
              TBSYS_LOG(WARN, "commit thread queue is full, ret=%d", ret);
            }
            else
            {
              need_free_task = false;
            }
            if (OB_SUCCESS != ret && task.sid.is_valid())
            {
              session_mgr_.end_session(task.sid.descriptor_, true);
            }
          }
        }
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "ret=%d affected_rows=%ld", ret, session_ctx->get_ups_result().get_affected_rows());
        PRINT_TRACE_BUF(session_ctx->get_tlog_buffer());
        if (OB_SUCCESS != ret
            && (!with_sid || !IS_SQL_ERR(ret)))
        {
          end_session_ret = ret;
          TBSYS_LOG(DEBUG, "need rollback session %s ret=%d", to_cstring(task.sid), ret);
        }
      }
      if (OB_SUCCESS != ret)
      {
        ret = (OB_ERR_SHARED_LOCK_CONFLICT == ret) ? OB_EAGAIN : ret;
        const char *error_string = ob_get_err_msg().ptr();
        UPS.response_result(ret, error_string, task.pkt);
        ob_reset_err_msg();
      }
      return need_free_task;
    }

    void TransExecutor::handle_get_trans_(ObPacket &pkt,
                                          ObGetParam &get_param,
                                          ObScanner &scanner,
                                          ObCellNewScanner &new_scanner,
                                          ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      uint32_t session_descriptor = UINT32_MAX;
      ROSessionCtx *session_ctx = NULL;
      int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      if (OB_SUCCESS != (ret = get_param.deserialize(pkt.get_buffer()->get_data(),
                                                          pkt.get_buffer()->get_capacity(),
                                                          pkt.get_buffer()->get_position())))
      {
        TBSYS_LOG(WARN, "deserialize get_param fail ret=%d", ret);
      }
      else if (!UPS.can_serve_read_req(get_param.get_is_read_consistency(), get_param.get_version_range().get_query_version()))
      {
        TBSYS_LOG(WARN, "the scan request require consistency, ObiRole:%s RoleMgr:%s, query_version=%ld",
                  UPS.get_obi_role().get_role_str(), UPS.get_role_mgr().get_role_str(), get_param.get_version_range().get_query_version());
        ret = OB_NOT_MASTER;
      }
      else if (OB_SUCCESS != (ret = session_mgr_.begin_session(ST_READ_ONLY, pkt.get_receive_ts(), process_timeout, process_timeout, session_descriptor)))
      {
        TBSYS_LOG(WARN, "begin session fail ret=%d", ret);
      }
      else if (NULL == (session_ctx = session_mgr_.fetch_ctx<ROSessionCtx>(session_descriptor)))
      {
        TBSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "start handle get, packet wait=%ld start_time=%ld timeout=%ld src=%s",
                      tbsys::CTimeUtil::getTime() - pkt.get_receive_ts(),
                      pkt.get_receive_ts(),
                      pkt.get_source_timeout(),
                      NULL == pkt.get_request() ? NULL : get_peer_ip(pkt.get_request()));
        thread_read_prepare();
        session_ctx->set_stmt_start_time(pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        if (OB_NEW_GET_REQUEST == pkt.get_packet_code())
        {
          new_scanner.reuse();
          common::ObRowDesc row_desc;
          if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(get_param, true, row_desc)))
          {
            TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
          }
          else
          {
            new_scanner.set_row_desc(row_desc);
            ret = UPS.get_table_mgr().new_get(*session_ctx, get_param, new_scanner, pkt.get_receive_ts(), process_timeout);
          }
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, new_scanner, buffer);
          }
        }
        else
        {
          scanner.reset();
          ret = UPS.get_table_mgr().get(*session_ctx, get_param, scanner, pkt.get_receive_ts(), process_timeout);
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, scanner, buffer);
          }
        }
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "get from table mgr ret=%d", ret);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_GET_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_GET_TIMEU, session_ctx->get_session_timeu());
        thread_read_complete();
        session_ctx->set_last_active_time(tbsys::CTimeUtil::getTime());
        session_mgr_.revert_ctx(session_descriptor);
        session_mgr_.end_session(session_descriptor);
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, pkt);
      }
    }

    void TransExecutor::handle_scan_trans_(ObPacket &pkt,
                                          ObScanParam &scan_param,
                                          ObScanner &scanner,
                                          ObCellNewScanner &new_scanner,
                                          ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      uint32_t session_descriptor = UINT32_MAX;
      ROSessionCtx *session_ctx = NULL;
      int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      if (OB_SUCCESS != (ret = scan_param.deserialize(pkt.get_buffer()->get_data(),
                                                          pkt.get_buffer()->get_capacity(),
                                                          pkt.get_buffer()->get_position())))
      {
        TBSYS_LOG(WARN, "deserialize get_param fail ret=%d", ret);
      }
      else if (!UPS.can_serve_read_req(scan_param.get_is_read_consistency(), scan_param.get_version_range().get_query_version()))
      {
        TBSYS_LOG(WARN, "the scan request require consistency, ObiRole:%s RoleMgr:%s, query_version=%ld",
                  UPS.get_obi_role().get_role_str(), UPS.get_role_mgr().get_role_str(), scan_param.get_version_range().get_query_version());
        ret = OB_NOT_MASTER;
      }
      else if (OB_SUCCESS != (ret = session_mgr_.begin_session(ST_READ_ONLY, pkt.get_receive_ts(), process_timeout, process_timeout, session_descriptor)))
      {
        TBSYS_LOG(WARN, "begin session fail ret=%d", ret);
      }
      else if (NULL == (session_ctx = session_mgr_.fetch_ctx<ROSessionCtx>(session_descriptor)))
      {
        TBSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor);
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "start handle scan, packet wait=%ld start_time=%ld timeout=%ld src=%s",
                      tbsys::CTimeUtil::getTime() - pkt.get_receive_ts(),
                      pkt.get_receive_ts(),
                      pkt.get_source_timeout(),
                      NULL == pkt.get_request() ? NULL : get_peer_ip(pkt.get_request()));
        thread_read_prepare();
        session_ctx->set_stmt_start_time(pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        if (OB_NEW_SCAN_REQUEST == pkt.get_packet_code())
        {
          new_scanner.reuse();
          common::ObRowDesc row_desc;
          if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(scan_param, row_desc)))
          {
            TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
          }
          else
          {
            new_scanner.set_row_desc(row_desc);
            ret = UPS.get_table_mgr().new_scan(*session_ctx, scan_param, new_scanner, pkt.get_receive_ts(), process_timeout);
          }
#if 0
          if (OB_SUCCESS == ret)
          {
            ObUpsRow tmp_ups_row;
            tmp_ups_row.set_row_desc(row_desc);
            if (OB_SUCCESS != (ret = ObNewScannerHelper::print_new_scanner(new_scanner, tmp_ups_row, true)))
            {
              TBSYS_LOG(WARN, "print new scanner fail:ret[%d]", ret);
            }
          }
#endif
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, new_scanner, buffer);
          }
        }
        else
        {
          scanner.reset();
          ret = UPS.get_table_mgr().scan(*session_ctx, scan_param, scanner, pkt.get_receive_ts(), process_timeout);
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, scanner, buffer);
          }
        }
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "get from table mgr ret=%d", ret);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_SCAN_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_SCAN_TIMEU, session_ctx->get_session_timeu());
        thread_read_complete();
        session_ctx->set_last_active_time(tbsys::CTimeUtil::getTime());
        session_mgr_.revert_ctx(session_descriptor);
        session_mgr_.end_session(session_descriptor);
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, pkt);
      }
    }

    void TransExecutor::handle_kill_zombie_()
    {
      const bool force = false;
      session_mgr_.kill_zombie_session(force);
    }

    void TransExecutor::handle_show_sessions_(ObPacket &pkt,
                                              ObNewScanner &scanner,
                                              ObDataBuffer &buffer)
    {
      scanner.reuse();
      session_mgr_.show_sessions(scanner);
      UPS.response_scanner(OB_SUCCESS, pkt, scanner, buffer);
    }

    void TransExecutor::handle_kill_session_(ObPacket &pkt)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      uint32_t session_descriptor = INVALID_SESSION_DESCRIPTOR;
      if (OB_SUCCESS != (ret = serialization::decode_vi32(pkt.get_buffer()->get_data(),
                                                          pkt.get_buffer()->get_capacity(),
                                                          pkt.get_buffer()->get_position(),
                                                          (int32_t*)&session_descriptor)))
      {
        TBSYS_LOG(WARN, "deserialize session descriptor fail ret=%d", ret);
      }
      else
      {
        ret = session_mgr_.kill_session(session_descriptor);
        TBSYS_LOG(INFO, "kill session ret=%d sd=%u", ret, session_descriptor);
      }
      UPS.response_result(ret, pkt);
    }

    void *TransExecutor::on_trans_begin()
    {
      TransParamData *ret = NULL;
      void *buffer = ob_malloc(sizeof(TransParamData), ObModIds::OB_UPS_PARAM_DATA);
      if (NULL != buffer)
      {
        ret = new(buffer) TransParamData();
        ret->buffer.set_data(ret->cbuffer, sizeof(ret->cbuffer));
      }
      return ret;
    }

    void TransExecutor::on_trans_end(void *ptr)
    {
      if (NULL != ptr)
      {
        ob_free(ptr);
        ptr = NULL;
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    bool is_write_packet(ObPacket& pkt)
    {
      bool ret = false;
      switch(pkt.get_packet_code())
      {
        case OB_MS_MUTATE:
        case OB_WRITE:
        case OB_PHY_PLAN_EXECUTE:
        case OB_END_TRANSACTION:
          ret = true;
          break;
        default:
          ret = false;
      }
      return ret;
    }

    int64_t TransExecutor::get_seq(void* ptr)
    {
      int64_t seq = 0;
      Task& task = *((Task*)ptr);
      int64_t trans_id = tbsys::CTimeUtil::getTime();
      int ret = OB_SUCCESS;
      seq = session_mgr_.get_trans_seq().next(trans_id);
      if (NULL == ptr)
      {
        TBSYS_LOG(ERROR, "commit queue, NULL ptr, will kill self");
        kill(getpid(), SIGTERM);
      }
      else if (is_write_packet(task.pkt))
      {
        {
          SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
          RWSessionCtx* session_ctx = NULL;
          if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx)))
          {
            task.sid.reset();
            TBSYS_LOG(ERROR, "fetch_session(sid=%s)=>%d", to_cstring(task.sid), ret);
          }
          else if (!UPS.get_log_mgr().check_log_size(session_ctx->get_ups_mutator().get_serialize_size()))
          {
            ret = OB_LOG_TOO_LARGE;
            TBSYS_LOG(ERROR, "mutator.size[%ld] too large",
                      session_ctx->get_ups_mutator().get_serialize_size());
          }
          else
          {
            session_ctx->set_trans_id(trans_id);
          }
        }
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = session_mgr_.precommit(task.sid.descriptor_)))
          {
            TBSYS_LOG(ERROR, "precommit(%s)=>%d", to_cstring(task.sid), ret);
          }
        }
      }
      return seq;
    }

    void TransExecutor::handle_commit(void *ptask, void *pdata)
    {
      int ret = OB_SUCCESS;
      thread_errno() = OB_SUCCESS;
      bool release_task = true;
      Task *task = (Task*)ptask;
      CommitParamData *param = (CommitParamData*)pdata;
      ObSpinLockGuard guard(write_clog_mutex_);
      if (NULL == task)
      {
        TBSYS_LOG(WARN, "null pointer task=%p", task);
      }
      else if (NULL == param)
      {
        TBSYS_LOG(WARN, "null pointer param data pdata=%p src=%s",
                  pdata, inet_ntoa_r(task->src_addr));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 > task->pkt.get_packet_code()
              || OB_PACKET_NUM <= task->pkt.get_packet_code())
      {
        TBSYS_LOG(ERROR, "unknown packet code=%d src=%s",
                  task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        ret = OB_UNKNOWN_PACKET;
      }
      else
      {
        //忽略log自带的前两个字段，trace id和chid，以后续的trace id和chid为准
        PROFILE_LOG(DEBUG, TRACE_ID SOURCE_CHANNEL_ID PCODE WAIT_TIME_US_IN_COMMIT_QUEUE,
                    (task->pkt).get_trace_id(),
                    (task->pkt).get_channel_id(),
                    (task->pkt).get_packet_code(),
                    tbsys::CTimeUtil::getTime() - (task->pkt).get_receive_ts());
        if (wait_for_commit_(task->pkt.get_packet_code()))
        {
          commit_log_();
        }
        release_task = commit_handler_[task->pkt.get_packet_code()](*this, *task, *param);
      }
      if (NULL != task)
      {
        if (OB_SUCCESS != ret
            || OB_SUCCESS != thread_errno())
        {
          TBSYS_LOG(WARN, "process fail ret=%d pcode=%d src=%s",
                    (OB_SUCCESS != ret) ? ret : thread_errno(), task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        }
        if (OB_SUCCESS != ret)
        {
          UPS.response_result(ret, task->pkt);
        }
        if (release_task)
        {
          allocator_.free(task);
          task = NULL;
        }
      }
    }

    int TransExecutor::handle_write_commit_(Task &task)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      {
        SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
        RWSessionCtx* session_ctx = NULL;
        if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx)))
        {
          TBSYS_LOG(ERROR, "fetch_session(sid=%s)=>%d", to_cstring(task.sid), ret);
        }
        else
        {
          batch_start_time() = (0 == batch_start_time()) ? tbsys::CTimeUtil::getTime() : batch_start_time();
          int64_t cur_timestamp = session_ctx->get_trans_id();
          session_ctx->get_uc_info().uc_checksum = ob_crc64(session_ctx->get_uc_info().uc_checksum, &cur_timestamp, sizeof(cur_timestamp));
          if (cur_timestamp <= 0)
          {
            TBSYS_LOG(ERROR, "session_ctx.trans_id=%ld <= 0, will kill self", cur_timestamp);
            kill(getpid(), SIGTERM);
          }
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "trans_checksum=%lu trans_id=%ld",
                         session_ctx->get_uc_info().uc_checksum, cur_timestamp);
          int ret = fill_log_(task, *session_ctx);
          if (OB_SUCCESS != ret)
          {
            if (OB_EAGAIN != ret)
            {
              TBSYS_LOG(WARN, "fill log fail ret=%d %s", ret, to_cstring(task.sid));
            }
            else if (OB_SUCCESS != (ret = commit_log_()))
            {
              TBSYS_LOG(WARN, "commit log fail ret=%d %s", ret, to_cstring(task.sid));
            }
            else if (OB_SUCCESS != (ret = fill_log_(task, *session_ctx)))
            {
              TBSYS_LOG(ERROR, "second fill log fail ret=%d %s serialize_size=%ld uncommited_number=%ld",
                        ret, to_cstring(task.sid),
                        session_ctx->get_ups_mutator().get_serialize_size(),
                        uncommited_session_list_.size());
            }
            else
            {
              TBSYS_LOG(INFO, "second fill log succ %s", to_cstring(task.sid));
            }
          }
        }
      }
      if (OB_SUCCESS == ret
          && (0 == TransCommitThread::get_queued_num()
              || MAX_BATCH_NUM <= uncommited_session_list_.size()))
      {
        ret = commit_log_();
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, task.pkt);
      }
      return ret;
    }

    void TransExecutor::on_commit_idle()
    {
      commit_log_();
      try_submit_auto_freeze_();
    }

    int TransExecutor::fill_log_(Task &task, RWSessionCtx &session_ctx)
    {
      int ret = OB_SUCCESS;
      MemTable *mt = NULL;
      if (0 != session_ctx.get_ups_mutator().get_mutator().size()
          || (OB_PHY_PLAN_EXECUTE != task.pkt.get_packet_code()
              && OB_END_TRANSACTION != task.pkt.get_packet_code()))
      {
        if (NULL == (mt = session_ctx.get_uc_info().host))
        {
          ret = OB_ERR_UNEXPECTED;
        }
        else
        {
          int64_t uc_checksum = 0;
          uc_checksum = mt->calc_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
          session_ctx.get_ups_mutator().set_mutate_timestamp(session_ctx.get_trans_id());
          session_ctx.get_ups_mutator().set_memtable_checksum_before_mutate(mt->get_uncommited_checksum());
          session_ctx.get_ups_mutator().set_memtable_checksum_after_mutate(uc_checksum);

          ret = UPS.get_table_mgr().fill_commit_log(session_ctx.get_ups_mutator(), session_ctx.get_tlog_buffer());
          if (OB_SUCCESS == ret)
          {
            session_ctx.get_uc_info().uc_checksum = uc_checksum;
            mt->update_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
          }
        }
      }
      else
      {
        session_ctx.get_uc_info().host = NULL;
      }
      if (OB_SUCCESS == ret)
      {
        if (0 != uncommited_session_list_.push_back(&task))
        {
          ret = (OB_SUCCESS == ret) ? OB_MEM_OVERFLOW : ret;
          TBSYS_LOG(ERROR, "unexpected push task to uncommited_session_list fail list_size=%ld, will kill self", uncommited_session_list_.size());
          kill(getpid(), SIGTERM);
        }
        else
        {
          // 保证在flush commit log成功后不会被kill掉
          session_ctx.set_frozen();
        }
      }
      FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "checksum=%lu affected_rows=%ld ret=%d",
                    (NULL == mt) ? 0 : mt->get_uncommited_checksum(),
                    session_ctx.get_ups_result().get_affected_rows(), ret);
      return ret;
    }

    int TransExecutor::commit_log_()
    {
      int ret = OB_SUCCESS;
      if (0 < uncommited_session_list_.size())
      {
        CLEAR_TRACE_BUF(TraceLog::get_logbuffer());
        ret = UPS.get_table_mgr().flush_commit_log(TraceLog::get_logbuffer());
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "flush commit log fail ret=%d uncommited_number=%ld, will kill self", ret, uncommited_session_list_.size());
          kill(getpid(), SIGTERM);
        }
        bool rollback = (OB_SUCCESS != ret);
        int64_t i = 0;
        ObList<Task*>::iterator iter;
        for (iter = uncommited_session_list_.begin(); iter != uncommited_session_list_.end(); iter++, i++)
        {
          Task *task = *iter;
          if (NULL == task)
          {
            TBSYS_LOG(ERROR, "unexpected task null pointer batch=%ld, will kill self", uncommited_session_list_.size());
            kill(getpid(), SIGTERM);
          }
          else
          {
            {
              int ret_ok = OB_SUCCESS;
              SessionGuard session_guard(session_mgr_, lock_mgr_, ret_ok);
              RWSessionCtx *session_ctx = NULL;
              if (OB_SUCCESS != (ret = session_guard.fetch_session(task->sid, session_ctx)))
              {
                TBSYS_LOG(ERROR, "unexpected fetch_session fail ret=%d %s, will kill self", ret, to_cstring(task->sid));
                kill(getpid(), SIGTERM);
              }
              else
              {
                FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "%sbatch=%ld:%ld", TraceLog::get_logbuffer().buffer, i, uncommited_session_list_.size());
                ups_result_buffer_.set_data(ups_result_memory_, OB_MAX_PACKET_LENGTH);
                session_ctx->get_ups_result().serialize(ups_result_buffer_.get_data(),
                                                        ups_result_buffer_.get_capacity(),
                                                        ups_result_buffer_.get_position());
              }
            }
            if (OB_SUCCESS != (ret = session_mgr_.end_session(task->sid.descriptor_, rollback)))
            {
              TBSYS_LOG(ERROR, "unexpected end_session fail ret=%d %s, will kill self", ret, to_cstring(task->sid));
              kill(getpid(), SIGTERM);
            }
            ret = rollback ? OB_TRANS_ROLLBACKED : ret;
            if (OB_PHY_PLAN_EXECUTE == task->pkt.get_packet_code()
                && OB_SUCCESS == ret)
            {
              UPS.response_buffer(ret, task->pkt, ups_result_buffer_);
            }
            else
            {
              UPS.response_result(ret, task->pkt);
            }
            allocator_.free(task);
            task = NULL;
          }
        }
        uncommited_session_list_.clear();
        OB_STAT_INC(UPDATESERVER, UPS_STAT_BATCH_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_BATCH_TIMEU, tbsys::CTimeUtil::getTime() - batch_start_time());
        batch_start_time() = 0;
      }
      try_submit_auto_freeze_();
      return ret;
    }

    void TransExecutor::try_submit_auto_freeze_()
    {
      int err = OB_SUCCESS;
      static int64_t last_try_freeze_time = 0;
      if (TRY_FREEZE_INTERVAL < (tbsys::CTimeUtil::getTime() - last_try_freeze_time)
          && UPS.get_table_mgr().need_auto_freeze())
      {
        int64_t cur_ts = tbsys::CTimeUtil::getTime();
        if (OB_SUCCESS != (err = UPS.submit_auto_freeze()))
        {
          TBSYS_LOG(WARN, "submit_auto_freeze()=>%d", err);
        }
        else
        {
          TBSYS_LOG(INFO, "submit async auto freeze task, last_ts=%ld, cur_ts=%ld", last_try_freeze_time, cur_ts);
          last_try_freeze_time = cur_ts;
        }
      }
    }

    void *TransExecutor::on_commit_begin()
    {
      CommitParamData *ret = NULL;
      void *buffer = ob_malloc(sizeof(CommitParamData), ObModIds::OB_UPS_PARAM_DATA);
      if (NULL != buffer)
      {
        ret = new(buffer) CommitParamData();
        ret->buffer.set_data(ret->cbuffer, sizeof(ret->cbuffer));
      }
      return ret;
    }

    void TransExecutor::on_commit_end(void *ptr)
    {
      if (NULL != ptr)
      {
        ob_free(ptr);
        ptr = NULL;
      }
    }

    void TransExecutor::log_trans_info() const
    {
      TBSYS_LOG(INFO, "==========log trans executor start==========");
      TBSYS_LOG(INFO, "allocator info hold=%ld allocated=%ld", allocator_.hold(), allocator_.allocated());
      TBSYS_LOG(INFO, "session_mgr info flying session num ro=%ld rp=%ld rw=%ld",
                session_mgr_.get_flying_rosession_num(),
                session_mgr_.get_flying_rpsession_num(),
                session_mgr_.get_flying_rwsession_num());
      TBSYS_LOG(INFO, "queued_num trans_thread=%ld commit_thread=%ld",
                TransHandlePool::get_queued_num(),
                TransCommitThread::get_queued_num());
      TBSYS_LOG(INFO, "==========log trans executor end==========");
    }

    int &TransExecutor::thread_errno()
    {
      static __thread int thread_errno = OB_SUCCESS;
      return thread_errno;
    }

    int64_t &TransExecutor::batch_start_time()
    {
      static __thread int64_t batch_start_time = 0;
      return batch_start_time;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    void TransExecutor::phandle_non_impl(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UNUSED(buffer);
      TBSYS_LOG(ERROR, "packet code=%d no implement phandler", pkt.get_packet_code());
      UPS.response_result(OB_NOT_IMPLEMENT, pkt);
    }

    void TransExecutor::phandle_freeze_memtable(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UPS.ups_freeze_memtable(pkt.get_api_version(),
                              &pkt,
                              buffer,
                              pkt.get_packet_code());
    }

    void TransExecutor::phandle_clear_active_memtable(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UNUSED(buffer);
      UPS.ups_clear_active_memtable(pkt.get_api_version(),
                                    pkt.get_request(),
                                    pkt.get_channel_id());
    }

    void TransExecutor::phandle_check_cur_version(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UNUSED(pkt);
      UNUSED(buffer);
      UPS.ups_check_cur_version();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    bool TransExecutor::thandle_non_impl(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      TBSYS_LOG(ERROR, "packet code=%d no implement thandler", task.pkt.get_packet_code());
      UPS.response_result(OB_NOT_IMPLEMENT, task.pkt);
      return true;
    }

    bool TransExecutor::thandle_scan_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      host.handle_scan_trans_(task.pkt, pdata.scan_param, pdata.scanner, pdata.new_scanner, pdata.buffer);
      return true;
    }

    bool TransExecutor::thandle_get_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      host.handle_get_trans_(task.pkt, pdata.get_param, pdata.scanner, pdata.new_scanner, pdata.buffer);
      return true;
    }

    bool TransExecutor::thandle_write_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      bool ret = true;
      pdata.buffer.get_position() = 0;
      if (OB_PHY_PLAN_EXECUTE == task.pkt.get_packet_code())
      {
        ret = host.handle_phyplan_trans_(task, pdata.phy_plan, pdata.new_scanner, pdata.allocator, pdata.buffer);
      }
      else
      {
        ret = host.handle_write_trans_(task, pdata.mutator, pdata.new_scanner);
      }
      return ret;
    }

    bool TransExecutor::thandle_start_session(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      host.handle_start_session_(task, pdata.buffer);
      return true;
    }

    bool TransExecutor::thandle_kill_zombie(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      UNUSED(task);
      UNUSED(pdata);
      host.handle_kill_zombie_();
      return true;
    }

    bool TransExecutor::thandle_show_sessions(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      host.handle_show_sessions_(task.pkt, pdata.new_scanner, pdata.buffer);
      return true;
    }

    bool TransExecutor::thandle_kill_session(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      UNUSED(pdata);
      host.handle_kill_session_(task.pkt);
      return true;
    }

    bool TransExecutor::thandle_end_session(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      return host.handle_end_session_(task, pdata.buffer);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    bool TransExecutor::chandle_non_impl(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      TBSYS_LOG(ERROR, "packet code=%d no implement chandler", task.pkt.get_packet_code());
      UPS.response_result(OB_NOT_IMPLEMENT, task.pkt);
      return true;
    }

    bool TransExecutor::chandle_write_commit(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(pdata);
      int ret = host.handle_write_commit_(task);
      return (OB_SUCCESS != ret);
    }

    bool TransExecutor::chandle_send_log(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      pdata.buffer.get_position() = 0;
      UPS.ups_slave_write_log(task.pkt.get_api_version(),
                              *(task.pkt.get_buffer()),
                              task.pkt.get_request(),
                              task.pkt.get_channel_id(),
                              pdata.buffer);
      return true;
    }

    bool TransExecutor::chandle_fake_write_for_keep_alive(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(task);
      UNUSED(pdata);
      UPS.ups_handle_fake_write_for_keep_alive();
      return true;
    }

    bool TransExecutor::chandle_slave_reg(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      pdata.buffer.get_position() = 0;
      UPS.ups_slave_register(task.pkt.get_api_version(),
                            *(task.pkt.get_buffer()),
                            task.pkt.get_request(),
                            task.pkt.get_channel_id(),
                            pdata.buffer);
      return true;
    }

    bool TransExecutor::chandle_switch_schema(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      UPS.ups_switch_schema(task.pkt.get_api_version(),
                            &(task.pkt),
                            *(task.pkt.get_buffer()));
      return true;
    }

    bool TransExecutor::chandle_force_fetch_schema(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      UPS.ups_force_fetch_schema(task.pkt.get_api_version(),
                                task.pkt.get_request(),
                                task.pkt.get_channel_id());
      return true;
    }

    bool TransExecutor::chandle_switch_commit_log(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      pdata.buffer.get_position() = 0;
      UPS.ups_switch_commit_log(task.pkt.get_api_version(),
                                task.pkt.get_request(),
                                task.pkt.get_channel_id(),
                                pdata.buffer);
      return true;
    }

  }
}
