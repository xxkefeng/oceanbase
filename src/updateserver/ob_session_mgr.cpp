////===================================================================
 //
 // ob_session_mgr.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-08-20 by Yubai (yubai.lk@taobao.com) 
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

#include <algorithm>
#include "ob_session_mgr.h"
#include "ob_update_server_main.h"

namespace oceanbase
{
  using namespace common;
  namespace updateserver
  {
    CallbackMgr::CallbackMgr() : callback_list_(NULL)
    {
    }

    CallbackMgr::~CallbackMgr()
    {
    }

    void CallbackMgr::reset()
    {
      callback_list_ = NULL;
    }

    int CallbackMgr::callback(const bool rollback, BaseSessionCtx &session)
    {
      int ret = OB_SUCCESS;
      CallbackInfo *iter = callback_list_;
      while (NULL != iter)
      {
        if (NULL != iter->callback)
        {
          int tmp_ret = iter->callback->cb_func(rollback, iter->data, session);
          if (OB_SUCCESS != tmp_ret)
          {
            TBSYS_LOG(WARN, "invode callback fail ret=%d cb=%p data=%p", tmp_ret, iter->callback, iter->data);
            ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
          }
        }
        iter = iter->next;
      }
      callback_list_ = NULL;
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    MinTransIDGetter::MinTransIDGetter(SessionMgr &sm) : sm_(sm),
                                                         min_trans_id_(INT64_MAX)
    {
    }

    MinTransIDGetter::~MinTransIDGetter()
    {
    }

    void MinTransIDGetter::operator()(const uint32_t sd)
    {
      BaseSessionCtx *ctx = sm_.fetch_ctx(sd);
      if (NULL != ctx)
      {
        if (min_trans_id_ > ctx->get_trans_id())
        {
          min_trans_id_ = ctx->get_trans_id();
        }
        sm_.revert_ctx(sd);
      }
    }

    int64_t MinTransIDGetter::get_min_trans_id()
    {
      return min_trans_id_;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ZombieKiller::ZombieKiller(SessionMgr &sm,
                               const bool force) : sm_(sm),
                                                   force_(force)
    {
    }

    ZombieKiller::~ZombieKiller()
    {
    }

    void ZombieKiller::operator()(const uint32_t sd)
    {
      BaseSessionCtx *ctx = sm_.fetch_ctx(sd);
      if (NULL != ctx)
      {
        int64_t session_end_time = ctx->get_session_start_time() + ctx->get_session_timeout();
        session_end_time = (0 <= session_end_time) ? session_end_time : INT64_MAX;
        int64_t idle_end_time = ctx->get_last_active_time() + ctx->get_session_idle_time();
        idle_end_time = (0 <= idle_end_time) ? idle_end_time: INT64_MAX;
        int64_t end_time = std::min(session_end_time, idle_end_time);
        bool is_zombie = false;
        if (tbsys::CTimeUtil::getTime() > end_time
            || force_)
        {
          ctx->kill();
        }
        is_zombie = ctx->is_killed();
        sm_.revert_ctx(sd);
        if (is_zombie)
        {
          bool rollback = true;
          bool force = false;
          if (OB_SUCCESS == sm_.end_session(sd, rollback, force))
          {
            TBSYS_LOG(INFO, "session killed succ, sd=%u", sd);
          }
          else
          {
            TBSYS_LOG(INFO, "session killed fail, maybe using, sd=%u", sd);
          }
        }
      }
    }

    ShowSessions::ShowSessions(SessionMgr &sm, ObNewScanner &scanner) : sm_(sm),
                                                                        scanner_(scanner),
                                                                        row_desc_(),
                                                                        row_()
    {
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_SD);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_TYPE);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_TID);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_STIME);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_SSTIME);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_TIMEO);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_STIMEO);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_ITIME);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_ATIME);
      row_desc_.set_rowkey_cell_count(1);
      row_.set_row_desc(row_desc_);
    }

    ShowSessions::~ShowSessions()
    {
    }

    void ShowSessions::operator()(const uint32_t sd)
    {
      BaseSessionCtx *ctx = sm_.fetch_ctx(sd);
      if (NULL != ctx)
      {
        ObObj cell;
        cell.set_int(ctx->get_session_descriptor());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_SD, cell);
        cell.set_int(ctx->get_type());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_TYPE, cell);
        cell.set_int(ctx->get_trans_id());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_TID, cell);
        cell.set_int(ctx->get_session_start_time());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_STIME, cell);
        cell.set_int(ctx->get_stmt_start_time());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_SSTIME, cell);
        cell.set_int(ctx->get_session_timeout());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_TIMEO, cell);
        cell.set_int(ctx->get_stmt_timeout());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_STIMEO, cell);
        cell.set_int(ctx->get_session_idle_time());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_ITIME, cell);
        cell.set_int(ctx->get_last_active_time());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_ATIME, cell);
        TBSYS_LOG(INFO, "[session] %s", to_cstring(row_));
        scanner_.add_row(row_);
        sm_.revert_ctx(sd);
      }
    }

    ObNewScanner &ShowSessions::get_scanner()
    {
      return scanner_;
    }

    const ObRowDesc &ShowSessions::get_row_desc() const
    {
      return row_desc_;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    SessionMgr::SessionMgr() : inited_(false),
                               trans_seq_(),
                               commited_trans_id_(-1),
                               min_flying_trans_id_(-1),
                               calc_timestamp_(0),
                               ctx_map_(),
                               session_lock_()
    {
    }

    SessionMgr::~SessionMgr()
    {
      if (inited_)
      {
        destroy();
      }
    }

    int SessionMgr::init(const uint32_t max_ro_num,
                        const uint32_t max_rp_num,
                        const uint32_t max_rw_num,
                        ISessionCtxFactory *factory)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 >= max_ro_num
              || 0 >= max_rp_num
              || 0 >= max_rw_num
              || NULL == (factory_ = factory))
      {
        TBSYS_LOG(WARN, "invalid param max_ro_num=%u max_rp_num=%u max_rw_num=%u factory=%p",
                  max_ro_num, max_rp_num, max_rw_num, factory);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = ctx_map_.init(max_ro_num + max_rp_num + max_rw_num)))
      {
        TBSYS_LOG(WARN, "init ctx_map fail, ret=%d num=%u", ret, max_ro_num + max_rp_num + max_rw_num);
      }
      else
      {
        const int64_t MAX_CTX_NUM[SESSION_TYPE_NUM] = {max_ro_num, max_rp_num, max_rw_num};
        BaseSessionCtx *ctx = NULL;
        for (int i = 0; i < SESSION_TYPE_NUM; i++)
        {
          if (OB_SUCCESS != (ret = ctx_list_[i].init(MAX_CTX_NUM[i])))
          {
            TBSYS_LOG(WARN, "init ctx_list fail, ret=%d type=%d max_ctx_num=%ld", ret, i, MAX_CTX_NUM[i]);
            break;
          }
          for (int64_t j = 0; OB_SUCCESS == ret && j < MAX_CTX_NUM[i]; j++)
          {
            if (NULL == (ctx = factory_->alloc((SessionType)i, *this)))
            {
              TBSYS_LOG(WARN, "alloc ctx fail, type=%d", i);
              ret = OB_MEM_OVERFLOW;
              break;
            }
            if (OB_SUCCESS != (ret = ctx_list_[i].push(ctx)))
            {
              TBSYS_LOG(WARN, "push ctx to list fail, ret=%d ctx=%p", ret, ctx);
              break;
            }
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        inited_ = true;
      }
      else
      {
        destroy();
      }
      return ret;
    }

    void SessionMgr::destroy()
    {
      if (0 != ctx_map_.size())
      {
        TBSYS_LOG(ERROR, "some transaction have not end, counter=%d", ctx_map_.size());
      }

      ctx_map_.destroy();

      for (int i = 0; i < SESSION_TYPE_NUM; i++)
      {
        BaseSessionCtx *ctx = NULL;
        while (OB_SUCCESS == ctx_list_[i].pop(ctx))
        {
          if (NULL != ctx
              && NULL != factory_)
          {
            factory_->free(ctx);
          }
        }
        ctx_list_[i].destroy();
      }

      calc_timestamp_ = 0;
      min_flying_trans_id_ = -1;
      commited_trans_id_ = -1;
      factory_ = NULL;
      inited_ = false;
    }

    int SessionMgr::begin_session(const SessionType type, const int64_t start_time, const int64_t timeout, const int64_t idle_time,  uint32_t &session_descriptor)
    {
      int ret = OB_SUCCESS;
      SessionLockGuard guard(type, session_lock_);
      BaseSessionCtx *ctx = NULL;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == (ctx = alloc_ctx_(type)))
      {
        TBSYS_LOG(WARN, "alloc ctx fail, type=%d", type);
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        while (true)
        {
          uint32_t sd = 0;
          const int64_t begin_trans_id = commited_trans_id_;
          ctx->set_trans_id(begin_trans_id);
          ctx->set_session_start_time(start_time);
          ctx->set_session_timeout(timeout);
          ctx->set_session_idle_time(idle_time);
          if (OB_SUCCESS != (ret = ctx_map_.assign(ctx, sd)))
          {
            TBSYS_LOG(WARN, "assign from ctx_map fail, ret=%d ctx=%p type=%d", ret, ctx, type);
            free_ctx_(ctx);
            break;
          }
          if (begin_trans_id != commited_trans_id_)
          {
            ctx_map_.erase(sd);
          }
          else
          {
            ctx->set_session_descriptor(sd);
            session_descriptor = sd;
            FILL_TRACE_BUF(ctx->get_tlog_buffer(), "type=%d sd=%u ctx=%p trans_id=%ld", type, sd, ctx, begin_trans_id);
            break;
          }
        }
      }
      return ret;
    }

    int SessionMgr::precommit(const uint32_t session_descriptor)
    {
      return do_end_session(session_descriptor, false, true, false);
    }

    int SessionMgr::end_session(const uint32_t session_descriptor, const bool rollback, const bool force)
    {
      return do_end_session(session_descriptor, rollback, force, true);
    }

    int SessionMgr::do_end_session(const uint32_t session_descriptor, const bool rollback, const bool force, const bool publish)
    {
      int ret = OB_SUCCESS;
      BaseSessionCtx *ctx = NULL;
      FetchMod fetch_mod = force ? FM_MUTEX_BLOCK : FM_MUTEX_NONBLOCK;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == (ctx = ctx_map_.fetch(session_descriptor, fetch_mod)))
      {
        TBSYS_LOG(WARN, "fetch ctx fail, sd=%u rollback=%s force=%s",
                  session_descriptor, STR_BOOL(rollback), STR_BOOL(force));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (!force
              && !ctx->is_killed())
      {
        TBSYS_LOG(INFO, "end session not force, session is still alive, will not end it, sd=%u", session_descriptor);
        ctx_map_.revert(session_descriptor);
        ret = OB_UPS_TRANS_RUNNING;
      }
      else
      {
        ctx->end(rollback);
        FILL_TRACE_BUF(ctx->get_tlog_buffer(), "rollback=%s type=%d sd=%u ctx=%p trans_id=%ld trans_timeu=%ld",
                      STR_BOOL(rollback), ctx->get_type(), session_descriptor, ctx, ctx->get_trans_id(),
                      tbsys::CTimeUtil::getTime() - ctx->get_session_start_time());
        if (!rollback)
        {
          OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_COUNT, 1);
          OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_TIMEU, ctx->get_session_timeu());
        }
        if (publish)
        {
          if (ST_READ_WRITE == ctx->get_type() && !rollback)
          {
            commited_trans_id_ = ctx->get_trans_id();
            ctx->publish();
          }
          PRINT_TRACE_BUF(ctx->get_tlog_buffer());
          bool erase = true;
          ctx_map_.revert(session_descriptor, erase);
          ctx->reset();
          free_ctx_(ctx);
        }
        else
        {
          ctx->set_frozen();
          ctx_map_.revert(session_descriptor);
        }
      }
      return ret;
    }

    BaseSessionCtx *SessionMgr::fetch_ctx(const uint32_t session_descriptor)
    {
      return ctx_map_.fetch(session_descriptor);
    }

    void SessionMgr::revert_ctx(const uint32_t session_descriptor)
    {
      ctx_map_.revert(session_descriptor);
    }

    int64_t SessionMgr::get_min_flying_trans_id()
    {
      if (calc_timestamp_ + CALC_INTERVAL < tbsys::CTimeUtil::getTime())
      {
        MinTransIDGetter cb(*this);
        ctx_map_.traverse(cb);
        min_flying_trans_id_ = std::min(cb.get_min_trans_id(), (int64_t)commited_trans_id_);
        calc_timestamp_ = tbsys::CTimeUtil::getTime();
      }
      return min_flying_trans_id_;
    }

    void SessionMgr::flush_min_flying_trans_id()
    {
      MinTransIDGetter cb(*this);
      ctx_map_.traverse(cb);
      min_flying_trans_id_ = std::min(cb.get_min_trans_id(), (int64_t)commited_trans_id_);
      calc_timestamp_ = tbsys::CTimeUtil::getTime();
    }

    int SessionMgr::wait_write_session_end_and_lock(const int64_t timeout_us)
    {
      int ret = OB_SUCCESS;
      int64_t now_time = tbsys::CTimeUtil::getTime();
      int64_t abs_timeout_us = now_time + timeout_us;
      int64_t start_time = now_time;
      session_lock_.wrlock();
      while (true)
      {
        if (0 == ctx_list_[ST_REPLAY].get_free()
            && 0 == ctx_list_[ST_READ_WRITE].get_free())
        {
          break;
        }
        now_time = tbsys::CTimeUtil::getTime();
        if (now_time > abs_timeout_us)
        {
          ret = OB_PROCESS_TIMEOUT;
          break;
        }
        if (now_time >= (start_time + FORCE_KILL_WAITTIME))
        {
          start_time = now_time;
          TBSYS_LOG(INFO, "wait time over %ld, will force kill session", FORCE_KILL_WAITTIME);
          const bool force = true;
          kill_zombie_session(force);
        }
        asm("pause");
      }
      if (OB_SUCCESS != ret)
      {
        session_lock_.unlock();
      }
      return ret;
    }

    void SessionMgr::unlock_write_session()
    {
      session_lock_.unlock();
    }

    void SessionMgr::kill_zombie_session(const bool force)
    {
      TBSYS_LOG(INFO, "start kill_zombie_session force=%s", STR_BOOL(force));
      ZombieKiller cb(*this, force);
      ctx_map_.traverse(cb);
    }

    int SessionMgr::kill_session(const uint32_t session_descriptor)
    {
      int ret = OB_SUCCESS;
      BaseSessionCtx *session = fetch_ctx(session_descriptor);
      if (NULL == session)
      {
        TBSYS_LOG(WARN, "invalid session descriptor=%u", session_descriptor);
      }
      else
      {
        session->kill();
        revert_ctx(session_descriptor);
      }
      return ret;
    }

    void SessionMgr::show_sessions(ObNewScanner &scanner)
    {
      ShowSessions cb(*this, scanner);
      ctx_map_.traverse(cb);
    }

    int64_t SessionMgr::get_commited_trans_id() const
    {
      return commited_trans_id_;
    }

    BaseSessionCtx *SessionMgr::alloc_ctx_(const SessionType type)
    {
      BaseSessionCtx *ret = NULL;
      if (SESSION_TYPE_START >= type
          || SESSION_TYPE_NUM <= type)
      {
        TBSYS_LOG(WARN, "invalid session_type=%d", type);
      }
      else if (OB_SUCCESS != ctx_list_[type].pop(ret)
              || NULL == ret)
      {
        TBSYS_LOG(WARN, "alloc ctx fail, type=%d free=%ld", type, ctx_list_[type].get_total());
      }
      else
      {
        TBSYS_LOG(DEBUG, "alloc ctx succ type=%d ctx=%p", type, ret);
      }
      return ret;
    }

    void SessionMgr::free_ctx_(BaseSessionCtx *ctx)
    {
      if (NULL == ctx)
      {
        TBSYS_LOG(WARN, "ctx null pointer");
      }
      else
      {
        SessionType type = ctx->get_type();
        uint32_t sd = ctx->get_session_descriptor();
        if (SESSION_TYPE_START >= type
            || SESSION_TYPE_NUM <= type)
        {
          TBSYS_LOG(WARN, "invalid session_type=%d ctx=%p", type, ctx);
        }
        else if (OB_SUCCESS != ctx_list_[type].push(ctx))
        {
          TBSYS_LOG(WARN, "free ctx fail, ctx=%p type=%d free=%ld", ctx, type, ctx_list_[type].get_total());
        }
        else
        {
          TBSYS_LOG(DEBUG, "free ctx succ type=%d sd=%u ctx=%p", type, sd, ctx);
        }
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    void KillZombieDuty::runTimerTask()
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        TBSYS_LOG(WARN, "get ups_main fail");
      }
      else
      {
        ObUpdateServer &ups = ups_main->get_update_server();
        ups.submit_kill_zombie();
      }
    }
  }
}

