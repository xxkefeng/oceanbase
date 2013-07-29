////===================================================================
 //
 // ob_sessionctx_factory.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
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

#include "common/ob_mod_define.h"
#include "ob_sessionctx_factory.h"

namespace oceanbase
{
  namespace updateserver
  {
    RWSessionCtx::RWSessionCtx(const SessionType type,
                              SessionMgr &host,
                              FIFOAllocator &fifo_allocator) : BaseSessionCtx(type, host),
                                                               CallbackMgr(),
                                                               mod_(fifo_allocator),
                                                               page_arena_(ALLOCATOR_PAGE_SIZE, mod_),
                                                               page_arena_wrapper_(page_arena_),
                                                               stat_(ST_ALIVE),
                                                               alive_flag_(true),
                                                               commit_done_(false),
                                                               is_replaying_(false),
                                                               ups_mutator_(page_arena_),
                                                               ups_result_(page_arena_wrapper_),
                                                               uc_info_(),
                                                               lock_info_(NULL),
                                                               publish_callback_list_()
    {
    }

    RWSessionCtx::~RWSessionCtx()
    {
    }

    void RWSessionCtx::end(const bool need_rollback)
    {
      if (!commit_done_)
      {
        callback(need_rollback, *this);
        if (NULL != lock_info_)
        {
          lock_info_->on_trans_end();
        }
        commit_done_ = true;
      }
    }

    void RWSessionCtx::publish()
    {
      bool rollback = false;
      publish_callback_list_.callback(rollback, *this);
    }

    int RWSessionCtx::add_publish_callback(ISessionCallback *callback, void *data)
    {
      return publish_callback_list_.add_callback_info(*this, callback, data);
    }

    void *RWSessionCtx::alloc(const int64_t size)
    {
      TBSYS_LOG(DEBUG, "session alloc %p size=%ld", this, size);
      return page_arena_.alloc(size);
    }

    void RWSessionCtx::set_is_replaying(const bool is_replaying)
    {
      is_replaying_ = is_replaying;
    }

    const bool RWSessionCtx::get_is_replaying() const
    {
      return is_replaying_;
    }

    void RWSessionCtx::reset()
    {
      ups_result_.clear();
      ups_mutator_.clear();
      stat_ = ST_ALIVE;
      alive_flag_ = true;
      commit_done_ = false;
      is_replaying_ = false;
      page_arena_.free();
      CallbackMgr::reset();
      BaseSessionCtx::reset();
      uc_info_.reset();
      lock_info_ = NULL;
      publish_callback_list_.reset();
    }

    ObUpsMutator &RWSessionCtx::get_ups_mutator()
    {
      return ups_mutator_;
    }

    TransUCInfo &RWSessionCtx::get_uc_info()
    {
      return uc_info_;
    }

    TEValueUCInfo *RWSessionCtx::alloc_tevalue_uci()
    {
      TEValueUCInfo *ret = (TEValueUCInfo*)alloc(sizeof(TEValueUCInfo));
      if (NULL != ret)
      {
        ret->reset();
      }
      return ret;
    }

    int RWSessionCtx::init_lock_info(LockMgr& lock_mgr, const IsolationLevel isolation)
    {
      int ret = OB_SUCCESS;
      if (NULL == (lock_info_ = lock_mgr.assign(isolation, *this)))
      {
        TBSYS_LOG(WARN, "assign lock_info fail");
        ret = OB_MEM_OVERFLOW;
      }
      else if (OB_SUCCESS != (ret = lock_info_->on_trans_begin()))
      {
        TBSYS_LOG(WARN, "invoke on_trans_begin fail ret=%d", ret);
      }
      return ret;
    }

    ILockInfo *RWSessionCtx::get_lock_info()
    {
      return lock_info_;
    }

    int64_t RWSessionCtx::get_min_flying_trans_id()
    {
      return get_host().get_min_flying_trans_id();
    }

    void RWSessionCtx::flush_min_flying_trans_id()
    {
      get_host().flush_min_flying_trans_id();
    }

    sql::ObUpsResult &RWSessionCtx::get_ups_result()
    {
      return ups_result_;
    }

    const bool volatile &RWSessionCtx::is_alive() const
    {
      return alive_flag_;
    }

    bool RWSessionCtx::is_killed() const
    {
      return (ST_KILLING == stat_);
    }

    void RWSessionCtx::kill()
    {
      if (ST_ALIVE != ATOMIC_CAS(&stat_, ST_ALIVE, ST_KILLING))
      {
        TBSYS_LOG(WARN, "session will not be killed sd=%u stat=%d session_start_time=%ld stmt_start_time=%ld session_timeout=%ld stmt_timeout=%ld",
                  get_session_descriptor(), stat_, get_session_start_time(), get_stmt_start_time(), get_session_timeout(), get_stmt_timeout());
      }
      else
      {
        TBSYS_LOG(INFO, "session is being killed sd=%u stat=%d session_start_time=%ld stmt_start_time=%ld session_timeout=%ld stmt_timeout=%ld",
                  get_session_descriptor(), stat_, get_session_start_time(), get_stmt_start_time(), get_session_timeout(), get_stmt_timeout());
        alive_flag_ = false;
      }
    }

    void RWSessionCtx::set_frozen()
    {
      Stat old_stat = stat_;
      stat_ = ST_FROZEN;
      if (ST_KILLING == old_stat)
      {
        TBSYS_LOG(INFO, "session has been set frozen, will not be killed, sd=%u", get_session_descriptor());
      }
    }

    bool RWSessionCtx::is_frozen() const
    {
      return (stat_ == ST_FROZEN);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    SessionCtxFactory::SessionCtxFactory() : mod_(ObModIds::OB_UPS_SESSION_CTX),
                                             allocator_(ALLOCATOR_PAGE_SIZE, mod_),
                                             ctx_allocator_()
    {
      if (OB_SUCCESS != ctx_allocator_.init(ALLOCATOR_TOTAL_LIMIT, ALLOCATOR_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE))
      {
        TBSYS_LOG(ERROR, "init allocator fail");
      }
      else
      {
        ctx_allocator_.set_mod_id(ObModIds::OB_UPS_SESSION_CTX);
      }
    }

    SessionCtxFactory::~SessionCtxFactory()
    {
    }

    BaseSessionCtx *SessionCtxFactory::alloc(const SessionType type, SessionMgr &host)
    {
      char *buffer = NULL;
      BaseSessionCtx *ret = NULL;
      switch (type)
      {
      case ST_READ_ONLY:
        buffer = allocator_.alloc(sizeof(ROSessionCtx));
        if (NULL != buffer)
        {
         ret = new(buffer) ROSessionCtx(type, host);
        }
        break;
      case ST_REPLAY:
        buffer = allocator_.alloc(sizeof(RPSessionCtx));
        if (NULL != buffer)
        {
          ret = new(buffer) RPSessionCtx(type, host, ctx_allocator_);
        }
        break;
      case ST_READ_WRITE:
        buffer = allocator_.alloc(sizeof(RWSessionCtx));
        if (NULL != buffer)
        {
          ret = new(buffer) RWSessionCtx(type, host, ctx_allocator_);
        }
        break;
      default:
        TBSYS_LOG(WARN, "invalid session type=%d", type);
        break;
      }
      return ret;
    }

    void SessionCtxFactory::free(BaseSessionCtx *ptr)
    {
      UNUSED(ptr);
    }
  }
}

