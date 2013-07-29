////===================================================================
 //
 // ob_session_mgr.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-08-19 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_SESSION_MGR_H_
#define  OCEANBASE_UPDATESERVER_SESSION_MGR_H_
#include "common/ob_define.h"
#include "common/page_arena.h"
#include "common/ob_fixed_queue.h"
#include "common/ob_list.h"
#include "common/ob_trace_log.h"
#include "common/ob_timer.h"
#include "common/ob_new_scanner.h"
#include "common/ob_transaction.h"
#include "common/ob_spin_lock.h"
#include "ob_id_map.h"
#include "ob_ups_mutator.h"
#include "ob_inc_seq.h"

#define INVALID_SESSION_DESCRIPTOR IDMAP_INVALID_ID
namespace oceanbase
{
  namespace updateserver
  {
    class BaseSessionCtx;
    class ObTriggerHandler;
    class LockMgr;
    class ISessionCallback
    {
      public:
        virtual ~ISessionCallback() {};
      public:
        virtual int cb_func(const bool rollback, void *data, BaseSessionCtx &session) = 0;
    };

    enum SessionType
    {
      SESSION_TYPE_START = -1,
      ST_READ_ONLY = 0,
      ST_REPLAY = 1,
      ST_READ_WRITE = 2,
      SESSION_TYPE_NUM,
    };

    class SessionMgr;
    class ISessionCtxFactory
    {
      public:
        virtual ~ISessionCtxFactory() {};
      public:
        virtual BaseSessionCtx *alloc(const SessionType type, SessionMgr &host) = 0;
        virtual void free(BaseSessionCtx *ptr) = 0;
    };

    class BaseSessionCtx
    {
      public:
        BaseSessionCtx(const SessionType type,
                       SessionMgr &host) : type_(type),
                                           host_(host)

        {
          reset();
        };
        virtual ~BaseSessionCtx() {};
      public:
        virtual void end(const bool need_rollback)
        {
          UNUSED(need_rollback);
        };
        virtual void publish()
        {
          // do nothing
        };
        virtual void reset()
        {
          trans_id_ = INT64_MAX;
          session_start_time_ = 0;
          stmt_start_time_ = 0;
          session_timeout_ = INT64_MAX;
          stmt_timeout_ = INT64_MAX;
          session_descriptor_ = UINT32_MAX;
          CLEAR_TRACE_BUF(tlog_buffer_);
        };
        virtual void kill()
        {
          // do nothing;
        };
        virtual bool is_killed() const
        {
          return false;
        };
        virtual int init_lock_info(LockMgr &lock_mgr, const common::IsolationLevel isolation)
        {
          UNUSED(lock_mgr);
          UNUSED(isolation);
          return common::OB_SUCCESS;
        };
        virtual int add_publish_callback(ISessionCallback *callback, void *data)
        {
          UNUSED(callback);
          UNUSED(data);
          return common::OB_SUCCESS;
        };
      public:
        SessionType get_type() const
        {
          return type_;
        };

        virtual void set_frozen(){}
        
        int64_t get_trans_id() const
        {
          return trans_id_;
        };
        void set_trans_id(const int64_t trans_id)
        {
          trans_id_ = trans_id;
        };
        
        int64_t get_session_start_time() const
        {
          return session_start_time_;
        };
        void set_session_start_time(const int64_t session_start_time)
        {
          session_start_time_ = session_start_time;
          last_active_time_ = session_start_time;;
        };
        int64_t get_session_timeu() const
        {
          return tbsys::CTimeUtil::getTime() - session_start_time_;
        };

        int64_t get_stmt_start_time() const
        {
          return stmt_start_time_;
        };
        void set_stmt_start_time(const int64_t stmt_start_time)
        {
          stmt_start_time_ = stmt_start_time;
          last_active_time_ = stmt_start_time;;
        };

        int64_t get_session_timeout() const
        {
          return session_timeout_;
        };
        void set_session_timeout(const int64_t timeout)
        {
          session_timeout_ = timeout;
        };

        bool is_session_expired() const
        {
          int64_t cur_time = tbsys::CTimeUtil::getTime();
          bool bret = ((cur_time - session_start_time_) > session_timeout_
                      || (cur_time - last_active_time_) > session_idle_time_);
          if (bret)
          {
            TBSYS_LOG(WARN, "session expired, sd=%u ctx=%p cur_time=%ld "
                      "session_start_time=%ld session_timeout=%ld "
                      "last_active_time=%ld session_idle_time=%ld",
                      session_descriptor_, this, cur_time,
                      session_start_time_, session_timeout_,
                      last_active_time_, session_idle_time_);
          }
          return bret;
        };
        
        int64_t get_stmt_timeout() const
        {
          return stmt_timeout_;
        };
        void set_stmt_timeout(const int64_t timeout)
        {
          stmt_timeout_ = timeout;
        };

        int64_t get_session_idle_time() const
        {
          return session_idle_time_;
        };
        void set_session_idle_time(const int64_t time)
        {
          session_idle_time_ = time;
        };
        
        int64_t get_last_active_time() const
        {
          return last_active_time_;
        };
        void set_last_active_time(const int64_t time)
        {
          last_active_time_ = time;
        };

        uint32_t get_session_descriptor() const
        {
          return session_descriptor_;
        };
        void set_session_descriptor(const uint32_t session_descriptor)
        {
          session_descriptor_ = session_descriptor;
        };

        SessionMgr &get_host()
        {
          return host_;
        };
        common::TraceLog::LogBuffer& get_tlog_buffer() const
        {
          return tlog_buffer_;
        };

        void lock_session()
        {
          session_lock_.lock();
        };
        void unlock_session()
        {
          session_lock_.unlock();
        };

      private:
        const SessionType type_;
        SessionMgr &host_;
        int64_t trans_id_;
        int64_t session_start_time_;
        int64_t stmt_start_time_;
        int64_t session_timeout_;
        int64_t stmt_timeout_;
        int64_t session_idle_time_;
        int64_t last_active_time_;
        uint32_t session_descriptor_;
        mutable common::TraceLog::LogBuffer tlog_buffer_;
        common::ObSpinLock session_lock_;
    };

    class CallbackMgr
    {
      struct CallbackInfo
      {
        ISessionCallback *callback;
        void *data;
        CallbackInfo *next;
      };
      public:
        CallbackMgr();
        ~CallbackMgr();
      public:
        template <typename Allocator>
        int add_callback_info(Allocator &allocator, ISessionCallback *callback, void *data);
        void reset();
        int callback(const bool rollback, BaseSessionCtx &session);
      private:
        CallbackInfo *callback_list_;
    };

    class SessionMgr;
    class MinTransIDGetter
    {
      public:
        MinTransIDGetter(SessionMgr &sm);
        ~MinTransIDGetter();
        void operator()(const uint32_t sd);
        int64_t get_min_trans_id();
      private:
        SessionMgr &sm_;
        int64_t min_trans_id_;
    };

    class ZombieKiller
    {
      public:
        ZombieKiller(SessionMgr &sm, const bool force);
        ~ZombieKiller();
        void operator()(const uint32_t sd);
      private:
        SessionMgr &sm_;
        const bool force_;
    };

    class ShowSessions
    {
      public:
        static const uint64_t SESSION_TABLE_ID = 100;
        static const uint64_t SESSION_COL_ID_TYPE = 16;   // type
        static const uint64_t SESSION_COL_ID_SD = 17;     // session descriptor
        static const uint64_t SESSION_COL_ID_TID = 18;    // trans id
        static const uint64_t SESSION_COL_ID_STIME = 19;  // session start time
        static const uint64_t SESSION_COL_ID_SSTIME = 20; // stmt start time
        static const uint64_t SESSION_COL_ID_TIMEO = 21;  // session timeout
        static const uint64_t SESSION_COL_ID_STIMEO = 22; // stmt timeout
        static const uint64_t SESSION_COL_ID_ITIME = 23; // session idle time
        static const uint64_t SESSION_COL_ID_ATIME = 24; // session last active time
      public:
        ShowSessions(SessionMgr &sm, common::ObNewScanner &scanner);
        ~ShowSessions();
        void operator()(const uint32_t sd);
        common::ObNewScanner &get_scanner();
        const common::ObRowDesc &get_row_desc() const;
      private:
        SessionMgr &sm_;
        common::ObNewScanner &scanner_;
        common::ObRowDesc row_desc_;
        common::ObRow row_;
    };

    class SessionLockGuard
    {
      public:
        SessionLockGuard(const SessionType type,
                        common::SpinRWLock &lock) : lock_(NULL),
                                                    lock_succ_(false)
        {
          if (ST_REPLAY == type)
          {
            lock.rdlock();
            lock_ = &lock;
            lock_succ_ = true;
          }
          else if (ST_READ_WRITE == type)
          {
            if (lock.try_rdlock())
            {
              lock_ = &lock;
              lock_succ_ = true;
            }
          }
          else
          {
            lock_succ_ = true;
          }
        };
        ~SessionLockGuard()
        {
          if (NULL != lock_)
          {
            lock_->unlock();
          }
        };
        bool is_lock_succ() const
        {
          return lock_succ_;
        };
      private:
        common::SpinRWLock *lock_;
        bool lock_succ_;
    };

    class SessionMgr
    {
      typedef common::ObFixedQueue<BaseSessionCtx> SessionCtxList;
      typedef ObIDMap<BaseSessionCtx, uint32_t> SessionCtxMap;
      public:
        static const int64_t CALC_INTERVAL = 10000;
        static const int64_t FORCE_KILL_WAITTIME = 100000;

      public:
        SessionMgr();
        ~SessionMgr();

      public:
        int init(const uint32_t max_ro_num,
                const uint32_t max_rp_num,
                const uint32_t max_rw_num,
                ISessionCtxFactory *factory);
        
        void destroy();

      public:
        int begin_session(const SessionType type, const int64_t start_time, const int64_t timeout, const int64_t idle_time, uint32_t &session_descriptor);
        int precommit(const uint32_t session_descriptor);
        int end_session(const uint32_t session_descriptor, const bool rollback = true, const bool force = true);

        template <class CTX>
        CTX *fetch_ctx(const uint32_t session_descriptor);
        BaseSessionCtx *fetch_ctx(const uint32_t session_descriptor);
        void revert_ctx(const uint32_t session_descriptor);

        int64_t get_min_flying_trans_id();
        void flush_min_flying_trans_id();
        int wait_write_session_end_and_lock(const int64_t timeout_us);
        void unlock_write_session();
        void kill_zombie_session(const bool force);
        int kill_session(const uint32_t session_descriptor);
        void show_sessions(common::ObNewScanner &scanner);
        int64_t get_commited_trans_id() const;
        int64_t get_flying_rosession_num() const {return ctx_list_[ST_READ_ONLY].get_free();};
        int64_t get_flying_rpsession_num() const {return ctx_list_[ST_REPLAY].get_free();};
        int64_t get_flying_rwsession_num() const {return ctx_list_[ST_READ_WRITE].get_free();};
        IncSeq& get_trans_seq() { return trans_seq_; }

    protected:
        int do_end_session(const uint32_t session_descriptor, const bool rollback = true, const bool force = true, const bool publish = true);
      private:
        BaseSessionCtx *alloc_ctx_(const SessionType type);
        void free_ctx_(BaseSessionCtx *ctx);

      private:
        bool inited_;
        ISessionCtxFactory *factory_;
        IncSeq trans_seq_;
        volatile int64_t commited_trans_id_;
        mutable volatile int64_t min_flying_trans_id_;
        mutable volatile int64_t calc_timestamp_;

        SessionCtxList ctx_list_[SESSION_TYPE_NUM];
        SessionCtxMap ctx_map_;

        common::SpinRWLock session_lock_;
    };

    template <class CTX>
    CTX *SessionMgr::fetch_ctx(const uint32_t session_descriptor)
    {
      CTX *ret = NULL;
      BaseSessionCtx *ctx = fetch_ctx(session_descriptor);
      if (NULL != ctx)
      {
        ret = dynamic_cast<CTX*>(ctx);
        if (NULL == ret)
        {
          revert_ctx(session_descriptor);
        }
      }
      return ret;
    }

    template <typename Allocator>
    int CallbackMgr::add_callback_info(Allocator &allocator, ISessionCallback *callback, void *data)
    {
      int ret = common::OB_SUCCESS;
      CallbackInfo *cbi = NULL;
      if (NULL == callback)
      {
        ret = common::OB_INVALID_ARGUMENT;
      }
      else if (NULL == (cbi = (CallbackInfo*)allocator.alloc(sizeof(CallbackInfo))))
      {
        ret = common::OB_MEM_OVERFLOW;
      }
      else
      {
        cbi->callback = callback;
        cbi->data = data;
        cbi->next = callback_list_;
        callback_list_ = cbi;
      }
      return ret;
    }

    class KillZombieDuty : public common::ObTimerTask
    {
      public:
        static const int64_t SCHEDULE_PERIOD = 1L * 1000000L;
      public:
        KillZombieDuty() {};
        virtual ~KillZombieDuty() {};
        virtual void runTimerTask();
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_SESSION_MGR_H_

