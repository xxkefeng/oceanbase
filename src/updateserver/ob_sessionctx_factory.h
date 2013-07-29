////===================================================================
 //
 // ob_sessionctx_factory.h updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
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

#ifndef  OCEANBASE_UPDATESERVER_SESSIONCTX_FACTORY_H_
#define  OCEANBASE_UPDATESERVER_SESSIONCTX_FACTORY_H_
#include "common/ob_define.h"
#include "sql/ob_ups_result.h"
#include "ob_session_mgr.h"
#include "ob_ups_mutator.h"
#include "ob_table_engine.h"
#include "ob_lock_mgr.h"
#include "ob_fifo_allocator.h"

namespace oceanbase
{
  namespace updateserver
  {
    typedef BaseSessionCtx ROSessionCtx;


    struct TEValueUCInfo
    {
      TEValue *value;
      uint32_t session_descriptor;
      int16_t uc_cell_info_cnt;
      int16_t uc_cell_info_size;
      ObCellInfoNode *uc_list_head;
      ObCellInfoNode *uc_list_tail;
      TEValueUCInfo()
      {
        reset();
      };
      void reset()
      {
        value = NULL;
        session_descriptor = INVALID_SESSION_DESCRIPTOR;
        uc_cell_info_cnt = 0;
        uc_cell_info_size = 0;
        uc_list_head = NULL;
        uc_list_tail = NULL;
      };
    };

    struct TransUCInfo
    {
      int64_t uc_row_counter;
      uint64_t uc_checksum;
      MemTable *host;
      TransUCInfo()
      {
        reset();
      };
      void reset()
      {
        uc_row_counter = 0;
        uc_checksum = 0;
        host = NULL;
      };
    };

    class PageArenaWrapper : public ObIAllocator
    {
      public:
        PageArenaWrapper(common::ModuleArena &arena) : arena_(arena) {};
        ~PageArenaWrapper() {};
      public:
        void *alloc(const int64_t sz) {return arena_.alloc(sz);};
        void free(void *ptr) {arena_.free((char*)ptr);};
        void set_mod_id(int32_t mod_id) {UNUSED(mod_id);};
      private:
        common::ModuleArena &arena_;
    };

    class SessionMgr;
    class RWSessionCtx : public BaseSessionCtx, public CallbackMgr
    {
      enum Stat
      {
        // 状态转移:
        // ST_ALIVE --> ST_FROZEN
        // ST_FROZEN --> ST_FROZEN
        // ST_KILLING --> ST_FROZEN
        // ST_ALIVE --> ST_KILLING
        ST_ALIVE = 0,
        ST_FROZEN = 1,
        ST_KILLING = 2,
      };
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L;
      public:
        RWSessionCtx(const SessionType type, SessionMgr &host, FIFOAllocator &fifo_allocator);
        ~RWSessionCtx();
      public:
        void end(const bool need_rollback);
        void publish();
        void *alloc(const int64_t size);
        void reset();
        void kill();
        int add_publish_callback(ISessionCallback *callback, void *data);
      public:
        void set_is_replaying(const bool is_replaying);
        const bool get_is_replaying() const;
        ObUpsMutator &get_ups_mutator();
        TransUCInfo &get_uc_info();
        TEValueUCInfo *alloc_tevalue_uci();
        int init_lock_info(LockMgr& lock_mgr, const IsolationLevel isolation);
        ILockInfo *get_lock_info();
        int64_t get_min_flying_trans_id();
        void flush_min_flying_trans_id();
        sql::ObUpsResult &get_ups_result();
        const bool volatile &is_alive() const;
        bool is_killed() const;
        void set_frozen();
        bool is_frozen() const;
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena page_arena_;
        PageArenaWrapper page_arena_wrapper_;
        volatile Stat stat_;
        volatile bool alive_flag_;
        bool commit_done_;
        bool is_replaying_;
        ObUpsMutator ups_mutator_;
        sql::ObUpsResult ups_result_;
        TransUCInfo uc_info_;
        ILockInfo *lock_info_;
        CallbackMgr publish_callback_list_;
    };

    typedef RWSessionCtx RPSessionCtx;
    class SessionCtxFactory : public ISessionCtxFactory
    {
      static const int64_t ALLOCATOR_TOTAL_LIMIT = 5L * 1024L * 1024L * 1024L;
      static const int64_t ALLOCATOR_HOLD_LIMIT = static_cast<int64_t>(1.5 * 1024L * 1024L * 1024L); //1.5G //ALLOCATOR_TOTAL_LIMIT / 2;
      static const int64_t ALLOCATOR_PAGE_SIZE = 4L * 1024L * 1024L;
      public:
        SessionCtxFactory();
        ~SessionCtxFactory();
      public:
        BaseSessionCtx *alloc(const SessionType type, SessionMgr &host);
        void free(BaseSessionCtx *ptr);
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        FIFOAllocator ctx_allocator_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_SESSIONCTX_FACTORY_H_
