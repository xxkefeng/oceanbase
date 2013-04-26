////===================================================================
 //
 // ob_trans_executor.h updateserver / Oceanbase
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

#ifndef  OCEANBASE_UPDATESERVER_TRANS_EXECUTOR_H_
#define  OCEANBASE_UPDATESERVER_TRANS_EXECUTOR_H_
#include "common/ob_define.h"
#include "common/ob_packet.h"
#include "common/ob_spin_lock.h"
#include "common/data_buffer.h"
#include "common/ob_mod_define.h"
#include "sql/ob_physical_plan.h"
#include "sql/ob_ups_result.h"
#include "ob_session_mgr.h"
#include "ob_queue_thread.h"
#include "ob_fifo_allocator.h"
#include "ob_sessionctx_factory.h"
#include "ob_lock_mgr.h"
#include "ob_util_interface.h"

namespace oceanbase
{
  namespace updateserver
  {
    class TransHandlePool : public S2MQueueThread
    {
      public:
        TransHandlePool() {};
        virtual ~TransHandlePool() {};
      public:
        void handle(void *ptask, void *pdata)
        {
          handle_trans(ptask, pdata);
        };
        void *on_begin()
        {
          return on_trans_begin();
        };
        void on_end(void *ptr)
        {
          on_trans_end(ptr);
        };
      public:
        virtual void handle_trans(void *ptask, void *pdata) = 0;
        virtual void *on_trans_begin() = 0;
        virtual void on_trans_end(void *ptr) = 0;
    };

    class TransCommitThread : public SeqQueueThread
    {
      public:
        TransCommitThread() {};
        virtual ~TransCommitThread() {};
      public:
        void handle(void *ptask, void *pdata)
        {
          handle_commit(ptask, pdata);
        };
        void *on_begin()
        {
          return on_commit_begin();
        };
        void on_end(void *ptr)
        {
          on_commit_end(ptr);
        };
        void on_idle()
        {
          on_commit_idle();
        };
        void on_push_fail(void* task)
        {
          on_commit_push_fail(task);
        }
          
      public:
        virtual void handle_commit(void *ptask, void *pdata) = 0;
        virtual void *on_commit_begin() = 0;
        virtual void on_commit_end(void *ptr) = 0;
        virtual void on_commit_push_fail(void* ptr) = 0;
        virtual void on_commit_idle() = 0;
        virtual int64_t get_seq(void* task) = 0;
    };

    class TransExecutor : public TransHandlePool, public TransCommitThread
    {
      struct TransParamData
      {
        TransParamData() : mod(common::ObModIds::OB_UPS_PHYPLAN_ALLOCATOR),
                           allocator(2 * OB_MAX_PACKET_LENGTH, mod)
                           //allocator(common::ModuleArena::DEFAULT_PAGE_SIZE, mod)
        {
        };
        common::ObMutator mutator;
        common::ObGetParam get_param;
        common::ObScanParam scan_param;
        common::ObScanner scanner;
        common::ObCellNewScanner new_scanner;
        sql::ObPhysicalPlan phy_plan;
        common::ModulePageAllocator mod;
        common::ModuleArena allocator;
        char cbuffer[OB_MAX_PACKET_LENGTH];
        common::ObDataBuffer buffer;
      };
      struct CommitParamData
      {
        char cbuffer[OB_MAX_PACKET_LENGTH];
        common::ObDataBuffer buffer;
      };
      struct Task
      {
        common::ObPacket pkt;
        ObTransID sid;
        easy_addr_t src_addr;
        void reset()
        {
          sid.reset();
        };
      };
      static const int64_t TASK_QUEUE_LIMIT = 100000;
      static const int64_t FINISH_THREAD_IDLE = 5000;
      static const int64_t ALLOCATOR_TOTAL_LIMIT = 1L * 1024L * 1024L * 1024L;
      static const int64_t ALLOCATOR_HOLD_LIMIT = ALLOCATOR_TOTAL_LIMIT / 2;
      static const int64_t ALLOCATOR_PAGE_SIZE = 4L * 1024L * 1024L;
      static const int64_t MAX_RO_NUM = 100000;
      static const int64_t MAX_RP_NUM = 10000;
      static const int64_t MAX_RW_NUM = 20000;
      static const int64_t QUERY_TIMEOUT_RESERVE = 50000;
      static const int64_t TRY_FREEZE_INTERVAL = 1000000;
      static const int64_t MAX_BATCH_NUM = 500;
      typedef void (*packet_handler_pt)(common::ObPacket &pkt, common::ObDataBuffer &buffer);
      typedef bool (*trans_handler_pt)(TransExecutor &host, Task &task, TransParamData &pdata);
      typedef bool (*commit_handler_pt)(TransExecutor &host, Task &task, CommitParamData &pdata);
      public:
        TransExecutor(ObUtilInterface &ui);
        ~TransExecutor();
      public:
        int init(const int64_t thread_num);
        void destroy();
      public:
        void handle_packet(common::ObPacket &pkt);

        void handle_trans(void *ptask, void *pdata);
        void *on_trans_begin();
        void on_trans_end(void *ptr);

        void handle_commit(void *ptask, void *pdata);
        void *on_commit_begin();
        void on_commit_push_fail(void* ptr);
        void on_commit_end(void *ptr);
        void on_commit_idle();
        int64_t get_seq(void* ptr);

        SessionMgr &get_session_mgr() {return session_mgr_;};
        LockMgr &get_lock_mgr() {return lock_mgr_;};
        void log_trans_info() const;
        int &thread_errno();
        int64_t &batch_start_time();
      private:
        bool handle_in_situ_(const int pcode);
        int push_task_(Task &task);
        bool wait_for_commit_(const int pcode);

        int get_session_type(const ObTransID& sid, SessionType& type);
        void handle_start_session_(Task &task, common::ObDataBuffer &buffer);
        bool handle_end_session_(Task &task, common::ObDataBuffer &buffer);
        bool handle_write_trans_(Task &task, common::ObMutator &mutator, common::ObNewScanner &scanner);
        bool handle_phyplan_trans_(Task &task,
                                  sql::ObPhysicalPlan &phy_plan,
                                  common::ObNewScanner &new_scanner,
                                  common::ModuleArena &allocator,
                                  ObDataBuffer& buffer);
        void handle_get_trans_(common::ObPacket &pkt,
                              common::ObGetParam &get_param,
                              common::ObScanner &scanner,
                              common::ObCellNewScanner &new_scanner,
                              common::ObDataBuffer &buffer);
        void handle_scan_trans_(common::ObPacket &pkt,
                                common::ObScanParam &scan_param,
                                common::ObScanner &scanner,
                                common::ObCellNewScanner &new_scanner,
                                common::ObDataBuffer &buffer);
        void handle_kill_zombie_();
        void handle_show_sessions_(common::ObPacket &pkt,
                                  common::ObNewScanner &scanner,
                                  common::ObDataBuffer &buffer);
        void handle_kill_session_(ObPacket &pkt);
        int fill_return_rows_(sql::ObPhyOperator &phy_op, common::ObNewScanner &new_scanner, sql::ObUpsResult &ups_result);
        void reset_warning_strings_();
        void fill_warning_strings_(sql::ObUpsResult &ups_result);

        int handle_write_commit_(Task &task);
        int fill_log_(Task &task, RWSessionCtx &session_ctx);
        int commit_log_();
        void try_submit_auto_freeze_();
      private:
        static void phandle_non_impl(common::ObPacket &pkt, ObDataBuffer &buffer);
        static void phandle_freeze_memtable(common::ObPacket &pkt, ObDataBuffer &buffer);
        static void phandle_clear_active_memtable(common::ObPacket &pkt, ObDataBuffer &buffer);
        static void phandle_check_cur_version(common::ObPacket &pkt, ObDataBuffer &buffer);
      private:
        static bool thandle_non_impl(TransExecutor &host, Task &task, TransParamData &pdata);
        static bool thandle_scan_trans(TransExecutor &host, Task &task, TransParamData &pdata);
        static bool thandle_get_trans(TransExecutor &host, Task &task, TransParamData &pdata);
        static bool thandle_write_trans(TransExecutor &host, Task &task, TransParamData &pdata);
        static bool thandle_start_session(TransExecutor &host, Task &task, TransParamData &pdata);
        static bool thandle_kill_zombie(TransExecutor &host, Task &task, TransParamData &pdata);
        static bool thandle_show_sessions(TransExecutor &host, Task &task, TransParamData &pdata);
        static bool thandle_kill_session(TransExecutor &host, Task &task, TransParamData &pdata);
        static bool thandle_end_session(TransExecutor &host, Task &task, TransParamData &pdata);
      private:
        static bool chandle_non_impl(TransExecutor &host, Task &task, CommitParamData &pdata);
        static bool chandle_write_commit(TransExecutor &host, Task &task, CommitParamData &data);
        static bool chandle_fake_write_for_keep_alive(TransExecutor &host, Task &task, CommitParamData &pdata);
        static bool chandle_send_log(TransExecutor &host, Task &task, CommitParamData &pdata);
        static bool chandle_slave_reg(TransExecutor &host, Task &task, CommitParamData &pdata);
        static bool chandle_switch_schema(TransExecutor &host, Task &task, CommitParamData &pdata);
        static bool chandle_force_fetch_schema(TransExecutor &host, Task &task, CommitParamData &pdata);
        static bool chandle_switch_commit_log(TransExecutor &host, Task &task, CommitParamData &pdata);
      private:
        ObUtilInterface &ui_;
        common::ThreadSpecificBuffer my_thread_buffer_;
        packet_handler_pt packet_handler_[common::OB_PACKET_NUM];
        trans_handler_pt trans_handler_[common::OB_PACKET_NUM];
        commit_handler_pt commit_handler_[common::OB_PACKET_NUM];

        FIFOAllocator allocator_;
        SessionCtxFactory session_ctx_factory_;
        SessionMgr session_mgr_;
        LockMgr lock_mgr_;
        ObSpinLock write_clog_mutex_;

        common::ObList<Task*> uncommited_session_list_;
        char ups_result_memory_[OB_MAX_PACKET_LENGTH];
        common::ObDataBuffer ups_result_buffer_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_TRANS_EXECUTOR_H_

