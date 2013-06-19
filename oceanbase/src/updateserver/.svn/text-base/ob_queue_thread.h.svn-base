////===================================================================
 //
 // ob_queue_thread.h updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-09-01 by Yubai (yubai.lk@taobao.com) 
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

#ifndef  OCEANBASE_UPDATESERVER_QUEUE_THREAD_H_
#define  OCEANBASE_UPDATESERVER_QUEUE_THREAD_H_
#include <sys/epoll.h>
#include "common/ob_define.h"
#include "common/ob_fixed_queue.h"
#include "common/page_arena.h"
#include "common/ob_spin_rwlock.h"
#include "common/ob_seq_queue.h"

#define CPU_CACHE_LINE 64

namespace oceanbase
{
  namespace updateserver
  {
    class S2MCond
    {
      static const int64_t SPIN_WAIT_NUM = 20000;
      public:
        S2MCond();
        ~S2MCond();
      public:
        inline void signal();
        inline int timedwait(const int64_t time_us);
      private:
        volatile bool bcond_;
        pthread_cond_t cond_;
        pthread_mutex_t mutex_;
    } __attribute__ ((aligned (CPU_CACHE_LINE)));

    class S2MQueueThread
    {
      struct ThreadConf
      {
        pthread_t pd;
        uint64_t index;
        volatile bool run_flag;
        S2MCond queue_cond;
        volatile bool using_flag;
        common::ObFixedQueue<void> task_queue;
        S2MQueueThread *host;
        ThreadConf() : pd(0),
                       index(0),
                       run_flag(true),
                       queue_cond(),
                       using_flag(false),
                       task_queue(),
                       host(NULL)
        {
        };
      } __attribute__ ((aligned (CPU_CACHE_LINE)));
      static const int64_t QUEUE_WAIT_TIME = 100 * 1000;
      static const int64_t MAX_THREAD_NUM = 256;;
      public:
        S2MQueueThread();
        virtual ~S2MQueueThread();
      public:
        int init(const int64_t thread_num, const int64_t task_num_limit);
        void destroy();
        int64_t get_queued_num() const;
        int add_thread(const int64_t thread_num, const int64_t task_num_limit);
        int sub_thread(const int64_t thread_num);
        int64_t get_thread_num() const {return thread_num_;};
      public:
        int push(void *task);
        int push(void *task, const uint64_t task_sign);
        virtual void handle(void *task, void *pdata) = 0;
        virtual void *on_begin() {return NULL;};
        virtual void on_end(void *ptr) {UNUSED(ptr);};
      private:
        void *rebalance_(const ThreadConf &cur_thread);
        int launch_thread_(const int64_t thread_num, const int64_t task_num_limit);
        static void *thread_func_(void *data);
      private:
        int64_t thread_num_;
        volatile uint64_t thread_conf_iter_;
        common::SpinRWLock thread_conf_lock_;
        ThreadConf thread_conf_array_[MAX_THREAD_NUM];
        volatile int64_t queued_num_;
    };

    typedef S2MCond M2SCond;

    class M2SQueueThread
    {
      static const int64_t QUEUE_WAIT_TIME;
      public:
        M2SQueueThread();
        virtual ~M2SQueueThread();
      public:
        int init(const int64_t task_num_limit,
                const int64_t idle_interval);
        void destroy();
      public:
        int push(void *task);
        int64_t get_queued_num() const;
        virtual void handle(void *task, void *pdata) = 0;
        virtual void *on_begin() {return NULL;};
        virtual void on_end(void *ptr) {UNUSED(ptr);};
        virtual void on_idle() {};
      private:
        static void *thread_func_(void *data);
      private:
        bool inited_;
        pthread_t pd_;
        volatile bool run_flag_;
        M2SCond queue_cond_;
        common::ObFixedQueue<void> task_queue_;
        int64_t idle_interval_;
        int64_t last_idle_time_;
    } __attribute__ ((aligned (CPU_CACHE_LINE)));

    class SeqQueueThread
    {
      static const int64_t QUEUE_WAIT_TIME;
      public:
        SeqQueueThread();
        virtual ~SeqQueueThread();
      public:
        int init(const int64_t task_num_limit,
                const int64_t idle_interval);
        void destroy();
      public:
        virtual int push(void *task);
        int64_t get_queued_num() const;
        virtual void handle(void *task, void *pdata) = 0;
        virtual void on_push_fail(void *ptr) {UNUSED(ptr);};
        virtual void *on_begin() {return NULL;};
        virtual void on_end(void *ptr) {UNUSED(ptr);};
        virtual void on_idle() {};
        virtual int64_t get_seq(void* task) = 0;
      private:
        static void *thread_func_(void *data);
      private:
        bool inited_;
        pthread_t pd_;
        volatile bool run_flag_;
        common::ObSeqQueue task_queue_;
        int64_t idle_interval_;
        int64_t last_idle_time_;
    } __attribute__ ((aligned (CPU_CACHE_LINE)));
  }
}

#endif //OCEANBASE_UPDATESERVER_QUEUE_THREAD_H_

