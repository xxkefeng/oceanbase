////===================================================================
 //
 // ob_queue_thread.cpp updateserver / Oceanbase
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

#include <errno.h>
#include "ob_queue_thread.h"

namespace oceanbase
{
  using namespace common;
  namespace updateserver
  {
    S2MCond::S2MCond() : bcond_(false)
    {
      pthread_mutex_init(&mutex_, NULL);
      pthread_cond_init(&cond_, NULL);
    }

    S2MCond::~S2MCond()
    {
      pthread_mutex_destroy(&mutex_);
      pthread_cond_destroy(&cond_);
    }

    void S2MCond::signal()
    {
      if (false == ATOMIC_CAS(&bcond_, false, true))
      {
        pthread_mutex_lock(&mutex_);
        bcond_ = true;
        pthread_cond_signal(&cond_);
        pthread_mutex_unlock(&mutex_);
      }
    }

    int S2MCond::timedwait(const int64_t time_us)
    {
      int ret = OB_SUCCESS;
      for (int64_t i = 0; i < SPIN_WAIT_NUM && !bcond_; i++)
      {
        asm("pause");
      }
      if (false == ATOMIC_CAS(&bcond_, true, false))
      {
        int64_t abs_time = time_us + tbsys::CTimeUtil::getTime();
        struct timespec ts;
        ts.tv_sec = abs_time / 1000000;
        ts.tv_nsec = (abs_time % 1000000) * 1000;
        pthread_mutex_lock(&mutex_);
        while (false == ATOMIC_CAS(&bcond_, true, false))
        {
          int tmp_ret = pthread_cond_timedwait(&cond_, &mutex_, &ts);
          if (ETIMEDOUT == tmp_ret)
          {
            ret = OB_PROCESS_TIMEOUT;
            break;
          }
        }
        pthread_mutex_unlock(&mutex_);
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    S2MQueueThread::S2MQueueThread() : thread_num_(0),
                                       thread_conf_iter_(0),
                                       thread_conf_lock_(),
                                       queued_num_(0)
    {
    }

    S2MQueueThread::~S2MQueueThread()
    {
      destroy();
    }

    int S2MQueueThread::init(const int64_t thread_num, const int64_t task_num_limit)
    {
      int ret = OB_SUCCESS;
      if (0 != thread_num_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 >= thread_num
              || MAX_THREAD_NUM < thread_num
              || 0 >= task_num_limit)
      {
        TBSYS_LOG(WARN, "invalid param, thread_num=%ld task_num_limit=%ld",
                  thread_num, task_num_limit);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = launch_thread_(thread_num, task_num_limit);
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      else
      {
        thread_conf_iter_ = 0;
      }
      return ret;
    }

    void S2MQueueThread::destroy()
    {
      for (int64_t i = 0; i < thread_num_; i++)
      {
        ThreadConf &tc = thread_conf_array_[i];
        tc.run_flag = false;
        tc.queue_cond.signal();
        pthread_join(tc.pd, NULL);
        tc.task_queue.destroy();
      }
      thread_num_ = 0;
    }

    int64_t S2MQueueThread::get_queued_num() const
    {
      return queued_num_;
    }

    int S2MQueueThread::push(void *task)
    {
      int ret = OB_SUCCESS;
      if (0 >= thread_num_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == task)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        SpinRLockGuard guard(thread_conf_lock_);
        uint64_t i = ATOMIC_INC(&thread_conf_iter_);
        ThreadConf &tc = thread_conf_array_[i % thread_num_];
        if (OB_SUCCESS != (ret = tc.task_queue.push(task)))
        {
          //TBSYS_LOG(WARN, "push task to queue fail, ret=%d", ret);
          if (OB_SIZE_OVERFLOW == ret)
          {
            ret = OB_EAGAIN;
          }
        }
        else
        {
          tc.queue_cond.signal();
        }
      }
      if (OB_SUCCESS == ret)
      {
        ATOMIC_ADD(&queued_num_, 1);
      }
      return ret;
    }

    int S2MQueueThread::push(void *task, const uint64_t task_sign)
    {
      int ret = OB_SUCCESS;
      if (0 == task_sign
          || OB_INVALID_ID == task_sign)
      {
        ret = push(task);
      }
      else if (0 >= thread_num_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == task)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        SpinRLockGuard guard(thread_conf_lock_);
        ThreadConf &tc = thread_conf_array_[task_sign % thread_num_];
        if (OB_SUCCESS != (ret = tc.task_queue.push(task)))
        {
          //TBSYS_LOG(WARN, "push task to queue fail, ret=%d", ret);
          if (OB_SIZE_OVERFLOW == ret)
          {
            ret = OB_EAGAIN;
          }
        }
        else
        {
          tc.queue_cond.signal();
        }
      }
      if (OB_SUCCESS == ret)
      {
        ATOMIC_ADD(&queued_num_, 1);
      }
      return ret;
    }

    int S2MQueueThread::add_thread(const int64_t thread_num, const int64_t task_num_limit)
    {
      int ret = OB_SUCCESS;
      if (0 >= thread_num_)
      {
        ret = OB_NOT_INIT;
      }
      else if (0 >= thread_num)
      {
        TBSYS_LOG(WARN, "invalid param, thread_num=%ld", thread_num);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        SpinWLockGuard guard(thread_conf_lock_);
        ret = launch_thread_(thread_num, task_num_limit);
      }
      return ret;
    }

    int S2MQueueThread::sub_thread(const int64_t thread_num)
    {
      int ret = OB_SUCCESS;
      int64_t prev_thread_num = 0;
      int64_t cur_thread_num = 0;
      if (0 >= thread_num_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        SpinWLockGuard guard(thread_conf_lock_);
        if (0 >= thread_num
            || thread_num >= thread_num_)
        {
          TBSYS_LOG(WARN, "invalid param, thread_num=%ld thread_num_=%ld",
                    thread_num, thread_num_);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          prev_thread_num = thread_num_;
          thread_num_ -= thread_num;
          cur_thread_num = thread_num_;
        }
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i = cur_thread_num; i < prev_thread_num; i++)
        {
          ThreadConf &tc = thread_conf_array_[i];
          tc.run_flag = false;
          tc.queue_cond.signal();
          pthread_join(tc.pd, NULL);
          tc.task_queue.destroy();
          TBSYS_LOG(INFO, "join thread succ, index=%ld", i);
        }
      }
      return ret;
    }

    void *S2MQueueThread::rebalance_(const ThreadConf &cur_thread)
    {
      void *ret = NULL;
      static __thread int64_t rebalance_counter = 0;
      for (uint64_t i = 1; (int64_t)i <= thread_num_; i++)
      {
        SpinRLockGuard guard(thread_conf_lock_);
        uint64_t balance_idx = (cur_thread.index + i) % thread_num_;
        ThreadConf &tc = thread_conf_array_[balance_idx];
        if (!tc.using_flag)
        {
          tc.task_queue.pop(ret);
        }
        if (NULL != ret)
        {
          if (0 == (rebalance_counter++ % 10000))
          {
            TBSYS_LOG(INFO, "task has been rebalance between threads rebalance_counter=%ld cur_idx=%ld balance_idx=%ld", rebalance_counter, cur_thread.index, balance_idx);
          }
          break;
        }
      }
      return ret;
    }

    int S2MQueueThread::launch_thread_(const int64_t thread_num, const int64_t task_num_limit)
    {
      int ret = OB_SUCCESS;
      int64_t thread_num2launch = std::min(MAX_THREAD_NUM - thread_num_, thread_num);
      for (int64_t i = 0; i < thread_num2launch; i++)
      {
        ThreadConf &tc = thread_conf_array_[thread_num_];
        tc.index = thread_num_;
        tc.run_flag = true;
        tc.using_flag = false;
        tc.host = this;
        if (OB_SUCCESS != (ret = tc.task_queue.init(task_num_limit)))
        {
          TBSYS_LOG(WARN, "task queue init fail, task_num_limit=%ld", task_num_limit);
          break;
        }
        int tmp_ret = 0;
        if (0 != (tmp_ret = pthread_create(&(tc.pd), NULL, thread_func_, &tc)))
        {
          TBSYS_LOG(WARN, "pthread_create fail, ret=%d", tmp_ret);
          ret = OB_ERR_UNEXPECTED;
          tc.task_queue.destroy();
          break;
        }
        TBSYS_LOG(INFO, "create thread succ, index=%ld", thread_num_);
        thread_num_ += 1;
      }
      return ret;
    }

    void *S2MQueueThread::thread_func_(void *data)
    {
      ThreadConf *const tc = (ThreadConf*)data;
      if (NULL == tc
          || NULL == tc->host)
      {
        TBSYS_LOG(WARN, "thread_func param null pointer");
      }
      else
      {
        void *pdata = tc->host->on_begin();
        while (tc->run_flag
              || 0 != tc->task_queue.get_total())
        {
          void *task = NULL;
          tc->using_flag = true;
          tc->task_queue.pop(task);
          tc->using_flag = false; // not need strict consist, so do not use barrier
          if (NULL != task
              || NULL != (task = tc->host->rebalance_(*tc)))
          {
            ATOMIC_ADD(&tc->host->queued_num_, -1);
            tc->host->handle(task, pdata);
          }
          else
          {
            tc->queue_cond.timedwait(QUEUE_WAIT_TIME);
          }
        }
        tc->host->on_end(pdata);
      }
      return NULL;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    const int64_t M2SQueueThread::QUEUE_WAIT_TIME = 100 * 1000;

    M2SQueueThread::M2SQueueThread() : inited_(false),
                                       pd_(0),
                                       run_flag_(true),
                                       queue_cond_(),
                                       task_queue_(),
                                       idle_interval_(INT64_MAX),
                                       last_idle_time_(0)
    {
    }

    M2SQueueThread::~M2SQueueThread()
    {
      destroy();
    }

    int M2SQueueThread::init(const int64_t task_num_limit,
                            const int64_t idle_interval)
    {
      int ret = OB_SUCCESS;
      int tmp_ret = 0;
      run_flag_ = true;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (OB_SUCCESS != (ret = task_queue_.init(task_num_limit)))
      {
        TBSYS_LOG(WARN, "task_queue init fail, ret=%d task_num_limit=%ld", ret, task_num_limit);
      }
      else if (0 != (tmp_ret = pthread_create(&pd_, NULL, thread_func_, this)))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "pthread_create fail, ret=%d", tmp_ret);
      }
      else
      {
        inited_ = true;
        idle_interval_ = idle_interval;
        last_idle_time_ = 0;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void M2SQueueThread::destroy()
    {
      if (0 != pd_)
      {
        run_flag_ = false;
        queue_cond_.signal();
        pthread_join(pd_, NULL);
        pd_ = 0;
      }

      task_queue_.destroy();

      inited_ = false;
    }

    int M2SQueueThread::push(void *task)
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == task)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (ret = task_queue_.push(task)))
        {
          //TBSYS_LOG(WARN, "push task to queue fail, ret=%d", ret);
          if (OB_SIZE_OVERFLOW == ret)
          {
            ret = OB_EAGAIN;
          }
        }
        else
        {
          queue_cond_.signal();
        }
      }
      return ret;
    }

    int64_t M2SQueueThread::get_queued_num() const
    {
      return task_queue_.get_total();
    }

    void *M2SQueueThread::thread_func_(void *data)
    {
      M2SQueueThread *const host = (M2SQueueThread*)data;
      if (NULL == host)
      {
        TBSYS_LOG(WARN, "thread_func param null pointer");
      }
      else
      {
        void *pdata = host->on_begin();
        while (host->run_flag_
              || 0 != host->task_queue_.get_total())
        {
          void *task = NULL;
          host->task_queue_.pop(task);
          if (NULL != task)
          {
            host->handle(task, pdata);
          }
          else if ((host->last_idle_time_ + host->idle_interval_) <= tbsys::CTimeUtil::getTime())
          {
            host->on_idle();
            host->last_idle_time_ = tbsys::CTimeUtil::getTime();
          }
          else
          {
            host->queue_cond_.timedwait(std::min(QUEUE_WAIT_TIME, host->idle_interval_));
          }
        }
        host->on_end(pdata);
      }
      return NULL;
    }

    const int64_t SeqQueueThread::QUEUE_WAIT_TIME = 100 * 1000;

    SeqQueueThread::SeqQueueThread() : inited_(false),
                                       pd_(0),
                                       run_flag_(true),
                                       idle_interval_(INT64_MAX),
                                       last_idle_time_(0)
    {
    }

    SeqQueueThread::~SeqQueueThread()
    {
      destroy();
    }

    int SeqQueueThread::init(const int64_t task_num_limit,
                            const int64_t idle_interval)
    {
      int ret = OB_SUCCESS;
      int tmp_ret = 0;
      run_flag_ = true;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (OB_SUCCESS != (ret = task_queue_.init(task_num_limit)))
      {
        TBSYS_LOG(WARN, "task_queue init fail, ret=%d task_num_limit=%ld", ret, task_num_limit);
      }
      else if (0 != (ret = task_queue_.start(1)))
      {
        TBSYS_LOG(ERROR, "task_queue_.start(1)=>%d", ret);
      }
      else if (0 != (tmp_ret = pthread_create(&pd_, NULL, thread_func_, this)))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "pthread_create fail, ret=%d", tmp_ret);
      }
      else
      {
        inited_ = true;
        idle_interval_ = idle_interval;
        last_idle_time_ = 0;
      }
      if (OB_SUCCESS != ret)
      {
        destroy();
      }
      return ret;
    }

    void SeqQueueThread::destroy()
    {
      if (0 != pd_)
      {
        run_flag_ = false;
        pthread_join(pd_, NULL);
        pd_ = 0;
      }

      inited_ = false;
    }

    int SeqQueueThread::push(void *task)
    {
      int ret = OB_SUCCESS;
      int64_t seq = 0;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == task)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= (seq = get_seq(task)))
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "get_seq(task[%p])>%ld", task, seq);
      }
      else
      {
        ret = task_queue_.add(seq, task);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "task_queue_.add(seq=%ld, task=%p)=>%d", seq, task, ret);
        }
      }
      if (OB_SUCCESS != ret)
      {
        on_push_fail(task);
      }
      return ret;
    }

    int64_t SeqQueueThread::get_queued_num() const
    {
      return task_queue_.next_is_ready()? 1: 0;
    }

    void *SeqQueueThread::thread_func_(void *data)
    {
      int err = OB_SUCCESS;
      SeqQueueThread *const host = (SeqQueueThread*)data;
      if (NULL == host)
      {
        TBSYS_LOG(WARN, "thread_func param null pointer");
      }
      else
      {
        void *pdata = host->on_begin();
        int64_t seq = 0;
        while (host->run_flag_)
        {
          void *task = NULL;
          if (OB_SUCCESS != (err = host->task_queue_.get(seq, task, QUEUE_WAIT_TIME))
            && OB_EAGAIN != err)
          {
            TBSYS_LOG(ERROR, "get(seq=%ld)=>%d", seq, err);
            break;
          }
          else if (OB_SUCCESS == err)
          {
            host->handle(task, pdata);
          }
          else if ((host->last_idle_time_ + host->idle_interval_) <= tbsys::CTimeUtil::getTime())
          {
            host->on_idle();
            host->last_idle_time_ = tbsys::CTimeUtil::getTime();
          }
        }
        host->on_end(pdata);
      }
      return NULL;
    }
  }
}

