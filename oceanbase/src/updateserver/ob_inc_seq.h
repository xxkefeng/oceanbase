/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_UPDATESERVER_OB_INC_SEQ_H__
#define __OB_UPDATESERVER_OB_INC_SEQ_H__
#include "string.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace updateserver
  {
    class SeqLock
    {
      struct Item
      {
        uint64_t value_;
      } CACHE_ALIGNED;
      public:
        static const uint64_t N_THREAD = 128;
        static const uint64_t N_THREAD_MASK = N_THREAD - 1;
        Item done_seq_[N_THREAD];
        uint64_t do_seq_ CACHE_ALIGNED;
        SeqLock(): do_seq_(0)
        {
          memset(done_seq_, 0, sizeof(done_seq_));
          __sync_synchronize();
        }
        ~SeqLock() {}
        int64_t lock()
        {
          uint64_t seq = __sync_add_and_fetch(&do_seq_, 1);
          volatile uint64_t& last_done_seq = done_seq_[(seq-1) & N_THREAD_MASK].value_;
          while(last_done_seq < seq - 1)
          {
            PAUSE() ;
          }
          __sync_synchronize();
          return seq;
        }

        int64_t unlock(int64_t seq)
        {
          volatile uint64_t& this_done_seq = done_seq_[seq & N_THREAD_MASK].value_;
          __sync_synchronize();
          this_done_seq = seq;
          return seq;
        }
    } CACHE_ALIGNED;

    struct RWLock
    {
      const static int64_t N_THREAD = 128;
      volatile int64_t read_ref_[N_THREAD][CACHE_ALIGN_SIZE/sizeof(int64_t)];
      volatile uint64_t thread_num_;
      volatile uint64_t write_uid_ CACHE_ALIGNED;
      pthread_key_t key_ CACHE_ALIGNED;
      RWLock()
      {
        write_uid_ = 0;
        memset((void*)read_ref_, 0, sizeof(read_ref_));
        pthread_key_create(&key_, NULL);
      }
      ~RWLock()
      {
        pthread_key_delete(key_);
      }
      void rdlock()
      {
        volatile int64_t* ref = (volatile int64_t*)pthread_getspecific(key_);
        if (NULL == ref)
        {
          pthread_setspecific(key_, (void*)(ref = read_ref_[__sync_fetch_and_add(&thread_num_, 1) % N_THREAD]));
        }
        while(true)
        {
          if (0 == write_uid_)
          {
            ref++;
            __sync_synchronize();
            if (0 == write_uid_)
            {
              break;
            }
            ref--;
          }
          PAUSE();
        }
      }
      void rdunlock()
      {
        int64_t* ref = (int64_t*)pthread_getspecific(key_);
        __sync_synchronize();
        (*ref)--;
      }
      void wrlock()
      {
        while(!__sync_bool_compare_and_swap(&write_uid_, 0, 1))
          ;
        for(uint64_t i = 0; i < thread_num_; i++)
        {
          while(*read_ref_[i] > 0)
            ;
        }
        __sync_synchronize();
      }
      void wrunlock()
      {
        __sync_synchronize();
        write_uid_ = 0;
      }
    } CACHE_ALIGNED;

    class IncSeq
    {
      private:
        RWLock rwlock_;
        SeqLock seq_;
        volatile int64_t value_ CACHE_ALIGNED;
      public:
        IncSeq(): seq_(), value_(0){}
        ~IncSeq(){}
        void update(int64_t& value)
        {
          rwlock_.wrlock();
          if(value_ < value)
          {
            value_ = value;
          }
          rwlock_.wrunlock();
        }

        int64_t next(int64_t& value)
        {
          int64_t seq = seq_.lock();
          rwlock_.rdlock();
          if (value > value_)
          {
            value_ = value;
          }
          else
          {
            value = ++value_;
          }
          rwlock_.rdunlock();
          return seq_.unlock(seq);
        }
    } CACHE_ALIGNED;
  }; // end namespace updateserver
}; // end namespace oceanbase

#endif /* __OB_UPDATESERVER_OB_INC_SEQ_H__ */

