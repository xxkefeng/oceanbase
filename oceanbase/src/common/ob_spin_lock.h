/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_spin_lock.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SPIN_LOCK_H
#define _OB_SPIN_LOCK_H 1
#include <pthread.h>
#include "tbsys.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * A simple wrapper of pthread spin lock
     * 
     */
    class ObSpinLock
    {
      public:
        ObSpinLock();
        ~ObSpinLock();
        int lock();
        int unlock();
      private:
        // disallow copy
        ObSpinLock(const ObSpinLock &other);
        ObSpinLock& operator=(const ObSpinLock &other);
      private:
        // data members
        pthread_spinlock_t lock_;
    };

    inline ObSpinLock::ObSpinLock()
    {
      int ret = pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
      if (0 != ret)
      {
        TBSYS_LOG(ERROR, "init spin lock failed, err=%s", strerror(ret));
      }
    }

    inline ObSpinLock::~ObSpinLock()
    {
      pthread_spin_destroy(&lock_);
    }

    inline int ObSpinLock::lock()
    {
      return pthread_spin_lock(&lock_);
    }
    
    inline int ObSpinLock::unlock()
    {
      return pthread_spin_unlock(&lock_);
    }

    ////////////////////////////////////////////////////////////////
    class ObSpinLockGuard
    {
      public:
        ObSpinLockGuard(ObSpinLock &lock);
        ~ObSpinLockGuard();
      private:
        // disallow copy
        ObSpinLockGuard(const ObSpinLockGuard &other);
        ObSpinLockGuard& operator=(const ObSpinLockGuard &other);
        // disallow new
        void* operator new (std::size_t size) throw (std::bad_alloc);
        void* operator new (std::size_t size, const std::nothrow_t& nothrow_constant) throw();
        void* operator new (std::size_t size, void* ptr) throw();
      private:
        // data members
        ObSpinLock &lock_;
    };

    inline ObSpinLockGuard::ObSpinLockGuard(ObSpinLock &lock)
      :lock_(lock)
    {
      int ret = lock_.lock();
      if (0 != ret)
      {
        TBSYS_LOG(ERROR, "failed to lock spinlock, err=%s", strerror(ret));
      }
    }

    inline ObSpinLockGuard::~ObSpinLockGuard()
    {
      lock_.unlock();
    }

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_SPIN_LOCK_H */

