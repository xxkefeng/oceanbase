/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_cached_allocator.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_CACHED_ALLOCATOR_H
#define _OB_CACHED_ALLOCATOR_H 1
#include "ob_pool.h"
#include "ob_spin_lock.h"
namespace oceanbase
{
  namespace common
  {
    // @note thread-safe
    template <typename T>
    class ObCachedAllocator
    {
      public:
        ObCachedAllocator();
        virtual ~ObCachedAllocator();

        T *alloc();
        void free(T *obj);
      private:
        // disallow copy
        ObCachedAllocator(const ObCachedAllocator &other);
        ObCachedAllocator& operator=(const ObCachedAllocator &other);
      private:
        // data members
        ObSpinLock lock_;
        ObPool pool_;
        ObArray<T*> cached_objs_;
    };

    template<typename T>
    ObCachedAllocator<T>::ObCachedAllocator()
      :pool_(sizeof(T))
    {
    }

    template<typename T>
    ObCachedAllocator<T>::~ObCachedAllocator()
    {
      TBSYS_LOG(DEBUG, "free cached objs, count=%ld", cached_objs_.count());
      T *obj = NULL;
      while (OB_SUCCESS == cached_objs_.pop_back(obj))
      {
        obj->~T();
        pool_.free(obj);
      }
    }

    template<typename T>
    T* ObCachedAllocator<T>::alloc()
    {
      T* ret = NULL;
      ObSpinLockGuard guard(lock_);
      if (OB_SUCCESS != cached_objs_.pop_back(ret))
      {
        void *p = pool_.alloc();
        if (NULL == p)
        {
          TBSYS_LOG(ERROR, "no memory");
        }
        else
        {
          ret = new(p) T();
        }
      }
      return ret;
    }

    template<typename T>
    void ObCachedAllocator<T>::free(T *obj)
    {
      if (NULL != obj)
      {
        int err = OB_SUCCESS;
        ObSpinLockGuard guard(lock_);
        if (OB_SUCCESS != (err = cached_objs_.push_back(obj)))
        {
          TBSYS_LOG(ERROR, "failed to push obj into array, err=%d", err);
          // free directly
          obj->~T();
          pool_.free(obj);
        }
      }
    }

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_CACHED_ALLOCATOR_H */

