/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_pooled_allocator.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_POOLED_ALLOCATOR_H
#define _OB_POOLED_ALLOCATOR_H 1

#include "ob_pool.h"
namespace oceanbase
{
  namespace common
  {
    // @note thread-safe
    template <typename T>
    class ObPooledAllocator
    {
      public:
        ObPooledAllocator();
        virtual ~ObPooledAllocator();

        T *alloc();
        void free(T *obj);
      private:
        // disallow copy
        ObPooledAllocator(const ObPooledAllocator &other);
        ObPooledAllocator& operator=(const ObPooledAllocator &other);
      private:
        // data members
        ObLockedPool the_pool_;
    };

    template <typename T>
    ObPooledAllocator<T>::ObPooledAllocator()
      :the_pool_(sizeof(T))
    {
    }
    
    template <typename T>
    ObPooledAllocator<T>::~ObPooledAllocator()
    {
    }

    template <typename T>
    T* ObPooledAllocator<T>::alloc()
    {
      T *ret = NULL;
      void *p = the_pool_.alloc();
      if (NULL == p)
      {
        TBSYS_LOG(ERROR, "no memory");
      }
      else
      {
        ret = new(p) T();
      }
      return ret;
    }
    
    template <typename T>
    void ObPooledAllocator<T>::free(T *obj)
    {
      if (NULL != obj)
      {
        obj->~T();
        the_pool_.free(obj);
      }
    }

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_POOLED_ALLOCATOR_H */

