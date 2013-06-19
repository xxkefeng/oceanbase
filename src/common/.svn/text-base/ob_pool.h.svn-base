/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_simple_pool.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SIMPLE_POOL_H
#define _OB_SIMPLE_POOL_H 1
#include "ob_spin_lock.h"
#include "ob_array.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * fixed size objects pool
     * suitable for allocating & deallocating lots of objects dynamically & frequently 
     * @note not thread-safe
     * @note obj_size >= sizeof(void*)
     * @see ObLockedPool
     */
    class ObPool
    {
      public:
        ObPool(int64_t obj_size, int64_t block_size = 64*1024);
        virtual ~ObPool();

        void* alloc();
        void free(void *obj);

        uint64_t get_free_count() const;
        uint64_t get_in_use_count() const;
        uint64_t get_total_count() const;
      private:
        void* freelist_pop();
        void freelist_push(void *obj);
        void alloc_new_block();
        // disallow copy
        ObPool(const ObPool &other);
        ObPool& operator=(const ObPool &other);
      private:
        struct FreeNode
        {
          FreeNode* next_;
        };
      private:
        // data members
        int64_t obj_size_;
        int64_t block_size_;
        volatile uint64_t in_use_count_;
        volatile uint64_t free_count_;
        volatile uint64_t total_count_;
        FreeNode* freelist_;
        ObArray<void*> blocks_;
    };

    inline uint64_t ObPool::get_free_count() const
    {
      return free_count_;
    }

    inline uint64_t ObPool::get_in_use_count() const
    {
      return in_use_count_;
    }

    inline uint64_t ObPool::get_total_count() const
    {
      return total_count_;
    }
    ////////////////////////////////////////////////////////////////
    /// thread-safe pool by using spin-lock
    class ObLockedPool: public ObPool
    {
      public:
        ObLockedPool(int64_t obj_size, int64_t block_size = 64*1024);
        virtual ~ObLockedPool();

        void* alloc();
        void free(void *obj);

      private:
        // disallow copy
        ObLockedPool(const ObLockedPool &other);
        ObLockedPool& operator=(const ObLockedPool &other);
      private:
        // data members
        ObSpinLock lock_;
    };

  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_SIMPLE_POOL_H */

