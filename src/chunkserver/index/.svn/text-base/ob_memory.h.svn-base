/**
 * (C) 2010-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 *
 * Authors:
 *   huating <huating.zmq@alipay.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_MEMORY_H_
#define OCEANBASE_CHUNKSERVER_MEMORY_H_

#include "common/ob_tc_malloc.h"
#include "common/ob_tsi_factory.h"

namespace oceanbase
{
  namespace common 
  {
    template <typename Type>
    class ObSingleton 
    {
      public:
        // Return a pointer to the one true instance of the class.
        static Type* get() 
        {
          once_init();
          return instance_;
        }

      private:
        // Create the instance.
        static void init() 
        {
          instance_ = create_instance();
        }

        // Create and return the instance. You can use Singleton for objects which
        // require more complex setup by defining a specialization for your type.
        static Type* create_instance() 
        {
          // use ::new to work around a gcc bug when operator new is overloaded
          return ::new Type;
        }

        static void once_init()
        {
          while(once_ < 2)
          {
            if (__sync_bool_compare_and_swap(&once_, 0, 1))
            {
              init();
              if (NULL == instance_)
              {
                __sync_bool_compare_and_swap(&once_, 1, 0);
                TBSYS_LOG(ERROR, "once init fail");
              }
              else
              {
                __sync_bool_compare_and_swap(&once_, 1, 2);
              }
            }
          }
        }

        static volatile int64_t once_;
        static Type* instance_;
    };

    template <int32_t MOD_ID = 0>
    class ObTCAllocator : public ObIAllocator
    {
      public:
        typedef ObTCAllocator<MOD_ID> allocator_type;
        typedef ObTCAllocator<MOD_ID>* allocator_pointer;
        virtual ~ObTCAllocator() {};

      public:
        virtual void *alloc(const int64_t sz) {return ob_tc_malloc(sz, mod_id_);}
        virtual void free(void *ptr) { ob_tc_free(ptr, mod_id_);}

        // Returns a singleton instance of the tc allocator.
        static allocator_pointer get() 
        {
          return ObSingleton<allocator_type>::get();
        }

      private:
        friend class ObSingleton<allocator_type>;

        DISALLOW_COPY_AND_ASSIGN(ObTCAllocator);
        ObTCAllocator() : mod_id_(MOD_ID) {};

        int32_t mod_id_;
    };

    // Abstract policy for modifying allocation requests - e.g. enforcing quotas.
    class ObMediator 
    {
     public:
      ObMediator() {}
      virtual ~ObMediator() {}

      // Called by an allocator when a allocation request is processed.
      // Must return a value equal to requested, or zero. Returning
      // zero indicates denial to allocate. Returning non-zero indicates 
      // that the request should be capped at that value.
      virtual int64_t alloc(int64_t requested) = 0;

      // Called by an allocator when the specified amount (in bytes) is released.
      virtual void free(int64_t amount) = 0;
    };

    // Optionally thread-safe skeletal implementation of a 'quota' abstraction,
    // providing methods to allocate resources against the quota, and return them.
    class ObQuota : public ObMediator 
    {
      public:
        explicit ObQuota(bool enforced) : usage_(0), enforced_(enforced) {}
        virtual ~ObQuota() {}

        // Returns a value equal to requested if not exceeding remaining
        // quota or if the quota is not enforced (soft quota), and adjusts the usage
        // value accordingly.  Otherwise, returns zero. The semantics of 'remaining
        // quota' are defined by subclasses (that must supply get_quota_internal()
        // method).
        virtual int64_t alloc(int64_t requested)
        {
          const int64_t quota = get_quota_internal();
          int64_t allocation = 0;

          if (usage_ > quota || requested > quota - usage_)
          {
            // OOQ (Out of quota).
            if (!enforced()) {
              // The quota is unenforced. Perform a requested allocation.
              allocation = requested;
            } else {
              allocation = 0;
            }
            TBSYS_LOG(WARN, "Out of quota. usage[%ld] + requested[%ld] > quota[%ld], "
                            "allocation[%ld], The quota is %s", 
                      usage_, requested, quota, allocation, (enforced() ? "" : "not "));
          }
          else 
          {
            allocation = std::min(requested, quota - usage_);
          }
          __sync_add_and_fetch(&usage_, allocation);

          return allocation;
        }

        virtual void free(int64_t amount)
        {
          if (__sync_add_and_fetch(&usage_, -amount) < 0)
          {
            TBSYS_LOG(ERROR, "free(size=%ld): allocated[%ld] < 0 after free", amount, usage_);
          }
        }

        // Returns the current quota value.
        int64_t get_quota() const
        {
          return get_quota_internal();
        }

        // Returns the current usage value, defined as a sum of all the values
        // granted by calls to Allocate, less these released via calls to Free.
        int64_t get_usage() const
        {
          return usage_;
        }

        bool enforced() const 
        {
          return enforced_;
        }

      protected:
        // Overridden by specific implementations, to define semantics of
        // the quota, i.e. the total amount of resources that the mediator will
        // allocate. Called directly from GetQuota that optionally provides
        // thread safety. An 'Allocate' request will succeed if
        // GetUsage() + minimal <= GetQuota() or if the quota is not enforced (soft
        // quota).
        virtual int64_t get_quota_internal() const = 0;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObQuota);
        volatile int64_t usage_;
        bool enforced_;
    };

    class ObStaticQuota : public ObQuota 
    {
      public:
        explicit ObStaticQuota(int64_t quota)
            : ObQuota(true) 
        {
          set_quota(quota);
        }

        ObStaticQuota(int64_t quota, bool enforced)
            : ObQuota(enforced) 
        {
          set_quota(quota);
        }

        virtual ~ObStaticQuota() {}

        // Sets quota to the new value.
        void set_quota(const int64_t quota)
        {
          quota_ = quota;
        }

      protected:
        virtual int64_t get_quota_internal() const 
        { 
          return quota_; 
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObStaticQuota);
        volatile int64_t quota_;
    };

    // Places resource limits on another allocator, using the specified Mediator
    // (e.g. quota) implementation.
    //
    // If the mediator and the delegate allocator are thread-safe, this allocator
    // is also thread-safe, to the extent that it will not introduce any
    // state inconsistencies. However, without additional synchronization,
    // allocation requests are not atomic end-to-end. This way, it is deadlock-
    // resilient (even if you have cyclic relationships between allocators) and
    // allows better concurrency. But, it may cause over-conservative
    // allocations under memory contention, if you have multiple levels of
    // mediating allocators. For example, if two requests that can't both be
    // satisfied are submitted concurrently, it may happen that both of them fails.
    // This is usually not a problem, however, as you don't really want to
    // operate so close to memory limits that some of your allocations can't be
    // satisfied. If you do have a simple, cascading graph of allocators though,
    // and want to force requests be atomic end-to-end, put a
    // ObThreadSafeAllocator at the entry point.
    class ObMediatingAllocator : public ObIAllocator 
    {
      public:
        ObMediatingAllocator(ObIAllocator* const delegate,
                             ObMediator* const mediator,
                             bool trace_size = true)
        : delegate_(delegate),
          mediator_(mediator),
          trace_size_(trace_size) {}

        virtual ~ObMediatingAllocator() {}

        virtual void* alloc(const int64_t sz)
        {
          // Not allow the mediator to trim the request.
          void* buffer = NULL;
          Block* block = NULL;
          int64_t granted = 0;

          if (NULL != mediator_ && NULL != delegate_ && sz > 0) {
            granted = sz + (trace_size_ ? sizeof(*block) : 0);
            if (mediator_->alloc(granted) == granted)
            {
              buffer = delegate_->alloc(granted);
              if (NULL == buffer) 
              {
                mediator_->free(granted);
              }
              else if (trace_size_)
              {
                block = new (buffer) Block(sz);
                buffer = block->buf_;
              }
            }
          }

          return buffer;
        }

        virtual void free(void* ptr)
        {
          if (NULL != mediator_ && NULL != delegate_)
          {
            if (trace_size_)
            {
              Block* block = (NULL != ptr) ? (Block*)((char*)ptr - sizeof(*block)) : NULL;
              int64_t size = (NULL != block) ? block->size_ : 0;
              mediator_->free(size);
            }
            delegate_->free(ptr);
          }
        }

      private:
        struct Block
        {
          Block(int64_t size): size_(size) {}
          ~Block() {}
          const int64_t size_;
          char buf_[0];
        };

      private:
        ObIAllocator* delegate_;
        ObMediator* mediator_;
        bool trace_size_;
    };

    // Convenience non-thread-safe static memory bounds enforcer.
    // Combines ObMemoryLimitAllocator with a ObStaticQuota.
    template <bool trace_size = true>
    class ObMemoryLimitAllocator : public ObIAllocator 
    {
      public:
        // Creates a limiter based on the default, heap allocator. Quota is infinite.
        // (Can be set using SetQuota).
        ObMemoryLimitAllocator()
        : quota_(INT64_MAX),
          allocator_(ObTCAllocator<>::get(), &quota_, trace_size) {}

        // Creates a limiter based on the default, heap allocator.
        explicit ObMemoryLimitAllocator(int64_t quota)
        : quota_(quota),
          allocator_(ObTCAllocator<>::get(), &quota_, trace_size) {}

        // Creates a limiter relaying to the specified delegate allocator.
        ObMemoryLimitAllocator(int64_t quota, ObIAllocator* const delegate)
        : quota_(quota),
          allocator_(delegate, &quota_, trace_size) {}

        // Creates a (possibly non-enforcing) limiter relaying to the specified
        // delegate allocator.
        ObMemoryLimitAllocator(size_t quota, bool enforced, ObIAllocator* const delegate)
        : quota_(quota, enforced),
          allocator_(delegate, &quota_, trace_size) {}

        virtual ~ObMemoryLimitAllocator() {}

        virtual void* alloc(const int64_t sz)
        {
          return allocator_.alloc(sz);
        }

        virtual void free(void* ptr)
        {
          return allocator_.free(ptr);
        }

        size_t get_quota() const { return quota_.get_quota(); }
        size_t get_usage() const { return quota_.get_usage(); }
        void set_quota(const int64_t quota) { quota_.set_quota(quota); }

      private:
        ObStaticQuota quota_;
        ObMediatingAllocator allocator_;
    };

    template <bool trace_size = true>
    class ObMultThreadMemoryLimitAllocator : public ObIAllocator 
    {
      public:
        // Creates a limiter based on the default, heap allocator. Quota is infinite.
        // (Can be set using SetQuota).
        ObMultThreadMemoryLimitAllocator()
        : quota_(INT64_MAX),
          delegate_(ObTCAllocator<>::get()) {}

        // Creates a limiter based on the default, heap allocator.
        explicit ObMultThreadMemoryLimitAllocator(int64_t quota)
        : quota_(quota),
          delegate_(ObTCAllocator<>::get()) {}

        // Creates a limiter relaying to the specified delegate allocator.
        ObMultThreadMemoryLimitAllocator(int64_t quota, ObIAllocator* const delegate)
        : quota_(quota),
          delegate_(delegate) {}

        // Creates a (possibly non-enforcing) limiter relaying to the specified
        // delegate allocator.
        ObMultThreadMemoryLimitAllocator(size_t quota, bool enforced, 
                                         ObIAllocator* const delegate)
        : quota_(quota, enforced),
          delegate_(delegate) {}

        virtual ~ObMultThreadMemoryLimitAllocator() {}

        virtual void* alloc(const int64_t sz)
        {
          void* buffer = NULL;
          ObMediatingAllocator* allocator = 
            GET_TSI_ARGS(ObMediatingAllocator, TSI_CS_MEDIATING_ALLOCATOR_1, 
                delegate_, &quota_, trace_size);
          if (NULL != allocator)
          {
            buffer = allocator->alloc(sz);
          }
          return buffer;
        }

        virtual void free(void* ptr)
        {
          ObMediatingAllocator* allocator = 
            GET_TSI_ARGS(ObMediatingAllocator, TSI_CS_MEDIATING_ALLOCATOR_1, 
                delegate_, &quota_, trace_size);
          if (NULL != allocator)
          {
            allocator->free(ptr);
          }
        }

        size_t get_quota() const { return quota_.get_quota(); }
        size_t get_usage() const { return quota_.get_usage(); }
        void set_quota(const int64_t quota) { quota_.set_quota(quota); }

      private:
        ObStaticQuota quota_;
        ObIAllocator* delegate_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_MEMORY_H_
