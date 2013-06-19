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
#include "ob_tsi_block_allocator.h"

namespace oceanbase
{
  namespace common
  {
    void TSIBlockCache::call_clear_block_list(BlockList* block_list)
    {
      if (NULL != block_list && NULL != block_list->host_)
      {
        ((TSIBlockCache*)(block_list->host_))->clear_block_list(block_list);
      }
    }

    void TSIBlockCache::clear_block_list(BlockList* block_list)
    {
      if (NULL != block_list)
      {
        Block* block = NULL;
        while(NULL != (block = block_list->pop()))
        {
          global_block_list_.push(block, INT64_MAX);
        }
        block_list_pool_.push(block_list);
      }
    }

    void TSIBlockCache::clear(ObIAllocator* allocator)
    {
      Block* block = NULL;
      if (NULL != allocator)
      {
        for(int64_t i = 0; i < MAX_THREAD_NUM; i++)
        {
          while(NULL != (block = block_list_[i].pop()))
          {
            allocator->free(block);
          }
        }
        while(NULL != (block = global_block_list_.pop()))
        {
          allocator->free(block);
        }
      }
    }

    TSIBlockCache::BlockList* TSIBlockCache::get_block_list_head()
    {
      int err = OB_SUCCESS;
      int syserr = 0;
      BlockList* block_list = NULL;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "not inited");
      }
      else if (NULL != (block_list = (BlockList*)pthread_getspecific(key_)))
      {}
      else if (OB_SUCCESS != (err = block_list_pool_.pop(block_list)))
      {
        TBSYS_LOG(ERROR, "block_list_pool.pop()=>%d, too many threads", err);
      }
      else if (0 != (syserr = pthread_setspecific(key_, block_list)))
      {
        block_list_pool_.push(block_list);
        block_list = NULL;
        TBSYS_LOG(ERROR, "pthread_setspecific()=>%d", syserr);
      }
      return block_list;
    }

    TSIBlockCache::Block* TSIBlockCache::get()
    {
      BlockList* block_list = NULL;
      Block* block = NULL;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "not inited");
      }
      else if (NULL == (block_list = get_block_list_head()))
      {
        TBSYS_LOG(ERROR, "get_block_list()=>NULL");
      }
      else if (NULL != (block = block_list->pop()))
      {
        // do nothing
      }
      else if (NULL != (block = global_block_list_.pop()))
      {
        // do nothing
      }
      return block;
    }

    int TSIBlockCache::put(Block* block, const int64_t limit, const int64_t global_limit)
    {
      int err = OB_SUCCESS;
      BlockList* block_list = NULL;
      if (!inited_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "not inited");
      }
      else if (NULL == block)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "block == NULL");
      }
      else if (NULL == (block_list = get_block_list_head()))
      {
        err = OB_MEM_OVERFLOW;
        TBSYS_LOG(ERROR, "get_block_list()=>NULL");
      }
      else if (OB_SUCCESS != (err = block_list->push(block, limit))
               && OB_SIZE_OVERFLOW != err)
      {
        TBSYS_LOG(ERROR, "block_list->push(%p, limit=%ld)=>%d", block, limit, err);
      }
      else if (OB_SUCCESS == err)
      {} // do nothing
      else if (OB_SUCCESS != (err = global_block_list_.push(block, global_limit))
               && OB_SIZE_OVERFLOW != err)
      {
        TBSYS_LOG(ERROR, "global_block_list->push(%p, %ld)=>%d", block, global_limit, err);
      }
      return err;
    }

    void ObTSIBlockAllocator::set_normal_block_tclimit(const int64_t limit)
    {
      normal_block_tclimit_ = limit;
    }

    void ObTSIBlockAllocator::set_big_block_tclimit(const int64_t limit)
    {
      big_block_tclimit_ = limit;
    }

    void ObTSIBlockAllocator::set_normal_block_glimit(const int64_t limit)
    {
      normal_block_glimit_ = limit;
    }

    void ObTSIBlockAllocator::set_big_block_glimit(const int64_t limit)
    {
      big_block_glimit_ = limit;
    }

    int ObTSIBlockAllocator::init(ObIAllocator* allocator)
    {
      int err = OB_SUCCESS;
      if (inited_)
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == allocator)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        allocator_ = allocator;
        inited_ = true;
      }
      return err;
    }

    ObTSIBlockAllocator::Block* ObTSIBlockAllocator::alloc_block(const int64_t size)
    {
      Block* p = NULL;
      if (NULL == (p = (Block*)allocator_->alloc(size + sizeof(*p))))
      {
        TBSYS_LOG(ERROR, "alloc(%ld)=>NULL", size + sizeof(*p));
      }
      else
      {
        new(p) Block(size);
      }
      return p;
    }

    void ObTSIBlockAllocator::free_block(Block* p)
    {
      if (NULL == p)
      {}
      else
      {
        p->~Block();
        allocator_->free(p);
      }
    }

    void* ObTSIBlockAllocator::alloc(const int64_t size)
    {
      return mod_alloc(size, mod_id_);
    }

    void ObTSIBlockAllocator::free(void* p)
    {
      mod_free(p, mod_id_);
    }

    void* ObTSIBlockAllocator::mod_alloc(const int64_t size, const int32_t mod_id)
    {
      int err = OB_SUCCESS;
      Block* block = NULL;
      if (!inited_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "not inited.");
      }
      else if (size <= 0)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "size[%ld] <= 0", size);
      }
      else if (normal_block_size_ == size)
      {
        block = normal_block_cache_.get();
      }
      else if (big_block_size_ == size)
      {
        block = big_block_cache_.get();
      }
      else
      {
        TBSYS_LOG(WARN, "size[%ld] not equal %ld or %ld", size, normal_block_size_, big_block_size_);
      }
      if (OB_SUCCESS == err && NULL == block)
      {
        block = alloc_block(size);
      }
      if (NULL != block)
      {
        ob_mod_usage_update(size, mod_id);
      }
      return block? block->buf_: NULL;
    }

    void ObTSIBlockAllocator::mod_free(void* p, const int32_t mod_id)
    {
      int err = OB_SUCCESS;
      Block* block = p? (Block*)((char*)p - sizeof(*block)): NULL;
      int64_t size = block? block->size_: 0;
      if (!inited_)
      {
        err = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "not inited.");
      }
      else if (NULL == block)
      {
        err = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "p == NULL");
      }
      else if (normal_block_size_ == block->size_)
      {
        if (OB_SUCCESS == normal_block_cache_.put(block, normal_block_tclimit_, normal_block_glimit_))
        {
          block = NULL;
        }
      }
      else if (big_block_size_ == block->size_)
      {
        if (OB_SUCCESS == big_block_cache_.put(block, big_block_tclimit_, big_block_glimit_))
        {
          block = NULL;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "size[%ld] not equal %ld or %ld", block->size_, normal_block_size_, big_block_size_);
      }
      if (OB_SUCCESS == err && NULL != block)
      {
        free_block(block);
      }
      ob_mod_usage_update(-size, mod_id);
    }
  }; // end namespace common
}; // end namespace oceanbase
