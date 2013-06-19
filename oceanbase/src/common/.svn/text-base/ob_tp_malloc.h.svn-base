/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tp_malloc.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TP_MALLOC_H
#define _OB_TP_MALLOC_H 1
#include "ob_define.h"
#include "page_arena.h"
namespace oceanbase
{
  namespace common
  {
    /** a simple thread-local storage prefered allocator used by SQL execution module
     *
    {
      // Usage in SQL executor:
      ob_tp_reuse();
      ObResultSet result;
      OBSQL.direct_execute(stmt, result, context);
      result.open();
      const ObRow *row = NULL;
      while (OB_SUCCESS == (result.get_next_row(row))) // use TPArenaArena and ob_tp_malloc internally
      {
        ... // all TPArenaArena should constructed using ob_get_tp_arena()
      }
      ob_get_tp_arena->free();
    }
     */
    /// initialize
    int ob_tp_malloc_init();    // called by ob_init_memory_pool()
    /// allocate memory from the thread specific buffer, or from ob_malloc() if not enough space avaiable
    void *ob_tp_malloc(const int64_t nbyte, const int32_t mod_id);
    /// free the memory back to the thread specific buffer or ob_free()
    void ob_tp_free(void *ptr, const int32_t mod_id);
    /// reuse the thread specific buffer
    void ob_tp_reuse();

    // A PageArena's PageAllocator using ob_tp_malloc
    struct TPPageAllocator
    {
      explicit TPPageAllocator(int32_t mod_id = ObModIds::OB_TP_PAGE_ARENA) : mod_id_(mod_id) {}
      void set_mod_id(int32_t mod_id) { mod_id_ = mod_id; }
      void *allocate(const int64_t sz) { return ob_tp_malloc(sz, mod_id_); }
      void deallocate(void *p) { ob_tp_free(p, mod_id_); }
      void freed(const int64_t sz) {UNUSED(sz); /* mostly for effcient bulk stat reporting */ }
      private:
        int32_t mod_id_;
    };
    typedef PageArena<char, TPPageAllocator> TPArena;
    TPArena *ob_get_tp_arena();

    // A PageArena's PageAllocator using TPArena
    struct TPArenaArenaPageAllocator
    {
      explicit TPArenaArenaPageAllocator(TPArena &arena) : allocator_(arena) {}
      void *allocate(const int64_t sz) { return allocator_.alloc(sz); }
      void deallocate(void *p) { allocator_.free(reinterpret_cast<char*>(p)); }
      void freed(const int64_t sz) {UNUSED(sz); /* mostly for effcient bulk stat reporting */ }
      private:
        TPArena &allocator_;
    };
    typedef PageArena<char, TPArenaArenaPageAllocator> TPArenaArena;
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_TP_MALLOC_H */
