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
#include "ob_tc_malloc.h"
#include "ob_allocator.h"

namespace oceanbase
{
  namespace common
  {
    ObTSIBlockAllocator& get_global_tsi_block_allocator()
    {
      static int64_t init_lock = 0;
      static ObMalloc sys_allocator(ObModIds::TSI_BLOCK_ALLOC);
      static ObTSIBlockAllocator global_tsi_block_allocator;
      while(init_lock < 2)
      {
        if (__sync_bool_compare_and_swap(&init_lock, 0, 1))
        {
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = global_tsi_block_allocator.init(&sys_allocator)))
          {
            __sync_bool_compare_and_swap(&init_lock, 1, 0);
            TBSYS_LOG(ERROR, "global_tsi_block_allocator.init() fail, err=%d", err);
          }
          else
          {
            __sync_bool_compare_and_swap(&init_lock, 1, 2);
          }
        }
      }
      return global_tsi_block_allocator;
    }

    void *ob_tc_malloc(const int64_t nbyte, const int32_t mod_id)
    {
      return get_global_tsi_block_allocator().mod_alloc(nbyte, mod_id);
    }

    void ob_tc_free(void *ptr, const int32_t mod_id)
    {
      get_global_tsi_block_allocator().mod_free(ptr, mod_id);
    }
  }; // end namespace common
}; // end namespace oceanbase
