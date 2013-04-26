/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tp_malloc.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_tp_malloc.h"
#include "page_arena.h"
#include "ob_malloc.h"
#include "thread_buffer.h"
#include "ob_tsi_factory.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace common
  {
    static ThreadSpecificBuffer& get_the_tsbuff()
    {
      static ThreadSpecificBuffer tsbuff;
      return tsbuff;
    }

    int ob_tp_malloc_init()
    {
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer& tsbuff = get_the_tsbuff();
      (void)tsbuff;
      return ret;
    }
    static const char TAG_ALLOC_FROM_LOCAL = 1;
    static const char TAG_ALLOC_FROM_GLOBAL = 0;
    void *ob_tp_malloc(const int64_t nbyte, const int32_t mod_id)
    {
      void *ret = NULL;
      ThreadSpecificBuffer& tsbuff = get_the_tsbuff();
      ThreadSpecificBuffer::Buffer *buff = tsbuff.get_buffer();
      if (OB_UNLIKELY(NULL == buff))
      {
        TBSYS_LOG(ERROR, "no memory");
        ob_print_mod_memory_usage();
      }
      else
      {
        if (OB_LIKELY(1 + nbyte < buff->remain()))
        {
          char *ptr = buff->current();
          buff->advance(static_cast<int32_t>(1+nbyte));
          *ptr = TAG_ALLOC_FROM_LOCAL;             // tag means allocated from the thread specific buffer
          ret = ptr + 1;
        }
        else
        {
          // allocate from the global memory
          char *ptr = reinterpret_cast<char*>(ob_malloc(1+nbyte, mod_id));
          if (OB_UNLIKELY(NULL == ret))
          {
            TBSYS_LOG(ERROR, "no memory");
            ob_print_mod_memory_usage();
          }
          else
          {
            *ptr = TAG_ALLOC_FROM_GLOBAL;           // tag means allocated from the global memory
            ret = ptr + 1;
          }
        }
      }
      return ret;
    }

    void ob_tp_free(void *ptr, const int32_t mod_id)
    {
      if (NULL != ptr)
      {
        char* tag = reinterpret_cast<char*>(ptr);
        if (TAG_ALLOC_FROM_GLOBAL == *(tag-1))
        {
          ob_free(tag, mod_id);
        }
      }
    }

    void ob_tp_reuse()
    {
      ThreadSpecificBuffer& tsbuff = get_the_tsbuff();
      ThreadSpecificBuffer::Buffer *buff = tsbuff.get_buffer();
      if (OB_UNLIKELY(NULL == buff))
      {
        TBSYS_LOG(ERROR, "thread specific buffer not init");
      }
      else
      {
        buff->reset();
      }
    }

    TPArena *ob_get_tp_arena()
    {
      return GET_TSI_MULT(TPArena, TSI_SQL_TP_ARENA_1);
    }

  } // end namespace common
} // end namespace oceanbase
