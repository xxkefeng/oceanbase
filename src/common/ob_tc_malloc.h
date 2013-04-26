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
#ifndef __OB_COMMON_OB_TC_MALLOC_H__
#define __OB_COMMON_OB_TC_MALLOC_H__
#include "ob_tsi_block_allocator.h"

namespace oceanbase
{
  namespace common
  {
    ObTSIBlockAllocator& get_global_tsi_block_allocator();
    void *ob_tc_malloc(const int64_t nbyte, const int32_t mod_id = 0);
    void ob_tc_free(void *ptr, const int32_t mod_id = 0);
    class ObTCMalloc : public ObIAllocator
    {
      public:
        ObTCMalloc() : mod_id_(0) {};
        ~ObTCMalloc() {};
      public:
        void set_mod_id(int32_t mod_id) {mod_id_ = mod_id;};
        void *alloc(const int64_t sz) {return ob_tc_malloc(sz, mod_id_);};
        void free(void *ptr) {ob_tc_free(ptr, mod_id_);};
      private:
        int32_t mod_id_;
    };

  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_TC_MALLOC_H__ */
