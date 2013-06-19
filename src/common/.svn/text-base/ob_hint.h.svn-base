/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_hint.h,  08/09/2012 11:23:53 AM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *   provide search hint, scan hint, get hint
 *
 */

#ifndef  OCEANBASE_COMMON_HINT_H_
#define OCEANBASE_COMMON_HINT_H_
namespace oceanbase
{
  namespace common
  {
    struct ObRpcScanHint
    {
      int64_t max_parallel_count;
      int64_t max_memory_limit;
      int64_t timeout_us;
      bool only_frozen_version_data_;
      bool only_static_data_;
      bool read_consistency_;
      ObRpcScanHint() :
        max_parallel_count(20),
        max_memory_limit(1024*1024*512),
        timeout_us(1000*1000*10),
        only_frozen_version_data_(false),
        only_static_data_(false),
        read_consistency_(false)
      {
      }
    };
  }
}
#endif // end of header
