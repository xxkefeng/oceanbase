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
#include "common/ob_define.h"

namespace oceanbase
{
  namespace updateserver
  {
    struct ObBatchEventStat
    {
      int64_t batch_count_;
      int64_t cur_end_;
      int64_t mvalue_;
      int64_t mstart_;
      int64_t mend_;
      void clear_mvalue();
      void add(const int64_t start_id, const int64_t end_id, const int64_t value);
      int serialize(char* buf, int64_t len, int64_t& pos) const;
      int deserialize(const char* buf, int64_t len, int64_t& pos);
    };

    struct ObClogStat
    {
      static const int64_t STRUCT_VERSION = 0;
      void add_disk_us(int64_t start_id, int64_t end_id, int64_t time);
      void add_net_us(int64_t start_id, int64_t end_id, int64_t time);
      void clear();
      int serialize(char* buf, int64_t len, int64_t& pos) const;
      int deserialize(const char* buf, int64_t len, int64_t& pos);
      int64_t to_string(char* buf, const int64_t len) const;
      ObBatchEventStat disk_stat_;
      ObBatchEventStat net_stat_;
      ObBatchEventStat batch_stat_;
    };
  }; // end namespace updateserver
}; // end namespace oceanbase
