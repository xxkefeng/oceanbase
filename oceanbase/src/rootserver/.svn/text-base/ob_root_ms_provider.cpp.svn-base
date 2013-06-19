/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_ms_provider.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_ms_provider.h"
#include "common/ob_scan_param.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootMsProvider::ObRootMsProvider(const ObChunkServerManager &server_manager)
  :server_manager_(server_manager), scan_param_hash_(0),
   ms_num_(0), count_(0)
{
}

ObRootMsProvider::~ObRootMsProvider()
{
  scan_param_hash_ = 0;
  ms_num_ = 0;
  count_ = 0;
}

int ObRootMsProvider::reset(const uint32_t scan_param_hash)
{
  int ret = OB_SUCCESS;
  scan_param_hash_ = scan_param_hash;
  ms_num_ = 0;
  count_ = 0;
  ObChunkServerManager::const_iterator it = server_manager_.begin();
  for (; server_manager_.end() != it; ++it)
  {
    if (ObServerStatus::STATUS_DEAD != it->ms_status_)
    {
      ++ms_num_;
    }
  }

  if (0 == ms_num_)
  {
    TBSYS_LOG(WARN, "no available ms");
    ret = OB_NO_CS_SELECTED;
  }
  else
  {
    uint32_t start_pos = scan_param_hash_ % ms_num_;
    const uint32_t asize = static_cast<uint32_t>(server_manager_.end() - server_manager_.begin());
    for (it = server_manager_.begin();
         server_manager_.end() != it && count_ < MAX_SERVER_COUNT && 0 < asize;
         ++it)
    {
      ObChunkServerManager::const_iterator real_it = server_manager_.begin() + ((it - server_manager_.begin() + start_pos) % asize);
      if (ObServerStatus::STATUS_DEAD != real_it->ms_status_)
      {
        ObServer ms = real_it->server_;
        ms.set_port(real_it->port_ms_);
        ms_carray_[count_++] = ms;
      }
    } // end for
  }
  if (0 == count_)
  {
    TBSYS_LOG(WARN, "no available ms");
    ret = OB_NO_CS_SELECTED;
  }
  return ret;
}

// @return OB_SUCCESS or
// - OB_MS_ITER_END when there is no more mergeserver
int ObRootMsProvider::get_ms(const ObScanParam &scan_param, const int64_t retry_num, ObServer &ms)
{
  int ret = OB_SUCCESS;
  const ObNewRange* range = scan_param.get_range();
  uint32_t scan_param_hash = range->start_key_.murmurhash2(0);
  if (scan_param_hash_ != scan_param_hash
      || 0 >= ms_num_
      || 0 >= count_)
  {
    if (OB_SUCCESS != (ret = reset(scan_param_hash)))
    {
      TBSYS_LOG(WARN, "failed to init ms provider, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (retry_num >= count_)
    {
      TBSYS_LOG(DEBUG, "no more ms for scan, retry=%ld count=%ld", retry_num, count_);
      ret = OB_MS_ITER_END;
    }
    else
    {
      ms = ms_carray_[retry_num];
    }
  }
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get ms. ret=%d", ret);
  }
  return ret;
}


// @return OB_SUCCESS or
// - OB_MS_ITER_END when there is no more mergeserver
int ObRootMsProvider::get_ms(const int64_t retry_num, ObServer &ms)
{
  int ret = OB_SUCCESS;
  uint32_t ms_rand_seed = 0;
  if (scan_param_hash_ != ms_rand_seed
      || 0 >= ms_num_
      || 0 >= count_)
  {
    if (OB_SUCCESS != (ret = reset(ms_rand_seed)))
    {
      TBSYS_LOG(WARN, "failed to init ms provider, err=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (retry_num >= count_)
    {
      TBSYS_LOG(DEBUG, "no more ms for scan, retry=%ld count=%ld", retry_num, count_);
      ret = OB_MS_ITER_END;
    }
    else
    {
      ms = ms_carray_[retry_num];
    }
  }
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "fail to get ms. ret=%d", ret);
  }
  return ret;
}

