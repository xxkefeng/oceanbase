/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_ms_provider.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_MS_PROVIDER_H
#define _OB_ROOT_MS_PROVIDER_H 1

#include "common/roottable/ob_ms_provider.h"
#include "ob_chunk_server_manager.h"

namespace oceanbase
{
  namespace rootserver
  {
    /// ms provider to select ms randomly by hash(scan_param)
    class ObRootMsProvider: public common::ObMsProvider
    {
      public:
        ObRootMsProvider(const ObChunkServerManager &server_manager);
        virtual ~ObRootMsProvider();

        int get_ms(const common::ObScanParam &scan_param, const int64_t retry_num, common::ObServer &ms);
        int get_ms(const int64_t retry_num, common::ObServer &ms);
      private:
        int reset(const uint32_t scan_param_hash);
      private:
        DISALLOW_COPY_AND_ASSIGN(ObRootMsProvider);
        static const int64_t MAX_SERVER_COUNT = common::OB_TABLET_MAX_REPLICA_COUNT;
        const ObChunkServerManager &server_manager_;
        common::ObServer ms_carray_[MAX_SERVER_COUNT];
        uint32_t scan_param_hash_;
        uint32_t ms_num_;
        int64_t count_;
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_MS_PROVIDER_H */

