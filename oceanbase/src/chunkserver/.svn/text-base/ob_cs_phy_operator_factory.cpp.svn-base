/**
 * (C) 2007-2013 Alipay Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *  yongle.xh<yongle.xh@alipay.com>
 */
#include "ob_cs_phy_operator_factory.h"
#include "sql/ob_tablet_scan_v2.h"
#include "sql/ob_tablet_get_v2.h"

#define new_operator(__type__, __allocator__, ...)      \
  ({                                                    \
    __type__ *ret = NULL;                               \
    void* buf = __allocator__.alloc(sizeof(__type__));  \
    if (NULL != buf)                                    \
    {                                                   \
      ret = new(buf) __type__(__VA_ARGS__);             \
    }                                                   \
    ret;                                                \
   })

namespace oceanbase
{
  using namespace sql;
  namespace chunkserver
  {
    ObPhyOperator *ObCsPhyOperatorFactory::get_one(ObPhyOperatorType type, common::ModuleArena &allocator)
    {
      ObPhyOperator *op = NULL;
      if (!inited_)
      {
        TBSYS_LOG(ERROR, "not inited yet, ups_rpc_proxy_=%p tablet_mgr_=%p", ups_rpc_proxy_, tablet_mgr_);
      }
      else
      {
        switch(type)
        {
          case PHY_TABLET_SCAN_V2:
            op = new_operator(ObTabletScanV2, allocator, plan_context_, tablet_mgr_, ups_rpc_proxy_, schema_mgr_);
            break;
          case PHY_TABLET_GET_V2:
            op = new_operator(ObTabletGetV2, allocator, plan_context_, tablet_mgr_, ups_rpc_proxy_, schema_mgr_);
            break;
          default:
            op = ObPhyOperatorFactory::get_one(type, allocator);
            break;
        }
      }
      return op;
    }
  }; // end namespace chunkserver
}; // end namespace oceanbase
