/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_phy_operator_factory.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_PHY_OPERATOR_FACTORY_H
#define _OB_PHY_OPERATOR_FACTORY_H 1

#include "ob_phy_operator_type.h"
#include "common/page_arena.h"

namespace oceanbase
{
  namespace sql
  {
    class ObPhyOperator;
    class ObPhyOperatorFactory
    {
      public:
        ObPhyOperatorFactory(){}
        virtual ~ObPhyOperatorFactory(){}
      public:
        virtual ObPhyOperator *get_one(ObPhyOperatorType phy_operator_type, common::ModuleArena &allocator);
    };
  }
}

#endif /* _OB_PHY_OPERATOR_FACTORY_H */
