/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_modify.h
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#ifndef _OB_UPS_MODIFY_H
#define _OB_UPS_MODIFY_H 1

#include "ob_husk_filter.h"

namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    namespace test
    {
    }
    typedef ObHuskFilter<PHY_UPS_MODIFY> ObUpsModify;
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPS_MODIFY_H */
