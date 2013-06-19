/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_last_rowkey_interface.h 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_CUR_ROWKEY_INTERFACE_H
#define _OB_CUR_ROWKEY_INTERFACE_H 1

#include "common/ob_rowkey.h"

namespace oceanbase
{
  namespace sql
  {
    class ObCurRowkeyInterface
    {
      public:
        virtual int get_cur_rowkey(const common::ObRowkey *&rowkey) const = 0;
        virtual ~ObCurRowkeyInterface() {}
    };
  }
}

#endif /* _OB_CUR_ROWKEY_INTERFACE_H */
  

