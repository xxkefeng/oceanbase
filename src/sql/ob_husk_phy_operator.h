/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_husk_phy_operator.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_HUSK_PHY_OPERATOR_H
#define _OB_HUSK_PHY_OPERATOR_H 1
#include "ob_single_child_phy_operator.h"
namespace oceanbase
{
  namespace sql
  {
    enum ObLockFlag
    {
      LF_NONE = 0,
      LF_WRITE = 1,
    };
    class ObHuskPhyOperator: public ObSingleChildPhyOperator
    {
      public:
        ObHuskPhyOperator(){};
        virtual ~ObHuskPhyOperator(){};
        virtual void set_param(void *param) = 0;
      private:
        // types and constants
      private:
        // disallow copy
        ObHuskPhyOperator(const ObHuskPhyOperator &other);
        ObHuskPhyOperator& operator=(const ObHuskPhyOperator &other);
        // function members
      private:
        // data members
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_HUSK_PHY_OPERATOR_H */
