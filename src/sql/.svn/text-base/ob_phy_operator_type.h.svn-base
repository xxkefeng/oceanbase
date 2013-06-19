/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_phy_operator_type.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_PHY_OPERATOR_TYPE_H
#define _OB_PHY_OPERATOR_TYPE_H 1

namespace oceanbase
{
  namespace sql
  {
    enum ObPhyOperatorType
    {
      PHY_INVALID,              /*0*/
      PHY_PROJECT,
      PHY_LIMIT,
      PHY_FILTER,
      PHY_TABLET_SCAN,
      PHY_TABLE_RPC_SCAN,
      PHY_TABLE_MEM_SCAN,
      PHY_RENAME,
      PHY_TABLE_RENAME,
      PHY_SORT,
      PHY_MEM_SSTABLE_SCAN, /*10*/
      PHY_LOCK_FILTER,
      PHY_INC_SCAN,
      PHY_UPS_MODIFY,
      PHY_INSERT_DB_SEM_FILTER,
      PHY_MULTIPLE_SCAN_MERGE,
      PHY_MULTIPLE_GET_MERGE,
      PHY_VALUES,
      PHY_EMPTY_ROW_FILTER,
      PHY_EXPR_VALUES,
      PHY_UPS_EXECUTOR,

      PHY_END /* end of phy operator type */
    };

    void ob_print_phy_operator_stat();
    const char* ob_phy_operator_type_str(ObPhyOperatorType type);
    void ob_inc_phy_operator_stat(ObPhyOperatorType type);
    void ob_dec_phy_operator_stat(ObPhyOperatorType type);
  }
}

#define OB_PHY_OP_INC(type) oceanbase::sql::ob_inc_phy_operator_stat(ObPhyOperatorType type)

#endif /* _OB_PHY_OPERATOR_TYPE_H */
