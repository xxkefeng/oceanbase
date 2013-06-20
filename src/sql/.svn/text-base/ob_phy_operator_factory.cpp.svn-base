/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_phy_operator_factory.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_phy_operator_factory.h"
#include "ob_project.h"
#include "ob_limit.h"
#include "ob_filter.h"
#include "ob_table_mem_scan.h"
#include "ob_rename.h"
#include "ob_table_rename.h"
#include "ob_sort.h"
#include "ob_mem_sstable_scan.h"
#include "ob_lock_filter.h"
#include "ob_inc_scan.h"
#include "ob_insert_dbsem_filter.h"
#include "ob_ups_modify.h"
#include "ob_multiple_scan_merge.h"
#include "ob_multiple_get_merge.h"
#include "ob_empty_row_filter.h"
#include "ob_phy_operator.h"

using namespace oceanbase;
using namespace sql;

#define CASE_CLAUSE(OP_TYPE, OP) \
    case OP_TYPE: \
      tmp = allocator.alloc(sizeof(OP)); \
      if (NULL != tmp) \
      { \
        ret = new(tmp)OP; \
      } \
      break

ObPhyOperator *ObPhyOperatorFactory::get_one(ObPhyOperatorType phy_operator_type, common::ModuleArena &allocator)
{
  ObPhyOperator *ret = NULL;
  void *tmp = NULL;
  switch(phy_operator_type)
  {
    case PHY_INVALID:
      break;
    CASE_CLAUSE(PHY_PROJECT, ObProject);
    CASE_CLAUSE(PHY_LIMIT, ObLimit);
    CASE_CLAUSE(PHY_FILTER, ObFilter);
    CASE_CLAUSE(PHY_TABLE_MEM_SCAN, ObTableMemScan);
    CASE_CLAUSE(PHY_RENAME, ObRename);
    CASE_CLAUSE(PHY_TABLE_RENAME, ObTableRename);
    CASE_CLAUSE(PHY_SORT, ObSort);
    CASE_CLAUSE(PHY_MEM_SSTABLE_SCAN, ObMemSSTableScan);
    CASE_CLAUSE(PHY_LOCK_FILTER, ObLockFilter);
    CASE_CLAUSE(PHY_INC_SCAN, ObIncScan);
    CASE_CLAUSE(PHY_INSERT_DB_SEM_FILTER, ObInsertDBSemFilter);
    CASE_CLAUSE(PHY_UPS_MODIFY, ObUpsModify);
    CASE_CLAUSE(PHY_MULTIPLE_SCAN_MERGE, ObMultipleScanMerge);
    CASE_CLAUSE(PHY_MULTIPLE_GET_MERGE, ObMultipleGetMerge);
    CASE_CLAUSE(PHY_EMPTY_ROW_FILTER, ObEmptyRowFilter);
    CASE_CLAUSE(PHY_EXPR_VALUES, ObExprValues);
    default:
      break;
  }
  return ret;
}
