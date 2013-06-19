/*
 * (C) 1999-2013 Alibaba Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_sql_plan_param.cpp,  05/27/2013 05:57:14 PM Yu Huang Exp $
 * 
 * Author:  
 *   Huang Yu <xiaochu.yh@alipay.com>
 * Description:  
 *   
 * 
 */
#include "ob_sql_plan_param.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

#define CREATE_PHY_OPERRATOR(op, type_name, physical_plan, ret)    \
  ({                                                                    \
   op = (type_name*)trans_malloc(sizeof(type_name));   \
   if (op == NULL) \
   { \
   ret = OB_ALLOCATE_MEMORY_FAILED; \
   TBSYS_LOG(WARN, "Can not malloc space for %s", #type_name);  \
   } \
   else\
   {\
   (op) = new(op) type_name();    \
   (op)->set_phy_plan(physical_plan);              \
   if ((ret = (physical_plan)->store_phy_operator(op)) != OB_SUCCESS) \
   { \
   TBSYS_LOG(WARN, "Add physical operator %s failed", #type_name);  \
   } \
   } \
   ret;})


#define IMPL_GET_OP_FUNCTION(type_name, member_name) \
  type_name *ObSqlPlanParam::get_##member_name(bool create_if_not_exist) { \
  if (create_if_not_exist && NULL == member_name##_) { \
    int ret = OB_SUCCESS; \
    CREATE_PHY_OPERRATOR(member_name##_, type_name, &inner_plan_, ret); \
  } \
  return member_name##_; \
}

inline void *ObSqlPlanParam::trans_malloc(const size_t nbyte)
{
  OB_ASSERT(allocator_);
  return allocator_->alloc(nbyte);
}

inline void ObSqlPlanParam::trans_free(void* p)
{
  OB_ASSERT(allocator_);
  allocator_->free(p);
}

IMPL_GET_OP_FUNCTION(ObProject, op_project);
IMPL_GET_OP_FUNCTION(ObScalarAggregate, op_scalar_agg);
IMPL_GET_OP_FUNCTION(ObMergeGroupBy, op_group);
IMPL_GET_OP_FUNCTION(ObSort, op_group_columns_sort);
IMPL_GET_OP_FUNCTION(ObLimit, op_limit);
IMPL_GET_OP_FUNCTION(ObFilter, op_filter);
IMPL_GET_OP_FUNCTION(ObTableRename, op_table_rename);
IMPL_GET_OP_FUNCTION(ObHuskTabletScanV2, op_tablet_scan);
IMPL_GET_OP_FUNCTION(ObHuskTabletGetV2, op_tablet_get);
  
