/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_physical_plan.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_physical_plan.h"
#include "common/utility.h"
#include "ob_table_rpc_scan.h"
#include "ob_mem_sstable_scan.h"
#include "common/serialization.h"
#include "ob_phy_operator_factory.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;

ObPhysicalPlan::ObPhysicalPlan()
  :curr_frozen_version_(OB_INVALID_VERSION),
   ts_timeout_us_(0),
   main_query_(NULL),
   operators_store_(common::OB_MALLOC_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_PHY_PLAN)),
   allocator_(NULL),
   op_factory_(NULL),
   my_result_set_(NULL),
   start_trans_(false)
{
}

ObPhysicalPlan::~ObPhysicalPlan()
{
  clear();
}

int ObPhysicalPlan::deserialize_header(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &start_trans_)))
  {
    TBSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = start_trans_req_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = trans_id_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "failed to decode trans_id_, err=%d", ret);
  }
  return ret;
}

int ObPhysicalPlan::add_phy_query(ObPhyOperator *phy_query, int32_t* idx, bool main_query)
{
  int ret = OB_SUCCESS;
  if ( (ret = phy_querys_.push_back(phy_query)) == OB_SUCCESS)
  {
    if (idx != NULL)
      *idx = static_cast<int32_t>(phy_querys_.count() - 1);
    if (main_query)
      main_query_ = phy_query;
  }
  return ret;
}

int ObPhysicalPlan::store_phy_operator(ObPhyOperator *op)
{
  return operators_store_.push_back(op);
}

ObPhyOperator* ObPhysicalPlan::get_phy_query(int32_t index) const
{
  ObPhyOperator *op = NULL;
  if (index >= 0 && index < phy_querys_.count())
    op = phy_querys_.at(index);
  return op;
}

ObPhyOperator* ObPhysicalPlan::get_phy_operator(int64_t index) const
{
  ObPhyOperator *op = NULL;
  if (index >= 0 && index < operators_store_.count())
    op = operators_store_.at(index);
  return op;
}

ObPhyOperator* ObPhysicalPlan::get_main_query() const
{
  return main_query_;
}

void ObPhysicalPlan::set_main_query(ObPhyOperator *query)
{
  main_query_ = query;
}

int ObPhysicalPlan::remove_phy_query(ObPhyOperator *phy_query)
{
  int ret = OB_SUCCESS;
  UNUSED(phy_query);
  // if (OB_SUCCESS != (ret = phy_querys_.remove_if(phy_query)))
  // {
  //   TBSYS_LOG(WARN, "phy query not exist, phy_query=%p", phy_query);
  // }
  return ret;
}

int64_t ObPhysicalPlan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "PhysicalPlan(operators_num=%ld query_num=%ld "
                  "trans_id=%s start_trans=%c trans_req=%s)\n",
                  operators_store_.count(), phy_querys_.count(),
                  to_cstring(trans_id_), start_trans_?'Y':'N', to_cstring(start_trans_req_));
  for (int32_t i = 0; i < phy_querys_.count(); ++i)
  {
    if (main_query_ == phy_querys_.at(i))
      databuff_printf(buf, buf_len, pos, "====MainQuery====:\n");
    else
      databuff_printf(buf, buf_len, pos, "====SubQuery%d====:\n", i);
    int64_t pos2 = phy_querys_.at(i)->to_string(buf + pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}

int ObPhysicalPlan::deserialize_tree(const char *buf, int64_t data_len, int64_t &pos, ModuleArena &allocator,
                                     OperatorStore &operators_store, ObPhyOperator *&root)
{
  int ret = OB_SUCCESS;
  int32_t phy_operator_type = 0;
  if (NULL == op_factory_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "op_factory == NULL");
  }
  else if (OB_SUCCESS != (ret = decode_vi32(buf, data_len, pos, &phy_operator_type)))
  {
    TBSYS_LOG(WARN, "fail to decode phy operator type:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    root = op_factory_->get_one(static_cast<ObPhyOperatorType>(phy_operator_type), allocator);
    if (NULL == root)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get operator fail:type[%d]", phy_operator_type);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = root->deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize operator:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = operators_store.push_back(root)))
    {
      TBSYS_LOG(WARN, "fail to push operator to operators_store:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (root->get_type() <= PHY_INVALID || root->get_type() >= PHY_END)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "invalid operator type:[%d]", root->get_type());
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int32_t i=0; OB_SUCCESS == ret && i<root->get_child_num();i++)
    {
      ObPhyOperator *child = NULL;
      if (OB_SUCCESS != (ret = deserialize_tree(buf, data_len, pos, allocator, operators_store, child)))
      {
        TBSYS_LOG(WARN, "fail to deserialize tree:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = root->set_child(i, *child)))
      {
        TBSYS_LOG(WARN, "fail to set child:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int ObPhysicalPlan::serialize_tree(char *buf, int64_t buf_len, int64_t &pos, const ObPhyOperator &root) const
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, root.get_type())))
    {
      TBSYS_LOG(WARN, "fail to encode op type:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = root.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize root:ret[%d] type=%d op=%s", ret, root.get_type(), to_cstring(root));
    }
    else
    {
      TBSYS_LOG(DEBUG, "serialize operator succ, type=%d", root.get_type());
    }
  }

  for (int64_t i=0;OB_SUCCESS == ret && i<root.get_child_num();i++)
  {
    if (NULL != root.get_child(static_cast<int32_t>(i)) )
    {
      if (OB_SUCCESS != (ret = serialize_tree(buf, buf_len, pos, *(root.get_child(static_cast<int32_t>(i))))))
      {
        TBSYS_LOG(WARN, "fail to serialize tree:ret[%d]", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "this operator should has child:type[%d]", root.get_type());
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObPhysicalPlan)
{
  int ret = OB_SUCCESS;
  // @todo yzf, support multiple queries
  int32_t main_query_idx = 0;
  // get current trans id
  OB_ASSERT(my_result_set_);
  common::ObTransID trans_id = my_result_set_->get_session()->get_trans_id();
  FILL_TRACE_LOG("trans_id=%s", to_cstring(trans_id));
  if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, start_trans_)))
  {
    TBSYS_LOG(WARN, "failed to serialize trans_id_, err=%d buf_len=%ld pos=%ld",
              ret, buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = start_trans_req_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "serialize error, buf_len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = trans_id.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "trans_id.serialize(buf=%p[%ld-%ld])=>%d", buf, pos, buf_len, ret);
  }
  else if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, main_query_idx)))
  {
    TBSYS_LOG(WARN, "fail to encode main query idx:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, 1)))
  {
    TBSYS_LOG(WARN, "fail to encode phy queryes size :ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialize_tree(buf, buf_len, pos, *main_query_)))
  {
    TBSYS_LOG(WARN, "fail to serialize tree:ret[%d]", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObPhysicalPlan)
{
  int ret = OB_SUCCESS;
  int32_t main_query_idx = -1;
  int32_t phy_querys_size = 0;
  ObPhyOperator *root = NULL;

  clear();
  if (NULL == allocator_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "allocator_ is not setted");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &start_trans_)))
  {
    TBSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = start_trans_req_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = trans_id_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "trans_id.deserialize(buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
  }
  else if (OB_SUCCESS != (ret = decode_vi32(buf, data_len, pos, &main_query_idx)))
  {
    TBSYS_LOG(WARN, "fail to decode main query idx:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = decode_vi32(buf, data_len, pos, &phy_querys_size)))
  {
    TBSYS_LOG(WARN, "fail to decode phy querys size:ret[%d]", ret);
  }

  for (int32_t i=0;OB_SUCCESS == ret && i<phy_querys_size;i++)
  {
    if (OB_SUCCESS != (ret = deserialize_tree(buf, data_len, pos, *allocator_, operators_store_, root)))
    {
      TBSYS_LOG(WARN, "fail to deserialize_tree:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = phy_querys_.push_back(root)))
    {
      TBSYS_LOG(WARN, "fail to push item to phy querys:ret[%d]", ret);
    }
  }

  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (OB_SUCCESS != phy_querys_.at(main_query_idx, main_query_))
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "fail to get main query:main_query_idx[%d], size[%ld]", main_query_idx, phy_querys_.count());
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObPhysicalPlan)
{
  int64_t size = 0;
  return size;
}
