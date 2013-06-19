/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_physical_plan.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef _OB_PHYSICAL_PLAN_H
#define _OB_PHYSICAL_PLAN_H
#include "ob_phy_operator.h"
#include "ob_phy_operator_factory.h"
#include "common/ob_vector.h"
#include "common/page_arena.h"
#include "common/ob_transaction.h"

namespace oceanbase
{
  namespace sql
  {
    class ObTableRpcScan;
    class ObResultSet;

    class ObPhysicalPlan
    {
      public:
        ObPhysicalPlan();
        virtual ~ObPhysicalPlan();

        int add_phy_query(ObPhyOperator *phy_query, int32_t* idx = NULL, bool main_query = false);
        int store_phy_operator(ObPhyOperator *op);
        int32_t get_query_size() const { return phy_querys_.size(); }
        ObPhyOperator* get_phy_query(int32_t index) const;
        ObPhyOperator* get_main_query() const;
        void set_main_query(ObPhyOperator *query);
        int remove_phy_query(ObPhyOperator *phy_query);
        ObPhyOperator *get_operator_by_type(ObPhyOperator *root,  ObPhyOperatorType phy_operator_type);

        void clear();
        int64_t to_string(char* buf, const int64_t buf_len) const;

        int set_allocator(common::ModuleArena *allocator);
        int set_operator_factory(ObPhyOperatorFactory* factory);
        int set_param(const ObPhyOperatorType type, void *param);
        int deserialize_header(const char* buf, const int64_t data_len, int64_t& pos);

        int64_t get_curr_frozen_version() const;
        void set_curr_frozen_version(int64_t fv);

        const common::ObTransID& get_trans_id() const;

        int add_qid_index(const uint64_t query_id, const int64_t index);
        int get_index_by_qid(const uint64_t query_id, int64_t& index) const;

        /**
         * set the timestamp when the execution of this plan should time out
         *
         * @param ts_timeout_us [in] the microseconds timeout
         */
        void set_timeout_timestamp(const int64_t ts_timeout_us);
        int64_t get_timeout_timestamp() const;
        /**
         * Whether it has been time-out.
         * If we have timed-out, the operators' open() or get_next_row() should
         * return OB_PROCESS_TIMEOUT and abort processing
         *
         * @param remain_us [out] if not time-out, return the remaining microseconds
         * @return true or false
         */
        bool is_timeout(int64_t *remain_us = NULL) const;

        void set_result_set(ObResultSet *rs);
        ObResultSet* get_result_set();

        void set_start_trans(bool did_start) {start_trans_ = did_start;};
        bool get_start_trans() const {return start_trans_;};
        common::ObTransReq& get_trans_req() {return start_trans_req_;};
        NEED_SERIALIZE_AND_DESERIALIZE;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObPhysicalPlan);
        ObPhyOperator* get_phy_operator(int64_t index) const;
        int64_t get_operator_size() const { return operators_store_.count(); }
        int deserialize_tree(const char *buf, int64_t data_len, int64_t &pos, common::ModuleArena &allocator, common::ObArray<ObPhyOperator *> &operators_store, ObPhyOperator *&root);
        int serialize_tree(char *buf, int64_t buf_len, int64_t &pos, const ObPhyOperator &root) const;
        ObPhyOperator *get_root_operator(ObPhyOperator *op) const;

      private:
        common::ObTransID trans_id_;
        int64_t curr_frozen_version_; // do not serialize
        int64_t ts_timeout_us_;       // execution timeout for this plan
        ObPhyOperator   *main_query_;
        oceanbase::common::ObVector<ObPhyOperator *> phy_querys_;
        oceanbase::common::ObArray<ObPhyOperator *> operators_store_;
        common::ModuleArena *allocator_;
        ObPhyOperatorFactory* op_factory_;
        void *params_[PHY_END];
        ObResultSet *my_result_set_; // The result set who owns this physical plan
        bool start_trans_;
        common::ObTransReq start_trans_req_;
        common::ObRowDesc::PlacementHashMap<uint64_t, int64_t, common::OB_MAX_SUBQUERIES_NUM> qid_idx_map_;
    };

    inline int ObPhysicalPlan::set_operator_factory(ObPhyOperatorFactory* factory)
    {
      op_factory_ = factory;
      return common::OB_SUCCESS;
    }

    inline int ObPhysicalPlan::set_allocator(common::ModuleArena *allocator)
    {
      int ret = common::OB_SUCCESS;
      if (NULL == allocator)
      {
        ret = common::OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "allocator is null");
      }
      else
      {
        allocator_ = allocator;
      }
      return ret;
    }

    inline int ObPhysicalPlan::set_param(const ObPhyOperatorType type, void *param)
    {
      int ret = common::OB_SUCCESS;
      if (type <= PHY_INVALID || type >= PHY_END || NULL == param)
      {
        ret = common::OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "type is invalid or param is null:type[%d], param[%p]", type, param);
      }
      else if(params_[type] != NULL)
      {
        ret = common::OB_EAGAIN;
        TBSYS_LOG(WARN, "set twice[%d]", type);
      }
      else
      {
        params_[type] = param;
      }

      return ret;
    }

    inline void ObPhysicalPlan::clear()
    {
      TBSYS_LOG(DEBUG, "clear physical plan, addr=%p", this);
      main_query_ = NULL;
      phy_querys_.reset();

      for(int32_t i = 0; i < operators_store_.count(); i++)
      {
        // we can not delete, because it will release space which is not allocated
        // delete operators_store_.at(i);
        operators_store_.at(i)->~ObPhyOperator();
        operators_store_.at(i) = NULL;
      }
      operators_store_.clear();
    }

    inline const common::ObTransID& ObPhysicalPlan::get_trans_id() const
    {
      return trans_id_;
    }

    inline int64_t ObPhysicalPlan::get_curr_frozen_version() const
    {
      return this->curr_frozen_version_;
    }

    inline void ObPhysicalPlan::set_curr_frozen_version(int64_t fv)
    {
      curr_frozen_version_ = fv;
    }

    inline void ObPhysicalPlan::set_timeout_timestamp(const int64_t ts_timeout_us)
    {
      ts_timeout_us_ = ts_timeout_us;
    }

    inline int64_t ObPhysicalPlan::get_timeout_timestamp() const
    {
      return this->ts_timeout_us_;
    }

    inline bool ObPhysicalPlan::is_timeout(int64_t *remain_us /*= NULL*/) const
    {
      int64_t now = tbsys::CTimeUtil::getTime();
      if (NULL != remain_us)
      {
        if (OB_LIKELY(ts_timeout_us_ > 0))
        {
          *remain_us = ts_timeout_us_ - now;
        }
        else
        {
          *remain_us = INT64_MAX; // no timeout
        }
      }
      return (ts_timeout_us_ > 0 && now > ts_timeout_us_);
    }

    inline void ObPhysicalPlan::set_result_set(ObResultSet *rs)
    {
      my_result_set_ = rs;
    }

    inline ObResultSet* ObPhysicalPlan::get_result_set()
    {
      return my_result_set_;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_PHYSICAL_PLAN_H */
