/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_join.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_JOIN_H
#define _OB_TABLET_JOIN_H 1

#include "common/ob_define.h"
#include "common/ob_array.h"
#include "common/ob_cache.h"
#include "ob_phy_operator.h"
#include "common/ob_kv_storecache.h"
#include "common/ob_get_param.h"
#include "common/ob_row_store.h"
#include "ob_ups_multi_get.h"
#include "common/thread_buffer.h"
#include "ob_multiple_merge.h"

namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    namespace test
    {
      class ObTabletJoinTest_fetch_ups_row_Test;
      class ObTabletJoinTest_compose_get_param_Test;
      class ObTabletJoinTest_gen_ups_row_desc_Test;
      class ObTabletJoinTest_get_right_table_rowkey_Test;
      class ObTabletJoinTest_fetch_fused_row_Test;

      class ObTabletScanTest_create_plan_join_Test;
      class ObTabletScanTest_create_plan_not_join_Test;
    }

    class ObTabletJoin: public ObPhyOperator
    {
      public:
        struct JoinInfo
        {
          uint64_t left_column_id_;
          uint64_t right_column_id_;

          JoinInfo();
        };

        struct TableJoinInfo
        {
          uint64_t left_table_id_; //左表table_id
          uint64_t right_table_id_;

          /* join条件信息，左表的column对应于右表的rowkey */
          ObArray<uint64_t> join_condition_;

          /* 指示左表哪些列是从右表取过来的 */
          ObArray<JoinInfo> join_column_;
          private:
            int serialize_uint64(char *buf, const int64_t buf_len, int64_t& pos, const uint64_t &u_val) const
            {
              int ret = OB_SUCCESS;
              ObObj obj;
              const int64_t *cond = reinterpret_cast<const int64_t *>(&u_val);
              obj.set_int(*cond);
              ret = obj.serialize(buf, buf_len, pos);
              return ret;
            }
            int deserialize_uint64(const char *buf, const int64_t data_len, int64_t& pos, uint64_t &u_val) const
            {
              int ret = OB_SUCCESS;
              ObObj obj;
              int64_t val;
              int64_t *cond = reinterpret_cast<int64_t *>(&u_val);
              if (OB_SUCCESS == (ret = obj.deserialize(buf, data_len, pos)))
              {
                if (OB_SUCCESS == (ret = obj.get_int(val)))
                {
                  *cond = val;
                }
              }
              return ret;
            }
          public:
            TableJoinInfo();

            int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
            {
              int ret = OB_SUCCESS;
              ObObj obj;
              int64_t i = 0;

              if (OB_SUCCESS != (ret = serialize_uint64(buf, buf_len, pos, left_table_id_)))
              {
                TBSYS_LOG(WARN, "fail to serialize table id. ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = serialize_uint64(buf, buf_len, pos, right_table_id_)))
              {
                TBSYS_LOG(WARN, "fail to serialize table id. ret=%d", ret);
              }
              else
              {
                obj.set_int(join_condition_.count());
                if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
                {
                  TBSYS_LOG(WARN, "fail to serialize object. ret=%d", ret);
                }
                for (i = 0; OB_SUCCESS == ret && i < join_condition_.count(); i++)
                {
                  const uint64_t &cond = join_condition_.at(i);
                  if (OB_SUCCESS != (ret = serialize_uint64(buf, buf_len, pos, cond)))
                  {
                    TBSYS_LOG(WARN, "fail to serialize table id. ret=%d", ret);
                  }
                }
                obj.set_int(join_column_.count());
                if (OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
                {
                  TBSYS_LOG(WARN, "fail to serialize object. ret=%d", ret);
                }
                for (i = 0; OB_SUCCESS == ret && i < join_column_.count(); i++)
                {
                  const JoinInfo & join_info = join_column_.at(i);
                  if (OB_SUCCESS != (ret = serialize_uint64(buf, buf_len, pos, join_info.left_column_id_)))
                  {
                    TBSYS_LOG(WARN, "fail to serialize table id. ret=%d", ret);
                  }
                  else if (OB_SUCCESS != (ret = serialize_uint64(buf, buf_len, pos, join_info.right_column_id_)))
                  {
                    TBSYS_LOG(WARN, "fail to serialize table id. ret=%d", ret);
                  }
                }
              }
              return ret;
            }

            int deserialize(const char* buf, const int64_t data_len, int64_t& pos)
            {
              int ret = OB_SUCCESS;
              ObObj obj;
              int64_t i = 0;
              int64_t array_len = 0;
              JoinInfo join_info;
              uint64_t tmp_cond = 0;

              if (OB_SUCCESS != (ret = deserialize_uint64(buf, data_len, pos, left_table_id_)))
              {
                TBSYS_LOG(WARN, "fail to deserialize table id. ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = deserialize_uint64(buf, data_len, pos, right_table_id_)))
              {
                TBSYS_LOG(WARN, "fail to deserialize table id. ret=%d", ret);
              }
              else
              {
                if (OB_SUCCESS == ret)
                {
                  if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
                  {
                    TBSYS_LOG(WARN, "fail to deserialize table id. ret=%d", ret);
                  }
                  else if (OB_SUCCESS != (ret = obj.get_int(array_len)))
                  {
                    TBSYS_LOG(WARN, "fail to get int. type=%d, ret=%d", obj.get_type(), ret);
                  }
                  for (i = 0; OB_SUCCESS == ret && i < array_len; i++)
                  {
                    if (OB_SUCCESS != (ret = deserialize_uint64(buf, data_len, pos, tmp_cond)))
                    {
                      TBSYS_LOG(WARN, "fail to serialize table id. ret=%d", ret);
                    }
                    if (OB_SUCCESS != (ret = join_condition_.push_back(tmp_cond)))
                    {
                      TBSYS_LOG(WARN, "fail to push condition to array. ret=%d", ret);
                    }
                  }
                }
                if (OB_SUCCESS == ret)
                {
                  if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
                  {
                    TBSYS_LOG(WARN, "fail to deserialize table id. ret=%d", ret);
                  }
                  else if (OB_SUCCESS != (ret = obj.get_int(array_len)))
                  {
                    TBSYS_LOG(WARN, "fail to get int. type=%d, ret=%d", obj.get_type(), ret);
                  }
                  for (i = 0; OB_SUCCESS == ret && i < array_len; i++)
                  {
                    if (OB_SUCCESS != (ret = deserialize_uint64(buf, data_len, pos, join_info.left_column_id_)))
                    {
                      TBSYS_LOG(WARN, "fail to serialize table id. ret=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = deserialize_uint64(buf, data_len, pos, join_info.right_column_id_)))
                    {
                      TBSYS_LOG(WARN, "fail to serialize table id. ret=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = join_column_.push_back(join_info)))
                    {
                      TBSYS_LOG(WARN, "fail to push join info to array. ret=%d", ret);
                    }
                  }
                }
              }
              return ret;
            }
            inline int64_t get_serialize_size(void) const
            {
              return 0;
            }

        };

      public:
        ObTabletJoin();
        virtual ~ObTabletJoin();

        int set_child(int32_t child_idx, ObPhyOperator &child_operator);

        virtual int open();
        virtual int close();
        virtual void reset();

        int get_next_row(const ObRow *&row);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int get_row_desc(const common::ObRowDesc *&row_desc) const; 

        void set_network_timeout(int64_t network_timeout);
        inline void set_batch_count(const int64_t batch_count);
        inline void set_table_join_info(const TableJoinInfo &table_join_info);
        inline void set_right_table_rowkey_info(const ObRowkeyInfo *right_table_rowkey_info);
        inline void set_is_read_consistency(bool is_read_consistency)
        {
          is_read_consistency_ = is_read_consistency;
        }

        inline int set_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy)
        {
          return ups_multi_get_.set_rpc_proxy(rpc_proxy);
        }

        inline void set_version_range(const ObVersionRange &version_range)
        {
          version_range_ = version_range;
        }

        friend class test::ObTabletScanTest_create_plan_join_Test;
        friend class test::ObTabletScanTest_create_plan_not_join_Test;
        friend class test::ObTabletJoinTest_compose_get_param_Test;
        friend class test::ObTabletJoinTest_gen_ups_row_desc_Test;
        friend class test::ObTabletJoinTest_get_right_table_rowkey_Test;
        friend class test::ObTabletJoinTest_fetch_fused_row_Test;
        
      protected:
        // disallow copy
        ObTabletJoin(const ObTabletJoin &other);
        ObTabletJoin& operator=(const ObTabletJoin &other);

        int get_right_table_rowkey(const ObRow &row, ObRowkey &rowkey, ObObj *rowkey_obj) const;
        int compose_get_param(uint64_t table_id, const ObRowkey &rowkey, ObGetParam &get_param);
        bool check_inner_stat();

        int fetch_next_batch_row(ObGetParam *get_param);
        int fetch_fused_row(ObGetParam *get_param);

        int gen_ups_row_desc();

        virtual int fetch_fused_row_prepare() = 0;
        virtual int get_ups_row(const ObRowkey &rowkey, ObRow &row, const ObGetParam &get_param) = 0;
        virtual int gen_get_param(ObGetParam &get_param, const ObRow &fused_row) = 0;

      protected:
        // data members
        TableJoinInfo table_join_info_;
        int64_t batch_count_;
        bool    fused_row_iter_end_;
        ObMultipleMerge *join_merger_;
        ObUpsMultiGet ups_multi_get_;
        ObRowStore fused_row_store_;
        ThreadSpecificBuffer thread_buffer_;
        ObRowDesc ups_row_desc_;
        ObRowDesc ups_row_desc_for_join_;
        ObRow curr_row_;
        const ObRow *fused_row_;
        ObArray<const ObRowStore::StoredRow *> fused_row_array_;
        int64_t fused_row_idx_;
        bool is_read_consistency_;
        int64_t valid_fused_row_count_;
        ObVersionRange version_range_;
        const ObRowkeyInfo *right_table_rowkey_info_;
    };

    void ObTabletJoin::set_table_join_info(const TableJoinInfo &table_join_info)
    {
      table_join_info_ = table_join_info;
    }

    void ObTabletJoin::set_right_table_rowkey_info(const ObRowkeyInfo *right_table_rowkey_info)
    {
      right_table_rowkey_info_ = right_table_rowkey_info;
    }

    void ObTabletJoin::set_batch_count(const int64_t batch_count)
    {
      batch_count_ = batch_count;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLET_JOIN_H */
