/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_phy_operator.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_PHY_OPERATOR_H
#define _OB_PHY_OPERATOR_H 1
#include "common/ob_row.h"
#include "ob_phy_operator_type.h"
#include "common/ob_define.h"
#include "sql/ob_multi_cg_scanner.h"
namespace oceanbase
{
  namespace sql
  {
    class ObPhysicalPlan;
    /// 物理运算符接口
    class ObPhyOperator : public ObRowIterator
    {
      public:
        ObPhyOperator();
        virtual ~ObPhyOperator() {}

        /// 添加子运算符，有些运算符（例如join）可能有多个子运算符。叶运算符无子运算符。
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator) = 0;

        /// 获取子预算符，child_idx对应的子操作符不存在会返回NULL
        virtual ObPhyOperator *get_child(int32_t child_idx) const
        {
          UNUSED(child_idx);
          return NULL;
        }

        virtual int32_t get_child_num() const
        {
          return 0;
        }

        inline void set_parent(ObPhyOperator *parent)
        {
          OB_ASSERT(NULL != parent);
          parent_op_ = parent;
        }

        inline ObPhyOperator *get_parent() const
        {
          return parent_op_;
        }

        virtual enum ObPhyOperatorType get_type() const
        {
          return PHY_INVALID;
        }

        /// 打开物理运算符。申请资源，打开子运算符等。构造row description；给子运算符传递配置等。
        virtual int open() = 0;

        /// 关闭物理运算符。释放资源，关闭子运算符等。
        /// @note If the operator is closed successfully, it could be opened and used again.
        virtual int close() = 0;

        /**
         * 获得下一行的引用
         * @note 在下次调用get_next或者close前，返回的row有效
         * @pre 调用open()
         * @param row [out]
         *
         * @return OB_SUCCESS或OB_ITER_END或错误码
         */
        virtual int get_next_row(const common::ObRow *&row) = 0;

        /**
         * get the row description
         * the row desc should have been valid after open() and before close()
         * @pre call open() first
         */
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const = 0;

        /**
         * 打印本物理运算符及其所有子运算符
         * @note 先打印本运算符，再打印所有子运算符
         * @param buf [in] 打印到该缓冲区
         * @param buf_len [in] 缓冲区大小
         *
         * @return 打印字符数
         */
        virtual int64_t to_string(char* buf, const int64_t buf_len) const = 0;

        /**
         * Set the physical plan object who owns this operator.
         *
         * @param the_plan
         */
        void set_phy_plan(ObPhysicalPlan *the_plan);

        VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
      private:
        DISALLOW_COPY_AND_ASSIGN(ObPhyOperator);
      protected:
        int64_t magic_;
        ObPhysicalPlan *my_phy_plan_; //< the physical plan object who owns this operator. Use this->my_phy_plan_->my_result_set_->my_session_ to get the environment.
        ObPhyOperator *parent_op_; //< any operator can find its parent through this clue
    };

    inline ObPhyOperator::ObPhyOperator()
      :magic_(0xABCD1986ABCD1986),
       my_phy_plan_(NULL),
       parent_op_(NULL)
    {
    }

    inline void ObPhyOperator::set_phy_plan(ObPhysicalPlan *the_plan)
    {
      my_phy_plan_ = the_plan;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_PHY_OPERATOR_H */
