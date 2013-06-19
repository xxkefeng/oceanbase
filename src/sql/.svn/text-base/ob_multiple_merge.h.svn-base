/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_multiple_merge.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_MULTIPLE_MERGE_H
#define _OB_MULTIPLE_MERGE_H 1

#include "ob_phy_operator.h"
#include "ob_cur_rowkey_interface.h"

namespace oceanbase
{
  namespace sql
  {
    using namespace common;

    class ObMultipleMerge : public ObPhyOperator, public ObCurRowkeyInterface
    {
      public:
        ObMultipleMerge();
        virtual ~ObMultipleMerge();
        virtual void reset();
        virtual void reuse();
        static const int64_t MAX_CHILD_OPERATOR_NUM = 128;
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator );
        virtual ObPhyOperator *get_child(int32_t child_idx) const;

        virtual int32_t get_child_num() const;
        
        virtual int get_row_desc(const ObRowDesc *&row_desc) const;
        void set_is_ups_row(bool is_ups_row);

        int get_cur_rowkey(const common::ObRowkey *&rowkey) const;

        VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;

      protected:
        int copy_rowkey(const ObRow &row, ObRow &result_row, bool deep_copy);
      
      protected:
        CharArena allocator_;
        ObPhyOperator *child_array_[MAX_CHILD_OPERATOR_NUM];
        int32_t child_num_;
        // default value of row, true for ObRow::DEFAULT_NOP, false for ObRow::DEFAULT_NULL
        bool is_ups_row_; // default true
        ObRow cur_row_;
        const ObRowkey *cur_rowkey_;
    };
  }
}

#endif /* _OB_MULTIPLE_MERGE_H */
