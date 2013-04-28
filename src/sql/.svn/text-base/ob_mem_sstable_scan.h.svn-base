/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_mem_sstable_scan.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_MEM_SSTABLE_SCAN_H
#define _OB_MEM_SSTABLE_SCAN_H 1

#include "ob_phy_operator.h"
#include "ob_values.h"
#include "ob_table_rpc_scan.h"
namespace oceanbase
{
  namespace sql
  {
    class ObMemSSTableScan : public ObPhyOperator
    {
      public:
        ObMemSSTableScan();
        virtual ~ObMemSSTableScan();

        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

        virtual int open();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int close();
        virtual ObPhyOperatorType get_type() const;
        void set_tmp_table(ObValues *tmp_table) {tmp_table_ = tmp_table;};

        NEED_SERIALIZE_AND_DESERIALIZE;

      private:
        ObRow cur_row_;
        ObRowDesc cur_row_desc_;
        ObRowStore row_store_;
        bool from_deserialize_;
        ObValues *tmp_table_;
    };
  }
}

#endif /* _OB_MEM_SSTABLE_SCAN_H */
