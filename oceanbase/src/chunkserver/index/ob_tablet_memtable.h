/**
 * (C) 2010-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 *
 * Authors:
 *   huating <huating.zmq@alipay.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_TABLET_MEMTABLE_H_
#define OCEANBASE_CHUNKSERVER_TABLET_MEMTABLE_H_

#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_row_store.h"
#include "ob_table.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletMemtableScanner : public sql::ObNoChildrenPhyOperator
    {
      public:
        ObTabletMemtableScanner(common::ObRowStore& row_store, 
                                common::ObRowDesc& row_desc);
        virtual ~ObTabletMemtableScanner();

        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual int get_row_desc(const common::ObRowDesc*& row_desc) const;

        virtual int open();
        virtual int get_next_row(const common::ObRow*& row);
        virtual int close();

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletMemtableScanner);

        common::ObRow cur_row_;
        common::ObRowDesc& cur_row_desc_;
        common::ObRowStore& row_store_;
    };

    class ObTabletMemtable : public ObTable
    {
      public:
        ObTabletMemtable();
        virtual ~ObTabletMemtable();

        void reset();

        virtual int append(const common::ObRow& row);

        virtual int close(const bool is_append_succ);

        int open(const common::ObRowDesc& row_desc);

        virtual int open(const ObScanOptions& scan_options);

        virtual sql::ObPhyOperator* get_phyoperator() { return &scanner_; }

        inline int64_t get_row_count() const { return row_count_; }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletMemtable);

        volatile int64_t row_count_;
        common::ObRowDesc cur_row_desc_;
        common::ObRowStore row_store_;
        ObTabletMemtableScanner scanner_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_TABLET_MEMTABLE_H_
