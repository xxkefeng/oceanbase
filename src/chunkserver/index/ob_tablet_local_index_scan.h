/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
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
#ifndef OCEANBASE_CHUNKSERVER_TABLET_LOCAL_INDEX_SCAN_H_
#define OCEANBASE_CHUNKSERVER_TABLET_LOCAL_INDEX_SCAN_H_

#include "sql/ob_sort.h"
#include "sql/ob_project.h"
#include "sql/ob_interval_sample.h"
#include "ob_tablet_sstable.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletLocalIndexScanner
    {
      public:
        ObTabletLocalIndexScanner()
        : input_table_(NULL),
          root_(NULL)
        {
        }
        ~ObTabletLocalIndexScanner() { }

        int open(ObTable& input_table, const ObScanOptions& scan_options);

        //must be called after open()
        inline sql::ObPhyOperator* get_phyoperator() 
        { 
          return root_; 
        }

      private:
        int build_scan_param(const ObScanOptions& scan_options);
        int fill_scan_columns(const ObScanOptions& scan_options);
        int init_input_table(const ObScanOptions& scan_options);
        int init_projecter(const ObScanOptions& scan_options);
        int add_project_column(const uint64_t data_table_id, 
                               const uint64_t index_table_id, 
                               const uint64_t column_id);
        int init_sorter(const ObScanOptions& scan_options);
        int init_sampler(const ObScanOptions& scan_options);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletLocalIndexScanner);

        sstable::ObSSTableScanParam scan_param_;
        ObTable* input_table_;
        sql::ObProject projecter_;
        sql::ObSort sorter_;
        sql::ObIntervalSample sampler_;
        sql::ObPhyOperator* root_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_TABLET_LOCAL_INDEX_SCAN_H_
