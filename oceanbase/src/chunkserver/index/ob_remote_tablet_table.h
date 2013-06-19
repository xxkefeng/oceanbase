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
#ifndef OCEANBASE_CHUNKSERVER_REMOTE_TABLET_TABLE_H_
#define OCEANBASE_CHUNKSERVER_REMOTE_TABLET_TABLE_H_

#include "sql/ob_remote_sstable_scan.h"
#include "ob_table.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObRemoteTabletTable : public ObTable
    {
      public:
        ObRemoteTabletTable(); 

        virtual ~ObRemoteTabletTable();

        virtual int append(const common::ObRow& row);

        virtual int close(const bool is_append_succ);

        virtual int open(const ObScanOptions& scan_options);

        virtual sql::ObPhyOperator* get_phyoperator() { return scanner_; }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObRemoteTabletTable);

        sql::ObRemoteSSTableScan* scanner_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_REMOTE_TABLET_TABLE_H_
