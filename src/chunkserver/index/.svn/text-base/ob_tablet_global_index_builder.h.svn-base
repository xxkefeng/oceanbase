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
#ifndef OCEANBASE_CHUNKSERVER_TABLET_GLOBAL_INDEX_BUILDER_H_
#define OCEANBASE_CHUNKSERVER_TABLET_GLOBAL_INDEX_BUILDER_H_

#include "sql/ob_no_children_phy_operator.h"
#include "ob_tablet_local_index_scan.h"
#include "ob_tablet_sstable.h"
#include "ob_remote_tablet_table.h"
#include "ob_tablet_predicator.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;
    class ObBasicTabletImage;
    class ObTabletDistribution;
    class ObTabletDistItem;
    class ObSortFileSetter;

    struct ObDataTabletScanStatus
    {
      ObDataTabletScanStatus()
      {
        reset();
      }

      void reset()
      {
        got_row_cnt_ = 0;
        item_ = NULL;
        complete_ = false;
      }

      int64_t got_row_cnt_;
      const ObTabletDistItem *item_;
      bool complete_;
    };

    class ObMultiLocalIndexScan : public sql::ObNoChildrenPhyOperator
    {
      public:
        ObMultiLocalIndexScan()
        : data_table_tablet_dist_(NULL),
          index_tablet_item_(NULL),
          scan_type_(INVALID_SCAN),
          cur_operator_(NULL)
        {
        }
        virtual ~ObMultiLocalIndexScan() { }

        int open_scan_context(const ObScanOptions& scan_options, 
                              const ObTabletDistribution& data_table_tablet_dist,
                              const ObTabletDistItem& index_tablet_item,
                              const common::ObServer& self_server,
                              bool& need_sort);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow*& row);
        virtual int get_row_desc(const common::ObRowDesc*& row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

      private:
        enum ScanType
        {
          INVALID_SCAN = 0,
          BROADCAST_SCAN,
          MIGRAGE_SCAN,
        };

        bool is_broadcast_scan();
        bool is_migrage_scan();

        int init_scan_type();
        int init_local_index_scan();
        int init_broadcast_scan();
        int init_migrate_scan();
        int setup_scanner();

        // switch to next uncomplete data tablet and iterate one row.
        int scan_next_data_tablet(const common::ObRow *&row);

        int scan_cur_replica(const common::ObRow *&row);

        const ObServer &get_selected_server(const ObTabletDistItem &item);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObMultiLocalIndexScan);

        const static int64_t WAIT_CS_BUILD_LOCAL_INDEX_INTERVAL = 5 * 1000 * 1000;
        const static int ONE_REPLICA_RETRY_TIMES = 3;

        ObScanOptions cur_scan_options_;
        const ObTabletDistribution* data_table_tablet_dist_;
        const ObTabletDistItem* index_tablet_item_;
        common::ObServer self_server_;

        ScanType scan_type_;
        ObTabletLocalIndexScanner scanner_;
        ObTabletSSTable local_input_sstable_;
        ObRemoteTabletTable remote_input_table_;

        sql::ObPhyOperator* cur_operator_;

        common::ObArray<ObDataTabletScanStatus> scan_status_array_;
        int64_t data_tablet_index_;
    };

    class ObTabletGlobalIndexScanner
    {
      public:
        ObTabletGlobalIndexScanner()
        : root_(NULL)
        {
        }
        ~ObTabletGlobalIndexScanner() { }

        int open(const ObScanOptions& scan_options, 
                 const ObTabletDistribution& data_table_tablet_dist,
                 const ObTabletDistItem& index_tablet_item,
                 const common::ObServer& self_server);

        //must be called after open()
        inline sql::ObPhyOperator* get_phyoperator() 
        {
          return root_; 
        }

      private:
        int open_sorter(const ObScanOptions& scan_options);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletGlobalIndexScanner);

        ObMultiLocalIndexScan local_index_scanner_;
        sql::ObSort sorter_;
        sql::ObPhyOperator* root_;
    };

    class ObTabletGlobalIndexBuilder
    {
      public: 
        ObTabletGlobalIndexBuilder(ObTabletManager& tablet_manager, 
                                   ObGlobalIndexTabletImage& tablet_image);
        ~ObTabletGlobalIndexBuilder();

        int build(ObTabletDistribution& data_table_tablet_dist, 
                  const common::ObSchemaManagerV2& schema, 
                  const ObTabletDistItem& index_tablet_item,
                  const ObRpcOptions& rpc_option,
                  const common::ObServer& self_server,
                  ObSortFileSetter &sort_file_setter,
                  const bool &stop,
                  const int64_t write_sstable_version = sstable::SSTableReader::COMPACT_SSTABLE_VERSION);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletGlobalIndexBuilder);

        ObTabletManager& tablet_manager_;
        ObBasicTabletImage& tablet_image_;

        ObTabletSSTable output_sstable_;
        ObTabletGlobalIndexScanner scanner_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_TABLET_GLOBAL_INDEX_BUILDER_H_
