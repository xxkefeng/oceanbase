/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         ob_tablet_merger.h is for what ...
 *
 *  Version: $Id: ob_tablet_merger.h 12/25/2012 02:46:43 PM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#ifndef OB_CHUNKSERVER_OB_TABLET_MERGER_V0_H_
#define OB_CHUNKSERVER_OB_TABLET_MERGER_V0_H_

#include "common/ob_define.h"
#include "ob_tablet_merge_filter.h"
#include "sql/ob_tablet_scan.h"
#include "sstable/ob_sstable_writer.h"

namespace oceanbase
{
  namespace common
  {
    class ObTableSchema;
  }
  namespace sstable
  {
    class ObSSTableSchema;
  }
  namespace sql
  {
    class ObProject;
    class ObSqlScanParam;
  }

  namespace chunkserver
  {
    class ObTabletManager;
    class ObTablet;
    class ObChunkMerge;

    class ObTabletMerger
    {
      public:
        ObTabletMerger(ObChunkMerge& chunk_merge, ObTabletManager& manager);
        virtual ~ObTabletMerger();

        virtual int init();
        virtual int merge(ObTablet *tablet, int64_t frozen_version) = 0;

        int update_meta(ObTablet* old_tablet, const common::ObVector<ObTablet*> & tablet_array);
        int gen_sstable_file_location(const int32_t disk_no, 
            sstable::ObSSTableId& sstable_id, char* path, const int64_t path_size);
        int64_t calc_tablet_checksum(const int64_t checksum);

      protected:
        ObChunkMerge& chunk_merge_;
        ObTabletManager& manager_;

        ObTablet* old_tablet_;            // merge or import tablet object.
        int64_t frozen_version_;          // merge or import tablet new version.
        sstable::ObSSTableId sstable_id_; // current generate new sstable id
        char path_[common::OB_MAX_FILE_NAME_LENGTH]; // current generate new sstable path
        common::ObVector<ObTablet *> tablet_array_;  //generate new tablet objects.

        ObTabletMergerFilter tablet_merge_filter_;
        // for read tablet data..
        sql::ObSqlScanParam sql_scan_param_;
        sql::ObTabletScan tablet_scan_;
    };

    class ObTabletMergerV0 : public ObTabletMerger
    {
      public:
        ObTabletMergerV0(ObChunkMerge& chunk_merge, ObTabletManager& manager);
        ~ObTabletMergerV0() {}

        virtual int init();
        virtual int merge(ObTablet *tablet, int64_t frozen_version);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletMergerV0);

        int prepare_merge(ObTablet *tablet, int64_t frozen_version);
        int create_new_sstable();
        int create_hard_link_sstable();
        int finish_sstable(const common::ObNewRange& range, const bool is_sstable_split, const bool is_tablet_unchanged);

        int build_sstable_schema(const uint64_t table_id, sstable::ObSSTableSchema& sstable_schema);
        int build_project(const sstable::ObSSTableSchema& schema, sql::ObProject& project);
        int build_project(const sstable::ObSSTableSchemaColumnDef& def, sql::ObProject& project);
        int build_project(const sstable::ObSSTableSchemaColumnDef* def, const int64_t size, sql::ObProject& project);
        int build_project(const sstable::ObSSTableSchemaColumnDef** def, const int64_t size, sql::ObProject& project);
        int build_sql_scan_param(const sstable::ObSSTableSchema& schema, sql::ObSqlScanParam& scan_param);

        int wait_aio_buffer() const;
        int reset();
        int open();
        int do_merge();

        int build_extend_info(const bool is_tablet_unchanged, const int64_t size, ObTabletExtendInfo& extend_info);
        int build_new_tablet(const bool is_tablet_unchanged, const int64_t size, ObTablet* &tablet);
        int cleanup_uncomplete_sstable_files();

        int update_range_start_key(
            const common::ObNewRange& base_range, 
            const common::ObRowkey& split_rowkey, 
            common::ObMemBufAllocatorWrapper& allocator,
            common::ObNewRange & new_range);
        int update_range_end_key(
            const common::ObNewRange& base_range, 
            const common::ObRowkey & split_rowkey,
            common::ObMemBufAllocatorWrapper& allocator,
            common::ObNewRange &new_range);
        int64_t calc_split_row_num(const ObTablet *tablet, 
            int64_t max_sstable_size, int64_t over_size_percent);
        int append_row(const common::ObRow& row, 
            const int64_t max_sstable_size, 
            const int64_t current_row_count,
            int64_t &split_row_num,
            bool &is_tablet_splited);

      private:

        sstable::ObSSTableSchema sstable_schema_;
        common::ObTableSchema *table_schema_;

        // for write merged new sstable
        sstable::ObSSTableWriter writer_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif //OB_CHUNKSERVER_OB_TABLET_MERGER_V2_H_


