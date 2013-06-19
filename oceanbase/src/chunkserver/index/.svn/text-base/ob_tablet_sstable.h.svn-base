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
#ifndef OCEANBASE_CHUNKSERVER_TABLET_SSTABLE_H_
#define OCEANBASE_CHUNKSERVER_TABLET_SSTABLE_H_

#include "compactsstablev2/ob_sstable_store_struct.h"
#include "compactsstablev2/ob_compact_sstable_writer.h"
#include "sql/ob_sstable_scan.h"
#include "sstable/ob_sstable_writer.h"
#include "ob_table.h"
#include "ob_index_tablet_image.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObBaseTabletSSTableAppender;
    class ObTabletCompactSSTableAppender;
    class ObTabletSSTableAppender;
    class ObTabletManager;

    struct ObAppendOptions
    {
      ObAppendOptions()
      {
        reset();
      }

      void reset()
      {
        version_range_.reset();
        tablet_range_.reset();
        schema_ = NULL;
        disk_manager_ = NULL;
        tablet_image_ = NULL;
        tablet_manager_ = NULL;
        data_tablet_ = NULL;
      }

      compactsstablev2::ObFrozenMinorVersionRange version_range_;
      common::ObNewRange tablet_range_;
      const common::ObSchemaManagerV2* schema_;
      ObDiskManager* disk_manager_;
      ObBasicTabletImage* tablet_image_;
      ObTabletManager* tablet_manager_;
      ObTablet* data_tablet_;
      int64_t write_sstable_version_; 
    };

    class ObTabletSSTable : public ObTable
    {
      public:
        ObTabletSSTable();
        virtual ~ObTabletSSTable();

        int open(ObAppendOptions& append_options);

        virtual int append(const common::ObRow& row); 

        virtual int close(const bool is_append_succ);

        virtual int open(const ObScanOptions& scan_options);

        virtual sql::ObPhyOperator* get_phyoperator() { return scanner_; }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletSSTable);
        ObBaseTabletSSTableAppender* writer_;
        ObTabletCompactSSTableAppender* compact_sstable_appender_;
        ObTabletSSTableAppender* sstable_appender_;
        sql::ObSSTableScan* scanner_;
    };

    class ObBaseTabletSSTableAppender
    {
      public: 
        ObBaseTabletSSTableAppender() { }

        virtual ~ObBaseTabletSSTableAppender() { }

        virtual int open(const ObAppendOptions& options) = 0;

        //no support sstable split
        virtual int append(const common::ObRow& row) = 0;

        virtual int close(const bool is_append_succ) = 0;

        static int64_t calc_tablet_checksum(const int64_t sstable_checksum);

      protected:
        virtual void build_extend_info(ObTabletExtendInfo& extend_info) = 0;
        int update_tablet_meta();
        void cleanup();

        struct Context
        {
          Context()
          {
            reset();
          }

          void reset();
          int build();
          int gen_sstable_path();
          int build_compressor_name(ObTableSchema& table_schema);
          int init_row_store_type();

          ObAppendOptions options_;
          sstable::ObSSTableId sstable_id_;
          int row_store_type_;
          int64_t table_count_;
          int64_t block_size_;
          common::ObString compressor_name_;
          common::ObString sstable_path_;
          char path_str_[common::OB_MAX_FILE_NAME_LENGTH];

          //only for sstable
          int64_t approx_space_usage_;
          int64_t sstable_size_;

          //only for compactsstablev2
          int64_t max_sstable_size_;
          int64_t min_split_sstable_size_;
        };

        Context context_;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObBaseTabletSSTableAppender);
    };

    class ObTabletCompactSSTableAppender : public ObBaseTabletSSTableAppender
    {
      public: 
        ObTabletCompactSSTableAppender() { }

        virtual ~ObTabletCompactSSTableAppender() { }

        virtual int open(const ObAppendOptions& options);

        //no support sstable split
        virtual int append(const common::ObRow& row);

        virtual int close(const bool is_append_succ);

      private:
        int create_sstable();
        virtual void build_extend_info(ObTabletExtendInfo& extend_info);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletCompactSSTableAppender);

        compactsstablev2::ObSSTableSchema sstable_schema_;
        compactsstablev2::ObCompactSSTableWriter writer_;
    };

    //This is for old sstable pattern
    class ObTabletSSTableAppender : public ObBaseTabletSSTableAppender
    {
      public: 
        ObTabletSSTableAppender() { }

        virtual ~ObTabletSSTableAppender() { }

        virtual int open(const ObAppendOptions& options);

        //no support sstable split
        virtual int append(const common::ObRow& row);

        virtual int close(const bool is_append_succ);

      private:
        virtual void build_extend_info(ObTabletExtendInfo& extend_info);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletSSTableAppender);

        sstable::ObSSTableSchema sstable_schema_;
        sstable::ObSSTableWriter writer_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_TABLET_SSTABLE_H_
