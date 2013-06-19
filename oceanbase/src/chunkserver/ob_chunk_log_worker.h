/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_chunk_log_worker.h
 *
 * Authors:
 *   zian <yunliang.shi@alipay.com>
 *
 */

#ifndef OCEANBASE_CHUNK_SERVER_LOG_WORKER_H_
#define OCEANBASE_CHUNK_SERVER_LOG_WORKER_H_

#include "common/ob_define.h"
#include "sstable/ob_disk_path.h"
#include "ob_tablet.h"
#include "common/ob_log_entry.h"
#include "common/page_arena.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObChunkLogManager;
    class ObTabletManager;

    struct ObLogBuf
    {
      char* buf_;
      int64_t pos_;
      int64_t buf_size_;

      ObLogBuf():
        buf_(NULL), pos_(0), buf_size_(0){}

    };

    class ObChunkLogWorker
    {

      enum LogSubCommand
      {
        OB_CS_SET_REMOVED = 1,
        OB_CS_SET_MERGED =  2,
        OB_CS_DEL_TABLET =  3,
        OB_CS_DEL_TABLE =   4,
        OB_CS_ADD_TABLET =  5,
        OB_CS_SET_NEXT_BROTHER = 6,
      };

      public:
        ObChunkLogWorker();
        ~ObChunkLogWorker()
        {

        }

        void set_tablet_manager(ObTabletManager* manager)
        {
          manager_ = manager;
        }

        void set_log_manager(ObChunkLogManager* log_manager)
        {
          log_manager_ = log_manager;
        }

        void set_recovered(int recovered = true)
        {
          recovered_ = recovered;
        }

        /* log*/
        //set next brother flag
        int set_with_next_brother(const int64_t data_version, const common::ObNewRange& range, const int status = 0);

        //set merged flag
        int set_tablet_merged(const int64_t data_version, const common::ObNewRange& range, const int status = 1);

        //delete one table
        int delete_table(const uint64_t table_id);

        //delete one tablet
        int remove_tablet(const int64_t data_version, const common::ObNewRange& range, const int32_t disk_no);

        //add one tablet
        int add_tablet(const int64_t data_version, const ObTablet* const tablet, const int8_t for_create = 0);

        //apply
        int apply(common::LogCommand cmd, const char* log_data, const int64_t& data_len);

        //flush log
        int flush_log(const common::LogCommand cmd, bool flush = true);

      private:
        //get the seialize buf
        ObLogBuf* get_log_buf();

        //sub apply
        int do_sub_apply(LogSubCommand cmd, const char* log_data, const int64_t& data_len, int64_t& pos);

        /* for replay */
        int do_check_point(const char* log_data, const int64_t& data_len);

        //parse merge flag
        int do_tablet_merged(const char* log_data, const int64_t& log_length, int64_t& pos);

        //parse removed flag
        int do_set_with_next_brother(const char* log_data, const int64_t& log_length, int64_t& pos);

        //parse delete table
        int do_delete_table(const char* log_data, const int64_t& log_length, int64_t& pos);

        //parse delete one tablet
        int do_remove_tablet(const char* log_data, const int64_t& log_length, int64_t& pos);

        //parse add one tablet
        int do_add_tablet(const char* log_data, const int64_t& log_length, int64_t& pos);

      private:
        bool recovered_;
        ObChunkLogManager* log_manager_;
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        ObTabletManager* manager_;
    };
  }
}
#endif
