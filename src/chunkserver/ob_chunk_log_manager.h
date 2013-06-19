/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_chunk_log_manager.h for load new block cache.
 *
 * Authors:
 *   zian <yunliang.shi@alipay.com>
 *
 */
#ifndef OCEANBASE_CHUNK_SERVER_LOG_MANAGER_H_
#define OCEANBASE_CHUNK_SERVER_LOG_MANAGER_H_

#include "ob_chunk_log_worker.h"
#include "common/ob_define.h"
#include "common/ob_log_writer.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObChunkLogWorker;
    class ObTabletManager;

    class ObChunkLogManager : public common::ObLogWriter
    {
      public:
        static const int DISK_STRING_MAX_LEN;
        ObChunkLogManager();
        ~ObChunkLogManager();

      public:
        int init(ObTabletManager* manager, const char* log_dir, int64_t log_file_max_size, int64_t log_sync_type);

        bool has_ckpt(){return ckpt_id_ > 0;}

        /// @brief replay all commit log from replay point
        /// after initialization, invoke this method to replay all commit log
        int replay_log();

        const int32_t *get_disk_no_array(int32_t& disk_num) const;

        int do_check_point(const uint64_t checkpoint_id, const int32_t* disk_no_array, const int32_t size);

        ObChunkLogWorker* get_log_worker()
        {
          return &log_worker_;
        }

        tbsys::CThreadMutex* get_log_sync_mutex() { return &log_sync_mutex_; }

      private:
        int parse_disk_array();

      private:
        const char* get_log_dir_path() const { return log_dir_; }
        inline uint64_t get_replay_point() {return replay_point_;}
        inline uint64_t get_check_point() {return ckpt_id_;}

        int32_t disk_no_array_[common::OB_MAX_DISK_NUMBER];
        int32_t disk_num_;

        uint64_t ckpt_id_;
        uint64_t replay_point_;
        uint64_t max_log_id_;
        bool is_initialized_;
        bool is_log_dir_empty_;
        char log_dir_[common::OB_MAX_FILE_NAME_LENGTH];
        ObChunkLogWorker log_worker_;
        tbsys::CThreadMutex log_sync_mutex_;
    };

  }
}

#endif
