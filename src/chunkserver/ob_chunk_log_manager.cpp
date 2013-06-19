/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_chunk_log_manager.cpp for load new block cache.
 *
 * Authors:
 *   zian <yunliang.shi@alipay.com>
 *
 */

#include "ob_chunk_log_manager.h"
#include "ob_chunk_server_main.h"
#include "ob_chunk_server.h"
#include "common/ob_log_dir_scanner.h"
#include "common/ob_direct_log_reader.h"
#include "common/ob_log_reader.h"
#include "common/file_utils.h"
#include "common/file_directory_utils.h"

namespace oceanbase
{
  namespace chunkserver
  {
    const int ObChunkLogManager::DISK_STRING_MAX_LEN = 10 * common::OB_MAX_DISK_NUMBER;

    ObChunkLogManager::ObChunkLogManager()
      : ckpt_id_(0), replay_point_(0), max_log_id_(0), is_initialized_(false), is_log_dir_empty_(false)
    {
    }

    ObChunkLogManager::~ObChunkLogManager()
    {
    }

    int ObChunkLogManager::init(ObTabletManager* manager, const char* log_dir, int64_t log_file_max_size, int64_t log_sync_type)
    {
      int ret = OB_SUCCESS;

      if (is_initialized_)
      {
        TBSYS_LOG(ERROR, "rootserver's log manager has been initialized");
        ret = OB_INIT_TWICE;
      }

      log_worker_.set_tablet_manager(manager);
      log_worker_.set_log_manager(this);

      if (OB_SUCCESS == ret)
      {
        if (NULL == log_dir)
        {
          TBSYS_LOG(ERROR, "commit log directory is null");
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          int log_dir_len = static_cast<int32_t>(strlen(log_dir));
          if (log_dir_len >= OB_MAX_FILE_NAME_LENGTH)
          {
            TBSYS_LOG(ERROR, "Argument is invalid[log_dir_len=%d log_dir=%s]", log_dir_len, log_dir);
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            strncpy(log_dir_, log_dir, log_dir_len);
            log_dir_[log_dir_len] = '\0';
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObLogDirScanner scanner;

        ret = scanner.init(log_dir_);
        if (OB_SUCCESS != ret && OB_DISCONTINUOUS_LOG != ret)
        {
          TBSYS_LOG(ERROR, "ObLogDirScanner init error");
        }
        else
        {
          if (!scanner.has_ckpt())
          {
            // no check point
            replay_point_ = 1;
            if (!scanner.has_log())
            {
              max_log_id_ = 1;
              is_log_dir_empty_ = true;
            }
            else
            {
              ret = scanner.get_max_log_id(max_log_id_);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "ObLogDirScanner get_max_log_id error[ret=%d]", ret);
              }
            }
            if (OB_SUCCESS == ret)
            {
              TBSYS_LOG(INFO, "check point does not exist, take replay_point[replay_point_=%lu][ckpt_id=%lu]", replay_point_, ckpt_id_);
            }
          }
          else
          {
            // has checkpoint
            uint64_t min_log_id;

            ret = scanner.get_max_ckpt_id(ckpt_id_);

            if (ret == OB_SUCCESS)
            {
              ret = scanner.get_min_log_id(min_log_id);
            }

            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "get_min_log_id error[ret=%d]", ret);
              ret = OB_ERROR;
            }
            else
            {
              if (min_log_id > ckpt_id_ + 1)
              {
                TBSYS_LOG(ERROR, "missing log file[min_log_id=%lu ckeck_point=%lu", min_log_id, ckpt_id_);
                ret = OB_ERROR;
              }
              else
              {
                replay_point_ = ckpt_id_ + 1; // replay from next log file
              }
            }

            if (OB_SUCCESS == ret)
            {
              ret = scanner.get_max_log_id(max_log_id_);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "get_max_log_id error[ret=%d]", ret);
                ret = OB_ERROR;
              }
            }
          }
        }
      }

      if(OB_SUCCESS == ret && has_ckpt())
      {
        ret = parse_disk_array();
      }

      if (OB_SUCCESS == ret)
      {
        ret = ObLogWriter::init(log_dir, log_file_max_size, NULL, log_sync_type);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "ObLogWriter init failed[ret=%d]", ret);
        }
        else
        {
          is_initialized_ = true;
          TBSYS_LOG(INFO, "ObChunkLogMgr[this=%p] init succ[replay_point_=%lu max_log_id_=%lu]", this, replay_point_, max_log_id_);
        }
      }

      return ret;
    }

    const int32_t* ObChunkLogManager::get_disk_no_array(int32_t& disk_num) const
    {
      disk_num = disk_num_;
      return disk_no_array_;
    }

    int ObChunkLogManager::parse_disk_array()
    {
      char file_name[OB_MAX_FILE_NAME_LENGTH];
      int ret = 0;
      char disk_str[DISK_STRING_MAX_LEN];
      int disk_str_len = 0;
      FileUtils ckpt_file;
      int err = 0;
      err = snprintf(file_name, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir_, ckpt_id_, DEFAULT_CKPT_EXTENSION);

      if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH )
      {
        TBSYS_LOG(ERROR, "generate check point file name failed, error: %d", err);
        ret = OB_ERROR;
      }
      else if (!FileDirectoryUtils::exists(file_name))
      {
        ret = OB_FILE_NOT_EXIST;
        TBSYS_LOG(WARN, "checkpoint file[\"%s\"] does not exist", file_name);
      }
      else if (0 > (err = ckpt_file.open(file_name, O_RDONLY)))
      {
        ret = OB_IO_ERROR;
        TBSYS_LOG(WARN, "open file[\"%s\"] error[%s]", file_name, strerror(errno));
      }
      else if (0 > (disk_str_len = static_cast<int32_t>(ckpt_file.read(disk_str, sizeof(disk_str))))
          || disk_str_len >= (int)sizeof(disk_str))
      {
        ret = disk_str_len < 0 ? OB_IO_ERROR: OB_BUF_NOT_ENOUGH;
        TBSYS_LOG(ERROR, "read error[%s] or file contain invalid data[len=%d]", strerror(errno), disk_str_len);
      }
      else
      {
        disk_num_ = 0;

        char * disk_no_str = NULL;
        disk_no_str = strtok (disk_str," ");
        while (disk_no_str != NULL)
        {
          disk_no_array_[disk_num_++] = atoi(disk_no_str);
          disk_no_str = strtok (NULL, " ");
        }
      }

      return ret;
    }

    int ObChunkLogManager::replay_log()
    {
      int ret = OB_SUCCESS;

      ObDirectLogReader direct_reader;
      ObLogReader log_reader;

      char* log_data = NULL;
      int64_t log_length = 0;
      LogCommand cmd = OB_LOG_UNKNOWN;
      uint64_t seq = 0;

      TBSYS_LOG(INFO, "begin replay log");
      ret = check_inner_stat();

      if (ret == OB_SUCCESS && !is_log_dir_empty_)
      {
        ret = log_reader.init(&direct_reader, log_dir_, replay_point_, 0, false);
        if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "ObLogReader init failed, ret=%d", ret);
        }
        else
        {
          ret = log_reader.read_log(cmd, seq, log_data, log_length);
          if (OB_FILE_NOT_EXIST != ret && OB_READ_NOTHING != ret && OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "ObLogReader read error[ret=%d]", ret);
          }

          while (ret == OB_SUCCESS)
          {
            if (OB_LOG_NOP != cmd)
            {
              ret = log_worker_.apply(cmd, log_data, log_length);
              if (OB_SUCCESS != ret)
              {
                TBSYS_LOG(ERROR, "apply log failed:command[%d], log[%s], ret[%d]", cmd, log_data, ret);
                break;
              }
            }
            ret = log_reader.read_log(cmd, seq, log_data, log_length);
            if (OB_FILE_NOT_EXIST != ret && OB_READ_NOTHING != ret && OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "ObLogReader read error[ret=%d]", ret);
            }
          } // end while

          // handle exception, when the last log file contain SWITCH_LOG entry
          // but the next log file is missing
          if (OB_FILE_NOT_EXIST == ret)
          {
            max_log_id_++;
            ret = OB_SUCCESS;
          }

          // reach the end of commit log
          if (OB_READ_NOTHING == ret)
          {
            ret = OB_SUCCESS;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObLogCursor start_cursor;
        if (OB_SUCCESS != (ret = log_reader.get_next_cursor(start_cursor))
            && OB_READ_NOTHING != ret)
        {
          TBSYS_LOG(ERROR, "get_cursor(%s)=>%d", to_cstring(start_cursor), ret);
        }
        else if (OB_SUCCESS == ret)
        {}
        else
        {
          set_cursor(start_cursor, replay_point_, 1, 0);
          ret = OB_SUCCESS;
        }

        if (OB_SUCCESS != ret)
        {}
        else if (OB_SUCCESS != (ret = start_log(start_cursor)))
        {
          TBSYS_LOG(ERROR, "ObLogWriter start_log[%s]", to_cstring(start_cursor));
        }
        else
        {
          TBSYS_LOG(INFO, "start log [%s] done", to_cstring(start_cursor));
        }
      }
      return ret;
    }


    int ObChunkLogManager::do_check_point(const uint64_t ckpt_id, const int32_t* disk_no_array, const int32_t size)
    {
      // do check point
      int ret = OB_SUCCESS;

      // write server status into default checkpoint file
      char filename[OB_MAX_FILE_NAME_LENGTH];
      char filename_tmp[OB_MAX_FILE_NAME_LENGTH];
      int err = 0;
      int err_tmp = 0;
      FileUtils ckpt_file;
      err = snprintf(filename, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s", log_dir_, ckpt_id, DEFAULT_CKPT_EXTENSION);
      err_tmp = snprintf(filename_tmp, OB_MAX_FILE_NAME_LENGTH, "%s/%lu.%s.bak", log_dir_, ckpt_id, DEFAULT_CKPT_EXTENSION);
      if (err < 0 || err >= OB_MAX_FILE_NAME_LENGTH || err_tmp < 0 || err_tmp >= OB_MAX_FILE_NAME_LENGTH)
      {
        TBSYS_LOG(ERROR, "generate check point file name failed, error: %d error_tmp: %d", err, err_tmp);
        ret = OB_ERROR;
      }
      else
      {
        err = ckpt_file.open(filename_tmp, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);
        if (err < 0)
        {
          TBSYS_LOG(ERROR, "open check point file[%s] failed: %s", filename, strerror(errno));
          ret = OB_ERROR;
        }
        else
        {
          char disk_str[DISK_STRING_MAX_LEN];
          int disk_str_len = 0;
          for(int32_t i = 0; i < size; i++)
          {
            if(0 == i)
            {
              disk_str_len += snprintf(disk_str + disk_str_len, DISK_STRING_MAX_LEN, "%d", disk_no_array[i]);
            }
            else
            {
              disk_str_len += snprintf(disk_str + disk_str_len, DISK_STRING_MAX_LEN, " %d", disk_no_array[i]);
            }

            if(disk_str_len > DISK_STRING_MAX_LEN)
            {
              TBSYS_LOG(ERROR, "disk_str_len is too long[disk_str_len=%d DISK_STRING_MAX_LEN=%d", disk_str_len, DISK_STRING_MAX_LEN);
              ret = OB_ERROR;
            }
          }


          if(OB_SUCCESS == ret)
          {
            err = static_cast<int32_t>(ckpt_file.write(disk_str, disk_str_len));
            if (err < 0)
            {
              TBSYS_LOG(ERROR, "write error[%s]", strerror(errno));
              ret = OB_ERROR;
            }
          }

          ckpt_file.close();

          //rename
          if(OB_SUCCESS == ret)
          {
            if (!FileDirectoryUtils::rename(filename_tmp, filename))
            {
              TBSYS_LOG(ERROR, "rename src meta = %s to dst meta =%s error.", filename_tmp, filename);
              ret = OB_IO_ERROR;
            }
          }
        }
      }


      if (ret == OB_SUCCESS)
      {
        TBSYS_LOG(INFO, "do check point success, check point id: %lu => %lu", ckpt_id_, ckpt_id);
        ckpt_id_ = ckpt_id;
        replay_point_ = ckpt_id;
      }

      return ret;
    }

  }
}
