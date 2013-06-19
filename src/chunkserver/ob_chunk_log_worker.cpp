/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_chunk_log_worker.cpp
 *
 * Authors:
 *   zian <yunliang.shi@alipay.com>
 *
 */
#include "ob_chunk_log_worker.h"
#include "ob_chunk_log_manager.h"
#include "common/ob_mod_define.h"
#include "ob_chunk_server_main.h"

namespace oceanbase
{
  namespace chunkserver
  {
    ObChunkLogWorker::ObChunkLogWorker():recovered_(false), mod_(ObModIds::OB_CS_COMMIT_LOG), allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_), manager_(NULL)
    {

    }

    int ObChunkLogWorker::set_tablet_merged(const int64_t data_version, const common::ObNewRange& range, const int status /*=1*/)
    {
      int ret = OB_SUCCESS;

      if(recovered_)
      {
        TBSYS_LOG(INFO, "set_tablet_merged data_version:%ld range:%s merged:%d", data_version, to_cstring(range), status);

        ObLogBuf* log_buf = get_log_buf();

        if(OB_SUCCESS != (ret = serialization::encode_vi32(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, OB_CS_SET_MERGED)))
        {
          TBSYS_LOG(ERROR, "serialize the cmd error: ret=[%d] sub_cmd=[%d]", ret, OB_CS_SET_MERGED);
        }
        else if(OB_SUCCESS != (ret = serialization::encode_vi64(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, data_version)))
        {
          TBSYS_LOG(ERROR, "serialize the data_version error: ret=[%d] data_version=[%ld]", ret, data_version);
        }
        else if(OB_SUCCESS != (ret = range.serialize(log_buf->buf_, log_buf->buf_size_, log_buf->pos_)))
        {
          TBSYS_LOG(ERROR, "serialize the range error: ret=[%d]", ret);
        }
        else  if(OB_SUCCESS != (ret = serialization::encode_vi32(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, status)))
        {
          TBSYS_LOG(ERROR, "serialize the merged error: ret=[%d] merged=[%d]", ret, status);
        }
      }

      return ret;
    }


    int ObChunkLogWorker::set_with_next_brother(const int64_t data_version, const common::ObNewRange& range, const int status /*= 0*/ )
    {
      int ret = OB_SUCCESS;

      if(recovered_)
      {
        TBSYS_LOG(INFO, "set_with_next_brother data_version:%ld range:%s brother:%d", data_version, to_cstring(range), status);

        ObLogBuf* log_buf = get_log_buf();

        if(OB_SUCCESS != (ret = serialization::encode_vi32(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, OB_CS_SET_NEXT_BROTHER)))
        {
          TBSYS_LOG(ERROR, "serialize the cmd error: ret=[%d] sub_cmd=[%d]", ret, OB_CS_SET_NEXT_BROTHER);
        }
        else  if(OB_SUCCESS != (ret = serialization::encode_vi64(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, data_version)))
        {
          TBSYS_LOG(ERROR, "serialize the data_version error: ret=[%d] data_version=[%ld]", ret, data_version);
        }
        else if(OB_SUCCESS != (ret = range.serialize(log_buf->buf_, log_buf->buf_size_, log_buf->pos_)))
        {
          TBSYS_LOG(ERROR, "serialize the range error: ret=[%d]", ret);
        }
        else  if(OB_SUCCESS != (ret = serialization::encode_vi32(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, status)))
        {
          TBSYS_LOG(ERROR, "serialize the merged error: ret=[%d] merged=[%d]", ret, status);
        }
      }
      return ret;
    }


    int ObChunkLogWorker::delete_table(const uint64_t table_id)
    {
      int ret = OB_SUCCESS;

      if(recovered_)
      {
        TBSYS_LOG(INFO, "delete_table table_id:%lu", table_id);

        ObLogBuf* log_buf = get_log_buf();

        if(OB_SUCCESS != (ret = serialization::encode_vi32(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, OB_CS_DEL_TABLE)))
        {
          TBSYS_LOG(ERROR, "serialize the cmd error: ret=[%d] sub_cmd=[%d]", ret, OB_CS_DEL_TABLE);
        }
        else  if(OB_SUCCESS != (ret = serialization::encode_vi64(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, table_id)))
        {
          TBSYS_LOG(ERROR, "serialize the table_id error: ret=[%d] table_id=[%ld]", ret, table_id);
        }
      }

      return ret;
    }


    int ObChunkLogWorker::remove_tablet(const int64_t data_version, const common::ObNewRange& range, const int32_t disk_no)
    {
      int ret = OB_SUCCESS;

      if(recovered_)
      {
        TBSYS_LOG(INFO, "remove_tablet data_version:%ld range:%s disk_no:%d", data_version, to_cstring(range), disk_no);

        ObLogBuf* log_buf = get_log_buf();

        if(OB_SUCCESS != (ret = serialization::encode_vi32(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, OB_CS_DEL_TABLET)))
        {
          TBSYS_LOG(ERROR, "serialize the cmd error: ret=[%d] sub_cmd=[%d]", ret, OB_CS_DEL_TABLE);
        }
        else  if(OB_SUCCESS != (ret = serialization::encode_vi64(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, data_version)))
        {
          TBSYS_LOG(ERROR, "serialize the data_version error: ret=[%d] data_version=[%ld]", ret, data_version);
        }
        else  if(OB_SUCCESS != (ret = serialization::encode_vi32(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, disk_no)))
        {
          TBSYS_LOG(ERROR, "serialize the disk_no error: ret=[%d] disk_no=[%d]", ret, disk_no);
        }
        else if(OB_SUCCESS != (ret = range.serialize(log_buf->buf_, log_buf->buf_size_, log_buf->pos_)))
        {
          TBSYS_LOG(ERROR, "serialize the range error: ret=[%d]", ret);
        }

      }
      return ret;
    }


    int ObChunkLogWorker::add_tablet(const int64_t data_version, const ObTablet* const tablet, int8_t for_create)
    {
      int ret = OB_SUCCESS;

      if(recovered_)
      {
        TBSYS_LOG(INFO, "add_tablet data_version:%ld range:%s disk_no:%d", data_version, to_cstring(tablet->get_range()), tablet->get_disk_no());

        ObLogBuf* log_buf = get_log_buf();

        ObTabletRangeInfo info;
        tablet->get_range_info(info);
        const common::ObNewRange range = tablet->get_range();
        const ObTabletExtendInfo extend = tablet->get_extend_info();

        if(OB_SUCCESS != (ret = serialization::encode_vi32(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, OB_CS_ADD_TABLET)))
        {
          TBSYS_LOG(ERROR, "serialize the cmd error: ret=[%d] sub_cmd=[%d]", ret, OB_CS_DEL_TABLE);
        }
        else if(OB_SUCCESS != (ret = serialization::encode_vi64(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, data_version)))
        {
          TBSYS_LOG(ERROR, "serialize the data_version error: ret=[%d] data_version=[%ld]", ret, data_version);
        }
        else if(OB_SUCCESS != (ret = serialization::encode_i8(log_buf->buf_, log_buf->buf_size_, log_buf->pos_, for_create)))
        {
          TBSYS_LOG(ERROR, "serialize the data_version error: ret=[%d] for_create=[%d]", ret, for_create);
        }
        else if(OB_SUCCESS != (ret = info.serialize(log_buf->buf_, log_buf->buf_size_, log_buf->pos_)))
        {
          TBSYS_LOG(ERROR, "serialize the range info error: ret=[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = range.serialize(log_buf->buf_, log_buf->buf_size_, log_buf->pos_)))
        {
          TBSYS_LOG(ERROR, "serialize the range error: ret=[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = extend.serialize(log_buf->buf_, log_buf->buf_size_, log_buf->pos_)))
        {
          TBSYS_LOG(ERROR, "serialize the extend info error: ret=[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = tablet->serialize(log_buf->buf_, log_buf->buf_size_, log_buf->pos_)))
        {
          TBSYS_LOG(ERROR, "serialize the tablet error: ret=[%d]", ret);
        }
      }

      return ret;
    }

    ObLogBuf* ObChunkLogWorker::get_log_buf()
    {
      static __thread ObLogBuf* log_buf = NULL;

      if(log_buf == NULL)
      {
        char* ptr = allocator_.alloc(sizeof(ObLogBuf));
        if(ptr)
        {
          log_buf = new (ptr) ObLogBuf();
          char * buf = allocator_.alloc(OB_MAX_PACKET_LENGTH);
          log_buf->buf_ = buf;
          log_buf->buf_size_ = OB_MAX_PACKET_LENGTH;
          log_buf->pos_ = 0;
        }
      }

      return log_buf;
    }

    int ObChunkLogWorker::flush_log(const LogCommand cmd, bool flush)
    {
      int ret = OB_SUCCESS;
      ObLogBuf* log_buf = get_log_buf();

      TBSYS_LOG(INFO, "flush commit log, cmd type: %d is_flush %d", cmd, flush);

      if(flush)
      {
        tbsys::CThreadGuard guard(log_manager_->get_log_sync_mutex());

        if(log_buf->pos_ > 0)
        {
          ret = log_manager_->write_and_flush_log(cmd, log_buf->buf_, log_buf->pos_);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "write and flush log failed:cmd[%d], log[%s], size[%ld], ret[%d]",
                cmd, log_buf->buf_, log_buf->pos_, ret);
          }
        }
      }

      //reset the buf
      log_buf->pos_ = 0;

      return ret;
    }


    int ObChunkLogWorker::do_sub_apply(LogSubCommand cmd, const char* log_data, const int64_t& data_len, int64_t& pos)
    {
      int ret = OB_SUCCESS;

      switch(cmd)
      {
        case OB_CS_SET_MERGED:
          ret = do_tablet_merged(log_data, data_len, pos);
          break;
        case OB_CS_DEL_TABLE:
          ret = do_delete_table(log_data, data_len, pos);
          break;
        case OB_CS_DEL_TABLET:
          ret = do_remove_tablet(log_data, data_len, pos);
          break;
        case OB_CS_SET_NEXT_BROTHER:
          ret = do_set_with_next_brother(log_data, data_len, pos);
          break;
        case OB_CS_ADD_TABLET:
          ret = do_add_tablet(log_data, data_len, pos);
          break;
        default:
          TBSYS_LOG(WARN, "unknow log command [%d]", cmd);
          ret = OB_INVALID_ARGUMENT;
          break;
      }

      return ret;
    }


    int ObChunkLogWorker::apply(common::LogCommand cmd, const char* log_data, const int64_t& data_len)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      LogSubCommand sub_cmd;

      switch(cmd)
      {
        case OB_LOG_SWITCH_LOG:
          TBSYS_LOG(INFO, "apply: switch_log");
          break;
        case OB_LOG_CS_DAILY_MERGE:
        case OB_LOG_CS_SRC_MIGRATE_TABLET:
        case OB_LOG_CS_DEST_LOAD_TABLET:
        case OB_LOG_CS_BYPASS_LOAD_TABLET:
        case OB_LOG_CS_MERGE_TABLET:
        case OB_LOG_CS_DEL_TABLE:
        case OB_LOG_CS_DEL_TABLET:
        case OB_LOG_CS_CREATE_TABLET:
          {
            while(pos < data_len)
            {
              if(OB_SUCCESS != (ret = serialization::decode_vi32(log_data, data_len, pos, reinterpret_cast<int32_t*>(&sub_cmd))))
              {
                TBSYS_LOG(ERROR, "deserialize the cmd error: ret=[%d]", ret);
              }
              else if(OB_SUCCESS != (ret = do_sub_apply(sub_cmd, log_data, data_len, pos)))
              {
                TBSYS_LOG(ERROR, "apply log error, ret=%d", ret);
              }
            }

            if(OB_SUCCESS == ret && pos != data_len)
            {
              TBSYS_LOG(ERROR, "log apply error, still has data not parse, data_len:%ld pos:%ld", data_len, pos);
              ret = OB_CS_APPLY_LOG_ERR;
            }

            break;
          }
        default:
          TBSYS_LOG(ERROR, "unknow log command [%d]", cmd);
          ret = OB_INVALID_ARGUMENT;
          break;
      }
      return ret;
    }

    int ObChunkLogWorker::do_tablet_merged(const char* log_data, const int64_t& log_length, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t data_version = 0;
      int status = 0;
      common::ObNewRange range;
      ObTablet* tablet = NULL;

      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);

      if(OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &data_version)))
      {
        TBSYS_LOG(ERROR, "deserialize the data_version error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = range.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize the range error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi32(log_data, log_length, pos, &status)))
      {
        TBSYS_LOG(ERROR, "deserialize the merged error: ret=[%d]", ret);
      }
      else
      {
        //apply the redo log
        ret = manager_->get_serving_tablet_image().acquire_tablet(range, ObMultiVersionTabletImage::SCAN_FORWARD, data_version, tablet);

        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "recover the tablet index error,  not fount the tablet, range=[%s]", to_cstring(range));
          ret = OB_CS_TABLET_NOT_EXIST;
        }
        else
        {
          tablet->set_merged(status);
          manager_->get_serving_tablet_image().release_tablet(tablet);
          TBSYS_LOG(INFO, "do_tablet_merged data_version:%ld range:%s merged:%d", data_version, to_cstring(tablet->get_range()), status);
        }

      }

      return ret;
    }


    int ObChunkLogWorker::do_set_with_next_brother(const char* log_data, const int64_t& log_length, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t data_version = 0;
      int status = 0;
      ObTablet* tablet = NULL;
      common::ObNewRange range;
      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);

      if(OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &data_version)))
      {
        TBSYS_LOG(ERROR, "deserialize the data_version error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = range.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize the range error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi32(log_data, log_length, pos, &status)))
      {
        TBSYS_LOG(ERROR, "deserialize the merged error: ret=[%d]", ret);
      }
      else
      {
        //apply the redo log
        ret = manager_->get_serving_tablet_image().acquire_tablet(range, ObMultiVersionTabletImage::SCAN_FORWARD, data_version, tablet);

        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "recover the tablet index error,  not fount the tablet, range=[%s]", to_cstring(range));
          ret = OB_CS_TABLET_NOT_EXIST;
        }
        else
        {
          tablet->set_with_next_brother(status);
          manager_->get_serving_tablet_image().release_tablet(tablet);
          TBSYS_LOG(INFO, "do_set_with_next_brother data_version:%ld range:%s merged:%d", data_version, to_cstring(tablet->get_range()), status);
        }
      }

      return ret;
    }



    int ObChunkLogWorker::do_delete_table(const char* log_data, const int64_t& log_length, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t table_id = 0;

      if(OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &table_id)))
      {
        TBSYS_LOG(ERROR, "deserialize the table_id error: ret=[%d]", ret);
      }
      else
      {
        //apply the redo log
        ret = manager_->get_serving_tablet_image().delete_table(table_id);
        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "delete table failed: table_id=[%ld] ret=[%d]", table_id, ret);
        }

        TBSYS_LOG(INFO, "do_delete_table table_id:%lu", table_id);
      }

      return ret;
    }

    int ObChunkLogWorker::do_remove_tablet(const char* log_data, const int64_t& log_length, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t data_version = 0;
      int32_t disk_no = -1;
      common::ObNewRange range;
      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);


      if(OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &data_version)))
      {
        TBSYS_LOG(ERROR, "deserialize the data_version error: ret=[%d]", ret);
      }
      if(OB_SUCCESS != (ret = serialization::decode_vi32(log_data, log_length, pos, &disk_no)))
      {
        TBSYS_LOG(ERROR, "deserialize the disk_no error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = range.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize the range error: ret=[%d]", ret);
      }
      else
      {
        //apply the redo log
        ret = manager_->get_serving_tablet_image().remove_tablet(range, data_version, disk_no);

        if(OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "delete tablet failed: ret=[%d]", ret);
        }

        TBSYS_LOG(INFO, "do_remove_tablet data_version:%ld range:%s disk_no:%d", data_version, to_cstring(range), disk_no);
      }

      return ret;
    }


    int ObChunkLogWorker::do_add_tablet(const char* log_data, const int64_t& log_length, int64_t& pos)
    {
      int ret = OB_SUCCESS;
      int64_t data_version = 0;
      int8_t for_create = 0;
      ObTablet* tablet = NULL;
      ObTabletRangeInfo info;
      ObTabletExtendInfo extend;

      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      ObNewRange range;
      range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);

      if(OB_SUCCESS != (ret = serialization::decode_vi64(log_data, log_length, pos, &data_version)))
      {
        TBSYS_LOG(ERROR, "deserialize the data_version error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = serialization::decode_i8(log_data, log_length, pos, &for_create)))
      {
        TBSYS_LOG(ERROR, "deserialize the for_create error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = info.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize the range info error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = range.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize the range error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = extend.deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize the extend info error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = manager_->get_serving_tablet_image().alloc_tablet_object(range, data_version, tablet)))
      {
        TBSYS_LOG(ERROR, "alloc tablet error: ret=[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = tablet->deserialize(log_data, log_length, pos)))
      {
        TBSYS_LOG(ERROR, "deserialize the tablet info error: ret=[%d]", ret);
      }
      else
      {
        tablet->set_merged(info.is_merged_);
        tablet->set_with_next_brother(info.is_with_next_brother_);
        tablet->set_extend_info(extend);

        bool load_sstable = !THE_CHUNK_SERVER.get_config().lazy_load_sstable;
        if(OB_SUCCESS != (ret = manager_->get_serving_tablet_image().add_tablet(tablet, load_sstable, for_create)))
        {
          TBSYS_LOG(ERROR, "add the tablet error: ret=[%d]", ret);
        }

        TBSYS_LOG(INFO, "do_add_tablet data_version:%ld range:%s disk_no:%d for_create:%d", data_version, to_cstring(tablet->get_range()), tablet->get_disk_no(), for_create);
      }

      return ret;
    }
  }
}
