/*
 *  (C) 2007-2010 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         ????.cpp is for what ...
 *
 *  Version: $Id: ipvsadm.c,v 1.27 2005/12/10 16:00:07 wensong Exp $
 *
 *  Authors:
 *     Author Name <email address>
 *        - some work details if you want
 */

#include "ob_tablet.h"
#include "common/ob_crc64.h"
#include "common/utility.h"
#include "sstable/ob_sstable_reader.h"
#include "ob_tablet_image.h"

using namespace oceanbase::common;
using namespace oceanbase::sstable;
using namespace oceanbase::compactsstable;

namespace oceanbase 
{ 
  namespace chunkserver 
  {
    //----------------------------------------
    // struct ObTabletRangeInfo
    //----------------------------------------
    DEFINE_SERIALIZE(ObTabletRangeInfo)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len)) 
      {
        ret = OB_ERROR;
      }

      if ( OB_SUCCESS == ret
          && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, start_key_size_)
          && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, end_key_size_)
          && OB_SUCCESS == serialization::encode_i8 (buf, buf_len, pos, reserved8_)
          && OB_SUCCESS == serialization::encode_i8 (buf, buf_len, pos, is_merged_)
          && OB_SUCCESS == serialization::encode_i8 (buf, buf_len, pos, is_with_next_brother_)
          && OB_SUCCESS == serialization::encode_i8(buf, buf_len, pos, border_flag_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, table_id_))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletRangeInfo)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();

      if (NULL == buf || serialize_size > data_len)
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret
          && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &start_key_size_)
          && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &end_key_size_)
          && OB_SUCCESS == serialization::decode_i8 (buf, data_len, pos, &reserved8_)
          && OB_SUCCESS == serialization::decode_i8 (buf, data_len, pos, &is_merged_)
          && OB_SUCCESS == serialization::decode_i8 (buf, data_len, pos, &is_with_next_brother_)
          && OB_SUCCESS == serialization::decode_i8 (buf, data_len, pos, &border_flag_)
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &table_id_))
      {
        ret = OB_SUCCESS;
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletRangeInfo)
    {
      int64_t total_size = 0;

      total_size += serialization::encoded_length_i16(start_key_size_);
      total_size += serialization::encoded_length_i16(end_key_size_);
      total_size += serialization::encoded_length_i8(is_merged_);
      total_size += serialization::encoded_length_i8(reserved8_);
      total_size += serialization::encoded_length_i8(is_with_next_brother_);
      total_size += serialization::encoded_length_i8(border_flag_);
      total_size += serialization::encoded_length_i64(table_id_);

      return total_size;
    }

    //----------------------------------------
    // struct ObTabletExtendInfo 
    //----------------------------------------
    DEFINE_SERIALIZE(ObTabletExtendInfo)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();

      if((NULL == buf) || (serialize_size + pos > buf_len))
      {
        ret = OB_ERROR;
      }

      if ( OB_SUCCESS == ret
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, row_count_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, occupy_size_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, check_sum_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, row_checksum_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, last_do_expire_version_)
          && OB_SUCCESS == serialization::encode_i64(buf, buf_len, pos, sequence_num_)
          && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, sstable_version_)
          && OB_SUCCESS == serialization::encode_i16(buf, buf_len, pos, reserved16_)
          && OB_SUCCESS == serialization::encode_i32(buf, buf_len, pos, reserved32_) )
      {
        for (int64_t i = 0; i < RESERVED_LEN && OB_SUCCESS == ret; ++i)
        {
          ret = serialization::encode_i64(buf, buf_len, pos, reserved_[i]);
        }
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTabletExtendInfo)
    {
      int ret = OB_SUCCESS;
      int64_t serialize_size = get_serialize_size();

      if (NULL == buf || serialize_size > data_len)
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &row_count_)
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &occupy_size_)
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&check_sum_))
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, reinterpret_cast<int64_t*>(&row_checksum_))
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &last_do_expire_version_)
          && OB_SUCCESS == serialization::decode_i64(buf, data_len, pos, &sequence_num_)
          && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &sstable_version_)
          && OB_SUCCESS == serialization::decode_i16(buf, data_len, pos, &reserved16_)
          && OB_SUCCESS == serialization::decode_i32(buf, data_len, pos, &reserved32_)
          )
      {
        for (int64_t i = 0; i < RESERVED_LEN && OB_SUCCESS == ret; ++i)
        {
          ret = serialization::decode_i64(buf, data_len, pos, &reserved_[i]);
        }
      }
      else
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTabletExtendInfo)
    {
      int64_t total_size = 0;

      total_size += serialization::encoded_length_i64(row_count_);
      total_size += serialization::encoded_length_i64(occupy_size_);
      total_size += serialization::encoded_length_i64(check_sum_);
      total_size += serialization::encoded_length_i64(row_checksum_);
      total_size += serialization::encoded_length_i64(last_do_expire_version_);
      total_size += serialization::encoded_length_i64(sequence_num_);
      total_size += serialization::encoded_length_i16(sstable_version_);
      total_size += serialization::encoded_length_i16(reserved16_);
      total_size += serialization::encoded_length_i32(reserved32_);
      for (int64_t i = 0; i < RESERVED_LEN; ++i)
      {
        total_size += serialization::encoded_length_i64(reserved_[i]);
      }

      return total_size;
    }



    //----------------------------------------
    // class ObTablet
    //----------------------------------------
    ObTablet::ObTablet(ObTabletImage* image) : sstable_reader_(NULL)
    {
      reset();
      image_ = image;
    }

    ObTablet::~ObTablet()
    {
      destroy();
    }

    void ObTablet::destroy()
    {
      // ObTabletImage will free reader's memory on destory.
      if (0 != ref_count_)
      {
        TBSYS_LOG(ERROR, "try to destroy tablet, but ref count is not 0, "
                         "ref_count_=%u",
          ref_count_);
      }

      if(NULL != sstable_reader_)
      {
        sstable_reader_->~SSTableReader();
        sstable_reader_ = NULL;
      }

      if (NULL != local_index_)
      {
        local_index_->~ObTablet();
      }
      reset();
    }

    void ObTablet::reset()
    {
      sstable_loaded_ = OB_NOT_INIT;
      merged_ = 0;
      merge_count_ = 0;
      disk_no_ = 0;
      data_version_ = 0;
      ref_count_ = 0;
      image_ = NULL;
      memset(&extend_info_, 0, sizeof(extend_info_));
      sstable_id_.reset();
      sstable_reader_ = NULL;
      local_index_state_ = UNAVALIABLE;
      local_index_ = NULL;
    }

    int ObTablet::add_sstable_by_id(const ObSSTableId& sstable_id)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == sstable_loaded_)
      {
        TBSYS_LOG(ERROR, "sstable already loaded, cannot add.");
        ret = OB_INIT_TWICE;
      }
      // check disk no?
      else if (0 != disk_no_ && disk_no_ != static_cast<int32_t>(get_sstable_disk_no(sstable_id.sstable_file_id_)))
      {
        TBSYS_LOG(ERROR, "add file :%ld not in same disk:%d",
            sstable_id.sstable_file_id_, disk_no_);
        ret = OB_ERROR;
      }
      else
      {
        sstable_id_ =  sstable_id;
      }

      return ret;
    }

    int ObTablet::set_range_by_info(const ObTabletRangeInfo& info,
        char* row_key_stream_ptr, const int64_t row_key_stream_size)
    {
      int ret = OB_SUCCESS;

      if (info.start_key_size_ + info.end_key_size_ > row_key_stream_size)
      {
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        range_.start_key_.deserialize_from_stream(row_key_stream_ptr, info.start_key_size_);
        range_.end_key_.deserialize_from_stream(row_key_stream_ptr + info.start_key_size_,
            info.end_key_size_);
        range_.table_id_ = info.table_id_;
        range_.border_flag_.set_data(static_cast<int8_t>(info.border_flag_));
        merged_ = info.is_merged_;
        with_next_brother_ = info.is_with_next_brother_;
        if (range_.border_flag_.is_min_value())
        {
          range_.start_key_.set_min_row();
        }
        if (range_.border_flag_.is_max_value())
        {
          range_.end_key_.set_max_row();
        }
      }
      return ret;
    }

    void ObTablet::get_range_info(ObTabletRangeInfo& info) const
    {
      info.start_key_size_ = static_cast<int16_t>(range_.start_key_.get_serialize_objs_size());
      info.end_key_size_ = static_cast<int16_t>(range_.end_key_.get_serialize_objs_size());
      info.table_id_ = range_.table_id_;
      info.border_flag_ = range_.border_flag_.get_data();
      info.is_merged_ = static_cast<int8_t>(merged_);
      info.is_with_next_brother_ = static_cast<int8_t>(with_next_brother_);
    }

    DEFINE_SERIALIZE(ObTablet)
    {
      int ret = OB_ERROR;
      ret = serialization::encode_vi64(buf, buf_len, pos, data_version_);

      int64_t size = 1;
      ret = serialization::encode_vi64(buf, buf_len, pos, size);

      if(OB_SUCCESS == ret)
      {
        ret = sstable_id_.serialize(buf, buf_len, pos);
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObTablet)
    {
      int ret = OB_ERROR;

      ret = serialization::decode_vi64(buf, data_len, pos, &data_version_);

      int64_t size = 0;
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, pos, &size);
      }

      if(1 != size)
      {
        TBSYS_LOG(ERROR, "tablet sstable id size not equal to 1, range:%s size:%ld", to_cstring(range_), size);
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = sstable_id_.deserialize(buf, data_len, pos);
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObTablet)
    {
      int64_t total_size = 0;
      int64_t size = 0;
      total_size += serialization::encoded_length_vi64(data_version_);
      total_size += serialization::encoded_length_vi64(size);
      total_size += sstable_id_.get_serialize_size();

      return total_size;
    }

    /*
     * @return OB_SUCCESS if sstable_id exists in tablet
     * or OB_CS
     */
    int ObTablet::include_sstable(const ObSSTableId& sstable_id) const
    {
      int ret = OB_SUCCESS;

      if (sstable_id_ == sstable_id)
      {
      }
      else
      {
        ret = OB_ENTRY_NOT_EXIST;
      }

      return ret;
    }

    int ObTablet::load_sstable(const int64_t tablet_version)
    {
      int ret = OB_SUCCESS;
      sstable::SSTableReader* reader = NULL;

      load_sstable_mutex_.lock();

      if (NULL == image_)
      {
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS == sstable_loaded_)
      {
        ret = OB_SUCCESS;
      }
      else
      {

        if(sstable_id_.sstable_file_id_ > 0)
        {
          if (get_sstable_version() < SSTableReader::COMPACT_SSTABLE_VERSION)
          {
            reader = image_->alloc_sstable_object();
          }
          else
          {
            reader = image_->alloc_compact_sstable_object();
          }

          if (NULL == reader)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else if ( OB_SUCCESS != (ret = reader->open(sstable_id_.sstable_file_id_, tablet_version)))
          {
            TBSYS_LOG(ERROR, "read sstable failed, sstable id=%ld, ret =%d",
                sstable_id_.sstable_file_id_, ret);
          }
          else if(OB_INVALID_ID == range_.table_id_)
          {
            range_ = reader->get_range();
          }

          if(OB_SUCCESS == ret)
          {
            sstable_reader_ = reader;
          }
        }
      }

      sstable_loaded_ = ret;

      load_sstable_mutex_.unlock();

      return ret;
    }

    int64_t ObTablet::get_sstable_file_seq() const
    {
      return sstable_id_.sstable_file_id_;
    }

    /*
     * get row count of all sstables in this tablet
     */
    int64_t ObTablet::get_row_count() const
    {
      int64_t row_count = 0;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).lock();
      if (extend_info_.occupy_size_ == 0 && sstable_id_.sstable_file_id_ > 0)
      {
        const_cast<ObTablet*>(this)->calc_extend_info();
      }
      row_count = extend_info_.row_count_;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).unlock();
      return row_count;
    }

    /*
     * get row checksum of all sstables in this tablet
     */
    uint64_t ObTablet::get_row_checksum() const
    {
      uint64_t row_checksum = 0;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).lock();
      if (extend_info_.occupy_size_ == 0 && sstable_id_.sstable_file_id_ > 0)
      {
        const_cast<ObTablet*>(this)->calc_extend_info();
      }
      row_checksum = extend_info_.row_checksum_;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).unlock();
      return row_checksum;
    }

    /*
     * get approximate occupy size of all sstables in this tablet
     */
    int64_t ObTablet::get_occupy_size() const
    {
      int64_t occupy_size = 0;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).lock();
      if (extend_info_.occupy_size_ == 0 && sstable_id_.sstable_file_id_ > 0)
      {
        const_cast<ObTablet*>(this)->calc_extend_info();
      }
      occupy_size = extend_info_.occupy_size_;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).unlock();
      return occupy_size;
    }

    int64_t ObTablet::get_checksum() const
    {
      int64_t check_sum = 0;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).lock();
      if (extend_info_.occupy_size_ == 0 && sstable_id_.sstable_file_id_ > 0)
      {
        const_cast<ObTablet*>(this)->calc_extend_info();
      }
      check_sum = extend_info_.check_sum_;
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).unlock();
      return check_sum;
    }

    const ObTabletExtendInfo& ObTablet::get_extend_info() const
    {
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).lock();
      if (extend_info_.occupy_size_ == 0 && sstable_id_.sstable_file_id_ > 0)
      {
        const_cast<ObTablet*>(this)->calc_extend_info();
      }
      const_cast<tbsys::CThreadMutex&>(extend_info_mutex_).unlock();
      return extend_info_;
    }

    int ObTablet::calc_extend_info()
    {
      // calc extend info by sstable reader.
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t checksum_len = sizeof(uint64_t);
      int64_t sstable_checksum = 0;
      char checksum_buf[checksum_len];

      if (OB_SUCCESS != sstable_loaded_)
        ret = (sstable_loaded_ = load_sstable()); 

      if (OB_SUCCESS == ret)
      {
        extend_info_.row_count_ = 0;
        extend_info_.occupy_size_ = 0;
        extend_info_.check_sum_ = 0;
        extend_info_.row_checksum_ = 0;

        if (NULL != sstable_reader_)
        {
          extend_info_.row_count_ += sstable_reader_->get_row_count();
          extend_info_.occupy_size_ += sstable_reader_->get_sstable_size();
          extend_info_.row_checksum_ += sstable_reader_->get_sstable_row_checksum();
          sstable_checksum = sstable_reader_->get_sstable_checksum();
          pos = 0;
          ret = serialization::encode_i64(checksum_buf,
              checksum_len, pos, sstable_checksum);
          if (OB_SUCCESS == ret)
          {
            extend_info_.check_sum_ = ob_crc64(
                extend_info_.check_sum_, checksum_buf, checksum_len);
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "calc extend_info error, sstable,%ld reader = NULL",
              sstable_id_.sstable_file_id_);
          ret = OB_ERROR;
        }
      }

      return ret;
    }


    int ObTablet::dump(const bool dump_sstable) const
    {
      char range_buf[OB_RANGE_STR_BUFSIZ];
      range_.to_string(range_buf, OB_RANGE_STR_BUFSIZ);
      TBSYS_LOG(INFO, "range=%s, data version=%ld, disk_no=%d, merged=%d", 
          range_buf, data_version_, disk_no_, merged_);
      if (dump_sstable && OB_SUCCESS == sstable_loaded_)
      {
        //do nothing
        if (NULL != sstable_reader_) 
        {
          TBSYS_LOG(INFO, "sstable: id=%ld, "
              "size = %ld, row count =%ld, checksum=%ld", 
              sstable_id_.sstable_file_id_, 
              sstable_reader_->get_sstable_size(),
              sstable_reader_->get_row_count(),
              sstable_reader_->get_sstable_checksum()) ;
        }
      }
      else
      {
        TBSYS_LOG(INFO, "sstable id=%ld", sstable_id_.sstable_file_id_) ;
      }

      return OB_SUCCESS;
    }

    void ObTablet::set_merged(int status) 
    { 
      merged_ = status; 
      if (NULL != image_)
      {
        if (status > 0)
        {
          image_->incr_merged_tablet_count();
        }
        else 
        {
          image_->decr_merged_tablet_count();
        }
      }
    }

    bool ObTablet::try_create_local_index(const uint64_t table_id)
    {
      bool ret = true;

      load_sstable_mutex_.lock();
      if (NULL != local_index_ && local_index_->get_range().table_id_ != table_id)
      {
        if (UNAVALIABLE == local_index_state_ || CREATING == local_index_state_)
        {
          TBSYS_LOG(ERROR, "another table %ld's local index exist, with inconsistent state %d",
              local_index_->get_range().table_id_, local_index_state_);
          ret = false;
        }
        else 
        {
          TBSYS_LOG(INFO, "delete previous index table's local index tablet [%s]",
              to_cstring(*local_index_));
          int rc = image_->remove_sstable(local_index_);
          if (OB_SUCCESS != rc)
          {
            TBSYS_LOG(WARN, "remove sstable failed, rc %d", rc);
          }
          delete_local_index();
          local_index_state_ = CREATING;
        }
      }
      else if (CREATING == local_index_state_)
      {
        TBSYS_LOG(INFO, "tablet local index is creating, table_id=%lu", table_id);
        ret = false;
      }
      else if (UNAVALIABLE == local_index_state_)
      {
        TBSYS_LOG(INFO, "try create tablet local index success, table_id=%lu", table_id);
        local_index_state_ = CREATING;
      }
      load_sstable_mutex_.unlock();

      return ret;
    }

    int ObTablet::set_local_index(ObTablet* local_index)
    {
      int ret = OB_SUCCESS;

      load_sstable_mutex_.lock();
      if (NULL == local_index)
      {
        TBSYS_LOG(WARN, "invalid parameter, local_index=%p", local_index);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (CREATING == local_index_state_ && NULL == local_index_)
      {
        local_index_ = local_index;
        local_index_state_ = AVALIABLE;
      }
      else 
      {
        TBSYS_LOG(WARN, "failded to set local index, invalid state, local_index_state_=%d, "
                        "local_index_=%p",
                  local_index_state_, local_index_);
        ret = OB_ERROR;
      }
      load_sstable_mutex_.unlock();

      return ret;
    }

    void ObTablet::delete_local_index()
    {
      if (NULL != local_index_)
      {
        local_index_->~ObTablet();
        local_index_ = NULL;
      }
      local_index_state_ = UNAVALIABLE;
    }

    int64_t ObTablet::to_string(char* buffer, const int64_t length) const
    {
      int64_t pos = 0;
      int ret = 0;
      if (OB_SUCCESS != (ret = databuff_printf(buffer, length, pos, "range:")))
      {
      }
      else if ((pos += range_.to_string(buffer + pos, length)) > length)
      {
        ret = OB_BUF_NOT_ENOUGH;
      }
      else if (OB_SUCCESS != (ret = databuff_printf(buffer, length, pos, " sstable[%ld],"
              "data_version[%ld], merged_[%d], disk_no_[%d],"
              "row_count_[%ld], occupy_size_[%ld], check_sum_[%ld], "
              "last_do_expire_version_[%ld],sequence_num_[%ld], sstable_version_[%d]",
              sstable_id_.sstable_file_id_ > 0 ? sstable_id_.sstable_file_id_ : 0,
              data_version_, merged_, disk_no_, 
              extend_info_.row_count_, extend_info_.occupy_size_, extend_info_.check_sum_,
              extend_info_.last_do_expire_version_, extend_info_.sequence_num_, 
              extend_info_.sstable_version_)))
      {
      }
      return pos;
    }

  } // end namespace chunkserver
} // end namespace oceanbase



