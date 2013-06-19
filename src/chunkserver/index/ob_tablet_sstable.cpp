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
#include "common/file_directory_utils.h"
#include "ob_tablet_sstable.h"
#include "chunkserver/ob_disk_manager.h"
#include "chunkserver/ob_tablet_manager.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace common;
    using namespace sstable;
    using namespace compactsstablev2;

    ObTabletSSTable::ObTabletSSTable()
    : writer_(NULL), 
      compact_sstable_appender_(NULL),
      sstable_appender_(NULL),
      scanner_(NULL)
    {

    }

    ObTabletSSTable::~ObTabletSSTable()
    {
      if (NULL != compact_sstable_appender_)
      {
        OB_DELETE(ObTabletCompactSSTableAppender, ObModIds::OB_CS_BUILD_INDEX, 
                  compact_sstable_appender_);
        compact_sstable_appender_ = NULL;
      }

      if (NULL != sstable_appender_)
      {
        OB_DELETE(ObTabletSSTableAppender, ObModIds::OB_CS_BUILD_INDEX, 
                  sstable_appender_);
        sstable_appender_ = NULL;
      }

      if (NULL != scanner_)
      {
        OB_DELETE(ObSSTableScan, ObModIds::OB_CS_BUILD_INDEX, scanner_);
        scanner_ = NULL;
      }
      writer_ = NULL;
    }

    int ObTabletSSTable::open(ObAppendOptions& append_options)
    {
      int ret = OB_SUCCESS;

      if (SSTableReader::COMPACT_SSTABLE_VERSION > append_options.write_sstable_version_)
      {
        if (NULL == sstable_appender_)
        {
          sstable_appender_ = OB_NEW(ObTabletSSTableAppender, ObModIds::OB_CS_BUILD_INDEX);
        }
        if (NULL != sstable_appender_)
        {
          writer_ = sstable_appender_;
        }
      }
      else if (SSTableReader::COMPACT_SSTABLE_VERSION == append_options.write_sstable_version_)
      {
        if (NULL == compact_sstable_appender_)
        {
          compact_sstable_appender_ = OB_NEW(ObTabletCompactSSTableAppender, ObModIds::OB_CS_BUILD_INDEX);
        }
        if (NULL != compact_sstable_appender_)
        {
          writer_ = compact_sstable_appender_;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "invalid write_sstable_version, write_table_version:%ld",
                  append_options.write_sstable_version_);
        ret = OB_INVALID_ARGUMENT;
      }

      if (NULL == writer_)
      {
        TBSYS_LOG(ERROR, "new ObBaseTabletSSTableAppender instance failed, write_sstable_version:%ld",
                  append_options.write_sstable_version_);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if (NULL != writer_ && OB_SUCCESS == ret)
      {
        ret = writer_->open(append_options);
      }

      return ret;
    }

    int ObTabletSSTable::append(const ObRow& row)
    {
      return (NULL == writer_) ? OB_NOT_INIT : writer_->append(row);
    }

    int ObTabletSSTable::close(const bool is_append_succ)
    {
      return (NULL == writer_) ? OB_NOT_INIT : writer_->close(is_append_succ);
    }

    int ObTabletSSTable::open(const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;

      if (!scan_options.scan_local_ ||  !scan_options.is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == scanner_)
      {
        scanner_ = OB_NEW(ObSSTableScan, ObModIds::OB_CS_BUILD_INDEX);
        if (NULL == scanner_)
        {
          TBSYS_LOG(ERROR, "new ObSSTableScan instance failed");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (NULL != scanner_ && OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = scanner_->open_scan_context(
           *scan_options.scan_param_, *scan_options.scan_context_)))
        {
          TBSYS_LOG(WARN, "fail to open scan context, ret=%d", ret);
        }
      }

      return ret;
    }

    int64_t ObBaseTabletSSTableAppender::calc_tablet_checksum(const int64_t sstable_checksum)
    {
      int64_t tablet_checksum = 0;
      int64_t checksum_len = sizeof(uint64_t);
      char checksum_buf[checksum_len];
      int64_t pos = 0;

      if (OB_SUCCESS == serialization::encode_i64(checksum_buf, 
          checksum_len, pos, sstable_checksum))
      {
        tablet_checksum = ob_crc64(
            tablet_checksum, checksum_buf, checksum_len);
      }

      return tablet_checksum;
    }

    void ObBaseTabletSSTableAppender::Context::reset()
    {
      options_.reset();
      sstable_id_.reset();
      row_store_type_ = DENSE_DENSE;
      table_count_ = 1;
      block_size_ = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
      compressor_name_.reset();
      sstable_path_.reset();
      path_str_[0] = '\0';

      max_sstable_size_ = 0;  //no support split
      min_split_sstable_size_ = 0;

      approx_space_usage_ = 0;
      sstable_size_ = -1;
    }

    int ObBaseTabletSSTableAppender::Context::build()
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* table_schema = NULL;

      if (NULL == options_.schema_ || NULL == options_.disk_manager_ 
          || NULL == options_.tablet_image_)
      {
        TBSYS_LOG(WARN, "invalid write_options, no schema specified, "
                        "schema_=%p, disk_manager_=%p, tablet_image_=%p",
            options_.schema_, options_.disk_manager_, options_.tablet_image_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = init_row_store_type()))
      {
        TBSYS_LOG(WARN, "failed to init row store type, ret=%d", ret);
      }
      else if (NULL == (table_schema = 
               options_.schema_->get_table_schema(options_.tablet_range_.table_id_)))
      {
        TBSYS_LOG(WARN, "table_id_=%lu isn't existent in schema", 
                  options_.tablet_range_.table_id_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = gen_sstable_path()))
      {
        TBSYS_LOG(WARN, "gen_sstable_path failed, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = build_compressor_name(
         const_cast<ObTableSchema&>(*table_schema))))
      {
        TBSYS_LOG(WARN, "build_compressor_name failed, ret=%d", ret);
      }
      else
      {
        // if schema define sstable block size for table, use it
        // for the schema with version 2, the default block size is 64(KB),
        // we skip this case and use the config of chunkserver
        block_size_ = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
        if (table_schema->get_block_size() > 0
            && 64 != table_schema->get_block_size())
        {
          block_size_ = table_schema->get_block_size();
        }
      }

      return ret;
    }

    int ObBaseTabletSSTableAppender::Context::gen_sstable_path()
    {
      int ret = OB_SUCCESS;
      bool is_sstable_exist = false;
      int32_t disk_no = options_.disk_manager_->get_dest_disk();
      sstable_id_.sstable_file_id_ = options_.tablet_manager_->allocate_sstable_file_seq();
      sstable_id_.sstable_file_offset_ = 0;

      if (disk_no < 0)
      {
        TBSYS_LOG(WARN, "does't have enough disk space");
        sstable_id_.sstable_file_id_ = 0;
        ret = OB_CS_OUTOF_DISK_SPACE;
      }
      else 
      {
        do
        {
          path_str_[0] = '\0';
          sstable_id_.sstable_file_id_ = 
            (sstable_id_.sstable_file_id_ << 8) | (disk_no & DISK_NO_MASK);

          if ( OB_SUCCESS != (ret = get_sstable_path(
             sstable_id_, path_str_, OB_MAX_FILE_NAME_LENGTH)))
          {
            TBSYS_LOG(WARN, "can't get the path of new sstable, ret=%d", ret);
          }
          else if (true == (is_sstable_exist = FileDirectoryUtils::exists(path_str_)))
          {
            // reallocate new file seq until get file name not exist.
            sstable_id_.sstable_file_id_ = options_.tablet_manager_->allocate_sstable_file_seq();
          }
          else 
          {
            sstable_path_ = ObString::make_string(path_str_);
          }
        } while (OB_SUCCESS == ret && is_sstable_exist);
      }

      return ret;
    }

    int ObBaseTabletSSTableAppender::Context::build_compressor_name(ObTableSchema& table_schema)
    {
      int ret = OB_SUCCESS;
      const char *compressor_name = NULL;

      if (NULL == (compressor_name = table_schema.get_compress_func_name()) 
          || 0 == *compressor_name)
      {
        TBSYS_LOG(WARN,"no compressor with this sstable. table id=%lu", 
            table_schema.get_table_id());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        compressor_name_ = ObString::make_string(compressor_name);
      }

      return ret;
    }

    int ObBaseTabletSSTableAppender::Context::init_row_store_type()
    {
      int ret = OB_SUCCESS;

      if (SSTableReader::COMPACT_SSTABLE_VERSION > options_.write_sstable_version_)
      {
        row_store_type_ = OB_SSTABLE_STORE_DENSE;
      }
      else if (SSTableReader::COMPACT_SSTABLE_VERSION == options_.write_sstable_version_)
      {
        row_store_type_ = DENSE_DENSE;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid write_sstable_version, write_table_version:%ld",
                  options_.write_sstable_version_);
        ret = OB_INVALID_ARGUMENT;
      }

      return ret;
    }

    int ObBaseTabletSSTableAppender::update_tablet_meta()
    {
      int ret = OB_SUCCESS;
      ObTabletUpdateInfo update_info;

      update_info.data_tablet_ = context_.options_.data_tablet_;
      update_info.tablet_version_ = context_.options_.version_range_.major_version_;
      update_info.sstable_id_ = context_.sstable_id_;
      update_info.tablet_range_ = &context_.options_.tablet_range_;

      build_extend_info(update_info.extend_info_);
      if (OB_SUCCESS != (ret = context_.options_.tablet_image_->update(update_info)))
      {
        TBSYS_LOG(WARN, "failed to update tablet image, ret=%d", ret);
      }
     
      return ret;
    }

    void ObBaseTabletSSTableAppender::cleanup()
    {
      if (strlen(context_.path_str_) > 0)
      {
        if (FileDirectoryUtils::exists(context_.path_str_))
        {
          unlink(context_.path_str_);
          TBSYS_LOG(INFO, "delete sstable: %s", context_.path_str_);
        }
        context_.path_str_[0] = '\0';
      }

      int32_t disk_no = context_.sstable_id_.sstable_file_id_ & DISK_NO_MASK;
      if (disk_no > 0)
      {
        context_.options_.disk_manager_->add_used_space(disk_no, 0);
      }
      context_.sstable_id_.reset();
    }

    int ObTabletCompactSSTableAppender::open(const ObAppendOptions& options)
    {
      int ret = OB_SUCCESS;

      sstable_schema_.reset();
      writer_.reset();
      context_.reset();
      context_.options_ = options;
      if (OB_SUCCESS != (ret = context_.build()))
      {
        TBSYS_LOG(WARN, "failed to build append options, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = compactsstablev2::build_sstable_schema(
         context_.options_.tablet_range_.table_id_, 
         *context_.options_.schema_, sstable_schema_)))
      {
        TBSYS_LOG(WARN, "convert table schema to sstable schema failed, "
                        "table_id=%lu, ret=%d",
            context_.options_.tablet_range_.table_id_, ret);
      }
      else if (OB_SUCCESS != (ret = create_sstable()))
      {
        TBSYS_LOG(WARN, "failed to create sstable, ret=%d", ret);
      }

      if (OB_SUCCESS != ret)
      {
        cleanup();
      }
        
      return ret;
    }

    int ObTabletCompactSSTableAppender::create_sstable()
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = writer_.set_sstable_param(
         context_.options_.version_range_, 
         static_cast<ObCompactStoreType>(context_.row_store_type_), context_.table_count_, 
         context_.block_size_, context_.compressor_name_, 
         context_.max_sstable_size_, context_.min_split_sstable_size_)))
      {
        TBSYS_LOG(WARN, "ret=%d,set_sstable_param error, version_=%ld,"
            "table_id=%lu, store_type=%d, table_count=%ld, sstable_block_size=%ld,"
            "compressor_name=%s, max_sstable_size=%ld, min_split_sstable_size=%ld",
            ret, context_.options_.version_range_.major_version_, 
            context_.options_.tablet_range_.table_id_, 
            context_.row_store_type_, context_.table_count_, 
            context_.block_size_, to_cstring(context_.compressor_name_), 
            context_.max_sstable_size_, context_.min_split_sstable_size_);
      }
      else if (OB_SUCCESS != (ret = writer_.set_table_info(
          context_.options_.tablet_range_.table_id_, 
          sstable_schema_, context_.options_.tablet_range_)))
      {
        TBSYS_LOG(WARN, "set_table_info error, range=%s, ret=%d", 
                  to_cstring(context_.options_.tablet_range_), ret);
      }
      else if (OB_SUCCESS != (ret = writer_.set_sstable_filepath(context_.sstable_path_)))
      {
        if (OB_IO_ERROR == ret)
        {
          context_.options_.disk_manager_->set_disk_status(
              (context_.sstable_id_.sstable_file_id_ & DISK_NO_MASK), DISK_ERROR);
        }
        TBSYS_LOG(WARN, "create sstable failed, ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(INFO, "create new sstable, sstable_path:%s, "
                        "table_id=%lu, version=%ld", 
                  to_cstring(context_.sstable_path_), 
                  context_.options_.tablet_range_.table_id_,
                  context_.options_.version_range_.major_version_);
      }

      return ret;
    }

    int ObTabletCompactSSTableAppender::append(const ObRow& row)
    {
      int ret = OB_SUCCESS;
      bool is_sstable_split = false;

      if (OB_SUCCESS != (ret = writer_.append_row(row, is_sstable_split)))
      {
        TBSYS_LOG(WARN, "failed to append row into sstable, sstable=%s, ret=%d",
                  to_cstring(context_.sstable_path_), ret);
      }
      else if (is_sstable_split)
      {
        TBSYS_LOG(WARN, "not support sstable split");
        ret = OB_NOT_SUPPORTED;
      }

      return ret;
    }

    int ObTabletCompactSSTableAppender::close(const bool is_append_succ)
    {
      int ret = OB_SUCCESS;

      if (is_append_succ && OB_SUCCESS != (ret = writer_.finish())) 
      {
        TBSYS_LOG(WARN, "failed to close sstable=%s, ret=%d",
                  to_cstring(context_.sstable_path_), ret);
      }

      if (OB_SUCCESS != ret || !is_append_succ)
      {
        cleanup();
      }

      if (is_append_succ && OB_SUCCESS == ret
          && OB_SUCCESS != (ret = update_tablet_meta()))
      {
        TBSYS_LOG(WARN, "failed to update tablet meta, is_append_succ=%d, ret=%d", 
                  is_append_succ, ret);
      }
      
      return ret;
    }

    void ObTabletCompactSSTableAppender::build_extend_info(ObTabletExtendInfo& extend_info)
    {
      extend_info.row_count_ = writer_.get_sstable_row_count(0);
      extend_info.occupy_size_ = writer_.get_sstable_size(0);
      extend_info.check_sum_ = calc_tablet_checksum(writer_.get_sstable_checksum(0));
      extend_info.row_checksum_ = writer_.get_sstable_row_checksum(0);
      extend_info.last_do_expire_version_ = context_.options_.version_range_.major_version_;
      extend_info.sequence_num_ = 0;
      extend_info.sstable_version_ = SSTableReader::COMPACT_SSTABLE_VERSION;
    }

    int ObTabletSSTableAppender::open(const ObAppendOptions& options)
    {
      int ret = OB_SUCCESS;

      sstable_schema_.reset();
      context_.reset();
      context_.options_ = options;
      if (OB_SUCCESS != (ret = context_.build()))
      {
        TBSYS_LOG(WARN, "failed to build append options, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = sstable::build_sstable_schema(
         context_.options_.tablet_range_.table_id_, 
         *context_.options_.schema_, sstable_schema_)))
      {
        TBSYS_LOG(WARN, "convert table schema to sstable schema failed, "
                        "table_id=%lu, ret=%d",
            context_.options_.tablet_range_.table_id_, ret);
      }
      else if (OB_SUCCESS != (ret = writer_.create_sstable(
         sstable_schema_, context_.sstable_path_,
         context_.compressor_name_, context_.options_.version_range_.major_version_,
         context_.row_store_type_, context_.block_size_)))
      {
        TBSYS_LOG(WARN, "failed to create sstable, ret=%d", ret);
      }

      if (OB_SUCCESS != ret)
      {
        cleanup();
      }
        
      return ret;
    }

    int ObTabletSSTableAppender::append(const ObRow& row)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = writer_.append_row(row, context_.approx_space_usage_)))
      {
        TBSYS_LOG(WARN, "failed to append row into sstable, sstable=%s, ret=%d",
                  to_cstring(context_.sstable_path_), ret);
      }

      return ret;
    }

    int ObTabletSSTableAppender::close(const bool is_append_succ)
    {
      int ret = OB_SUCCESS;
      int64_t trailer_offset = -1;

      //you have to set_tablet_range() before  invoking close_sstable()
      writer_.set_tablet_range(context_.options_.tablet_range_);
      
      if (OB_SUCCESS != (ret = writer_.close_sstable(trailer_offset, context_.sstable_size_))) 
      {
        TBSYS_LOG(WARN, "failed to close sstable=%s, ret=%d",
                  to_cstring(context_.sstable_path_), ret);
      }

      if (OB_SUCCESS != ret || !is_append_succ)
      {
        cleanup();
      }

      if (is_append_succ && OB_SUCCESS == ret
          && OB_SUCCESS != (ret = update_tablet_meta()))
      {
        TBSYS_LOG(WARN, "failed to update tablet meta, is_append_succ=%d, ret=%d", 
                  is_append_succ, ret);
      }
      
      return ret;
    }

    void ObTabletSSTableAppender::build_extend_info(ObTabletExtendInfo& extend_info)
    {
      extend_info.row_count_ = writer_.get_trailer().get_row_count();
      extend_info.occupy_size_ = context_.sstable_size_;
      extend_info.check_sum_ = calc_tablet_checksum(
         static_cast<int64_t>(writer_.get_trailer().get_sstable_checksum()));
      extend_info.row_checksum_ = 0; 
      extend_info.last_do_expire_version_ = context_.options_.version_range_.major_version_;
      extend_info.sequence_num_ = 0;
      extend_info.sstable_version_ = SSTableReader::ROWKEY_SSTABLE_VERSION;
    }
  } /* chunkserver */
} /* oceanbase */
