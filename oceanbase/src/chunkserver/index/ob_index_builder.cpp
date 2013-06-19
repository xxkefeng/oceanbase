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
#include "common/ob_schema_manager.h"
#include "common/file_directory_utils.h"
#include "chunkserver/ob_tablet_manager.h"
#include "ob_index_builder.h"
#include "chunkserver/ob_disk_manager.h"
#include "sql/ob_sort.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace common;

    ObSortFileSetter::ObSortFileSetter()
      : init_(false), disk_no_array_(NULL), disk_num_(0), array_index_(0), memory_limit_(0)
    {
    }

    void ObSortFileSetter::init(const ObDiskManager &disk_manager, int64_t mem_per_thread)
    {
      disk_no_array_ = disk_manager.get_disk_no_array(disk_num_);
      array_index_ = 0;
      memory_limit_ = mem_per_thread;

      init_ = true;
    }

    int ObSortFileSetter::setup_sorter(sql::ObSort &sorter, const char *file_prefix)
    {
      int rc = OB_SUCCESS;
      if (!init_)
      {
        TBSYS_LOG(WARN, "setter not init");
        rc = OB_NOT_INIT;
      }
      else
      {
        sorter.set_mem_size_limit(memory_limit_);
        if (memory_limit_ > 0)
        {
          char tmpdir[OB_MAX_FILE_NAME_LENGTH];
          char tmpfile[OB_MAX_FILE_NAME_LENGTH];

          int64_t index = __sync_fetch_and_add(&array_index_, 1) % disk_num_;

          if (OB_SUCCESS != (rc = sstable::get_tmp_directory(disk_no_array_[index],
                  tmpdir, OB_MAX_FILE_NAME_LENGTH)))
          {
            TBSYS_LOG(WARN, "get_tmp_directory fail, disk_no %d, rc %d",
                disk_no_array_[index], rc);
          }
          else if(!FSU::exists(tmpdir) && (!FSU::create_directory(tmpdir)))
          {
            TBSYS_LOG(WARN, "create directory fail, dir [%s]", tmpdir);
            rc = OB_ERROR;
          }
          else if (!FSU::gen_tmpfile_name(tmpfile, OB_MAX_FILE_NAME_LENGTH,
                tmpdir, file_prefix))
          {
            TBSYS_LOG(WARN, "gen_tmpfile_name fail, tmpdir [%s]", tmpdir);
            rc = OB_ERROR;
          }
          else if (OB_SUCCESS != (rc = sorter.set_run_filename(
                  ObString::make_string(tmpfile))))
          {
            TBSYS_LOG(WARN, "set run filename fail, rc %d", rc);
          }
        }
      }

      return rc;
    }

    bool ObIndexBuildOptions::is_valid() const
    {
      bool ret = true;

      if (OB_INVALID_ID == data_table_id_ || OB_INVALID_ID == index_table_id_  
          || NULL == merger_schema_mgr_ || 0 == self_server_.get_ipv4() 
          || !rpc_option_.is_valid())
      {
        TBSYS_LOG(WARN, "invalid local index build options, rpc_is_valid=%d, %s", 
                  rpc_option_.is_valid(), to_cstring(*this));
        ret = false;
      }

      return ret;
    }

    int64_t ObIndexBuildOptions::to_string(
       char* buffer, const int64_t size) const
    {
      int64_t pos = 0;

      if (NULL != buffer && size > 0)
      {
        databuff_printf(buffer, size, pos, 
                        "data_table_id_=%lu, index_table_id_=%lu, sample_count_=%ld, "
                        "merger_schema_mgr_=%p, self_server_=",
                        data_table_id_, index_table_id_, sample_count_, 
                        merger_schema_mgr_);
        pos += self_server_.to_string(buffer + pos, size - pos);
      }

      return pos;
    }

    ObIndexBuilder::ObIndexBuilder(
       ObTabletManager& tablet_manager, ObIndexBuildOptions& build_option)
    : stop_(false),
      tablet_manager_(tablet_manager),
      build_option_(build_option)
    {

    }

    ObIndexBuilder::~ObIndexBuilder()
    {

    }
  } /* chunkserver */
} /* oceanbase */
