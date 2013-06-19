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
#include "sstable/ob_aio_buffer_mgr.h"
#include "ob_tablet_local_index_builder.h"
#include "ob_row_cursor.h"
#include "ob_row_writer.h"
#include "chunkserver/ob_tablet_manager.h"
#include "ob_index_tablet_image.h"
#include "ob_tablet_memtable.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace common;
    using namespace sql;
    using namespace sstable;

    ObTabletLocalIndexBuilder::ObTabletLocalIndexBuilder(
       ObTabletManager& tablet_manager, ObLocalIndexTabletImage& tablet_image)
    : tablet_manager_(tablet_manager), 
      tablet_image_(tablet_image)
    {

    }

    ObTabletLocalIndexBuilder::~ObTabletLocalIndexBuilder()
    {

    }

    int ObTabletLocalIndexBuilder::build(
       const ObSchemaManagerV2& schema, ObTablet& data_tablet, 
       const uint64_t index_table_id, ObSortFileSetter &sort_file_setter,
       const int64_t write_sstable_version)
    {
      int ret = OB_SUCCESS;
      ObAppendOptions append_options;
      ObScanOptions scan_options;
      sql::ScanContext scan_context;

      if (OB_INVALID_ID == index_table_id)
      {
        TBSYS_LOG(WARN, "invalid parameter, index_table_id=%lu", index_table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = wait_aio_buffer()))
      {
        TBSYS_LOG(WARN, "failed to wait_aio_buffer free, ret=%d", ret);
      }
      else if (!data_tablet.try_create_local_index(index_table_id))
      {
        ret = OB_NEED_RETRY;
      }
      else 
      {
        append_options.version_range_.major_version_ = data_tablet.get_data_version();
        append_options.tablet_range_.set_whole_range();
        append_options.tablet_range_.table_id_ = index_table_id;
        append_options.schema_ = &schema;
        append_options.disk_manager_ = &tablet_manager_.get_disk_manager();
        append_options.tablet_image_ = &tablet_image_;
        append_options.tablet_manager_ = &tablet_manager_;
        append_options.data_tablet_ = &data_tablet;
        append_options.write_sstable_version_= write_sstable_version;

        tablet_manager_.build_scan_context(scan_context);
        scan_options.index_table_id_ = index_table_id;
        scan_options.scan_local_ = true;
        scan_options.is_result_cached_ = false;
        scan_options.scan_only_rowkey_ = false;
        scan_options.need_sort_ = true;
        scan_options.schema_ = &schema;
        scan_options.data_tablet_range_ = &data_tablet.get_range();
        scan_options.scan_param_ = NULL;
        scan_options.scan_context_ = &scan_context;
        scan_options.rpc_option_.reset();
        scan_options.sort_file_setter_ = &sort_file_setter;

        if (OB_SUCCESS != (ret = extracter_.open(input_sstable_, scan_options)))
        {
          TBSYS_LOG(WARN, "failed to open tablet local index extracter, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = output_sstable_.open(append_options)))
        {
          TBSYS_LOG(WARN, "failed to open tablet output local index sstable, ret=%d", ret);
        }
        else 
        {
          ObRowCursor input_cursor(extracter_.get_phyoperator());
          ObTableSink table_sinker(&output_sstable_);
          ret = write_row_cursor(input_cursor, table_sinker);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to write tablet local index sstable, ret=%d", ret);
          }
        
          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = output_sstable_.close(OB_SUCCESS == ret))
              && OB_SUCCESS == ret)
          {
            TBSYS_LOG(WARN, "failed to close local index output sstable, "
                            "index_table_id=%lu, ret=%d",
                      index_table_id, err);
            ret = err;
          }
        }
      }

      return ret;
    }

    ObTabletLocalIndexSampler::ObTabletLocalIndexSampler(
       ObTabletManager& tablet_manager, 
       ObLocalIndexTabletImage& tablet_image,
       ObTabletMemtable& media_output_memtable)
    : tablet_manager_(tablet_manager), 
      tablet_image_(tablet_image),
      media_output_memtable_(media_output_memtable)
    {

    }

    ObTabletLocalIndexSampler::~ObTabletLocalIndexSampler()
    {

    }

    int ObTabletLocalIndexSampler::sample_tablet(
       const ObSchemaManagerV2& schema,
       const ObNewRange& data_tablet_range, 
       const ObNewRange& index_tablet_range, 
       const int64_t row_interval)
    {
      int ret = OB_SUCCESS;
      ObScanOptions scan_options;
      sql::ScanContext scan_context;

      if (row_interval <= 0)
      {
        TBSYS_LOG(WARN, "invalid argument, row_interval=%ld", row_interval);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = wait_aio_buffer()))
      {
        TBSYS_LOG(WARN, "failed to wait_aio_buffer free, ret=%d", ret);
      }
      else 
      {
        tablet_manager_.build_scan_context(scan_context);
        scan_options.index_table_id_ = index_tablet_range.table_id_;
        scan_options.scan_local_ = true;
        scan_options.is_result_cached_ = true;
        //only for tablet local index sampler scan data from local chunkserver
        scan_options.scan_only_rowkey_ = true;
        scan_options.need_sort_ = false;
        scan_options.row_interval_ = row_interval;
        scan_options.schema_ = &schema;
        scan_options.data_tablet_range_ = &data_tablet_range;
        scan_options.index_tablet_range_ = &index_tablet_range;
        scan_options.scan_param_ = NULL;
        scan_options.scan_context_ = &scan_context;
        scan_options.rpc_option_.reset();

        if (OB_SUCCESS != (ret = interval_sampler_.open(
           input_sstable_, scan_options)))
        {
          TBSYS_LOG(WARN, "failed to open tablet local index interval sampler, "
                          "ret=%d", ret);
        }
        else 
        {
          ObRowCursor input_cursor(interval_sampler_.get_phyoperator());
          ObThreadSafeTableSink table_sinker(&media_output_memtable_);
          ret = write_row_cursor(input_cursor, table_sinker);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to write tablet local index sample memtable, "
                            "ret=%d", ret);
          }

          int err = OB_SUCCESS;
          if (OB_SUCCESS != (err = media_output_memtable_.close(OB_SUCCESS == ret))
              && OB_SUCCESS == ret)
          {
            TBSYS_LOG(WARN, "failed to close sample output memtable, "
                            "index_table_id=%lu, ret=%d",
                      index_tablet_range.table_id_, err);
            ret = err;
          }
        }
      }

      return ret;
    }

    int ObTabletLocalIndexSampler::sample_memtable(
       ObTabletMemtable& input_memtable, 
       const ObSchemaManagerV2& schema, 
       const ObNewRange& tablet_range,
       const int64_t row_interval,
       ObSortFileSetter &sort_file_setter,
       sql::ObPhyOperator*& phy_operator)
    {
      int ret = OB_SUCCESS;
      ObScanOptions scan_options;
      sql::ScanContext scan_context;
      phy_operator = NULL;

      if (row_interval <= 0)
      {
        TBSYS_LOG(WARN, "invalid argument, row_interval=%ld", row_interval);
        ret = OB_INVALID_ARGUMENT;
      }
      else 
      {
        tablet_manager_.build_scan_context(scan_context);
        scan_options.index_table_id_ = tablet_range.table_id_;
        scan_options.scan_local_ = true;
        scan_options.is_result_cached_ = true;
        scan_options.scan_only_rowkey_ = true;
        scan_options.need_sort_ = true;
        scan_options.row_interval_ = row_interval;
        scan_options.schema_ = &schema;
        scan_options.data_tablet_range_ = &tablet_range;
        scan_options.scan_param_ = NULL;
        scan_options.scan_context_ = &scan_context;
        scan_options.sort_file_setter_ = &sort_file_setter;

        if (OB_SUCCESS != (ret = interval_sampler_.open(input_memtable, scan_options)))
        {
          TBSYS_LOG(WARN, "failed to open tablet local index interval sampler, "
                          "ret=%d", ret);
        }
        else if (NULL == (phy_operator = interval_sampler_.get_phyoperator()))
        {
          TBSYS_LOG(WARN, "after init, interval sampler returns NULL phyoperator");
          ret = OB_ERROR;
        }
      }

      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
