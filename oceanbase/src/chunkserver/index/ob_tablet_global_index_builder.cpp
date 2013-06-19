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
#include "ob_tablet_global_index_builder.h"
#include "ob_row_cursor.h"
#include "ob_row_writer.h"
#include "chunkserver/ob_tablet_manager.h"
#include "ob_index_tablet_image.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace common;
    using namespace sql;
    using namespace sstable;

    int ObMultiLocalIndexScan::open_scan_context(
       const ObScanOptions& scan_options, 
       const ObTabletDistribution& data_table_tablet_dist,
       const ObTabletDistItem& index_tablet_item,
       const common::ObServer& self_server,
       bool& need_sort)
    {
      int ret = OB_SUCCESS;

      cur_scan_options_ = scan_options;
      data_table_tablet_dist_ = &data_table_tablet_dist;
      index_tablet_item_ = &index_tablet_item;
      self_server_ = self_server;
      cur_operator_ = NULL;

      if (OB_SUCCESS != (ret = init_scan_type()))
      {
        TBSYS_LOG(WARN, "failed to init scan type, don't know how to build this "
                        "tablet for server=%s, index_tablet_item=[%s]", 
                  to_cstring(self_server_), to_cstring(index_tablet_item));
      }
      else if (OB_SUCCESS != (ret = init_local_index_scan()))
      {
        TBSYS_LOG(WARN, "failed to init local index scan, scan_type_=%d, ret=%d",
                  scan_type_, ret);
      }
      else 
      {
        need_sort = (BROADCAST_SCAN == scan_type_) ? true : false;
      }

      return  ret;
    }

    bool ObMultiLocalIndexScan::is_broadcast_scan()
    {
      int ret = false;
      const ObTabletDistItem &item = *index_tablet_item_;

      if (item.locations_count_ > 0)
      {
        int64_t selected = ObTabletLocationSelector()(item);

        ret = (item.locations_[selected].location_.chunkserver_ == self_server_
            && !item.has_tablet_avaliable());
      }
      return ret;
    }

    bool ObMultiLocalIndexScan::is_migrage_scan()
    {
      int ret = false;
      const ObTabletDistItem &item = *index_tablet_item_;

      if (item.locations_count_ > 0)
      {
        int64_t index = item.get_server_index(self_server_);

        ret = (index >= 0
            && item.locations_[index].tablet_status_ == common::TABLET_UNAVAILABLE
            && item.has_tablet_avaliable());
      }
      return ret;
    }

    int ObMultiLocalIndexScan::init_scan_type()
    {
      int ret = OB_SUCCESS;

      if (is_broadcast_scan())
      {
        scan_type_ = BROADCAST_SCAN;
      }
      else if (is_migrage_scan())
      {
        scan_type_ = MIGRAGE_SCAN;
      }
      else 
      {
        scan_type_ = INVALID_SCAN;
        ret = OB_NOT_SUPPORTED;
      }

      return ret;
    }

    const ObServer &ObMultiLocalIndexScan::get_selected_server(const ObTabletDistItem &item)
    {
      int64_t selected = ObTabletLocationSelector()(item);
      return item.locations_[selected].location_.chunkserver_;
    }

    int ObMultiLocalIndexScan::init_local_index_scan()
    {
      int ret = OB_SUCCESS;

      if (BROADCAST_SCAN == scan_type_)
      {
        ret = init_broadcast_scan();
      }
      else if (MIGRAGE_SCAN == scan_type_)
      {
        ret = init_migrate_scan();
      }
      else 
      {
        ret = OB_NOT_SUPPORTED;
      }

      return ret;
    }

    int ObMultiLocalIndexScan::init_broadcast_scan()
    {
      int rc = OB_SUCCESS;

      ObAllTabletPredicator predicator;
      ObTabletIterator iter(*data_table_tablet_dist_, predicator);

      scan_status_array_.clear();
      data_tablet_index_ = 0;

      scan_status_array_.reserve(data_table_tablet_dist_->get_tablet_item_count());
      const ObTabletDistItem *item = NULL;
      while (OB_ITER_END != iter.next(item))
      {
        ObDataTabletScanStatus status;
        status.item_ = item;
        if (OB_SUCCESS != (rc = scan_status_array_.push_back(status)))
        {
          TBSYS_LOG(WARN, "scan_status_array_.push_back fail, rc %d", rc);
          break;
        }
      }

      return rc;
    }

    int ObMultiLocalIndexScan::scan_next_data_tablet(const common::ObRow *&row)
    {
      const int64_t start_index = data_tablet_index_;
      bool got_row = false;

      int rc = OB_ITER_END;
      for (int64_t loop_count = 0; scan_status_array_.count() > 0;)
      {
        if (NULL != cur_scan_options_.stop_ && *cur_scan_options_.stop_)
        {
          rc = OB_CANCELED;
          break;
        }

        bool got_is_building = false;

        ObDataTabletScanStatus &scan_status = scan_status_array_.at(data_tablet_index_);
        if (!scan_status.complete_)
        {
          for (int64_t i = 0; i < scan_status.item_->locations_count_; i++)
          {
            int64_t location_start = scan_status.item_->location_start_;
            rc = scan_cur_replica(row);
            if (OB_SUCCESS == rc)
            {
              got_row = true;
              break;
            }
            else if (OB_ITER_END == rc)
            {
              scan_status.complete_ = true;
              rc = OB_SUCCESS;
              break;
            }
            else if (OB_NEED_RETRY == rc)
            {
              got_is_building = true;
              break;
            }
            else
            {
              scan_status.item_->chose_next_location(location_start);
            }
          }

          if (OB_SUCCESS != rc && OB_NEED_RETRY != rc)
          {
            TBSYS_LOG(ERROR, "scan local index from [%s] failed, after try all replicas",
                to_cstring(*scan_status.item_));
            break;
          }
        }

        if (got_row)
        {
          break;
        }

        data_tablet_index_ = (data_tablet_index_ + 1) % scan_status_array_.count();
        if (start_index == data_tablet_index_)
        {
          loop_count++;
          if (!got_is_building) // all data tablet completed
          {
            rc = OB_ITER_END;
            break;
          }
          else // all uncomplete data tablet is building local index
          {
            usleep(WAIT_CS_BUILD_LOCAL_INDEX_INTERVAL);
          }
        }

        if (loop_count >= 3)
        {
          TBSYS_LOG(WARN, "still has uncomplete data tablet, after %ld loops", loop_count + 1);
        }
      }

      return rc;
    }

    int ObMultiLocalIndexScan::scan_cur_replica(const common::ObRow *&row)
    {
      int rc = OB_ERROR;
      int run_times = ONE_REPLICA_RETRY_TIMES;

      // Only remote index scan need retry
      if (get_selected_server(
            *scan_status_array_.at(data_tablet_index_).item_) == self_server_)
      {
        run_times = 1;
      }

      for (int i = 0; i < run_times && (OB_SUCCESS != rc && OB_ITER_END != rc); i++)
      {
        if (OB_SUCCESS != (rc = setup_scanner()))
        {
          TBSYS_LOG(WARN, "setup_scanner failed, rc %d", rc);
        }
        else
        {
          // skip already received rows
          for (int n = 0; n < scan_status_array_.at(data_tablet_index_).got_row_cnt_; n++)
          {
            const ObRow *r;
            if (OB_SUCCESS != (rc = cur_operator_->get_next_row(r)))
            {
              break;
            }
          }

          if (OB_SUCCESS == rc)
          {
            rc = cur_operator_->get_next_row(row);
            if (OB_SUCCESS != rc && OB_ITER_END != rc)
            {
              TBSYS_LOG(WARN,"get next row failed, rc %d", rc);
            }
          }
          else if (OB_ITER_END == rc)
          {
            TBSYS_LOG(WARN, "can't get enough rows in current replica");
            rc = OB_ERROR;
          }
          else
          {
            TBSYS_LOG(WARN,"skip received rows failed, rc %d", rc);
          }

          if (OB_SUCCESS != rc)
          {
            cur_operator_->close();
            cur_operator_ = NULL;
          }
        }

        if (OB_NEED_RETRY == rc)
        {
          break;
        }
      }

      return rc;
    }

    int ObMultiLocalIndexScan::setup_scanner()
    {
      int ret = OB_SUCCESS;
      const ObTabletDistItem* data_table_tablet_item = NULL;
      cur_operator_ = NULL;

      if (BROADCAST_SCAN == scan_type_)
      {
        if (data_tablet_index_ >= scan_status_array_.count() || (NULL == 
              (data_table_tablet_item = scan_status_array_.at(data_tablet_index_).item_)))
        {
          TBSYS_LOG(WARN, "data_tablet_index_ out of bound or data table tablet item is NULL. "
                          "array count %ld, index %ld, item %p", scan_status_array_.count(),
                          data_tablet_index_, data_table_tablet_item);
          ret = OB_ERROR;
        }
        else
        {
          cur_scan_options_.data_tablet_range_ = &data_table_tablet_item->tablet_range_;
          cur_scan_options_.index_tablet_range_ = &index_tablet_item_->tablet_range_;
          if (get_selected_server(*data_table_tablet_item) == self_server_)
          {
            cur_scan_options_.scan_local_ = true;
            if (OB_SUCCESS != (ret = wait_aio_buffer()))
            {
              TBSYS_LOG(WARN, "failed to wait_aio_buffer free, ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = scanner_.open(local_input_sstable_, cur_scan_options_)))
            {
              TBSYS_LOG(WARN, "failed to scan tablet data from local chunkserver=%s, "
                              "index_tablet_range_=%s",
                        to_cstring(self_server_), 
                        to_cstring(*cur_scan_options_.index_tablet_range_));
            }
          }
          else 
          {
            cur_scan_options_.scan_local_ = false;
            cur_scan_options_.rpc_option_.server_ = 
              get_selected_server(*data_table_tablet_item);
            if (OB_SUCCESS != (ret = scanner_.open(remote_input_table_, cur_scan_options_)))
            {
              TBSYS_LOG(WARN, "failed to scan tablet data from remote chunkserver=%s, "
                              "index_tablet_range_=%s",
                        to_cstring(cur_scan_options_.rpc_option_.server_), 
                        to_cstring(*cur_scan_options_.index_tablet_range_));
            }
          }
        }
      }
      else 
      {
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret && NULL == (cur_operator_ = scanner_.get_phyoperator()))
      {
        TBSYS_LOG(WARN, "after init local index scanner, the phyoperator is NULL");
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObMultiLocalIndexScan::init_migrate_scan()
    {
      int ret = OB_SUCCESS;

      if (MIGRAGE_SCAN == scan_type_)
      {
        cur_scan_options_.data_tablet_range_ = &index_tablet_item_->tablet_range_;
        cur_scan_options_.scan_local_ = false;
        cur_scan_options_.index_tablet_range_ = NULL;
        bool has_avaliable_tablet = false;
        for (int64_t i = 0; i < index_tablet_item_->locations_count_; i++)
        {
          if (index_tablet_item_->locations_[i].tablet_status_ == common::TABLET_AVAILABLE)
          {
            cur_scan_options_.rpc_option_.server_
              = index_tablet_item_->locations_[i].location_.chunkserver_;
            has_avaliable_tablet = true;
            break;
          }
        }

        if (!has_avaliable_tablet)
        {
          TBSYS_LOG(ERROR, "no avaliable tablet in global index migrate, tablet_item %s",
              to_cstring(*index_tablet_item_));
        }
        else if (OB_SUCCESS != (ret = scanner_.open(remote_input_table_, cur_scan_options_)))
        {
          TBSYS_LOG(WARN, "failed to migrate scan tablet data from remote chunkserver=%s, "
                          "index_tablet_range_=%s",
                    to_cstring(cur_scan_options_.rpc_option_.server_),
                    to_cstring(*cur_scan_options_.index_tablet_range_));
        }
        else if (NULL == (cur_operator_ = scanner_.get_phyoperator()))
        {
          TBSYS_LOG(WARN, "after init local index scanner, the phyoperator is NULL");
          ret = OB_ERROR;
        }
      }
      else 
      {
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObMultiLocalIndexScan::open()
    {
      return OB_SUCCESS;
    }

    int ObMultiLocalIndexScan::close()
    {
      int ret = OB_SUCCESS;

      if (NULL != cur_operator_)
      {
        ret = cur_operator_->close();
        cur_operator_ = NULL;
      }

      return ret;
    }

    int ObMultiLocalIndexScan::get_next_row(const common::ObRow*& row)
    {
      int ret = OB_SUCCESS;

      if (OB_UNLIKELY(NULL == cur_operator_))
      {
        if (BROADCAST_SCAN == scan_type_)
        {
          ret = scan_next_data_tablet(row);
        }
        else
        {
          TBSYS_LOG(ERROR, "cur_operator_ is NULL");
          ret = OB_NOT_INIT;
        }
      }
      else
      {
        if (OB_SUCCESS != (ret = cur_operator_->get_next_row(row)))
        {
          if (OB_ITER_END != ret)
          {
            TBSYS_LOG(WARN, "child_op failed to get next row, ret=%d", ret);
          }
          if (BROADCAST_SCAN == scan_type_)
          {
            if (OB_SUCCESS != (ret = cur_operator_->close()))
            {
              TBSYS_LOG(WARN, "failed to close current operator, ret=%d", ret);
            }

            if (OB_ITER_END == ret)
            {
              scan_status_array_.at(data_tablet_index_).complete_ = true;
            }
            ret = scan_next_data_tablet(row);
          }
        }
      }

      if (OB_SUCCESS == ret && BROADCAST_SCAN == scan_type_)
      {
        scan_status_array_.at(data_tablet_index_).got_row_cnt_++;
      }

      return ret;
    }

    int ObMultiLocalIndexScan::get_row_desc(const common::ObRowDesc*& row_desc) const
    {
      int ret = OB_SUCCESS;

      if (OB_UNLIKELY(NULL == cur_operator_))
      {
        TBSYS_LOG(ERROR, "cur_operator_ is NULL");
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = cur_operator_->get_row_desc(row_desc);
      }

      return ret;
    }

    int64_t ObMultiLocalIndexScan::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;

      if (NULL != buf && buf_len > 0)
      {
        databuff_printf(buf, buf_len, pos, "index_table_id=%ld", 
                        cur_scan_options_.index_table_id_);
        if (NULL != cur_operator_)
        {
          int64_t pos2 = cur_operator_->to_string(buf + pos, buf_len - pos);
          pos += pos2;
        }
      }

      return pos;
    }

    int ObTabletGlobalIndexScanner::open(
       const ObScanOptions& scan_options, 
       const ObTabletDistribution& data_table_tablet_dist,
       const ObTabletDistItem& index_tablet_item,
       const common::ObServer& self_server)
    {
      int ret = OB_SUCCESS;
      bool need_sort = true;
      root_ = NULL;

      if (OB_SUCCESS != (ret = local_index_scanner_.open_scan_context(
         scan_options, data_table_tablet_dist, index_tablet_item, self_server, need_sort)))
      {
        TBSYS_LOG(WARN, "failed to init local index scanner, ret=%d", ret);
      }
      else
      {
        root_ = &local_index_scanner_;
        if (need_sort)
        {
          if (OB_SUCCESS != (ret = open_sorter(scan_options)))
          {
            TBSYS_LOG(WARN, "failed to init sorter, ret=%d", ret);
          }
          else 
          {
            root_ = &sorter_;
          }
        }
      }

      if (OB_SUCCESS != ret && NULL != root_)
      {
        root_->close();
        root_ = NULL;
      }

      return ret;
    }

    int ObTabletGlobalIndexScanner::open_sorter(const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* table_schema = NULL;

      sorter_.reset();
      if (NULL == (table_schema = scan_options.schema_->get_table_schema(
         scan_options.index_table_id_)))
      {
        TBSYS_LOG(WARN, "failed to get table schema, index_table_id_=%lu",
                  scan_options.index_table_id_);
        ret = OB_ERROR;
      }
      else if (NULL == scan_options.sort_file_setter_)
      {
        TBSYS_LOG(WARN, "sort file setter not set");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = scan_options.sort_file_setter_->setup_sorter(
              sorter_, "cs_global_index_builder_sort")))
      {
        TBSYS_LOG(WARN, "set sorter file failed, ret %d", ret);
      }
      else
      {
        const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
        const ObRowkeyColumn* rowkey_column = NULL;

        for (int64_t i = 0; i < rowkey_info.get_size() && OB_SUCCESS == ret; i++)
        {
          if (NULL == (rowkey_column = rowkey_info.get_column(i)))
          {
            TBSYS_LOG(WARN, "failed to get rowkey column, table_id_=%lu",
                      scan_options.index_table_id_);
            ret = OB_ERROR;
          }
          else if (OB_SUCCESS != (ret= sorter_.add_sort_column(
             scan_options.index_table_id_, rowkey_column->column_id_, true)))
          {
            TBSYS_LOG(WARN, "failed to add sort column into sorter, ret=%d", ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (NULL == root_)
          {
            TBSYS_LOG(WARN, "local index scanner operator is not initialization");
            ret = OB_ERROR;
          }
          else if (OB_SUCCESS != (ret = sorter_.set_child(0, local_index_scanner_)))
          {
            TBSYS_LOG(WARN, "failed to set child of sorter, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = sorter_.open()))
          {
            TBSYS_LOG(WARN, "failed to open sorter for tablet global index builder, "
                            "ret=%d", ret);
          }
        }
      }

      return ret;
    }

    ObTabletGlobalIndexBuilder::ObTabletGlobalIndexBuilder(
       ObTabletManager& tablet_manager, 
       ObGlobalIndexTabletImage& tablet_image)
    : tablet_manager_(tablet_manager), 
      tablet_image_(tablet_image)
    {

    }

    ObTabletGlobalIndexBuilder::~ObTabletGlobalIndexBuilder()
    {

    }

    int ObTabletGlobalIndexBuilder::build(
       ObTabletDistribution& data_table_tablet_dist, 
       const ObSchemaManagerV2& schema, 
       const ObTabletDistItem& index_tablet_item,
       const ObRpcOptions& rpc_option,
       const ObServer& self_server,
       ObSortFileSetter &sort_file_setter,
       const bool &stop,
       const int64_t write_sstable_version)
    {
      int ret = OB_SUCCESS;
      ObAppendOptions append_options;
      ObScanOptions scan_options;
      sql::ScanContext scan_context;

      append_options.version_range_.major_version_ = tablet_manager_.get_last_not_merged_version();
      append_options.tablet_range_ = index_tablet_item.tablet_range_;
      append_options.schema_ = &schema;
      append_options.disk_manager_ = &tablet_manager_.get_disk_manager();
      append_options.tablet_image_ = &tablet_image_;
      append_options.tablet_manager_ = &tablet_manager_;
      append_options.data_tablet_ = NULL;
      append_options.write_sstable_version_= write_sstable_version;

      tablet_manager_.build_scan_context(scan_context);
      scan_options.index_table_id_ = index_tablet_item.tablet_range_.table_id_;
      scan_options.scan_local_ = false;
      scan_options.is_result_cached_ = false;
      scan_options.scan_only_rowkey_ = false;
      scan_options.need_sort_ = false;
      scan_options.schema_ = &schema;
      scan_options.data_tablet_range_ = NULL;
      scan_options.index_tablet_range_ = &index_tablet_item.tablet_range_;
      scan_options.scan_param_ = NULL;
      scan_options.scan_context_ = &scan_context;
      scan_options.rpc_option_ = rpc_option;
      scan_options.sort_file_setter_ = &sort_file_setter;
      scan_options.stop_ = &stop;

      if (OB_SUCCESS != (ret = scanner_.open(
         scan_options, data_table_tablet_dist, index_tablet_item, self_server)))
      {
        TBSYS_LOG(WARN, "failed to open tablet local index multi scanner, ret=%d", ret);
      }
      else if (NULL == scanner_.get_phyoperator())
      {
        TBSYS_LOG(WARN, "after init, get NULL phyoperator from global index scanner");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = output_sstable_.open(append_options)))
      {
        TBSYS_LOG(WARN, "failed to open tablet output global index sstable, ret=%d", ret);
      }
      else 
      {
        ObRowCursor input_cursor(scanner_.get_phyoperator());
        ObTableSink table_sinker(&output_sstable_);
        ret = write_row_cursor(input_cursor, table_sinker);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "failed to write tablet global index sstable, ret=%d", ret);
        }
      
        int err = OB_SUCCESS;
        if (OB_SUCCESS != (err = output_sstable_.close(OB_SUCCESS == ret))
            && OB_SUCCESS == ret)
        {
          TBSYS_LOG(WARN, "failed to close global index output sstable, "
                          "index_table_id=%lu, ret=%d",
                    scan_options.index_table_id_, err);
          ret = err;
        }
      }

      if (OB_SUCCESS == ret
          && OB_SUCCESS != (ret = const_cast<ObTabletDistItem&>(index_tablet_item).set_state(
             self_server, common::TABLET_AVAILABLE)))
      {
        TBSYS_LOG(WARN, "failed to set tablet item to available state, "
                        "tablet_range_=%s, ret=%d",
                  to_cstring(index_tablet_item.tablet_range_), ret);
      }

      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
