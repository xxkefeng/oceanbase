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
#include "ob_tablet_distribution.h"
#include "ob_tablet_predicator.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace tbsys;
    using namespace oceanbase;
    using namespace common;

    int64_t ObTabletDistItem::to_string(char* buffer, const int64_t size) const
    {
      int64_t pos = 0;

      if (NULL != buffer && size > 0)
      {
        databuff_printf(buffer, size, pos, 
                        "locations_count_=%ld, in_array_pos_=%ld, location_start_=%ld, "
                        "tablet_range_=", locations_count_, in_array_pos_, location_start_);
        pos += tablet_range_.to_string(buffer + pos, size - pos);
        for (int64_t i = 0; i < locations_count_; ++i)
        {
          databuff_printf(buffer, size, pos,
                          ", tablet_status_[%ld]=%d, chunkserver_[%ld]=",
                          i, locations_[i].tablet_status_, i);
          pos += locations_[i].location_.chunkserver_.to_string(
             buffer + pos, size - pos);
        }
      }

      return pos;
    }

    int ObTabletDistScanner::init(
       const uint64_t table_id, const ObRpcOptions& rpc_option)
    {
      int ret = OB_SUCCESS;

      if (OB_INVALID_ID == table_id || !rpc_option.is_valid())
      {
        TBSYS_LOG(WARN, "invalid parameter, table_id=%lu, rpc_is_valid=%d", 
                  table_id, rpc_option.is_valid());
        ret = OB_INVALID_ARGUMENT;
      }
      else 
      {
        table_id_ = table_id;
        rpc_option_ = rpc_option;
        end_rowkey_.set_min_row();
        tablet_item_.reset();
        scan_param_.reset();
        scanner_.reset();

        if (OB_SUCCESS != (ret = scan()))
        {
          TBSYS_LOG(WARN, "failed to scan root table, table_id=%lu ret=%d", 
                    table_id_, ret);
        }
      }

      return ret;
    }

    void ObTabletDistScanner::build_scan_param()
    {
      ObString fake_table_name;
      ObNewRange range;

      range.table_id_ = table_id_;
      range.set_whole_range();
      if (!end_rowkey_.is_min_row())
      {
        range.start_key_ = end_rowkey_;
      }
      scan_param_.set(table_id_, fake_table_name, range);
      scan_param_.set_is_read_consistency(false);
    }

    int ObTabletDistScanner::scan()
    {
      int ret = OB_SUCCESS;
      int64_t retry_times = rpc_option_.retry_times_;

      build_scan_param();
      while (retry_times-- > 0)
      {
        ret = rpc_option_.rpc_stub_->scan(rpc_option_.timeout_, rpc_option_.server_, 
                              scan_param_, scanner_);
        if (OB_SUCCESS == ret || OB_RESPONSE_TIME_OUT != ret) 
        {
          break;
        }
        usleep(static_cast<useconds_t>(rpc_option_.timeout_));
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to scan root table from root_server=%s, "
                        "table_id=%lu, ret=%d",
                  to_cstring(rpc_option_.server_), table_id_, ret);
      }
      else
      {
        scanner_iter_ = scanner_.begin();
      }

      return ret;
    }
    
    int ObTabletDistScanner::next_tablet_item()
    {
      int ret = OB_SUCCESS;
      ObCellInfo *cell = NULL;
      bool is_row_changed = false;
      int64_t ip = 0;
      int64_t port = 0;
      int64_t version = 0;
      int64_t status = -1;
      ObServer server;

      if (OB_SUCCESS != (ret = scanner_iter_.get_cell(&cell, &is_row_changed)))
      {
        TBSYS_LOG(WARN, "get cell from scanner iterator failed, table_id=%lu, ret=%d", 
                  table_id_, ret);
      }
      else if (is_row_changed)
      {
        tablet_item_.tablet_range_.table_id_ = table_id_;
        tablet_item_.tablet_range_.start_key_ = end_rowkey_;
        tablet_item_.tablet_range_.border_flag_.unset_inclusive_start();
        if (OB_SUCCESS != (ret = cell->row_key_.deep_copy(end_rowkey_, allocator_)))
        {
          TBSYS_LOG(WARN, "failed to copy last end rowkey, table_id=%lu, "
                          "rowkey=%s, ret=%d", 
                    table_id_, to_cstring(cell->row_key_), ret);
        }
        else 
        {
          tablet_item_.tablet_range_.end_key_ = end_rowkey_;
          tablet_item_.tablet_range_.border_flag_.set_inclusive_end();
        }

        //ignore the first is_row_changed flag
        is_row_changed = false;
      }

      while (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "get cell from scanner iterator failed, table_id=%lu, ret=%d", 
                    table_id_, ret);
        }
        else if (is_row_changed)
        {
          break; //just get one row
        }
        else if (NULL != cell)
        {
          if ((cell->column_name_.compare("1_port") == 0)
              || (cell->column_name_.compare("2_port") == 0)
              || (cell->column_name_.compare("3_port") == 0))
          {
            ret = cell->value_.get_int(port);
            TBSYS_LOG(DEBUG, "port is %ld", port);
          }
          else if ((cell->column_name_.compare("1_ipv4") == 0)
              || (cell->column_name_.compare("2_ipv4") == 0)
              || (cell->column_name_.compare("3_ipv4") == 0))
          {
            ret = cell->value_.get_int(ip);
            TBSYS_LOG(DEBUG, "ip is %ld", ip);
          }
          else if (cell->column_name_.compare("1_tablet_version") == 0 ||
              cell->column_name_.compare("2_tablet_version") == 0 ||
              cell->column_name_.compare("3_tablet_version") == 0)
          {
            ret = cell->value_.get_int(version);
            TBSYS_LOG(DEBUG, "tablet_version is %ld, rowkey:%s",
                      version, to_cstring(cell->row_key_));
          }
          else if ((cell->column_name_.compare("1_tablet_status") == 0)
              || (cell->column_name_.compare("2_tablet_status") == 0)
              || (cell->column_name_.compare("3_tablet_status") == 0))
          {
            ret = cell->value_.get_int(status);
            TBSYS_LOG(DEBUG, "tablet_status is %ld", status);
          }

          if (OB_SUCCESS == ret)
          {
            if (0 != port && 0 != ip && 0 != version && status >= 0)
            {
              server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
              TBSYS_LOG(DEBUG, "add tablet s:%s, %ld", to_cstring(server), version);
              ObTabletLocation addr(version, server);
              tablet_item_.locations_[tablet_item_.locations_count_].tablet_status_ = 
                static_cast<int32_t>(status);
              tablet_item_.locations_[tablet_item_.locations_count_++].location_ = addr;
              ip = 0;
              port = 0;
              version = 0;
              status = -1;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "check get value failed, table_id=%lu, ret=%d", 
                      table_id_, ret);
          }

          if (++scanner_iter_ == scanner_.end())
          {
            tablet_item_.tablet_range_.end_key_ = end_rowkey_;
            if (!end_rowkey_.is_max_row())
            {
              tablet_item_.tablet_range_.border_flag_.set_inclusive_end();
              if (OB_SUCCESS != (ret = scan()))
              {
                TBSYS_LOG(WARN, "failed to scan root table, table_id=%lu, ret=%d",
                          table_id_, ret);
              }
            }
            else 
            {
              tablet_item_.tablet_range_.border_flag_.unset_inclusive_end();
            }
            break;
          }
          ret = scanner_iter_.get_cell(&cell, &is_row_changed);
        }
        else 
        {
          //impossible
          TBSYS_LOG(WARN, "unexpect error, cell=%p", cell);
          ret = OB_ERROR;
          break;
        }
      }

      return ret;
    }

    int ObTabletDistScanner::next(ObTabletDistItem*& tablet_item)
    {
      int ret = OB_SUCCESS;
      tablet_item = NULL;

      tablet_item_.reset();
      if (end_rowkey_.is_max_row())
      {
        ret = OB_ITER_END;
      }
      else if (OB_SUCCESS != (ret = next_tablet_item()))
      {
        TBSYS_LOG(WARN, "failed to iterate new tablet item, ret=%d", ret);
      }
      else
      {
        tablet_item = &tablet_item_;
      }

      return ret;
    }

    int ObTabletDistribution::init(
       const int64_t table_id, const ObRpcOptions& rpc_option)
    {
      int ret = OB_SUCCESS;
      ObTabletDistItem* tablet_item = NULL;

      reset();
      if (OB_SUCCESS != (ret = tablet_dist_scanner_.init(table_id, rpc_option)))
      {
        TBSYS_LOG(WARN, "failed to initialize tablet distribution scanner, "
                        "table_id=%lu, ret=%d", 
                  table_id, ret);
      }
      else 
      {
        while (OB_SUCCESS == ret)
        {
          if (OB_ITER_END == (ret = tablet_dist_scanner_.next(tablet_item)))
          {
            ret = OB_SUCCESS;
            break;
          }
          else if (OB_SUCCESS != ret || NULL == tablet_item)
          {
            TBSYS_LOG(WARN, "failed to scan tablet distribution from rootserver, "
                            "ret=%d", ret);
            break;
          }
          else
          {
            tablet_item->in_array_pos_ = get_tablet_item_count();
            ret = tablet_item_array_.push_back(*tablet_item);
          }
        }
      }

      if (OB_SUCCESS == ret && get_tablet_item_count() <= 0)
      {
        TBSYS_LOG(WARN, "after init, get no tablet from rootserver for table_id=%lu", 
                  table_id);
        ret = OB_ERROR;
      }

      return ret;
    }

    void ObTabletDistribution::reset()
    {
      allocator_.reuse();
      tablet_item_array_.clear();
    }

    int ObTabletDistribution::add_tablet_item(const ObTabletDistItem& tablet_item)
    {
      int ret = OB_SUCCESS;
      int64_t index = tablet_item_array_.count(); 
      ObRowkey new_rowkey;

      if (OB_SUCCESS != (ret = tablet_item_array_.push_back(tablet_item)))
      {
        TBSYS_LOG(WARN, "failed to push tablet item in tablet distribution, ret=%d",
                  ret);
      }
      else if (OB_SUCCESS != (ret = tablet_item_array_.at(index).tablet_range_.start_key_.
                              deep_copy(new_rowkey, allocator_)))
      {
        TBSYS_LOG(WARN, "failed to deep_copy start key, ret=%d", ret);
      }
      else
      {
        tablet_item_array_.at(index).in_array_pos_ = index;

        tablet_item_array_.at(index).tablet_range_.start_key_ = new_rowkey;
        if (OB_SUCCESS != (ret = tablet_item_array_.at(index).tablet_range_.end_key_.
                                deep_copy(new_rowkey, allocator_)))
        {
          TBSYS_LOG(WARN, "failed to deep_copy end key, ret=%d", ret);
        }
        else 
        {
          tablet_item_array_.at(index).tablet_range_.end_key_ = new_rowkey;
        }
      }

      return ret;
    }

    bool ObTabletDistribution::has_unavaliable_tablet(const common::ObServer& server) const
    {
      ObTabletUnavaliablePredicator predicator(server);
      ObTabletIterator tablet_iterator(*this, predicator);
      return tablet_iterator.has_tablet();
    }

    int ObTabletIterator::next(ObTabletHandle& tablet_handle)
    {
      int ret = OB_SUCCESS;
      const ObTabletDistItem* tablet_item = NULL;
      ObTablet* tablet = NULL;

      if (OB_SUCCESS == (ret = next(tablet_item)))
      {
        if (NULL != tablet_item
            && OB_SUCCESS != (ret = tablet_dist_->tablet_image_.acquire_tablet(
               tablet_item->tablet_range_, tablet)))
        {
          TBSYS_LOG(WARN, "failed to acquire_tablet, tablet_range_=%s",
                    to_cstring(tablet_item->tablet_range_));
        }
        else if (NULL != tablet)
        {
          ObTabletHandle tmp_tablet_handle(tablet, &tablet_dist_->tablet_image_);
          tablet_handle = tmp_tablet_handle;
        }
      }

      return ret;
    }

    int ObTabletIterator::next(const ObTabletDistItem*& tablet_item)
    {
      int ret = OB_SUCCESS;
      tablet_item = NULL;

      if (NULL == tablet_dist_ || NULL == predicator_)
      {
        TBSYS_LOG(WARN, "tablet iterator isn't initialized, tablet_dist_=%p, "
                        "predicator_=%p", tablet_dist_, predicator_);
        ret = OB_NOT_INIT;
      }
      else 
      {
        CThreadGuard guard(&mutex_);
        while (true)
        {
          if (cur_index_ < tablet_dist_->tablet_item_array_.count() 
              && got_count_ < limit_count_)
          {
            tablet_item = &tablet_dist_->tablet_item_array_.at(cur_index_++);
            if (predicator_->predicate(*tablet_item))
            {
              ++got_count_;
              break;
            }
          }
          else 
          {
            tablet_item = NULL;
            ret = OB_ITER_END;
            break;
          }
        }
      }

      return ret;
    }

    bool ObTabletIterator::has_tablet()
    {
      bool ret = false;
      const ObTabletDistItem* tablet_item = NULL;

      if (NULL == tablet_dist_ || NULL == predicator_)
      {
        TBSYS_LOG(WARN, "tablet iterator isn't initialized, tablet_dist_=%p, "
                        "predicator_=%p", tablet_dist_, predicator_);
        ret = false;
      }
      else 
      {
        CThreadGuard guard(&mutex_);
        for (cur_index_ = 0; cur_index_ < tablet_dist_->tablet_item_array_.count(); 
             ++cur_index_)
        {
          tablet_item = &tablet_dist_->tablet_item_array_.at(cur_index_++);
          if (predicator_->predicate(*tablet_item))
          {
            ret = true;
            break;
          }
        }
        cur_index_ = 0;
      }

      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
