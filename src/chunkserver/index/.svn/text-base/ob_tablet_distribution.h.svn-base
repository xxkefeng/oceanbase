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
#ifndef OCEANBASE_CHUNKSERVER_TABLET_DISTRIBUTION_H_
#define OCEANBASE_CHUNKSERVER_TABLET_DISTRIBUTION_H_

#include <algorithm>
#include "common/ob_server.h"
#include "common/ob_rowkey.h"
#include "common/ob_scanner.h"
#include "common/ob_scan_param.h"
#include "common/ob_array.h"
#include "common/ob_general_rpc_stub.h"
#include "chunkserver/ob_tablet.h"
#include "ob_table.h"
#include "ob_index_tablet_image.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObPredicator;
    class ObTabletIterator;

    struct ObTabletLocationItem
    {
      int32_t tablet_status_;
      common::ObTabletLocation location_;
    };

    struct ObTabletDistItem
    {
      static const int64_t MAX_REPLICA_COUNT = 3;

      common::ObNewRange tablet_range_;
      int64_t in_array_pos_;
      // replica locaton array start offset, for global index build retry.
      mutable volatile int64_t location_start_;
      int64_t locations_count_;
      ObTabletLocationItem locations_[MAX_REPLICA_COUNT];

      ObTabletDistItem()
      {
        reset();
      }

      void reset()
      {
        tablet_range_.reset();
        in_array_pos_ = 0;
        location_start_ = 0;
        locations_count_ = 0;
        for (int64_t i = 0; i < MAX_REPLICA_COUNT; ++i)
        {
          locations_[i].tablet_status_ = common::TABLET_UNAVAILABLE;
        }
      }

      int set_state(const ObServer& server, int state)
      {
        int ret = common::OB_SUCCESS;
        int64_t i = 0;

        for (i = 0; i < locations_count_; ++i)
        {
          if (locations_[i].location_.chunkserver_ == server)
          {
            locations_[i].tablet_status_ = state;
            break;
          }
        }
        if (locations_count_ == i)
        {
          ret = common::OB_ENTRY_NOT_EXIST;
        }

        return ret;
      }

      int get_state(const ObServer& server, int& state) const
      {
        int ret = common::OB_SUCCESS;
        int64_t i = 0;

        for (i = 0; i < locations_count_; ++i)
        {
          if (locations_[i].location_.chunkserver_ == server)
          {
            state = locations_[i].tablet_status_;
            break;
          }
        }
        if (locations_count_ == i)
        {
          ret = common::OB_ENTRY_NOT_EXIST;
        }

        return ret;
      }

      inline const int64_t get_server_index(const ObServer& server) const 
      {
        int64_t i = 0;

        for (i = 0; i < locations_count_; ++i)
        {
          if (locations_[i].location_.chunkserver_ == server)
          {
            break;
          }
        }
        if (locations_count_ == i)
        {
          i = -1;
        }

        return i;
      }

      inline const bool has_tablet_avaliable() const
      {
        int rc = false;
        for (int64_t i = 0; i < locations_count_; i++)
        {
          if (locations_[i].tablet_status_ == common::TABLET_AVAILABLE)
          {
            rc = true;
            break;
          }
        }

        return rc;
      }

      inline void chose_next_location(int64_t start) const
      {
        if (locations_count_ > 0)
        {
          int64_t new_start = (start + 1) % locations_count_;
          __sync_bool_compare_and_swap(&location_start_, start, new_start);
        }
      }

      int64_t to_string(char* buffer, const int64_t size) const;
    };

    class ObTabletDistScanner
    {
      public:
        ObTabletDistScanner(ObIAllocator& allocator) 
        : allocator_(allocator)
        { 
        }
        ~ObTabletDistScanner() { }

        int init(const uint64_t table_id, const ObRpcOptions& rpc_option);

        int next(ObTabletDistItem*& tablet_item);

      private:
        int scan();
        void build_scan_param();
        int next_tablet_item();  

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletDistScanner);
        
        ObIAllocator& allocator_;
        uint64_t table_id_;
        ObRpcOptions rpc_option_;

        common::ObRowkey end_rowkey_;
        ObTabletDistItem tablet_item_;

        common::ObScanParam scan_param_;
        common::ObScanner scanner_;
        common::ObScannerIterator scanner_iter_;
    };

    class ObTabletHandle
    {
      public:
        ObTabletHandle()
        : tablet_(NULL), 
          tablet_image_(NULL)
        {

        }

        ObTabletHandle(ObTablet* tablet, ObBasicTabletImage* tablet_image)
        : tablet_(tablet), 
          tablet_image_(tablet_image)
        {

        }

        explicit ObTabletHandle(ObTabletHandle& other)
        {
          *this = other;
        };

        ~ObTabletHandle()
        {
          if (NULL != tablet_ && NULL != tablet_image_)
          {
            if (common::OB_SUCCESS != tablet_image_->release_tablet(tablet_))
            {
              TBSYS_LOG(WARN, "failed release_tablet, tablet_range_=%s",
                        to_cstring(tablet_->get_range()));
            }
          }
          tablet_ = NULL;
          tablet_image_ = NULL;
        }

        ObTabletHandle& operator = (ObTabletHandle& other)
        {
          if (this != &other)
          {
            std::swap(tablet_, other.tablet_);
            std::swap(tablet_image_, other.tablet_image_);
          }

          return *this;
        }

        ObTablet* get_tablet() const
        {
          return tablet_;
        }

        void reset()
        {
          ObTabletHandle tmp_handle;
          *this = tmp_handle;
        }

      private:
        ObTablet* tablet_;
        ObBasicTabletImage* tablet_image_;
    };

    class ObTabletDistribution
    {
      public: 
        ObTabletDistribution(ObBasicTabletImage& tablet_image)
        : allocator_(common::ObModIds::OB_CS_BUILD_INDEX),
          tablet_image_(tablet_image),
          tablet_dist_scanner_(allocator_)
        {

        }
        ~ObTabletDistribution() { }

        int init(const int64_t table_id, const ObRpcOptions& rpc_option);

        void reset();
        int add_tablet_item(const ObTabletDistItem& tablet_item);

        inline const common::ObArray<ObTabletDistItem>& get_tablet_item_array() const
        {
          return tablet_item_array_;
        }

        inline int64_t get_tablet_item_count() const
        {
          return tablet_item_array_.count();
        }

        bool has_unavaliable_tablet(const common::ObServer& server) const;

      private:
        friend class ObTabletIterator;
        DISALLOW_COPY_AND_ASSIGN(ObTabletDistribution);

        common::ObArenaAllocator allocator_;
        ObBasicTabletImage& tablet_image_;
        ObTabletDistScanner tablet_dist_scanner_;
        common::ObArray<ObTabletDistItem> tablet_item_array_;
    };

    class ObTabletIterator
    {
      public:
        ObTabletIterator(const ObTabletDistribution& tablet_dist, 
                         const ObPredicator& predicator,
                         const int64_t limit_count = INT64_MAX)
        : cur_index_(0),
          limit_count_(limit_count),
          got_count_(0),
          tablet_dist_(&tablet_dist),
          predicator_(&predicator)
        {

        }

        ObTabletIterator()
        : cur_index_(0),
          limit_count_(INT64_MAX),
          got_count_(0),
          tablet_dist_(NULL),
          predicator_(NULL)
        {

        }

        ~ObTabletIterator() { }

        int next(ObTabletHandle& tablet_handle);

        int next(const ObTabletDistItem*& tablet_item);

        bool has_tablet();

        void reset_iterator(
           const ObTabletDistribution& tablet_dist, 
           const ObPredicator& predicator,
           const int64_t limit_count = INT64_MAX) 
        { 
          cur_index_ = 0;
          limit_count_ = limit_count;
          got_count_ = 0;
          tablet_dist_ = &tablet_dist;
          predicator_ = &predicator;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletIterator);

        int64_t cur_index_;
        int64_t limit_count_;
        int64_t got_count_;
        const ObTabletDistribution* tablet_dist_;
        const ObPredicator* predicator_;
        tbsys::CThreadMutex mutex_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_TABLET_DISTRIBUTION_H_
