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
#ifndef OCEANBASE_CHUNKSERVER_TABLET_PREDICATOR_H_
#define OCEANBASE_CHUNKSERVER_TABLET_PREDICATOR_H_

#include "common/ob_server.h"
#include "ob_tablet_distribution.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletDistItem;

    struct ObTabletLocationSelector
    {
      // Return %item.locations_ array index
      int64_t operator()(const ObTabletDistItem &item) const
      {
        int64_t index = 0;
        if (item.locations_count_ > 0)
        {
          index = (item.in_array_pos_ + item.location_start_) % item.locations_count_;
        }
        return index;
      }
    };

    class ObPredicator
    {
      public:
        virtual ~ObPredicator() { }

        void reset(const common::ObServer& server, 
                   int status = common::TABLET_UNAVAILABLE)
        {
          server_ = server;
          tablet_status_ = status;
        }

        virtual bool predicate(const ObTabletDistItem& tablet_dist_item) const = 0;

      protected:
        ObPredicator(const common::ObServer& server, const int status)
        : server_(server),
          tablet_status_(status)
        {

        }

        ObPredicator()
        : tablet_status_(common::TABLET_UNAVAILABLE)
        {
        }

        common::ObServer server_;
        int tablet_status_;
        ObTabletLocationSelector location_selector_;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObPredicator);
    };

    class ObSelectedTabletPredicator : public ObPredicator
    {
      public:
        ObSelectedTabletPredicator(const common::ObServer& server, 
                                const int status = common::TABLET_UNAVAILABLE)
        : ObPredicator(server, status)
        {

        }

        ObSelectedTabletPredicator()
        : ObPredicator()
        {

        }

        virtual ~ObSelectedTabletPredicator() { }

        virtual bool predicate(const ObTabletDistItem& tablet_dist_item) const
        {
          bool ret = false;
          int64_t selected = location_selector_(tablet_dist_item);

          if (tablet_dist_item.locations_count_ > 0
              && tablet_dist_item.locations_[selected].location_.chunkserver_ == server_
              && tablet_dist_item.locations_[selected].tablet_status_ == tablet_status_)
          {
            ret = true;
          }

          return ret;
        } 

      private:
        DISALLOW_COPY_AND_ASSIGN(ObSelectedTabletPredicator);
    };

    class ObAllTabletPredicator : public ObPredicator
    {
      public:
        ObAllTabletPredicator()
        : ObPredicator()
        {

        }

        virtual ~ObAllTabletPredicator() { }

        virtual bool predicate(const ObTabletDistItem& tablet_dist_item) const
        {
          UNUSED(tablet_dist_item);
          return true;
        } 

      private:
        DISALLOW_COPY_AND_ASSIGN(ObAllTabletPredicator);
    };

    class ObTabletUnavaliablePredicator : public ObPredicator
    {
      public:
        ObTabletUnavaliablePredicator(const common::ObServer& server)
        : ObPredicator(server, common::TABLET_UNAVAILABLE)
        {

        }

        ObTabletUnavaliablePredicator()
        : ObPredicator()
        {

        }

        virtual ~ObTabletUnavaliablePredicator() { }

        virtual bool predicate(const ObTabletDistItem& tablet_dist_item) const
        {
          bool ret = false;
          int64_t index = tablet_dist_item.get_server_index(server_);

          if (tablet_dist_item.locations_count_ > 0 && index >= 0
              && tablet_dist_item.locations_[index].tablet_status_ == tablet_status_)
          {
            ret = true;
          }

          return ret;
        } 

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletUnavaliablePredicator);
    };

    // is selected and no avaliable tablet
    class ObSelectedAndNoAvaliablePredicator : public ObPredicator
    {
      public:
        ObSelectedAndNoAvaliablePredicator(const common::ObServer& server)
        : ObPredicator(server, common::TABLET_UNAVAILABLE)
        {

        }

        ObSelectedAndNoAvaliablePredicator()
        : ObPredicator()
        {

        }

        virtual ~ObSelectedAndNoAvaliablePredicator() { }

        virtual bool predicate(const ObTabletDistItem& tablet_dist_item) const
        {
          bool ret = false;
          const int64_t index = tablet_dist_item.get_server_index(server_);
          const int64_t selected = location_selector_(tablet_dist_item);

          if (tablet_dist_item.locations_count_ > 0 && index >= 0
              && index == selected && !tablet_dist_item.has_tablet_avaliable())
          {
            ret = true;
          }

          return ret;
        } 

      private:
        DISALLOW_COPY_AND_ASSIGN(ObSelectedAndNoAvaliablePredicator);
    };

    // have avaliable tablet
    class ObTabletAvaliablePredicator : public ObPredicator
    {
      public:
        ObTabletAvaliablePredicator(const common::ObServer &server)
          : ObPredicator(server, common::TABLET_UNAVAILABLE)
        {
        }

        ObTabletAvaliablePredicator() : ObPredicator() { }

        virtual ~ObTabletAvaliablePredicator() {}

        virtual bool predicate(const ObTabletDistItem &tablet_dist_item) const
        {
          bool ret = false;
          const int64_t index = tablet_dist_item.get_server_index(server_);

          if (tablet_dist_item.locations_count_ > 0 && index >= 0
              && tablet_dist_item.locations_[index].tablet_status_ == tablet_status_
              && tablet_dist_item.has_tablet_avaliable())
          {
            ret = true;
          }

          return ret;
        } 
    };

  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_TABLET_PREDICATOR_H_
