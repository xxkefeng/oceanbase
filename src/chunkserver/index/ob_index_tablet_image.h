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
#ifndef OCEANBASE_CHUNKSERVER_INDEX_TABLET_IMAGE_H_
#define OCEANBASE_CHUNKSERVER_INDEX_TABLET_IMAGE_H_

#include "chunkserver/ob_tablet_image.h"

namespace oceanbase
{
  namespace chunkserver
  {
    struct ObTabletUpdateInfo
    {
      ObTabletUpdateInfo()
      : data_tablet_(NULL),
        tablet_version_(0),
        tablet_range_(NULL)
      {

      }

      ObTablet *data_tablet_;
      int64_t tablet_version_;
      sstable::ObSSTableId sstable_id_;
      const common::ObNewRange* tablet_range_;
      ObTabletExtendInfo extend_info_;
    };

    class ObBasicTabletImage
    {
      public: 
        virtual ~ObBasicTabletImage()
        {

        }

        int acquire_tablet(const common::ObNewRange &range, 
            ObTablet*& tablet) const;

        int release_tablet(ObTablet* tablet) const;

        virtual int update(const ObTabletUpdateInfo& update_info) = 0;

      protected:
        ObBasicTabletImage(ObMultiVersionTabletImage& tablet_image)
        : tablet_image_(tablet_image)
        {

        }

        int new_tablet_object(const ObTabletUpdateInfo& update_info, 
                              ObTablet*& new_tablet);

      protected:
        ObMultiVersionTabletImage& tablet_image_;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObBasicTabletImage);
    };

    class ObLocalIndexTabletImage : public ObBasicTabletImage
    {
      public: 
        ObLocalIndexTabletImage(ObMultiVersionTabletImage& tablet_image)
        : ObBasicTabletImage(tablet_image)
        {

        }

        virtual ~ObLocalIndexTabletImage()
        {

        }

        virtual int update(const ObTabletUpdateInfo& update_info);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObLocalIndexTabletImage);
    };

    class ObGlobalIndexTabletImage : public ObBasicTabletImage
    {
      public: 
        ObGlobalIndexTabletImage(ObMultiVersionTabletImage& tablet_image)
        : ObBasicTabletImage(tablet_image)
        {

        }

        virtual ~ObGlobalIndexTabletImage()
        {

        }

        virtual int update(const ObTabletUpdateInfo& update_info);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObGlobalIndexTabletImage);
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_INDEX_TABLET_IMAGE_H_
