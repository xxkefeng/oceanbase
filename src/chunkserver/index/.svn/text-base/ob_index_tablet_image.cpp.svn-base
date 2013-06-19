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
#include "sstable/ob_disk_path.h"
#include "ob_tablet_sstable.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace tbsys;
    using namespace oceanbase;
    using namespace common;
    using namespace sstable;

    int ObBasicTabletImage::acquire_tablet(
       const ObNewRange &range, ObTablet*& tablet) const
    {
      return tablet_image_.acquire_tablet(range, 
             ObMultiVersionTabletImage::SCAN_FORWARD, 0, tablet);
    }

    int ObBasicTabletImage::release_tablet(ObTablet* tablet) const
    {
      return tablet_image_.release_tablet(tablet);
    }

    int ObBasicTabletImage::new_tablet_object(
       const ObTabletUpdateInfo& update_info, ObTablet*& new_tablet)
    {
      int ret = OB_SUCCESS;
      new_tablet = NULL;

      if (OB_SUCCESS != (ret = tablet_image_.alloc_tablet_object(
         *update_info.tablet_range_, update_info.tablet_version_, new_tablet)))
      {
        TBSYS_LOG(WARN, "alloc_tablet_object failed, range=%s, version=%ld, ret=%d", 
            to_cstring(*update_info.tablet_range_), update_info.tablet_version_, ret);
      }
      else if (NULL != new_tablet 
               && OB_SUCCESS != (ret = new_tablet->add_sstable_by_id(update_info.sstable_id_)) )
      {
        TBSYS_LOG(WARN, "add sstable to tablet failed, ret=%d", ret);
      }
      else
      {
        new_tablet->set_data_version(update_info.tablet_version_);
        new_tablet->set_disk_no(static_cast<int32_t>(get_sstable_disk_no(
           update_info.sstable_id_.sstable_file_id_)));
        new_tablet->set_extend_info(update_info.extend_info_);
      }

      return ret;
    }

    int ObLocalIndexTabletImage::update(const ObTabletUpdateInfo& update_info)
    {
      int ret = OB_SUCCESS;
      ObTablet* local_index = NULL;

      if (NULL == update_info.data_tablet_)
      {
        TBSYS_LOG(WARN, "invalid parameter, data_tablet_=%p", 
                  update_info.data_tablet_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = new_tablet_object(update_info, local_index)))
      {
        TBSYS_LOG(WARN, "failed to new tablet object, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = update_info.data_tablet_->set_local_index(local_index)))
      {
        TBSYS_LOG(WARN, "failed to set local index for tablet, data_tablet_range=%s, "
                        "local_index_range=%s",
                  to_cstring(update_info.data_tablet_->get_range()),
                  to_cstring(local_index->get_range()));
      }

      if (OB_SUCCESS != ret && NULL != local_index)
      {
        local_index->~ObTablet();
      }

      return ret;
    }

    int ObGlobalIndexTabletImage::update(const ObTabletUpdateInfo& update_info)
    {
      int ret = OB_SUCCESS;
      ObTablet* new_tablet = NULL;

      if (OB_SUCCESS != (ret = new_tablet_object(update_info, new_tablet)))
      {
        TBSYS_LOG(WARN, "failed to new tablet object, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = tablet_image_.add_tablet(new_tablet)))
      {
        TBSYS_LOG(WARN, "failed to add new tablet into tablet image, tablet_range=%s",
                  to_cstring(new_tablet->get_range()));
      }
      else if (OB_SUCCESS != (ret = tablet_image_.write(
        new_tablet->get_data_version(), new_tablet->get_disk_no())))
      {
        TBSYS_LOG(WARN, "write new tablet meta failed, version=%ld, disk_no=%d, "
                        "tablet_range_=%s", 
                  new_tablet->get_data_version(), new_tablet->get_disk_no(),
                  to_cstring(new_tablet->get_range()));
      }

      if (OB_SUCCESS != ret && NULL != new_tablet)
      {
        new_tablet->~ObTablet();
      }

      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
