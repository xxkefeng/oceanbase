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
#include "common/ob_tsi_factory.h"
#include "common/ob_schema_manager.h"
#include "ob_tablet_predicator.h"
#include "ob_local_index_builder.h"
#include "chunkserver/ob_tablet_manager.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace common;

    ObLocalIndexBuilder::ObLocalIndexBuilder(
       ObTabletManager& tablet_manager, ObIndexBuildOptions& build_option)
    : ObIndexBuilder(tablet_manager, build_option),
      tablet_image_(tablet_manager.get_serving_tablet_image()),
      data_table_tablet_dist_(tablet_image_),
      data_tablet_iter_(NULL)
    {

    }

    ObLocalIndexBuilder::~ObLocalIndexBuilder() 
    {

    }

    // NOTE: we don't call init() before build() when delay building local index.
    int ObLocalIndexBuilder::init()
    {
      int ret = OB_SUCCESS;

      if (!build_option_.is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = data_table_tablet_dist_.init(
         build_option_.data_table_id_, build_option_.rpc_option_)))
      {
        TBSYS_LOG(WARN, "failed to init data table tablet distribution, "
                        "data_table_id=%lu, ret=%d", 
                  build_option_.data_table_id_, ret);
      }
      else 
      {
        /**
         * only if this chunkserver is the selected location server of the 
         * data table tablet, it build the local index and store the 
         * local index with sstable format
         */
        stop_ = false;
        data_tablet_iter_ = &get_selected_tablet_iterator(
           data_table_tablet_dist_, common::TABLET_AVAILABLE);
      }

      return ret;
    }

    int ObLocalIndexBuilder::build_all()
    {
      int ret = OB_SUCCESS;
      ObTablet* data_tablet = NULL;
      ObTabletHandle tablet_handle;

      if (NULL == data_tablet_iter_)
      {
        TBSYS_LOG(WARN, "not inited, data_tablet_iter_=%p", data_tablet_iter_);
        ret = OB_NOT_INIT;
      }

      while (OB_SUCCESS == ret)
      {
        if (stop_)
        {
          ret = OB_CANCELED;
          break;
        }
        ret = data_tablet_iter_->next(tablet_handle);
        if (OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
          break;
        }
        else if ((OB_SUCCESS != ret && OB_ITER_END != ret)
            || NULL == (data_tablet = tablet_handle.get_tablet()))
        {
          TBSYS_LOG(WARN, "failed to get tablet from data table tablet "
                          "distribution, tablet=%p, ret=%d", 
                    data_tablet, ret);
          break;
        }
        else if (OB_SUCCESS != (ret = build(build_option_, *data_tablet)))
        {
          TBSYS_LOG(WARN, "failed to build data tablet local index "
                          "from data tablet=%s",
                    to_cstring(data_tablet->get_range()));
          break;
        }
        tablet_handle.reset();
      }

      return ret;
    }

    int ObLocalIndexBuilder::build(ObIndexBuildOptions &option, ObTablet& data_tablet)
    {
      int ret = OB_SUCCESS;
      const ObSchemaManagerV2* schema_mgr = NULL;
      ObTabletLocalIndexBuilder* tablet_local_index_builder = NULL;

      if (data_tablet.get_range().table_id_ != option.data_table_id_)
      {
        TBSYS_LOG(WARN, "data tablet table_id=%lu not equal to data_table_id_=%lu",
                  data_tablet.get_range().table_id_, option.data_table_id_);
      }
      else if (NULL != data_tablet.get_local_index()
               && data_tablet.get_local_index()->get_range().table_id_ == option.index_table_id_) 
      {
        TBSYS_LOG(INFO, "local index of this tablet is existent, needn't build again, "
                        "tablet_range=%s, index_table_id_=%lu",
                  to_cstring(data_tablet.get_range()), option.index_table_id_);
        ret = OB_SUCCESS;
      }
      else if (NULL == (tablet_local_index_builder = GET_TSI_ARGS(
         ObTabletLocalIndexBuilder, TSI_CS_TABLE_LOCAL_INDEX_BUILDER_1, 
         tablet_manager_, tablet_image_)))
      {
        TBSYS_LOG(ERROR, "failed allocate memory for tablet local index builder instance");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if(NULL == (schema_mgr = option.merger_schema_mgr_->get_user_schema(0)))
      {
        TBSYS_LOG(WARN, "get user schema mgr fail, ret=%d", ret);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = tablet_local_index_builder->build(
         *schema_mgr, data_tablet, option.index_table_id_,
         option.sort_file_setter_, build_option_.write_sstable_version_)))
      {
        TBSYS_LOG(WARN, "failed to build data table tablet local index, "
                        "tablet_range=%s, ret=%d", 
                  to_cstring(data_tablet.get_range()), ret);
      }

      if (NULL != schema_mgr)
      {
        if(OB_SUCCESS != option.merger_schema_mgr_->release_schema(schema_mgr))
        {
          TBSYS_LOG(WARN, "release schema manager fail, ret=%d", ret);
        }
      }

      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
