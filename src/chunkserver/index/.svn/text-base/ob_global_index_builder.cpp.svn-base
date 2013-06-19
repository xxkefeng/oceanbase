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
#include "ob_tablet_distribution.h"
#include "ob_tablet_predicator.h"
#include "ob_global_index_builder.h"
#include "chunkserver/ob_tablet_manager.h"
#include "ob_tablet_global_index_builder.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace common;

    ObGlobalIndexBuilder::ObGlobalIndexBuilder(
       ObTabletManager& tablet_manager, ObIndexBuildOptions& build_option)
    : ObIndexBuilder(tablet_manager, build_option),
      tablet_image_(tablet_manager.get_serving_tablet_image()),
      data_table_tablet_dist_(NULL),
      index_table_tablet_dist_(tablet_image_),
      index_tablet_iter_(NULL)
    {

    }

    ObGlobalIndexBuilder::~ObGlobalIndexBuilder() 
    {

    }

    int ObGlobalIndexBuilder::init(ObTabletDistribution& data_table_tablet_dist)
    {
      int ret = OB_SUCCESS;

      do
      {
        if (!build_option_.is_valid())
        {
          ret = OB_INVALID_ARGUMENT;
        }
        else if (OB_SUCCESS != (ret = index_table_tablet_dist_.init(
           build_option_.index_table_id_, build_option_.rpc_option_)))
        {
          TBSYS_LOG(WARN, "failed to init index table tablet distribution, ret=%d", ret);
        }
        else if (has_unavaliable_tablet())
        {
          stop_ = false;
          data_table_tablet_dist_ = &data_table_tablet_dist;

          selected_and_no_avaliable_pred_.reset(build_option_.self_server_,
              common::TABLET_UNAVAILABLE);
          index_tablet_iter_ = &get_tablet_iterator(selected_and_no_avaliable_pred_);
          if (index_tablet_iter_->has_tablet())
          {
            break;
          }
          else
          {
            tablet_avaliable_pred_.reset(build_option_.self_server_,
                common::TABLET_UNAVAILABLE);
            index_tablet_iter_ = &get_tablet_iterator(tablet_avaliable_pred_);
            if (!index_tablet_iter_->has_tablet())
            {
              TBSYS_LOG(INFO, "the index tablets to be migrated in aren't avaliable, "
                              "sleep 30s and recheck again");
              usleep(WAIT_TABLET_AVALIABLE_TIME);
              ret = OB_CS_EAGAIN;
            }
          }
        }
      }
      while (OB_CS_EAGAIN == ret); 

      return ret;
    }

    int ObGlobalIndexBuilder::batch_build()
    {
      int ret = OB_SUCCESS;
      const ObTabletDistItem* tablet_item = NULL;

      if (NULL == index_tablet_iter_)
      {
        TBSYS_LOG(WARN, "not inited, index_tablet_iter_=%p", index_tablet_iter_);
        ret = OB_NOT_INIT;
      }

      while (OB_SUCCESS == ret)
      {
        if (stop_)
        {
          ret = OB_CANCELED;
          break;
        }

        ret = index_tablet_iter_->next(tablet_item);
        if (OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
          break;
        }
        else if ((OB_SUCCESS != ret && OB_ITER_END != ret)
            || NULL == tablet_item)
        {
          TBSYS_LOG(WARN, "failed to get tablet item from index table tablet "
                          "distribution, index_tablet_item=%p, ret=%d", 
                    tablet_item, ret);
          break;
        }
        else if (OB_SUCCESS != (ret = build_tablet(*tablet_item)))
        {
          TBSYS_LOG(WARN, "failed to build one global index tablet, "
                          "index_tablet_range=%s",
                    to_cstring(tablet_item->tablet_range_));
          break;
        }
        tablet_item = NULL;
      }

      return ret;
    }

    int ObGlobalIndexBuilder::build_tablet(const ObTabletDistItem& index_tablet_item)
    {
      int ret = OB_SUCCESS;
      const ObSchemaManagerV2* schema_mgr = NULL;
      ObTabletGlobalIndexBuilder* tablet_global_index_builder = NULL;

      if (NULL == (tablet_global_index_builder = GET_TSI_ARGS(
         ObTabletGlobalIndexBuilder, TSI_CS_TABLE_GLOBAL_INDEX_BUILDER_1, 
         tablet_manager_, tablet_image_)))
      {
        TBSYS_LOG(ERROR, "failed allocate memory for tablet global index builder instance");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if(NULL == (schema_mgr = build_option_.merger_schema_mgr_->get_user_schema(0)))
      {
        TBSYS_LOG(WARN, "get user schema manager fail, schema_mgr=%p, ret=%d", 
                  schema_mgr, ret);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = tablet_global_index_builder->build(
         *data_table_tablet_dist_, *schema_mgr, index_tablet_item, build_option_.rpc_option_,
         build_option_.self_server_, build_option_.sort_file_setter_, stop_, build_option_.write_sstable_version_)))
      {
        TBSYS_LOG(WARN, "failed to build tablet global index, tablet_range=%s, ret=%d", 
                  to_cstring(index_tablet_item.tablet_range_), ret);
      }

      if (NULL != schema_mgr)
      {
        if(OB_SUCCESS != build_option_.merger_schema_mgr_->release_schema(schema_mgr))
        {
          TBSYS_LOG(WARN, "release schema manager fail, schema_mgr=%p, ret=%d", 
                    schema_mgr, ret);
        }
      }

      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
