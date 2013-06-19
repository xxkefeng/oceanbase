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
#include "common/ob_tsi_factory.h"
#include "common/ob_schema_manager.h"
#include "ob_tablet_distribution.h"
#include "ob_tablet_predicator.h"
#include "ob_local_index_sampler.h"
#include "chunkserver/ob_tablet_manager.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace common;
    using namespace sql;

    ObLocalIndexSampler::ObLocalIndexSampler(ObTabletManager& tablet_manager,
        ObIndexBuildOptions& build_option, ObLocalIndexBuilder& local_index_builder)
    : ObIndexBuilder(tablet_manager, build_option),
      local_index_builder_(local_index_builder),
      tablet_image_(tablet_manager.get_serving_tablet_image()),
      data_tablet_iter_(NULL)
    {

    }

    ObLocalIndexSampler::~ObLocalIndexSampler() 
    {

    }

    int ObLocalIndexSampler::init()
    {
      int ret = OB_SUCCESS;

      if (!build_option_.is_valid() || build_option_.sample_count_ < 0)
      {
        TBSYS_LOG(WARN, "invalid argument, index_table_id=%lu, sample_count=%ld", 
                  build_option_.index_table_id_, build_option_.sample_count_);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        stop_ = false;
        media_sample_memtable_.reset();
        data_tablet_iter_ = &get_selected_tablet_iterator(
           local_index_builder_.get_data_table_tablet_dist(), common::TABLET_AVAILABLE, 
           get_sample_tablet_count());
      }

      return ret;
    }

    int64_t ObLocalIndexSampler::get_sample_tablet_count()
    {
      return is_rowkey_first_column_same() ? INT64_MAX : DEFAULT_SAMPLEE_TABLET_COUNT;
    }

    bool ObLocalIndexSampler::is_rowkey_first_column_same()
    {
      bool ret = false;
      const ObSchemaManagerV2* schema_mgr = NULL;
      const ObTableSchema* data_table_schema = NULL;
      const ObTableSchema* index_table_schema = NULL;

      if(NULL == (schema_mgr = build_option_.merger_schema_mgr_->get_user_schema(0)))
      {
        TBSYS_LOG(WARN, "get user schema mgr fail, schema_mgr=%p, ret=%d", 
                  schema_mgr, ret);
      }
      else if (NULL == (data_table_schema = schema_mgr->get_table_schema(
         build_option_.data_table_id_)))
      {
        TBSYS_LOG(WARN, "failed to get data table schema, data_table_id_=%lu",
                  build_option_.data_table_id_);
      }
      else if (NULL == (index_table_schema = schema_mgr->get_table_schema(
         build_option_.index_table_id_)))
      {
        TBSYS_LOG(WARN, "failed to get index table schema, index_table_id_=%lu",
                  build_option_.index_table_id_);
      }
      else 
      {
        const ObRowkeyInfo& data_rowkey_info = data_table_schema->get_rowkey_info();
        const ObRowkeyInfo& index_rowkey_info = index_table_schema->get_rowkey_info();

        if (data_rowkey_info.get_size() > 0 && index_rowkey_info.get_size() > 0
            && data_rowkey_info.get_column(0)->column_id_ 
            == index_rowkey_info.get_column(0)->column_id_)
        {
          ret = true;
        }
      }

      if (NULL != schema_mgr)
      {
        if(OB_SUCCESS != build_option_.merger_schema_mgr_->release_schema(schema_mgr))
        {
          TBSYS_LOG(WARN, "release schema mgr fail, ret=%d", ret);
        }
      }

      return ret;
    }

    int ObLocalIndexSampler::sample_all()
    {
      int ret = OB_SUCCESS;
      ObTablet* data_tablet = NULL;
      ObTablet* index_tablet = NULL;
      ObTabletHandle tablet_handle;

      if (build_option_.sample_count_ <= 0 && NULL == data_tablet_iter_)
      {
        TBSYS_LOG(WARN, "not inited, sample_count_=%ld, data_tablet_iter_=%p", 
                  build_option_.sample_count_, data_tablet_iter_);
        ret = OB_NOT_INIT;
      }

      while (OB_SUCCESS ==ret)
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
        else if (0 == data_tablet->get_row_count())
        {
          // no row in tablet, needn't sample this tablet
          TBSYS_LOG(INFO, "no row in tablet, needn't sample this tablet=%s",
                    to_cstring(data_tablet->get_range()));
          continue;
        }
        else if (NULL == (index_tablet = data_tablet->get_local_index())
                 && OB_SUCCESS != (ret = local_index_builder_.build(
                     build_option_, *data_tablet)))
        {
          TBSYS_LOG(WARN, "tablet local index isn't existent, but build this "
                          "tablet local index failed, data tablet=%s",
                    to_cstring(data_tablet->get_range()));
          break;
        }
        else if (NULL == index_tablet 
                 && NULL == (index_tablet = data_tablet->get_local_index()))
        {
          TBSYS_LOG(WARN, "after build local index tablet, local index tablet is "
                          "not existent, data_tablet=%s",
                    to_cstring(data_tablet->get_range()));
          break;
        }
        else if (OB_SUCCESS != (ret = sample(*data_tablet, *index_tablet)))
        {
          TBSYS_LOG(WARN, "failed to sample one tablet local index, "
                          "data_tablet=%s",
                    to_cstring(data_tablet->get_range()));
          break;
        }
        tablet_handle.reset();
      }

      return ret;
    }

    int ObLocalIndexSampler::sample(
       const ObTablet& data_tablet, const ObTablet& index_tablet)
    {
      ObPhyOperator* sampler = NULL;

      return sample(data_tablet.get_range(), index_tablet.get_range(), 
                    index_tablet.get_row_count(), false, sampler);
    }

    int ObLocalIndexSampler::sample(
       const ObNewRange& data_tablet_range, 
       const ObNewRange& index_tablet_range, 
       const int64_t row_count,
       const bool sample_media_result,
       ObPhyOperator*& sampler)
    {
      int ret = OB_SUCCESS;
      int64_t row_interval = 0;
      const ObSchemaManagerV2 *schema_mgr = NULL;
      ObTabletLocalIndexSampler* tablet_local_index_sampler = NULL;

      if (build_option_.sample_count_ <= 0)
      {
        TBSYS_LOG(WARN, "not inited, sample_count_=%ld", 
                  build_option_.sample_count_);
        ret = OB_NOT_INIT;
      }
      else if (row_count < 0)
      {
        TBSYS_LOG(WARN, "no row in local index tablet, row_count=%ld", 
                  row_count);
        ret = OB_ERROR;
      }
      else
      {
        row_interval = row_count / (build_option_.sample_count_ + 1) + 1;

        if (NULL == (tablet_local_index_sampler = GET_TSI_ARGS(
           ObTabletLocalIndexSampler, TSI_CS_TABLE_LOCAL_INDEX_SAMPLER_1, 
           tablet_manager_, tablet_image_, media_sample_memtable_)))
        {
          TBSYS_LOG(ERROR, "failed allocate memory for tablet local index "
                           "sampler instance");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if(NULL == (schema_mgr = build_option_.merger_schema_mgr_->get_user_schema(0)))
        {
          TBSYS_LOG(WARN, "get user schema mgr fail, schema_mgr=%p, ret=%d", 
                    schema_mgr, ret);
          ret = OB_ERROR;
        }
        else if (!sample_media_result
                 && OB_SUCCESS != (ret = tablet_local_index_sampler->sample_tablet(
                    *schema_mgr, data_tablet_range, index_tablet_range, row_interval)))
        {
          TBSYS_LOG(WARN, "failed to build tablet local index, data_tablet_range=%s, "
                          "row_interval=%ld, ret=%d", 
                    to_cstring(data_tablet_range), row_interval, ret);
        }
        else if (sample_media_result 
                 && OB_SUCCESS != (ret = tablet_local_index_sampler->sample_memtable(
                    media_sample_memtable_, *schema_mgr, index_tablet_range, 
                    row_interval, build_option_.sort_file_setter_, sampler)))
        {
          TBSYS_LOG(WARN, "failed to sample tablet local index media memtable, "
                          "data_tablet_range=%s, row_interval=%ld, ret=%d", 
                    to_cstring(data_tablet_range), row_interval, ret);
        }
      }

      if (NULL != schema_mgr)
      {
        if(OB_SUCCESS != build_option_.merger_schema_mgr_->release_schema(schema_mgr))
        {
          TBSYS_LOG(WARN, "release schema mgr fail, ret=%d", ret);
        }
      }

      return ret;
    }

    ObPhyOperator* ObLocalIndexSampler::get_phyoperator()
    {
      int err = OB_SUCCESS;
      ObPhyOperator* sampler = NULL;
      int64_t row_count = media_sample_memtable_.get_row_count();
      ObNewRange tablet_range;
      tablet_range.table_id_ = build_option_.index_table_id_;
      tablet_range.set_whole_range();

      if (OB_SUCCESS != (err = sample(
         tablet_range, tablet_range, row_count, true, sampler)))
      {
        TBSYS_LOG(WARN, "failed to initialize local index sampler operator, "
                        "index_table_id_=%lu, err=%d",
                  build_option_.index_table_id_, err);
      }

      return sampler;
    }
  } /* chunkserver */
} /* oceanbase */
