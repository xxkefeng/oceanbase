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
#ifndef OCEANBASE_CHUNKSERVER_LOCAL_INDEX_SAMPLER_H_
#define OCEANBASE_CHUNKSERVER_LOCAL_INDEX_SAMPLER_H_

#include "ob_local_index_builder.h"
#include "ob_tablet_memtable.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;

    class ObLocalIndexSampler : public ObIndexBuilder
    {
      public: 
        ObLocalIndexSampler(ObTabletManager& tablet_manager,
                            ObIndexBuildOptions& build_option,
                            ObLocalIndexBuilder& local_index_builder);
        virtual ~ObLocalIndexSampler();

        //not thread safe
        int init();

        //must be called after init(), thread safe
        int sample_all();

        //must be called after sample
        sql::ObPhyOperator* get_phyoperator();

      private:
        int64_t get_sample_tablet_count();
        bool is_rowkey_first_column_same();
        int init_sampler_phyoperator(sql::ObPhyOperator*& sampler);
        int sample(const ObTablet& data_tablet, const ObTablet& index_tablet);
        int sample(const common::ObNewRange& data_tablet_range, 
                   const common::ObNewRange& index_tablet_range,
                   const int64_t row_count, 
                   const bool sample_media_result,
                   sql::ObPhyOperator*& sampler);

      private:
        const static int64_t DEFAULT_SAMPLEE_TABLET_COUNT = 10;

        DISALLOW_COPY_AND_ASSIGN(ObLocalIndexSampler);

        ObLocalIndexBuilder& local_index_builder_;
        ObLocalIndexTabletImage tablet_image_;
        ObTabletIterator* data_tablet_iter_;

        ObTabletMemtable media_sample_memtable_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_LOCAL_INDEX_SAMPLER_H_
