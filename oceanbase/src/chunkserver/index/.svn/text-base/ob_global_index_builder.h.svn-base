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
#ifndef OCEANBASE_CHUNKSERVER_GLOBAL_INDEX_BUILDER_H_
#define OCEANBASE_CHUNKSERVER_GLOBAL_INDEX_BUILDER_H_

#include "ob_local_index_builder.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObGlobalIndexBuilder : public ObIndexBuilder
    {
      public: 
        ObGlobalIndexBuilder(ObTabletManager& tablet_manager, 
                             ObIndexBuildOptions& build_option);
        virtual ~ObGlobalIndexBuilder();

        //not thread safe
        int init(ObTabletDistribution& data_table_tablet_dist);

        //thread safe, must be called after init()
        int batch_build();

        inline ObTabletDistribution& get_index_table_tablet_dist()
        {
          return index_table_tablet_dist_;
        }

        //must be called after batch_build()
        inline bool has_unavaliable_tablet() const
        {
          return index_table_tablet_dist_.has_unavaliable_tablet(
             build_option_.self_server_);
        }

      private:
        inline ObTabletIterator& get_tablet_iterator(const ObPredicator& predicator)
        {
          tablet_iter_.reset_iterator(index_table_tablet_dist_, predicator);
          return tablet_iter_;
        }

        int build_tablet(const ObTabletDistItem& index_tablet_item);

      private:
        const static int32_t WAIT_TABLET_AVALIABLE_TIME = 30000000; //30s

        DISALLOW_COPY_AND_ASSIGN(ObGlobalIndexBuilder);

        ObGlobalIndexTabletImage tablet_image_;
        ObTabletDistribution* data_table_tablet_dist_;
        ObTabletDistribution index_table_tablet_dist_;
        ObSelectedAndNoAvaliablePredicator selected_and_no_avaliable_pred_;
        ObTabletAvaliablePredicator tablet_avaliable_pred_;
        ObTabletIterator* index_tablet_iter_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_GLOBAL_INDEX_BUILDER_H_
