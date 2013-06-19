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
#ifndef OCEANBASE_CHUNKSERVER_LOCAL_INDEX_BUILDER_H_
#define OCEANBASE_CHUNKSERVER_LOCAL_INDEX_BUILDER_H_

#include "ob_index_tablet_image.h"
#include "ob_tablet_local_index_builder.h"
#include "ob_index_builder.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;
    class ObIndexBuildOptions;

    class ObLocalIndexBuilder : public ObIndexBuilder
    {
      public: 
        ObLocalIndexBuilder(ObTabletManager& tablet_manager, 
                            ObIndexBuildOptions& build_option);
        virtual ~ObLocalIndexBuilder(); 

        //not thread safe
        int init();

        //must be called after init(), thread safe
        int build_all();

        //thread safe
        int build(ObIndexBuildOptions &option, ObTablet& data_tablet);

        inline ObTabletDistribution& get_data_table_tablet_dist()
        {
          return data_table_tablet_dist_;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObLocalIndexBuilder);

        ObLocalIndexTabletImage tablet_image_;
        ObTabletDistribution data_table_tablet_dist_;
        ObTabletIterator* data_tablet_iter_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_LOCAL_INDEX_BUILDER_H_
