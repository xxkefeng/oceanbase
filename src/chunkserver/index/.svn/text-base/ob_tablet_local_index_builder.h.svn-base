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
#ifndef OCEANBASE_CHUNKSERVER_TABLET_LOCAL_INDEX_BUILDER_H_
#define OCEANBASE_CHUNKSERVER_TABLET_LOCAL_INDEX_BUILDER_H_

#include "ob_tablet_local_index_scan.h"
#include "ob_tablet_sstable.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace sstable;
    class ObTabletManager;
    class ObBasicTabletImage;
    class ObTabletMemtable;
    class ObSortFileSetter;

    class ObTabletLocalIndexBuilder
    {
      public: 
        ObTabletLocalIndexBuilder(ObTabletManager& tablet_manager, 
                                  ObLocalIndexTabletImage& tablet_image);
        ~ObTabletLocalIndexBuilder();

        //myabe more than one thread to build local index of the same tablet,
        //only the first thread can build it, the other thread will fail, this 
        //function will return OB_NEED_RETRY, user need handle it
        int build(const common::ObSchemaManagerV2& schema, ObTablet& data_tablet, 
                  const uint64_t index_table_id, ObSortFileSetter &sort_file_setter,
                  const int64_t write_sstable_version = SSTableReader::COMPACT_SSTABLE_VERSION);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletLocalIndexBuilder);

        ObTabletManager& tablet_manager_;
        ObBasicTabletImage& tablet_image_;

        ObTabletLocalIndexScanner extracter_;
        ObTabletSSTable input_sstable_;
        ObTabletSSTable output_sstable_;
    };

    class ObTabletLocalIndexSampler
    {
      public: 
        ObTabletLocalIndexSampler(ObTabletManager& tablet_manager, 
                                  ObLocalIndexTabletImage& tablet_image,
                                  ObTabletMemtable& media_output_memtable);
        ~ObTabletLocalIndexSampler();

        int sample_tablet(const common::ObSchemaManagerV2& schema, 
                          const common::ObNewRange& data_tablet_range,
                          const common::ObNewRange& index_tablet_range,
                          const int64_t row_interval);

        int sample_memtable(ObTabletMemtable& input_memtable, 
                            const common::ObSchemaManagerV2& schema, 
                            const common::ObNewRange& tablet_range,
                            const int64_t row_interval,
                            ObSortFileSetter &sort_file_setter,
                            sql::ObPhyOperator*& phy_operator);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTabletLocalIndexSampler);

        ObTabletManager& tablet_manager_;
        ObBasicTabletImage& tablet_image_;

        ObTabletMemtable& media_output_memtable_;
        ObTabletSSTable input_sstable_;
        ObTabletLocalIndexScanner interval_sampler_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_TABLET_LOCAL_INDEX_BUILDER_H_
