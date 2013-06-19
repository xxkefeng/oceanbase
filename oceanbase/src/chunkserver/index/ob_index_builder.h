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
#ifndef OCEANBASE_CHUNKSERVER_INDEX_BUILDER_H_
#define OCEANBASE_CHUNKSERVER_INDEX_BUILDER_H_

#include "ob_tablet_predicator.h"
#include "ob_tablet_distribution.h"

namespace oceanbase
{
  namespace common
  {
   class ObMergerSchemaManager;
  }

  namespace sql
  {
    class ObSort;
  }

  namespace chunkserver
  {
    class ObTabletManager;
    class ObDiskManager;

    class ObSortFileSetter
    {
    public:
      ObSortFileSetter();

      void init(const ObDiskManager &disk_manager, int64_t mem_per_thread);
      int setup_sorter(sql::ObSort &sorter, const char *file_prefix);

    private:
      DISALLOW_COPY_AND_ASSIGN(ObSortFileSetter);

    private:
      bool init_;
      const int32_t *disk_no_array_;
      int32_t disk_num_;
      uint64_t array_index_;
      int64_t memory_limit_;
    };

    struct ObIndexBuildOptions
    {
      ObIndexBuildOptions()
      : data_table_id_(common::OB_INVALID_ID),
        index_table_id_(common::OB_INVALID_ID),
        sample_count_(0),
        merger_schema_mgr_(NULL)
      {
      }

      bool is_valid() const;

      int64_t to_string(char* buffer, const int64_t size) const;

      uint64_t data_table_id_;
      uint64_t index_table_id_;
      int64_t sample_count_;
      common::ObMergerSchemaManager* merger_schema_mgr_;
      ObRpcOptions rpc_option_;
      common::ObServer self_server_;
      ObSortFileSetter sort_file_setter_;
      int64_t write_sstable_version_; 
    };

    class ObIndexBuilder
    {
      public: 
        ObIndexBuilder(ObTabletManager& tablet_manager,
                       ObIndexBuildOptions& build_option);
        virtual ~ObIndexBuilder();

        void stop()
        {
          stop_ = true;
        }

      protected:
        inline ObTabletIterator& get_selected_tablet_iterator(
           ObTabletDistribution& tablet_dist, 
           int status = common::TABLET_UNAVAILABLE,
           const int64_t limit_count = INT64_MAX)
        {
          selected_tablet_predicator_.reset(build_option_.self_server_, status);
          tablet_iter_.reset_iterator(tablet_dist, selected_tablet_predicator_, 
                                      limit_count);
          return tablet_iter_;
        }

      protected:
        bool stop_;
        ObTabletManager& tablet_manager_;
        ObIndexBuildOptions& build_option_;
        ObSelectedTabletPredicator selected_tablet_predicator_;
        ObTabletIterator tablet_iter_;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObIndexBuilder);
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_INDEX_BUILDER_H_
