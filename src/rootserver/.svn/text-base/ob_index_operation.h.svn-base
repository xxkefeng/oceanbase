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
#ifndef OCEANBASE_ROOTSERVER_INDEX_OPERATION_H_
#define OCEANBASE_ROOTSERVER_INDEX_OPERATION_H_

#include "common/ob_bypass_task_info.h"
#include "common/ob_new_scanner.h"
#include "common/ob_row_store.h"
#include "common/ob_tablet_info.h"
#include "common/page_arena.h"
#include "sql/ob_no_children_phy_operator.h"
#include "sql/ob_sort.h"
#include "sql/ob_interval_sample.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObSampleMemtable: public sql::ObNoChildrenPhyOperator
    {
      public:
        ObSampleMemtable();
        virtual ~ObSampleMemtable();

        int set_row_desc(const common::ObRowDesc& row_desc);
        const common::ObRowStore &get_row_store() { return row_store_; }
        int append_row(const common::ObRow& row);
        inline int64_t get_row_count() const { return row_count_; }

        void reset();
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow*& row);
        virtual int get_row_desc(const common::ObRowDesc*& row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObSampleMemtable);

        tbsys::CThreadMutex mutex_;
        int64_t row_count_;
        common::ObRowDesc cur_row_desc_;
        common::ObRow cur_row_;
        common::ObRowStore row_store_;
    };

    class ObIndexSampler
    {
      public:
        ObIndexSampler();
        ~ObIndexSampler();

        void reset();

        int init(const common::ObSchemaManagerV2* schema_mgr, 
                 const common::ObBuildIndexInfo& build_index_info);

        inline int append_row(const common::ObRow& row)
        {
          return sample_memtable_.append_row(row);
        }

        inline int64_t get_row_count() const 
        { 
          return sample_memtable_.get_row_count(); 
        }

        int open(const common::ObSchemaManagerV2* schema_mgr, 
                 const common::ObBuildIndexInfo build_index_info);

        //must be called after open()
        inline sql::ObPhyOperator* get_phyoperator() 
        { 
          return root_; 
        }

        inline const common::ObRowDesc& get_cur_row_desc() const { return cur_row_desc_; }

      private:
        int init_row_desc(const common::ObSchemaManagerV2* schema_mgr, 
                          const common::ObBuildIndexInfo& build_index_info);
        int init_sample_memtable();
        int init_sorter(const common::ObSchemaManagerV2* schema_mgr, 
                        const common::ObBuildIndexInfo& build_index_info);
        int init_sampler(const common::ObBuildIndexInfo& build_index_info);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObIndexSampler);

        common::ObRowDesc cur_row_desc_;
        ObSampleMemtable sample_memtable_;
        sql::ObSort sorter_;
        sql::ObIntervalSample sampler_;
        sql::ObPhyOperator* root_;
    };

    class ObIndexOperation
    {
      public:
        ObIndexOperation();
        ~ObIndexOperation();

        void reset();

        int init_sample_table(const common::ObSchemaManagerV2* schema_mgr,
                              const common::ObBuildIndexInfo& build_index_info);

        int report_samples(const int32_t server_index, 
                           const common::ObNewScanner& sample_scanner);

        int build_sample_result(const common::ObSchemaManagerV2* schema_mgr);

        int get_next_sample_result(common::ObTabletInfoList& tablet_list);

        inline const common::ObBuildIndexInfo& get_build_index_info() const
        {
          return build_index_info_;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObIndexOperation);

        common::ObBuildIndexInfo build_index_info_;
        common::ObArenaAllocator allocator_;
        ObIndexSampler index_sampler_;
        common::ObRowkey cur_rowkey_;
        const common::ObRow* cur_row_;
    };
  } /* rootserver */
} /* oceanbase */

#endif // OCEANBASE_ROOTSERVER_INDEX_OPERATION_H_
