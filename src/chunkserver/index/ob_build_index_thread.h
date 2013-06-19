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
#ifndef OCEANBASE_CHUNKSERVER_BUILD_INDEX_THERAD_H_
#define OCEANBASE_CHUNKSERVER_BUILD_INDEX_THERAD_H_

#include <tbsys.h>
#include "common/ob_new_scanner.h"
#include "ob_tablet_predicator.h"
#include "ob_local_index_builder.h"
#include "ob_local_index_sampler.h"
#include "ob_global_index_builder.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObTabletManager;

    /**
     * build sample and index with multi-thread, the process
     * includes two steps:
     *   1. sample data table to get index table samples
     *   2. build global index
     */
    class ObBuildIndexThread : public tbsys::CDefaultRunnable
    {
      public: 
        ObBuildIndexThread(ObTabletManager& tablet_manager);
        ~ObBuildIndexThread();

        //start build index thread
        int init();

        //stop all build index thread
        void destroy();

        int start_build_sample(const uint64_t data_table_id, 
                               const uint64_t index_table_id,
                               const int64_t sample_count);

        int start_build_global_index(const uint64_t data_table_id, 
                                     const uint64_t index_table_id);

        // Build data tablet's local index.
        // May return OB_NEED_RETRY if another thread is building %thread's local index.
        int tablet_build_local_index(ObTablet *tablet,
                                     const uint64_t index_table_id);
        
        virtual void run(tbsys::CThread* thread, void* arg);

        bool is_finish_build() const { return state_.is_finish_build_; }

        void stop_all();

      private:
        const static int64_t MAX_BUILDER_THREAD = 16;

        enum ObBuildType
        {
          BUILD_NULL = 0,
          BUILD_LOCAL_INDEX,
          BUILD_SAMPLE,
          BUILD_GLOBAL_INDEX,
        };

        struct ObBuildState
        {
          ObBuildState()
          {
            reset();
          }

          void reset()
          {
            build_type_ = BUILD_NULL;
            is_finish_build_ = true;
            is_build_succ_ = true;
            finish_build_thread_cnt_ = 0;
          }

          ObBuildType build_type_;
          bool is_finish_build_;
          bool is_build_succ_;
          volatile int64_t finish_build_thread_cnt_;
        };

        void init_build_option(ObIndexBuildOptions &build_option,
                               const uint64_t data_table_id, 
                               const uint64_t index_table_id,
                               const int64_t sample_count = 0) const;
        int build_index(const int64_t thread_index);

        //build local index functions
        int start_build_local_index();
        int do_build_local_index(const int64_t thread_index);
        int finish_build_local_index(const int64_t thread_index);
        void stop_build_local_index();

        //build sample functions
        int init_build_sample();
        int do_build_sample(const int64_t thread_index);
        int finish_build_sample();
        void stop_build_sample();
        int report_samples();
        int fill_sample_result_scanner();

        //build global index functions
        int do_build_global_index(const int64_t thread_index);
        int finish_build_global_index(const int64_t thread_index);
        int build_global_index_with_migrate(const int64_t thread_index);
        void stop_build_global_index();
        int report_build_status();
        int build_finally();

      private:
        DISALLOW_COPY_AND_ASSIGN(ObBuildIndexThread);

        //state variables
        bool inited_;
        ObBuildState state_;
        bool stop_build_;

        //build options
        ObIndexBuildOptions build_option_;

        //thread concurrence control mutex
        mutable tbsys::CThreadMutex mutex_;
        tbsys::CThreadCond cond_;

        //core builder and sampler
        ObLocalIndexBuilder local_index_builder_;
        ObLocalIndexSampler local_index_sampler_;
        ObGlobalIndexBuilder global_index_builder_;

        //temp scanner to store sample result
        const common::ObRow* cur_row_;
        common::ObNewScanner sample_result_scanner_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_BUILD_INDEX_THERAD_H_
