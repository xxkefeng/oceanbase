/**
 * (C) 2010-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_bypass_sstable_loader.cpp for bypass sstable loader.
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/ob_schema_manager.h"
#include "chunkserver/ob_chunk_server_main.h"
#include "chunkserver/ob_tablet_manager.h"
#include "ob_build_index_thread.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace tbsys;
    using namespace oceanbase;
    using namespace common;
    using namespace sstable;

    ObBuildIndexThread::ObBuildIndexThread(ObTabletManager& tablet_manager)
    : inited_(false), stop_build_(false),
      local_index_builder_(tablet_manager, build_option_),
      local_index_sampler_(tablet_manager, build_option_, local_index_builder_),
      global_index_builder_(tablet_manager, build_option_),
      cur_row_(NULL)
    {

    }

    ObBuildIndexThread::~ObBuildIndexThread()
    {
      destroy();
    }

    int ObBuildIndexThread::init()
    {
      int ret = OB_SUCCESS;

      if (!inited_)
      {
        int64_t thread_num = THE_CHUNK_SERVER.get_config().index_builder_thread_num;
        if (thread_num > MAX_BUILDER_THREAD)
        {
          thread_num = MAX_BUILDER_THREAD;
        }
        setThreadCount(static_cast<int32_t>(thread_num));
        start();
        inited_  = true;
        stop_build_ = false;
      }

      return ret;
    }

    void ObBuildIndexThread::destroy()
    {
      if (inited_ && _threadCount > 0)
      {
        inited_ = false;

        //stop the thread
        stop();
        stop_all();

        //signal
        cond_.broadcast();

        //join
        wait();
      }
    }

    void ObBuildIndexThread::stop_all()
    {
      state_.is_build_succ_ = false;
      stop_build_ = true;
      local_index_builder_.stop();
      local_index_sampler_.stop();
      global_index_builder_.stop();
    }

    void ObBuildIndexThread::run(CThread* thread, void* arg)
    {
      int64_t thread_index = reinterpret_cast<int64_t>(arg);
      static __thread bool thread_finish_build = true;
      UNUSED(thread);

      TBSYS_LOG(INFO, "index builder thread start run, thread_index=%ld",
        thread_index);
      while(!_stop)
      {
        cond_.lock();
        while (!_stop && thread_finish_build)
        {
          cond_.wait();
          thread_finish_build = false;
        }

        if (_stop)
        {
          cond_.broadcast();
          cond_.unlock();
          break;
        }
        cond_.unlock();

        build_index(thread_index);
        thread_finish_build = true;
      }
    }

    int ObBuildIndexThread::build_index(const int64_t thread_index)
    {
      int ret = OB_SUCCESS;

      switch (state_.build_type_)
      {
         case BUILD_LOCAL_INDEX:
           ret = do_build_local_index(thread_index);
           break ;
         case BUILD_SAMPLE:
           ret = do_build_sample(thread_index);
           break;
         case BUILD_GLOBAL_INDEX:
           ret = do_build_global_index(thread_index);
           break;
         case BUILD_NULL:
           TBSYS_LOG(WARN, "NULL build type=%d, do nothing", state_.build_type_);
           ret = OB_ERROR;
           break;
         default:
           TBSYS_LOG(WARN, "unsupport build type=%d", state_.build_type_);
           ret = OB_ERROR;
           break;
      }

      return ret;
    }

    int ObBuildIndexThread::start_build_local_index()
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "start build local index, data_table_id=%lu, index_table_id=%lu",
                build_option_.data_table_id_, build_option_.index_table_id_);

      if (BUILD_LOCAL_INDEX == state_.build_type_)
      {
        //get data table tablet distribution from rootserver
        if (OB_SUCCESS != (ret = local_index_builder_.init()))
        {
          TBSYS_LOG(WARN, "failed to init local index builder, data_table_id=%lu,"
                          "index_table_id=%lu, ret=%d", 
                    build_option_.data_table_id_, build_option_.index_table_id_, ret);
        }
      }
      else
      {
        TBSYS_LOG(WARN, "invalid build type=%d, cann't start build local index", 
                  state_.build_type_);
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObBuildIndexThread::do_build_local_index(const int64_t thread_index)
    {
      int ret = OB_SUCCESS;
       
      if (BUILD_LOCAL_INDEX != state_.build_type_)
      {
        TBSYS_LOG(WARN, "invalid build type=%d, cann't do build local index", 
                  state_.build_type_);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS !=(ret = local_index_builder_.build_all()))
      {
        TBSYS_LOG(WARN, "thread[%ld] build local index failed, ret %d", thread_index, ret);
      }

      if (OB_SUCCESS != ret)
      {
        stop_build_local_index();
      }

      if (_threadCount == __sync_add_and_fetch(&state_.finish_build_thread_cnt_, 1))
      {
        if (OB_SUCCESS != (ret = finish_build_local_index(thread_index)))
        {
          TBSYS_LOG(WARN, "failed to finish build local index, is_build_succ_=%d", 
                    state_.is_build_succ_);
        }
      }

      return ret;
    }

    void ObBuildIndexThread::stop_build_local_index()
    {
      /**
       * state_ members will be accessed by multi-thread, but all the
       * threads only read it except that multi-thread will set it to
       * false, but not set it to true in multi-thread case. so here
       * we not use lock to protect it.
       */
      state_.is_build_succ_ = false;
      local_index_builder_.stop();
    }

    int ObBuildIndexThread::finish_build_local_index(const int64_t thread_index)
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "finish build local index, data_table_id=%lu, index_table_id=%lu, "
                      "is_build_succ_=%d",
                build_option_.data_table_id_, build_option_.index_table_id_, 
                state_.is_build_succ_);

      if (BUILD_LOCAL_INDEX != state_.build_type_)
      {
        TBSYS_LOG(WARN, "invalid build type=%d, cann't finish build local index", 
                  state_.build_type_);
        ret = OB_ERROR;
      }
      else if (state_.is_build_succ_)
      {
        state_.build_type_ = BUILD_SAMPLE;
        state_.finish_build_thread_cnt_ = 0;
        if (OB_SUCCESS != (ret = init_build_sample()))
        {
          TBSYS_LOG(WARN, "failed to init build sample, ret=%d", ret);
        }
        else 
        {
          cond_.broadcast();
          ret = do_build_sample(thread_index);
        }
      }
      else 
      {
        state_.build_type_ = BUILD_SAMPLE;
        ret = finish_build_sample();
      }

      return ret;
    }

    int ObBuildIndexThread::tablet_build_local_index(ObTablet *tablet, 
        const uint64_t index_table_id)
    {
      int rc = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(WARN, "build index thread isn't initialized");
        rc = OB_NOT_INIT;
      }
      else if (NULL == tablet || OB_INVALID_ID == index_table_id)
      {
        TBSYS_LOG(WARN, "invalid argument tablet %p, index_table_id %ld",
            tablet, index_table_id);
        rc = OB_INVALID_ARGUMENT;
      }
      else if (NULL != tablet->get_local_index()
          && index_table_id == tablet->get_local_index()->get_range().table_id_)
      {
        TBSYS_LOG(INFO, "local index already exist, index tablet [%s]",
            to_cstring(*tablet->get_local_index()));
      }
      else
      {
        ObIndexBuildOptions option;
        init_build_option(option, tablet->get_range().table_id_, index_table_id);

        rc = local_index_builder_.build(option, *tablet);
        if (OB_SUCCESS != rc && OB_NEED_RETRY != rc)
        {
          TBSYS_LOG(WARN, "tablet [%s] build local index (index_table_id %ld) failed, rc %d",
            to_cstring(*tablet), index_table_id, rc);
        }
      }

      return rc;
    }

    int ObBuildIndexThread::start_build_sample(
       const uint64_t data_table_id, 
       const uint64_t index_table_id,
       const int64_t sample_count)
    {
      int ret = OB_SUCCESS;

      /**
       * sample operation includes two steps: 
       *   1. build tablet local index for tablets whose selected
       *   chunkserver is this current chunkserver
       *   2. after build tablet local index, get sample points from
       *   tablet local index sstable.
       */

      TBSYS_LOG(INFO, "start sample, data_table_id=%lu, index_table_id=%lu, "
                      "sample_count_=%ld",
                data_table_id, index_table_id, sample_count);

      if (!inited_)
      {
        TBSYS_LOG(WARN, "build index thread isn't initialized");
        ret = OB_NOT_INIT;
      }
      else if (OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_table_id
               || sample_count <= 0)
      {
        TBSYS_LOG(WARN, "invalid table id, data_table_id=%lu, index_table_id=%lu, "
                        "sample_count=%ld",
                  data_table_id, index_table_id, sample_count);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != state_.finish_build_thread_cnt_)
      {
        TBSYS_LOG(WARN, "build index thread is busy, cann't do new build work, "
                        "cur_build_type=%d, finish_build_thread_cnt=%ld", 
                  state_.build_type_, state_.finish_build_thread_cnt_);
        ret = OB_ERROR;
      }
      else
      {
        state_.reset();
        state_.build_type_ = BUILD_LOCAL_INDEX;
        state_.is_finish_build_ = false;
        stop_build_ = false;
        cur_row_ = NULL;
        init_build_option(build_option_, data_table_id, index_table_id, sample_count);
        THE_CHUNK_SERVER.get_tablet_manager().get_disk_manager().scan(
           THE_CHUNK_SERVER.get_config().datadir, OB_DEFAULT_MAX_TABLET_SIZE);

        if (OB_SUCCESS != (ret = start_build_local_index()))
        {
          TBSYS_LOG(WARN, "failed to start build local index, data_table_id=%lu, "
                          "index_table_id=%lu, ret=%d",
                    data_table_id, index_table_id, ret);
          state_.is_build_succ_ = false;
          state_.build_type_ = BUILD_SAMPLE;
          if (OB_SUCCESS != (ret = finish_build_sample()))
          {
            TBSYS_LOG(WARN, "failed to run finish_build_sample, ret=%d", ret);
          }
        }
        else 
        {
          cond_.broadcast();
        }
      }

      return ret;
    }

    void ObBuildIndexThread::init_build_option(
       ObIndexBuildOptions &build_option,
       const uint64_t data_table_id, const uint64_t index_table_id, 
       const int64_t sample_count/* = 0*/) const
    {
      build_option.data_table_id_ = data_table_id;
      build_option.index_table_id_ = index_table_id;
      build_option.sample_count_ = sample_count;
      build_option.merger_schema_mgr_ = THE_CHUNK_SERVER.get_schema_manager();
      build_option.rpc_option_.rpc_stub_ = &THE_CHUNK_SERVER.get_rpc_stub();
      build_option.rpc_option_.sql_rpc_stub_ = &THE_CHUNK_SERVER.get_sql_rpc_stub();
      build_option.rpc_option_.server_ = THE_CHUNK_SERVER.get_root_server(); //default value
      build_option.rpc_option_.retry_times_ = THE_CHUNK_SERVER.get_config().retry_times;
      build_option.rpc_option_.timeout_ = THE_CHUNK_SERVER.get_config().network_timeout;
      build_option.rpc_option_.scan_index_timeout_ =
        THE_CHUNK_SERVER.get_config().scan_index_timeout;
      build_option.write_sstable_version_ = THE_CHUNK_SERVER.get_config().merge_write_sstable_version;
      build_option.self_server_ = THE_CHUNK_SERVER.get_self();

      int64_t thread_num = THE_CHUNK_SERVER.get_config().index_builder_thread_num;
      OB_ASSERT(thread_num > 0);
      build_option.sort_file_setter_.init(
          THE_CHUNK_SERVER.get_tablet_manager().get_disk_manager(), 
          THE_CHUNK_SERVER.get_config().index_sort_mem_limit / thread_num);
    }

    int ObBuildIndexThread::init_build_sample()
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "init build sample, data_table_id=%lu, index_table_id=%lu",
                build_option_.data_table_id_, build_option_.index_table_id_);

      if (BUILD_SAMPLE == state_.build_type_)
      {
        if (OB_SUCCESS != (ret = local_index_sampler_.init()))
        {
          TBSYS_LOG(WARN, "failed to init local index sampler, data_table_id=%lu,"
                          "index_table_id=%lu, ret=%d", 
                    build_option_.data_table_id_, build_option_.index_table_id_, ret);
        }
      }
      else
      {
        TBSYS_LOG(WARN, "invalid build type=%d, cann't init build sample", 
                  state_.build_type_);
        ret = OB_ERROR;
      }

      return ret;
    }

    int ObBuildIndexThread::do_build_sample(const int64_t thread_index)
    {
      int ret = OB_SUCCESS;

      if (BUILD_SAMPLE != state_.build_type_)
      {
        TBSYS_LOG(WARN, "invalid build type=%d, cann't do build sample", 
                  state_.build_type_);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS !=(ret = local_index_sampler_.sample_all())
               && OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "thread[%ld] build sample failed, ret %d", thread_index, ret);
      }

      if (OB_SUCCESS != ret)
      {
        stop_build_sample();
      }

      if (_threadCount == __sync_add_and_fetch(&state_.finish_build_thread_cnt_, 1))
      {
        if (OB_SUCCESS != (ret = finish_build_sample()))
        {
          TBSYS_LOG(WARN, "failed to finish build sample, is_build_succ_=%d", 
                    state_.is_build_succ_);
        }
      }

      return ret;
    }

    void ObBuildIndexThread::stop_build_sample()
    {
      /**
       * state_ members will be accessed by multi-thread, but all the
       * threads only read it except that multi-thread will set it to
       * false, but not set it to true in multi-thread case. so here
       * we not use lock to protect it.
       */
      state_.is_build_succ_ = false;
      local_index_sampler_.stop();
    }

    int ObBuildIndexThread::finish_build_sample()
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "finish build sample, data_table_id=%lu, index_table_id=%lu, "
                      "is_build_succ_=%d",
                build_option_.data_table_id_, build_option_.index_table_id_, 
                state_.is_build_succ_);

      if (BUILD_SAMPLE != state_.build_type_)
      {
        TBSYS_LOG(WARN, "invalid build type=%d, cann't finish build sample", 
                  state_.build_type_);
        state_.is_build_succ_ = false;
        ret = OB_ERROR;
      }
      else if (!stop_build_ && OB_SUCCESS != (ret = report_samples()))
      {
        TBSYS_LOG(WARN, "failed to report samples to rootserver, ret=%d", ret);
      }

      state_.finish_build_thread_cnt_ = 0;
      state_.is_finish_build_ = true;

      return ret;
    }

    int ObBuildIndexThread::report_samples()
    {
      int ret = OB_SUCCESS;
      int continue_report = true;
      int64_t retry_times = THE_CHUNK_SERVER.get_config().retry_times;

      while (continue_report)
      {
        ret = fill_sample_result_scanner();
        if (OB_SUCCESS == ret || OB_ITER_END == ret || OB_SIZE_OVERFLOW == ret)
        {
          if (OB_SIZE_OVERFLOW == ret)
          {
            continue_report = true;
          }
          else 
          {
            continue_report = false;
          }
          RPC_RETRY_WAIT(true, retry_times, ret,
            CS_RPC_CALL_RS(report_samples, state_.is_build_succ_, 
              THE_CHUNK_SERVER.get_self(), build_option_.index_table_id_, 
              sample_result_scanner_, continue_report));
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "failed to report samples to rootserver, ret=%d", ret);
            //ignore the return status, ensure close the phyoperator
          }
        }
        else 
        {
          TBSYS_LOG(WARN, "fill_sample_result_scanner failed, ret=%d", ret);
          break;
        }
      }

      return ret;
    }

    int ObBuildIndexThread::fill_sample_result_scanner()
    {
      int ret = OB_SUCCESS;
      int64_t row_num = 0;
      ObPhyOperator* sampler = NULL;
      ObNewRange tablet_range;

      if (state_.is_build_succ_)
      {
        tablet_range.table_id_ = build_option_.index_table_id_;
        tablet_range.set_whole_range();
        sample_result_scanner_.reuse();
        sample_result_scanner_.set_range(tablet_range);
        sample_result_scanner_.set_data_version(
           THE_CHUNK_SERVER.get_tablet_manager().get_last_not_merged_version());

        if (NULL == (sampler = local_index_sampler_.get_phyoperator()))
        {
          TBSYS_LOG(WARN, "failed to get phyoperator form local index sampler");
          ret = OB_ERROR;
        }
        else
        {
          while (OB_SUCCESS == ret)
          {
            if(NULL == cur_row_)
            {
              ret = sampler->get_next_row(cur_row_);
              if(OB_ITER_END == ret)
              {
                if(OB_SUCCESS != (ret = sample_result_scanner_.set_is_req_fullfilled(
                   true, row_num)))
                {
                  TBSYS_LOG(WARN, "sample result scanner set is req fullfilled fail, "
                                  "ret=%d", ret);
                }
                break;
              }
              else if(OB_SUCCESS != ret)
              {
                TBSYS_LOG(WARN, "get next row fail, ret=%d", ret);
              }
            }

            if(OB_SUCCESS == ret)
            {
              ret = sample_result_scanner_.add_row(*cur_row_);
              if(OB_SIZE_OVERFLOW == ret)
              {
                if(OB_SUCCESS != (ret = sample_result_scanner_.set_is_req_fullfilled(
                   false, row_num)))
                {
                  TBSYS_LOG(WARN, "sample result  scanner set is req fullfilled fail, "
                                  "ret=%d", ret);
                }
                break;
              }
              else if(OB_SUCCESS == ret)
              {
                cur_row_ = NULL;
                row_num ++;
              }
              else
              {
                TBSYS_LOG(WARN, "add row into sample result scanner fail, ret=%d", ret);
              }
            }
          }

          if (OB_SUCCESS != ret)
          {
            sampler->close();
          }
        }
      }

      return ret;
    }

    int ObBuildIndexThread::start_build_global_index(
       const uint64_t data_table_id, const uint64_t index_table_id)
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "start build global index, data_table_id=%lu, index_table_id=%lu",
                data_table_id, index_table_id);

      if (!inited_)
      {
        TBSYS_LOG(WARN, "build index thread isn't initialized");
        ret = OB_NOT_INIT;
      }
      else if (OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_table_id)
      {
        TBSYS_LOG(WARN, "invalid table id, data_table_id=%lu, index_table_id=%lu",
                  data_table_id, index_table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != state_.finish_build_thread_cnt_)
      {
        TBSYS_LOG(WARN, "build index thread is busy, can't do new build work, "
                        "cur_build_type=%d, finish_build_thread_cnt=%ld", 
                  state_.build_type_, state_.finish_build_thread_cnt_);
        ret = OB_ERROR;
      }
      else
      {
        state_.reset();
        state_.build_type_ = BUILD_GLOBAL_INDEX;
        state_.is_finish_build_ = false;
        stop_build_ = false;
        init_build_option(build_option_, data_table_id, index_table_id);
        THE_CHUNK_SERVER.get_tablet_manager().get_disk_manager().scan(
           THE_CHUNK_SERVER.get_config().datadir, OB_DEFAULT_MAX_TABLET_SIZE);

        if (OB_SUCCESS != (ret = global_index_builder_.init(
           local_index_builder_.get_data_table_tablet_dist())))
        {
          TBSYS_LOG(WARN, "failed to init global index builder, data_table_id=%lu,"
                          "index_table_id=%lu, ret=%d", 
                    build_option_.data_table_id_, build_option_.index_table_id_, ret);
          state_.is_build_succ_ = false;
          if (OB_SUCCESS != (ret = build_finally()))
          {
            TBSYS_LOG(WARN, "failed to run build_finally, ret=%d", ret);
          }
        }
        else 
        {
          if (global_index_builder_.has_unavaliable_tablet())
          {
            cond_.broadcast();
          }
          else if (OB_SUCCESS != (ret = build_finally()))
          {
            TBSYS_LOG(WARN, "failed to run build_finally, ret=%d", ret);
          }
        }
      }

      return ret;
    }

    int ObBuildIndexThread::do_build_global_index(const int64_t thread_index)
    {
      int ret = OB_SUCCESS;
       
      if (BUILD_GLOBAL_INDEX != state_.build_type_)
      {
        TBSYS_LOG(WARN, "invalid build type=%d, can't do build global index", 
                  state_.build_type_);
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS !=(ret = global_index_builder_.batch_build())
               && OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "thread[%ld] build global index failed, ret %d", thread_index, ret);
      }

      if (OB_SUCCESS != ret)
      {
        stop_build_global_index();
      }

      if (_threadCount == __sync_add_and_fetch(&state_.finish_build_thread_cnt_, 1))
      {
        if (OB_SUCCESS != (ret = finish_build_global_index(thread_index)))
        {
          TBSYS_LOG(WARN, "failed to finish batch build global index, is_build_succ_=%d", 
                    state_.is_build_succ_);
        }
      }

      return ret;
    }

    void ObBuildIndexThread::stop_build_global_index()
    {
      /**
       * state_ members will be accessed by multi-thread, but all the
       * threads only read it except that multi-thread will set it to
       * false, but not set it to true in multi-thread case. so here
       * we not use lock to protect it.
       */
      state_.is_build_succ_ = false;
      global_index_builder_.stop();
    }

    int ObBuildIndexThread::finish_build_global_index(const int64_t thread_index)
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "finish batch build global index round, data_table_id=%lu, "
                      "index_table_id=%lu, is_build_succ_=%d",
                build_option_.data_table_id_, build_option_.index_table_id_, 
                state_.is_build_succ_);

      if (BUILD_GLOBAL_INDEX != state_.build_type_)
      {
        TBSYS_LOG(WARN, "invalid build type=%d, can't finish build global index", 
                  state_.build_type_);
        ret = OB_ERROR;
      }
      else if (state_.is_build_succ_)
      {
        if (OB_SUCCESS != (ret = THE_CHUNK_SERVER.get_tablet_manager().report_tablets()))
        {
          TBSYS_LOG(WARN, "failed to report tablets info to rootserver, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = build_global_index_with_migrate(thread_index)))
        {
          TBSYS_LOG(WARN, "failed to init build global index with migrate, ret=%d", ret);
        }
      }
      else 
      {
        ret = build_finally();
      }

      return ret;
    }

    int ObBuildIndexThread::build_global_index_with_migrate(const int64_t thread_index)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS == ret 
          && !global_index_builder_.has_unavaliable_tablet())
      {
        ret = build_finally();
      }
      else if (OB_SUCCESS != (ret = global_index_builder_.init(
         local_index_builder_.get_data_table_tablet_dist())))
      {
        TBSYS_LOG(WARN, "failed to init global index builder, data_table_id=%lu,"
                        "index_table_id=%lu, ret=%d", 
                  build_option_.data_table_id_, build_option_.index_table_id_, ret);
        state_.is_build_succ_ = false;
        ret = build_finally();
      }
      else if (global_index_builder_.has_unavaliable_tablet())
      {
        state_.finish_build_thread_cnt_ = 0;
        cond_.broadcast();
        ret = do_build_global_index(thread_index);
      }

      return ret;
    }

    int ObBuildIndexThread::report_build_status()
    {
      int ret = OB_SUCCESS;
      int64_t retry_times = THE_CHUNK_SERVER.get_config().retry_times;

      RPC_RETRY_WAIT(true, retry_times, ret,
          CS_RPC_CALL_RS(build_index_over, state_.is_build_succ_, 
                         THE_CHUNK_SERVER.get_self(), build_option_.index_table_id_));

      return ret;
    }

    int ObBuildIndexThread::build_finally()
    {
      int ret = OB_SUCCESS;

      TBSYS_LOG(INFO, "finish batch build global index finally, data_table_id=%lu, "
                      "index_table_id=%lu, is_build_succ_=%d",
                build_option_.data_table_id_, build_option_.index_table_id_, 
                state_.is_build_succ_);

      if (!stop_build_ && OB_SUCCESS != (ret = report_build_status()))
      {
        TBSYS_LOG(WARN, "failed to report build index status to rootserver, "
                        "is_build_succ_=%d, index_table_id_=%lu",
                  state_.is_build_succ_, build_option_.index_table_id_);
      }
      state_.finish_build_thread_cnt_ = 0;
      state_.is_finish_build_ = true;

      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
