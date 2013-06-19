/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_chunk_merge.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#include "ob_chunk_server_main.h"
#include "common/ob_read_common_data.h"
#include "ob_tablet_image.h"
#include "common/utility.h"
#include "ob_chunk_merge.h"
#include "sstable/ob_disk_path.h"
#include "common/ob_trace_log.h"
#include "ob_tablet_manager.h"
#include "common/ob_atomic.h"
#include "common/file_directory_utils.h"
#include "ob_tablet_merger_v0.h"
#include "ob_tablet_merger_v2.h"

namespace oceanbase
{
  namespace common
  {
    namespace hash
    {
      template <>
        struct hash_func <chunkserver::ObTablet*>
        {
          int64_t operator () (const chunkserver::ObTablet* const key) const
          {
            return reinterpret_cast<int64_t>(key);
          };
        };
      template <>
        struct equal_to <chunkserver::ObTablet*>
        {
          bool operator()(const chunkserver::ObTablet *a, const chunkserver::ObTablet *b) const
          {
            return (a == b);
          }
        };
    }
  }
  namespace chunkserver
  {
    using namespace tbutil;
    using namespace common;
    using namespace sstable;
    /*-----------------------------------------------------------------------------
     * ObTabletFactory 
     *-----------------------------------------------------------------------------*/
    ObTabletFactory::ObTabletFactory() : image_(NULL), frozen_version_(0)
    {
      reset();
    }

    ObTabletFactory::~ObTabletFactory()
    {
      destroy();
    }

    void ObTabletFactory::destroy()
    {
      for (Iterator iterator = cache_list_.begin(); iterator != cache_list_.end(); ++iterator)
      {
        image_->release_tablet(*iterator);
      }
      for (Iterator iterator = merging_list_.begin(); iterator != merging_list_.end(); ++iterator)
      {
        image_->release_tablet(*iterator);
      }
      cache_list_.clear();
      merging_list_.clear();
    }

    void ObTabletFactory::reset()
    {
      no_more_tablets_ = false;
      memset(disk_counter_, 0, sizeof(disk_counter_));
      destroy();
    }

    int ObTabletFactory::init(ObMultiVersionTabletImage *image, const int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      if (NULL == image || 0 == frozen_version) 
      {
        ret = common::OB_INVALID_ARGUMENT;
      }
      else
      {
        image_ = image;
        frozen_version_ = frozen_version;
        reset();
      }
      return ret;
    }

    int ObTabletFactory::get_tablet_from_cache_list(ObTablet* &tablet) 
    {
      int ret = OB_SUCCESS;
      tablet = NULL;
      Iterator iterator = cache_list_.begin();
      int64_t per_disk = THE_CHUNK_SERVER.get_config().merge_thread_per_disk;
      while (iterator != cache_list_.end())
      {
        tablet = *iterator;
        if (NULL == tablet) 
        {
          TBSYS_LOG(ERROR, "internal error, cache_list_ contains NULL tablet.");
          ret = OB_ENTRY_NOT_EXIST;
        }
        else if (disk_counter_[tablet->get_disk_no()] < per_disk)
        {
          // got one.
          ++disk_counter_[tablet->get_disk_no()];
          // move to merging_list_
          cache_list_.erase(iterator);
          merging_list_.push_back(tablet);
          break;
        }
        else
        {
          tablet = NULL;
          // move on to check next tablet.
          ++iterator; 
        }
      }
      return ret;
    }

    /**
     * get next tablet for merge.
     * @param [out] tablet == NULL means merge thread should wait for next get.
     * @param [out] all_tablet_merged == true means no more tablets for merge. should upgrade 
     * @return OB_SUCCESS means success otherwise ERROR.
     */
    int ObTabletFactory::get_next_tablet(ObTablet* &tablet, bool & all_tablet_merged)
    {
      int ret = OB_SUCCESS;
      tablet = NULL;
      all_tablet_merged = false;
      int64_t add_count = 0;
      if (OB_SUCCESS != (ret = get_tablet_from_cache_list(tablet)))
      {
        TBSYS_LOG(ERROR, "get_tablet_from_cache_list error, ret=%d, tablet=%p", ret, tablet);
      }
      else if (NULL == tablet)
      {
        if (!no_more_tablets_ && cache_list_.size() < TABLET_COUNT_PER_MERGE)
        {
          if (OB_SUCCESS != (ret = fill_cache(add_count)))
          {
            TBSYS_LOG(ERROR, "failed fill_cache old_cached_size=%ld", cache_list_.size());
          }
          else if (!no_more_tablets_ && add_count > 0)
          {
            // fill new tablets, retry.
            ret = get_tablet_from_cache_list(tablet);
          }
        }
        if (no_more_tablets_)
        {
          // okay , we reach the end of image, check if all tablets merge done.
          all_tablet_merged = ((cache_list_.size() + merging_list_.size()) == 0);
        }
      }
      return ret;
    }

    int ObTabletFactory::tablet_merge_done(ObTablet* tablet)
    {
      int ret = OB_SUCCESS;
      if (NULL == tablet)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = image_->release_tablet(tablet)))
      {
        TBSYS_LOG(WARN, "release_tablet (%p) error = %d", tablet, ret);
      }
      else if (disk_counter_[tablet->get_disk_no()] <= 0)
      {
        TBSYS_LOG(WARN, "tablet in disk (%d) counter (%ld) <= 0", 
            tablet->get_disk_no(), disk_counter_[tablet->get_disk_no()]);
      }
      else
      {
        --disk_counter_[tablet->get_disk_no()];
        merging_list_.erase(tablet);
      }
      return ret;
    }

    int ObTabletFactory::fill_cache(int64_t &add_count)
    {
      int ret = OB_SUCCESS;
      int hash_ret = OB_SUCCESS;
      int64_t old_cached_size = cache_list_.size();
      int64_t tablets_num = TABLET_COUNT_PER_MERGE;
      ObTablet *tablet_array[tablets_num];
      add_count = 0;

      hash::ObHashSet<ObTablet*, hash::NoPthreadDefendMode> exist_tablets;
      exist_tablets.create(TABLET_COUNT_PER_MERGE);
      
      if (old_cached_size < TABLET_COUNT_PER_MERGE)
      {
        // we have room then.
        // build hash table for filter exist tablets;
        for (Iterator iterator = cache_list_.begin(); iterator != cache_list_.end(); ++iterator)
        {
          exist_tablets.set(*iterator);
        }
        for (Iterator iterator = merging_list_.begin(); iterator != merging_list_.end(); ++iterator)
        {
          exist_tablets.set(*iterator);
        }

        if (OB_SUCCESS != (ret = image_->get_tablets_for_merge(
            frozen_version_, tablets_num, tablet_array)))
        {
          TBSYS_LOG(ERROR, "get_tablets_for_merge ret=%d, tablets_num=%ld", 
              ret , tablets_num);
        }
        else if (tablets_num == 0)
        {
          no_more_tablets_ = true;
        }
        else
        {
          // fill to cache_list_;
          for (int64_t i = 0; i < tablets_num; ++i)
          {
            if (hash::HASH_NOT_EXIST == (hash_ret = (exist_tablets.exist(tablet_array[i]))))
            {
              cache_list_.push_back(tablet_array[i]);
              ++add_count;
            }
            else if (hash::HASH_EXIST != hash_ret)
            {
              TBSYS_LOG(WARN, "check tablet:%p in hashset error=%d", tablet_array[i], hash_ret);
              ret = OB_ERROR;
            }
            else
            {
              image_->release_tablet(tablet_array[i]);
            }
          }
        }
      }
      return ret;
    }

    /*-----------------------------------------------------------------------------
     *  ObChunkMerge
     *-----------------------------------------------------------------------------*/

    ObChunkMerge::ObChunkMerge() : inited_(false), thread_num_(0), active_thread_num_(0),
                                   frozen_version_(0), newest_frozen_version_(0), frozen_timestamp_(0),
                                   write_sstable_version_(0), merge_start_time_(0),merge_last_end_time_(0),
                                   round_start_(false), pending_in_upgrade_(false),
                                   merge_load_high_(0),request_count_high_(0), merge_adjust_ratio_(0),
                                   merge_load_adjust_(0), merge_pause_row_count_(0), merge_pause_sleep_time_(0),
                                   merge_highload_sleep_time_(0), tablet_manager_(NULL)
    {
      memset(mergers_, 0, sizeof(mergers_));
    }

    void ObChunkMerge::set_config_param()
    {
      ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
      merge_load_high_ = chunk_server.get_config().merge_threshold_load_high;
      request_count_high_ = chunk_server.get_config().merge_threshold_request_high;
      merge_adjust_ratio_ = chunk_server.get_config().merge_adjust_ratio;
      merge_load_adjust_ = (merge_load_high_ * merge_adjust_ratio_) / 100;
      merge_pause_row_count_ = chunk_server.get_config().merge_pause_row_count;
      merge_pause_sleep_time_ = chunk_server.get_config().merge_pause_sleep_time;
      merge_highload_sleep_time_ = chunk_server.get_config().merge_highload_sleep_time;
    }

    int ObChunkMerge::create_merge_threads(const int64_t max_merge_thread)
    {
      int ret = OB_SUCCESS;

      setThreadCount(static_cast<int32_t>(max_merge_thread));
      active_thread_num_ = max_merge_thread;
      thread_num_ = start();

      if (thread_num_ <= 0)
      {
        TBSYS_LOG(ERROR, "cannot create merge thread , max_merge_thread=%ld", max_merge_thread);
        ret = OB_ERROR;
      }
      else 
      {
        if (thread_num_ != max_merge_thread)
        {
          TBSYS_LOG(WARN, "start %ld merge threads, less than expected %ld", thread_num_, max_merge_thread);
        }
        min_merge_thread_num_ = thread_num_ / 3;
        if (min_merge_thread_num_ == 0) min_merge_thread_num_ = 1;
        TBSYS_LOG(INFO, "create_merge_threads thread_num_=%ld, "
            "active_thread_num_=%ld, min_merge_thread_num_=%ld",
            thread_num_, active_thread_num_, min_merge_thread_num_);
      }
      return ret;
    }

    int ObChunkMerge::init(ObTabletManager *manager)
    {
      int ret = OB_SUCCESS;
      ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

      if (NULL == manager)
      {
        TBSYS_LOG(ERROR,"input error,manager is null");
        ret = OB_ERROR;
      }
      else if (!inited_)
      {
        inited_ = true;

        tablet_manager_ = manager;
        frozen_version_ = manager->get_last_not_merged_version();
        newest_frozen_version_ = frozen_version_;

        pthread_mutex_init(&mutex_,NULL);
        pthread_cond_init(&cond_,NULL);

        int64_t max_merge_thread = chunk_server.get_config().max_merge_thread_num;
        if (max_merge_thread <= 0 || max_merge_thread > MAX_MERGE_THREAD)
          max_merge_thread = MAX_MERGE_THREAD;

        set_config_param();
        if (OB_SUCCESS != (ret = create_merge_threads(max_merge_thread)))
        {
          TBSYS_LOG(ERROR, "create_merge_threads error, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = create_all_tablet_mergers()))
        {
          TBSYS_LOG(ERROR, "create_all_tablet_mergers error, ret=%d", ret);
        }
      }
      else
      {
        TBSYS_LOG(WARN,"ObChunkMerge have been inited");
      }

      if (OB_SUCCESS != ret && inited_)
      {
        pthread_mutex_destroy(&mutex_);
        pthread_cond_destroy(&cond_);
        inited_ = false;
      }
      return ret;
    }

    void ObChunkMerge::destroy()
    {
      if (inited_)
      {
        if (false == THE_CHUNK_SERVER.get_config().each_tablet_sync_meta
            && !is_merge_stoped())
        {
          tablet_manager_->sync_all_tablet_images();
        }
        inited_ = false;
        pthread_cond_broadcast(&cond_);
        usleep(50);

        wait();
        pthread_cond_destroy(&cond_);
        pthread_mutex_destroy(&mutex_);
        destroy_all_tablets_mergers();
      }
    }

    int ObChunkMerge::get_tablet_merger(const int64_t thread_no, ObTabletMerger* &merger)
    {
      int ret = OB_SUCCESS;
      merger = NULL;
      ObTabletMerger** mergers = NULL;
      if (write_sstable_version_ < SSTableReader::COMPACT_SSTABLE_VERSION)
      {
        mergers = mergers_;
      }
      else
      {
        mergers = mergers_ + MAX_MERGE_THREAD;
      }

      if (thread_no >= MAX_MERGE_THREAD)
      {
        TBSYS_LOG(ERROR, "thread_no=%ld >= max_merge_thread_num=%ld", thread_no, MAX_MERGE_THREAD);
        ret = OB_SIZE_OVERFLOW;
      }
      else if (NULL == mergers)
      {
        TBSYS_LOG(ERROR, "thread_no=%ld mergers is NULL, version=%ld", thread_no, write_sstable_version_);
        ret = OB_SIZE_OVERFLOW;
      }
      else if (NULL == (merger = mergers[thread_no]))
      {
        TBSYS_LOG(ERROR, "thread_no=%ld merger is NULL, version=%ld", thread_no, write_sstable_version_);
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }

    int ObChunkMerge::create_all_tablet_mergers()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = create_tablet_mergers<ObTabletMergerV0>(mergers_, MAX_MERGE_THREAD)))
      {
        TBSYS_LOG(ERROR, "create v1 Merger error, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = create_tablet_mergers<ObTabletMergerV2>(mergers_ + MAX_MERGE_THREAD, MAX_MERGE_THREAD)))
      {
        TBSYS_LOG(ERROR, "create v2 Merger error, ret=%d", ret);
      }
      return ret;
    }

    int ObChunkMerge::destroy_tablet_mergers(ObTabletMerger** mergers, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (NULL == mergers || 0 >= size)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        for (int64_t i = 0 ; i < size; ++i)
        {
          ObTabletMerger* merger = mergers[i];
          if (NULL != merger)
          {
            merger->~ObTabletMerger();
          }
        }
        void * ptr = mergers[0];
        ob_free(ptr);
        memset(mergers, 0, size * sizeof(ObTabletMerger*));
      }
      return ret;
    }

    int ObChunkMerge::destroy_all_tablets_mergers()
    {
      int ret = OB_SUCCESS;
      destroy_tablet_mergers(mergers_, MAX_MERGE_THREAD );
      destroy_tablet_mergers(mergers_ + MAX_MERGE_THREAD, MAX_MERGE_THREAD );
      return ret;
    }

    bool ObChunkMerge::can_launch_next_round(const int64_t frozen_version)
    {
      bool ret = false;
      int64_t now = tbsys::CTimeUtil::getTime();


      if (inited_ && frozen_version > frozen_version_ && is_merge_stoped()
          && frozen_version > tablet_manager_->get_last_not_merged_version()
          && now - merge_last_end_time_ > THE_CHUNK_SERVER.get_config().min_merge_interval
          && THE_CHUNK_SERVER.get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped()
          && THE_CHUNK_SERVER.get_tablet_manager().get_build_index_thread().is_finish_build())
      {
        ret = true;
      }
      return ret;
    }

    int ObChunkMerge::schedule(const int64_t frozen_version)
    {

      int ret = OB_SUCCESS;

      if (frozen_version > newest_frozen_version_)
      {
        newest_frozen_version_ = frozen_version;
      }

      // empty chunkserver, reset current frozen_version_ if
      // chunkserver got new tablet by migrate in or create new table;
      if ( 0 == frozen_version_ )
      {
        frozen_version_ = tablet_manager_->get_last_not_merged_version();
      }

      if (1 >= frozen_version || (!can_launch_next_round(frozen_version)))
      {
        // do nothing
        TBSYS_LOG(INFO, "frozen_version=%ld, current frozen version = %ld, "
            "serving data version=%ld cannot launch next round.",
            frozen_version, frozen_version_, tablet_manager_->get_last_not_merged_version());
        ret = OB_CS_EAGAIN;
      }
      else if (0 == tablet_manager_->get_last_not_merged_version()) //new chunkserver
      {
        // empty chunkserver, no need to merge.
        TBSYS_LOG(INFO, "empty chunkserver , wait for migrate in.");
        ret = OB_CS_EAGAIN;
      }
      else
      {
        if (frozen_version - frozen_version_ > 1)
        {
          TBSYS_LOG(WARN, "merge is too slow,[%ld:%ld]", frozen_version_, frozen_version);
        }
        // only plus 1 in one merge process.
        frozen_version_ += 1;

        ret = start_round(frozen_version_);
        if (OB_SUCCESS != ret)
        {
          // start merge failed, maybe rootserver or updateserver not in serving.
          // restore old frozen_version_ for next merge process.
          frozen_version_ -= 1;
        }
      }

      if (inited_ && OB_SUCCESS == ret && thread_num_ > 0)
      {
        merge_start_time_ = tbsys::CTimeUtil::getTime();
        write_sstable_version_ = THE_CHUNK_SERVER.get_config().merge_write_sstable_version;
        factory_.init(&tablet_manager_->get_serving_tablet_image(), frozen_version_);
        round_start_ = true;
        TBSYS_LOG(INFO, "start new round ,wake up all merge threads, "
            "run new merge process with version=%ld, write sstable version=%ld", 
            frozen_version_, write_sstable_version_);
        pthread_cond_broadcast(&cond_);
      }

      return ret;
    }

    void ObChunkMerge::run(tbsys::CThread* thread, void *arg)
    {
      UNUSED(thread);
      int64_t thread_no = reinterpret_cast<int64_t>(arg);
      merge_tablets(thread_no);
    }

    void ObChunkMerge::merge_tablets(const int64_t thread_no)
    {
      int ret = OB_SUCCESS;
      const int64_t sleep_interval = 5000000;
      ObTablet *tablet = NULL;
      ObTabletMerger *merger = NULL;

      ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

      while(OB_SUCCESS == ret)
      {
        if (!inited_)
        {
          break;
        }

        if ( !check_load())
        {
          TBSYS_LOG(INFO,"load is too high, go to sleep");
          pthread_mutex_lock(&mutex_);

          if (1 == active_thread_num_)
          {
            pthread_mutex_unlock(&mutex_);
            usleep(sleep_interval); //5s
          }
          else
          {
            --active_thread_num_;
            pthread_cond_wait(&cond_, &mutex_);
            TBSYS_LOG(INFO,"to merge,active_thread_num_ :%ld", active_thread_num_);
            ++active_thread_num_;
            pthread_mutex_unlock(&mutex_);
          }
        }

        pthread_mutex_lock(&mutex_);
        ret = get_tablets(tablet);
        while (true)
        {
          if (!inited_)
          {
            break;
          }
          if (OB_SUCCESS != ret)
          {
            pthread_mutex_unlock(&mutex_);
            usleep(sleep_interval);
            // retry to get tablet until got one or got nothing.
            pthread_mutex_lock(&mutex_);
          }
          else if (NULL == tablet) // got nothing
          {
            --active_thread_num_;
            TBSYS_LOG(INFO,"there is no tablet need merge, sleep wait for new merge proecess.");
            pthread_cond_wait(&cond_,&mutex_);
            TBSYS_LOG(INFO,"awake by signal, active_thread_num_:%ld",active_thread_num_);
            // retry to get new tablet for merge.
            ++active_thread_num_;
          }
          else // got tablet for merge
          {
            break;
          }
          ret = get_tablets(tablet);
        }
        pthread_mutex_unlock(&mutex_);

        int64_t retry_times = chunk_server.get_config().retry_times;

        // okay , we got a tablet for merge finally.
        if (NULL != tablet)
        {
          if (tablet->get_data_version() > frozen_version_)
          {
            //impossible
            TBSYS_LOG(ERROR,"local tablet version (%ld) > frozen_version_(%ld)",tablet->get_data_version(),frozen_version_);
            kill(getpid(),2);
          }
          else if ((tablet->get_merge_count() > retry_times) && (have_new_version_in_othercs(tablet)))
          {
            TBSYS_LOG(WARN,"too many times(%d),discard this tablet,wait for migrate copy.", tablet->get_merge_count());
            if (OB_SUCCESS == tablet_manager_->delete_tablet_on_rootserver(&tablet, 1))
            {
              int32_t disk_no = tablet->get_disk_no();
              TBSYS_LOG(INFO, "delete tablet (version=%ld) on rs succeed. ", tablet->get_data_version());
              if(OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().set_tablet_merged(tablet)))
              {
                TBSYS_LOG(WARN, "set the tablet merged error, ret=%d", ret);
              }
              if(OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().remove_tablet(tablet->get_range(), tablet->get_data_version(), disk_no)))
              {
                TBSYS_LOG(WARN, "remove the tablet error, ret=%d", ret);
              }

              //flush the change log to disk
              if(OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().flush_log(OB_LOG_CS_MERGE_TABLET, OB_SUCCESS == ret)))
              {
                TBSYS_LOG(ERROR,"flush redo log error, ret=%d", ret);
              }

              disk_no = 0;
              if (OB_SUCCESS != tablet_manager_->get_serving_tablet_image().remove_tablet(
                tablet->get_range(), tablet->get_data_version(), disk_no))
              {
                TBSYS_LOG(WARN, "failed to remove tablet from tablet image, "
                                "version=%ld, disk=%d, range=%s",
                          tablet->get_data_version(), tablet->get_disk_no(), 
                          to_cstring(tablet->get_range()));
              }
            }
          }
          else if (OB_SUCCESS != (ret = get_tablet_merger(thread_no, merger)))
          {
            TBSYS_LOG(ERROR, "cannot get_tablet_merger thread_no=%ld", thread_no);
          }
          else
          {
            if ( (newest_frozen_version_ - tablet->get_data_version())
                 > chunk_server.get_config().max_version_gap )
            {
              TBSYS_LOG(WARN,"this tablet version (%ld : %ld) is too old,maybe don't need to merge",
                  tablet->get_data_version(),newest_frozen_version_);
            }

            int merge_ret = OB_SUCCESS;
            if (OB_SUCCESS != (merge_ret = merger->merge(tablet, tablet->get_data_version() + 1))
                && OB_CS_TABLE_HAS_DELETED != merge_ret)
            {
              TBSYS_LOG(WARN,"merge tablet(%s), version[%ld] error(%d)", 
                  to_cstring(tablet->get_range()), tablet->get_data_version(), merge_ret);
            }
            tablet->inc_merge_count();
          }

          pthread_mutex_lock(&mutex_);
          if (OB_SUCCESS != (factory_.tablet_merge_done(tablet)))
          {
            TBSYS_LOG(WARN, "release tablet error.");
          }
          pthread_mutex_unlock(&mutex_);
        }

        if ( tablet_manager_->is_stoped() )
        {
          TBSYS_LOG(WARN,"stop in merging");
          ret = OB_CS_MERGE_CANCELED;
        }
      }

    }

    int ObChunkMerge::fetch_frozen_time_busy_wait(const int64_t frozen_version, int64_t &frozen_time)
    {
      int ret = OB_SUCCESS;
      if (0 == frozen_version)
      {
        TBSYS_LOG(ERROR,"invalid argument, frozen_version is 0");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObMergerRpcProxy* rpc_proxy = THE_CHUNK_SERVER.get_rpc_proxy();
        if (NULL != rpc_proxy)
        {
          int64_t retry_times  = THE_CHUNK_SERVER.get_config().retry_times;
          RPC_RETRY_WAIT(inited_, retry_times, ret,
                         rpc_proxy->get_frozen_time(frozen_version, frozen_time));
        }
        else
        {
          TBSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int ObChunkMerge::fetch_frozen_schema_busy_wait(
      const int64_t frozen_version, ObSchemaManagerV2& schema)
    {
      int ret = OB_SUCCESS;
      if (0 == frozen_version)
      {
        TBSYS_LOG(ERROR,"invalid argument, frozen_version is 0");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObMergerRpcProxy* rpc_proxy = THE_CHUNK_SERVER.get_rpc_proxy();
        if (NULL != rpc_proxy)
        {
          int64_t retry_times  = THE_CHUNK_SERVER.get_config().retry_times;
          RPC_RETRY_WAIT(inited_, retry_times, ret,
            rpc_proxy->get_frozen_schema(frozen_version, schema));
        }
        else
        {
          TBSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    /**
     * luanch next merge round, do something before doing actual
     * merge stuff .
     * 1. fetch new schema with current frozen version; must be
     * compatible with last schema;
     * 2. fetch new freeze timestamp with current frozen version,
     * for TTL (filter expired line);
     * 3. prepare for next merge, drop block cache used in pervoius
     * merge round, drop image slot used in prevoius version;
     * 4. initialize import sstable instance;
     */
    int ObChunkMerge::start_round(const int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      // save schema used by last merge process .
      if (current_schema_.get_version() > 0)
      {
        last_schema_ = current_schema_;
      }

      // fetch frozen schema with frozen_version_;
      ret = fetch_frozen_schema_busy_wait(frozen_version, current_schema_);
      if (OB_SUCCESS == ret)
      {
        if(current_schema_.get_version() > 0 && last_schema_.get_version() > 0)
        {
          if (current_schema_.get_version() < last_schema_.get_version())
          {
            TBSYS_LOG(ERROR,"the new schema old than last schema, current=%ld, last=%ld",
                current_schema_.get_version(), last_schema_.get_version());
            ret = OB_CS_SCHEMA_INCOMPATIBLE;
          }
          else if (!last_schema_.is_compatible(current_schema_))
          {
            TBSYS_LOG(ERROR,"the new schema and old schema is not compatible");
            ret = OB_CS_SCHEMA_INCOMPATIBLE;
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "cannot luanch next merge round cause schema issue.");
      }
      else if (OB_SUCCESS != (ret = fetch_frozen_time_busy_wait(frozen_version, frozen_timestamp_)))
      {
        TBSYS_LOG(ERROR, "cannot fetch frozen %ld timestamp from updateserver.", frozen_version);
      }
      else if (OB_SUCCESS != (ret = tablet_manager_->prepare_merge_tablets(frozen_version)))
      {
        TBSYS_LOG(ERROR, "not prepared for new merge process, version=%ld", frozen_version);
      }
      else
      {
        TBSYS_LOG(INFO, "new merge process, version=%ld, frozen_timestamp_=%ld",
            frozen_version, frozen_timestamp_);
      }

      return ret;
    }

    int ObChunkMerge::finish_round(const int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      pending_in_upgrade_ = true;
      tablet_manager_->sync_all_tablet_images();  //ignore err
      if ( OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().finish_merge()) )
      {
        TBSYS_LOG(ERROR, "upgrade_service to version = %ld failed", frozen_version);
      }
      else
      {
        TBSYS_LOG(INFO,"this version (%ld) is merge done,to switch cache and upgrade",frozen_version);
        // wait for other worker threads release old cache,
        // switch to use new cache.
        tablet_manager_->get_join_cache().destroy();
        tablet_manager_->get_disk_manager().scan(THE_CHUNK_SERVER.get_config().datadir,
                                                 OB_DEFAULT_MAX_TABLET_SIZE);
        // report tablets
        tablet_manager_->report_tablets();
        tablet_manager_->report_capacity_info();

        // upgrade complete, no need pending, for migrate in.
        pending_in_upgrade_ = false;
        round_start_ = false;

        // re scan all local disk to recycle sstable
        // left by RegularRecycler. e.g. migrated sstable.
        tablet_manager_->get_scan_recycler().recycle();

        merge_last_end_time_ = tbsys::CTimeUtil::getTime();
      }
      return ret;
    }

    /**
     * @brief get a tablet to merge,get lock first
     *
     * @return
     */
    int ObChunkMerge::get_tablets(ObTablet* &tablet)
    {
      tablet = NULL;
      bool all_tablet_merged = false;
      int ret = OB_SUCCESS;
      if (round_start_)
      {
        if (OB_SUCCESS != (ret = factory_.get_next_tablet(tablet, all_tablet_merged)))
        {
          TBSYS_LOG(WARN, "get_next_tablet ret=%d", ret);
        }
        else
        {
          pending_in_upgrade_ = factory_.pending_in_upgrade();
          if (NULL != tablet)
          {
            int64_t seconds = (tbsys::CTimeUtil::getTime() - merge_start_time_) / 1000L / 1000L;
            TBSYS_LOG(INFO, "merge consume seconds:%lds, minutes:%.2fm, hours:%.2fh, merge process:%s",
                seconds, (double)seconds / 60.0, (double)seconds / 3600.0,
                tablet_manager_->get_serving_tablet_image().print_tablet_image_stat());
          }
          if (all_tablet_merged)
          {
            if (OB_SUCCESS != (ret = finish_round(frozen_version_)))
            {
              TBSYS_LOG(WARN, "finish_round version=%ld, ret=%d", frozen_version_, ret);
            }
          }
        }
      }
      return ret;
    }

    bool ObChunkMerge::have_new_version_in_othercs(const ObTablet* tablet)
    {
      bool ret = false;

      if (tablet != NULL)
      {
        ObTabletLocation list[OB_SAFE_COPY_COUNT];
        int32_t size = OB_SAFE_COPY_COUNT;
        int32_t new_copy = 0;
        int err = CS_RPC_CALL_RS(get_tablet_info,  current_schema_,
            tablet->get_range().table_id_, tablet->get_range(), list, size);
        if (OB_SUCCESS == err)
        {
          for(int32_t i=0; i<size; ++i)
          {
            TBSYS_LOG(INFO,"version:%ld",list[i].tablet_version_); //for test
            if (list[i].tablet_version_ > tablet->get_data_version())
              ++new_copy;
          }
        }
        if (OB_SAFE_COPY_COUNT - 1 == new_copy)
          ret = true;
      }
      return ret;
    }

    bool ObChunkMerge::check_load()
    {
      bool ret = false;
      double loadavg[3];

      // ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
      volatile int64_t current_request_count_ = 0;

      if (getloadavg(loadavg,sizeof(loadavg)/sizeof(loadavg[0])) < 0)
      {
        TBSYS_LOG(WARN,"getloadavg failed");
        loadavg[0] = 0;
      }

      ObStat *stat = NULL;
      
      OB_STAT_GET(CHUNKSERVER, stat);
      if (NULL == stat)
      {
        //TBSYS_LOG(WARN,"get stat failed");
        current_request_count_  = 0;
      }
      else
      {
        current_request_count_ = stat->get_value(INDEX_META_REQUEST_COUNT_PER_SECOND);
      }


      pthread_mutex_lock(&mutex_);
      if (active_thread_num_ <= min_merge_thread_num_)
      {
        TBSYS_LOG(INFO, "current active thread :%ld < min merge thread: %ld, continue run.",
            active_thread_num_, min_merge_thread_num_);
        ret = true;
      }
      // check load and request if match the merge conditions.
      if ((loadavg[0]  < merge_load_high_)
          && (current_request_count_ < request_count_high_))
      {
        ret = true;
        int64_t sleep_thread = thread_num_ - active_thread_num_;
        int64_t remain_load = merge_load_high_ - static_cast<int64_t>(loadavg[0]) - 1; //loadavg[0] double to int
        int64_t remain_tablet = factory_.get_cache_list_size();
        if ((loadavg[0] < merge_load_adjust_) &&
            (remain_tablet > 0) && (sleep_thread > 0) && (remain_load > 0) )
        {
          TBSYS_LOG(INFO,"wake up %ld thread(s)",sleep_thread > remain_load ? remain_load : sleep_thread);
          while(sleep_thread-- > 0 && remain_load-- > 0)
          {
            pthread_cond_signal(&cond_);
          }
        }
      }
      else
      {
        TBSYS_LOG(INFO,"loadavg[0] : %f,merge_load_high_:%ld,current_request_count_:%ld,request_count_high_:%ld",
            loadavg[0],merge_load_high_,current_request_count_,request_count_high_);
      }
      pthread_mutex_unlock(&mutex_);
      return ret;
    }

  } /* chunkserver */
} /* oceanbase */
