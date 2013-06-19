#include <algorithm>
#include "ob_server_stats.h"

using namespace oceanbase::common;

/*
static char* root_server_stats_str[] =
{
  "succ_get_count",
  "succ_scan_count",
  "fail_get_count",
  "fail_scan_count",
};

static char* chunk_server_stats_str[] =
{
  "get_count",
  "scan_count",

  "get_time",
  "scan_time",

  "block_index_cache_hit",
  "block_index_cache_miss",

  "block_cache_hit",
  "block_cache_miss",

  "get_bytes",
  "scan_bytes",

  "disk_io_num",
  "disk_io_bytes",

  "block_index_cache_hit_ratio",
  "block_cache_hit_ratio",
};

static char* merge_server_stats_str[] =
{
  "succ_get_count",
  "succ_get_time",
  "fail_get_count",
  "fail_get_time",

  // scan
  "succ_scan_count",
  "succ_scan_time",
  "fail_scan_count",
  "fail_scan_time",

  // cache hit
  "cs_cache_hit",
  "cs_cache_miss",

  // cs version error
  "fail_cs_version",

  // local query
  "local_cs_query",
  "remote_cs_query",

  "cs_cache_hit_ratio"
};

static char* update_server_stats_str[] =
{
  "min",

  "get_count",
  "scan_count",
  "apply_count",
  "batch_count",
  "merge_count",

  "get_timeu",
  "scan_timeu",
  "apply_timeu",
  "batch_timeu",
  "merge_timeu",

  "memory_total",
  "memory_limit",
  "memtable_total",
  "memtable_used",
  "total_line",

  "active_memtable_limit",
  "active_memtable_total",
  "active_memtable_used",
  "active_total_line",

  "frozen_memtable_limit",
  "frozen_memtable_total",
  "frozen_memtable_used",
  "frozen_total_line",

  "apply_fail_count",
  "tbsys_drop_count",

  "max",
};


static char** stats_info_str[] =
{
  NULL,
  root_server_stats_str,
  chunk_server_stats_str,
  merge_server_stats_str,
  update_server_stats_str
};
*/

//static int stats_size[5] = {0, 4, 14, 14, 26};
static const char* server_name[] = {"rs", "cs", "ms", "ups", "sql", "mysql", "common", "sstable"};


namespace oceanbase
{
  namespace tools
  {

    //---------------------------------------------------------------
    // class Present
    //---------------------------------------------------------------

    const char *Present::rs_map[] = {
      "succ_get_count",
      "succ_scan_count",
      "fail_get_count",
      "fail_scan_count",
      "get_obi_role_count",
      "migrate_count",
      "copy_count",
    };

    const char *Present::ups_map[] = {
      "get_count",
      "scan_count",
      "apply_count",
      "batch_count",
      "merge_count",

      "get_time",
      "scan_time",
      "apply_time",
      "batch_time",
      "merge_time",

      "memory_total",
      "memory_limit",
      "memtable_total",
      "memtable_used",
      "total_line",

      "active_memtable_limit",
      "active_memtable_total",
      "active_memtable_used",
      "active_total_line",

      "frozen_memtable_limit",
      "frozen_memtable_total",
      "frozen_memtable_used",
      "frozen_total_line",

      "apply_fail_count",
      "tbsys_drop_count",
      "pakcet_long_wait_count",

    };

    const char *Present::cs_map[] = {
      "tablets_num",
      "merge_tablets_num",
      "merge_merged",
      "memory_used_default",
      "memory_used_network",
      "memory_used_thread_buffer",
      "memory_used_tablet",
      "memory_used_bi_cache",
      "memory_used_block_cache",
      "memory_used_bi_cache_unserving",
      "memory_used_block_cache_unserving",
      "memory_used_join_cache",
      "memory_used_merge_buffer",
      "memory_used_merge_split_buffer",
      "request_count",
      "request_count_per_second",
      "request_queue_wait_time",

      "get_count",
      "scan_count",
      "get_time",
      "scan_time",
    };

    const char *Present::sstable_map[] = {
      "block_index_cache_hit",
      "block_index_cache_miss",
      "block_cache_hit",
      "block_cache_miss",
      "get_bytes",
      "scan_bytes",
      "disk_io_num",
      "disk_io_bytes",
      "sstable_row_cache_hit",
      "sstable_row_cache_miss",
    };

    const char *Present::ms_map[] = { 
      "succ_get_count",
      "succ_get_time",
      "fail_get_count",
      "fail_get_time",

      // scan
      "succ_scan_count",
      "succ_scan_time",
      "fail_scan_count",
      "fail_scan_time",

      // cache hit
      "hit_cs_cache_count",
      "miss_cs_cache_count",

      // cs version error
      "fail_cs_version_count",

      // local query
      "local_cs_query_count",
      "remote_cs_query_count",

    };

    const char *Present::sql_map[] = {
      "sql_select_count",
      "sql_insert_count",
      "sql_replace_count",
      "sql_update_count",
      "sql_delete_count",
      "sql_ps_allocator_count"
    };

    const char *Present::common_map[] = {
      "row_desc_slow_find_count",
    };

    const char *Present::obmysql_map[] = {
      "sql_succ_query_count",
      "sql_fail_query_count",
      "sql_succ_prepare_count",
      "sql_fail_prepare_count",
      "sql_succ_exec_count",
      "sql_fail_exec_count",
      "sql_succ_close_count",
      "sql_fail_close_count",
      "sql_succ_login_count",
      "sql_fail_login_count",
    };

    Present::Present()
    {
    }

    const char** Present::strmap(const common::ObStatMod mod)
    {
      switch (mod)
      {
        case OB_STAT_ROOTSERVER:
          return rs_map;
        case OB_STAT_CHUNKSERVER:
          return cs_map;
        case OB_STAT_MERGESERVER:
          return ms_map;
        case OB_STAT_UPDATESERVER:
          return ups_map;
        case OB_STAT_SQL:
          return sql_map;
        case OB_STAT_OBMYSQL:
          return obmysql_map;
        case OB_STAT_COMMON:
          return common_map;
        case OB_STAT_SSTABLE:
          return sstable_map;
        default:
          return NULL;
      }
      return NULL;
    }

    const int64_t Present::mod_size(const common::ObStatMod mod)
    {
      switch (mod)
      {
        case OB_STAT_ROOTSERVER:
          return sizeof(rs_map)/sizeof(const char*);
        case OB_STAT_CHUNKSERVER:
          return sizeof(cs_map)/sizeof(const char*);
        case OB_STAT_MERGESERVER:
          return sizeof(ms_map)/sizeof(const char*);
        case OB_STAT_UPDATESERVER:
          return sizeof(ups_map)/sizeof(const char*);
        case OB_STAT_SQL:
          return sizeof(sql_map)/sizeof(const char*);
        case OB_STAT_OBMYSQL:
          return sizeof(obmysql_map)/sizeof(const char*);
        case OB_STAT_COMMON:
          return sizeof(common_map)/sizeof(const char*);
        case OB_STAT_SSTABLE:
          return sizeof(sstable_map)/sizeof(const char*);
        default:
          return 0;
      }
      return 0;
    }

    const char* Present::field_name(const common::ObStatMod mod, const uint32_t entry)  
    {
      const char** strs = strmap(mod);
      if ((int64_t)entry < mod_size(mod))
      {
        return strs[entry];
      }
      return NULL;
    }

    const int64_t Present::field_width(const common::ObStatMod mod, const uint32_t entry)
    {
      UNUSED(mod);
      UNUSED(entry);
      return 8;
    }

    //---------------------------------------------------------------
    // class ObServerStats
    //---------------------------------------------------------------

    int32_t ObServerStats::init()
    {
      return 0;
    }

    void ObServerStats::initialize_empty_value()
    {
      std::set<uint64_t>::const_iterator it = table_filter_.begin();
      while (it != table_filter_.end())
      {
        uint64_t table_id = *it;
        for (int i = 0; i < Present::mod_size(mod_); ++i)
        {

          current_.set_value(mod_, table_id, i, 0);
        }
        ++it;
      }
    }

    // retrive data from dataserver.
    int32_t ObServerStats::refresh()
    {
      int ret = rpc_stub_.fetch_stats(current_);
      return ret;
    }

    int ObServerStats::calc_hit_ratio(oceanbase::common::ObStat &stat_item,
        const int ratio, const int hit, const int miss)
    {
      int64_t hit_num = stat_item.get_value(hit);
      int64_t miss_num = stat_item.get_value(miss);
      if (hit_num + miss_num == 0) return 0;
      int64_t ratio_num  = hit_num * 100 / (hit_num + miss_num);
      return stat_item.set_value(ratio, ratio_num);
    }

    int ObServerStats::calc_div_value(oceanbase::common::ObStat &stat_item,
        const int div, const int count, const int time)
    {
      int64_t count_value = stat_item.get_value(count);
      int64_t time_value = stat_item.get_value(time);
      int64_t div_value = 0;
      if (count_value != 0) div_value = time_value / count_value;
      return stat_item.set_value(div, div_value);
    }

    int32_t ObServerStats::calc()
    {
      return 0;
    }

    int32_t ObServerStats::save()
    {
      return 0;
    }


    void ObServerStats::output_header()
    {
      //char** info_str = stats_info_str[current_.get_server_type()];
      //int32_t show_size = stats_size[current_.get_server_type()];
      if (show_date_) fprintf(stderr, "%18s|", "date      time");
      fprintf(stderr, "%s|", server_name[mod_]);
      if (index_filter_.size() > 0 )
      {
        for (uint32_t i = 0 ; i < index_filter_.size(); ++i)
        {
          uint32_t index = index_filter_[i];
          if (index >= Present::mod_size(mod_))
          {
            fprintf(stderr, "%8s ", "unknown") ;
          }
          else
          {
            fprintf(stderr, "%8s ", Present::field_name(mod_, index)) ;
          }
        }
      }
      else
      {
        for (uint32_t i = 0 ; i < Present::mod_size(mod_); ++i)
        {
          fprintf(stderr, "%8s ", Present::field_name(mod_, i)) ;
        }
      }
      fprintf(stderr, "\n");
    }

    int64_t ObServerStats::print_value(
        oceanbase::common::ObStatManager::const_iterator it,
        const uint32_t index) const
    {
      const int32_t FMT_SIZE = 24;
      char fmt[FMT_SIZE];
      fmt[0] = '%';
      snprintf(fmt+1, FMT_SIZE-1, "%ldld ", Present::field_width(mod_, index));
      int64_t value = it->get_value(index);
      //fprintf(stderr, "index=%d,interval=%d,value=%ld\n", index,interval, value);
      {
        // get value from current;
        ObStat *cur_stat = NULL;
        int ret = current_.get_stat(it->get_mod_id(), it->get_table_id(), cur_stat);
        if (OB_SUCCESS == ret && NULL != cur_stat)
        {
          value = cur_stat->get_value(index);
        }
      }
      fprintf(stderr, fmt, value ) ;
      return 0;
    }

    int32_t ObServerStats::output(const int32_t count, const int32_t interval)
    {
      if (show_header_ > 0 && count % show_header_ == 0) output_header();
      //if (count == 0) return 0;
      UNUSED(interval);

      time_t t;
      time(&t);
      struct tm tm;
      ::localtime_r((const time_t*)&t, &tm);
      char date_buffer[OB_MAX_FILE_NAME_LENGTH];
      snprintf(date_buffer,OB_MAX_FILE_NAME_LENGTH,"%04d-%02d-%02d %02d:%02d:%02d",
          tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
          tm.tm_hour, tm.tm_min, tm.tm_sec);

      ObStatManager::const_iterator it = current_.begin(mod_);
      while (it != current_.end(mod_))
      {
        if (table_filter_.size() > 0
            && table_filter_.find(it->get_table_id()) == table_filter_.end())
        {
          ++it;
          continue;
        }

        if (show_date_) fprintf(stderr, "%s ", date_buffer);
        fprintf(stderr, "%8ld ", it->get_table_id());

        if (index_filter_.size() > 0 )
        {
          for (uint32_t i = 0 ; i < index_filter_.size(); ++i)
          {
            uint32_t index = index_filter_[i];
            if (index >= Present::mod_size(mod_))
            {
              fprintf(stderr, "%4ld ", 0L) ;
            }
            else
            {
              print_value(it, index);
            }
          }
        }
        else
        {
          for (int i = 0 ; i < Present::mod_size(mod_); ++i)
          {
            print_value(it, i);
          }
        }
        fprintf(stderr, "\n");
        ++it;
      }
      return 0;
    }

  }
}
