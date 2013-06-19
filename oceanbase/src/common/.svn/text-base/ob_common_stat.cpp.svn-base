/*
 * (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_mysql_stat.cpp,  03/14/2013 05:13:14 PM xiaochu Exp $
 *
 * Author:
 *   xiaochu.yh <xiaochu.yh@taobao.com>
 * Description:
 *   Sql perf statistics
 *
 */
#include "ob_common_stat.h"

using namespace oceanbase::common;

const char *ObStatSingleton::rs_map[] = {
  "succ_get_count",
  "succ_scan_count",
  "fail_get_count",
  "fail_scan_count",
  "get_obi_role_count",
  "migrate_count",
  "copy_count",
  "get_schema_count",
  "report_version_count",
  "all_table_count",
  "all_tablet_count",
  "all_row_count",
  "all_data_size",
};

const char *ObStatSingleton::ups_map[] = {
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
  "total_rows",

  "active_memtable_limit",
  "active_memtable_total",
  "active_memtable_used",
  "active_total_rows",

  "frozen_memtable_limit",
  "frozen_memtable_total",
  "frozen_memtable_used",
  "frozen_total_rows",

  "apply_fail_count",
  "tbsys_drop_count",
  "pakcet_long_wait_count",

  "commit_log_size",
  "commit_log_id",

};

const char *ObStatSingleton::cs_map[] = {
  "serving_version",
  "old_ver_tablets_num",
  "old_ver_merged_tablets_num",
  "new_ver_tablets_num",
  "memory_used_default",
  "memory_used_network",
  "memory_used_thread_buffer",
  "memory_used_tablet",
  "memory_used_bi_cache",
  "memory_used_block_cache",
  "memory_used_bi_cache_unserving",
  "memory_used_block_cache_unserving",
  "memory_used_join_cache",
  "memory_used_sstable_row_cache",
  "memory_used_merge_buffer",
  "memory_used_merge_split_buffer",
  "request_count",
  "request_count_per_second",
  "queue_wait_time",

  "get_count",
  "scan_count",
  "get_time",
  "scan_time",
  "get_bytes",
  "scan_bytes",

  // cs version error
  "fail_cs_version_count",
};

const char *ObStatSingleton::sstable_map[] = {
  "block_index_cache_hit",
  "block_index_cache_miss",
  "block_cache_hit",
  "block_cache_miss",
  "sstable_disk_io_num",
  "sstable_disk_io_bytes",
  "sstable_row_cache_hit",
  "sstable_row_cache_miss",
  "sstable_get_rows",
  "sstable_scan_rows",
};

const char *ObStatSingleton::ms_map[] = {
  // ms_get
  "nb_get_count",
  "nb_get_time",
  // ms_scan
  "nb_scan_count",
  "nb_scan_time",

  // sql get
  "get_event_count",
  "get_event_time",
  // sql scan
  "scan_event_count",
  "scan_event_time",
};

const char *ObStatSingleton::sql_map[] = {
  "sql_grant_privilege_count",
  "sql_revoke_privilege_count",
  "sql_show_grants_count",

  "sql_create_user_count",
  "sql_drop_user_count",
  "sql_lock_user_count",
  "sql_set_password_count",
  "sql_rename_user_count",

  "sql_create_table_count",
  "sql_drop_table_count",

  "sql_ps_allocator_count",
};

const char *ObStatSingleton::common_map[] = {
  "row_desc_slow_find_count",
   // cache hit
  "location_cache_hit",
  "location_cache_miss",
  "rpc_bytes_in",
  "rpc_bytes_out",
};

const char *ObStatSingleton::obmysql_map[] = {
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
  "sql_logout_count",

  "sql_select_count",
  "sql_select_time",
  "sql_insert_count",
  "sql_insert_time",
  "sql_replace_count",
  "sql_replace_time",
  "sql_update_count",
  "sql_update_time",
  "sql_delete_count",
  "sql_delete_time",

  "sql_query_bytes",
};


ObStatManager *ObStatSingleton::mgr_ = NULL;
