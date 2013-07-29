#include "tblog.h"
#include "common/ob_define.h"
#include "common/murmur_hash.h"
#include "ob_sql_select_method_impl.h"
#include "ob_sql_ms_select.h"
#include "ob_sql_util.h"
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <errno.h>

ObDataSource* random_mergeserver_select(ObClusterInfo *cluster)
{
  ObDataSource* ds = NULL;
  if (0 < cluster->size_)
  {
    long int r = random() % (cluster->csize_);
    ds = cluster->dslist_ + r;
    TBSYS_LOG(DEBUG, "ds offset is %ld", r);
  }
  return ds;
}

/* consisten hash */
ObDataSource* consishash_mergeserver_select(ObClusterInfo *pool, const char* sql, unsigned long length)
{
  ObDataSource* datasource = NULL;
  uint32_t hashval = 0;
  hashval = oceanbase::common::murmurhash2(sql, static_cast<int32_t>(length), hashval);
  TBSYS_LOG(INFO, "hashval of this query is %u", hashval);
  int32_t index = 0;
  for (; index < g_config_using->cluster_size_; ++index)
  {
    if (pool->cluster_id_ == g_config_using->clusters_[index].cluster_id_)
    {
      break;
    }
  }
  ObSQLSelectMsTable *table = g_config_using->clusters_[index].table_;
   datasource = find_ds_by_val(table->items_, table->slot_num_, hashval);
  return datasource;
}

/* random */
ObSQLConn* random_conn_select(ObDataSource *pool)
{
  ObSQLListNode *node = NULL;
  ObSQLConn* conn = NULL;
  TBSYS_LOG(DEBUG, "pool is %p, conn_list is %p  free_conn_list is %p", pool, &pool->conn_list_, &pool->conn_list_.free_list_);
  TBSYS_LOG(DEBUG, "free list has %d connection before get", pool->conn_list_.free_list_.size_);
  TBSYS_LOG(DEBUG, "used list has %d connection before get", pool->conn_list_.used_list_.size_);
  node = pool->conn_list_.free_list_.head_;
  //conn = ob_sql_list_get_first(&pool->conn_list_.free_conn_list_, ObSQLConn, conn_list_node_);
  /* end TODO */
  if (NULL != node)
  {
    if (0 == pthread_mutex_lock(&pool->mutex_))
    {
      //ob_sql_list_get(conn, &pool->free_conn_list_, conn_list_node_, r);
      ob_sql_list_del(&pool->conn_list_.free_list_, node);
      ob_sql_list_add_tail(&pool->conn_list_.used_list_, node);
      TBSYS_LOG(DEBUG, "uesd list has %d connection after get", pool->conn_list_.used_list_.size_);
      TBSYS_LOG(DEBUG, "free list has %d connection after get", pool->conn_list_.free_list_.size_);
      pthread_mutex_unlock(&pool->mutex_);
    }
    else
    {
      TBSYS_LOG(ERROR, "lock pool->mutex_ %p failed, code id %d, mesg is %s", &pool->mutex_, errno, strerror(errno));
    }
  }
  else
  {
    TBSYS_LOG(DEBUG, "ob_sql_list_get_first(pool(%p) free conn list) is null", pool);
  }
  TBSYS_LOG(DEBUG, "default con select get node is %p", node);
  conn = (ObSQLConn*)node->data_;
  TBSYS_LOG(DEBUG, "default con select get con is %p", conn);
  return conn;
}
