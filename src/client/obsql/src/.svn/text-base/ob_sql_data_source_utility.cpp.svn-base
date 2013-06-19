#include "common/ob_define.h"
#include "ob_sql_data_source_utility.h"
#include "ob_sql_util.h"
#include "ob_sql_global.h"
#include "ob_sql_conn_acquire.h"
#include "common/ob_malloc.h"
#include "tblog.h"

using namespace oceanbase::common;
static int copy_ds(ObDataSource *dest, ObDataSource *src)
{
  int ret = OB_SQL_SUCCESS;
  if (NULL == dest || NULL == src)
  {
    TBSYS_LOG(ERROR, "invalid argument dest is %p, src is %p", dest, src);
    ret = OB_SQL_ERROR;
  }
  else
  {
    ObSQLConn *cconn = NULL;
    ObSQLConn *nconn = NULL;
    dest->cluster_ = src->cluster_;
    dest->server_ = src->server_;
    //for conn from src to dest
    ob_sql_list_init(&dest->conn_list_.free_conn_list_);
    ob_sql_list_init(&dest->conn_list_.used_conn_list_);
    ob_sql_list_for_each_entry_safe(cconn, nconn, &src->conn_list_.used_conn_list_, conn_list_node_)
    {
      ob_sql_list_del(&cconn->conn_list_node_);
      ob_sql_list_add_tail(&cconn->conn_list_node_, &dest->conn_list_.used_conn_list_);
    }
    ob_sql_list_for_each_entry_safe(cconn, nconn, &src->conn_list_.free_conn_list_, conn_list_node_)
    {
      ob_sql_list_del(&cconn->conn_list_node_);
      ob_sql_list_add_tail(&cconn->conn_list_node_, &dest->conn_list_.free_conn_list_);
    }
  }
  return ret;
}
/**
 * 在连接池里面创建一条新的连接
 */
int create_real_connection(ObDataSource *pool)
{
  int ret = OB_SQL_SUCCESS;
  ObSQLConn *conn = reinterpret_cast<ObSQLConn *>(ob_malloc(sizeof(ObSQLConn)));
  if (NULL == conn)
  {
    TBSYS_LOG(ERROR, "ob_malloc mem for ObSQLConn failed");
    ret = OB_SQL_ERROR;
  }
  else
  {
    memset(conn, 0, sizeof(ObSQLConn));
    char ipbuffer[OB_SQL_IP_BUFFER_SIZE];
    ret = get_server_ip(&pool->server_, ipbuffer, OB_SQL_IP_BUFFER_SIZE);
    if (ret != OB_SQL_SUCCESS)
    {
      TBSYS_LOG(ERROR,"can not get server ip address server(%u, %u)", pool->server_.ip_, pool->server_.port_);
      ob_free(conn);
      conn = NULL;
    }
    else
    {
      conn->mysql_ = (*g_func_set.real_mysql_init)(NULL);
      if (NULL == conn->mysql_)
      {
        TBSYS_LOG(ERROR, "init mysql handler failed");
        ret = OB_SQL_ERROR;
      }
      else
      {
        conn->mysql_ = (*g_func_set.real_mysql_real_connect)(conn->mysql_,
                                                                  ipbuffer, g_sqlconfig.username_, g_sqlconfig.passwd_,
                                                                  OB_SQL_DB, pool->server_.port_, NULL, 0);
        if (NULL == conn->mysql_)
        {
          TBSYS_LOG(ERROR, "failed to connect to server %s, Error: %s", get_server_str(&(pool->server_)), (*g_func_set.real_mysql_error)(conn->mysql_));
           ret = OB_SQL_ERROR;
        }
        else
        {
          TBSYS_LOG(DEBUG, "creat conn is %p real mysql connection %p server is %s", conn, conn->mysql_, get_server_str(&pool->server_));
          conn->pool_ = pool;
          conn->cluster_ = pool->server_;
          //client version to server
          //(*g_func_set.real_mysql_query)(conn->mysql_, OB_SQL_CLIENT_VERSION);
          ob_sql_list_add_tail(&conn->conn_list_node_, &pool->conn_list_.free_conn_list_);
          TBSYS_LOG(DEBUG, "coredebug pool is %p, conn_list is %p  free_conn_list is %p", pool, &pool->conn_list_, &pool->conn_list_.free_conn_list_);
          TBSYS_LOG(DEBUG, "coredebug free list has %d connection", get_list_size(&pool->conn_list_.free_conn_list_));
          TBSYS_LOG(DEBUG, "coredebug used list has %d connection", get_list_size(&pool->conn_list_.used_conn_list_));
        }
      }
    }
  }
  return ret;
}

int init_data_source(int32_t conns, ObServerInfo server, ObClusterInfo *cluster)
{
  int ret = OB_SQL_SUCCESS;
  ObDataSource* ds = cluster->dslist_ + cluster->csize_;
  TBSYS_LOG(INFO, "cluster is %p, ds offset is %d", cluster, cluster->csize_);
  pthread_mutex_init(&(ds->mutex_), NULL);
  ds->cluster_ = cluster;
  ds->server_ = server;
  ob_sql_list_init(&ds->conn_list_.free_conn_list_);
  ob_sql_list_init(&ds->conn_list_.used_conn_list_);
  //创建真正的连接
  int32_t index = 0;
  for (; index < conns && OB_SQL_SUCCESS == ret; ++index)
  {
    ret = create_real_connection(ds);
    if (OB_SQL_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "create real connection to %s failed", get_server_str(&server));
      if (0 != ob_sql_list_empty(&ds->conn_list_.free_conn_list_))
      {
        ObSQLConn *conn = NULL;
        while(NULL != (conn = ob_sql_list_get_first(&ds->conn_list_.free_conn_list_, ObSQLConn, conn_list_node_)))
        {
          ob_sql_list_del(&conn->conn_list_node_);
          (*g_func_set.real_mysql_close)(conn->mysql_);
          ob_free(conn);
        }
      }
      break;
    }
  }
  if (OB_SQL_SUCCESS == ret)
  {
    TBSYS_LOG(DEBUG, "init data source successful");
    cluster->csize_++;
  }
  else
  {
    TBSYS_LOG(ERROR, "init data source failed");
  }
  return ret;
}

int delete_data_source(ObDataSource *ds)
{
  int ret = OB_SQL_SUCCESS;
  if (NULL == ds)
  {
    TBSYS_LOG(ERROR, "invalid argument datasource is %p", ds);
    ret = OB_SQL_ERROR;
  }
  else
  {
    if (NULL == ds->cluster_)
    {
      TBSYS_LOG(ERROR, "datasource is not inited, ds->cluster_ is null");
      ret = OB_SQL_ERROR;
    }
    else if (ds->cluster_->csize_ <= ds - ds->cluster_->dslist_)
    {
      TBSYS_LOG(ERROR, "datasource is not invalid, ds->cluster has only %d illegal datasource, but ds index is %ld",
                ds->cluster_->csize_, ds - ds->cluster_->dslist_);
      ret = OB_SQL_ERROR;
    }
    else
    {
      ObSQLConnList *list = reinterpret_cast<ObSQLConnList*>(ob_malloc(sizeof(ObSQLConnList)));
      move_conn_list(list, ds);
      TBSYS_LOG(DEBUG, "delete ms size is %d", get_list_size(&g_delete_ms_list));
      ob_sql_list_add_tail(&list->delete_list_node_, &g_delete_ms_list);
      int32_t start = static_cast<int32_t>(ds - ds->cluster_->dslist_);
      start++;
      for (; OB_SQL_SUCCESS == ret && start < ds->cluster_->size_; ++start)
      {
        ret = copy_ds(&ds->cluster_->dslist_[start - 1], &ds->cluster_->dslist_[start]);
        if (OB_SQL_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "copy data source info from %p to %p failed ret is %d", &ds->cluster_->dslist_[start],
                    &ds->cluster_->dslist_[start - 1], ret);
        }
      }
      TBSYS_LOG(DEBUG, "delete ms list has %d", get_list_size(&g_delete_ms_list));
      ds->cluster_->size_--;
      ds->cluster_->csize_--;
      TBSYS_LOG(DEBUG, "cluster size is %d, %d", ds->cluster_->size_, ds->cluster_->csize_);
      TBSYS_LOG(DEBUG, "move ds %s to delete list", get_server_str(&ds->server_));
    }
  }
  return ret;
}
