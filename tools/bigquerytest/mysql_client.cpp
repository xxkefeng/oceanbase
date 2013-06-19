/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: mysql_api.cpp,v 0.1 2012/02/23 17:27:43 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include "mysql_client.h"

namespace
{
  const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;
}

MysqlClient::MysqlClient()
{
  key_ = INVALID_THREAD_KEY;
  stmt_key_ = INVALID_THREAD_KEY;
  //port_ = 0;
  //memset(host_, 0x00, sizeof(host_));
  addr_num_ = 0;
  create_thread_key();
  memset(mysql_user_, 0x00, sizeof(mysql_user_));
  memset(mysql_pass_, 0x00, sizeof(mysql_pass_));
  memset(mysql_db_, 0x00, sizeof(mysql_db_));
}

MysqlClient::~MysqlClient()
{
  close();
}

//int MysqlClient::init(char* host, int port, char* mysql_user, char* mysql_pass, char* mysql_db)
int MysqlClient::init(const ServerAddr* addr_ary, int64_t addr_num, char* mysql_user, char* mysql_pass, char* mysql_db)
{
  int err = OB_SUCCESS;

  if (NULL == addr_ary || addr_num <= 0 || NULL == mysql_user || NULL == mysql_pass || NULL == mysql_db)
  {
    TBSYS_LOG(WARN, "invalid param, addr_ary=%p, addr_num=%ld, mysql_user=%p, mysql_pass=%p, mysql_db=%p",
        addr_ary, addr_num, mysql_user, mysql_pass, mysql_db);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    //port_ = port;
    //strcpy(host_, host);
    addr_num_ = addr_num;
    for (int64_t i = 0; i < addr_num; ++i)
    {
      addr_ary_[i].port = addr_ary[i].port;
      strcpy(addr_ary_[i].host, addr_ary[i].host);
    }
    strcpy(mysql_user_, mysql_user);
    strcpy(mysql_pass_, mysql_pass);
    strcpy(mysql_db_, mysql_db);
  }

  return err;
}

int MysqlClient::close()
{
  void* ptr = pthread_getspecific(key_);
  if (NULL != ptr)
  {
    mysql_close((MYSQL*) ptr);
    ptr = NULL;
  }
  delete_thread_key();

  return 0;
}

int MysqlClient::exec_query(char* query_sql, Array& res)
{
  int err = OB_SUCCESS;

  MYSQL* conn = get_conn();
  assert(query_sql != NULL && query_sql[0] != '\0');
  assert(NULL != conn);

  err = mysql_query(conn, query_sql);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to exec query, query_sql=%s, err_msg=%s", query_sql, mysql_error(conn));
    err = OB_ERROR;
  }
  else
  {
    MYSQL_RES* result = mysql_store_result(conn);
    MYSQL_ROW row;
    int row_num = static_cast<int> (mysql_num_rows(result));
    int col_num = mysql_num_fields(result);
    err = res.init(row_num, col_num);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "failed to init array, row_num=%d, col_num=%d, err=%d",
          row_num, col_num, err);
    }
    else
    {
      int row_idx = 0;
      int type = 0;

      while (NULL != (row = mysql_fetch_row(result)))
      {
        mysql_field_seek(result, 0);
        for (int col_idx = 0; col_idx < col_num; ++col_idx)
        {
          MYSQL_FIELD* field = mysql_fetch_field(result);
          assert(NULL != field);
          switch (field->type)
          {
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_LONGLONG:
                type = Array::INT_TYPE;
                break;
            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_DOUBLE:
            case MYSQL_TYPE_NEWDECIMAL:
            case MYSQL_TYPE_DECIMAL:
                type = Array::FLOAT_TYPE;
                break;
            case MYSQL_TYPE_DATE:
            case MYSQL_TYPE_TIME:
            case MYSQL_TYPE_DATETIME:
                type = Array::DATETIME_TYPE;
                break;
            case MYSQL_TYPE_STRING:
            case MYSQL_TYPE_VAR_STRING:
                type = Array::VARCHAR_TYPE;
                break;
            default:
                TBSYS_LOG(ERROR, "invalid type, type=%d", field->type);
                err = OB_ERROR;
                break;
          }
          res.set_value(row_idx, col_idx, row[col_idx], type);
        }
        ++row_idx;
      }
    }

    mysql_free_result(result);
  }

  //TBSYS_LOG(INFO, "[MYSQL:exec_query]: sql=%s, err=%d", query_sql, err);
  return err;
}

int MysqlClient::exec_update(char* update_sql)
{
  int err = OB_SUCCESS;

  MYSQL* conn = get_conn();
  assert(NULL != update_sql && update_sql[0] != '\0');
  assert(NULL != conn);

  err = mysql_query(conn, update_sql);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to exec update, update_sql=%s, err_msg=%s", update_sql, mysql_error(conn));
    err = OB_ERROR;
  }

  TBSYS_LOG(INFO, "[MYSQL:exec_update]: sql=%s, err=%d", update_sql, err);
  return err;
}

int MysqlClient::stmt_prepare(const char* stmt_str, const int64_t length)
{
  int err = 0;
  assert(stmt_str != NULL && length > 0);
  MYSQL_STMT* stmt = get_stmt();
  assert(NULL != stmt);
  err = mysql_stmt_prepare(stmt, stmt_str, length);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to prepare stmt, stmt_str=%.*s, err=%d",
        static_cast<int>(length), stmt_str, err);
  }

  TBSYS_LOG(DEBUG, "[MYSQL:stmt_prepare]: stmt=%.*s, err=%d", static_cast<int> (length), stmt_str, err);
  return err;
}

int MysqlClient::stmt_bind_param(MYSQL_BIND* bind, const int64_t length)
{
  int err = 0;
  assert(NULL != bind && length > 0);
  MYSQL_STMT* stmt = get_stmt();
  assert(NULL != stmt);
  char buff[10240];
  buff[0] = '\0';
  char tmp_buff[10240];
  tmp_buff[0] = '\0';

  for (int64_t i = 0; i < length && 0 == err; ++i)
  {
    if (i == 0)
    {
      sprintf(tmp_buff, "%ld", *((int64_t*) bind[i].buffer));
    }
    else
    {
      sprintf(tmp_buff, ", %ld", *((int64_t*) bind[i].buffer));
    }
    strcat(buff, tmp_buff);
  }

  err = mysql_stmt_bind_param(stmt, bind);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to bind param, err=%d", err);
  }

  //TBSYS_LOG(INFO, "[MYSQL::stmt_bind_param]: stmt=%p, param=%s, err=%d", stmt, buff, err);
  return err;
}

int MysqlClient::stmt_execute()
{
  int err = 0;
  MYSQL_STMT* stmt = get_stmt();
  assert(NULL != stmt);
  
  unsigned int index = 0;
  TBSYS_LOG(DEBUG, "stmt=%p, param count is %u", stmt, stmt->param_count);
  for (; index < stmt->param_count; ++index)
  {
    TBSYS_LOG(DEBUG, "%u param type is %d", index, stmt->params[index].buffer_type);
  }
  err = mysql_stmt_execute(stmt);
  if (0 != err)
  {
    TBSYS_LOG(WARN, "failed to execute stmt, err=%d, mysql_error=%s", err, mysql_stmt_error(stmt));
  }

  return_stmt();

  TBSYS_LOG(DEBUG, "[MYSQL:stmt_execute]: err=%d", err);
  return err;
}

int MysqlClient::connect()
{
  int err = OB_SUCCESS;
  assert(addr_num_ > 0);

  MYSQL* conn = mysql_init(NULL);
  if (NULL == conn)
  {
    TBSYS_LOG(WARN, "failed to init conn");
    err = OB_ERROR;
  }
  else
  {
    int64_t idx = rand() % addr_num_;
    if (NULL == mysql_real_connect(conn, addr_ary_[idx].host, mysql_user_, mysql_pass_, mysql_db_,
          addr_ary_[idx].port, NULL, 0))
    {
      TBSYS_LOG(WARN, "failed to connect, host_=%s, port_=%d, mysql_user_=%s, mysql_pass=%s, mysql_db=%s, err_msg=%s",
          addr_ary_[idx].host, addr_ary_[idx].port, mysql_user_, mysql_pass_, mysql_db_, mysql_error(conn));
      err = OB_ERROR;
    }
    else
    {
      // TBSYS_LOG(INFO, "connect succ, idx=%ld, host_=%s, port_=%d, mysql_user_=%s, mysql_pass=%s, mysql_db=%s",
      //    idx, addr_ary_[idx].host, addr_ary_[idx].port, mysql_user_, mysql_pass_, mysql_db_);
      err = pthread_setspecific(key_, (void*) conn);
      if (0 != err)
      {
        TBSYS_LOG(WARN, "pthread_setspecific failed, err=%d", err);
      }
    }
  }

  return err;
}

int MysqlClient::create_thread_key()
{
  int ret = pthread_key_create(&key_, destroy_thread_key);
  if (0 != ret)
  {
    TBSYS_LOG(ERROR, "cannot create thread key:%d", ret);
  }
  else
  {
    ret = pthread_key_create(&stmt_key_, destroy_thread_stmt_key);
    if (0 != ret)
    {
      TBSYS_LOG(ERROR, "cannot create thread stmt key:%d", ret);
    }
  }
  return (0 == ret) ? OB_SUCCESS : OB_ERROR;
}

int MysqlClient::delete_thread_key()
{
  int ret = -1;
  if (INVALID_THREAD_KEY != key_)
  {
    ret = pthread_key_delete(key_);
  }
  if (0 != ret)
  {
    TBSYS_LOG(WARN, "delete thread key key_ failed.");
  }
  else
  {
    if (INVALID_THREAD_KEY != stmt_key_)
    {
      ret = pthread_key_delete(stmt_key_);
    }
  }
  return (0 == ret) ? OB_SUCCESS : OB_ERROR;
}

void MysqlClient::destroy_thread_key(void* ptr)
{
  TBSYS_LOG(DEBUG, "delete thread specific ptr:%p", ptr);
  if (NULL != ptr)
  {
    mysql_close((MYSQL*) ptr);
    ptr = NULL;
  }
}

void MysqlClient::destroy_thread_stmt_key(void* ptr)
{
  TBSYS_LOG(DEBUG, "delete thread stmt key:%p", ptr);
  if (NULL != ptr)
  {
    mysql_stmt_close((MYSQL_STMT*) ptr);
    ptr = NULL;
  }
}


MYSQL_STMT* MysqlClient::get_stmt()
{
  void* stmt_ptr = pthread_getspecific(stmt_key_);
  if (NULL == stmt_ptr)
  {
    void* ptr = pthread_getspecific(key_);
    if (NULL == ptr)
    {
      int err = connect();
      if (0 != err)
      {
        TBSYS_LOG(WARN, "failed to connect, err=%d", err);
      }
      else
      {
        ptr = pthread_getspecific(key_);
      }
    }

    MYSQL* conn = (MYSQL*) ptr;

    stmt_ptr = mysql_stmt_init(conn);
    //TBSYS_LOG(INFO, "stmt_init, conn=%p, stmt_ptr=%p", conn, stmt_ptr);
    if (NULL == stmt_ptr)
    {
      TBSYS_LOG(WARN, "failed to init stmt");
    }
    else
    {
      pthread_setspecific(stmt_key_, (void*) stmt_ptr);
    }
  }
  
  return (MYSQL_STMT*) stmt_ptr;
}

void MysqlClient::return_stmt()
{
  void* stmt_ptr = pthread_getspecific(stmt_key_);
  if (NULL != stmt_ptr)
  {
    mysql_stmt_close((MYSQL_STMT*) stmt_ptr);
    //TBSYS_LOG(INFO, "stmt_close, stmt_ptr=%p", stmt_ptr);
    stmt_ptr = NULL;
    pthread_setspecific(stmt_key_, NULL);
  }

  void* conn_ptr = pthread_getspecific(key_);
  if (NULL != conn_ptr)
  {
    mysql_close((MYSQL*) conn_ptr);
    //TBSYS_LOG(INFO, "conn_close, conn_ptr=%p", conn_ptr);
    conn_ptr = NULL;
    pthread_setspecific(key_, NULL);
  }
}

MYSQL* MysqlClient::get_conn()
{
  void* ptr = pthread_getspecific(key_);
  if (NULL == ptr)
  {
    int err = connect();
    if (0 != err)
    {
      TBSYS_LOG(WARN, "failed to connect, err=%d", err);
    }
    else
    {
      ptr = pthread_getspecific(key_);
    }
  }
  
  return (MYSQL*) ptr;
}


