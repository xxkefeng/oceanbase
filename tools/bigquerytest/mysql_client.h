/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: mysql_api.h,v 0.1 2012/02/22 13:43:19 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#ifndef __OCEANBASE_MYSQL_API_H__
#define __OCEANBASE_MYSQL_API_H__

#include "util.h"
#include <mysql/mysql.h>

class MysqlClient
{
  public:
    MysqlClient();
    ~MysqlClient();

    //int init(char* host, int port, char* mysql_user, char* mysql_pass, char* mysql_db);
    int init(const ServerAddr* addr_ary, int64_t addr_num, char* mysql_user, char* mysql_class, char* mysql_db);
    int close();

  public:
    int exec_query(char* query_sql, Array& res);
    int exec_update(char* update_sql);

  public:
    int stmt_prepare(const char* stmt_str, const int64_t length);
    int stmt_bind_param(MYSQL_BIND* bind, const int64_t length);
    int stmt_execute();

  private:
    int connect();
    MYSQL* get_conn();
    MYSQL_STMT* get_stmt();
    void return_stmt();
    int create_thread_key();
    int delete_thread_key();
    static void destroy_thread_key(void* ptr);
    static void destroy_thread_stmt_key(void* ptr);

  private:
    //char host_[128];
    //int port_;
    ServerAddr addr_ary_[100];
    int64_t addr_num_;
    pthread_key_t key_;
    pthread_key_t stmt_key_;
    char mysql_user_[128];
    char mysql_pass_[128];
    char mysql_db_[128];
};

#endif //__MYSQL_API_H__

