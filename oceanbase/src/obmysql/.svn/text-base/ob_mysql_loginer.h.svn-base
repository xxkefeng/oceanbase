/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_loginer.h is for what ...
 *
 * Version: ***: ob_mysql_loginer.h  Tue Jul 17 13:56:29 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_LOGINER_H_
#define OB_MYSQL_LOGINER_H_

#include "easy_io_struct.h"
#include "common/ob_define.h"
#include "common/thread_buffer.h"
#include "common/ob_malloc.h"
#include "common/ob_privilege_manager.h"
#include "common/ob_string.h"
#include "sql/ob_sql_session_info.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLServer;
    class ObMySQLLoginer
    {
      public:
        static const int32_t BUFFER_SIZE = 1024 * 1024 * 2; //2MB net buffer
      public:
        struct LoginInfo
        {
          LoginInfo();
          uint32_t capability_flags_;
          uint32_t max_packet_size_;
          uint8_t character_set_;
          common::ObString user_name_;
          common::ObString db_name_;
          common::ObString auth_response_;
        };
        ObMySQLLoginer();
        ~ObMySQLLoginer();
        void set_obmysql_server(ObMySQLServer *server);
        /**
         * Perform handshake, authorize client
         * @param c   connection to authorize
         *
         */
        int login(easy_connection_t* c, sql::ObSQLSessionInfo *& session);

        void set_privilege_manager(common::ObPrivilegeManager *privilege_mgr);



      private:
        common::ThreadSpecificBuffer::Buffer* get_buffer() const;

        /**
         * send handshake packet to client
         * @param
         *
         */
        int handshake(easy_connection_t* c);

        /**
         * read client auth packet from c->fd
         * do nothing
         * @param c   connection to read data
         *
         */
        int parse_packet(easy_connection_t* c);

        /**
         * send ok packet to client
         * @param c
         *
         */
        int check_privilege(easy_connection_t* c, sql::ObSQLSessionInfo *& session);

        int insert_new_session(easy_connection_t* c, sql::ObSQLSessionInfo *& session);

        /**
         * write data through raw socket
         * just used to send handshake && ok/error to client
         * @param fd       socket handler
         * @param buffer   data to send
         * @param length   length of data
         */
        int write_data(int fd, char* buffer, size_t length);

        int read_data(int fd, char* buffer, size_t length);
      private:
        common::ThreadSpecificBuffer buffer_;  //This buffer used to read/write data during handsheking
        common::ObPrivilegeManager *privilege_mgr_;
        LoginInfo login_info_;
        ObMySQLServer *server_;
    };
  }
}
#endif
