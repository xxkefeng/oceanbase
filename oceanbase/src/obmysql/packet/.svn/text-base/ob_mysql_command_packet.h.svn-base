/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_command_packet.h is for what ...
 *
 * Version: ***: ob_mysql_command_packet.h  Wed Jul 18 10:46:14 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_COMMAND_PACKET_H_
#define OB_MYSQL_COMMAND_PACKET_H_

#include "common/data_buffer.h"
#include "ob_mysql_packet_header.h"
#include "easy_io_struct.h"
#include "common/ob_define.h"
#include "common/ob_string.h"

namespace oceanbase
{
  namespace tests
  {
    class TestObMySQLCommandPacket;
  }

  namespace obmysql
  {
    class ObMySQLCommandPacket
    {
      friend class ObMySQLCommandQueue;
      friend class tests::TestObMySQLCommandPacket;
    public:
      ObMySQLCommandPacket();
      ~ObMySQLCommandPacket();

      common::ObString& get_command()
      {
        return command_;
      }

      const ObMySQLPacketHeader& get_packet_header() const
      {
        return header_;
      }

      inline void set_header(uint32_t pkt_length, uint8_t pkt_seq)
      {
        header_.length_ = pkt_length;
        header_.seq_ = pkt_seq;
      }

      inline void set_type(uint8_t type)
      {
        type_ = type;
      }

      inline uint8_t get_type() const
      {
        return type_;
      }

      int set_request(easy_request_t* req);

      void set_command(char* command, const int32_t length);

      inline easy_request_t* get_request() const
      {
        return req_;
      }

      int32_t get_command_length() const;
      
      void set_receive_ts(const int64_t &now);
      int64_t get_receive_ts() const;

    private:
      ObMySQLPacketHeader header_;
      uint8_t type_;
      common::ObString command_;
      easy_request_t* req_;                 //request pointer for send response
      ObMySQLCommandPacket* next_;
      int64_t receive_ts_;
    };
  }
}
#endif
