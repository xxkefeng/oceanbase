/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_packet.h is for what ...
 *
 * Version: ***: ob_mysql_packet.h  Tue Jul 31 15:14:12 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_MYSQL_PACKET_H_
#define OB_MYSQL_PACKET_H_

#include "ob_mysql_packet_header.h"

namespace oceanbase
{
  namespace obmysql
  {
    class ObMySQLPacket
    {
      public:
        ObMySQLPacket() { }
        virtual ~ObMySQLPacket() { }
        virtual int encode(char* buffer, int64_t length, int64_t& pos);
      public:
        const static int64_t MAX_STORE_LENGTH = 9; /* max bytes needed when store_length */

      public:
        virtual int serialize(char* buffer, int64_t len, int64_t& pos) = 0;

        /**
         *  不是准确的序列化(packey payload)需要的长度
         *  为了简化计算 变长序列化数字的时候都假设需要9bytes
         *  不包括MySQL包头需要的4bytes
         */
        virtual uint64_t get_serialize_size() = 0;

        void set_seq(uint8_t seq)
        {
          header_.seq_ = seq;
        }
      private:
        ObMySQLPacketHeader header_;
    };
  }
}
#endif
