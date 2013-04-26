/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-03-08 14:48:14 fufeng.syd>
 * Version: $Id$
 * Filename: ob_root_server_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#include "ob_root_server_config.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

int ObRootServerConfig::get_root_server(ObServer &server) const
{
  int ret = OB_SUCCESS;
  if (!server.set_ipv4_addr(root_server_ip, (int32_t)port))
  {
    TBSYS_LOG(WARN, "Bad rootserver address! ip: [%s], port: [%s]",
              root_server_ip.str(), port.str());
    ret = OB_BAD_ADDRESS;
  }
  return ret;
}

int ObRootServerConfig::get_master_root_server(ObServer &server) const
{
  int ret = OB_SUCCESS;
  if (!server.set_ipv4_addr(master_root_server_ip, (int32_t)master_root_server_port))
  {
    TBSYS_LOG(WARN, "Bad rootserver address! ip: [%s], port: [%s]",
              master_root_server_ip.str(), master_root_server_port.str());
    ret = OB_BAD_ADDRESS;
  }
  return ret;
}

// int ObRootServerConfig::reload_client()
// {
//   int ret = OB_SUCCESS;
//   ObSystemConfigKey key;

//   /* for macro tricks */
//   int64_t BNL_alpha_ = 0;
//   int64_t BNL_alpha_denominator_ = 0;
//   int64_t BNL_threshold_ = 0;
//   int64_t BNL_threshold_denominator_ = 0;
//   int32_t obi_count_ = 0;

//   READ_INT_CONFIG(BNL_alpha, ObClientConfig::DEFAULT_BNL_ALPHA);
//   READ_INT_CONFIG(BNL_alpha_denominator,
//                   ObClientConfig::DEFAULT_BNL_ALPHA_DENOMINATOR);
//   READ_INT_CONFIG(BNL_threshold, ObClientConfig::DEFAULT_BNL_THRESHOLD);
//   READ_INT_CONFIG(BNL_threshold_denominator,
//                   ObClientConfig::DEFAULT_BNL_THRESHOLD_DENOMINATOR);

//   client_config_.BNL_alpha_ = BNL_alpha_;
//   client_config_.BNL_alpha_denominator_ = BNL_alpha_denominator_;
//   client_config_.BNL_threshold_ = BNL_threshold_;
//   client_config_.BNL_threshold_denominator_ = BNL_threshold_denominator_;

//   READ_INT32_CONFIG(obi_count, 0);
//   client_config_.obi_list_.obi_count_ = obi_count_;

//   int32_t loaded_count = 0;
//   for (int32_t i = 1;
//        OB_SUCCESS == ret
//          && i <= obi_count_ && i <= client_config_.obi_list_.MAX_OBI_COUNT;
//        i++)
//   {
//     key.set_int(ObString::make_string("cluster_id"), i);
//     char vip[OB_IP_STR_BUFF];
//     int32_t port = 0;
//     int32_t read_percentage = 0;
//     int32_t random_ms = 0;
//     if (OB_SUCCESS == ret)
//     {
//       key.set_name("read_percentage");
//       if (OB_SUCCESS !=
//           (ret = system_config_->read_int32(key, read_percentage, -1)))
//       {
//         TBSYS_LOG(ERROR, "read read_percentage error, ret: [%d]", ret);
//       }
//     }
//     if (OB_SUCCESS == ret)
//     {
//       key.set_name("random_ms");
//       if (OB_SUCCESS != (ret = system_config_->read_int32(key, random_ms, -1)))
//       {
//         TBSYS_LOG(ERROR, "read random ms error, ret: [%d]", ret);
//       }
//     }
//     if (OB_SUCCESS == ret)
//     {
//       key.set_name("vip");
//       if (OB_SUCCESS !=
//           (ret = system_config_->read_str(key, vip, sizeof vip, "")))
//       {
//         TBSYS_LOG(ERROR, "read vip error, ret: [%d]", ret);
//       }
//     }
//     if (OB_SUCCESS == ret)
//     {
//       key.set_name("port");
//       if (OB_SUCCESS != system_config_->read_int32(key, port, 0))
//       {
//         TBSYS_LOG(ERROR, "read port error, ret: [%d]", ret);
//       }
//     }
//     if (OB_SUCCESS == ret)
//     {
//       ObServer rs_addr;
//       if (!rs_addr.set_ipv4_addr(vip, port))
//       {
//         TBSYS_LOG(ERROR, "invalid rs addr, addr=%s port=%d", vip, port);
//         ret = OB_INVALID_ARGUMENT;
//       }
//       else if (0 > read_percentage || 100 < read_percentage)
//       {
//         TBSYS_LOG(ERROR, "invalid obi read_percentage=%d", read_percentage);
//         ret = OB_INVALID_ARGUMENT;
//       }
//       else
//       {
//         ObiConfigEx &obi_conf = client_config_.obi_list_.conf_array_[i];
//         obi_conf.set_rs_addr(rs_addr);
//         obi_conf.set_read_percentage(read_percentage);
//         if (0 != random_ms)
//         {
//           obi_conf.set_flag_rand_ms();
//         }
//         else
//         {
//           obi_conf.unset_flag_rand_ms();
//         }
//         loaded_count++;
//       }
//     }
//   }
//   if (obi_count_ != loaded_count)
//   {
//     TBSYS_LOG(WARN, "config specify %d obi, but only %d loaded",
//               obi_count_, loaded_count);
//   }
//   client_config_.obi_list_.obi_count_ = loaded_count;
//   return ret;
// }
