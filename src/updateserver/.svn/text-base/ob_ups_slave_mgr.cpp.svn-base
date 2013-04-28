/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ob_ups_slave_mgr.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
using namespace oceanbase::updateserver;

static const int64_t DEFAULT_NETWORK_TIMEOUT = 1000 * 1000;

ObUpsSlaveMgr::ObUpsSlaveMgr()
{
  is_initialized_ = false;
  n_slave_last_post_ = -1;
  slave_num_ = 0;
  rpc_stub_ = NULL;
  role_mgr_ = NULL;
}

ObUpsSlaveMgr::~ObUpsSlaveMgr()
{
  ServerNode* node = NULL;
  ObDLink* p = slave_head_.server_list_link.next();
  while (p != &slave_head_.server_list_link)
  {
    node = (ServerNode*)p;
    p = p->next();
    p->prev()->remove();
    ob_free(node);
  }
}

int ObUpsSlaveMgr::init(ObUpsRoleMgr *role_mgr,
    ObCommonRpcStub *rpc_stub, int64_t log_sync_timeout)
{
  int err = OB_SUCCESS;
  if (NULL == role_mgr || NULL == rpc_stub)
  {
    err = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid argument, role_mgr_=%p, rpc_stub=%p", role_mgr, rpc_stub);
  }
  else
  {
    log_sync_timeout_ = log_sync_timeout;
    role_mgr_ = role_mgr;
    rpc_stub_ = rpc_stub;
    is_initialized_ = true;
  }
  return err;
}

int ObUpsSlaveMgr::set_log_sync_timeout_us(const int64_t timeout)
{
  return ObSlaveMgr::set_log_sync_timeout_us(timeout);
}

int ObUpsSlaveMgr::send_data(const char* data, const int64_t length)
{
  int ret = check_inner_stat();
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (tmp_ret = post_log_to_slave(data, length)))
    {
      ret = tmp_ret;
      TBSYS_LOG(ERROR, "post_log_to_slave(data=%p[%ld])=>%d", data, length, ret);
    }
    if (OB_SUCCESS != (tmp_ret = wait_post_log_to_slave(data, length)))
    {
      ret = tmp_ret;
      TBSYS_LOG(ERROR, "wait_post_log_to_slave(data=%p[%ld])=>%d", data, length, ret);
    }
  }
  return ret;
}

int ObUpsSlaveMgr::post_log_to_slave(const char* data, const int64_t length)
{
  int ret = check_inner_stat();
  int send_err = OB_SUCCESS;
  ObDataBuffer send_buf;
  const ObClientManager* client_mgr = rpc_stub_->get_client_mgr();

  if (0 <= n_slave_last_post_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "n_slave[%ld] >= 0, maybe you post_log but not wait", n_slave_last_post_);
  }
  if (OB_SUCCESS == ret)
  {
    if (NULL == client_mgr)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "client_mgr == NULL");
    }
    else if (NULL == data || length < 0)
    {
      TBSYS_LOG(ERROR, "parameters are invalid[data=%p length=%ld]", data, length);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      send_buf.set_data(const_cast<char*>(data), length);
      send_buf.get_position() = length;
    }
  }

  slave_info_mutex_.lock();
  n_slave_last_post_ = 0;

  if (OB_SUCCESS == ret)
  {
    ServerNode* slave_node = NULL;
    ObDLink* p = slave_head_.server_list_link.next();
    while (OB_SUCCESS == ret && p != NULL && p != &slave_head_.server_list_link)
    {
      if (n_slave_last_post_ >= MAX_SLAVE_NUM)
      {
        TBSYS_LOG(ERROR, "too many slaves[%d], MAX_SLAVE_NUM=%ld", slave_num_, MAX_SLAVE_NUM);
        break;
      }
      slave_node = (ServerNode*)(p);
      reqs_[n_slave_last_post_].p_slave_node_ = p;
      send_err = client_mgr->post_request(slave_node->server, OB_SEND_LOG, RPC_VERSION, log_sync_timeout_,
                                           send_buf, ObClientWaitObj::on_receive_response, &reqs_[n_slave_last_post_].wait_obj_);
      reqs_[n_slave_last_post_].wait_obj_.reset(send_err);
      if (OB_SUCCESS != send_err)
      {
        TBSYS_LOG(ERROR, "post_request to slave[%s] buf=%p[%ld], failed, err=%d",
                  to_cstring(slave_node->server), data, length, send_err);
      }
      p = p->next();
      n_slave_last_post_++;
    }

    if (NULL == p)
    {
      TBSYS_LOG(ERROR, "Server list encounter NULL pointer, this should not be reached");
      ret = OB_ERROR;
    }
  }
  return ret;
}
  
int ObUpsSlaveMgr::wait_post_log_to_slave(const char* data, const int64_t length)
{
  int ret = OB_SUCCESS;
  int send_err = OB_SUCCESS;
  int64_t failed_count = 0;
  int64_t timeout_delta_us = 10 * 1000;
  ServerNode* slave_node = NULL;
  if (0 > n_slave_last_post_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "n_slave[%ld] < 0, maybe not post data", n_slave_last_post_);
  }
  for(int64_t i = 0; OB_SUCCESS == ret && i < n_slave_last_post_; i++)
  {
    ObDataBuffer res;
    if (OB_SUCCESS != (send_err = reqs_[i].wait_obj_.wait(res, log_sync_timeout_ + timeout_delta_us)))
    {
      TBSYS_LOG(WARN, "reqs[%ld].wait()=>%d", i, send_err );
    }
    else
    {
      ObResultCode result_code;
      int64_t pos = 0;
      if (OB_SUCCESS != (send_err = result_code.deserialize(res.get_data(), res.get_position(), pos)))
      {
        TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], err[%d].", pos, send_err);
      }
      else
      {
        send_err = result_code.result_code_;
      }
    }

    if (OB_SUCCESS != send_err)
    {
      failed_count++;
      slave_node = (ServerNode*)(reqs_[i].p_slave_node_);
      TBSYS_LOG(WARN, "send_data to slave[%s], buf=%p[%ld], error[err=%d]", to_cstring(slave_node->server), data, length, send_err);

      //add: report to rootserver
      send_err = rpc_stub_->ups_report_slave_failure(slave_node->server, DEFAULT_NETWORK_TIMEOUT);
      if (OB_SUCCESS != send_err)
      {
        TBSYS_LOG(WARN, "fail to report slave failure. err = %d", send_err);
      }
      ObDLink *to_del = (ObDLink*)slave_node;
      to_del->remove();
      slave_num_ --;
      ob_free(slave_node);
    }
  } // end of loop
  slave_info_mutex_.unlock();
  n_slave_last_post_ = -1;
  if (OB_SUCCESS == ret && failed_count > 0)
  {
    ret = OB_PARTIAL_FAILED;
    TBSYS_LOG(ERROR, "send log to %ld slave failed", failed_count);
  }
  return ret;
}

int ObUpsSlaveMgr::grant_keep_alive()
{
  int err = OB_SUCCESS;
  ServerNode* slave_node = NULL;
  ObDLink *p = slave_head_.server_list_link.next();
  timeval time_val;
  gettimeofday(&time_val, NULL);
  int64_t cur_time_us = time_val.tv_sec * 1000 * 1000 + time_val.tv_usec;
  while(p != NULL && p != &slave_head_.server_list_link)
  {
    slave_node = (ServerNode*)(p);
    TBSYS_LOG(DEBUG, "send keep alive msg to slave[%s], time=%ld", slave_node->server.to_cstring(), cur_time_us);
    err = rpc_stub_->send_keep_alive(slave_node->server);
    if (OB_SUCCESS != err)
    {
      TBSYS_LOG(WARN, "fail to send keep alive msg to slave[%s], err = %d", slave_node->server.to_cstring(), err);
    }
    p = p->next();
  }
  return err;
}

int ObUpsSlaveMgr::add_server(const ObServer &server)
{
  return ObSlaveMgr::add_server(server);
}
int ObUpsSlaveMgr::delete_server(const ObServer &server)
{
  return ObSlaveMgr::delete_server(server);
}

int ObUpsSlaveMgr::reset_slave_list()
{
  return ObSlaveMgr::reset_slave_list();
}
int ObUpsSlaveMgr::get_num() const
{
  return ObSlaveMgr::get_num();
}
int ObUpsSlaveMgr::set_send_log_point(const ObServer &server, const uint64_t send_log_point)
{
  return ObSlaveMgr::set_send_log_point(server, send_log_point);
}
void ObUpsSlaveMgr::print(char *buf, const int64_t buf_len, int64_t& pos)
{
  return ObSlaveMgr::print(buf, buf_len, pos);
}
