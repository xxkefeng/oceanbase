/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* ob_remote_sstable_scan.cpp is for ...
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/

#include "ob_remote_sstable_scan.h"
#include "common/utility.h"
#include "sstable/ob_sstable_scan_param.h"
#include "chunkserver/ob_sql_rpc_stub.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::chunkserver;


ObRemoteSSTableScan::ObRemoteSSTableScan() : scan_param_(NULL),
  rpc_stub_(NULL), timeout_(0), session_id_(0), session_created_(false)
{
}

ObRemoteSSTableScan::~ObRemoteSSTableScan()
{
}


inline bool ObRemoteSSTableScan::check_inner_stat(void)
{
  return (NULL != scan_param_ && NULL != rpc_stub_);
}

int ObRemoteSSTableScan::set_param( const sstable::ObSSTableScanParam &param,
    const chunkserver::ObSqlRpcStub &rpc_stub, const common::ObServer &server,
    const int64_t timeout)
{
  scan_param_ = &param;
  rpc_stub_ = &rpc_stub;
  chunk_server_ = server;
  timeout_ = timeout;

  return OB_SUCCESS;
}

int ObRemoteSSTableScan::set_child(int32_t child_idx, ObPhyOperator &child_op)
{
  UNUSED(child_idx);
  UNUSED(child_op);
  return OB_NOT_IMPLEMENT;
}

int64_t ObRemoteSSTableScan::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  if (NULL != buf && buf_len > 0)
  {
    databuff_printf(buf, buf_len, pos, "RemoteSSTableScan()\n");
  }
  return pos;
}

int ObRemoteSSTableScan::open()
{
  int rc = OB_SUCCESS;

  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check_inner_stat() fail, plz call set_scan_param() first.");
    rc = OB_ERROR;
  }
  else
  {
    scanner_.reuse();
    session_id_ = 0;
    session_created_ = false;
    row_desc_.reset();

    uint64_t table_id = scan_param_->get_table_id();
    for (int64_t i = 0; OB_SUCCESS == rc && i < scan_param_->get_column_id_size(); i++)
    {
      rc = row_desc_.add_column_desc(table_id, *scan_param_->get_column_id_list().at(i));
    }

    if (OB_SUCCESS == rc)
    {
      row_desc_.set_rowkey_cell_count(scan_param_->get_rowkey_column_count());
      row_.set_row_desc(row_desc_);
    }
  }

  return rc;
}

int ObRemoteSSTableScan::get_next_row(const ObRow *&row)
{
  int rc = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check_inner_stat() fail");
    rc = OB_ERROR;
  }

  if (OB_SUCCESS == rc && !session_created_)
  {
    scanner_.reuse();
    rc = rpc_stub_->sstable_scan(timeout_, chunk_server_, *scan_param_, scanner_, session_id_);
    if (OB_SUCCESS != rc)
    {
      TBSYS_LOG(WARN, "sstable scan failed, rc %d, server [%s]", rc,
          to_cstring(chunk_server_));
    }
    else
    {
      scanner_.set_default_row_desc(&row_desc_);
      session_created_ = true;
    }
  }

  while(OB_SUCCESS == rc)
  {
    bool is_fullfilled = false;
    int64_t fill_num = 0;
    rc = scanner_.get_is_req_fullfilled(is_fullfilled, fill_num);
    if (OB_SUCCESS != rc)
    {
      TBSYS_LOG(WARN, "get is req fullfilled failed, rc %d", rc);
      break;
    }

    rc = scanner_.get_next_row(row_);
    if (OB_ITER_END == rc && !is_fullfilled)
    {
      scanner_.reuse();
      rc = rpc_stub_->get_next_session_scanner(timeout_, chunk_server_, session_id_, scanner_);
      TBSYS_LOG(DEBUG, "get_next_session_scanner rc %d, server %s", rc,
          to_cstring(chunk_server_));
      if (OB_SUCCESS != rc)
      {
        TBSYS_LOG(WARN, "get next session scanner failed, rc %d, server [%s], session_id %lu",
            rc, to_cstring(chunk_server_), session_id_);
      }
      else
      {
        scanner_.set_default_row_desc(&row_desc_);
        continue;
      }
    }
    break;
  }

  if (OB_SUCCESS == rc)
  {
    row = &row_;
  }

  return rc;
}

int ObRemoteSSTableScan::close()
{
  int rc = OB_SUCCESS;
  if (!check_inner_stat())
  {
    TBSYS_LOG(ERROR, "check_inner_stat() fail");
    rc = OB_ERROR;
  }

  if (OB_SUCCESS == rc && session_created_)
  {
    bool is_fullfilled = false;
    int64_t fill_num = 0;
    if (OB_SUCCESS != (rc = scanner_.get_is_req_fullfilled(is_fullfilled, fill_num)))
    {
      TBSYS_LOG(WARN, "get is req fullfilled failed, rc %d", rc);
    }
    else
    {
      if (!is_fullfilled)
      {
        rc = rpc_stub_->end_next_session(timeout_, chunk_server_, session_id_);
      }
    }
  }

  // Reset inner member, make check_inner_stat() fail.
  scan_param_ = NULL;
  rpc_stub_ = NULL;

  return rc;
}

