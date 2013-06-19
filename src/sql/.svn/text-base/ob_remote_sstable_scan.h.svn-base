/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* ob_remote_sstable_scan.h is for ...
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/
#ifndef OCEANBASE_SQL_OB_REMOTE_SSTABLE_SCAN_H_
#define OCEANBASE_SQL_OB_REMOTE_SSTABLE_SCAN_H_

#include "ob_phy_operator.h"
#include "common/ob_new_scanner.h"
#include "common/ob_row.h"
#include "common/ob_row_desc.h"
#include "common/ob_server.h"
#include "common/ob_define.h"

namespace oceanbase
{

namespace sstable
{
class ObSSTableScanParam;
}

namespace chunkserver
{
class ObSqlRpcStub;
}

namespace sql
{

// Remote chunkserver sstable scan or local index scan operator.
class ObRemoteSSTableScan : public ObPhyOperator
{
public:
  ObRemoteSSTableScan();
  virtual ~ObRemoteSSTableScan();

  virtual int set_param( const sstable::ObSSTableScanParam &param,
      const chunkserver::ObSqlRpcStub &rpc_stub, const common::ObServer &server,
      const int64_t timeout);

  virtual int open();
  virtual int close();

  virtual int get_row_desc(const common::ObRowDesc *&row_desc) const
  {
    row_desc = &row_desc_;
    return common::OB_SUCCESS;
  }
  virtual int get_next_row(const common::ObRow *&row);

  virtual int set_child(int32_t child_idx, ObPhyOperator &child_op);
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteSSTableScan);

  inline bool check_inner_stat(void);

private:
  common::ObServer chunk_server_;
  const sstable::ObSSTableScanParam *scan_param_;

  const chunkserver::ObSqlRpcStub *rpc_stub_;
  common::ObNewScanner scanner_;

  int64_t timeout_;
  int64_t session_id_;
  bool session_created_;

  common::ObRow row_;
  common::ObRowDesc row_desc_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_OB_REMOTE_SSTABLE_SCAN_H_
