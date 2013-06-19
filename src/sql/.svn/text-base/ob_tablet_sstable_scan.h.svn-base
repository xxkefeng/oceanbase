/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* ob_tablet_sstable_scan.h is for ...
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/
#ifndef OCEANBASE_SQL_OB_TABLET_SSTABLE_SCAN_H_
#define OCEANBASE_SQL_OB_TABLET_SSTABLE_SCAN_H_

#include "sql/ob_tablet_read.h"
#include "sql/ob_sstable_scan.h"

namespace oceanbase
{

namespace sstable
{
class ObSSTableScanParam;
}

namespace sql
{
// Use sstable::ObSSTableScanParam to scan sstable data or local index data.
class ObTabletSSTableScan : public ObTabletRead
{
public:
  ObTabletSSTableScan();
  virtual ~ObTabletSSTableScan();

  virtual int create_plan(const ObSchemaManagerV2 &schema_mgr);

  int64_t to_string(char *buf, const int64_t buf_len) const;

  inline void set_param(const sstable::ObSSTableScanParam &scan_param,
      const ScanContext &scan_context)
  {
    sstable_scan_param_ = &scan_param;
    scan_context_ = scan_context;
  }

  void reset(void);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletSSTableScan);

  const sstable::ObSSTableScanParam *sstable_scan_param_;
  ScanContext scan_context_;
  ObSSTableScan sstable_scan_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_OB_TABLET_SSTABLE_SCAN_H_
