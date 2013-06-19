/**
 * (C) 2010-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 *
 * Authors:
 *   huating <huating.zmq@alipay.com>
 *
 */
#include "ob_remote_tablet_table.h"
#include "ob_tablet_sstable.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace common;
    using namespace sstable;
    using namespace sql;

    ObRemoteTabletTable::ObRemoteTabletTable() 
    : scanner_(NULL) 
    {

    }

    ObRemoteTabletTable::~ObRemoteTabletTable() 
    {
      if (NULL != scanner_)
      {
        OB_DELETE(ObRemoteSSTableScan, ObModIds::OB_CS_BUILD_INDEX, scanner_);
      }
    }

    int ObRemoteTabletTable::append(const ObRow& row)
    {
      UNUSED(row);
      TBSYS_LOG(WARN, "not support now");
      return OB_SUCCESS;
    }

    int ObRemoteTabletTable::close(const bool is_append_succ)
    {
      UNUSED(is_append_succ);
      TBSYS_LOG(WARN, "not support now");
      return OB_SUCCESS;
    }

    int ObRemoteTabletTable::open(const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;

      if (scan_options.scan_local_ || !scan_options.is_valid() 
          || !scan_options.rpc_option_.is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == scanner_)
      {
        scanner_ = OB_NEW(ObRemoteSSTableScan, ObModIds::OB_CS_BUILD_INDEX);
        if (NULL == scanner_)
        {
          TBSYS_LOG(ERROR, "new ObRemoteSSTableScan instance failed");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }

      if (NULL != scanner_ && OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = scanner_->set_param(
           *scan_options.scan_param_, *scan_options.rpc_option_.sql_rpc_stub_,
           scan_options.rpc_option_.server_, scan_options.rpc_option_.scan_index_timeout_)))
        {
          TBSYS_LOG(WARN, "failed to set scan parameter for remote sstable scanner, "
                          "ret=%d", ret);
        }
      }

      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
