/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sstable_scan.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_sstable_scan.h"
#include "common/utility.h"
#include "sstable/ob_block_index_cache.h"
#include "chunkserver/ob_tablet_image.h"
#include "chunkserver/index/ob_build_index_thread.h"
#include "compactsstablev2/ob_compact_sstable_reader.h"
#include "compactsstablev2/ob_sstable_block_index_cache.h"
#include "compactsstablev2/ob_sstable_block_cache.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::chunkserver;

ObSSTableScan::ObSSTableScan()
  : iterator_(NULL), cur_rowkey_(NULL), sstable_version_(0), row_counter_(0)
{
}

ObSSTableScan::~ObSSTableScan()
{
}

int ObSSTableScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  TBSYS_LOG(WARN, "not implement");
  return ret;
}

int64_t ObSSTableScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "SSTableScan()\n");
  return pos;
}

int ObSSTableScan::init_sstable_scanner()
{
  int ret = OB_SUCCESS;
  ObTablet* tablet = scan_context_.tablet_;

  if (NULL == tablet)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if (scan_param_.is_local_index_scan())
  {
    ObTablet *index_tablet = tablet->get_local_index();
    if (NULL == index_tablet
        || scan_param_.get_range().table_id_ != index_tablet->get_range().table_id_)
    {
      TBSYS_LOG(DEBUG, "tablet local index isn't ready, try to build it now, tablet_range=%s",
                to_cstring(tablet->get_range()));
      if (NULL == scan_context_.build_index_thread_)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = scan_context_.build_index_thread_->tablet_build_local_index(
            tablet, scan_param_.get_range().table_id_);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (scan_param_.is_local_index_scan())
    {
      tablet = tablet->get_local_index();
    }
    if ((sstable_version_ = tablet->get_sstable_version()) < sstable::SSTableReader::COMPACT_SSTABLE_VERSION)
    {
      if (OB_SUCCESS != (ret = tablet->get_sstable_reader(scan_context_.sstable_reader_)) )
      {
        TBSYS_LOG(ERROR, "cannot find sstable with scan range: %s, sstable version: %ld",
            to_cstring(scan_param_.get_range()), sstable_version_);
      }
      else if (OB_SUCCESS != (ret = scanner_.set_scan_param(&scan_param_, &scan_context_)))
      {
        TBSYS_LOG(ERROR, "set_scan_param to scanner error, range: %s, sstable version: %ld",
            to_cstring(scan_param_.get_range()), sstable_version_);
      }
      else
      {
        iterator_ = &scanner_;
      }
    }
    else
    {
      if (OB_SUCCESS != (ret = tablet->get_sstable_reader(scan_context_.compact_context_.sstable_reader_)) )
      {
        TBSYS_LOG(ERROR, "cannot find sstable with scan range: %s, sstable version: %ld",
            to_cstring(scan_param_.get_range()), sstable_version_);
      }
      else if (OB_SUCCESS != (ret = compact_scanner_.set_scan_param(&scan_param_, &scan_context_.compact_context_)))
      {
        TBSYS_LOG(ERROR, "set_scan_param to scanner error, range: %s, sstable version: %ld",
            to_cstring(scan_param_.get_range()), sstable_version_);
      }
      else
      {
        iterator_ = &compact_scanner_;
      }
    }
  }

  return ret;
}

int ObSSTableScan::open()
{
  // set row description base on query columns;
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = init_sstable_scanner()))
  {
    TBSYS_LOG(WARN, "fail to set scan param for scanner");
  }
  // dispatch column groups according input query columns
  row_counter_ = 0;
  return ret;
}

int ObSSTableScan::close()
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "sstable scan row count=%ld", row_counter_);
  // release tablet object.
  if (NULL != scan_context_.tablet_)
  {
    ret = scan_context_.tablet_image_->release_tablet(scan_context_.tablet_);
    scan_context_.tablet_ = NULL;
  }
  return ret;
}

int ObSSTableScan::open_scan_context(const sstable::ObSSTableScanParam& param, const ScanContext& context)
{
  int ret = OB_SUCCESS;
  scan_param_ = param;
  scan_context_ = context;
  // acquire tablet object.
  int64_t query_version = 0;
  if ((query_version = scan_param_.get_version_range().get_query_version()) < 0)
  {
    TBSYS_LOG(ERROR, "empty version range to scan, version_range=%s",
        to_cstring(scan_param_.get_version_range()));
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = scan_context_.tablet_image_->acquire_tablet(scan_param_.get_data_range(),
          ObMultiVersionTabletImage::SCAN_FORWARD, query_version, scan_context_.tablet_)))
  {
    TBSYS_LOG(ERROR, "cannot acquire tablet with scan range: %s, version: %ld",
        to_cstring(scan_param_.get_data_range()), query_version);
  }

  return ret;
}

int ObSSTableScan::get_next_row( const ObRow *&row_value)
{
  int ret = OB_SUCCESS;
  const ObRowkey* row_key = NULL;
  if (NULL == iterator_)
  {
    TBSYS_LOG(ERROR, "internal error, iterator_ not set, call init_sstable_scanner");
    ret = OB_NOT_INIT;
  }
  else
  {
    ret = iterator_->get_next_row(row_value);
  }

  if (OB_SUCCESS == ret)
  {
    row_value->get_rowkey(row_key);
    cur_rowkey_ = row_key;
    ++row_counter_;
    TBSYS_LOG(DEBUG, "sstable get next row:%s", to_cstring(*row_value));
  }
  return ret;
}

int ObSSTableScan::get_tablet_data_version(int64_t &version)
{
  int ret = OB_SUCCESS;
  if(NULL == scan_context_.tablet_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "invoke this func after open");
  }
  else
  {
    version = scan_context_.tablet_->get_data_version();
  }
  return ret;
}

int ObSSTableScan::get_tablet_range(ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if(NULL == scan_context_.tablet_)
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "invoke this func after open");
  }
  else
  {
    range = scan_context_.tablet_->get_range();
  }
  return ret;
}

int ObSSTableScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (NULL == iterator_)
  {
    ret = OB_NOT_INIT;
  }
  else if(OB_SUCCESS != (ret = iterator_->get_row_desc(row_desc)))
  {
    TBSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
  }
  return ret;
}

int ObSSTableScan::get_cur_rowkey(const ObRowkey *&rowkey) const
{
  int ret = OB_SUCCESS;
  rowkey = cur_rowkey_;
  return ret;
}
