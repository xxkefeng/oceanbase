/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
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
#include "common/ob_schema.h"
#include "ob_tablet_memtable.h"
#include "ob_tablet_sstable.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace common;
    using namespace sql;

    ObTabletMemtable::ObTabletMemtable()
    : row_count_(0),
      scanner_(row_store_, cur_row_desc_)
    {

    }

    ObTabletMemtable::~ObTabletMemtable()
    {

    }

    void ObTabletMemtable::reset()
    {
      row_count_ = 0;
      cur_row_desc_.reset();
      row_store_.reuse();
    }

    int ObTabletMemtable::open(const ObRowDesc& row_desc)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = 0;
      uint64_t column_id = 0;

      for (int64_t i = 0; i < row_desc.get_column_num() && OB_SUCCESS == ret; ++i)
      {
        if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, table_id, column_id)))
        {
          TBSYS_LOG(WARN, "cannot get [%ld]th tid cid, row desc=%s", 
                    i, to_cstring(row_desc));
        }
        else if (OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(table_id, column_id)))
        {
          TBSYS_LOG(WARN, "cannot add [%ld]th [%lu,%lu] to column desc", 
                    i, table_id, column_id);
        }
      }

      if (OB_SUCCESS == ret)
      {
        cur_row_desc_.set_rowkey_cell_count(row_desc.get_rowkey_cell_count());
      }

      return ret;
    }

    int ObTabletMemtable::open(const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* table_schema = NULL;

      if (NULL == (table_schema = scan_options.schema_->get_table_schema(
         scan_options.index_table_id_)))
      {
        TBSYS_LOG(WARN, "failed to get table schema, table_id_=%lu",
                  scan_options.index_table_id_);
        ret = OB_ERROR;
      }
      else 
      {
        const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
        const ObRowkeyColumn* rowkey_column = NULL;

        for (int64_t i = 0; i < rowkey_info.get_size() && OB_SUCCESS == ret; i++)
        {
          if (NULL == (rowkey_column = rowkey_info.get_column(i)))
          {
            TBSYS_LOG(WARN, "failed to get rowkey column, table_id_=%lu", 
                      scan_options.index_table_id_);
            ret = OB_ERROR;
          }
          else if(OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(
             scan_options.index_table_id_, rowkey_column->column_id_)))
          {
            TBSYS_LOG(WARN, "scan parameter add rowkey column failed, table_id=%lu, "
                            "column_id_=%lu, ret=%d", 
                      scan_options.index_table_id_, rowkey_column->column_id_, ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          cur_row_desc_.set_rowkey_cell_count(rowkey_info.get_size());
        }
      }

      return ret;
    }

    int ObTabletMemtable::append(const ObRow& row)
    {
      int ret = OB_SUCCESS;
      const ObRowStore::StoredRow *stored_row = NULL;

      if (OB_SUCCESS == (ret = row_store_.add_row(row, stored_row)))
      {
        __sync_add_and_fetch(&row_count_, 1);
      }

      return ret;
    }

    int ObTabletMemtable::close(const bool is_append_succ)
    {
      UNUSED(is_append_succ);
      return OB_SUCCESS;
    }

    ObTabletMemtableScanner::ObTabletMemtableScanner(
       ObRowStore& row_store, ObRowDesc& row_desc)
    : cur_row_desc_(row_desc),
      row_store_(row_store)
    {

    }

    ObTabletMemtableScanner::~ObTabletMemtableScanner()
    {

    }

    int64_t ObTabletMemtableScanner::to_string(
       char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;

      if (NULL != buf && buf_len > 0)
      {
        databuff_printf(buf, buf_len, pos, "ObTabletMemtableScanner(");
        databuff_printf(buf, buf_len, pos, "row_desc=");
        pos += cur_row_desc_.to_string(buf + pos, buf_len - pos);
        databuff_printf(buf, buf_len, pos, ", row_store=");
        pos += row_store_.to_string(buf + pos, buf_len - pos);
        databuff_printf(buf, buf_len, pos, ")\n");
      }

      return pos;
    }

    int ObTabletMemtableScanner::get_row_desc(const ObRowDesc*& row_desc) const
    {
      int ret = OB_SUCCESS;

      if (cur_row_desc_.get_column_num() <= 0)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "cur_row_desc_ is empty");
      }
      else
      {
        row_desc = &cur_row_desc_;
      }

      return ret;      
    }

    int ObTabletMemtableScanner::open()
    {
      cur_row_.set_row_desc(cur_row_desc_);
      return OB_SUCCESS;
    }

    int ObTabletMemtableScanner::get_next_row(const ObRow*& row)
    {
      int ret = OB_SUCCESS;

      ret = row_store_.get_next_row(cur_row_);
      if (OB_ITER_END == ret)
      {
        TBSYS_LOG(DEBUG, "end of iteration");
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to get next row from row store:ret=%d", ret);
      }
      else
      {
        row = &cur_row_;
      }

      return ret;
    }

    int ObTabletMemtableScanner::close()
    {
      return OB_SUCCESS;
    }
  } /* chunkserver */
} /* oceanbase */
