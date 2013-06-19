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
#ifndef OCEANBASE_CHUNKSERVER_TABLE_H_
#define OCEANBASE_CHUNKSERVER_TABLE_H_

#include "tbsys.h"
#include "common/ob_general_rpc_stub.h"
#include "sql/ob_phy_operator.h"
#include "sql/ob_sstable_scan.h"
#include "chunkserver/ob_sql_rpc_stub.h"
#include "ob_row_cursor.h"
#include "ob_row_writer.h"

namespace oceanbase
{
  namespace chunkserver
  {
    struct ObRpcOptions
    {
      ObRpcOptions()
      {
        reset();
      }

      bool is_valid() const;

      int64_t to_string(char* buffer, const int64_t size) const;

      void reset()
      {
        rpc_stub_ = NULL;
        sql_rpc_stub_ = NULL;
        server_.reset();
        retry_times_ = 0;
        timeout_ = 0;
        scan_index_timeout_ = 0;
      }

      common::ObGeneralRpcStub* rpc_stub_;
      ObSqlRpcStub* sql_rpc_stub_;
      common::ObServer server_;
      int64_t retry_times_;
      int64_t timeout_;
      int64_t scan_index_timeout_;
    };

    class ObSortFileSetter;
    struct ObScanOptions
    {
      ObScanOptions()
      {
        reset();
      }

      void reset()
      {
        index_table_id_ = common::OB_INVALID_ID;
        scan_local_ = true;
        is_result_cached_ = false;
        scan_only_rowkey_ = false;
        need_sort_ = false;
        row_interval_ = 0;
        schema_ = NULL;
        data_tablet_range_ = NULL;
        index_tablet_range_ = NULL;
        scan_param_ = NULL;
        scan_context_ = NULL;
        rpc_option_.reset();
        sort_file_setter_ = NULL;
        stop_ = NULL;
      }

      bool is_valid() const;

      int64_t to_string(char* buffer, const int64_t size) const;

      uint64_t index_table_id_;
      bool scan_local_;
      bool is_result_cached_;
      bool scan_only_rowkey_;
      bool need_sort_;
      int64_t row_interval_;
      const common::ObSchemaManagerV2* schema_;
      const common::ObNewRange* data_tablet_range_;
      const common::ObNewRange* index_tablet_range_;
      sstable::ObSSTableScanParam* scan_param_;
      const sql::ScanContext* scan_context_;
      ObRpcOptions rpc_option_;
      ObSortFileSetter *sort_file_setter_;
      const bool *stop_;
    };

    class ObTable
    {
      public:
        virtual ~ObTable() { }

        virtual int append(const common::ObRow& row) = 0;

        virtual int close(const bool is_append_succ) = 0;

        virtual int open(const ObScanOptions& scan_options) = 0;

        virtual sql::ObPhyOperator* get_phyoperator() = 0;

      protected:
        // To allow instantiation in subclasses.
        ObTable() {}

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTable);
    };

    class ObTableSink : public ObRowSink 
    {
      public:
        explicit ObTableSink(ObTable* table) 
        : table_(table) 
        {
        }

        virtual int append(const common::ObRow& row)
        {
          return table_->append(row);
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTableSink);
        ObTable* table_;
    };

    class ObThreadSafeTableSink : public ObRowSink 
    {
      public:
        explicit ObThreadSafeTableSink(ObTable* table) 
        : table_(table) 
        {
        }

        virtual int append(const common::ObRow& row)
        {
          int ret = common::OB_SUCCESS;

          tbsys::CThreadGuard guard(&mutex_);
          ret = table_->append(row);

          return ret;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObThreadSafeTableSink);
        ObTable* table_;
        tbsys::CThreadMutex mutex_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_TABLE_H_
