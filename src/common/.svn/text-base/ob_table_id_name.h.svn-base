/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_id_name.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_TABLE_ID_NAME_H
#define _OB_TABLE_ID_NAME_H

#include "common/ob_string.h"
#include "common/roottable/ob_scan_helper.h"
#include "common/nb_query_res.h"

namespace oceanbase
{
  namespace common
  {
    struct ObTableIdName
    {
      ObString table_name_;
      uint64_t table_id_;

      ObTableIdName() : table_id_(0) { }
    };

    /* 获得系统所有表单名字和对应的表单id */
    /// @note not thread-safe
    class ObTableIdNameIterator
    {
      public:
        ObTableIdNameIterator();
        virtual ~ObTableIdNameIterator();

        int init(ObScanHelper* client_proxy, bool only_core_tables);
        virtual int get_next(ObTableIdName** table_info);

      private:
        int normal_get(const ObRow& row, ObTableIdName** table_info);
        int internal_get(ObTableIdName** table_info);
        int scan_tables();
        int alloc_objects();
        int destroy_objects();
      protected:
        bool inited_;
        bool only_core_tables_;
        int32_t table_idx_;
        ObTableIdName table_id_name_;
        ObScanHelper* client_proxy_;
        SQLQueryResultReader *reader_;
    };
  }
}

#endif /* _OB_TABLE_ID_NAME_H */


