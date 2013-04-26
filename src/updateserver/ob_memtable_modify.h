/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_modify.h
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_MEMTABLE_MODIFY_H_
#define  OCEANBASE_UPDATESERVER_MEMTABLE_MODIFY_H_

#include "sql/ob_ups_modify.h"
#include "common/ob_iterator.h"
#include "common/ob_iterator_adaptor.h"
#include "ob_sessionctx_factory.h"
#include "ob_ups_table_mgr.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    class MemTableModify : public sql::ObUpsModify, public RowkeyInfoCache
    {
      public:
        MemTableModify(RWSessionCtx &session, ObIUpsTableMgr &host);
        ~MemTableModify();
      public:
        int open();
        int close();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
      private:
        RWSessionCtx &session_;
        ObIUpsTableMgr &host_;
    };
  } // end namespace updateserver
} // end namespace oceanbase

#endif /* OCEANBASE_UPDATESERVER_MEMTABLE_MODIFY_H_ */

