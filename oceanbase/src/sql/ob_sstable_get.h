/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sstable_get.h 
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_SSTABLE_GET_H
#define _OB_SSTABLE_GET_H 1

#include "ob_phy_operator.h"
#include "common/ob_iterator_adaptor.h"
#include "ob_cur_rowkey_interface.h"
#include "chunkserver/ob_tablet_manager.h"
#include "ob_sstable_getter.h"
#include "sstable/ob_sstable_reader_i.h"
#include "common/ob_profile_log.h"

namespace oceanbase
{
  namespace sql
  {
    class ObSqlGetParam;
    
    class ObSSTableGet : public ObPhyOperator, public ObCurRowkeyInterface
    {
      public:
        ObSSTableGet();
        virtual ~ObSSTableGet() {};

        int open();
        int close();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        int64_t to_string(char* buf, const int64_t buf_len) const;

        template <typename PARAM>
        int open_tablet_manager(chunkserver::ObTabletManager *tablet_manager,
                                PARAM *sql_get_param,
                                const uint64_t *columns,
                                int64_t column_count,
                                int64_t rowkey_cell_count);

        int get_tablet_data_version(int64_t &data_version);
        int get_cur_rowkey(const common::ObRowkey *&rowkey) const;

      private:
        chunkserver::ObTabletManager *tablet_manager_;
        int64_t tablet_version_;
        sql::ObSSTableGetter sstable_getter_;
        compactsstablev2::ObCompactSSTableGetter compactsstable_getter_;

        //ref
        sql::ObRowIterator* getter_;
        const ObRowkey *cur_rowkey_;
    };

    template <typename PARAM>
      int ObSSTableGet::open_tablet_manager(chunkserver::ObTabletManager *tablet_manager,
          PARAM *sql_get_param,
          const uint64_t *columns,
          int64_t column_count,
          int64_t rowkey_cell_count)
      {
        int ret = OB_SUCCESS;
        INIT_PROFILE_LOG_TIMER();

        if (NULL == tablet_manager || NULL == sql_get_param)
        {
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "tablet_manager[%p] sql_get_param[%p]", tablet_manager, sql_get_param);
        }
        else
        {
          int16_t sstable_version = 0;
          tablet_manager_ = tablet_manager;
          const chunkserver::ObGetThreadContext* get_context = NULL;
          if (OB_SUCCESS != (ret = tablet_manager_->gen_sstable_getter_context(*sql_get_param, get_context, tablet_version_, sstable_version)))
          {
            TBSYS_LOG(WARN, "fail to gen sstable getter:ret[%d]", ret);
          }

          if(sstable_version < sstable::SSTableReader::COMPACT_SSTABLE_VERSION)
          {
            if(OB_SUCCESS != (ret = sstable_getter_.initialize(sql_get_param->get_table_id(),
                    sql_get_param->get_rowkey_list(), columns, column_count,
                    rowkey_cell_count, const_cast<chunkserver::ObGetThreadContext*>(get_context), false)))
            {
              getter_ = NULL;
              TBSYS_LOG(WARN, "fail to gen sstable getter:ret[%d]", ret);
            }
            else
            {
              getter_ = &sstable_getter_;
            }
          }
          else
          {
            if(OB_SUCCESS != (ret = compactsstable_getter_.initialize(sql_get_param->get_table_id(),
                    sql_get_param->get_rowkey_list(), columns, column_count,
                    rowkey_cell_count, &(get_context->compact_getter_), false)))
            {
              getter_ = NULL;
              TBSYS_LOG(WARN, "fail to gen compact sstable getter:ret[%d]", ret);
            }
            else
            {
              getter_ = &compactsstable_getter_;
            }
          }
        }

        PROFILE_LOG_TIME(DEBUG, "generate sstable getter, ret %d", ret);
        return ret;
      }
  }
}

#endif /* _OB_SSTABLE_GET_H */


