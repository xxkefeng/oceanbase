/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_SQL_OB_HUSK_SINGLE_CHILD_PHY_OPERATOR_H__
#define __OB_SQL_OB_HUSK_SINGLE_CHILD_PHY_OPERATOR_H__
#include "ob_husk_phy_operator.h"
#include "common/utility.h"
namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    template<ObPhyOperatorType PHY_TYPE>
    class ObHuskFilter: public ObSingleChildPhyOperator
    {
      public:
        ObHuskFilter(){}
        virtual ~ObHuskFilter() {}
      public:
        enum ObPhyOperatorType get_type() const
        {
          return PHY_TYPE;
        }

        int open()
        {
          return OB_NOT_SUPPORTED;
        }

        int close()
        {
          return OB_NOT_SUPPORTED;
        }

        int get_next_row(const common::ObRow *&row)
        {
          UNUSED(row);
          return OB_NOT_SUPPORTED;
        }

        int get_row_desc(const common::ObRowDesc *&row_desc) const
        {
          UNUSED(row_desc);
          return OB_NOT_SUPPORTED;
        }

        virtual int64_t to_string(char* buf, const int64_t buf_len) const
        {
          int64_t pos = 0;
          databuff_printf(buf, buf_len, pos, "HuskFilter(type=%d)\n", PHY_TYPE);
          if (NULL != child_op_)
          {
            pos += child_op_->to_string(buf+pos, buf_len-pos);
          }
          return pos;
        }

        virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const
        {
          UNUSED(buf);
          UNUSED(buf_len);
          UNUSED(pos);
          return OB_SUCCESS;
        }

        virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos)
        {
          UNUSED(buf);
          UNUSED(data_len);
          UNUSED(pos);
          return OB_SUCCESS;
        }

        virtual int64_t get_serialize_size(void) const
        {
          return 0;
        }
    };
  }; // end namespace sql
}; // end namespace oceanbase

#endif /* __OB_SQL_OB_HUSK_SINGLE_CHILD_PHY_OPERATOR_H__ */
