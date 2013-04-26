/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_insert_dbsem_filter.cpp
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#include "ob_insert_dbsem_filter.h"
#include "common/utility.h"
namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    ObInsertDBSemFilter::ObInsertDBSemFilter() : could_insert_(false),
                                                 insert_values_()
    {
    }

    ObInsertDBSemFilter::~ObInsertDBSemFilter()
    {
    }

    int ObInsertDBSemFilter::open()
    {
      int ret = OB_SUCCESS;
      could_insert_ = false;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = child_op_->open()))
      {
        TBSYS_LOG(WARN, "open child_op fail ret=%d %p", ret, child_op_);
      }
      else if (OB_SUCCESS != (ret = insert_values_.open()))
      {
        TBSYS_LOG(WARN, "open insert_values fail ret=%d", ret);
      }
      else
      {
        const ObRow *row = NULL;
        if (OB_SUCCESS != (ret = child_op_->get_next_row(row)))
        {
          if (OB_ITER_END != ret)
          {
            TBSYS_LOG(ERROR, "child_op get_next_row fail but not OB_ITER_END, ret=%d", ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "iterate nothing from child_op=%p type=%d will insert rows", child_op_, child_op_->get_type());
            could_insert_ = true;
            ret = OB_SUCCESS;
          }
        }
        else
        {
          TBSYS_LOG(DEBUG, "iterate row from child_op=%p type=%d %s will not insert anything",
                    child_op_, child_op_->get_type(), (NULL == row) ? "nil" : to_cstring(*row));
          TBSYS_LOG(USER_ERROR, "Duplicate entry \'%s\' for key \'PRIMARY\'", (NULL == row) ? "nil" : to_cstring(*row));
          ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        }
      }
      if (NULL != child_op_)
      {
        child_op_->close();     // close anyway
      }
      if (OB_SUCCESS != ret)
      {
        insert_values_.close(); // close if error
      }
      return ret;
    }

    int ObInsertDBSemFilter::close()
    {
      int ret = OB_SUCCESS;
      if (NULL == child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = insert_values_.close()))
        {
          TBSYS_LOG(WARN, "close insert_values fail ret=%d", tmp_ret);
        }
        if (OB_SUCCESS != (tmp_ret = child_op_->close()))
        {
          TBSYS_LOG(WARN, "close child_op fail ret=%d", tmp_ret);
        }
        could_insert_ = false;
      }
      return ret;
    }

    int ObInsertDBSemFilter::get_next_row(const common::ObRow *&row)
    {
      int ret = OB_SUCCESS;
      if (!could_insert_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        ret = insert_values_.get_next_row(row);
      }
      return ret;
    }

    int ObInsertDBSemFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      if (!could_insert_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        ret = insert_values_.get_row_desc(row_desc);
      }
      return ret;
    }

    int64_t ObInsertDBSemFilter::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "InsertDBSemFilter(values=");
      pos += insert_values_.to_string(buf+pos, buf_len-pos);
      databuff_printf(buf, buf_len, pos, ")\n");
      if (NULL != child_op_)
      {
        pos += child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }

    DEFINE_SERIALIZE(ObInsertDBSemFilter)
    {
      return insert_values_.serialize(buf, buf_len, pos);
    }

    DEFINE_DESERIALIZE(ObInsertDBSemFilter)
    {
      return insert_values_.deserialize(buf, data_len, pos);;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObInsertDBSemFilter)
    {
      return insert_values_.get_serialize_size();
    }
  } // end namespace sql
} // end namespace oceanbase
