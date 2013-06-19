/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_sql_get_simple_param.cpp for define get param
 *
 * Authors:
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "common/utility.h"
#include "common/ob_action_flag.h"
#include "common/ob_malloc.h"
#include "ob_sql_get_simple_param.h"
#include "common/ob_rowkey_helper.h"
#include "common/ob_schema.h"
#include "common/ob_string_buf.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace sql 
  {
    ObSqlGetSimpleParam::ObSqlGetSimpleParam() :
      max_row_capacity_(MAX_ROW_CAPACITY),
      rowkey_list_(), 
      buffer_pool_(ObModIds::OB_SQL_GET_PARAM, DEFAULT_ROW_BUF_SIZE)
    {
    }

    ObSqlGetSimpleParam::~ObSqlGetSimpleParam()
    {
    }

    int ObSqlGetSimpleParam::init()
    {
      int ret = OB_SUCCESS;
      reset();
      return ret;
    }

    void ObSqlGetSimpleParam::reset()
    {
      ObSqlReadSimpleParam::reset();
      rowkey_list_.clear();
      // if memory inflates too large, free.
      if (buffer_pool_.total() > DEFAULT_ROW_BUF_SIZE)
      {
        buffer_pool_.clear();
      }
      else
      {
        buffer_pool_.reuse();
      }
    }
   
    void ObSqlGetSimpleParam::reset_rowkey_list()
    {
      rowkey_list_.clear();
      // if memory inflates too large, free.
      if (buffer_pool_.total() > DEFAULT_ROW_BUF_SIZE)
      {
        buffer_pool_.clear();
      }
      else
      {
        buffer_pool_.reuse();
      }
    }
   
   
    int ObSqlGetSimpleParam::destroy()
    {
      rowkey_list_.clear();
      buffer_pool_.clear();
      return OB_SUCCESS;
    }

    int64_t ObSqlGetSimpleParam::to_string(char *buf, int64_t buf_size) const
    {
      int64_t pos = 0;
      if (NULL != buf && buf_size > 0)
      {
        databuff_printf(buf, buf_size, pos, "[Get RowNum:%ld]:\n", rowkey_list_.count());
        databuff_print_obj(buf, buf_size, pos, rowkey_list_);
      }
      return pos;
    }

    void ObSqlGetSimpleParam::print_memory_usage(const char* msg) const
    {
      TBSYS_LOG(INFO, "ObSqlGetSimpleParam use memory:%s, allocator:used:%ld,total:%ld,",
          msg,  buffer_pool_.used(), buffer_pool_.total());
    }

    int ObSqlGetSimpleParam::add_rowkey(const ObRowkey& rowkey, bool deep_copy_args)
    {
      int ret = OB_SUCCESS;
      ObRowkey stored_rowkey = rowkey;
      if (deep_copy_args)
      {
        ret = copy_rowkey(rowkey, stored_rowkey);
      }
      if (OB_SUCCESS == ret)
      {
        //cell info must include legal row key
        if (stored_rowkey.length() <= 0 || NULL == stored_rowkey.ptr())
        {
          TBSYS_LOG(WARN, "invalid row key, key_len=%ld, key_ptr=%p",
            stored_rowkey.length(), stored_rowkey.ptr());
          ret = OB_INVALID_ARGUMENT;
        }
      }
      if (OB_SUCCESS == ret)
      {
        //ensure there are enough space to store this cell
        if (rowkey_list_.count() < max_row_capacity_)
        {
            //store rowkey 
            if (OB_SUCCESS != (ret = rowkey_list_.push_back(stored_rowkey)))
            {
              TBSYS_LOG(WARN, "fail to push rowkey to list. ret=%d", ret);
            }
        }
        else
        {
          TBSYS_LOG(INFO, "get param is full, can't add rowkey anymore, row_size=%ld, max=%d",
            rowkey_list_.count(), max_row_capacity_);
          ret = OB_SIZE_OVERFLOW;
        }
      }
      return ret;
    }

    int ObSqlGetSimpleParam::copy_rowkey(const ObRowkey &rowkey, ObRowkey &stored_rowkey)
    {
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = rowkey.deep_copy(stored_rowkey, buffer_pool_)))
      {
        TBSYS_LOG(WARN,"fail to copy rowkey to local buffer [err:%d].[rowkey=%s]", err, to_cstring(rowkey));
      }
      return err;
    }

    /////////////////////////////////////////////////////////////////////////////
    //////////////////// SERIALIZE & DESERIALIZE ////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////
    
    int ObSqlGetSimpleParam::serialize_flag(char* buf, const int64_t buf_len, int64_t& pos,
        const int64_t flag) const
    {
      int ret = OB_SUCCESS;
      ObObj obj;

      obj.set_ext(flag);
      ret = obj.serialize(buf, buf_len, pos);

      return ret;
    }
    
    int ObSqlGetSimpleParam::serialize_int(char* buf, const int64_t buf_len, int64_t& pos,
        const int64_t value) const
    {
      int ret = OB_SUCCESS;
      ObObj obj;

      obj.set_int(value);
      ret = obj.serialize(buf, buf_len, pos);

      return ret;
    }
 
    int ObSqlGetSimpleParam::deserialize_int(const char* buf, const int64_t data_len, int64_t& pos,
        int64_t& value) const
    {
      int ret         = OB_SUCCESS;
      ObObjType type  = ObNullType;
      ObObj obj;

      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        type = obj.get_type();
        if (ObIntType == type)
        {
          obj.get_int(value);
        }
        else
        {
          TBSYS_LOG(WARN, "expected deserialize int type, but get type=%d", type);
          ret = OB_ERROR;
        }
      }

      return ret;
    }

    int64_t ObSqlGetSimpleParam::get_obj_serialize_size(const int64_t value, bool is_ext) const
    {
      ObObj obj;

      if (is_ext)
      {
        obj.set_ext(value);
      }
      else
      {
        obj.set_int(value);
      }

      return obj.get_serialize_size();
    }
 
    int ObSqlGetSimpleParam::serialize_rowkeys(char* buf, const int64_t buf_len,
      int64_t& pos, const ObArray<ObRowkey>& rowkey_list) const
    {
      int ret = OB_SUCCESS;
      int64_t i = 0;
      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld",
          buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        //serialize row keys        
        if (OB_SUCCESS != (ret = serialize_flag(buf, buf_len, pos, ObActionFlag::FORMED_ROW_KEY_FIELD)))
        {
          TBSYS_LOG(WARN, "fail to serialize flag. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = serialize_int(buf, buf_len, pos, rowkey_list.count())))
        {
          TBSYS_LOG(WARN, "fail to serialize rowkey size. ret=%d", ret);
        }
        else
        {
          if (OB_SUCCESS == ret)
          {
            for (i = 0; OB_SUCCESS == ret && i < rowkey_list.count(); i++)
            {
              ret = rowkey_list.at(i).serialize(buf, buf_len, pos);
              // TBSYS_LOG(DEBUG, "serialize rowkey[%ld]: %s. ret=%d", i, to_cstring(rowkey_list.at(i)), ret);
            }
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to serialize rowkey list. buf=%p, buf_len=%ld, pos=%ld, rowkey list size=%ld",
            buf, buf_len, pos, rowkey_list.count());
      }
      return ret;
    }

    int ObSqlGetSimpleParam::deserialize_rowkeys(const char* buf, const int64_t data_len,
      int64_t& pos)
    {
      int ret         = OB_SUCCESS;
      int64_t size    = 0;
      int64_t i       = 0;
      ObRowkey rowkey;
      ObObj tmp_objs[OB_MAX_ROWKEY_COLUMN_NUMBER];

      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        TBSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (ret = deserialize_int(buf, data_len, pos, size)))
        {
          TBSYS_LOG(WARN, "fail to deserialize_int(rowkey size). buf=%p, data_len=%ld, pos=%ld, ret=%d",
              buf, data_len, pos, ret);
        }
        else
        {
          rowkey.assign(tmp_objs, OB_MAX_ROWKEY_COLUMN_NUMBER);
          for (i = 0; i < size; i++)
          {
            //deserialize rowkey
            if (OB_SUCCESS != (ret = rowkey.deserialize(buf, data_len, pos)))
            {
              TBSYS_LOG(WARN, "fail to deserialize rowkey. buf=%p, data_len=%ld, pos=%ld, i=%ld, size=%ld",
                  buf, data_len, pos, i, size);
            }
            else if (OB_SUCCESS != (ret = add_rowkey(rowkey, true)))
            {
              TBSYS_LOG(WARN, "fail to add deserialized rowkey to get param. ret=%d, i=%ld, size=%ld", ret, i, size);
            }
          }
        }
      }
      return ret;
    }

    int64_t ObSqlGetSimpleParam::get_serialize_rowkeys_size(const ObArray<ObRowkey> &rowkey_list) const
    {
      int64_t total_size = 0;
      int64_t i = 0;
      total_size += get_obj_serialize_size(rowkey_list.count());
      total_size += get_obj_serialize_size(ObActionFlag::FORMED_ROW_KEY_FIELD, true);
      for (i = 0; i < rowkey_list.count(); i++)
      {
        total_size += rowkey_list.at(i).get_serialize_size();
      }
      return total_size;
    }

    DEFINE_SERIALIZE(ObSqlGetSimpleParam)
    {
      int ret = OB_SUCCESS;
      ObObj obj;

      if (OB_SUCCESS == ret)
      {
        ret = ObSqlReadSimpleParam::serialize(buf, buf_len, pos);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialize_rowkeys(buf, buf_len, pos, rowkey_list_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialize_flag(buf, buf_len, pos, ObActionFlag::END_PARAM_FIELD);
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObSqlGetSimpleParam)
    {
      int ret                 = OB_SUCCESS;
      bool end_flag           = false;
      int64_t ext_val         = -1;
      ObObjType type          = ObNullType;
      ObObj obj;
      //better reset
      if (OB_SUCCESS == ret)
      {
        reset();
      }

      // TODO: not scaleable 
      if (OB_SUCCESS == ret)
      {
        ret = ObSqlReadSimpleParam::deserialize(buf, data_len, pos);
      }
      //deserialize obj one by one
      while (OB_SUCCESS == ret && pos < data_len && !end_flag)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          //check type
          type = obj.get_type();
          if (ObExtendType == type)
          {
            //check extend type
            ext_val = obj.get_ext();
            switch (ext_val)
            {
            case ObActionFlag::FORMED_ROW_KEY_FIELD:
              ret = deserialize_rowkeys(buf, data_len, pos);
              break;
            case ObActionFlag::END_PARAM_FIELD:
              end_flag = true;
              break;
            default:
              TBSYS_LOG(WARN, "unkonwn extend field, extend=%ld", ext_val);
              break;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "wrong object type=%d", type);
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSqlGetSimpleParam)
    {
      int64_t total_size = 0;
      total_size += get_serialize_rowkeys_size(rowkey_list_);
      total_size += get_obj_serialize_size(ObActionFlag::END_PARAM_FIELD, true);
      total_size += ObSqlReadSimpleParam::get_serialize_size();
      return total_size;
    }
  } /* common */
} /* oceanbase */
