/**
 *  (C) 2010-2011 Taobao Inc.
 *  
 *  This program is free software; you can redistribute it
 *  and/or modify it under the terms of the GNU General Public
 *  License version 2 as published by the Free Software
 *  Foundation.
 *
 *  ob_sstable_scan_param.cpp is for what ...
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        
 */


#include "common/utility.h"
#include "ob_sstable_scan_param.h"
using namespace oceanbase::common;
namespace oceanbase
{
  namespace sstable
  {
    ObSSTableScanParam::ObSSTableScanParam()
    {
      reset();
    }
    ObSSTableScanParam::~ObSSTableScanParam()
    {
    }

    void ObSSTableScanParam::reset()
    {
      scan_flag_.flag_ = 0;
      column_id_list_.init(OB_MAX_COLUMN_NUMBER, column_ids_);
      range_.reset();
      local_index_range_.reset();
    }

    bool ObSSTableScanParam::is_valid() const
    {
      int ret = true;
      if (range_.empty())
      {
        TBSYS_LOG(ERROR, "range=%s is empty, cannot find any key.", to_cstring(range_));
        ret = false;
      }
      else if (column_id_list_.get_array_index() <= 0)
      {
        TBSYS_LOG(ERROR, "query not request any columns=%ld", 
            column_id_list_.get_array_index());
        ret = false;
      }
      return ret;
    }

    int64_t ObSSTableScanParam::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      pos += snprintf(buf, buf_len, "%s", "scan range:");
      if (pos < buf_len) 
      {
        pos += range_.to_string(buf + pos, buf_len - pos);
      }

      if (pos < buf_len)
      {
        for (int64_t i = 0; i < column_id_list_.get_array_index() && pos < buf_len;  ++i)
        {
          databuff_printf(buf, buf_len, pos, "<id=%lu>,", column_ids_[i]);
        }
      }
      return pos;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSSTableScanParam)
    {
      int64_t size = 0;
      size += get_basic_param_serialize_size();
      size += get_ranges_serialize_size();
      size += get_columns_serialize_size();

      ObObj obj;
      obj.set_ext(ObActionFlag::END_PARAM_FIELD);
      size += obj.get_serialize_size();

      return size;
    }

    DEFINE_SERIALIZE(ObSSTableScanParam)
    {
      int rc = OB_SUCCESS;
      if (OB_SUCCESS != (rc = serialize_basic_param(buf, buf_len, pos)))
      {
        TBSYS_LOG(ERROR, "fail to serialize basic param, rc %d", rc);
      }
      else if (OB_SUCCESS != (rc = serialize_ranges(buf, buf_len, pos)))
      {
        TBSYS_LOG(ERROR, "fail to serialize ranges, rc %d", rc);
      }
      else if (OB_SUCCESS != (rc = serialize_columns(buf, buf_len, pos)))
      {
        TBSYS_LOG(ERROR, "fail to serialize column list, rc %d", rc);
      }

      if (OB_SUCCESS == rc)
      {
        ObObj obj;
        obj.set_ext(ObActionFlag::END_PARAM_FIELD);
        rc = obj.serialize(buf, buf_len, pos);
      }

      return rc;
    }

    DEFINE_DESERIALIZE(ObSSTableScanParam)
    {
      int rc = OB_SUCCESS;
      ObObj obj;

      while (true)
      {
        while (OB_SUCCESS == (rc = obj.deserialize(buf, data_len, pos))
            && ObExtendType != obj.get_type())
        {
          ;
        }

        if (OB_SUCCESS != rc || ObActionFlag::END_PARAM_FIELD == obj.get_ext())
        {
          break;
        }

        switch (obj.get_ext())
        {
        case ObActionFlag::BASIC_PARAM_FIELD:
          {
            rc = deserialize_basic_param(buf, data_len, pos);
            break;
          }
        case ObActionFlag::NEWRANGE_PARAM_FIELD:
          {
            rc = deserialize_ranges(buf, data_len, pos);
            break;
          }
        case ObActionFlag::COLUMN_PARAM_FIELD:
          {
            rc = deserialize_columns(buf, data_len, pos);
            break;
          }
        default:
          {
            break;
          }
        }
      }
      return rc;
    }

    int ObSSTableScanParam::serialize_columns(char *buf, const int64_t &buf_len, int64_t &pos) const
    {
      int rc = set_ext_obj_value(buf, buf_len, pos, ObActionFlag::COLUMN_PARAM_FIELD);
      if (OB_SUCCESS == rc)
      {
        rc = set_int_obj_value(buf, buf_len, pos, column_id_list_.get_array_index());
      }
      for (int64_t i = 0; OB_SUCCESS == rc && i < column_id_list_.get_array_index(); i++)
      {
        rc = set_int_obj_value(buf, buf_len, pos, *column_id_list_.at(i));
      }

      return rc;
    }

    int ObSSTableScanParam::deserialize_columns(const char *buf, const int64_t &data_len, int64_t &pos)
    {
      int64_t size = 0;
      int rc = get_int_obj_value(buf, data_len, pos, size);
      for (int64_t i = 0; OB_SUCCESS == rc && i < size; i++)
      {
        int64_t value;
        if (OB_SUCCESS == (rc = get_int_obj_value(buf, data_len, pos, value)))
        {
          column_id_list_.push_back(value);
        }
      }

      return rc;
    }

    int64_t ObSSTableScanParam::get_columns_serialize_size(void) const
    {
      int64_t size = 0;

      ObObj obj;
      obj.set_ext(ObActionFlag::COLUMN_PARAM_FIELD);
      size += obj.get_serialize_size();
      obj.set_int(column_id_list_.get_array_index());
      size += obj.get_serialize_size();

      for (int64_t i = 0; i < column_id_list_.get_array_index(); i++)
      {
        obj.set_int(*column_id_list_.at(i));
        size += obj.get_serialize_size();
      }

      return size;
    }

    int ObSSTableScanParam::serialize_basic_param(char *buf, const int64_t &buf_len, int64_t &pos) const
    {
      int rc = set_ext_obj_value(buf, buf_len, pos, ObActionFlag::BASIC_PARAM_FIELD);

      if (OB_SUCCESS == rc)
      {
        rc = ObReadParam::serialize(buf, buf_len, pos);
      }
      if (OB_SUCCESS == rc)
      {
        rc = set_int_obj_value(buf, buf_len, pos, scan_flag_.flag_);
      }

      return rc;
    }

    int ObSSTableScanParam::deserialize_basic_param(const char *buf, const int64_t &data_len, int64_t &pos)
    {
      int rc = ObReadParam::deserialize(buf, data_len, pos);

      if (OB_SUCCESS == rc)
      {
        rc = get_int_obj_value(buf, data_len, pos, scan_flag_.flag_);
      }

      return rc;
    }

    int64_t ObSSTableScanParam::get_basic_param_serialize_size(void) const
    {
      int64_t size = 0;

      ObObj obj;
      obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
      size += obj.get_serialize_size();

      size += ObReadParam::get_serialize_size();

      obj.set_int(scan_flag_.flag_);
      size += obj.get_serialize_size();
      
      return size;
    }

    int ObSSTableScanParam::serialize_ranges(char *buf,
        const int64_t &buf_len, int64_t &pos) const
    {
      const ObNewRange *ranges[] = { &range_,
        (is_local_index_scan() ? &local_index_range_ : NULL),
        NULL };
      int rc = set_ext_obj_value(buf, buf_len, pos, ObActionFlag::NEWRANGE_PARAM_FIELD);

      for (const ObNewRange *const *range = ranges; OB_SUCCESS == rc && NULL != *range; range++)
      {
        if (OB_SUCCESS == rc )
        {
          rc = set_int_obj_value(buf, buf_len, pos, (*range)->table_id_);
        }
        if (OB_SUCCESS == rc )
        {
          rc = set_int_obj_value(buf, buf_len, pos, (*range)->border_flag_.get_data());
        }
        if (OB_SUCCESS == rc )
        {
          rc = set_rowkey_obj_array(buf, buf_len, pos, (*range)->start_key_.get_obj_ptr(),
              (*range)->start_key_.get_obj_cnt());
        }
        if (OB_SUCCESS == rc )
        {
          rc = set_rowkey_obj_array(buf, buf_len, pos, (*range)->end_key_.get_obj_ptr(),
              (*range)->end_key_.get_obj_cnt());
        }
      }

      return rc;
    }

    int ObSSTableScanParam::deserialize_ranges(const char *buf, const int64_t &data_len, int64_t &pos)
    {
      int rc = OB_SUCCESS;
      // scan_flag_ deserialize in deserialize_basic_param() which is called
      // before deserialize_ranges(). So, it is safe to call is_local_index_scan() here.
      ObNewRange *ranges[] = { &range_,
        (is_local_index_scan() ? &local_index_range_ : NULL),
        NULL };

      int64_t offset = 0; // offset in rowkey_obj_array_
      for (ObNewRange **range = ranges; OB_SUCCESS == rc && NULL != *range; range++)
      {
        int64_t table_id = OB_INVALID_ID;
        int64_t border_flag = 0;
        int64_t obj_cnt = 0;

        if (OB_SUCCESS == rc )
        {
          rc = get_int_obj_value(buf, data_len, pos, table_id);
        }
        if (OB_SUCCESS == rc )
        {
          (*range)->table_id_ = table_id;
          rc = get_int_obj_value(buf, data_len, pos, border_flag);
        }
        if (OB_SUCCESS == rc )
        {
          (*range)->border_flag_.set_data(static_cast<int8_t>(border_flag));
          rc = get_rowkey_obj_array(buf, data_len, pos, rowkey_obj_array_ + offset, obj_cnt);
        }
        if (OB_SUCCESS == rc)
        {
          (*range)->start_key_.assign(rowkey_obj_array_ + offset, obj_cnt);
          offset += OB_MAX_ROWKEY_COLUMN_NUMBER;

          rc = get_rowkey_obj_array(buf, data_len, pos, rowkey_obj_array_ + offset, obj_cnt);
        }
        if (OB_SUCCESS == rc)
        {
          (*range)->end_key_.assign(rowkey_obj_array_ + offset, obj_cnt);
          offset += OB_MAX_ROWKEY_COLUMN_NUMBER;
        }
      }

      return rc;
    }

    int64_t ObSSTableScanParam::get_ranges_serialize_size(void) const
    {
      int64_t size = 0;

      ObObj obj;
      obj.set_ext(ObActionFlag::NEWRANGE_PARAM_FIELD);
      size += obj.get_serialize_size();

      const ObNewRange *ranges[] = { &range_,
        (is_local_index_scan() ? &local_index_range_ : NULL),
        NULL };
      for (const ObNewRange *const *range = ranges; NULL != *range; range++)
      {
        obj.set_int((*range)->table_id_);
        size += obj.get_serialize_size();

        obj.set_int((*range)->border_flag_.get_data());
        size += obj.get_serialize_size();

        size += get_rowkey_obj_array_size((*range)->start_key_.get_obj_ptr(),
          (*range)->start_key_.get_obj_cnt());
        size += get_rowkey_obj_array_size((*range)->end_key_.get_obj_ptr(),
          (*range)->end_key_.get_obj_cnt());
      }

      return size;
    }

  }
}

