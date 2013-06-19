#include "ob_object.h"
#include "ob_action_flag.h"
#include "ob_common_param.h"
#include "ob_schema.h"
#include "ob_rowkey_helper.h"

namespace oceanbase
{
  namespace common
  {
    bool ObCellInfo::operator == (const ObCellInfo & other) const
    {
      return ((table_name_ == other.table_name_) && (table_id_ == other.table_id_)
          && (row_key_ == other.row_key_) && (column_name_ == other.column_name_)
          && (column_id_ == other.column_id_) && (value_ == other.value_));
    }

    //-------------------------------------------------------------------------------
    int set_ext_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_ext(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_ext_obj_value(char * buf, const int64_t buf_len, int64_t & pos, int64_t& value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
          && ObExtendType == obj.get_type())
      {
        ret = obj.get_ext(value);
      }
      return ret;
    }

    int set_int_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_int(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_int_obj_value(const char* buf, const int64_t buf_len, int64_t & pos, int64_t & int_value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
          && ObIntType == obj.get_type())
      {
        ret = obj.get_int(int_value);
      }
      return ret;
    }

    int set_str_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const ObString &value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_varchar(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_str_obj_value(const char* buf, const int64_t buf_len, int64_t & pos, ObString & str_value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
          && ObVarcharType == obj.get_type())
      {
        ret = obj.get_varchar(str_value);
      }
      return ret;
    }


    int set_rowkey_obj_array(char* buf, const int64_t buf_len, int64_t & pos, const ObObj* array, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == (ret = set_int_obj_value(buf, buf_len, pos, size)))
      {
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          ret = array[i].serialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    int get_rowkey_obj_array(const char* buf, const int64_t buf_len, int64_t & pos, ObObj* array, int64_t& size)
    {
      int ret = OB_SUCCESS;
      size = 0;
      if (OB_SUCCESS == (ret = get_int_obj_value(buf, buf_len, pos, size)))
      {
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          ret = array[i].deserialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    int64_t get_rowkey_obj_array_size(const ObObj* array, const int64_t size)
    {
      int64_t total_size = 0;
      ObObj obj;
      obj.set_int(size);
      total_size += obj.get_serialize_size();

      for (int64_t i = 0; i < size; ++i)
      {
        total_size += array[i].get_serialize_size();
      }
      return total_size;
    }

    int get_rowkey_compatible(const char* buf, const int64_t buf_len, int64_t & pos,
        const ObRowkeyInfo& info, ObObj* array, int64_t& size, bool& is_binary_rowkey)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      int64_t obj_count = 0;
      ObString str_value;

      is_binary_rowkey = false;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos)) )
      {
        if (ObIntType == obj.get_type() && (OB_SUCCESS == (ret = obj.get_int(obj_count))))
        {
          // new rowkey format.
          for (int64_t i = 0; i < obj_count && OB_SUCCESS == ret; ++i)
          {
            if (i >= size)
            {
              ret = OB_SIZE_OVERFLOW;
            }
            else
            {
              ret = array[i].deserialize(buf, buf_len, pos);
            }
          }

          if (OB_SUCCESS == ret) size = obj_count;
        }
        else if (ObVarcharType == obj.get_type() && OB_SUCCESS == (ret = obj.get_varchar(str_value)))
        {
          is_binary_rowkey = true;
          // old fashion , binary rowkey stream
          if (size < info.get_size())
          {
            TBSYS_LOG(WARN, "input size=%ld not enough, need rowkey obj size=%ld", size, info.get_size());
            ret = OB_SIZE_OVERFLOW;
          }
          else if (str_value.length() == 0)
          {
            // allow empty binary rowkey , incase min, max range.
            size = 0;
          }
          else if (str_value.length() < info.get_binary_rowkey_length())
          {
            TBSYS_LOG(WARN, "binary rowkey length=%d < need rowkey length=%ld",
                str_value.length(), info.get_binary_rowkey_length());
            ret = OB_SIZE_OVERFLOW;
          }
          else
          {
            size = info.get_size();
            ret = ObRowkeyHelper::binary_rowkey_to_obj_array(info, str_value, array, size);
          }
        }
      }

      return ret;
    }

    int get_rowkey_info_from_sm(const ObSchemaManagerV2* schema_mgr,
        const uint64_t table_id, const ObString& table_name, ObRowkeyInfo& rowkey_info)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* tbl = NULL;
      if (NULL == schema_mgr)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (table_id > 0 && table_id != OB_INVALID_ID)
      {
        tbl = schema_mgr->get_table_schema(table_id);
      }
      else if (NULL != table_name.ptr())
      {
        tbl = schema_mgr->get_table_schema(table_name);
      }

      if (NULL == tbl)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        rowkey_info = tbl->get_rowkey_info();
      }
      return ret;
    }

  } /* common */
} /* oceanbase */
