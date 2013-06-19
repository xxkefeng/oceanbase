#include "ob_sql_read_simple_param.h"
#include "common/utility.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObSqlReadSimpleParam::ObSqlReadSimpleParam() :
  is_read_master_(true),
  is_result_cached_(false),
  is_only_static_data_(false),
  data_version_(OB_NEWEST_DATA_VERSION),
  table_id_(OB_INVALID_ID),
  renamed_table_id_(OB_INVALID_ID),
  request_timeout_(0),
  column_id_list_(OB_MAX_COLUMN_NUMBER, column_ids_)
{
}

ObSqlReadSimpleParam::~ObSqlReadSimpleParam()
{
}

void ObSqlReadSimpleParam::reset(void)
{
  is_read_master_ = true;
  is_result_cached_ = false;
  is_only_static_data_ = false;
  data_version_ = OB_NEWEST_DATA_VERSION;
  column_id_list_.clear();
  table_id_ = OB_INVALID_ID;
  renamed_table_id_ = OB_INVALID_ID;
  request_timeout_ = 0;
}

DEFINE_SERIALIZE(ObSqlReadSimpleParam)
{
  int ret = OB_SUCCESS;
  ObObj obj;

  // is read master 
  if (OB_SUCCESS == ret)
  {
    obj.set_int(get_is_read_consistency());
    ret = obj.serialize(buf, buf_len, pos);
  }
  // is cache
  if (OB_SUCCESS == ret)
  {
    obj.set_int(get_is_result_cached());
    ret = obj.serialize(buf, buf_len, pos);
  }
  // is static data
  if (OB_SUCCESS == ret)
  {
    obj.set_int(get_is_only_static_data());
    ret = obj.serialize(buf, buf_len, pos);
  }
  // data version
  if (OB_SUCCESS == ret)
  {
    obj.set_int(get_data_version());
    ret = obj.serialize(buf, buf_len, pos);
  }
  // table id
  if (OB_SUCCESS == ret)
  {
    obj.set_int(get_table_id());
    ret = obj.serialize(buf, buf_len, pos);
  }
   // renamed table id
  if (OB_SUCCESS == ret)
  {
    obj.set_int(get_renamed_table_id());
    ret = obj.serialize(buf, buf_len, pos);
  }
  // request timeout
  if (OB_SUCCESS == ret)
  {
    obj.set_int(get_request_timeout());
    ret = obj.serialize(buf, buf_len, pos);
  }
 
  // column IDs
  int64_t column_count = get_column_id_size();
  if (OB_SUCCESS == ret)
  {
    obj.set_int(column_count);
    ret = obj.serialize(buf, buf_len, pos);
  }
  for (int i = 0; (OB_SUCCESS == ret) && (i < column_count); i++)
  {
    obj.set_int(*column_id_list_.at(i));
    ret = obj.serialize(buf, buf_len, pos);
  }

  return ret;
}


DEFINE_DESERIALIZE(ObSqlReadSimpleParam)
{
  int ret = OB_SUCCESS;
  int64_t value = 0;
  ObObj obj; 

  // is read master
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = obj.get_int(value)))
    {
      TBSYS_LOG(WARN, "fail to get value. ret=%d, obj=%s", ret, to_cstring(obj));
    }
    else
    {
      set_is_read_consistency(value);
    }
  }
  // is cache
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = obj.get_int(value)))
    {
      TBSYS_LOG(WARN, "fail to get value. ret=%d, obj=%s", ret, to_cstring(obj));
    }
    else
    {
      set_is_result_cached(value);
    }
  }
 
  // is static data 
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = obj.get_int(value)))
    {
      TBSYS_LOG(WARN, "fail to get value. ret=%d, obj=%s", ret, to_cstring(obj));
    }
    else
    {
      set_is_only_static_data(value);
    }
  }

  // data version
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = obj.get_int(value)))
    {
      TBSYS_LOG(WARN, "fail to get value. ret=%d, obj=%s", ret, to_cstring(obj));
    }
    else
    {
      set_data_version(value);
    }
  }

  // table id
  uint64_t tid = OB_INVALID_ID;
  uint64_t renamed_tid = OB_INVALID_ID;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = obj.get_int(value)))
    {
      TBSYS_LOG(WARN, "fail to get value. ret=%d, obj=%s", ret, to_cstring(obj));
    }
    else
    {
      tid = value;
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = obj.get_int(value)))
    {
      TBSYS_LOG(WARN, "fail to get value. ret=%d, obj=%s", ret, to_cstring(obj));
    }
    else
    {
      renamed_tid = value;
    }
  }
  if (OB_SUCCESS == ret)
  {
      set_table_id(renamed_tid, tid);
  }
 
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = obj.get_int(value)))
    {
      TBSYS_LOG(WARN, "fail to get value. ret=%d, obj=%s", ret, to_cstring(obj));
    }
    else
    {
      set_request_timeout(value);
    }
  }
 
  // column id list 
  int64_t column_count = 0;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = obj.get_int(value)))
    {
      TBSYS_LOG(WARN, "fail to get value. ret=%d, obj=%s", ret, to_cstring(obj));
    }
    else
    {
      column_count = value;
      OB_ASSERT(column_count > 0);
    }
  }
  for (int i = 0; (OB_SUCCESS == ret) && (i < column_count); i++)
  {
    if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize. ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = obj.get_int(value)))
    {
      TBSYS_LOG(WARN, "fail to get value. ret=%d, obj=%s", ret, to_cstring(obj));
    }
    else
    {
      if (false == column_id_list_.push_back(value))
      {
        TBSYS_LOG(WARN, "fail to push value to column_id_list. size=%ld, index=%ld", 
            column_id_list_.get_array_size(), column_id_list_.get_array_index());
        ret = OB_NOT_INIT;
      }
    }
  }
  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(ObSqlReadSimpleParam)
{
  int64_t pos = 0;
  ObObj obj;

  obj.set_int(get_is_read_consistency());
  pos += obj.get_serialize_size();
  obj.set_int(get_is_result_cached());
  pos += obj.get_serialize_size();
  obj.set_int(get_is_only_static_data());
  pos += obj.get_serialize_size();
  obj.set_int(get_data_version());
  pos += obj.get_serialize_size();
  obj.set_int(get_table_id());
  pos += obj.get_serialize_size();
  obj.set_int(get_column_id_size());
  pos += obj.get_serialize_size();
  for (int i = 0; i < get_column_id_size(); i++)
  {
    obj.set_int(*column_id_list_.at(i));
    pos += obj.get_serialize_size();
  }
  return pos;
}

int64_t ObSqlReadSimpleParam::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "is_read_master_=%d ", is_read_master_);
  databuff_printf(buf, buf_len, pos, "is_result_cached_=%d ", is_result_cached_);
  databuff_printf(buf, buf_len, pos, "is_only_static_data_=%d ", is_only_static_data_);
  databuff_printf(buf, buf_len, pos, "data_version_=%ld ", data_version_);
  databuff_printf(buf, buf_len, pos, "table_id_=%lu ", table_id_);
  databuff_printf(buf, buf_len, pos, "renamed_table_id_=%lu ", renamed_table_id_);
  const int64_t count = column_id_list_.get_array_index();
  for (int64_t i=0; i<count; ++i)
  {
    databuff_printf(buf, buf_len, pos, "column_ids_[%ld]=%lu ", i, column_ids_[i]);
  }
  return pos;
}
