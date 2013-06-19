#include "common/ob_action_flag.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "ob_sql_scan_simple_param.h"

namespace oceanbase
{
  namespace sql
  {

    void ObSqlScanSimpleParam::reset(void)
    {
      ObSqlReadSimpleParam::reset();
      data_range_.table_id_ = OB_INVALID_ID;
      data_range_.start_key_.assign(NULL, 0);
      data_range_.end_key_.assign(NULL, 0);
      scan_flag_.flag_ = 0;
      buffer_pool_.reset();
    }

    // ObSqlScanSimpleParam
    ObSqlScanSimpleParam::ObSqlScanSimpleParam() :
      data_range_(),
      scan_flag_(),
      buffer_pool_(ObModIds::OB_SQL_SCAN_PARAM)
    {
    }

    ObSqlScanSimpleParam::~ObSqlScanSimpleParam()
    {
    }

    int ObSqlScanSimpleParam::set_range(const ObNewRange& range, bool deep_copy_args)
    {
      int err = OB_SUCCESS;
      data_range_ = range;
      if (deep_copy_args)
      {
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(range.start_key_,&(data_range_.start_key_)))))
        {
          TBSYS_LOG(WARN,"fail to copy range.start_key_ to local buffer [err:%d]", err);
        }
        if ((OB_SUCCESS == err) && (OB_SUCCESS != (err = buffer_pool_.write_string(range.end_key_,&(data_range_.end_key_)))))
        {
          TBSYS_LOG(WARN,"fail to copy range.end_key_ to local buffer [err:%d]", err);
        }
      }
      return err;
    }

    // BASIC_PARAM_FIELD
    int ObSqlScanSimpleParam::serialize_basic_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = set_ext_obj_value(buf, buf_len, pos, ObActionFlag::BASIC_PARAM_FIELD)))
      {
        TBSYS_LOG(WARN, "fail to serialize BASIC_PARAM_FIELD. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = set_int_obj_value(buf, buf_len, pos, data_range_.table_id_)))
      {
        TBSYS_LOG(WARN, "fail to serialize table_id_ obj. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = set_int_obj_value(buf, buf_len, pos, data_range_.border_flag_.get_data())))
      {
        TBSYS_LOG(WARN, "fail to serialize border_flag_ obj. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = set_rowkey_obj_array(buf, buf_len, pos,
              data_range_.start_key_.get_obj_ptr(), data_range_.start_key_.get_obj_cnt())))
      {
        TBSYS_LOG(WARN, "fail to serialize start key. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = set_rowkey_obj_array(buf, buf_len, pos,
              data_range_.end_key_.get_obj_ptr(), data_range_.end_key_.get_obj_cnt())))
      {
        TBSYS_LOG(WARN, "fail to serialize end key. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = set_int_obj_value(buf, buf_len, pos, scan_flag_.flag_)))
      {
        TBSYS_LOG(WARN, "fail to serialize scan_flag_ obj. ret=%d", ret);
      }
      return ret;
    }

    int ObSqlScanSimpleParam::deserialize_basic_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      ObObj obj;
      int ret = OB_SUCCESS;
      int64_t table_id = 0;
      int64_t border_flag = 0;
      int64_t start_key_len = OB_MAX_ROWKEY_COLUMN_NUMBER;
      int64_t end_key_len = OB_MAX_ROWKEY_COLUMN_NUMBER;
      // deserialize scan range
      // tid
      if (OB_SUCCESS != (ret = get_int_obj_value(buf, data_len, pos, table_id)))
      {
        TBSYS_LOG(WARN, "fail to deserialize table_id_ obj. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = get_int_obj_value(buf, data_len, pos, border_flag)))
      {
        TBSYS_LOG(WARN, "fail to deserialize border_flag_ obj. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = get_rowkey_obj_array(buf, data_len, pos,
              start_rowkey_obj_array_, start_key_len)))
      {
        TBSYS_LOG(WARN, "fail to get start rowkey.ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = get_rowkey_obj_array(buf, data_len, pos,
              end_rowkey_obj_array_, end_key_len)))
      {
        TBSYS_LOG(WARN, "fail to get end rowkey. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = get_int_obj_value(buf, data_len, pos, scan_flag_.flag_)))
      {
        TBSYS_LOG(WARN, "fail to deserialize scan_flag_ obj. ret=%d", ret);
      }
      else
      {
        data_range_.table_id_ = table_id;
        data_range_.border_flag_.set_data(static_cast<int8_t>(border_flag));
        data_range_.start_key_.assign(start_rowkey_obj_array_, start_key_len);
        data_range_.end_key_.assign(end_rowkey_obj_array_, end_key_len);
      }
      return ret;
    }

    int64_t ObSqlScanSimpleParam::get_basic_param_serialize_size(void) const
    {
      int64_t total_size = 0;
      ObObj obj;
      // BASIC_PARAM_FIELD
      obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
      total_size += obj.get_serialize_size();
      // scan range
      obj.set_int(data_range_.table_id_);
      total_size += obj.get_serialize_size();
      obj.set_int(data_range_.border_flag_.get_data());
      total_size += obj.get_serialize_size();
      // start_key_
      total_size += get_rowkey_obj_array_size(
          data_range_.start_key_.get_obj_ptr(), data_range_.start_key_.get_obj_cnt());
      // end_key_
      total_size += get_rowkey_obj_array_size(
          data_range_.end_key_.get_obj_ptr(), data_range_.end_key_.get_obj_cnt());
      obj.set_int(scan_flag_.flag_);
      total_size += obj.get_serialize_size();
      return total_size;
    }

    DEFINE_SERIALIZE(ObSqlScanSimpleParam)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      // BASIC_PARAM_FIELD
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = ObSqlReadSimpleParam::serialize(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize base ObReadParam. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = serialize_basic_param(buf, buf_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to serialize basic param. buf=%p, buf_len=%ld, pos=%ld, ret=%d", buf, buf_len, pos, ret);
        }
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObSqlScanSimpleParam)
    {
      ObObj obj;
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = ObSqlReadSimpleParam::deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize base ObReadParam. ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
      {
        TBSYS_LOG(WARN, "fail to deserialize meta type. ret=%d. data_len=%ld,pos=%ld", ret, data_len, pos);
      }
      else
      {
        OB_ASSERT(ObExtendType ==obj.get_type() && ObActionFlag::BASIC_PARAM_FIELD == obj.get_ext());
        if (OB_SUCCESS != (ret = deserialize_basic_param(buf, data_len, pos)))
        {
          TBSYS_LOG(WARN, "fail to deserialize basic param. ret=%d", ret);
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObSqlScanSimpleParam)
    {
      int64_t total_size = get_basic_param_serialize_size();
      total_size += ObSqlReadSimpleParam::get_serialize_size();
      return total_size;
    }

    void ObSqlScanSimpleParam::dump(void) const
    {
    }

    int64_t ObSqlScanSimpleParam::to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "ObSqlScanSimpleParam(");
      pos += ObSqlReadSimpleParam::to_string(buf + pos, buf_len - pos);
      databuff_printf(buf, buf_len, pos, ",data_range=%s)", to_cstring(data_range_));
      //TODO: scan flag
      return pos;
    }
  } /* sql */
} /* oceanbase */
