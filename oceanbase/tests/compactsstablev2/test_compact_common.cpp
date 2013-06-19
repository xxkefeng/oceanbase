#include "test_compact_common.h"

using namespace oceanbase;
using namespace compactsstablev2;
using namespace common;
using namespace sstable;

int make_version_range(ObFrozenMinorVersionRange& version_range, const int version_flag)
{
  int ret = OB_SUCCESS;
  version_range.major_version_ = 10;
  version_range.major_frozen_time_ = 2;
  version_range.next_transaction_id_ = 3;
  version_range.next_commit_log_id_ = 4;
  version_range.start_minor_version_ = 5;
  version_range.end_minor_version_ = 6;
  version_range.is_final_minor_version_ = 0;

  if (0 == version_flag)
  {
    //do nothing
  }
  else if (1 == version_flag)
  {
    version_range.major_version_ = -1;
  }
  else if (2 == version_flag)
  {
    version_range.major_frozen_time_ = -1;
  }
  else if (3 == version_flag)
  {
    version_range.next_transaction_id_ = -1;
  }
  else if (4 == version_flag)
  {
    version_range.next_commit_log_id_ = -1;
  }
  else if (5 == version_flag)
  {
    version_range.start_minor_version_ = -1;
  }
  else if (6 == version_flag)
  {
    version_range.end_minor_version_ = -1;
  }
  else if (7 == version_flag)
  {
    version_range.is_final_minor_version_ = -1;
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid version flag: version_flag=[%d]", version_flag);
    ret = OB_ERROR;
  }

  return ret;
}

int make_comp_name(ObString& comp_name, const int comp_flag)
{
  int ret = OB_SUCCESS;
  static char s_comp_name_buf[1024];
  memset(s_comp_name_buf, 0, 1024);

  if (0 == comp_flag)
  {
    comp_name.assign_ptr(NULL, 0);
  }
  else if (1 == comp_flag)
  {
    memcpy(s_comp_name_buf, "invalid", 10);
    comp_name.assign_ptr(s_comp_name_buf, 10);
  }
  else if (2 == comp_flag)
  {
    memcpy(s_comp_name_buf, "none", 10);
    comp_name.assign_ptr(s_comp_name_buf, 10);
  }
  else if (3 == comp_flag)
  {
    memcpy(s_comp_name_buf, "lzo_1.0", 10);
    comp_name.assign_ptr(s_comp_name_buf, 10);
  }
  else if (4 == comp_flag)
  {
    memcpy(s_comp_name_buf, "snappy_1.0", 20);
    comp_name.assign_ptr(s_comp_name_buf, 20);
  }
  else if (5 == comp_flag)
  {
    memcpy(s_comp_name_buf, "lz4_1.0", 10);
    comp_name.assign_ptr(s_comp_name_buf, 10);
  }
  else
  {
    TBSYS_LOG(ERROR, "invlid comp_flag: comp_flag=[%d]", comp_flag);
    ret = OB_ERROR;
  }

  return ret;
}

int make_compressor(ObCompressor*& compressor, const int comp_flag)
{
  int ret = OB_SUCCESS;
  static ObCompressor* compressor_none = NULL;
  static ObCompressor* compressor_lzo = NULL;
  static ObCompressor* compressor_snappy = NULL;
  static ObCompressor* compressor_lz4 = NULL;

  if (0 == comp_flag)
  {
    //do nothing
  }
  else if (1 == comp_flag)
  {
    //do nothing
  }
  else if (2 == comp_flag)
  {
    if (NULL == compressor_none)
    {
      if (NULL == (compressor_none = create_compressor("none")))
      {
        TBSYS_LOG(WARN, "create compressor error: none");
        ret = OB_ERROR;
      }
    }
    compressor = compressor_none;
  }
  else if (3 == comp_flag)
  {
    if (NULL == compressor_lzo)
    {
      if (NULL == (compressor_lzo = create_compressor("lzo_1.0")))
      {
        TBSYS_LOG(WARN, "create compressor error: lzo_1.0");
        ret = OB_ERROR;
      }
    }
    compressor = compressor_lzo;
  }
  else if (4 == comp_flag)
  {
    if (NULL == compressor_snappy)
    {
      if (NULL == (compressor_snappy = create_compressor("snappy_1.0")))
      {
        TBSYS_LOG(WARN, "create compressor error: snappy_1.0");
        ret = OB_ERROR;
      }
    }
    compressor = compressor_snappy;
  }
  else if (5 == comp_flag)
  {
    if (NULL == compressor_lz4)
    {
      if (NULL == (compressor_lz4 = create_compressor("lz4_1.0")))
      {
        TBSYS_LOG(WARN, "create compressor error: lz4_1.0");
        ret = OB_ERROR;
      }
    }
    compressor = compressor_lz4;
  }
  else
  {
    TBSYS_LOG(WARN, "invalid compressor flag: comp_flag=[%d]", comp_flag);
    ret = OB_ERROR;
  }

  return ret;
}

int make_file_path(ObString& file_path, const int64_t file_num)
{
  int ret = OB_SUCCESS;
  static char s_file_path_buf[1024];
  memset(s_file_path_buf, 0, 1024);

  if (file_num >= 0)
  {
    snprintf(s_file_path_buf, 1024, "%ld%s", file_num, ".sst");
    file_path.assign_ptr(s_file_path_buf, 30);
  }
  else if (-1 == file_num)
  {
    snprintf(s_file_path_buf, 1024, "invalid/1.sst");
    file_path.assign_ptr(s_file_path_buf, 30);
  }
  else if (-2 == file_num)
  {
    file_path.assign_ptr(s_file_path_buf, 0);
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid file_num: file_num=[%ld]", file_num);
    ret = OB_ERROR;
  }

  return ret;
}

void make_column_def(compactsstablev2::ObSSTableSchemaColumnDef& def, 
    const uint64_t table_id,
    const uint64_t column_id, 
    const int value_type,
    const int64_t rowkey_seq)
{
  def.table_id_ = table_id;
  def.column_id_ = column_id;
  def.column_value_type_ = value_type;
  def.rowkey_seq_ = rowkey_seq;
}

int make_schema(compactsstablev2::ObSSTableSchema& schema, const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  schema.reset();
  compactsstablev2::ObSSTableSchemaColumnDef def;
  ObObjType type_array[9] = {ObVarcharType, ObIntType, ObDateTimeType, ObPreciseDateTimeType, ObVarcharType, ObCreateTimeType, ObModifyTimeType, ObBoolType, ObDecimalType};

  int64_t rowkey_seq = 0;
  for (int64_t i = 1; i <= 9; i ++)
  {
    if (i <= 2)
    {
      rowkey_seq = i;
    }
    else
    {
      rowkey_seq = 0;
    }
    make_column_def(def, table_id, i, type_array[i - 1], rowkey_seq);
    if (OB_SUCCESS != (ret = schema.add_column_def(def)))
    {
      TBSYS_LOG(WARN, "schema add column def error: schema=[%s], def=[%s]", to_cstring(schema), to_cstring(def));
    }
  }

  return ret;
}

int make_rowkey(ObRowkey& rowkey, const int64_t data, const int flag)
{
  int ret = OB_SUCCESS;
  static ObObj s_rowkey_buf_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  static char s_temp_buf[OB_MAX_VARCHAR_LENGTH];
  ObObj obj;
  ObString temp_string;

  if (0 == flag)
  {
    //column_id=1
    sprintf(s_temp_buf, "%s", "prefix");
    temp_string.assign(s_temp_buf, static_cast<const ObString::obstr_size_t>(strlen(s_temp_buf)));
    obj.set_varchar(temp_string);
    s_rowkey_buf_array[0] = obj;

    //column_id=2
    obj.set_int(data);
    s_rowkey_buf_array[1] = obj;
    
    //rowkey
    rowkey.assign(s_rowkey_buf_array, 2);
  }
  else if (1 == flag)
  {
    rowkey.set_min_row();
  }
  else if (2 == flag)
  {
    rowkey.set_max_row();
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid flag: flag=[%d]", flag);
    ret = OB_ERROR;
  }

  return ret;
}

int make_row(ObRow& row, const uint64_t table_id, const int64_t data, const int flag)
{
  int ret = OB_SUCCESS;
  static ObRowDesc s_desc;
  static char s_temp_buf_1[OB_MAX_VARCHAR_LENGTH];
  static char s_temp_buf_5[OB_MAX_VARCHAR_LENGTH];
  s_desc.reset();
  ObObj obj[10];
  ObString temp_string;
  //ObObj obj_1, obj_2, obj_3, obj_4, obj_5, obj_6, obj_7, obj_8, obj_9;

  //common
  //column_id=1
  sprintf(s_temp_buf_1, "%s", "prefix");
  temp_string.assign(s_temp_buf_1, static_cast<ObString::obstr_size_t>(strlen(s_temp_buf_1)));
  obj[1].set_varchar(temp_string);

  //column_id=2
  obj[2].set_int(data);

  //column_id=3
  obj[3].set_datetime(data);

  //column_id=4
  obj[4].set_precise_datetime(data);

  //column_id=5
  sprintf(s_temp_buf_5, "%ld", data);
  temp_string.assign_ptr(s_temp_buf_5, static_cast<const ObString::obstr_size_t>(strlen(s_temp_buf_5)));
  obj[5].set_varchar(temp_string);

  //column_id=6
  obj[6].set_createtime(data);

  //column_id=7
  obj[7].set_modifytime(data);

  //column_id=8
  bool temp_bool= data % 2 ? true : false;
  obj[8].set_bool(temp_bool);

  //column_id=9
  ObNumber temp_num;
  temp_num.from(data);
  obj[9].set_decimal(temp_num);

  if (0 == flag)
  {//normal
    for (int64_t i = 1; i <= 9; i ++)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, i)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      s_desc.set_rowkey_cell_count(2);
      row.set_row_desc(s_desc);
    }

    if (OB_SUCCESS == ret)
    {
      for (int64_t i = 1; i <= 9; i ++)
      {
        if (OB_SUCCESS != (ret = row.set_cell(table_id, i, obj[i])))
        {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], i=[%ld], obj=[%s]", ret, table_id, i, to_cstring(obj[i]));
          break;
        }
      }
    }
  }
  else if (1 == flag)
  {//del row
    for (int64_t i = 1; i <= 2; i ++)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, i)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d], table_id=[%lu]", ret, table_id);
      }
    }

    if (OB_SUCCESS == ret)
    {
      s_desc.set_rowkey_cell_count(2);
      row.set_row_desc(s_desc);
    }

    if (OB_SUCCESS == ret)
    {
      for (int64_t i = 1; i <= 2; i ++)
      {
        if (OB_SUCCESS != (ret = row.set_cell(table_id, i, obj[i])))
        {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], i=[%ld], obj=[%s]", ret, table_id, i, to_cstring(obj[i]));
          break;
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      obj[0].set_ext(ObActionFlag::OP_DEL_ROW);
      if (OB_SUCCESS != (ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj[0])))
      {
        TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], obj=[%s]", ret, table_id, to_cstring(obj[0]));
      }
    }
  }
  else if (2 == flag)
  {//new add
    for (int64_t i = 1; i <= 2; i ++)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, i)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d], table_id=[%lu]", ret, table_id);
      }
    }

    if (OB_SUCCESS == ret)
    {
      s_desc.set_rowkey_cell_count(2);
      row.set_row_desc(s_desc);
    }

    if (OB_SUCCESS == ret)
    {
      for (int64_t i = 1; i <= 2; i ++)
      {
        if (OB_SUCCESS != (ret = row.set_cell(table_id, i, obj[i])))
        {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], i=[%ld], obj=[%s]", ret, table_id, i, to_cstring(obj[i]));
          break;
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      obj[0].set_ext(ObActionFlag::OP_NEW_ADD);
      if (OB_SUCCESS != (ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj[0])))
      {
        TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], obj=[%s]", ret, table_id, to_cstring(obj[0]));
      }
    }
  }
  else if (3 == flag)
  {//no row value
    for (int64_t i = 1; i <= 2; i ++)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, i)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      s_desc.set_rowkey_cell_count(2);
      row.set_row_desc(s_desc);
    }

    if (OB_SUCCESS == ret)
    {
      for (int64_t i = 1; i <= 2; i ++)
      {
        if (OB_SUCCESS != (ret = row.set_cell(table_id, i, obj[i])))
        {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], i=[%ld], obj=[%s]", ret, table_id, i, to_cstring(obj[i]));
          break;
        }
      }
    }
  }
  else if (4 == flag)
  {//normal(scan, dense_dense)
    for (int64_t i = 1; i <= 9; i ++)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, i)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      s_desc.set_rowkey_cell_count(2);
      row.set_row_desc(s_desc);
    }

    if (OB_SUCCESS == ret)
    {
      for (int64_t i = 1; i <= 9; i ++)
      {
        if (OB_SUCCESS != (ret = row.set_cell(table_id, i, obj[i])))
        {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], i=[%ld], obj=[%s]", ret, table_id, i, to_cstring(obj[i]));
          break;
        }
      }
    }
  }
  else if (5 == flag)
  {//normal(scan, dense_sparse)
    for (int64_t i = 1; i <= 9; i ++)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, i)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      s_desc.set_rowkey_cell_count(2);
      row.set_row_desc(s_desc);
    }

    if (OB_SUCCESS == ret)
    {
      for (int64_t i = 1; i <= 9; i ++)
      {
        if (OB_SUCCESS != (ret = row.set_cell(table_id, i, obj[i])))
        {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], i=[%ld], obj=[%s]", ret, table_id, i, to_cstring(obj[i]));
          break;
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      obj[0].set_ext(ObActionFlag::OP_VALID);

      if (OB_SUCCESS != (ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj[0])))
      {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], obj=[%s]", ret, table_id, to_cstring(obj[0]));
      }
    }
  }
  else if (6 == flag)
  {//del row(scan, dense_sparse)
    for (int64_t i = 1; i <= 9; i ++)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, i)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      s_desc.set_rowkey_cell_count(2);
      row.set_row_desc(s_desc);
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = row.reset(false, ObRow::DEFAULT_NOP)))
      {
        TBSYS_LOG(WARN, "row reset error: ret=[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      obj[0].set_ext(ObActionFlag::OP_DEL_ROW);

      if (OB_SUCCESS != (ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj[0])))
      {
        TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], obj[0]=[%s]", ret, table_id, to_cstring(obj[0]));
      }
    }
  }
  else if (7 == flag)
  {//new add(scan, dense_sparse)
    for (int64_t i = 1; i <= 9; i ++)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, i)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      s_desc.set_rowkey_cell_count(2);
      row.set_row_desc(s_desc);
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = row.reset(false, ObRow::DEFAULT_NOP)))
      {
        TBSYS_LOG(WARN, "row reset error: ret=[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      for (int64_t i = 1; i <= 9; i ++)
      {
        if (OB_SUCCESS != (ret = row.set_cell(table_id, i, obj[i])))
        {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], i=[%ld], obj=[%s]", ret, table_id, i, to_cstring(obj[i]));
          break;
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      obj[0].set_ext(ObActionFlag::OP_NEW_ADD);

      if (OB_SUCCESS != (ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj[0])))
      {
        TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], obj[0]=[%s]", ret, table_id, to_cstring(obj[0]));
      }
    }
  }
  else if (8 == flag)
  {//no row value(scan, dense_dense)
    for (int64_t i = 1; i <= 9; i ++)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, i)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      s_desc.set_rowkey_cell_count(2);
      row.set_row_desc(s_desc);
    }

    if (OB_SUCCESS == ret)
    {
      for (int64_t i = 1; i <= 2; i ++)
      {
        if (OB_SUCCESS != (ret = row.set_cell(table_id, i, obj[i])))
        {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], i=[%ld], obj=[%s]", ret, table_id, i, to_cstring(obj[i]));
          break;
        }
      }
    }
  }
  else if (9 == flag)
  {//no row value(scan, dense_sparse)
    for (int64_t i = 1; i <= 9; i ++)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, i)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID)))
      {
        TBSYS_LOG(WARN, "desc add column desc error: ret=[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      s_desc.set_rowkey_cell_count(2);
      row.set_row_desc(s_desc);
    }

    if (OB_SUCCESS == ret)
    {
      for (int64_t i = 1; i <= 2; i ++)
      {
        if (OB_SUCCESS != (ret = row.set_cell(table_id, i, obj[i])))
        {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], i=[%ld], obj=[%s]", ret, table_id, i, to_cstring(obj[i]));
          break;
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      obj[0].set_ext(ObActionFlag::OP_VALID);

      if (OB_SUCCESS != (ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj[0])))
      {
          TBSYS_LOG(WARN, "row set cell error: ret=[%d], table_id=[%lu], obj=[%s]", ret, table_id, to_cstring(obj[0]));
      }
    }
  }
  else
  {
    TBSYS_LOG(ERROR, "invalid flag:flag=%d", flag);
    ret = OB_ERROR;
  }

  return ret;
}

int make_range(ObNewRange& range,
    const uint64_t table_id,
    const int64_t start,
    const int64_t end,
    const int start_flag,
    const int end_flag)
{
  int ret = OB_SUCCESS;
  static ObObj s_startkey_buf_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  static ObObj s_endkey_buf_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  static char s_startkey_buf_str[OB_MAX_VARCHAR_LENGTH];
  static char s_endkey_buf_str[OB_MAX_VARCHAR_LENGTH];
  //static char s_temp_buf[OB_MAX_VARCHAR_LENGTH];
  ObString temp_string;
  ObObj obj;
  range.reset();

  //range.table_id_
  range.table_id_ = table_id;

  //range.start_key_
  if (0 == start_flag)
  {
    range.start_key_.set_min_row();
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.set_min_value();
  }
  else
  {
    //column_id=1
    sprintf(s_startkey_buf_str, "%s", "prefix");
    temp_string.assign(s_startkey_buf_str, static_cast<const ObString::obstr_size_t>(strlen(s_startkey_buf_str)));
    obj.set_varchar(temp_string);
    s_startkey_buf_array[0] = obj;

    //column_id=2
    obj.set_int(start);
    s_startkey_buf_array[1] = obj;

    if (1 == start_flag)
    {
      range.border_flag_.unset_inclusive_start();
    }
    else if (2 == start_flag)
    {
      range.border_flag_.set_inclusive_start();
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid end_flag: start_flag=[%d]", start_flag);
      ret = OB_ERROR;
    }

    range.start_key_.assign(s_startkey_buf_array, 2);
  }

  //range.end_key_
  if (0 == end_flag)
  {
    range.end_key_.set_max_row();
    range.border_flag_.unset_inclusive_end();
    range.border_flag_.set_max_value();
  }
  else
  {
    //column_id=1
    sprintf(s_endkey_buf_str, "%s", "prefix");
    temp_string.assign(s_endkey_buf_str, static_cast<const ObString::obstr_size_t>(strlen(s_endkey_buf_str)));
    obj.set_varchar(temp_string);
    s_endkey_buf_array[0] = obj;

    //column_id=2
    obj.set_int(end);
    s_endkey_buf_array[1] = obj;

    if (1 == end_flag)
    {
      range.border_flag_.unset_inclusive_end();
    }
    else if (2 == end_flag)
    {
      range.border_flag_.set_inclusive_end();
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid end_flag: start_flag=[%d]", end_flag);
      ret = OB_ERROR;
    }

    range.end_key_.assign(s_endkey_buf_array, 2);
  }

  return ret;
}

int make_scan_param(ObSSTableScanParam& sstable_scan_param,
    const uint64_t table_id,
    const int64_t start,
    const int64_t end,
    const int start_flag,
    const int end_flag,
    const int scan_flag,
    const int read_flag,
    const uint64_t* column_array,
    const int64_t column_cnt,
    const bool not_exit_col_ret_nop,
    const uint64_t full_row_scan_flag,
    const uint64_t daily_merge_scan_flag,
    const int64_t rowkey_column_cnt)
{
  int ret = OB_SUCCESS;
  static ObNewRange s_range;
  enum ScanFlag::Direction direction = ScanFlag::FORWARD;
  enum ScanFlag::SyncMode sync_mode = ScanFlag::SYNCREAD;

  //reset
  sstable_scan_param.reset();

  //range
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = make_range(s_range, table_id, start, end, start_flag, end_flag)))
    {
      TBSYS_LOG(WARN, "make range error: ret=[%d], table_id=[%lu], start=[%ld], end=[%ld], start_flag=[%d], end_flag=[%d]", ret, table_id, start, end, start_flag, end_flag);
    }
    else
    {
      sstable_scan_param.set_range(s_range);
    }
  }

  //scan flag
  if (OB_SUCCESS == ret)
  {
    if (0 == scan_flag)
    {
      direction = ScanFlag::FORWARD;
    }
    else if (1 == scan_flag)
    {
      direction = ScanFlag::BACKWARD;
    }
  }

  //read flag
  if (OB_SUCCESS == ret)
  {
    if (0 == read_flag)
    {
      sync_mode = ScanFlag::SYNCREAD;
    }
    else if (1 == read_flag)
    {
      sync_mode = ScanFlag::ASYNCREAD;
    }
  }

  //scan flag
  if (OB_SUCCESS == ret)
  {
    ScanFlag scan_flag(sync_mode, direction, not_exit_col_ret_nop, daily_merge_scan_flag, full_row_scan_flag, 2);
    sstable_scan_param.set_scan_flag(scan_flag);
  }

  //scan_column
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < column_cnt; i ++)
    {
      if (OB_SUCCESS != (ret = sstable_scan_param.add_column(column_array[i])))
      {
        TBSYS_LOG(WARN, "sstable scan param add column error: i=[%ld], ret=[%d], column_id=[%lu]", i, ret, column_array[i]);
        break;
      }
    }
  }

  //rowkey column count
  if (OB_SUCCESS == ret && 0 != rowkey_column_cnt)
  {
    sstable_scan_param.set_rowkey_column_count(static_cast<int16_t>(rowkey_column_cnt));
  }

  return ret;
}

int create_file(const uint64_t file_id,
    const int comp_flag,
    const ObCompactStoreType store_type,
    const uint64_t* table_id,
    const int64_t table_count,
    const int64_t* range_start,
    const int64_t* range_end,
    const int* range_start_flag,
    const int* range_end_flag,
    const int64_t* row_data,
    const int* ext_flag,
    const int64_t* row_count)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  compactsstablev2::ObSSTableSchema schema;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  ObString file_path;

  if (OB_SUCCESS != (ret = make_version_range(version_range, 0)))
  {
    TBSYS_LOG(WARN, "make version range error: ret=[%d], version_range=[%s]", ret, to_cstring(version_range));
  }
  else if (OB_SUCCESS != (ret = make_comp_name(comp_name, comp_flag)))
  {
    TBSYS_LOG(WARN, "make comp name error: ret=[%d], comp_name=[%s], comp_flag=[%d]", ret, to_cstring(comp_name), comp_flag);
  }
  else if (OB_SUCCESS != (ret = make_file_path(file_path, file_id)))
  {
    TBSYS_LOG(WARN, "make file path error: ret=[%d], file_path=[%s], fild_id=[%lu]", ret, to_cstring(file_path), file_id);
  }
  else if (OB_SUCCESS != (ret = writer.set_sstable_param(version_range, store_type, table_count, block_size, comp_name, def_sstable_size, min_split_sstable_size)))
  {
    TBSYS_LOG(WARN, "writer set sstable param error: ret=[%d], version_range=[%s], store_type=[%d], table_count=[%ld], block_size=[%ld], comp_name=[%s], def_sstable_size=[%ld], min_split_sstable_size=[%ld]", ret, to_cstring(version_range), store_type, table_count, block_size, to_cstring(comp_name), def_sstable_size, min_split_sstable_size);
  }

  ObRow row;
  ObRowkey rowkey;
  bool is_split = false;
  int64_t temp_i = 0;

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < table_count; i ++)
    {
      if (OB_SUCCESS != (ret = make_range(range, table_id[i], range_start[i], range_end[i], range_start_flag[i], range_end_flag[i])))
      {
        TBSYS_LOG(WARN, "make range error: i=[%ld], table_id=[%lu], start=[%ld], end=[%ld], start_flag=[%d], end_flag=[%d]", i, table_id[i], range_start[i], range_end[i], range_start_flag[i], range_end_flag[i]);
        break;
      }
      else if (OB_SUCCESS != (ret = make_schema(schema, table_id[i])))
      {
        TBSYS_LOG(WARN, "make schema error: i=[%ld], table_id=[%lu]", i, table_id[i]);
        break;
      }
      else if (OB_SUCCESS != (ret = writer.set_table_info(table_id[i], schema, range)))
      {
        TBSYS_LOG(WARN, "writer set table info error: i=[%ld], ret=[%d], table_id=[%lu], schema=[%s], range=[%s]", i, ret, table_id[i], to_cstring(schema), to_cstring(range));
        break;
      }

      if (OB_SUCCESS == ret && 0 == i)
      {
        if (OB_SUCCESS != (ret = writer.set_sstable_filepath(file_path)))
        {
          TBSYS_LOG(WARN, "writer set sstable filepath error: i=[%ld], ret=[%d], file_path=[%s]", i, ret, to_cstring(file_path));
          break;
        }
      }

      for (int64_t j = 0; j < row_count[i]; j ++)
      {
        if (OB_SUCCESS != (ret = make_row(row, table_id[i], row_data[temp_i], ext_flag[temp_i])))
        {
          TBSYS_LOG(WARN, "make row error: j=[%ld], ret=[%d], row=[%s], table_id=[%lu]", j, ret, to_cstring(row), table_id[i]);
          break;
        }
        else if (OB_SUCCESS != (ret = writer.append_row(row, is_split)))
        {
          TBSYS_LOG(WARN, "writer append row error: j=[%ld], ret=[%d], row=[%s], is_split=[%d]", j, ret, to_cstring(row), is_split);
          break;
        }
        else
        {
          temp_i ++;
        }
      }

      if (OB_SUCCESS != ret)
      {
        break;
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = writer.finish()))
    {
      TBSYS_LOG(WARN, "writer finish error: ret=[%d]", ret); 
    }
  }

  return ret;
}

void delete_file(const int64_t i)
{
  ObString file_path;
  make_file_path(file_path, i);
  remove(file_path.ptr());
}
