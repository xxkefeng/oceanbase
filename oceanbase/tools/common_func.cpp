/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         test.cpp is for what ...
 *
 *  Version: $Id: test.cpp 2010年11月17日 16时12分46秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



#include <assert.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <set>
#include "common_func.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/serialization.h"
#include "common/ob_malloc.h"
#include "common/utility.h"
#include "sstable/ob_sstable_reader.h"
#include "sstable/ob_sstable_scan_param.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
using namespace serialization;
using namespace chunkserver;

hash::ObHashMap<const char*, int> obj_type_map;

void init_obj_type_map_()
{
  static bool inited = false;
  if (!inited)
  {
    obj_type_map.create(16);
    obj_type_map.set("null", ObNullType);
    obj_type_map.set("int", ObIntType);
    obj_type_map.set("float", ObFloatType);
    obj_type_map.set("double", ObDoubleType);
    obj_type_map.set("date_time", ObDateTimeType);
    obj_type_map.set("precise_date_time", ObPreciseDateTimeType);
    obj_type_map.set("var_char", ObVarcharType);
    //obj_type_map.set("seq", ObSeqType);
    obj_type_map.set("create_time", ObCreateTimeType);
    obj_type_map.set("modify_time", ObModifyTimeType);
    inited = true;
  }
}

/* 从min和max中返回一个随机值 */

int64_t random_number(int64_t min, int64_t max)
{
  static int dev_random_fd = -1;
  char *next_random_byte;
  int bytes_to_read;
  int64_t random_value = 0;

  assert(max > min);

  if (dev_random_fd == -1)
  {
    dev_random_fd = open("/dev/urandom", O_RDONLY);
    assert(dev_random_fd != -1);
  }

  next_random_byte = (char *)&random_value;
  bytes_to_read = sizeof(random_value);

  /* 因为是从/dev/random中读取，read可能会被阻塞，一次读取可能只能得到一个字节，
   * 循环是为了让我们读取足够的字节数来填充random_value.
   */
  do
  {
    int bytes_read;
    bytes_read = static_cast<int32_t>(read(dev_random_fd, next_random_byte, bytes_to_read));
    bytes_to_read -= bytes_read;
    next_random_byte += bytes_read;
  } while(bytes_to_read > 0);

  return min + (abs(static_cast<int32_t>(random_value)) % (max - min + 1));
}


int parse_number_range(const char *number_string,
    int32_t *number_array, int32_t &number_array_size)
{
  int ret = OB_ERROR;
  if (NULL != strstr(number_string, ","))
  {
    ret = parse_string_to_int_array(number_string, ',', number_array, number_array_size);
    if (ret) return ret;
  }
  else if (NULL != strstr(number_string, "~"))
  {
    int32_t min_max_array[2];
    min_max_array[0] = 0;
    min_max_array[1] = 0;
    int tmp_size = 2;
    ret = parse_string_to_int_array(number_string, '~', min_max_array, tmp_size);
    if (ret) return ret;
    int32_t min = min_max_array[0];
    int32_t max = min_max_array[1];
    int32_t index = 0;
    for (int i = min; i <= max; ++i)
    {
      if (index > number_array_size) return OB_SIZE_OVERFLOW;
      number_array[index++] = i;
    }
    number_array_size = index;
  }
  else
  {
    number_array[0] = static_cast<int32_t>(strtol(number_string, NULL, 10));
    number_array_size = 1;
  }
  return OB_SUCCESS;
}

int number_to_hex_buf(const char* sp, const int len, char* hex, const int64_t hexlen, int64_t &pos)
{
  const int max_len = 32;
  assert(len < max_len-1);
  char number[max_len];
  memset(number, 0, max_len);
  strncpy(number, sp, len);
  int64_t value = strtoll(number, NULL, 10);
  return encode_i64(hex, hexlen, pos, value);
}

int parse_string(const char* src, const char del, const char* dst[], int64_t& size)
{
  int ret = OB_SUCCESS;
  int64_t obj_index = 0;

  char *str = (char*)ob_malloc(OB_MAX_FILE_NAME_LENGTH);
  strcpy(str, src);
  str[strlen(src)] = 0;

  const char* st_ptr = str;
  char* et_ptr = NULL;
  char* last_ptr = str + strlen(str) - 1;

  //skip white space;
  while (*st_ptr != 0 && *st_ptr == ' ') ++st_ptr;

  while (NULL != st_ptr && NULL != (et_ptr = strchr((char*)st_ptr, del)))
  {
    //set del character to '\0'
    *et_ptr = 0;

    if (obj_index >= size)
    {
      ret = OB_SIZE_OVERFLOW;
      break;
    }
    else
    {
      dst[obj_index++] = st_ptr;
      st_ptr = et_ptr + 1;
      //skip white space;
      while (*st_ptr != 0 && *st_ptr == ' ') ++st_ptr;
    }
  }

  // last item;
  if (OB_SUCCESS == ret && *st_ptr != 0 && obj_index < size)
  {
    while (last_ptr > st_ptr && *last_ptr == ' ')
    {
      *last_ptr = 0;
      --last_ptr;
    }
    dst[obj_index++] = st_ptr;
  }

  if (OB_SUCCESS == ret)
  {
    size = obj_index;
  }
  return ret;
}

int parse_object(const char* object_str, ObObj& obj)
{
  int ret = OB_SUCCESS;
  // int:10
  int64_t size = 2;
  const char* dst[size];
  static const char* obj_type_name[] =
  {
    "null",
    "int",
    "float",
    "double",
    "datetime",
    "precisedatetime",
    "varchar",
    "seq",
    "createtime",
    "modifytime",
    "extend",
    "bool",
    "decimal"
  };

  if (OB_SUCCESS != (ret = parse_string(object_str, ':', dst, size)))
  {
    return ret;
  }

  if (size == 1)
  {
    if (strcasecmp(dst[0], "MIN") == 0)
    {
      obj.set_min_value();
    }
    else if (strcasecmp(dst[0], "MAX") == 0)
    {
      obj.set_max_value();
    }
    else
    {
      fprintf(stderr, "incorrect format [%s], not min or max value.\n", object_str);
      ret = OB_ERROR;
    }
  }
  else if (size == 2)
  {
    int type = 0;
    bool found = false;
    for (; type < (int)sizeof(obj_type_name); ++type)
    {
      const char* type_name = obj_type_name[type];
      if (strcasecmp(dst[0], type_name) == 0)
      {
        found = true;
        break;
      }
    }

    if (found)
    {
      ObString varchar;

      switch(type)
      {
        case ObNullType:
          obj.set_null();
          break;
        case ObIntType:
          obj.set_int(strtoll(dst[1], NULL, 10));
          break;
        case ObVarcharType:
          varchar.assign((char*)dst[1], static_cast<int32_t>(strlen(dst[1])));
          obj.set_varchar(varchar);
          break;
        case ObFloatType:
          obj.set_float(static_cast<float>(atof(dst[1])));
          break;
        case ObDoubleType:
          obj.set_double(atof(dst[1]));
          break;
        case ObDateTimeType:
          obj.set_datetime(strtoll(dst[1], NULL, 10));
          break;
        case ObPreciseDateTimeType:
          obj.set_datetime(strtoll(dst[1], NULL, 10));
          break;
        case ObCreateTimeType:
          obj.set_createtime(strtoll(dst[1], NULL, 10));
          break;
        case ObModifyTimeType:
          obj.set_modifytime(strtoll(dst[1], NULL, 10));
          break;
        case ObExtendType:
          if (strcasecmp(dst[1], "min") == 0)
          {
            obj.set_min_value();
          }
          else if (strcasecmp(dst[1], "max") == 0)
          {
            obj.set_max_value();
          }
          else
          {
            obj.set_ext(strtoll(dst[1], NULL, 10));
          }
          break;
        case ObBoolType:
          obj.set_bool(strtoll(dst[1], NULL, 10));
          break;
        case ObDecimalType:
          // TODO
          break;
        default:
          break;
      }
    }
    else
    {
      fprintf(stderr, "unknown type %s\n", dst[0]);
    }
  }
  else
  {
    fprintf(stderr, "incorrect format [%s]\n", object_str);
    ret = OB_ERROR;
  }

  return ret;
}

int parse_rowkey(const char* strkey, ObObj* array, int64_t& size)
{
  // int:value,varchar:cadf,float:0.332
  const char* dst[size];
  int ret = parse_string(strkey, ',', dst, size);
  for (int i = 0; i < size && OB_SUCCESS == ret; ++i)
  {
    ret = parse_object(dst[i], array[i]);
  }
  return ret;
}

int parse_range_str(const char* range_str, int hex_format, oceanbase::common::ObNewRange &range)
{
  UNUSED(hex_format);
  //[int:1,varchar:aaa; int:2,varchar:bbb]
  int ret = OB_SUCCESS;
  int64_t len = 0;

  if(NULL == range_str)
  {
    ret = OB_INVALID_ARGUMENT;
  }

  if(OB_SUCCESS == ret)
  {
    len = static_cast<int>(strlen(range_str));
    if (len < 5)
      ret = OB_ERROR;
  }

  if(OB_SUCCESS == ret)
  {
    const char start_border = range_str[0];
    if (start_border == '[')
      range.border_flag_.set_inclusive_start();
    else if (start_border == '(')
      range.border_flag_.unset_inclusive_start();
    else
    {
      fprintf(stderr, "start char of range_str(%c) must be [ or (\n", start_border);
      ret = OB_ERROR;
    }
  }

  if(OB_SUCCESS == ret)
  {
    const char end_border = range_str[len - 1];
    if (end_border == ']')
      range.border_flag_.set_inclusive_end();
    else if (end_border == ')')
      range.border_flag_.unset_inclusive_end();
    else
    {
      fprintf(stderr, "end char of range_str(%c) must be [ or (\n", end_border);
      ret = OB_ERROR;
    }
  }

  if(OB_SUCCESS == ret)
  {
    int64_t obj_array_size = OB_MAX_ROWKEY_COLUMN_NUMBER;
    ObObj * sk_objs = new ObObj[obj_array_size];
    ObObj * ek_objs = new ObObj[obj_array_size];
    int64_t start_key_size = OB_MAX_ROWKEY_COLUMN_NUMBER;
    int64_t end_key_size = OB_MAX_ROWKEY_COLUMN_NUMBER;

    int64_t key_size = 2;
    const char* dst[key_size];
    char rowkey_str[OB_MAX_FILE_NAME_LENGTH];
    memset(rowkey_str, 0, OB_MAX_FILE_NAME_LENGTH);
    memcpy(rowkey_str, range_str + 1, strlen(range_str) - 2 );
    if (OB_SUCCESS != (ret = parse_string(rowkey_str, ';', dst, key_size)))
    {
      fprintf(stderr, "incorrect format [%s], ret=%d.\n", rowkey_str, ret);
    }
    else if (key_size != 2)
    {
      fprintf(stderr, "incorrect format [%s], has %ld rowkey \n", rowkey_str, key_size);
      ret = OB_ERROR;
    }
    else if (OB_SUCCESS != (ret = parse_rowkey(dst[0], sk_objs, start_key_size)))
    {
      fprintf(stderr, "incorrect start rowkey [%s], ret=%d.\n", dst[0], ret);
    }
    else if (OB_SUCCESS != (ret = parse_rowkey(dst[1], ek_objs, end_key_size)))
    {
      fprintf(stderr, "incorrect end rowkey [%s], ret=%d.\n", dst[1], ret);
    }
    else
    {
      range.start_key_.assign(sk_objs, start_key_size);
      range.end_key_.assign(ek_objs, end_key_size);
    }
  }

  return ret;
}

/**
 * hex_format:
 * 0 : plain string
 * 1 : hex format string "FACB012D"
 * 2 : int64_t number "1232"
 */
void dump_scanner(ObScanner &scanner)
{
  ObScannerIterator iter;
  int total_count = 0;
  int row_count = 0;
  bool is_row_changed = false;
  for (iter = scanner.begin(); iter != scanner.end(); iter++)
  {
    ObCellInfo *cell_info;
    iter.get_cell(&cell_info, &is_row_changed);
    if (is_row_changed)
    {
      ++row_count;
      fprintf(stderr,"table_id:%lu, rowkey:\n", cell_info->table_id_);
      hex_dump(cell_info->row_key_.ptr(),static_cast<int32_t>(cell_info->row_key_.length()));
    }
    fprintf(stderr, "%s\n", common::print_cellinfo(cell_info, "CLI_GET"));
    ++total_count;
  }
  fprintf(stderr, "row_count=%d,total_count=%d\n", row_count, total_count);
}

int dump_tablet_info(ObScanner &scanner)
{
  int ret = OB_SUCCESS;
  int64_t ip = 0;
  int64_t port = 0;
  int64_t version = 0;
  int64_t occupy_size = 0;
  int64_t total_row_count = 0;

  ObServer server;
  char tmp_buf[32];
  ObRowkey start_key;
  ObRowkey end_key;
  ObMemBuf start_key_buf;
  ObMemBufAllocatorWrapper start_key_allocator(start_key_buf);
  ObCellInfo * cell = NULL;
  ObScannerIterator iter;
  bool row_change = false;
  int row_count = 0;

  start_key.set_min_row();

  iter = scanner.begin();
  for (; OB_SUCCESS == ret && iter != scanner.end() ; ++iter)
  {
    ret = iter.get_cell(&cell, &row_change);
    //fprintf(stderr, "row_change=%d, column_name=%.*s\n", row_change,
     //   cell->column_name_.length(), cell->column_name_.ptr());
    //cell->value_.dump();

    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
      break;
    }

    if (row_change && OB_SUCCESS == ret)
    {
      end_key = cell->row_key_;
      row_count++;
      ip = port = version = 0;
      fprintf(stderr, "\nrange:(%s; %s]\n", to_cstring(start_key), to_cstring(end_key));
      if (OB_SUCCESS != (ret = end_key.deep_copy(start_key, start_key_allocator)))
      {
        TBSYS_LOG(ERROR, "failed to copy end key, ret=%d", ret);
      }
    }

    if (OB_SUCCESS == ret && cell != NULL)
    {
      if ((cell->column_name_.compare("1_port") == 0)
          || (cell->column_name_.compare("2_port") == 0)
          || (cell->column_name_.compare("3_port") == 0))
      {
        ret = cell->value_.get_int(port);
        //TBSYS_LOG(DEBUG,"port is %ld",port);
      }
      else if ((cell->column_name_.compare("1_ipv4") == 0)
          || (cell->column_name_.compare("2_ipv4") == 0)
          || (cell->column_name_.compare("3_ipv4") == 0))
      {
        ret = cell->value_.get_int(ip);
        //TBSYS_LOG(DEBUG,"ip is %ld",ip);
      }
      else if (cell->column_name_.compare("1_tablet_version") == 0 ||
          cell->column_name_.compare("2_tablet_version") == 0 ||
          cell->column_name_.compare("3_tablet_version") == 0)
      {
        ret = cell->value_.get_int(version);
        //hex_dump(cell->row_key_.ptr(),cell->row_key_.length(),false,TBSYS_LOG_LEVEL_INFO);
        //TBSYS_LOG(DEBUG,"tablet_version is %d",version);
      }
      else if (cell->column_name_.compare("occupy_size") == 0 )
      {
        ret = cell->value_.get_int(occupy_size);
        fprintf(stderr, "occupy_size=%ld\n", occupy_size);
      }
      else if (cell->column_name_.compare("record_count") == 0 )
      {
        ret = cell->value_.get_int(total_row_count);
        fprintf(stderr, "record_count=%ld\n", total_row_count);
      }

      //fprintf(stderr, "%.*s\n", cell->column_name_.length(), cell->column_name_.ptr());

      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "check get value failed:ret[%d]", ret);
        break;
      }
    }

    if (port != 0 && ip != 0 && version != 0)
    {
      server.set_ipv4_addr(static_cast<int32_t>(ip), static_cast<int32_t>(port));
      server.to_string(tmp_buf,sizeof(tmp_buf));
      if (row_count > 0) fprintf(stderr, "server= %s ,version= %ld\n", tmp_buf, version);
      ip = port = version = 0;
    }
  }

  return ret;
}



int parse_rowkey_type_array(const char* rowkey_type_str, int* array, int32_t& size)
{
  int ret = OB_SUCCESS;
  int type = ObNullType;
  std::vector<char*> list;
  tbsys::CStringUtil::split(const_cast<char*>(rowkey_type_str), ",", list);
  int32_t cnt = 0;
  for (size_t i = 0; i < list.size(); ++i)
  {
    if (hash::HASH_EXIST == obj_type_map.get(list[i], type))
    {
      if (cnt < size) array[cnt++] = type;
      else ret = OB_SIZE_OVERFLOW;
    }
    else
    {
      ret = OB_ERROR;
      fprintf(stderr, "unrecognize type :(%s)\n", list[i]);
    }
  }
  size = cnt;
  return ret;
}

int parse_rowkey_obj_array(const int* type_array, const int32_t size, const char* rowkey_value_str, ObObj* obj_array)
{
  int ret = OB_SUCCESS;
  int64_t tmp_value = 0;
  std::vector<char*> list;
  tbsys::CStringUtil::split(const_cast<char*>(rowkey_value_str), ",", list);
  if ((int32_t)list.size() != size)
  {
    ret = OB_ERROR;
    fprintf(stderr, "type array size = %d not match with value size =%d\n", size, (int32_t)list.size());
  }
  else
  {
    for (int i = 0; i < size; ++i)
    {
      tmp_value = strtoll(list[i], NULL, 10);
      switch (type_array[i])
      {
        case ObNullType:
          obj_array[i].set_null();
          break;
        case ObIntType:
          obj_array[i].set_int(tmp_value);
          break;
        case ObFloatType:
          tmp_value = strtoll(list[i], NULL, 10);
          obj_array[i].set_int(tmp_value);
          break;
        case ObDoubleType:
          obj_array[i].set_double(static_cast<double>(tmp_value));
          break;
        case ObDateTimeType:
          obj_array[i].set_datetime(tmp_value);
          break;
        case ObPreciseDateTimeType:
          obj_array[i].set_precise_datetime(tmp_value);
          break;
        case ObCreateTimeType:
          obj_array[i].set_createtime(tmp_value);
          break;
        case ObModifyTimeType:
          obj_array[i].set_modifytime(tmp_value);
          break;
        case ObVarcharType:
          {
            ObString str_value(0, static_cast<ObString::obstr_size_t>(strlen(list[i])), list[i]);
            obj_array[i].set_varchar(str_value);
          }
        case ObExtendType:
          obj_array[i].set_ext(tmp_value);
          break;
        default:
          break;
      }
    }
  }

  return ret;
}

int parse_rowkey(const char* rowkey_type_str, const char* rowkey_value_str, PageArena<char>& allocer, ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  int size = OB_MAX_ROWKEY_COLUMN_NUMBER;
  int type_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
  ObRowkey tmp_rowkey;

  ret = parse_rowkey_type_array(rowkey_type_str, type_array, size);
  if (OB_SUCCESS == ret)
  {
    ret = parse_rowkey_obj_array(type_array, size, rowkey_value_str, obj_array);
  }

  if (OB_SUCCESS == ret)
  {
    tmp_rowkey.assign(obj_array, size);
    ret = tmp_rowkey.deep_copy(rowkey, allocer);
  }

  return ret;
}

void dump_tablet_image(ObTabletImage & image, bool load_sstable)
{
  ObTablet *tablet  = NULL;
  int tablet_index = 0;

  int ret = image.begin_scan_tablets();
  while (OB_SUCCESS == ret)
  {
    ret = image.get_next_tablet(tablet);
    if (OB_SUCCESS == ret)
    {
      fprintf(stderr, "tablet(%d) : %s \n", tablet_index, to_cstring(tablet->get_range()));
      dump_tablet(*tablet, load_sstable);

      ++tablet_index;
    }
    if (NULL != tablet)
    {
      tablet->dec_ref();
    }
  }
  image.end_scan_tablets();
}

void dump_multi_version_tablet_image(ObMultiVersionTabletImage & image, bool load_sstable)
{
  ObTablet *tablet  = NULL;
  int tablet_index = 0;

  int ret = image.begin_scan_tablets();
  while (OB_SUCCESS == ret)
  {
    ret = image.get_next_tablet(tablet);
    if (OB_SUCCESS == ret)
    {
      fprintf(stderr, "tablet(%d) : %s \n", tablet_index, to_cstring(tablet->get_range()));
      dump_tablet(*tablet, load_sstable);

      ++tablet_index;
    }
    if (NULL != tablet) image.release_tablet(tablet);
  }
  image.end_scan_tablets();
}

int dump_tablet(const ObTablet & tablet, const bool dump_sstable)
{
  // dump sstable info
  const sstable::ObSSTableId& sstable_id_= tablet.get_sstable_id();

  // dump tablet basic info
  fprintf(stderr, "range=%s, data version=%ld, disk_no=%d, "
      "merged=%d, last do expire version=%ld, seq num=%ld, sstable version=%d sstable_file_id:%ld\n",
      to_cstring(tablet.get_range()), tablet.get_data_version(), tablet.get_disk_no(),
      tablet.is_merged(), tablet.get_last_do_expire_version(), tablet.get_sequence_num(),
      tablet.get_sstable_version(), sstable_id_.sstable_file_id_);

  if (dump_sstable)
  {
    const_cast<ObTablet&>(tablet).load_sstable(tablet.get_data_version());
    sstable::SSTableReader* sstable_reader_
      = tablet.get_sstable_reader();
    if (NULL != sstable_reader_)
    {
      fprintf(stderr, "sstable: id=%ld, row count=%ld, sstable size=%ld,"
          "checksum=%ld\n", sstable_id_.sstable_file_id_,
          sstable_reader_->get_row_count(), sstable_reader_->get_sstable_size(), sstable_reader_->get_sstable_checksum());
    }
  }
  else
  {
    fprintf(stderr, "sstable: id=%ld\n", sstable_id_.sstable_file_id_) ;
  }
  return OB_SUCCESS;
}

int build_scan_param(const QueryParam& query_param, ObSqlScanParam& scan_param, ObRowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  range.table_id_ = query_param.table_id;
  int32_t query_column_size = 512;
  int32_t query_column_array[query_column_size];
  if (NULL == query_param.query_columns || NULL == query_param.scan_range || 0 >= query_param.table_id)
  {
    fprintf(stderr, "invalid table=%ld, scan_range=%s, query_columns=%s\n", 
        query_param.table_id, query_param.scan_range, query_param.query_columns);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = parse_range_str(query_param.scan_range, 1, range)))
  {
    fprintf(stderr, "parse_range_str (%s) ret=%d\n", query_param.scan_range, ret);
  }
  else if (OB_SUCCESS != (ret = parse_number_range(query_param.query_columns, 
          query_column_array, query_column_size)))
  {
    fprintf(stderr, "parse query_column ret=%d\n", ret);
  }
  else if (OB_SUCCESS != (ret = build_scan_param(range, 
          query_column_array, query_column_size, query_param.version, scan_param, row_desc)))
  {
  }
  else
  {
    scan_param.set_scan_direction(oceanbase::common::ScanFlag::FORWARD);
    scan_param.set_read_mode(query_param.is_async_read ? ScanFlag::ASYNCREAD : ScanFlag::SYNCREAD);
    scan_param.set_is_result_cached(query_param.is_result_cached);
  }

  return ret;
}

int build_scan_param(
    const oceanbase::common::ObNewRange& range,  
    const int32_t* query_column_array, 
    const int32_t query_column_size, 
    const int64_t query_data_version,
    oceanbase::sql::ObSqlScanParam& scan_param,
    oceanbase::common::ObRowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = fill_scan_project(range.table_id_, 
          query_column_array, query_column_size, scan_param, row_desc)))
  {
    fprintf(stderr, "fill_scan_project error=%d\n", ret);
  }
  else
  {
    scan_param.set_table_id(range.table_id_, range.table_id_);
    scan_param.set_data_version(query_data_version);
    scan_param.set_range(range);

    fprintf(stderr, "query scan param= %s\n", to_cstring(scan_param));

    if (query_column_size == 1 && query_column_array[0] == 0)
    {
      scan_param.set_full_row_scan(true);
    }
  }
  return ret;
}

int build_sstable_scan_param(
    const QueryParam &query_param, const QueryParam *local_index_param,
    oceanbase::sstable::ObSSTableScanParam &scan_param,
    oceanbase::common::ObRowDesc& row_desc)
{
  int rc = OB_SUCCESS;
  ObNewRange range;
  range.table_id_ = query_param.table_id;
  uint64_t table_id = range.table_id_;
  int32_t query_column_size = 512;
  int32_t query_column_array[query_column_size];
  if (!query_param.query_columns || !query_param.scan_range || 0 >= query_param.table_id)
  {
    fprintf(stderr, "invalid query param");
    rc = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (rc = parse_range_str(query_param.scan_range, 1, range)))
  {
    fprintf(stderr, "parse_range_str failed, rc %d, range str [%s]", rc, query_param.scan_range);
  }
  else if (OB_SUCCESS != (rc = parse_number_range(query_param.query_columns,
          query_column_array, query_column_size)))
  {
    fprintf(stderr, "parse_number_range failed, rc %d, query_columns [%s]", rc,
        query_param.query_columns);
  }
  else {
    scan_param.set_range(range);
    scan_param.set_rowkey_column_count(static_cast<int16_t>(std::max(
            range.start_key_.get_obj_cnt(), range.end_key_.get_obj_cnt())));
  }

  ObNewRange local_index_range;
  if (OB_SUCCESS == rc && local_index_param)
  {
    local_index_range.table_id_ = local_index_param->table_id;
    table_id = local_index_range.table_id_;

    if (!local_index_param->scan_range)
    {
      fprintf(stderr, "invalid query param");
      rc = OB_INVALID_ARGUMENT;
    }
    else if (OB_SUCCESS != (rc = parse_range_str(
            local_index_param->scan_range, 1, local_index_range)))
    {
      fprintf(stderr, "parse_range_str failed, rc %d, range str [%s]",
          rc, local_index_param->scan_range);
    }
    else
    {
      scan_param.set_local_index_range(local_index_range);
      scan_param.set_rowkey_column_count(static_cast<int16_t>(std::max(
              local_index_range.start_key_.get_obj_cnt(),
              local_index_range.end_key_.get_obj_cnt())));
    }
  }

  if (OB_SUCCESS == rc)
  {
    for (int32_t i = 0; i < query_column_size; i++)
    {
      scan_param.add_column(query_column_array[i]);
      row_desc.add_column_desc(table_id, query_column_array[i]);
    }

    ObVersionRange vrange;
    vrange.start_version_ = vrange.end_version_ = query_param.version;
    vrange.border_flag_.set_inclusive_start();
    vrange.border_flag_.set_inclusive_end();

    scan_param.set_scan_direction(ScanFlag::FORWARD);
  }

  return rc;
}

void dump_scanner(oceanbase::common::ObNewScanner &scanner, oceanbase::common::ObRowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  common::ObRow current_row;
  current_row.set_row_desc(row_desc);
  int64_t row_count = 0;
  while (OB_SUCCESS == ret && OB_SUCCESS == scanner.get_next_row(current_row))
  {
    fprintf(stderr, "row[%ld]=[%s]\n", row_count++, to_cstring(current_row));
  }
}

void dump_phy_operator(oceanbase::sql::ObPhyOperator &op)
{
  int rc = OB_SUCCESS;
  const common::ObRow *row = NULL;
  int64_t count = 0;
  while (OB_SUCCESS == rc)
  {
    rc = op.get_next_row(row);
    if (OB_SUCCESS == rc)
    {
      fprintf(stderr, "row [%ld] = [%s]\n", count++, to_cstring(*row));
    }
    else if (OB_ITER_END == rc)
    {
      fprintf(stderr, "dump finish, %ld rows dumped.\n", count);
    }
    else
    {
      fprintf(stderr, "get_next_row failed, rc %d", rc);
      break;
    }
  }
}

int fill_project(int64_t table_id, int64_t column_id, oceanbase::sql::ObProject& project)
{
  int ret = OB_SUCCESS;
  sql::ObSqlExpression sql_expression;
  sql::ExprItem item;

  item.value_.cell_.tid = table_id;
  item.value_.cell_.cid = column_id;
  item.type_ = T_REF_COLUMN;
  sql_expression.set_tid_cid(table_id, column_id);

  if (OB_SUCCESS != (ret = sql_expression.add_expr_item(item)))
  {
    TBSYS_LOG(WARN, "add_expr_item ret=%d, tid=%ld, cid=%ld", ret, table_id, column_id);
  }
  else if (OB_SUCCESS != (ret = sql_expression.add_expr_item_end()))
  {
    TBSYS_LOG(WARN, "add_expr_item_end ret=%d, tid=%ld, cid=%ld", ret, table_id, column_id);
  }
  else if (OB_SUCCESS != (ret = project.add_output_column(sql_expression)))
  {
    TBSYS_LOG(WARN, "add_output_column ret=%d, tid=%ld, cid=%ld", ret, table_id, column_id);
  }
  return ret;
}

int fill_scan_project(
    const int64_t table_id, const int32_t* query_column_array, const int32_t query_column_size, 
    ObSqlScanParam& scan_param, ObRowDesc& row_desc)
{
  int ret = OB_SUCCESS;
  ObProject project;
  std::set<uint64_t> column_id_set;
  uint64_t column_id = 0;

  for (int64_t j = 0; j < query_column_size && OB_SUCCESS == ret; ++j)
  {
    column_id = query_column_array[j];
    if (column_id_set.find(column_id) == column_id_set.end())
    {
      fill_project(table_id, column_id, project);
      row_desc.add_column_desc(table_id, column_id);
      column_id_set.insert(column_id);
    }
  }

  if (OB_SUCCESS == ret) scan_param.set_project(project);

  return ret;
}

int build_scan_param(const QueryParam& query_param, oceanbase::common::ObScanParam& scan_param)
{
  int rc = OB_SUCCESS;
  ObNewRange range;
  ObVersionRange version_range;
  range.table_id_ = query_param.table_id;
  int32_t query_column_size = 512;
  int32_t query_column_array[query_column_size];
  if (!query_param.query_columns || !query_param.scan_range || 0 >= query_param.table_id)
  {
    fprintf(stderr, "invalid query param");
    rc = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (rc = parse_range_str(query_param.scan_range, 1, range)))
  {
    fprintf(stderr, "parse_range_str failed, rc %d, range str [%s]", rc, query_param.scan_range);
  }
  else if (OB_SUCCESS != (rc = parse_number_range(query_param.query_columns,
          query_column_array, query_column_size)))
  {
    fprintf(stderr, "parse_number_range failed, rc %d, query_columns [%s]", rc,
        query_param.query_columns);
  }
  else 
  {
    ObString table_name;
    scan_param.set(range.table_id_, table_name, range);

    version_range.start_version_ = ObVersion(query_param.version);
    version_range.border_flag_.unset_min_value();
    version_range.border_flag_.set_inclusive_start();
    if (query_param.end_version <= 0)
    {
      version_range.border_flag_.set_max_value();
    }
    else
    {
      version_range.end_version_ = ObVersion(query_param.end_version);
      version_range.border_flag_.unset_max_value();
      version_range.border_flag_.set_inclusive_end();
    }
    scan_param.set_version_range(version_range);

    for (int32_t i = 0; i < query_column_size; ++i)
    {
      scan_param.add_column(query_column_array[i]);
    }
  }
  return rc;
}

