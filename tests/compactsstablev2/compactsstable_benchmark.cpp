#include "compactsstable_benchmark.h"

using namespace oceanbase;
using namespace common;
using namespace sstable;
using namespace serialization;
using namespace compactsstablev2;
using namespace chunkserver;

int read_record(const int fd, const int64_t offset, const int64_t size, const char*& buffer)
{
  int ret = OB_SUCCESS;
  static char* record_buf = NULL;
  static const int64_t record_buf_size = 10 * 1024 * 1024;

  if (NULL == record_buf)
  {
    if (NULL == (record_buf = (char*)ob_malloc(record_buf_size)))
    {
      TBSYS_LOG(WARN, "ob malloc error: record_buf_size=[%ld]", record_buf_size);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (-1 == lseek(fd, offset, SEEK_SET))
    {
      TBSYS_LOG(WARN, "lseek error: fd=[%d], offset=[%ld]", fd, offset);
      ret = OB_ERROR;
    }
    else if (size != read(fd, record_buf, size))
    {
      TBSYS_LOG(WARN, "read record error: size=[%ld], fd=[%d]", size, fd);
    }
    else
    {
      buffer = record_buf;
    }
  }

  return ret;
}

int get_row(char* buf, int64_t size, int64_t block_id, SrcSSTable& src_sstable)
{
  UNUSED(size);
  UNUSED(block_id);
  static int64_t row_num = 0;
  int ret = OB_SUCCESS;
  int64_t user_id = 0;
  int8_t item_type = 0;
  int64_t item_id = 0;
  int64_t pos = 0;
  char* buf_ptr = 0;
  int32_t row_index[1000];
  int16_t row_key_length = 0;
  sstable::ObSSTableBlockHeader block_header;

  if (OB_SUCCESS != (ret = block_header.deserialize(buf, block_header.get_serialize_size(), pos)))
  {
    TBSYS_LOG(WARN, "block header deserialize error: ret=[%d]", ret);
  }
  else
  {
    //TBSYS_LOG(INFO, "block_id=[%ld], row_count=[%d]", block_id, block_header.row_count_);
  }

  if (OB_SUCCESS == ret)
  {
    buf_ptr = buf + block_header.row_index_array_offset_;
    for (int64_t i = 0; i <= block_header.row_count_; i ++)
    {
      pos = 0;
      if (OB_SUCCESS != (ret = decode_i32(buf_ptr, 4, pos, &row_index[i])))
      {
        TBSYS_LOG(WARN, "deserialize error: i=[%ld], ret=[%d]", i, ret);
        break;
      }
      buf_ptr += 4;
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < block_header.row_count_; i ++)
    {
      ObObj obj;
      buf_ptr = buf + row_index[i];
      if (OB_SUCCESS == ret)
      {
        pos = 0;
        if (OB_SUCCESS != (ret = decode_i16(buf_ptr, 2, pos, &row_key_length)))
        {
          TBSYS_LOG(WARN, "decode error");
          break;
        }
      }

      if (OB_SUCCESS == ret)
      {
        buf_ptr += 2;
        pos = 0;
        if (OB_SUCCESS != (ret = decode_i64(buf_ptr, 8, pos, &user_id)))
        {
          TBSYS_LOG(WARN, "decode error");
          break;
        }
        else
        {
          if (OB_SUCCESS != (ret = src_sstable.row_array_[row_num].raw_set_cell(0, obj)))
          {
            TBSYS_LOG(WARN, "raw set cell error");
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        buf_ptr += 8;
        pos = 0;
        if (OB_SUCCESS != (ret = decode_i8(buf_ptr, 1, pos, &item_type)))
        {
          TBSYS_LOG(WARN, "decode error");
          break;
        }
        else
        {
          obj.set_int(item_type, true);
          if (OB_SUCCESS != (ret = src_sstable.row_array_[row_num].raw_set_cell(1, obj)))
          {
            TBSYS_LOG(WARN, "raw set cell error");
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        buf_ptr += 1;
        pos = 0;
        if (OB_SUCCESS != (ret = decode_i64(buf_ptr, 8, pos, &item_id)))
        {
          TBSYS_LOG(WARN, "decode error");
          break;
        }
        else
        {
          obj.set_int(item_id, true);
          if (OB_SUCCESS != (ret = src_sstable.row_array_[row_num].raw_set_cell(2, obj)))
          {
            TBSYS_LOG(WARN, "raw set cell error");
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        buf_ptr += 8;
        pos = 0;
        for (int64_t j = 3; j <= 18; j ++)
        {
          if (OB_SUCCESS != (ret = obj.deserialize(buf_ptr, 10240, pos)))
          {
            TBSYS_LOG(WARN, "deserialize error");
            break;
          }
          else if (OB_SUCCESS != (ret = src_sstable.row_array_[row_num].raw_set_cell(j, obj)))
          {
            TBSYS_LOG(WARN, "raw set cell error");
            break;
          }
          else
          {
            buf_ptr += pos;
            pos = 0;
          }
        }
      }

      row_num ++;
    }

  }

  return ret;
}

int get_row2(char* buf, int64_t size, int64_t block_id, SrcSSTable& src_sstable, ObRow& row, ObCompactSSTableWriter& writer)
{
  UNUSED(size);
  UNUSED(src_sstable);
  UNUSED(block_id);
  static int64_t row_num = 0;
  int ret = OB_SUCCESS;
  int64_t user_id = 0;
  int8_t item_type = 0;
  int64_t item_id = 0;
  int64_t pos = 0;
  char* buf_ptr = 0;
  int32_t row_index[1000];
  int16_t row_key_length = 0;
  sstable::ObSSTableBlockHeader block_header;

  if (OB_SUCCESS != (ret = block_header.deserialize(buf, block_header.get_serialize_size(), pos)))
  {
    TBSYS_LOG(WARN, "block header deserialize error: ret=[%d]", ret);
  }
  else
  {
    //TBSYS_LOG(INFO, "block_id=[%ld], row_count=[%d]", block_id, block_header.row_count_);
  }

  if (OB_SUCCESS == ret)
  {
    buf_ptr = buf + block_header.row_index_array_offset_;
    for (int64_t i = 0; i <= block_header.row_count_; i ++)
    {
      pos = 0;
      if (OB_SUCCESS != (ret = decode_i32(buf_ptr, 4, pos, &row_index[i])))
      {
        TBSYS_LOG(WARN, "deserialize error: i=[%ld], ret=[%d]", i, ret);
        break;
      }
      buf_ptr += 4;
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < block_header.row_count_; i ++)
    {
      row.reset(true, ObRow::DEFAULT_NOP);
      ObObj obj;
      buf_ptr = buf + row_index[i];
      if (OB_SUCCESS == ret)
      {
        pos = 0;
        if (OB_SUCCESS != (ret = decode_i16(buf_ptr, 2, pos, &row_key_length)))
        {
          TBSYS_LOG(WARN, "decode error");
          break;
        }
      }

      if (OB_SUCCESS == ret)
      {
        buf_ptr += 2;
        pos = 0;
        if (OB_SUCCESS != (ret = decode_i64(buf_ptr, 8, pos, &user_id)))
        {
          TBSYS_LOG(WARN, "decode error");
          break;
        }
        else
        {
          obj.set_int(user_id, true);
          if (OB_SUCCESS != (ret = row.raw_set_cell(0, obj)))
          {
            TBSYS_LOG(WARN, "raw set cell error");
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        buf_ptr += 8;
        pos = 0;
        if (OB_SUCCESS != (ret = decode_i8(buf_ptr, 1, pos, &item_type)))
        {
          TBSYS_LOG(WARN, "decode error");
          break;
        }
        else
        {
          obj.set_int(item_type, true);
          if (OB_SUCCESS != (ret = row.raw_set_cell(1, obj)))
          {
            TBSYS_LOG(WARN, "raw set cell error");
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        buf_ptr += 1;
        pos = 0;
        if (OB_SUCCESS != (ret = decode_i64(buf_ptr, 8, pos, &item_id)))
        {
          TBSYS_LOG(WARN, "decode error");
          break;
        }
        else
        {
          obj.set_int(item_id, true);
          if (OB_SUCCESS != (ret = row.raw_set_cell(2, obj)))
          {
            TBSYS_LOG(WARN, "raw set cell error");
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        buf_ptr += 8;
        pos = 0;
        for (int64_t j = 3; j <= 18; j ++)
        {
          if (OB_SUCCESS != (ret = obj.deserialize(buf_ptr, 10240, pos)))
          {
            TBSYS_LOG(WARN, "deserialize error");
            break;
          }
          else if (OB_SUCCESS != (ret = row.raw_set_cell(j, obj)))
          {
            TBSYS_LOG(WARN, "raw set cell error");
            break;
          }
          else
          {
            buf_ptr += pos;
            pos = 0;
          }
        }
      }

      bool is_split = false;
      if (OB_SUCCESS != (ret = writer.append_row(row, is_split)))
      {
        TBSYS_LOG(WARN, "append row error");
        break;
      }

      row_num ++;
    }
  }

  return ret;
}

int get_block(const int fd, const PerfTestConfig& config, SrcSSTable& src_sstable)
{
  UNUSED(config);
  int ret = OB_SUCCESS;
  ObCompressor* src_compressor = NULL;
  int64_t row_count = 100000;

  int64_t block_buffer_size = 1024 * 1024 * 1024;
  int64_t decompress_buf_size = 10 * 1024 * 1024;
  char* decompress_buf = NULL;
  const char* buffer = NULL;

  static ObRowDesc row_desc;

  if (NULL == src_compressor)
  {
    if (NULL == (src_compressor = create_compressor("lzo_1.0")))
    {
      TBSYS_LOG(WARN, "create compressor error");
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == (src_sstable.row_array_ = (ObRow*)ob_malloc(row_count * sizeof(ObRow))))
    {
      TBSYS_LOG(WARN, "malloc error: size=[%ld]", row_count * sizeof(ObRow));
      ret = OB_ERROR;
    }
    else if (NULL == (src_sstable.block_buf_ = (char*)ob_malloc(block_buffer_size)))
    {
      TBSYS_LOG(WARN, "malloc error: size=[%ld]", block_buffer_size);
      ret = OB_ERROR;
    }
    else if (NULL == (decompress_buf = (char*)ob_malloc(decompress_buf_size)))
    {
      TBSYS_LOG(WARN, "malloc error: size=[%ld]", decompress_buf_size);
      ret = OB_ERROR;
    }
    else
    {
      for (int64_t i = 1; i <= 19; i ++)
      {
        if (OB_SUCCESS != (ret = row_desc.add_column_desc(1003, i)))
        {
          TBSYS_LOG(WARN, "add_column desc error: i=[%ld], ret=[%d]", i, ret);
          break;
        }
      }

      if (OB_SUCCESS == ret)
      {
        row_desc.set_rowkey_cell_count(3);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ObRow* tmp_row = NULL;
    for (int64_t i = 0; i < row_count; i ++)
    {
      tmp_row = new((char*)(src_sstable.row_array_) + i * sizeof(ObRow))ObRow();
      tmp_row->set_row_desc(row_desc);
    }

    TBSYS_LOG(INFO, "set row desc success");
  }

  if (OB_SUCCESS == ret)
  {
    int64_t size = 0;
    int64_t real_size = 0;
    ObRecordHeader record_header;
    //int64_t pos = 0;
    int64_t block_buf_pos = 0;
    const char* payload_ptr = NULL;
    int64_t payload_len = 0;
    const int16_t magic = ObSSTableWriter::DATA_BLOCK_MAGIC;
    for (int64_t i = 0; i < 397; i ++)
    {
      if (OB_SUCCESS != (ret = read_record(fd, src_sstable.sstable_trailer_.get_first_block_data_offset() + size, src_sstable.block_index_item_[i].block_record_size_, buffer)))
      {
        TBSYS_LOG(WARN, "read record error: i=[%ld], ret=[%d], fd=[%d]", i, ret, fd);
        break;
      }
      else if (OB_SUCCESS != (ret = ObRecordHeader::check_record(buffer, src_sstable.block_index_item_[i].block_record_size_, magic, record_header, payload_ptr, payload_len)))
      {
        TBSYS_LOG(WARN, "check record error: i=[%ld], ret=[%d]", i, ret);
        break;
      }
      else if (OB_SUCCESS != (ret = src_compressor->decompress(payload_ptr, payload_len, decompress_buf, decompress_buf_size, real_size)))
      {
        TBSYS_LOG(WARN, "decompress error: i=[%ld], ret=[%d]", i, ret);
        break;
      }
      else
      {
        memcpy(src_sstable.block_buf_ + block_buf_pos, decompress_buf, real_size);
        if (OB_SUCCESS != (ret = get_row(src_sstable.block_buf_ + block_buf_pos, real_size, i, src_sstable)))
        {
          TBSYS_LOG(WARN, "get row error: i=[%ld], ret=[%d]", i, ret);
          break;
        }
        else
        {
          block_buf_pos += real_size;
          size += src_sstable.block_index_item_[i].block_record_size_;
        }
      }
    }
  }

  return ret;
}

int get_block2(const int fd, const PerfTestConfig& config, SrcSSTable& src_sstable, ObCompactSSTableWriter& writer)
{
  UNUSED(config);
  int ret = OB_SUCCESS;
  char* decompress_buf = NULL;
  int64_t real_size = 0;
  static ObRowDesc row_desc;
  ObCompressor* src_compressor = NULL;
  ObRow row;
  const int16_t magic = ObSSTableWriter::DATA_BLOCK_MAGIC;
  const char* buffer = NULL;
  ObRecordHeader record_header;
  const char* payload_ptr;
  int64_t payload_len = 0;
  int64_t decompress_buf_size = 10 * 1024 * 1024;

  if (NULL == src_compressor)
  {
    if (NULL == (src_compressor = create_compressor("lzo_1.0")))
    {
      TBSYS_LOG(WARN, "create compressor error");
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == (decompress_buf = (char*)ob_malloc(decompress_buf_size)))
    {
      TBSYS_LOG(WARN, "decompress error");
      ret = OB_ERROR;
    }
    else
    {
      for (int64_t i = 1; i <= 19; i ++)
      {
        if (OB_SUCCESS != (ret = row_desc.add_column_desc(1003, i)))
        {
          TBSYS_LOG(WARN, "add_column desc error");
          break;
        }
      }

      if (OB_SUCCESS == ret)
      {
        row_desc.set_rowkey_cell_count(3);
        TBSYS_LOG(INFO, "set row desc success");
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    row.set_row_desc(row_desc);
  }

  if (OB_SUCCESS == ret)
  {
    int64_t size = 0;
    for (int64_t i = 0; i < src_sstable.block_index_header_.sstable_block_count_; i ++)
    {
      if (OB_SUCCESS != (ret = read_record(fd, src_sstable.sstable_trailer_.get_first_block_data_offset() + size, src_sstable.block_index_item_[i].block_record_size_, buffer)))
      {
        TBSYS_LOG(WARN, "read record error: i=[%ld], ret=[%d], fd=[%d]", i, ret, fd);
        break;
      }
      else if (OB_SUCCESS != (ret = ObRecordHeader::check_record(buffer, src_sstable.block_index_item_[i].block_record_size_, magic, record_header, payload_ptr, payload_len)))
      {
        TBSYS_LOG(WARN, "check record error: i=[%ld], ret=[%d]", i, ret);
        break;
      }
      else if (OB_SUCCESS != (ret = src_compressor->decompress(payload_ptr, payload_len, decompress_buf, decompress_buf_size, real_size)))
      {
        TBSYS_LOG(WARN, "decompress error: i=[%ld], ret=[%d]", i, ret);
        break;
      }
      else
      {
        if (OB_SUCCESS != (ret = get_row2(decompress_buf, real_size, i, src_sstable, row, writer)))
        {
          TBSYS_LOG(WARN, "get row error: i=[%ld], ret=[%d]", i , ret);
          break;
        }
        else
        {
          size += src_sstable.block_index_item_[i].block_record_size_;
        }
      }
    }
  }

  return ret;
}


int make_schema(compactsstablev2::ObSSTableSchema& schema)
{
  int ret = OB_SUCCESS;
  compactsstablev2::ObSSTableSchemaColumnDef def;

  make_column_def(def, 1003, 1, ObIntType, 1);
  schema.add_column_def(def);

  make_column_def(def, 1003, 2, ObIntType, 2);
  schema.add_column_def(def);

  make_column_def(def, 1003, 3, ObIntType, 3);
  schema.add_column_def(def);

  make_column_def(def, 1003, 4, ObCreateTimeType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 5, ObModifyTimeType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 6, ObVarcharType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 7, ObDateTimeType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 8, ObIntType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 9, ObVarcharType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 10, ObVarcharType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 11, ObIntType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 12, ObVarcharType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 13, ObVarcharType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 14, ObIntType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 15, ObIntType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 16, ObIntType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 17, ObIntType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 18, ObIntType, 0);
  schema.add_column_def(def);

  make_column_def(def, 1003, 19, ObIntType, 0);
  schema.add_column_def(def);

  return ret;
}

void usage(const char* program)
{
  UNUSED(program);
}

int get_sstable_size(const char* file_name, int64_t& sstable_size)
{
  int ret = OB_SUCCESS;

  if (0 == strlen(file_name))
  {
    TBSYS_LOG(ERROR, "file name is null: file_name=[%s]", file_name);
    ret = OB_ERROR;
  }
  else if (0 >= (sstable_size = get_file_size(file_name)))
  {
    TBSYS_LOG(ERROR, "invalid sstable size: file_name=[%s], sstable_size=[%ld]", file_name, sstable_size);
    ret = OB_ERROR;
  }

  return ret;
}

int get_sstable_trailer_offset(const int fd, const int64_t sstable_size, int64_t& sstable_trailer_offset)
{
  int ret = OB_SUCCESS;
  const char* buffer = NULL;
  int64_t pos = 0;

  if (-1 == read_record(fd, sstable_size - sizeof(int64_t), sizeof(int64_t), buffer))
  {
    TBSYS_LOG(WARN, "record record error: offset=[%ld], size=[%ld]", sstable_size -sizeof(int64_t), sizeof(int64_t));
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = decode_i64(buffer, sizeof(int64_t), pos, &sstable_trailer_offset)))
  {
    TBSYS_LOG(WARN, "decode error: ret=[%d], pos=[%ld]", ret, pos);
  }
  else
  {
    TBSYS_LOG(INFO, "sstable_trailer_offset=[%ld]", sstable_trailer_offset);
  }

  return ret;
}

int get_sstable_trailer(const int fd, const int64_t trailer_offset, ObSSTableTrailer& sstable_trailer)
{
  int ret = OB_SUCCESS;
  const char* buffer = NULL;
  int64_t pos = 0;
  ObRecordHeader record_header;
  const char* payload_ptr = NULL;
  int64_t payload_len = 0;
  const int16_t magic = ObSSTableWriter::TRAILER_MAGIC;

  if (OB_SUCCESS != (ret = read_record(fd, trailer_offset, sstable_trailer.get_serialize_size() + record_header.get_serialize_size(), buffer)))
  {
    TBSYS_LOG(WARN, "read record error: ret=[%d], fd=[%d], trailer_offset=[%ld], size=[%ld]",
        ret, fd, trailer_offset, sstable_trailer.get_serialize_size() + record_header.get_serialize_size());
  }
  else if (OB_SUCCESS != (ret = ObRecordHeader::check_record(buffer, sstable_trailer.get_serialize_size() + record_header.get_serialize_size(),
          magic, record_header, payload_ptr, payload_len)))
  {
    TBSYS_LOG(WARN, "check record error: ret=[%d], size=[%ld]", ret, sstable_trailer.get_serialize_size() + record_header.get_serialize_size());
  }
  else if (OB_SUCCESS != (ret = sstable_trailer.deserialize(payload_ptr, payload_len, pos)))
  {
    TBSYS_LOG(WARN, "sstable trailer deserialize error: ret=[%d], payload_len=[%ld]", ret, payload_len);
  }

  return ret;
}

int get_sstable_schema(const int fd, const sstable::ObSSTableTrailer& sstable_trailer, sstable::ObSSTableSchema& schema)
{
  int ret = OB_SUCCESS;
  const char* buffer = NULL;
  int64_t pos = 0;
  ObRecordHeader record_header;
  const char* payload_ptr = NULL;
  int64_t payload_len = 0;
  const int16_t magic = ObSSTableWriter::SCHEMA_MAGIC;

  if (OB_SUCCESS != (ret = read_record(fd, sstable_trailer.get_schema_record_offset(), sstable_trailer.get_schema_record_size(), buffer)))
  {
    TBSYS_LOG(WARN, "read record error: ret=[%d], fd=[%d], offset=[%ld], size=[%ld]", ret, fd, sstable_trailer.get_schema_record_offset(), sstable_trailer.get_schema_record_size());
  }
  else if (OB_SUCCESS != (ret = ObRecordHeader::check_record(buffer, sstable_trailer.get_schema_record_size(), magic, record_header, payload_ptr, payload_len)))
  {
    TBSYS_LOG(WARN, "check record error: ret=[%d], size[%ld]", ret, sstable_trailer.get_schema_record_size() + record_header.get_serialize_size());
  }
  else if (OB_SUCCESS != (ret = schema.deserialize(payload_ptr, payload_len, pos)))
  {
    TBSYS_LOG(WARN, "deserialize error: ret=[%d], size=[%ld]", ret, payload_len);
    ret = OB_ERROR;
  }
  else
  {
    TBSYS_LOG(INFO, "get sstable schema success");
    schema.dump();
  }

  return ret;
}

int get_block_index(const int fd, const sstable::ObSSTableTrailer& sstable_trailer, sstable::ObSSTableBlockIndexHeader& block_index_header, sstable::ObSSTableBlockIndexItem*& block_index_item)
{
  int ret = OB_SUCCESS;
  const char* buffer = NULL;
  int64_t pos = 0;
  ObRecordHeader record_header;
  const char* payload_ptr = NULL;
  int64_t payload_len = 0;
  const int16_t magic = ObSSTableWriter::BLOCK_INDEX_MAGIC;

  if (OB_SUCCESS != (ret = read_record(fd, sstable_trailer.get_block_index_record_offset(), sstable_trailer.get_block_index_record_size(), buffer)))
  {
    TBSYS_LOG(WARN, "read record error: ret=[%d], fd=[%d], offset=[%ld], size=[%ld]", ret, fd, sstable_trailer.get_block_index_record_offset(), sstable_trailer.get_block_index_record_size());
  }
  else if (OB_SUCCESS != (ret = ObRecordHeader::check_record(buffer, sstable_trailer.get_block_index_record_size(), magic, record_header, payload_ptr, payload_len)))
  {
    TBSYS_LOG(WARN, "check record error: ret=[%d], size=[%ld]", ret, sstable_trailer.get_block_index_record_size());
  }
  else if (OB_SUCCESS != (ret = block_index_header.deserialize(payload_ptr, block_index_header.get_serialize_size(), pos)))
  {
    TBSYS_LOG(WARN, "block index header deserialize error: ret=[%d], size=[%ld]", ret, block_index_header.get_serialize_size());
  }
  else if (NULL == (block_index_item = (ObSSTableBlockIndexItem*)ob_malloc(block_index_header.sstable_block_count_ * sizeof(ObSSTableBlockIndexItem))))
  {
    TBSYS_LOG(WARN, "malloc memeory error: size=[%ld]", block_index_header.sstable_block_count_ * sizeof(ObSSTableBlockIndexItem));
    ret = OB_ERROR;
  }
  else
  {
    for (int64_t i = 0; i < block_index_header.sstable_block_count_; i ++)
    {
      pos = 0;
      if (OB_SUCCESS != (ret = block_index_item[i].deserialize(payload_ptr + block_index_header.get_serialize_size() + i * block_index_item[0].get_serialize_size(), block_index_item[0].get_serialize_size(), pos)))
      {
        TBSYS_LOG(WARN, "deserialize error: i=[%ld], size=[%ld]", i, block_index_item[0].get_serialize_size());
        break;
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    TBSYS_LOG(INFO, "block count=[%ld]", block_index_header.sstable_block_count_);
  }

  return ret;
}

int sstable_write_test(const int fd, const PerfTestConfig& config, SrcSSTable& src_sstable)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  compactsstablev2::ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t block_size = 64 * 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  comp_name.assign_ptr(const_cast<char*>(config.compress_name_), static_cast<ObString::obstr_size_t>(strlen(config.compress_name_) + 1));
  file_path.assign_ptr(const_cast<char*>(config.dst_sstable_path_), static_cast<ObString::obstr_size_t>(strlen(config.dst_sstable_path_) + 1));

  remove(config.dst_sstable_path_);

  if (OB_SUCCESS != (ret = get_block(fd, config, src_sstable)))
  {
    TBSYS_LOG(WARN, "load block error: ret=[%d], fd=[%d]", ret, fd);
  }
  else if (OB_SUCCESS != (ret = make_version_range(version_range, 0)))
  {
    TBSYS_LOG(WARN, "make version range error");
  }
  else if (OB_SUCCESS != (ret = make_schema(schema)))
  {
    TBSYS_LOG(WARN, "make schema error");
  }
  else if (OB_SUCCESS != (ret = make_range(range, 1003, 0, 0, 0, 0)))
  {
    TBSYS_LOG(WARN, "make range error");
  }
  else if (OB_SUCCESS != (ret = writer.set_sstable_param(version_range, store_type,table_count, block_size, comp_name, def_sstable_size, min_split_sstable_size)))
  {
    TBSYS_LOG(WARN, "set sstable param error");
  }
  else if (OB_SUCCESS != (ret = writer.set_table_info(1003, schema, range)))
  {
    TBSYS_LOG(WARN, "set tablet info error");
  }
  else if (OB_SUCCESS != (ret = writer.set_sstable_filepath(file_path)))
  {
    TBSYS_LOG(WARN, "set sstable filepath error");
  }
  else
  {
    //99750
    struct timeval start;
    struct timeval end;
    long t;
    bool is_split;

    gettimeofday(&start, 0);
    for (int64_t k = 0; k < 1; k ++)
    {
      for (int64_t i = 0; i < 99759; i ++)
      {
        if (OB_SUCCESS != (ret = writer.append_row(src_sstable.row_array_[i], is_split)))
        {
          TBSYS_LOG(WARN, "append row error");
          break;
        }
      }
    }
    gettimeofday(&end, 0);
    t = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
    TBSYS_LOG(WARN, "time = %ld", t);

    writer.finish();
  }

  return ret;
}

int sstable_scan_test(const int fd, const PerfTestConfig& config, SrcSSTable& src_sstable)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  compactsstablev2::ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t block_size = 64 * 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  comp_name.assign_ptr(const_cast<char*>(config.compress_name_), static_cast<ObString::obstr_size_t>(strlen(config.compress_name_) + 1));
  file_path.assign_ptr(const_cast<char*>(config.dst_sstable_path_), static_cast<ObString::obstr_size_t>(strlen(config.dst_sstable_path_) + 1));

  remove(config.dst_sstable_path_);

  if (OB_SUCCESS != (ret = make_version_range(version_range, 0)))
  {
    TBSYS_LOG(WARN, "make version range error");
  }
  else if (OB_SUCCESS != (ret = make_schema(schema)))
  {
    TBSYS_LOG(WARN, "make schema error");
  }
  else if (OB_SUCCESS != (ret = make_range(range, 1003, 0, 0, 0, 0)))
  {
    TBSYS_LOG(WARN, "make range error");
  }
  else if (OB_SUCCESS != (ret = writer.set_sstable_param(version_range, store_type,table_count, block_size, comp_name, def_sstable_size, min_split_sstable_size)))
  {
    TBSYS_LOG(WARN, "set sstable param error");
  }
  else if (OB_SUCCESS != (ret = writer.set_table_info(1003, schema, range)))
  {
    TBSYS_LOG(WARN, "set tablet info error");
  }
  else if (OB_SUCCESS != (ret = writer.set_sstable_filepath(file_path)))
  {
    TBSYS_LOG(WARN, "set sstable filepath error");
  }
  else if (OB_SUCCESS != (ret = get_block2(fd, config, src_sstable, writer)))
  {
    TBSYS_LOG(WARN, "load block error: ret=[%d], fd=[%d]", ret, fd);
  }
  else
  {
    writer.finish();
  }

  if (OB_SUCCESS == ret)
  {
    FileInfoCache g_fic;
    ObSSTableBlockCache g_block_cache(g_fic);
    ObSSTableBlockIndexCache g_index_cache(g_fic);
    
    if (OB_SUCCESS != (ret = g_fic.init(1024)))
    {
      TBSYS_LOG(WARN, "gfic init error");
    }
    else if (OB_SUCCESS != (ret = g_block_cache.init(100 * 1024 * 1024)))
    {
      TBSYS_LOG(WARN, "g block cache init error");
    }
    else if (OB_SUCCESS != (ret = g_index_cache.init(1024 * 1024 * 1024)))
    {
      TBSYS_LOG(WARN, "g block index cache init error");
    }

    ObScanParam s_scan_param;
    char table_name_str[1024];
    ObString table_name;
    table_name.assign_ptr(table_name_str, 1024);
    table_name.write("bought_shop_info", 30);

    ObNewRange range;
    range.start_key_.set_min_row();
    range.border_flag_.set_min_value();
    range.end_key_.set_max_row();
    range.border_flag_.set_max_value();
    
    if (OB_SUCCESS != (ret = s_scan_param.set(1003, table_name, range, true)))
    {
      TBSYS_LOG(WARN, "s_scan_param set error");
    }
    else
    {
      s_scan_param.set_scan_direction(ScanFlag::FORWARD);
    }

    for (int64_t i = 1; i <= 19; i ++)
    {
      if (OB_SUCCESS != (ret = s_scan_param.add_column(i)))
      {
        TBSYS_LOG(WARN, "add column error");
        break;
      }
    }

    ObSSTableScanParam sstable_scan_param;
    if (OB_SUCCESS != (ret = sstable_scan_param.assign(s_scan_param)))
    {
      TBSYS_LOG(WARN, "sstable scan param assign error");
    }
    else
    {
      //sstable_scan_param.set_full_row_scan(true);
      sstable_scan_param.set_rowkey_column_count(3);
      sstable_scan_param.set_not_exit_col_ret_nop(true);
    }

    ObCompactSSTableScanner scanner;
    ObCompactSSTableScanner::ScanContext scan_context;
    ObCompactSSTableReader reader(g_fic);

    if (OB_SUCCESS != (ret = reader.init(1)))
    {
      TBSYS_LOG(WARN, "init error");
    }
    else
    {
      scan_context.sstable_reader_ = &reader;
      scan_context.block_cache_ = &g_block_cache;
      scan_context.block_index_cache_ = &g_index_cache;
      if (OB_SUCCESS != (ret = scanner.set_scan_param(&sstable_scan_param, &scan_context)))
      {
        TBSYS_LOG(WARN, "set scan param error");
      }
    }

    const ObRow* row_value = NULL;

    int64_t j= 0;
    struct timeval start;
    struct timeval end;
    long t;
    gettimeofday(&start, 0);

    //ProfilerStart("CPUProfile");
    for (int64_t z = 0; z < 1; z ++)
    {
      if (OB_SUCCESS != (ret = scanner.set_scan_param(&sstable_scan_param, &scan_context)))
      {
        TBSYS_LOG(WARN, "set scan param error");
        break;
      }
      while (OB_SUCCESS == ret && OB_ITER_END != ret)
      {
        j ++;
        ret = scanner.get_next_row(row_value);
      }
      
      if (OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
      }
    }

    //ProfilerStop();
    gettimeofday(&end, 0);
    t = (end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec;
    TBSYS_LOG(WARN, "time=%ld", t);
    TBSYS_LOG(WARN, "j=%ld", j);
  }

  return ret;
}

int load_sstable_file(const PerfTestConfig& config, SrcSSTable& src_sstable)
{
  int ret = OB_SUCCESS;
  int fd = 0;

  if (OB_SUCCESS != (ret = get_sstable_size(config.file_name_, src_sstable.sstable_size_)))
  {
    TBSYS_LOG(ERROR, "get sstable size error: ret=[%d], file_name=[%s]", ret, config.file_name_);
  }
  else
  {
    TBSYS_LOG(INFO, "sstable size is: [%ld]", src_sstable.sstable_size_);
  }

  //open sstable file
  if (OB_SUCCESS == ret)
  {
    if (-1 == (fd = open(config.file_name_, O_RDONLY)))
    {
      TBSYS_LOG(WARN, "open file error: file_name=[%s]", config.file_name_);
      ret = OB_ERROR;
    }
    else
    {
      TBSYS_LOG(INFO, "open file success: fd=[%d]", fd);
    }
  }

  //load sstable trailer offset
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_sstable_trailer_offset(fd, src_sstable.sstable_size_, src_sstable.sstable_trailer_offset_)))
    {
      TBSYS_LOG(WARN, "get sstable trailer error: ret=[%d], fd=[%d], sstable_size=[%ld]", ret, fd, src_sstable.sstable_size_);
    }
    else
    {
      TBSYS_LOG(INFO, "get sstable trailer offset success: fd=[%d], trailer_offset=[%ld]", fd, src_sstable.sstable_trailer_offset_);
    }
  }

  //sstable trailer
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_sstable_trailer(fd, src_sstable.sstable_trailer_offset_, src_sstable.sstable_trailer_)))
    {
     TBSYS_LOG(WARN, "get sstable trailer error: ret=[%d], trailer_offset=[%ld]", ret, src_sstable.sstable_trailer_offset_);
    }
    else
    {
      TBSYS_LOG(INFO, "get sstable trailer succeess");
      src_sstable.sstable_trailer_.dump();
    }
  }

  //sstable schema
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_sstable_schema(fd, src_sstable.sstable_trailer_, src_sstable.schema_)))
    {
      TBSYS_LOG(WARN, "get sstable schema error: ret=[%d]", ret);
    }
  }

  //block index
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = get_block_index(fd, src_sstable.sstable_trailer_, src_sstable.block_index_header_, src_sstable.block_index_item_)))
    {
      TBSYS_LOG(WARN, "get block index error: ret=[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (0 == strcmp(config.test_name_, "sstable_write_test"))
    {
      if (OB_SUCCESS != (ret = sstable_write_test(fd, config, src_sstable)))
      {
        TBSYS_LOG(WARN, "sstable write test error: ret=[%d]", ret);
      }
    }
    else if (0 == strcmp(config.test_name_, "sstable_scan_test"))
    {
      if (OB_SUCCESS != (ret = sstable_scan_test(fd, config, src_sstable)))
      {
        TBSYS_LOG(WARN, "sstable scan test error: ret=[%d]", ret);
      }
    }
  }

  //close sstable file
  close(fd);

  return ret;
}

int main(int argc, char** argv)
{
  UNUSED(argc);
  UNUSED(argv);
  int ret = OB_SUCCESS;
  SrcSSTable src_sstable;
  PerfTestConfig config;
  ob_init_memory_pool();
  int64_t quiet = 0;

  if (OB_SUCCESS != (ret = TBSYS_CONFIG.load(CONFIG_FILE_NAME)))
  {
    TBSYS_LOG(WARN, "load config file error: config_file=[%s]", CONFIG_FILE_NAME);
  }
  else
  {
    config.file_name_ = TBSYS_CONFIG.getString("src_sstable", "file_name");
    config.test_name_ = TBSYS_CONFIG.getString("test_info", "test_name");
    config.dst_sstable_path_ = TBSYS_CONFIG.getString("test_info", "dst_sstable_path");
  }
  
  if (OB_SUCCESS == ret)
  {
    if (0 == strcmp(config.test_name_, "sstable_write_test"))
    {
      config.row_count_ = TBSYS_CONFIG.getInt("test_info", "row_count");
      config.compress_name_ = TBSYS_CONFIG.getString("test_info", "compress_name", "lz4_1.0");
    }
    else if (0 == strcmp(config.test_name_, "sstable_scan_test"))
    {
      config.row_count_ = TBSYS_CONFIG.getInt("test_info", "row_count");
      config.compress_name_ = TBSYS_CONFIG.getString("test_info", "compress_name", "lz4_1.0");
    }
  }

  if (OB_SUCCESS == ret)
  {
    int tmp_ret = 0;
    while (-1 != (tmp_ret = getopt(argc, argv, "qf:t:n:c:h:d")))
    {
      switch(tmp_ret)
      {
        case 'q':
          quiet = 1;
          break;
        case 'f':
          config.file_name_ = optarg;
          break;
        case 't':
          config.test_name_ = optarg;
          break;
        case 'n':
          config.row_count_ = atol(optarg);
          break;
        case 'c':
          config.compress_name_ = optarg;
          break;
        case 'd':
          config.dst_sstable_path_ = optarg;
          break;
        default:
          TBSYS_LOG(ERROR, "the args is not defined: tmp_ret=[%d]", tmp_ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (1 == quiet)
    {
      TBSYS_LOGGER.setLogLevel("WARN");
    }
  }

  //load sstable file
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = load_sstable_file(config, src_sstable)))
    {
      TBSYS_LOG(ERROR, "load sstable file error: ret=[%d]", ret);
    }
  }

  return ret;
}
