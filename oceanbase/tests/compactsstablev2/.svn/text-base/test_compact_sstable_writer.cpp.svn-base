#include "gtest/gtest.h"
#include "common/ob_define.h"
#include "compactsstablev2/ob_compact_sstable_writer.h"
#include "common/compress/ob_compressor.h"
#include "test_compact_common.h"
#include <inttypes.h>

using namespace oceanbase;
using namespace common;
using namespace compactsstablev2;

class TestCompactSSTableWriter : public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }

  /**
   * check sstable file
   * @param fd: file fd
   * @param compressor: compressor
   * @param compressor_str: compressor str
   * @param start_row_num: start row num
   * @param schema: schema
   * @param version_range: version range
   * @param range_start_num: range start num
   * @param range_end_num: range end num
   * @param range_start_flag: 0(min)
   *                          1(uninclusive start)
   *                          2(inclusive start)
   * @param range_end_flag:   0(max)
   *                          1(uninclusive end)
   *                          2(inclusive end)
   * @param store_type: store type
   * @param table_id: table id
   * @param table_count: table count
   * @param block_size: block size
   * @param row_checksum: row checksum
   */
  void check_sstable_file(const int fd,
      ObCompressor* compressor,
      const ObString& compressor_str,
      const int64_t* start_row_num,
      const ObSSTableSchema& schema,
      const ObFrozenMinorVersionRange& version_range,
      const int64_t* range_start_num,
      const int64_t* range_end_num,
      const int* range_start_flag,
      const int* range_end_flag,
      const ObCompactStoreType& store_type,
      const uint64_t* table_id,
      const int64_t table_count,
      const int64_t block_size,
      const uint64_t row_checksum)
  {
    int ret = OB_SUCCESS;

    int64_t file_size = 0;
    ObRecordHeaderV2 record_header;
    ObSSTableTrailerOffset trailer_offset;
    ObSSTableHeader sstable_header;
    ObSSTableTableIndex* table_index_ptr = NULL;
    ObSSTableBlockIndex** block_index_ptr = NULL;
    ObSSTableBlockHeader* block_header_ptr = NULL;
    ObSSTableBlockRowIndex* row_index_ptr = NULL;
    char** block_endkey_ptr = NULL;

    char* buf = (char*)ob_malloc(10 * 1024 * 1024);
    int64_t read_offset = 0;
    int64_t read_size = 0;
    int64_t real_read_size = 0;
    const char* payload_ptr = NULL;
    int64_t payload_size = 0;
    char* uncomp_buf = (char*)ob_malloc(1024 * 1024);
    //int64_t uncomp_buf_size = 1024 * 1024;
    int64_t uncomp_buf_len = 0;
    int64_t ii = 0;

    int64_t cur_table_offset = 0;
    char cur_row_buf[1024];
    ObCompactCellWriter row_writer;
    row_writer.init(cur_row_buf, 1024, store_type);
    ObRow row;
    ObRowkey rowkey;

    //sstable size
    file_size = get_file_size(fd);
    ASSERT_NE(0, file_size);

    //sstable trailer offset
    read_offset = file_size - sizeof(ObSSTableTrailerOffset);
    read_size = sizeof(ObSSTableTrailerOffset);
    lseek(fd, read_offset, SEEK_SET);
    real_read_size = read(fd, buf, read_size);
    ASSERT_EQ(read_size, real_read_size);
    memcpy(&trailer_offset, buf, read_size);
    ASSERT_EQ(static_cast<int64_t>(file_size - sizeof(ObSSTableTrailerOffset)
        - sizeof(ObSSTableHeader) - sizeof(ObRecordHeaderV2)),
        trailer_offset.offset_);

    //sstable header
    read_offset = trailer_offset.offset_;
    read_size = sizeof(ObSSTableHeader) + sizeof(ObRecordHeaderV2);
    lseek(fd, read_offset, SEEK_SET);
    real_read_size = read(fd, buf, read_size);
    ret = ObRecordHeaderV2::check_record(buf, real_read_size,
        OB_SSTABLE_HEADER_MAGIC, record_header, payload_ptr, payload_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    memcpy(&sstable_header, payload_ptr, payload_size);
    ASSERT_EQ(static_cast<int64_t>(sizeof(ObSSTableHeader)), sstable_header.header_size_);
    ASSERT_EQ(sstable_header.row_checksum_, row_checksum);
    ASSERT_EQ(1 * ObSSTableHeader::SSTABLE_HEADER_VERSION, sstable_header.header_version_);
    ASSERT_EQ(version_range.major_version_, sstable_header.major_version_);
    ASSERT_EQ(version_range.major_frozen_time_, sstable_header.major_frozen_time_);
    ASSERT_EQ(version_range.next_transaction_id_, sstable_header.next_transaction_id_);
    ASSERT_EQ(version_range.next_commit_log_id_, sstable_header.next_commit_log_id_);
    ASSERT_EQ(version_range.start_minor_version_, sstable_header.start_minor_version_);
    ASSERT_EQ(version_range.end_minor_version_, sstable_header.end_minor_version_);
    ASSERT_EQ(version_range.is_final_minor_version_, sstable_header.is_final_minor_version_);
    ASSERT_EQ(store_type, sstable_header.row_store_type_);
    ASSERT_EQ(0, sstable_header.reserved16_);
    ASSERT_EQ(static_cast<uint64_t>(1001), sstable_header.first_table_id_);
    ASSERT_EQ(table_count, sstable_header.table_count_);
    ASSERT_EQ(static_cast<int64_t>(sizeof(ObSSTableTableIndex)), sstable_header.table_index_unit_size_);
    ASSERT_EQ(sstable_header.table_count_ * 9, sstable_header.schema_array_unit_count_);
    ASSERT_EQ(static_cast<int>(sizeof(ObSSTableTableSchemaItem)), sstable_header.schema_array_unit_size_);
    ASSERT_EQ(block_size, sstable_header.block_size_);
    for (int64_t i = 0; i < compressor_str.length(); i ++)
    {
      ASSERT_EQ(compressor_str.ptr()[i],
          sstable_header.compressor_name_[i]);
    }
    for (int64_t i = 0; i < 64; i ++)
    {
      ASSERT_EQ(0, sstable_header.reserved64_[i]);
    }

    //sstable schema
    read_offset = sstable_header.schema_record_offset_;
    read_size = sstable_header.schema_array_unit_count_
      * sizeof(ObSSTableTableSchemaItem) + sizeof(ObRecordHeaderV2);
    lseek(fd, read_offset, SEEK_SET);
    real_read_size = read(fd, buf, read_size);
    ASSERT_EQ(real_read_size, read_size);
    ret = ObRecordHeaderV2::check_record(buf, real_read_size,
        OB_SSTABLE_SCHEMA_MAGIC, record_header,
        payload_ptr, payload_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    int64_t def_column_count = sstable_header.schema_array_unit_count_;
    int64_t tmp_def_column_count;
    const ObSSTableSchemaColumnDef* def = schema.get_column_def_array(
        tmp_def_column_count);
    const ObSSTableTableSchemaItem* def_item
      = (const ObSSTableTableSchemaItem*)(payload_ptr);
    ASSERT_TRUE(NULL != def);
    //ASSERT_EQ(schema.get_column_count(), def_column_count);
    for (int64_t i = 0; i < def_column_count; i ++)
    {
      ASSERT_EQ(def_item[i].table_id_, static_cast<uint64_t>(1001 + (i/9)));
      ASSERT_EQ(def_item[i].column_id_, def[i%9].column_id_);
      ASSERT_EQ(def_item[i].rowkey_seq_, def[i%9].rowkey_seq_);
      ASSERT_EQ(def_item[i].column_attr_, 0);
      ASSERT_EQ(def_item[i].column_value_type_,
          def[i%9].column_value_type_);
      for (int64_t j = 0; j < 3; j ++)
      {
        ASSERT_EQ(def_item[i].reserved16_[j], 0);
      }
    }

    //table index
    read_offset = sstable_header.table_index_offset_;
    read_size = sstable_header.table_count_
      * sstable_header.table_index_unit_size_
      + sizeof(ObRecordHeaderV2);
    lseek(fd, read_offset, SEEK_SET);
    real_read_size = read(fd, buf, read_size);
    ASSERT_EQ(real_read_size, read_size);
    ret = ObRecordHeaderV2::check_record(buf, real_read_size,
        OB_SSTABLE_TABLE_INDEX_MAGIC, record_header,
        payload_ptr, payload_size);
    ASSERT_EQ(OB_SUCCESS, ret);
    table_index_ptr = (ObSSTableTableIndex*)ob_malloc(
        sizeof(ObSSTableTableIndex) * sstable_header.table_count_);
    memcpy(table_index_ptr, payload_ptr, payload_size);
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      ASSERT_EQ(static_cast<int64_t>(sizeof(ObSSTableTableIndex)), table_index_ptr[i].size_);
      ASSERT_EQ(1 * ObSSTableTableIndex::TABLE_INDEX_VERSION,
          table_index_ptr[i].version_);
      ASSERT_EQ(table_id[i], table_index_ptr[i].table_id_);
      ASSERT_EQ(9, table_index_ptr[i].column_count_);
      ASSERT_EQ(2, table_index_ptr[i].columns_in_rowkey_);
      ASSERT_EQ(SSTABLE_BLOOMFILTER_HASH_COUNT,
          table_index_ptr[i].bloom_filter_hash_count_);
      for (int64_t j = 0; j < 8; j ++)
      {
        ASSERT_EQ(0, table_index_ptr[i].reserved_[j]);
      }
    }

    //table bloomfilter
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      read_offset = table_index_ptr[i].bloom_filter_offset_;
      read_size = table_index_ptr[i].bloom_filter_size_;
      lseek(fd, read_offset, SEEK_SET);
      real_read_size = read(fd, buf, read_size);
      ASSERT_EQ(real_read_size, read_size);
      ret = ObRecordHeaderV2::check_record(buf, real_read_size,
          OB_SSTABLE_TABLE_BLOOMFILTER_MAGIC,
          record_header, payload_ptr, payload_size);
      ASSERT_EQ(OB_SUCCESS, ret);
    }

    //table range
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      read_offset = table_index_ptr[i].range_keys_offset_;
      read_size = 1
        + table_index_ptr[i].range_start_key_length_
        + table_index_ptr[i].range_end_key_length_
        + sizeof(ObRecordHeaderV2);
      lseek(fd, read_offset, SEEK_SET);
      real_read_size = read(fd, buf, read_size);
      ASSERT_EQ(real_read_size, read_size);
      ret = ObRecordHeaderV2::check_record(buf, real_read_size,
          OB_SSTABLE_TABLE_RANGE_MAGIC,
          record_header, payload_ptr, payload_size);
      ASSERT_EQ(OB_SUCCESS, ret);
      int64_t pos = 0;
      int8_t* tmp_data = (int8_t*)payload_ptr;
      ObNewRange tmp_range;
      make_range(tmp_range, table_id[i], range_start_num[i],
          range_end_num[i], range_start_flag[i], range_end_flag[i]);
      ASSERT_EQ(*tmp_data, tmp_range.border_flag_.get_data());
      pos ++;
      row_writer.reset();
      row_writer.append_rowkey(tmp_range.start_key_);
      for (int64_t j = 0; j < table_index_ptr[i].range_start_key_length_;
          j ++)
      {
        ASSERT_EQ(cur_row_buf[j], payload_ptr[pos + j]);
      }
      pos += table_index_ptr[i].range_start_key_length_;
      row_writer.reset();
      row_writer.append_rowkey(tmp_range.end_key_);
      for (int64_t j = 0; j < table_index_ptr[i].range_end_key_length_;
          j ++)
      {
        ASSERT_EQ(cur_row_buf[j], payload_ptr[pos + j]);
      }
    }

    //block index
    block_index_ptr = (ObSSTableBlockIndex**)ob_malloc(
        sstable_header.table_count_ * sizeof(ObSSTableBlockIndex*));
    ASSERT_TRUE(NULL != block_index_ptr);
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      if (table_index_ptr[i].block_count_ != 0)
      {
        block_index_ptr[i] = (ObSSTableBlockIndex*)ob_malloc(
            (table_index_ptr[i].block_count_ + 1) *
            sizeof(ObSSTableBlockIndex));
        read_offset = table_index_ptr[i].block_index_offset_;
        read_size = table_index_ptr[i].block_index_size_;
        lseek(fd, read_offset, SEEK_SET);
        real_read_size = read(fd, buf, read_size);
        ASSERT_EQ(real_read_size, read_size);
        ret = ObRecordHeaderV2::check_record(buf, real_read_size,
            OB_SSTABLE_BLOCK_INDEX_MAGIC,
            record_header, payload_ptr, payload_size);
        ASSERT_EQ(OB_SUCCESS, ret);
        memcpy(block_index_ptr[i], payload_ptr, payload_size);
      }
    }

    //block endkey
    block_endkey_ptr = (char**)ob_malloc(
        sstable_header.table_count_ * sizeof(char*));
    ASSERT_TRUE(NULL != block_endkey_ptr);
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      if (table_index_ptr[i].block_count_ != 0)
      {
        read_offset = table_index_ptr[i].block_endkey_offset_;
        read_size = table_index_ptr[i].block_endkey_size_;
        lseek(fd, read_offset, SEEK_SET);
        real_read_size = read(fd, buf, read_size);
        ASSERT_EQ(real_read_size, read_size);
        ret = ObRecordHeaderV2::check_record(buf, real_read_size,
            OB_SSTABLE_BLOCK_ENDKEY_MAGIC,
            record_header, payload_ptr, payload_size);
        ASSERT_EQ(OB_SUCCESS, ret);
        block_endkey_ptr[i] = (char*)ob_malloc(payload_size);
        memcpy(block_endkey_ptr[i], payload_ptr, payload_size);
      }
    }

    //block
    for (int64_t i = 0; i < sstable_header.table_count_; i ++)
    {
      cur_table_offset = table_index_ptr[i].block_data_offset_;
      ii = start_row_num[0];
      for (int64_t j = 0; j < table_index_ptr[i].block_count_; j ++)
      {
        read_offset = block_index_ptr[i][j].block_data_offset_;
        read_size = block_index_ptr[i][j + 1].block_data_offset_
          - block_index_ptr[i][j].block_data_offset_;
        lseek(fd, read_offset + table_index_ptr[i].block_data_offset_,
            SEEK_SET);
        real_read_size = read(fd, buf, read_size);
        ASSERT_EQ(real_read_size, read_size);
        ret = ObRecordHeaderV2::check_record(buf, real_read_size,
            OB_SSTABLE_BLOCK_DATA_MAGIC,
            record_header, payload_ptr, payload_size);
        ASSERT_EQ(OB_SUCCESS, ret);
        if (record_header.data_length_ == record_header.data_zlength_)
        {
          memcpy(uncomp_buf, payload_ptr, payload_size);
          uncomp_buf_len = payload_size;
        }
        else
        {
          ret = compressor->decompress(payload_ptr, payload_size,
              uncomp_buf, record_header.data_length_, uncomp_buf_len);
          ASSERT_EQ(OB_SUCCESS, ret);
        }

        int64_t pos = 0;
        block_header_ptr = (ObSSTableBlockHeader*)uncomp_buf;
        ASSERT_EQ(0, block_header_ptr->reserved64_);
        pos = sizeof(ObSSTableBlockHeader);
        for (int64_t k = 0; k < block_header_ptr->row_count_; k ++)
        {
          make_row(row, table_index_ptr[i].table_id_, ii, 0);
          row_writer.reset();
          row_writer.append_row(row);
          for (int64_t z = 0; z < row_writer.size(); z ++)
          {
            ASSERT_EQ(cur_row_buf[z], uncomp_buf[pos + z]);
          }

          row_index_ptr = (ObSSTableBlockRowIndex*)(&uncomp_buf[
            block_header_ptr->row_index_offset_
            + k * sizeof(ObSSTableBlockRowIndex)]);
          ASSERT_EQ(pos, row_index_ptr->row_offset_);
          pos += row_writer.size();

          if (k == block_header_ptr->row_count_ - 1)
          {
            row_writer.reset();
            make_rowkey(rowkey, ii, 0);
            row_writer.append_rowkey(rowkey);
            char* tmp_buf = (char*)(block_endkey_ptr[i]
              + block_index_ptr[i][j].block_endkey_offset_);
            ASSERT_EQ(row_writer.size(), block_index_ptr[i][j + 1].block_endkey_offset_ - block_index_ptr[i][j].block_endkey_offset_);
            for (int64_t z = 0; z < row_writer.size(); z++)
            {
              ASSERT_EQ(cur_row_buf[z], tmp_buf[z]);
            }
          }
          ii ++;
        }
        row_index_ptr = (ObSSTableBlockRowIndex*)(&uncomp_buf[
          block_header_ptr->row_index_offset_
          + block_header_ptr->row_count_
          * sizeof(ObSSTableBlockRowIndex)]);
        ASSERT_EQ(pos, row_index_ptr->row_offset_);
      }
    }

    ob_free(buf);
    ob_free(uncomp_buf);

  }
};

//TODO:check bloomfilter

/**
 * test construct
 */
TEST_F(TestCompactSSTableWriter, construct)
{
  ObCompactSSTableWriter writer;

  EXPECT_FALSE(writer.sstable_inited_);
  EXPECT_FALSE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_FALSE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL == writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_FALSE(writer.split_flag_);
  ASSERT_EQ(0, writer.def_sstable_size_);
  ASSERT_EQ(0, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_TRUE(0 == writer.sstable_checksum_);
  ASSERT_EQ(0, writer.sstable_table_init_count_);
  //sstable_writer_buffer_
  //sstable_
  //table_
  //block_
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

TEST_F(TestCompactSSTableWriter, destruct)
{
}

/**
 * test set_sstable_param1
 * --DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param1)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t table_count = 5;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name, def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_FALSE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_FALSE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL != writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_FALSE(writer.split_flag_);
  ASSERT_EQ(0, writer.def_sstable_size_);
  ASSERT_EQ(0, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_TRUE(0 == writer.sstable_checksum_);
  ASSERT_EQ(5, writer.sstable_table_init_count_);
  //sstable_writer_buffer_
  //sstable_
  //table_
  //block_
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * test set_sstable_param2
 * --DENSE_DENSE
 * --not split
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param2)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_FALSE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_FALSE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL != writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_FALSE(writer.split_flag_);
  ASSERT_EQ(0, writer.def_sstable_size_);
  ASSERT_EQ(0, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_TRUE(0 == writer.sstable_checksum_);
  ASSERT_EQ(1, writer.sstable_table_init_count_);
  //sstable_writer_buffer_
  //sstable_
  //table_
  //block_
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * test set_sstable_param3
 * --DENSE_DENSE
 * --split
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param3)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 100;
  const int64_t min_split_sstable_size = 50;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_FALSE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_FALSE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL != writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_TRUE(writer.split_flag_);
  ASSERT_EQ(100, writer.def_sstable_size_);
  ASSERT_EQ(50, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_TRUE(0 == writer.sstable_checksum_);
  ASSERT_EQ(1, writer.sstable_table_init_count_);
  //sstable_writer_buffer_
  //sstable_
  //table_
  //block_
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * test set_sstable_param4
 * compress string == NULL
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param4)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 100;
  const int64_t min_split_sstable_size = 50;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_FALSE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_FALSE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL == writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_TRUE(writer.split_flag_);
  ASSERT_EQ(100, writer.def_sstable_size_);
  ASSERT_EQ(50, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_TRUE(0 == writer.sstable_checksum_);
  //sstable_writer_buffer_
  //sstable_
  //table_
  //block_
  ASSERT_EQ(1, writer.sstable_table_init_count_);
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * test set_sstable_param5
 * sstable reinited
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param5)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 100;
  const int64_t min_split_sstable_size = 50;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param6
 * invalid row store type
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param6)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 100;
  const int64_t min_split_sstable_size = 50;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param7
 * invalid version range
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param7)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param8
 * --invalid version range
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param8)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 7);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param9
 * --invalid table count
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param9)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t table_count = -1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param10
 * --invalid block size
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param10)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 0;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param11
 * --min/def sstable size error
 *  --min_split_sstable_size > def_sstable_size
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param11)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 50;
  const int64_t min_split_sstable_size = 100;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param12
 * --min/def sstable size error
 *  --min_split_sstable_size != 0 && def_sstable_size == 0
 * --store_type==DENSE_SPARSE
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param12)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 100;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set_sstable_param13
 * --invalid compressor
 */
TEST_F(TestCompactSSTableWriter, set_sstable_param13)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 1);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_ERROR, ret);
}

/**
 * test set table info1
 * --success(one table)
 */
TEST_F(TestCompactSSTableWriter, set_table_info1)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 2);
  ASSERT_EQ(OB_SUCCESS, ret);


  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const uint64_t table_id = 1001;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_TRUE(writer.table_inited_);
  EXPECT_FALSE(writer.sstable_file_inited_);
  EXPECT_TRUE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL != writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_FALSE(writer.split_flag_);
  ASSERT_EQ(0, writer.def_sstable_size_);
  ASSERT_EQ(0, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_TRUE(0 == writer.sstable_checksum_);
  ASSERT_EQ(1, writer.sstable_table_init_count_);
  //sstable_writer_buffer_
  //sstable_
  //table_
  //block_
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * tst set table info2
 * --fail(sstable is not init)
 */
TEST_F(TestCompactSSTableWriter, set_table_info2)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObSSTableSchema schema;
  ObNewRange range;
  const uint64_t table_id = 1001;

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);
 
  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

/**
 * test set sstable filepath1
 * --success
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath1)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;
  ObString file_path;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const uint64_t table_id = 1001;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t file_num = 1;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_TRUE(writer.sstable_inited_);
  EXPECT_TRUE(writer.table_inited_);
  EXPECT_TRUE(writer.sstable_file_inited_);
  EXPECT_TRUE(writer.sstable_first_table_);
  EXPECT_FALSE(writer.not_table_first_row_);
  EXPECT_TRUE(NULL != writer.compressor_);
  EXPECT_TRUE(writer.dio_flag_);
  EXPECT_TRUE(writer.filesys_ == &writer.default_filesys_);
  EXPECT_FALSE(writer.split_buf_inited_);
  EXPECT_FALSE(writer.split_flag_);
  ASSERT_EQ(0, writer.def_sstable_size_);
  ASSERT_EQ(0, writer.min_split_sstable_size_);
  ASSERT_EQ(0, writer.cur_offset_);
  ASSERT_EQ(0, writer.cur_table_offset_);
  ASSERT_TRUE(0 == writer.sstable_checksum_);
  ASSERT_EQ(1, writer.sstable_table_init_count_);
  //sstable_writer_buffer
  //sstable_
  //table_
  //block_
  ASSERT_EQ(0, writer.sstable_trailer_offset_.offset_);
  ASSERT_EQ(0, writer.query_struct_.sstable_size_);
}

/**
 * test set sstable filepath2
 * --fail(sstable not init)
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath2)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObString file_path;

  const int64_t file_num = 1;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(1);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

/**
 * test set sstable filepath3
 * --table not inited
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath3)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;
  ObString file_path;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t file_num = 1;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(1);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_NOT_INIT, ret);
}

/**
 * test set sstable filepath4
 * --sstable file init twice
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath4)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;
  ObString file_path;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const uint64_t table_id = 1001;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t file_num = 1;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_INIT_TWICE, ret);
}

/**
 * test set sstable filepath5
 * --empty file name
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath5)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;
  ObString file_path;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const uint64_t table_id = 1001;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t file_num = -2;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 * test set sstable filepath6
 * --empty file name
 */
TEST_F(TestCompactSSTableWriter, set_sstable_filepath6)
{
  int ret = OB_SUCCESS;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObNewRange range;
  ObSSTableSchema schema;
  ObString file_path;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t table_count = 1;
  const int64_t blocksize = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const uint64_t table_id = 1001;

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, blocksize, comp_name,
      def_sstable_size, min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t file_num = -1;
  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);

  ret = writer.set_sstable_filepath(file_path);
  EXPECT_NE(OB_ERROR, ret);
}

/**
 * test append_row1
 * --DENSE_DENSE
 * --one row
 * --not compress
 */
TEST_F(TestCompactSSTableWriter, append_row1)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  bool is_split = false;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObCompactCellWriter row_writer;
  char buf[100 * 1024];
  memset(buf, 0, sizeof(buf));
  uint64_t row_checksum = 0;
  for (int64_t i = 0; i < 1; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
  
    row_writer.init(buf, 100 * 1024, store_type);
    row_writer.append_row(row);
    row_checksum += ob_crc64_sse42(buf, row_writer.size());
    row_writer.reset();
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
    ret = OB_ERROR;
  }
  else
  {
    int64_t start_row_num = 0;
    const int64_t range_start_num = 0;
    const int64_t range_end_num = 0;
    const int range_start_flag = 0;
    const int range_end_flag = 0;
    check_sstable_file(fd, compressor, comp_name, &start_row_num, schema, version_range,
        &range_start_num, &range_end_num, &range_start_flag, &range_end_flag,
        store_type, &table_id, table_count, block_size, row_checksum);
  }

  //close and delete file
  close(fd);
}

/**
 * test append_row2
 * --DENSE_DENSE
 * --one block(15 row)
 */
TEST_F(TestCompactSSTableWriter, append_row2)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  bool is_split = false;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObCompactCellWriter row_writer;
  char buf[100 * 1024];
  memset(buf, 0, sizeof(buf));
  uint64_t row_checksum = 0;
  for (int64_t i = 0; i < 15; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);

    row_writer.init(buf, 100 * 1024, store_type);
    row_writer.append_row(row);
    row_checksum += ob_crc64_sse42(buf, row_writer.size());
    row_writer.reset();
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }
  
  int64_t start_row_num[1] = {0};
  const int64_t range_start_num = 0;
  const int64_t range_end_num = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  check_sstable_file(fd, compressor, comp_name, start_row_num,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size, row_checksum);

  //close and delete file
  close(fd);
}

/**
 * test append_row3
 * --DENSE_DENSE
 * --three block(15 * 2 + 2 row)
 */
TEST_F(TestCompactSSTableWriter, append_row3)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  bool is_split = false;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObCompactCellWriter row_writer;
  char buf[100 * 1024];
  memset(buf, 0, sizeof(buf));
  uint64_t row_checksum = 0;
  for (int64_t i = 0; i < 32; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);

    row_writer.init(buf, 100 * 1024, store_type);
    row_writer.append_row(row);
    row_checksum += ob_crc64_sse42(buf, row_writer.size());
    row_writer.reset();
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }
  
  int64_t start_row_num[1] = {0};
  const int64_t range_start_num = 0;
  const int64_t range_end_num = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  check_sstable_file(fd, compressor, comp_name, start_row_num,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size, row_checksum);

  //close and delete file
  close(fd);
}

/**
 * test append_row4
 * --DENSE_SPARSE
 * --three table
 */
TEST_F(TestCompactSSTableWriter, append_row4)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_SPARSE;
  uint64_t table_id[3] = {1001, 1002, 1003};
  const int64_t table_count = 3;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  bool is_split = false;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id[0], 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id[0]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id[0], schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObCompactCellWriter row_writer;
  char buf[100 * 1024];
  memset(buf, 0, sizeof(buf));
  uint64_t row_checksum = 0;
  for (int64_t i = 0; i < 100; i ++)
  {
    make_row(row, table_id[0], i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);

    row_writer.init(buf, 100 * 1024, store_type);
    row_writer.append_row(row);
    row_checksum += ob_crc64_sse42(buf, row_writer.size());
    row_writer.reset();
  }

  //table_id:1002
  ret = make_range(range, table_id[1], 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  make_schema(schema, table_id[1]);

  ret = writer.set_table_info(table_id[1], schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 100; i ++)
  {
    make_row(row, table_id[1], i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
    row_writer.init(buf, 100 * 1024, store_type);
    row_writer.append_row(row);
    row_checksum += ob_crc64_sse42(buf, row_writer.size());
    row_writer.reset();
  }

  //table_id:1003
  ret = make_range(range, table_id[2], 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id[2]);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id[2], schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = 0; i < 100; i ++)
  {
    make_row(row, table_id[2], i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
    row_writer.init(buf, 100 * 1024, store_type);
    row_writer.append_row(row);
    row_checksum += ob_crc64_sse42(buf, row_writer.size());
    row_writer.reset();
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }
  
  int64_t start_row_num[3] = {0, 0, 0};
  const int64_t range_start_num[3] = {0, 0, 0};
  const int64_t range_end_num[3] = {0, 0, 0};
  const int range_start_flag[3] = {0, 0, 0};
  const int range_end_flag[3] = {0, 0, 0};
  check_sstable_file(fd, compressor, comp_name, start_row_num,
      schema, version_range, range_start_num, range_end_num,
      range_start_flag, range_end_flag,
      store_type, table_id, table_count, block_size, row_checksum);

  //close and delete file
  close(fd);
}


/**
 * test row_checksum
 * --DENSE_DENSE
 * --not compress
 * --split
 * --(170 row)
 */
TEST_F(TestCompactSSTableWriter, append_row5)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 4000;
  const int64_t min_split_sstable_size = 2000;
  int64_t file_num = 1;
  bool is_split = false;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(1);
  delete_file(2);
  delete_file(3);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObCompactCellWriter row_writer;
  char buf[100 * 1024];
  memset(buf, 0, sizeof(buf));
  uint64_t row_checksum_sstables[3] = {0, 0, 0};
  int64_t tmp_i = 0;
  for (int64_t i = 0; i < 170; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);

    row_writer.init(buf, 100 * 1024, store_type);
    row_writer.append_row(row);
    if (true == is_split)
    {
      tmp_i ++;
      row_checksum_sstables[tmp_i] += ob_crc64_sse42(buf, row_writer.size());
      file_num ++;
      ret = make_file_path(file_path, file_num);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = writer.set_sstable_filepath(file_path);
      ASSERT_EQ(OB_SUCCESS, ret);
      is_split = false;
    }
    else
    {
      row_checksum_sstables[tmp_i] += ob_crc64_sse42(buf, row_writer.size());
    }
    row_writer.reset();
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  ret = make_file_path(file_path, 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }
  int64_t start_row_num1[1] = {0};
  int64_t range_start_num = 0;
  int64_t range_end_num = 56;
  int range_start_flag = 0;
  int range_end_flag = 2;
  check_sstable_file(fd, compressor, comp_name, start_row_num1,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size, row_checksum_sstables[0]);

  close(fd);

  ret = make_file_path(file_path, 2);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }
  int64_t start_row_num2[1] = {57};
  range_start_num = 56;
  range_end_num = 112;
  range_start_flag = 1;
  range_end_flag = 2;
  check_sstable_file(fd, compressor, comp_name, start_row_num2,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size, row_checksum_sstables[1]);

  close(fd);

  ret = make_file_path(file_path, 3);
  ASSERT_EQ(OB_SUCCESS, ret);
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }
  int64_t start_row_num3[1] = {113};
  range_start_num = 112;
  range_end_num = 0;
  range_start_flag = 1;
  range_end_flag = 0;
  check_sstable_file(fd, compressor, comp_name, start_row_num3,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size, row_checksum_sstables[2]);
  close(fd);
}

/**
 * test append_row6
 * --DENSE_DENSE
 * --empty sstable
 */
TEST_F(TestCompactSSTableWriter, append_row6)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  bool is_split = false;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObCompactCellWriter row_writer;
  char buf[100 * 1024];
  memset(buf, 0, sizeof(buf));
  uint64_t row_checksum = 0;
  for (int64_t i = 0; i < 0; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
    row_writer.init(buf, 100 * 1024, store_type);
    row_writer.append_row(row);
    row_checksum += ob_crc64_sse42(buf, row_writer.size());
    row_writer.reset();
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
  }

  int64_t start_row_num[1] = {0};
  const int64_t range_start_num = 0;
  const int64_t range_end_num = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  check_sstable_file(fd, compressor, comp_name, start_row_num,
      schema, version_range, &range_start_num, &range_end_num,
      &range_start_flag, &range_end_flag, store_type,
      &table_id, table_count, block_size, row_checksum);

  //close and delete file
  close(fd);
}

/**
 * test append_row7
 * --DENSE_DENSE
 * --one row
 * --snappy
 */
TEST_F(TestCompactSSTableWriter, append_row7)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  ObCompactSSTableWriter writer;
  ObFrozenMinorVersionRange version_range;
  ObString comp_name;
  ObSSTableSchema schema;
  ObNewRange range;
  ObString file_path;
  ObRow row;
  const ObCompactStoreType store_type = DENSE_DENSE;
  uint64_t table_id = 1001;
  const int64_t table_count = 1;
  const int64_t block_size = 1024;
  const int64_t def_sstable_size = 0;
  const int64_t min_split_sstable_size = 0;
  const int64_t file_num = 1;
  bool is_split = false;

  ret = make_version_range(version_range, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_comp_name(comp_name, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_compressor(compressor, 4);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_sstable_param(version_range, store_type,
      table_count, block_size, comp_name, def_sstable_size,
      min_split_sstable_size);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_range(range, table_id, 0, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_schema(schema, table_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.set_table_info(table_id, schema, range);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = make_file_path(file_path, file_num);
  ASSERT_EQ(OB_SUCCESS, ret);

  delete_file(file_num);

  ret = writer.set_sstable_filepath(file_path);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObCompactCellWriter row_writer;
  char buf[100 * 1024];
  memset(buf, 0, sizeof(buf));
  uint64_t row_checksum = 0;
  for (int64_t i = 0; i < 1; i ++)
  {
    make_row(row, table_id, i, 0);

    ret = writer.append_row(row, is_split);
    ASSERT_EQ(OB_SUCCESS, ret);
  
    row_writer.init(buf, 100 * 1024, store_type);
    row_writer.append_row(row);
    row_checksum += ob_crc64_sse42(buf, row_writer.size());
    row_writer.reset();
  }

  ret = writer.finish();
  ASSERT_EQ(OB_SUCCESS, ret);

  //open file
  int fd = 0;
  if (-1 == (fd = open(file_path.ptr(), O_RDONLY)))
  {
    TBSYS_LOG(WARN, "open error");
    ret = OB_ERROR;
  }
  else
  {
    int64_t start_row_num = 0;
    const int64_t range_start_num = 0;
    const int64_t range_end_num = 0;
    const int range_start_flag = 0;
    const int range_end_flag = 0;
    check_sstable_file(fd, compressor, comp_name, &start_row_num, schema, version_range,
        &range_start_num, &range_end_num, &range_start_flag, &range_end_flag,
        store_type, &table_id, table_count, block_size, row_checksum);
  }

  //close and delete file
  close(fd);
}

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
