#include "test_compact_common.h"

using namespace oceanbase;
using namespace common;
using namespace compactsstablev2;
using namespace chunkserver;
using namespace sstable;

static FileInfoCache g_fic;
static ObSSTableBlockCache g_block_cache(g_fic);
static ObSSTableBlockIndexCache g_index_cache(g_fic);

class TestCompactSSTableScanner : public ::testing::Test
{
public:
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
    int ret = OB_SUCCESS;

    ret = g_fic.clear();
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = g_block_cache.clear();
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = g_index_cache.clear();
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  static void SetUpTestCase()
  {
    int ret = OB_SUCCESS;

    ret = g_fic.init(1024);
    ASSERT_EQ(OB_SUCCESS, ret);

    const int64_t cache_mem_size = 10 * 1024 * 1024;
    ret = g_block_cache.init(cache_mem_size);
    ASSERT_EQ(OB_SUCCESS, ret);

    const int64_t index_cache_mem_size = 10 * 1024 * 1024;
    ret = g_index_cache.init(index_cache_mem_size);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  static void TearDownTestCase()
  {
    int ret = OB_SUCCESS;

    ret = g_fic.destroy();
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = g_block_cache.destroy();
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = g_index_cache.destroy();
    ASSERT_EQ(OB_SUCCESS, ret);
  }
};


/**
 *test the construct of CompactSSTableScanner
 */
TEST_F(TestCompactSSTableScanner, construct)
{
  ObCompactSSTableScanner scanner;

  ASSERT_TRUE(NULL == scanner.scan_context_);
  ASSERT_TRUE(NULL == scanner.sstable_scan_param_);
  ASSERT_EQ(0, scanner.scan_column_indexes_.get_column_count());
  ASSERT_EQ(0, scanner.index_array_.block_count_);
  ASSERT_EQ(0 + ObCompactSSTableScanner::INVALID_CURSOR, scanner.index_array_cursor_);
  ASSERT_EQ(0 + ObCompactSSTableScanner::ITERATE_NOT_INITIALIZED, scanner.iterate_status_);
  //block_scanner_
  //row_value_
  //row_desc_
  //column_ids_
  //column_objs_
  ASSERT_EQ(0, scanner.row_column_cnt_);
  ASSERT_EQ(-1, scanner.max_scan_column_offset_);

  ASSERT_TRUE(NULL == scanner.uncomp_buf_);
  ASSERT_EQ(0 + ObCompactSSTableScanner::UNCOMP_BUF_SIZE, scanner.uncomp_buf_size_);
  ASSERT_TRUE(NULL == scanner.internal_buf_);
  ASSERT_EQ(0 + ObCompactSSTableScanner::INTERNAL_BUF_SIZE, scanner.internal_buf_size_);
  ASSERT_EQ(0 + ObCompactSSTableScanner::INVALID_SCAN_FLAG, scanner.scan_flag_);
}

/**
 *test the destruct of CompactSSTableScanner
 */
TEST_F(TestCompactSSTableScanner, destruct)
{
}

/**
 *test set scan param1
 *--success
 */
TEST_F(TestCompactSSTableScanner, set_scan_param1)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  const int64_t row_data[10] = {0,1,2,3,4,5,6,7,8,9};
  const int ext_flag[10] = {0,0,0,0,0,0,0,0,0};
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 10;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 2;
  column_array[1] = 3;
  column_array[2] = 5;
  column_array[3] = 20;
  const int64_t column_cnt = 4;
  const int64_t start_num = 2;
  const int64_t end_num = 4;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 0);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS ,ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_TRUE(NULL != scanner.scan_context_);
  ASSERT_TRUE(NULL != scanner.scan_context_->sstable_reader_);
  ASSERT_TRUE(NULL != scanner.scan_context_->block_cache_);
  ASSERT_TRUE(NULL != scanner.scan_context_->block_index_cache_);
  ASSERT_TRUE(NULL != scanner.sstable_scan_param_);
  ASSERT_EQ(4, scanner.scan_column_indexes_.get_column_count());
  const ObSSTableScanColumnIndexes::Column* scan_column_array = scanner.scan_column_indexes_.get_column_info();
  for (int64_t i = 0; i < scanner.scan_column_indexes_.get_column_count(); i++)
  {
    ASSERT_EQ(column_array[i], scan_column_array[i].id_);
  }

  ASSERT_EQ(1, scanner.index_array_.block_count_);
  ASSERT_EQ(1, scanner.index_array_cursor_);

  ASSERT_EQ(ObCompactSSTableScanner::ITERATE_LAST_BLOCK, scanner.iterate_status_);
  ASSERT_FALSE(scanner.end_of_data_);
  //block_scanner_
  //row_value_
  //row_desc_
  //column_ids_
  //column_objs_
  ASSERT_EQ(0, scanner.row_column_cnt_);
  ASSERT_EQ(4, scanner.max_scan_column_offset_);
  ASSERT_FALSE(NULL == scanner.uncomp_buf_);
  ASSERT_EQ(1024 * 1024, 1 * ObCompactSSTableScanner::UNCOMP_BUF_SIZE);
  ASSERT_FALSE(NULL == scanner.internal_buf_);
  ASSERT_EQ(1024 * 1024, 1 * ObCompactSSTableScanner::INTERNAL_BUF_SIZE); 
  ASSERT_EQ(0 + ObCompactSSTableScanner::DENSE_DENSE_NORMAL_ROW_SCAN, scanner.scan_flag_);
}

/**
 *test set scan param2
 *--fail
 *--scan_parm==NULL
 */
TEST_F(TestCompactSSTableScanner, set_scan_param2)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  const int64_t row_data[10] = {0,1,2,3,4,5,6,7,8,9};
  const int ext_flag[10] = {0,0,0,0,0,0,0,0,0};
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 10;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS ,ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(NULL, &scan_context);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 *test set scan param3
 *--fail
 *--scan_context==NULL
 */
TEST_F(TestCompactSSTableScanner, set_scan_param3)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  const int64_t row_data[10] = {0,1,2,3,4,5,6,7,8,9};
  const int ext_flag[10] = {0,0,0,0,0,0,0,0,0};
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 10;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 2;
  column_array[1] = 3;
  column_array[2] = 5;
  column_array[3] = 20;
  const int64_t column_cnt = 4;
  const int64_t start_num = 2;
  const int64_t end_num = 4;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 0);

  //scan context
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS ,ret);

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, NULL);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

/**
 *test set scan param4
 *--success
 *--table is not exist
 */
TEST_F(TestCompactSSTableScanner, set_scan_param4)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  const int64_t row_data[10] = {0,1,2,3,4,5,6,7,8,9};
  const int ext_flag[10] = {0,0,0,0,0,0,0,0,0};
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 10;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 2;
  column_array[1] = 3;
  column_array[2] = 5;
  column_array[3] = 20;
  const int64_t column_cnt = 4;
  const int64_t start_num = 2;
  const int64_t end_num = 4;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, 1003, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 0);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS ,ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value = NULL;
  ret = scanner.get_next_row(row_value);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test set scan param5
 *--success;
 *--empty table
 */
TEST_F(TestCompactSSTableScanner, set_scan_param5)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  const int64_t row_data[10] = {0,1,2,3,4,5,6,7,8,9};
  const int ext_flag[10] = {0,0,0,0,0,0,0,0,0};
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 0;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 2;
  column_array[1] = 3;
  column_array[2] = 5;
  column_array[3] = 20;
  const int64_t column_cnt = 4;
  const int64_t start_num = 2;
  const int64_t end_num = 4;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 0);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS ,ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value = NULL;
  ret = scanner.get_next_row(row_value);
  ASSERT_EQ(OB_ITER_END, ret);
}

//TODO: test get_row_desc

/**
 * DENSE_DENSE
 * --1.full row scan; daily merge scan; not reverse scan; sync; [5,25]
 * --2.full row scan; daily merge scan; is reverse scan; sync; [5,25]
 * --3.full row scan; daily merge scan; not reverse scan; sync; (5,25]
 * --4.full row scan; daily merge scan; is reverse scan; sync; (5,25]
 * --5.full row scan; daily merge scan; not reverse scan; sync; [5,25)
 * --6.full row scan; daily merge scan; is reverse scan; sync; [5,25)
 * --7.full row scan; daily merge scan; not reverse scan; sync; (5,25)
 * --8.full row scan; daily merge scan; is reverse scan; sync; (5,25)
 * --9.full row scan; daily merge scan; not reverse scan; sync; (min, max)
 * --10.full row scan; daily merge scan; is reverse scan; sync; (min, max)
 * --11.not full row scan; daily merge scan; not reverse scan; sync; [5,25]
 * --12.not full row scan; not daily merge scan; not reverse scan; sync; [5,25]; total rowkey column
 * --13.not full row scan; not daily merge scan; not reverse scan; sync; [5,25]; not total rowkey column
 * --14.full row scan; not daily merge scan; not reverse scan; sync; [5,25]
 * --15.reader==NULL
 * --16.not exit return nop
 * --17.not exit return null
 * --18.aync
 * --19.reader==NULL; full row scan;
 * --20.reader==NULL; daily merge scan
 * --21.reader==NULL; not full row scan; not daily merge scan
 *
 * DENSE_SPARSE
 * --31.full row scan; daily merge scan; not reverse scan; sync; [5,25]
 * --32.not full row scan; not daily merge scan; not reverse scan; sync; [5,25]; total_rowkey_column
 * --33.not full row scan; not daily merge scan; not reverse scan; sync; [5,25]; not total rowkey column
 * --34.multi table.
 * --35.table not exist; full row scan;
 * --36.table not exist; daily merge scan;
 * --37.table not exist; not full row scan; not daily merge scan;
 * --38.table row count = 0; full row scan;
 * --39.table row count = 0; daily merge scan;
 * --40.table row count = 0; not full row scan; not daily merge scan; total_rowkey_column
 * --41.table row count = 0; not full row scan; not daily merge scan; not total rowkey column
 */

/**
 *test get next row1
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--inclusive start, inclusive end
 *--not compress
 *--is not reverse scan
 *--sync
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row1)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row2
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--inclusive start, inclusive end
 *--not compress
 *--is reverse scan
 *--sync
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row2)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 1;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 25; i >= 5; i --)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get new row3
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--uninvlusive start, inclusive end
 *--not compress
 *--is not reverse scan
 *--sync
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row3)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 1;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 6; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get new row4
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--uninvlusive start, inclusive end
 *--not compress
 *--is reverse scan
 *--sync read
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row4)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 1;
  const int end_flag = 2;
  const int scan_flag = 1;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 25; i >= 6; i --)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get new row5
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--invlusive start, uninclusive end
 *--not compress
 *--is not reverse scan
 *--pre read
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row5)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 1;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 24; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get new row6
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--invlusive start, uninclusive end
 *--not compress
 *--is reverse scan
 *--pre read
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row6)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 1;
  const int scan_flag = 1;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 24; i >= 5; i --)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get new row7
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--uninlusive start, uninclusive end
 *--not compress
 *--is not reverse scan
 *--sync
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row7)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 1;
  const int end_flag = 1;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 6; i <= 24; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get new row8
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--uninlusive start, uninclusive end
 *--not compress
 *--is reverse scan
 *--sync
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row8)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 1;
  const int end_flag = 1;
  const int scan_flag = 1;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 24; i >= 6; i --)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row9
 *--success
 *--(rowkey:min-max)
 *--uninclusive start, uninclusive end
 *--not compress
 *--is not reverse scan
 *--sync read
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row9)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 0;
  const int end_flag = 0;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 0; i <= 99; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row10
 *--success
 *--(rowkey:min-max)
 *--uninclusive start, uninclusive end
 *--not compress
 *--is reverse scan
 *--sync read
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row10)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 0;
  const int end_flag = 0;
  const int scan_flag = 1;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 99; i >= 0; i --)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row11
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--inclusive start, inclusive end
 *--not compress
 *--is not reverse scan
 *--sync read
 *--not full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row11)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 5;
  column_array[4] = 6;
  column_array[5] = 7;
  column_array[6] = 8;
  const int64_t column_cnt = 7;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(7, row_value1->get_column_num());

    int64_t tmp = 0;
    for (int64_t j = 0; j < 9; j ++)
    {
      if (3 == j || 8 == j)
      {
        continue;
      }

      if (j >= 8)
      {
        tmp = j - 2;
      }
      else if (j >= 4)
      {
        tmp = j - 1;
      }
      else
      {
        tmp = j;
      }

      ret = row_value1->raw_get_cell(tmp, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row12
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--inclusive start, inclusive end
 *--not compress
 *--is not reverse scan
 *--sync read
 *--not full row scan
 *--not daily merge scan
 *--not total rowkey column
 */
TEST_F(TestCompactSSTableScanner, get_next_row12)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 5;
  column_array[4] = 6;
  column_array[5] = 7;
  column_array[6] = 8;
  const int64_t column_cnt = 7;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(7, row_value1->get_column_num());

    int64_t tmp = 0;
    for (int64_t j = 0; j < 9; j ++)
    {
      if (3 == j || 8 == j)
      {
        continue;
      }

      if (j >= 8)
      {
        tmp = j - 2;
      }
      else if (j >= 4)
      {
        tmp = j - 1;
      }
      else
      {
        tmp = j;
      }

      ret = row_value1->raw_get_cell(tmp, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row13
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--inclusive start, inclusive end
 *--not compress
 *--is not reverse scan
 *--sync read
 *--not full row scan
 *--not daily merge scan
 *--not total rowkey column
 */
TEST_F(TestCompactSSTableScanner, get_next_row13)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 3;
  column_array[2] = 5;
  column_array[3] = 6;
  column_array[4] = 7;
  column_array[5] = 8;
  const int64_t column_cnt = 6;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 0);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(6, row_value1->get_column_num());

    int64_t tmp = 0;
    for (int64_t j = 0; j < 9; j ++)
    {
      if (1 == j || 3 == j || 8 == j)
      {
        continue;
      }

      if (j >= 8)
      {
        tmp = j - 3;
      }
      else if (j >= 4)
      {
        tmp = j - 2;
      }
      else if (j >= 1)
      {
        tmp = j - 1;
      }
      else
      {
        tmp = j;
      }

      ret = row_value1->raw_get_cell(tmp, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row14
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--inclusive start, inclusive end
 *--not compress
 *--is not reverse scan
 *--sync
 *--full row scan
 *--not daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row14)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row15
 *--sstable reader==NULL
 */
TEST_F(TestCompactSSTableScanner, get_next_row15)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = NULL;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row16
 *--not exit return nop
 */
TEST_F(TestCompactSSTableScanner, get_next_row16)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  column_array[9] = 10;
  const int64_t column_cnt = 10;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num() + 1, row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }

    ObObj temp_obj;
    temp_obj.set_ext(ObActionFlag::OP_NOP);
    ret = row_value1->raw_get_cell(9, cell1, table_id1, column_id1);
    ASSERT_EQ(OB_SUCCESS, ret);
    int64_t temp_ext1;
    int64_t temp_ext2;
    ret = cell1->get_ext(temp_ext1);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = temp_obj.get_ext(temp_ext2);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(temp_ext1, temp_ext2);
    ASSERT_TRUE(1001 == table_id1);
    ASSERT_TRUE(10 == column_id1);
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row17
 *--not exit return null
 */
TEST_F(TestCompactSSTableScanner, get_next_row17)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  column_array[9] = 10;
  const int64_t column_cnt = 10;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num() + 1, row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }

    ObObj temp_obj;
    temp_obj.set_null();
    ret = row_value1->raw_get_cell(9, cell1, table_id1, column_id1);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(temp_obj == *cell1);
    ASSERT_TRUE(1001 == table_id1);
    ASSERT_TRUE(10 == column_id1);
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row18
 *--success
 *--(rowkey:5-25)
 *--DENSE_DENSE
 *--inclusive start, inclusive end
 *--not compress
 *--is not reverse scan
 *--aync
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row18)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 1;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 4);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 9; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      ASSERT_TRUE(*cell1 == *cell2);
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row19
 *reader==NULL, full row scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row19)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = NULL;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);

  const ObRowDesc* temp_desc = NULL;
  ret = scanner.get_row_desc(temp_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2, temp_desc->get_rowkey_cell_count());
}

/**
 *test get next row20
 *reader==NULL, daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row20)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = NULL;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);

  const ObRowDesc* temp_desc = NULL;
  ret = scanner.get_row_desc(temp_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2, temp_desc->get_rowkey_cell_count());
}

/**
 *test get next row21
 *reader==NULL, not daily merge scan, not full row scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row21)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_DENSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = false;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 0);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = NULL;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);

  const ObRowDesc* temp_desc = NULL;
  ret = scanner.get_row_desc(temp_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, temp_desc->get_rowkey_cell_count());
}

/**
 *test get next row31
 *--success
 *--(rowkey:5-25)
 *--DENSE_SPARSE
 *--inclusive start, inclusive end
 *--not compress
 *--is not reverse scan
 *--sync
 *--full row scan
 *--daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row31)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 5);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 10; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      if (9 != j)
      {
        ASSERT_TRUE(*cell1 == *cell2);
      }
      else
      {
        int64_t temp_ext1;
        int64_t temp_ext2;
        ret = cell1->get_ext(temp_ext1);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cell2->get_ext(temp_ext2);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_EQ(temp_ext1, temp_ext2);
      }
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row32
 *--success
 *--(rowkey:5-25)
 *--DENSE_SPARSE
 *--inclusive start, inclusive end
 *--not compress
 *--is not reverse scan
 *--sync
 *--not full row scan
 *--not daily merge scan
 *--total rowkey column
 */
TEST_F(TestCompactSSTableScanner, get_next_row32)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 5;
  column_array[4] = 6;
  column_array[5] = 7;
  column_array[6] = 8;
  const int64_t column_cnt = 7;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id, i, 5);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(8, row_value1->get_column_num());

    int64_t tmp = 0;
    for (int64_t j = 0; j < 10; j ++)
    {
      if (3 == j || 8 == j)
      {
        continue;
      }

      if (j >= 8)
      {
        tmp = j - 2;
      }
      else if (j >= 4)
      {
        tmp = j - 1;
      }
      else
      {
        tmp = j;
      }

      ret = row_value1->raw_get_cell(tmp, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);
      if (9 != j)
      {
        ASSERT_TRUE(*cell1 == *cell2);
      }
      else
      {
        int64_t temp_ext1;
        int64_t temp_ext2;
        ret = cell1->get_ext(temp_ext1);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cell2->get_ext(temp_ext2);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_EQ(temp_ext1, temp_ext2);
      }
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row33
 *--success
 *--(rowkey:5-25)
 *--DENSE_SPARSE
 *--inclusive start, inclusive end
 *--not compress
 *--is not reverse scan
 *--sync read
 *--not full row scan
 *--not daily merge scan
 *--not total rowkey column
 */
TEST_F(TestCompactSSTableScanner, get_next_row33)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 1;
  const uint64_t table_id = 1001;
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[100];
  int ext_flag[100];
  for (int64_t i = 0; i < 100; i ++)
  {
    row_data[i] = i;
    ext_flag[i] = 0;
  }
  const int64_t range_start = 0;
  const int64_t range_end = 0;
  const int range_start_flag = 0;
  const int range_end_flag = 0;
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count = 100;

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, &table_id, table_count,
      &range_start, &range_end, &range_start_flag, 
      &range_end_flag, row_data, ext_flag, &row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 3;
  column_array[2] = 5;
  column_array[3] = 6;
  column_array[4] = 7;
  column_array[5] = 8;
  const int64_t column_cnt = 6;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, table_id, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 0);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);

    ret = make_row(row_value2, table_id, i, 5);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(7, row_value1->get_column_num());

    int64_t tmp = 0;
    for (int64_t j = 0; j < 10; j ++)
    {
      if (1 == j || 3 == j || 8 == j)
      {
        continue;
      }

      if (j >= 8)
      {
        tmp = j - 3;
      }
      else if (j >= 4)
      {
        tmp = j - 2;
      }
      else if (j >= 1)
      {
        tmp = j - 1;
      }
      else
      {
        tmp = j;
      }

      ret = row_value1->raw_get_cell(tmp, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);

      if (9 != j)
      {
        ASSERT_TRUE(*cell1 == *cell2);
      }
      else
      {
        int64_t temp_ext1;
        int64_t temp_ext2;
        ret = cell1->get_ext(temp_ext1);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cell2->get_ext(temp_ext2);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_EQ(temp_ext1, temp_ext2);
      }
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row34
 *--success
 *--multi table
 */
TEST_F(TestCompactSSTableScanner, get_next_row34)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 3;
  const uint64_t table_id[3] = {1001, 1002, 1003};
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[300];
  int ext_flag[300];
  for (int64_t i = 0; i < 300; i ++)
  {
    row_data[i] = i%100;
    ext_flag[i] = 0;
  }
  const int64_t range_start[3] = {0, 0, 0};
  const int64_t range_end[3] = {0, 0, 0};
  const int range_start_flag[3] = {0, 0, 0};
  const int range_end_flag[3] = {0, 0, 0};
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count[3] = {100, 100, 100};

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, table_id, table_count,
      range_start, range_end, range_start_flag, 
      range_end_flag, row_data, ext_flag, row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, table_id[1], start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRowkey row_key2;
  const ObRowkey* row_key3;
  const ObRow* row_value1;
  ObRow row_value2;

  const ObObj* cell1 = NULL;
  const ObObj* cell2 = NULL;
  uint64_t table_id1 = 0;
  uint64_t table_id2 = 0;
  uint64_t column_id1 = 0;
  uint64_t column_id2 = 0;

  for (int64_t i = 5; i <= 25; i ++)
  {
    ret = scanner.get_next_row(row_value1);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_rowkey(row_key2, i, 0);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = row_value1->get_rowkey(row_key3);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(*row_key3 == row_key2);

    ret = make_row(row_value2, table_id[1], i, 5);
    ASSERT_EQ(OB_SUCCESS, ret);

    ASSERT_EQ(row_value2.get_column_num(), row_value1->get_column_num());

    for (int64_t j = 0; j < 10; j ++)
    {
      ret = row_value1->raw_get_cell(j, cell1, table_id1, column_id1);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = row_value2.raw_get_cell(j, cell2, table_id2, column_id2);
      ASSERT_EQ(OB_SUCCESS, ret);

      ASSERT_EQ(table_id1, table_id2);
      ASSERT_EQ(column_id1, column_id2);

      if (9 != j)
      {
        ASSERT_TRUE(*cell1 == *cell2);
      }
      else
      {
        int64_t tmp_ext1;
        int64_t tmp_ext2;
        ret = cell1->get_ext(tmp_ext1);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = cell2->get_ext(tmp_ext2);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_EQ(tmp_ext1, tmp_ext2);
      }
    }
  }

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);
}

/**
 *test get next row35
 *--success
 *--table not exist; full row scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row35)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 3;
  const uint64_t table_id[3] = {1001, 1002, 1003};
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[300];
  int ext_flag[300];
  for (int64_t i = 0; i < 300; i ++)
  {
    row_data[i] = i%100;
    ext_flag[i] = 0;
  }
  const int64_t range_start[3] = {0, 0, 0};
  const int64_t range_end[3] = {0, 0, 0};
  const int range_start_flag[3] = {0, 0, 0};
  const int range_end_flag[3] = {0, 0, 0};
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count[3] = {100, 100, 100};

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, table_id, table_count,
      range_start, range_end, range_start_flag, 
      range_end_flag, row_data, ext_flag, row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, 1004, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);

  const ObRowDesc* temp_desc = NULL;
  ret = scanner.get_row_desc(temp_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2, temp_desc->get_rowkey_cell_count());
}

/**
 *test get next row36
 *--success
 *--table not exist; daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row36)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 3;
  const uint64_t table_id[3] = {1001, 1002, 1003};
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[300];
  int ext_flag[300];
  for (int64_t i = 0; i < 300; i ++)
  {
    row_data[i] = i%100;
    ext_flag[i] = 0;
  }
  const int64_t range_start[3] = {0, 0, 0};
  const int64_t range_end[3] = {0, 0, 0};
  const int range_start_flag[3] = {0, 0, 0};
  const int range_end_flag[3] = {0, 0, 0};
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count[3] = {100, 100, 100};

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, table_id, table_count,
      range_start, range_end, range_start_flag, 
      range_end_flag, row_data, ext_flag, row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, 1004, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);

  const ObRowDesc* temp_desc = NULL;
  ret = scanner.get_row_desc(temp_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2, temp_desc->get_rowkey_cell_count());
}

/**
 *test get next row37
 *--success
 *--table not exist; not daily merge scan; not full row scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row37)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 3;
  const uint64_t table_id[3] = {1001, 1002, 1003};
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[300];
  int ext_flag[300];
  for (int64_t i = 0; i < 300; i ++)
  {
    row_data[i] = i%100;
    ext_flag[i] = 0;
  }
  const int64_t range_start[3] = {0, 0, 0};
  const int64_t range_end[3] = {0, 0, 0};
  const int range_start_flag[3] = {0, 0, 0};
  const int range_end_flag[3] = {0, 0, 0};
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count[3] = {100, 100, 100};

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, table_id, table_count,
      range_start, range_end, range_start_flag, 
      range_end_flag, row_data, ext_flag, row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, 1004, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);

  const ObRowDesc* temp_desc = NULL;
  ret = scanner.get_row_desc(temp_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, temp_desc->get_rowkey_cell_count());
}

/**
 *test get next row38
 *--success
 *--table not exist; full row scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row38)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 3;
  const uint64_t table_id[3] = {1001, 1002, 1003};
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[300];
  int ext_flag[300];
  for (int64_t i = 0; i < 300; i ++)
  {
    row_data[i] = i%100;
    ext_flag[i] = 0;
  }
  const int64_t range_start[3] = {0, 0, 0};
  const int64_t range_end[3] = {0, 0, 0};
  const int range_start_flag[3] = {0, 0, 0};
  const int range_end_flag[3] = {0, 0, 0};
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count[3] = {100, 100, 0};

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, table_id, table_count,
      range_start, range_end, range_start_flag, 
      range_end_flag, row_data, ext_flag, row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = true;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, 1003, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);

  const ObRowDesc* temp_desc = NULL;
  ret = scanner.get_row_desc(temp_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2, temp_desc->get_rowkey_cell_count());
}

/**
 *test get next row39
 *--success
 *--table not exist; daily merge scan
 */
TEST_F(TestCompactSSTableScanner, get_next_row39)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 3;
  const uint64_t table_id[3] = {1001, 1002, 1003};
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[300];
  int ext_flag[300];
  for (int64_t i = 0; i < 300; i ++)
  {
    row_data[i] = i%100;
    ext_flag[i] = 0;
  }
  const int64_t range_start[3] = {0, 0, 0};
  const int64_t range_end[3] = {0, 0, 0};
  const int range_start_flag[3] = {0, 0, 0};
  const int range_end_flag[3] = {0, 0, 0};
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count[3] = {100, 100, 0};

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, table_id, table_count,
      range_start, range_end, range_start_flag, 
      range_end_flag, row_data, ext_flag, row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = true;
  make_scan_param(sstable_scan_param, 1003, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 2);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);

  const ObRowDesc* temp_desc = NULL;
  ret = scanner.get_row_desc(temp_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2, temp_desc->get_rowkey_cell_count());
}

/**
 *test get next row40
 *--success
 *--table not exist; not daily merge scan; not full row scan; total rowkey column
 */
TEST_F(TestCompactSSTableScanner, get_next_row40)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 3;
  const uint64_t table_id[3] = {1001, 1002, 1003};
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[300];
  int ext_flag[300];
  for (int64_t i = 0; i < 300; i ++)
  {
    row_data[i] = i%100;
    ext_flag[i] = 0;
  }
  const int64_t range_start[3] = {0, 0, 0};
  const int64_t range_end[3] = {0, 0, 0};
  const int range_start_flag[3] = {0, 0, 0};
  const int range_end_flag[3] = {0, 0, 0};
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count[3] = {100, 100, 0};

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, table_id, table_count,
      range_start, range_end, range_start_flag, 
      range_end_flag, row_data, ext_flag, row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 2;
  column_array[2] = 3;
  column_array[3] = 4;
  column_array[4] = 5;
  column_array[5] = 6;
  column_array[6] = 7;
  column_array[7] = 8;
  column_array[8] = 9;
  const int64_t column_cnt = 9;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, 1003, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 0);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);

  const ObRowDesc* temp_desc = NULL;
  ret = scanner.get_row_desc(temp_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(2, temp_desc->get_rowkey_cell_count());
}

/**
 *test get next row41
 *--success
 *--table not exist; not daily merge scan; not full row scan; not total rowkey column
 */
TEST_F(TestCompactSSTableScanner, get_next_row41)
{
  int ret = OB_SUCCESS;
  const int64_t table_count = 3;
  const uint64_t table_id[3] = {1001, 1002, 1003};
  const uint64_t file_id = 1;
  const int comp_flag = 2;
  int64_t row_data[300];
  int ext_flag[300];
  for (int64_t i = 0; i < 300; i ++)
  {
    row_data[i] = i%100;
    ext_flag[i] = 0;
  }
  const int64_t range_start[3] = {0, 0, 0};
  const int64_t range_end[3] = {0, 0, 0};
  const int range_start_flag[3] = {0, 0, 0};
  const int range_end_flag[3] = {0, 0, 0};
  const ObCompactStoreType store_type = DENSE_SPARSE;
  const int64_t row_count[3] = {100, 100, 0};

  //删除文件
  delete_file(file_id);

  //创建文件
  create_file(file_id, comp_flag, store_type, table_id, table_count,
      range_start, range_end, range_start_flag, 
      range_end_flag, row_data, ext_flag, row_count);

  //scan param
  ObSSTableScanParam sstable_scan_param;
  uint64_t column_array[1024];
  column_array[0] = 1;
  column_array[1] = 3;
  column_array[2] = 4;
  column_array[3] = 5;
  column_array[4] = 6;
  column_array[5] = 7;
  column_array[6] = 8;
  column_array[7] = 9;
  const int64_t column_cnt = 8;
  const int64_t start_num = 5;
  const int64_t end_num = 25;
  const int start_flag = 2;
  const int end_flag = 2;
  const int scan_flag = 0;
  const int read_flag = 0;
  const bool not_exit_col_ret_nop = true;
  const bool full_row_scan_flag = false;
  const bool daily_merge_scan_flag = false;
  make_scan_param(sstable_scan_param, 1003, start_num, end_num,
      start_flag, end_flag, scan_flag, read_flag,
      column_array, column_cnt, not_exit_col_ret_nop,
      full_row_scan_flag, daily_merge_scan_flag, 0);

  //scan context
  ObCompactSSTableScanner::ScanContext scan_context;
  ObCompactSSTableReader reader(g_fic);
  ret = reader.init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  scan_context.sstable_reader_ = &reader;
  scan_context.block_cache_ = &g_block_cache;
  scan_context.block_index_cache_ = &g_index_cache;

  //scanner
  ObCompactSSTableScanner scanner;
  ret = scanner.set_scan_param(&sstable_scan_param, &scan_context);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObRow* row_value1;

  ret = scanner.get_next_row(row_value1);
  ASSERT_EQ(OB_ITER_END, ret);

  const ObRowDesc* temp_desc = NULL;
  ret = scanner.get_row_desc(temp_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(0, temp_desc->get_rowkey_cell_count());
}

TEST_F(TestCompactSSTableScanner, test_compressor)
{
  ObCompressor* compressor;
  if (NULL == (compressor = create_compressor("lz1_1.0")))
  {
    TBSYS_LOG(WARN, "create compressor error");
  }
  else
  {
    TBSYS_LOG(WARN, "destroy compressor");
  }
}

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
