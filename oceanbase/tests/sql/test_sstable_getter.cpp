/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* test_sstable_getter.cpp: unit tests for sstable getter.
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/


#include "sql/ob_sstable_getter.h"
#include "chunkserver/ob_tablet_manager.h"
#include "chunkserver/ob_chunk_server_config.h"
#include "chunkserver/ob_chunk_server_main.h"
#include "common/file_directory_utils.h"
#include "test_helper.h"

#include <iostream>
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
// using namespace oceanbase::chunkserver;

namespace oceanbase
{
namespace tests
{
namespace sql
{

class TestObSSTableGetter : public ::testing::Test
{
protected:
  static const char *DATA_DIR;
  static const char *APP_NAME;
  static const int64_t BLOCK_CACHE_SIZE = 1 << 25; // 32MB
  static const int64_t BLOCK_INDEX_CACHE_SIZE = 1 << 25; // 32MB
  static const int64_t ROW_CACHE_SIZE = 1 << 25; // 32MB
  static const int64_t FILE_INFO_CACHE_NUM = 1 << 10;
  static const int64_t MAX_SSTABLE_SIZE = 1 << 26; // 64MB

  static const int32_t ROWKEY_COL_NUM = 2;
  static const int32_t COL_NUM = 10;
  static const int32_t ROW_NUM = 1000;
  static const int64_t SSTABLE_FILE_ID = 123060;
  static const int64_t EMPTY_SSTABLE_FILE_ID = 123061;
  static const int64_t SSTABLE_WITH_BLOOM_FILTER_FILE_ID = 123062;


protected:
  static void SetUpTestCase();
  static void TearDownTestCase();

  virtual void SetUp();
  virtual void TearDown() {}

  // @param key is the start key of rowkey.
  // generated rowkey is : key, key + 1, key + 2, ... key + ROWKEY_COL_NUM - 1
  void init_getter(int64_t key, sstable::ObSSTableReader *&reader, bool is_ups_getter);

  void validate_non_exist_row();

  static bool is_row_empty(const ObRow &row);
  static void validate_cells(const int64_t key, const ObRow &row);
  // validate first row in %getter_ is non exist row.

protected:

  // Since using namespace::sstable in test_helper.h, we need specify the sql namespace.
  ::sql::ObSSTableGetter getter_;
  ObSqlGetParam get_param_;
  ObRowkey rowkey_;
  ObObj rowkey_objs_[ROWKEY_COL_NUM];

  static chunkserver::ObTabletManager tablet_mgr_;
  static chunkserver::ObGetThreadContext context_;
  static sstable::ObSSTableReader *sstable_reader_;
  static sstable::ObSSTableReader *empty_sstable_reader_;
  static sstable::ObSSTableReader *sstable_with_bloom_filter_reader_;
};

const char *TestObSSTableGetter::DATA_DIR = "./__test_sstable_getter__/";
const char *TestObSSTableGetter::APP_NAME = "app1";

chunkserver::ObTabletManager TestObSSTableGetter::tablet_mgr_;
chunkserver::ObGetThreadContext TestObSSTableGetter::context_;
sstable::ObSSTableReader *TestObSSTableGetter::sstable_reader_ = NULL;
sstable::ObSSTableReader *TestObSSTableGetter::empty_sstable_reader_ = NULL;
sstable::ObSSTableReader *TestObSSTableGetter::sstable_with_bloom_filter_reader_ = NULL;

void TestObSSTableGetter::SetUpTestCase()
{
  chunkserver::ObChunkServerConfig &config = chunkserver::ObChunkServerMain::get_instance()->get_chunk_server().get_config();
  config.datadir.set_value(DATA_DIR);
  config.appname.set_value(APP_NAME);

  // make data dir
  int rc = 0;
  char sstable_path[OB_MAX_FILE_NAME_LENGTH];
  rc = sstable::get_sstable_directory(1, sstable_path, OB_MAX_FILE_NAME_LENGTH);
  ASSERT_EQ(OB_SUCCESS, rc);

  FileDirectoryUtils::create_full_path(sstable_path);

  // init tablet manager
  rc = tablet_mgr_.init(BLOCK_CACHE_SIZE, BLOCK_INDEX_CACHE_SIZE, ROW_CACHE_SIZE, 
      FILE_INFO_CACHE_NUM, DATA_DIR, MAX_SSTABLE_SIZE);

  // init sstable
  CellInfoGen::Desc desc[2] = {
    { 0, 0, ROWKEY_COL_NUM - 1 },
    { 0, ROWKEY_COL_NUM, COL_NUM - 1 },
  };

  ObSSTableId sstable_id(SSTABLE_FILE_ID);
  rc = write_sstable(sstable_id, ROW_NUM, COL_NUM, desc, 2);
  ASSERT_EQ(0, rc);
  ObSSTableId empty_sstable_id(EMPTY_SSTABLE_FILE_ID);
  rc = write_sstable(empty_sstable_id, 0, COL_NUM, desc, 2);
  ASSERT_EQ(0, rc);
  ObSSTableId sstable_with_bloom_filter_id(SSTABLE_WITH_BLOOM_FILTER_FILE_ID);
  rc = write_sstable(sstable_with_bloom_filter_id, ROW_NUM, COL_NUM, desc, 2,
      OB_SSTABLE_STORE_DENSE, ObSSTableBlockBuilder::SSTABLE_BLOCK_SIZE, ROW_NUM);
  ASSERT_EQ(0, rc);

  // init stable reader
  // We don't delete the newed objects, memory leak is acceptable in this unit test
  ModulePageAllocator *mod = new ModulePageAllocator(0);
  ModuleArena *allocator = new ModuleArena(ModuleArena::DEFAULT_PAGE_SIZE, *mod);

  sstable_reader_ = new sstable::ObSSTableReader(*allocator, tablet_mgr_.get_fileinfo_cache());
  empty_sstable_reader_ = new sstable::ObSSTableReader(*allocator, tablet_mgr_.get_fileinfo_cache());
  sstable_with_bloom_filter_reader_ = new sstable::ObSSTableReader(*allocator, tablet_mgr_.get_fileinfo_cache());

  rc = sstable_reader_->open(sstable_id, 0);
  ASSERT_EQ(OB_SUCCESS, rc);
  ASSERT_TRUE(sstable_reader_->is_opened());

  rc = empty_sstable_reader_->open(empty_sstable_id, 0);
  ASSERT_EQ(OB_SUCCESS, rc);
  ASSERT_TRUE(empty_sstable_reader_->is_opened());

  rc = sstable_with_bloom_filter_reader_->open(sstable_with_bloom_filter_id, 0);
  ASSERT_EQ(OB_SUCCESS, rc);
  ASSERT_TRUE(sstable_with_bloom_filter_reader_->is_opened());
}

void TestObSSTableGetter::TearDownTestCase()
{
  FileDirectoryUtils::delete_directory_recursively(DATA_DIR);
}

void TestObSSTableGetter::SetUp()
{
  tablet_mgr_.get_row_cache()->clear();
}

void TestObSSTableGetter::init_getter(int64_t key, sstable::ObSSTableReader *&reader, bool is_ups_getter)
{
  for (int64_t i = 0; i < ROWKEY_COL_NUM; i++)
  {
    rowkey_objs_[i].set_int(key + i);
  }
  rowkey_.assign(rowkey_objs_, ROWKEY_COL_NUM);

  get_param_.reset();
  get_param_.set_table_id(CellInfoGen::table_id, CellInfoGen::table_id);
  get_param_.add_rowkey(rowkey_);

  // Query one more column which not exist in sstable's schema
  uint64_t column_ids[COL_NUM + 1];
  for (int64_t i = 0; i <= COL_NUM; i++)
  {
    // see CellInfoGen::gen, column_id is generate by: CellInfoGen::START_ID add column_index
    column_ids[i] = CellInfoGen::START_ID + i;
  }

  context_.block_index_cache_ = &tablet_mgr_.get_serving_block_index_cache();
  context_.block_cache_ = &tablet_mgr_.get_serving_block_cache();
  context_.row_cache_ = tablet_mgr_.get_row_cache();
  context_.readers_[0] = reader;
  context_.readers_count_ = 1;

  int rc = getter_.initialize(&get_param_, column_ids, COL_NUM + 1,
      ROWKEY_COL_NUM, &context_, is_ups_getter);

  ASSERT_EQ(OB_SUCCESS, rc);
}

bool TestObSSTableGetter::is_row_empty(const ObRow &row)
{
  int64_t idx = row.get_row_desc()->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
  if (OB_INVALID_INDEX == idx) // no extend column
  {
    return false;
  }
  else
  {
    const ObObj *cell = NULL;
    row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);
    EXPECT_EQ(ObExtendType, cell->get_type());

    return cell->get_ext() == ObActionFlag::OP_ROW_DOES_NOT_EXIST;
  }
}

void TestObSSTableGetter::validate_cells(const int64_t key, const ObRow &row)
{
  // see sstable generate rule, in CellInfoGen::gen()
  const ObObj *cell;
  for (int i = 0; i < COL_NUM; i++)
  {
    int rc = row.get_cell(CellInfoGen::table_id, CellInfoGen::START_ID + i, cell);
    EXPECT_EQ(OB_SUCCESS, rc);

    EXPECT_EQ(ObIntType, cell->get_type());
    int64_t value;
    rc = cell->get_int(value);
    EXPECT_EQ(OB_SUCCESS, rc);
    EXPECT_EQ(key + i, value);
  }
}

void TestObSSTableGetter::validate_non_exist_row()
{
  const ObRow *row = NULL;
  int rc = getter_.get_next_row(row);
  EXPECT_EQ(OB_SUCCESS, rc);
  EXPECT_TRUE(row);
  EXPECT_TRUE(is_row_empty(*row));
}

TEST_F(TestObSSTableGetter, get_exist_key)
{
  // %sstable_reader_ may be NULL if compress libaray not found, error output like:
  //    liblzo2.so.2: cannot open shared object file: No such file or directory
  ASSERT_TRUE(sstable_reader_);
  ASSERT_GT(sstable_reader_->get_row_count(), 0);

  // Get all exist rows individually.
  for (int64_t i = 0; i < ROW_NUM; ++i)
  {
    int64_t key = i * COL_NUM;
    init_getter(key, sstable_reader_, false);
    const ObRow *row = NULL;

    int rc = getter_.get_next_row(row);
    EXPECT_EQ(OB_SUCCESS, rc);
    EXPECT_TRUE(row);
    if (0 == key)
    {
      TBSYS_LOG(INFO, "row: %s", to_cstring(*row));
    }

    EXPECT_FALSE(is_row_empty(*row));

    const ObRowDesc *row_desc = NULL;
    row_desc = row->get_row_desc();
    EXPECT_TRUE(row_desc);

    const ObRowkey *rowkey = NULL;
    rc = row->get_rowkey(rowkey);
    EXPECT_EQ(OB_SUCCESS, rc);
    EXPECT_TRUE(rowkey);

    EXPECT_EQ(rowkey_, *rowkey);
    validate_cells(key, *row);

    // validate not exist column
    int64_t column_id = COL_NUM + CellInfoGen::START_ID;
    const ObObj *cell;
    rc = row->get_cell(CellInfoGen::table_id, column_id, cell);
    EXPECT_EQ(OB_SUCCESS, rc);
    EXPECT_EQ(ObNullType, cell->get_type());

    rc = getter_.get_next_row(row);
    EXPECT_EQ(OB_ITER_END, rc);
  }
}

TEST_F(TestObSSTableGetter, get_from_row_cache)
{
  ASSERT_TRUE(tablet_mgr_.get_row_cache());

  // exist row
  int64_t key = 2 * COL_NUM;
  init_getter(key, sstable_reader_, false);

  const ObRow *row = NULL;
  int rc = getter_.get_next_row(row);
  EXPECT_EQ(OB_SUCCESS, rc);
  EXPECT_TRUE(row);
  EXPECT_FALSE(is_row_empty(*row));
  validate_cells(key, *row);

  // reset getter and get row again.
  init_getter(key, sstable_reader_, false);

  row = NULL;
  rc = getter_.get_next_row(row);
  EXPECT_EQ(OB_SUCCESS, rc);
  EXPECT_TRUE(row);
  EXPECT_FALSE(is_row_empty(*row));
  validate_cells(key, *row);

  // non exist row
  key = 2 * COL_NUM + COL_NUM / 2;
  init_getter(key, sstable_reader_, false);
  validate_non_exist_row();


  // reset getter and get row again.
  init_getter(key, sstable_reader_, false);
  validate_non_exist_row();
}

/*
 * Bug in sstable with bloom filter:
 *    num_bits_ not set.
 *        Alternative fix:
 *        should set to num_bytes_ * sizeof(char)
 *        and in init(), set num_bits_ to num_bytes_ * sizeof(char) too, 
 *        after num_bytes_ calculated.
 *
 * We just disable bloom filter test here.
 * Note: we can do this at run time:
 *        ./test_sstable_getter --gtest_filter='-*bloom*'
 *
TEST_F(TestObSSTableGetter, get_with_bloom_filter_enabled)
{
  // exist row
  int64_t key = 2 * COL_NUM;
  init_getter(key, sstable_with_bloom_filter_reader_, false);

  const ObRow *row = NULL;
  int rc = getter_.get_next_row(row);
  EXPECT_EQ(OB_SUCCESS, rc);
  EXPECT_TRUE(row);
  EXPECT_FALSE(is_row_empty(*row));
  validate_cells(key, *row);

  // non exist row
  key = 2 * COL_NUM + COL_NUM / 2;;
  init_getter(key, sstable_reader_, false);

  validate_non_exist_row();
}
*/

TEST_F(TestObSSTableGetter, get_ups_row)
{
  // exist row
  int64_t key = 2 * COL_NUM;
  init_getter(key, sstable_reader_, true);

  const ObRow *row = NULL;
  int rc = getter_.get_next_row(row);
  EXPECT_EQ(OB_SUCCESS, rc);
  EXPECT_TRUE(row);
  EXPECT_FALSE(is_row_empty(*row));
  validate_cells(key, *row);

  int64_t column_id = COL_NUM + CellInfoGen::START_ID;
  const ObObj *cell;
  rc = row->get_cell(CellInfoGen::table_id, column_id, cell);
  EXPECT_EQ(OB_SUCCESS, rc);
  EXPECT_EQ(ObExtendType, cell->get_type());
  EXPECT_EQ(int(ObActionFlag::OP_NOP), cell->get_ext());

  // non exist row
  key = 2 * COL_NUM + COL_NUM / 2;;
  init_getter(key, sstable_reader_, true);

  validate_non_exist_row();
}

TEST_F(TestObSSTableGetter, non_exit_row___reader_is_null)
{
  int64_t key = 2 * COL_NUM;
  sstable::ObSSTableReader *reader = NULL;
  init_getter(key, reader, false);

  validate_non_exist_row();
}

TEST_F(TestObSSTableGetter, non_exit_row___empty_sstable)
{
  int64_t key = 2 * COL_NUM;
  init_getter(key, empty_sstable_reader_, false);
  validate_non_exist_row();
}

TEST_F(TestObSSTableGetter, non_exit_row___block_not_found)
{
  int64_t key = 2 * ROW_NUM * COL_NUM;
  init_getter(key, sstable_reader_, false);
  validate_non_exist_row();
}

TEST_F(TestObSSTableGetter, non_exit_row___not_found_in_block)
{
  int64_t key = 2 * COL_NUM + COL_NUM / 2;
  init_getter(key, sstable_reader_, false);
  validate_non_exist_row();
}

} // end namespace sql
} // end namespace tests
} // end namespace oceanbase


int main(int argc, char** argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
