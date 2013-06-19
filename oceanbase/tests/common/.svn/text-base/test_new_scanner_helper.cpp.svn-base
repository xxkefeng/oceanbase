/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * test_new_scanner_helper.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */


#include "common/ob_new_scanner_helper.h"
#include "common/ob_malloc.h"
#include "common/ob_string_buf.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

#define TABLE_ID 1000LU
#define COLUMN_NUM 8
#define ROW_NUM 10


class ObNewScannerHelperTest: public ::testing::Test
{
  public:
    ObNewScannerHelperTest();
    virtual ~ObNewScannerHelperTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObNewScannerHelperTest(const ObNewScannerHelperTest &other);
    ObNewScannerHelperTest& operator=(const ObNewScannerHelperTest &other);
};

ObNewScannerHelperTest::ObNewScannerHelperTest()
{
}

ObNewScannerHelperTest::~ObNewScannerHelperTest()
{
}

void ObNewScannerHelperTest::SetUp()
{
}

void ObNewScannerHelperTest::TearDown()
{
}

//test ObNewScannerHelper.add_cell
TEST_F(ObNewScannerHelperTest, add_cell_test)
{
  ObRow row;
  ObRowDesc row_desc;
  ObObj *cell = NULL;
  ObCellInfo cell_info;

  cell_info.table_id_ = TABLE_ID;

  row.get_cell(TABLE_ID, 88, cell);

  for (int i = 0; i < COLUMN_NUM; i ++)
  {
    row_desc.add_column_desc(TABLE_ID, i + OB_APP_MIN_COLUMN_ID);
  }
  row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
  row.set_row_desc(row_desc);

  row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);
  cell->set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);

  cell_info.value_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_NOP);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_VALID == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_int(3);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_VALID == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_DEL_ROW == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_NOP);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_NEW_ADD == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_NOP);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_NEW_ADD == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_DEL_ROW == cell->get_ext()) << cell->get_ext();

  cell_info.value_.set_ext(ObActionFlag::OP_DEL_ROW);
  ObNewScannerHelper::add_cell(row, cell_info, true);
  ASSERT_TRUE(ObActionFlag::OP_DEL_ROW == cell->get_ext()) << cell->get_ext();

}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

