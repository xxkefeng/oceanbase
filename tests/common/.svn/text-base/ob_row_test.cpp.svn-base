/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */


#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_row.h"

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

class ObRowTest: public ::testing::Test
{
  public:
    ObRowTest();
    virtual ~ObRowTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObRowTest(const ObRowTest &other);
    ObRowTest& operator=(const ObRowTest &other);
  protected:
    // data members
};

ObRowTest::ObRowTest()
{
}

ObRowTest::~ObRowTest()
{
}

void ObRowTest::SetUp()
{
}

void ObRowTest::TearDown()
{
}


// ObRow.reset
TEST_F(ObRowTest, reset_test)
{
  ObRow row;
  ObRowDesc row_desc;
  const uint64_t TABLE_ID = 1001;
  const int64_t COLUMN_NUM = 8;
  ObObj cell;
  const ObObj *value = NULL;
  uint64_t table_id = 0;
  uint64_t column_id = 0;
  int64_t int_value = 0;

  row_desc.set_rowkey_cell_count(2);

  for (int i = 0; i < COLUMN_NUM; i ++)
  {
    row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
  }
  row.set_row_desc(row_desc);

  for (int i = 0; i < COLUMN_NUM; i ++)
  {
    cell.set_int(i);
    row.raw_set_cell(i, cell);
  }

  row.reset(true, ObRow::DEFAULT_NULL);
  for (int i = 0; i < row_desc.get_rowkey_cell_count(); i ++)
  {
    row.raw_get_cell(i, value, table_id, column_id);
    value->get_int(int_value);
    ASSERT_EQ(i, int_value);
  }

  for (int64_t i = row_desc.get_rowkey_cell_count(); i < COLUMN_NUM; i ++)
  {
    row.raw_get_cell(i, value, table_id, column_id);
    ASSERT_TRUE( ObNullType == value->get_type() );
  }

  row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
  cell.set_ext(ObActionFlag::OP_DEL_ROW);
  row.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);

  row.reset(true, ObRow::DEFAULT_NOP);

  for (int i = 0; i < row_desc.get_rowkey_cell_count(); i ++)
  {
    row.raw_get_cell(i, value, table_id, column_id);
    value->get_int(int_value);
    ASSERT_EQ(i, int_value);
  }

  for (int64_t i = row_desc.get_rowkey_cell_count(); i < COLUMN_NUM; i ++)
  {
    row.raw_get_cell(i, value, table_id, column_id);
    ASSERT_TRUE( ObActionFlag::OP_NOP == value->get_ext() ) << "ext value: " << value->get_ext();
  }

  row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, value);
  ASSERT_TRUE( ObActionFlag::OP_ROW_DOES_NOT_EXIST == value->get_ext() );
}

TEST_F(ObRowTest, assign_test)
{
  ObRow row;
  ObRowDesc row_desc;

  row_desc.add_column_desc(1001, 8);
  row_desc.add_column_desc(1001, 9);
  row_desc.add_column_desc(1001, 10);

  row_desc.set_rowkey_cell_count(2);
  row.set_row_desc(row_desc);

  const ObRowkey *rowkey = NULL;
  ASSERT_EQ(OB_SUCCESS, row.get_rowkey(rowkey));
  ASSERT_TRUE(rowkey);
  ASSERT_EQ(2, rowkey->get_obj_cnt());
  ASSERT_TRUE(rowkey->get_obj_ptr());

  ObRow row2 = row;

  ASSERT_EQ(row.get_column_num(), row2.get_column_num());

  rowkey = NULL;
  ASSERT_EQ(OB_SUCCESS, row2.get_rowkey(rowkey));
  ASSERT_TRUE(rowkey);
  ASSERT_EQ(2, rowkey->get_obj_cnt());
  ASSERT_TRUE(rowkey->get_obj_ptr());
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
