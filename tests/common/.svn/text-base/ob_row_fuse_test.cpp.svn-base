/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_fuse_test.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "common/ob_malloc.h"
#include <gtest/gtest.h>
#include "common/ob_row_fuse.h"
#include "common/ob_row.h"
#include "common/ob_row_desc.h"

using namespace oceanbase;
using namespace common;

#define OK(value) ASSERT_EQ(OB_SUCCESS, (value))

namespace test
{
  void set_flag(ObRow &row, int64_t action_flag)
  {
    ObObj cell;
    cell.set_ext(action_flag);
    row.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);
  }
  class ObRowFuseTest: public ::testing::Test
  {
    public:
      ObRowFuseTest();
      virtual ~ObRowFuseTest();
      virtual void SetUp();
      virtual void TearDown();
    private:
      // disallow copy
      ObRowFuseTest(const ObRowFuseTest &other);
      ObRowFuseTest& operator=(const ObRowFuseTest &other);
    protected:
      // data members
  };

  ObRowFuseTest::ObRowFuseTest()
  {
  }

  ObRowFuseTest::~ObRowFuseTest()
  {
  }

  void ObRowFuseTest::SetUp()
  {
  }

  void ObRowFuseTest::TearDown()
  {
  }

  TEST_F(ObRowFuseTest, basic_join_test2)
  {
    uint64_t TABLE_ID = 1000;

    ObRowDesc row_desc;
    for(int i=0;i<8;i++)
    {
      row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
    }
    row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);

    ObRow row;
    row.set_row_desc(row_desc);
    ObObj value;

    for(int i=0;i<8;i++)
    {
      value.set_int(i);
      row.raw_set_cell(i, value);
    }

    ObRowDesc ups_row_desc;
    for(int i=0;i<4;i++)
    {
      ups_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
    }
    ups_row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);

    ObRow ups_row;
    ups_row.set_row_desc(ups_row_desc);
    set_flag(ups_row, ObActionFlag::OP_DEL_ROW);

    for(int i=0;i<4;i++)
    {
      value.set_ext(ObActionFlag::OP_NOP);
      ups_row.raw_set_cell(i, value);
    }

    ObRow result;
    OK(ObRowFuse::join_row(&ups_row, &row, &result));

    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;

    const ObObj *cell = NULL;

    for(int i=0;i<4;i++)
    {
      OK(result.raw_get_cell(i, cell, table_id, column_id));
      ASSERT_EQ(ObNullType, cell->get_type());
    }

    for(int i=4;i<8;i++)
    {
      int64_t int_value = 0;
      OK(result.raw_get_cell(i, cell, table_id, column_id));
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i);
    }


    for(int i=0;i<4;i++)
    {
      value.set_int(i+100);
      ups_row.raw_set_cell(i, value);
    }

    OK(ObRowFuse::join_row(&ups_row, &row, &result));
    for(int i=0;i<4;i++)
    {
      int64_t int_value = 0;
      OK(result.raw_get_cell(i, cell, table_id, column_id));
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i+100);
    }

    for(int i=4;i<8;i++)
    {
      int64_t int_value = 0;
      OK(result.raw_get_cell(i, cell, table_id, column_id));
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i);
    }

    ups_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + 10);
    ASSERT_TRUE(OB_SUCCESS != ObRowFuse::join_row(&ups_row, &row, &result));
  }


    /*
     * after init row
     * seq no:         0 1  2  3  4  5  6  7
     * row:            1 2 +3 +4 +5 +6 +7 +8
     * result_row:     1 2  4  5  6  7  8  9
     *
     * not change:     1 2  4  5  6  7  8  9   i+2
     * copy:           1 2  3  4  5  6  7  8   i+1
     * apply:          1 2  7  9 11 13 15 17   i*2 + 3
     *
     */
  void init_row(ObRow &row, ObRow &result_row, int64_t column_num)
  {
    ObObj value;
    const ObRowDesc *row_desc = row.get_row_desc();

    for (int64_t i = row_desc->get_rowkey_cell_count(); i < column_num; ++i)
    {
      value.set_int(i+1, true);
      row.raw_set_cell(i, value);
    }

    for (int64_t i = row_desc->get_rowkey_cell_count(); i < column_num; ++i)
    {
      value.set_int(i + 2);
      result_row.raw_set_cell(i, value);
    }

    for(int64_t i = 0; i < row_desc->get_rowkey_cell_count(); ++i)
    {
      value.set_int(i+1);
      row.raw_set_cell(i, value);
      result_row.raw_set_cell(i, value);
    }
  }

  #define MY_ASSERT_EQUAL(value1, value2) \
    if ((value1) != (value2)) \
    { \
      TBSYS_LOG(WARN, "value1[%ld] != value2[%ld]", (value1), (value2)); \
      return false; \
    }

  enum ResultState
  {
    NOT_CHANGE,
    COPY,
    APPLY,
    ALL_NULL
  };

  bool check_result(const ObRow &row, enum ResultState state, int64_t column_num)
  {
    int64_t int_value = 0;
    const ObRowDesc *row_desc = row.get_row_desc();
    const ObObj *cell = NULL;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;

    if (NULL == row_desc)
    {
      return false;
    }
    for (int64_t i = row_desc->get_rowkey_cell_count(); i < column_num; i ++)
    {
      row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      switch (state)
      {
        case NOT_CHANGE:
          MY_ASSERT_EQUAL( int_value, i + 2 );
          break;
        case COPY:
          MY_ASSERT_EQUAL( int_value, i + 1);
          break;
        case APPLY:
          MY_ASSERT_EQUAL( int_value, i * 2 + 3);
          break;
        case ALL_NULL:
          MY_ASSERT_EQUAL( static_cast<int64_t>(ObNullType), static_cast<int64_t>(cell->get_type()) );
      }
    }
    for (int64_t i = 0; i < row_desc->get_rowkey_cell_count(); i ++)
    {
      row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      MY_ASSERT_EQUAL( int_value, i + 1 );
    }
    return true;
  }

  //test ObRowFuse.fuse_row
  TEST_F(ObRowFuseTest, fuse_row_test)
  {
    ObRow row;
    ObRow result_row;

    ObRowDesc row_desc;
    ObRowDesc result_row_desc;
    const uint64_t TABLE_ID = 1001;
    const int64_t COLUMN_NUM = 8;
    bool is_row_empty = false;

    row_desc.set_rowkey_cell_count(2);
    result_row_desc.set_rowkey_cell_count(2);

    for (int i = 0; i < COLUMN_NUM; i ++)
    {
      row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
      result_row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
    }
    row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);

    row.set_row_desc(row_desc);
    result_row.set_row_desc(result_row_desc);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_NEW_ADD);
    ObRowFuse::fuse_row(row, result_row, is_row_empty);
    ASSERT_TRUE(check_result(result_row, COPY, COLUMN_NUM)) << to_cstring(row) << std::endl << to_cstring(result_row) << std::endl;
    ASSERT_TRUE(!is_row_empty);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_DEL_ROW);
    ObRowFuse::fuse_row(row, result_row, is_row_empty);
    ASSERT_TRUE(check_result(result_row, ALL_NULL, COLUMN_NUM));
    ASSERT_TRUE(is_row_empty);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    is_row_empty = true;
    ObRowFuse::fuse_row(row, result_row, is_row_empty);
    ASSERT_TRUE(check_result(result_row, NOT_CHANGE, COLUMN_NUM));
    ASSERT_TRUE(is_row_empty);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_VALID);
    ObRowFuse::fuse_row(row, result_row, is_row_empty);
    ASSERT_TRUE(check_result(result_row, APPLY, COLUMN_NUM));
    ASSERT_TRUE(!is_row_empty);
  }

  //test ObRowFuse.apply_row
  TEST_F(ObRowFuseTest, apply_row_test)
  {
    ObRow row;
    ObRow result_row;

    ObRowDesc row_desc;
    const uint64_t TABLE_ID = 1001;
    const int64_t COLUMN_NUM = 8;
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    int64_t int_value = 0;
    const ObObj *cell = NULL;

    row_desc.set_rowkey_cell_count(2);

    for (int i = 0; i < COLUMN_NUM; i ++)
    {
      row_desc.add_column_desc(TABLE_ID, OB_APP_MIN_COLUMN_ID + i);
    }
    row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);

    row.set_row_desc(row_desc);
    result_row.set_row_desc(row_desc);

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_DEL_ROW);
    set_flag(result_row, ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    ObRowFuse::apply_row(row, result_row, true);

    for (int64_t i = row_desc.get_rowkey_cell_count(); i < COLUMN_NUM; i++)
    {
      result_row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i+1);
    }

    for (int i = 0; i < row_desc.get_rowkey_cell_count(); i ++)
    {
      result_row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i+1);
    }

    result_row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);
    ASSERT_TRUE( ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell->get_ext() );

    init_row(row, result_row, COLUMN_NUM);
    set_flag(row, ObActionFlag::OP_DEL_ROW);
    set_flag(result_row, ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    ObRowFuse::apply_row(row, result_row, false);

    for (int64_t i = row_desc.get_rowkey_cell_count(); i < COLUMN_NUM; i++)
    {
      result_row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i * 2 + 3);
    }

    for (int i = 0; i < row_desc.get_rowkey_cell_count(); i ++)
    {
      result_row.raw_get_cell(i, cell, table_id, column_id);
      cell->get_int(int_value);
      ASSERT_EQ(int_value, i + 1);
    }

    result_row.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, cell);
    ASSERT_TRUE( ObActionFlag::OP_ROW_DOES_NOT_EXIST == cell->get_ext() );
  }

  TEST_F(ObRowFuseTest, basic_join_test)
  {
    ObRowDesc row_desc;
    uint64_t table_id = 1000;
    for(int i=0;i<8;i++)
    {
      row_desc.add_column_desc(table_id, OB_APP_MIN_COLUMN_ID + i);
    }
    row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);

    ObRow ups_row;
    ups_row.set_row_desc(row_desc);
    set_flag(ups_row, ObActionFlag::OP_VALID);

    ObObj cell;
    for(int i=0;i<8;i++)
    {
      cell.set_int(i, true);
      ups_row.raw_set_cell(i, cell);
    }

    ObRow row;
    row.set_row_desc(row_desc);

    for(int i=0;i<8;i++)
    {
      cell.set_int(i);
      row.raw_set_cell(i, cell);
    }

    ObRow result;

    ObRowFuse::join_row(&ups_row, &row, &result);

    const ObObj *result_cell = NULL;
    uint64_t result_table_id = 0;
    uint64_t result_column_id = 0;
    int64_t int_value = 0;
    for(int i=0;i<8;i++)
    {
      result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
      result_cell->get_int(int_value);
      ASSERT_EQ(i * 2, int_value);
    }

    set_flag(ups_row, ObActionFlag::OP_DEL_ROW);
    ObRowFuse::join_row(&ups_row, &row, &result);

    for(int i=0;i<8;i++)
    {
      result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
      result_cell->get_int(int_value);
      ASSERT_EQ(i, int_value);
    }

    for(int i=0;i<8;i++)
    {
      cell.set_int(3*i, false);
      ups_row.raw_set_cell(i, cell);
    }

    set_flag(ups_row, ObActionFlag::OP_VALID);
    ObRowFuse::join_row(&ups_row, &row, &result);

    for(int i=0;i<8;i++)
    {
      result.raw_get_cell(i, result_cell, result_table_id, result_column_id);
      result_cell->get_int(int_value);
      ASSERT_EQ(3*i, int_value);
    }
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
