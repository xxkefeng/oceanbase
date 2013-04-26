/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_se_array_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "common/ob_se_array.h"
#include <gtest/gtest.h>
using namespace oceanbase::common;

class ObSEArrayTest: public ::testing::Test
{
  public:
    ObSEArrayTest();
    virtual ~ObSEArrayTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSEArrayTest(const ObSEArrayTest &other);
    ObSEArrayTest& operator=(const ObSEArrayTest &other);
  private:
    // data members
};

ObSEArrayTest::ObSEArrayTest()
{
}

ObSEArrayTest::~ObSEArrayTest()
{
}

void ObSEArrayTest::SetUp()
{
}

void ObSEArrayTest::TearDown()
{
}

struct A
{
  int64_t a_;
};

TEST_F(ObSEArrayTest, basic_test)
{
  ObSEArray<A, 256> searray, searray2;
  A a;
  int NUMS[] = {256, 1024};
  for (int k = 0; k < (int)ARRAYSIZEOF(NUMS); ++k)
  {
    int num = NUMS[k];
    searray.clear();
    searray2.clear();
    for (int64_t i = 0; i < num; ++i)
    {
      a.a_ = i;
      ASSERT_EQ(OB_SUCCESS, searray.push_back(a));
      ASSERT_EQ(i+1, searray.count());
    }
    searray2 = searray;
    for (int64_t i = 0; i < num; ++i)
    {
      ASSERT_EQ(i, searray2.at(i).a_);
    }
    searray.clear();
    ASSERT_EQ(0, searray.count());
  }
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
