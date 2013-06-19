/*
 *
 * This program is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License version 2 as 
 * published by the Free Software Foundation.
 *
 * ob_test_charset.cpp is for what ...
 *
 * Version: ***: ob_test_charset.cpp  Fri Apr 19 10:09:08 2013 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@alipay.com
 *     -some work detail if you want 
 *
 */
#include <mysql/mysql.h>
#include <stdio.h>
#include <gtest/gtest.h>

int charset[5] =  { 5, 24, 16, 13, 28};
const char* tables[5] = {"ta", "tb", "tc", "td", "te"};
class ObSqlCharsetTest: public ::testing::Test
{
  public:
  ObSqlCharsetTest(){};
  virtual ~ObSqlCharsetTest(){};
  virtual void SetUp();
  virtual void TearDown();
  
  MYSQL *mysql_;
  
  void test_create_table_abnormal();
  void test_create_table_normal();
  void test_result_charset();
  void test_result_charset_ps();
  void insert_tables();
};

void ObSqlCharsetTest::TearDown()
{
  mysql_close(mysql_);
  mysql_ = NULL;
}

void ObSqlCharsetTest::SetUp()
{
  mysql_ = mysql_init(NULL);
  ASSERT_TRUE(NULL != mysql_);
  
  mysql_ = mysql_real_connect(mysql_, "10.232.36.177", "admin", "admin", "test", 3142, 0, NULL);
  ASSERT_TRUE(NULL != mysql_);
  int ret = mysql_query(mysql_, "drop table if exists ta, tb, tc, td, tf, te");
  ASSERT_EQ(ret, 0);
}

void ObSqlCharsetTest::test_create_table_abnormal()
{
  const char *ta = "create table test(c1 int primary key, c2 varchar, c3 int) charset='latin'";
  const char *tb = "create table test(c1 int primary key, c2 varchar, c3 int) character='latin1'";

  int ret = mysql_query(mysql_, ta);
  ASSERT_TRUE(ret != 0);
  
  ret = mysql_query(mysql_, tb);
  ASSERT_TRUE(ret != 0);
}

void ObSqlCharsetTest::test_create_table_normal()
{
  const char *ta = "create table ta(c1 int primary key, c2 varchar, c3 int) charset='latin1'";
  const char *tb = "create table tb(c1 int primary key, c2 varchar, c3 int) default charset='gb2312'";
  const char *tc = "create table tc(c1 int primary key, c2 varchar, c3 int) character set='hebrew'";
  const char *td = "create table td(c1 int primary key, c2 varchar, c3 int) default character set='sjis'";
  const char *te = "create table te(c1 int primary key, c2 varchar, c3 int)";

  int ret = mysql_query(mysql_, ta);
  ASSERT_EQ(ret, 0);
  ret = mysql_query(mysql_, tb);
  ASSERT_EQ(ret, 0);
  ret = mysql_query(mysql_, tc);
  ASSERT_EQ(ret, 0);
  ret = mysql_query(mysql_, td);
  ASSERT_EQ(ret, 0);
  ret = mysql_query(mysql_, te);
  ASSERT_EQ(ret, 0);
}

void ObSqlCharsetTest::insert_tables()
{
  
  char insert_str[100];
  for (int i = 0; i < 5; ++i)
  {
    snprintf(insert_str, 100, "insert into  %s values(1, '%s', 2), (2, '%s', 3), (3, '%s', 4)",
             tables[i], tables[i], tables[i], tables[i]);
    int ret = mysql_query(mysql_, insert_str);
    ASSERT_EQ(ret, 0);
  }
}

void ObSqlCharsetTest::test_result_charset()
{
  
  char select_str[100];
  test_create_table_normal();
  insert_tables();
  for (int i = 0; i < 5; ++i)
  {
    snprintf(select_str, 100, "select * from %s", tables[i]);
    int ret = mysql_query(mysql_, select_str);
    ASSERT_EQ(ret, 0);
    MYSQL_RES *res = mysql_store_result(mysql_);
    ASSERT_TRUE(NULL != res);
    ASSERT_TRUE(res->field_count == 3);
    for (int j = 0; j < 3; j++)
    {
      MYSQL_FIELD *field = res->fields + j;
      ASSERT_EQ(field->charsetnr, charset[i]);
    }
  }
}

void ObSqlCharsetTest::test_result_charset_ps()
{
  int charset[5] =  { 5, 24, 16, 13, 28};
  char ps_select_str[100];
  test_create_table_normal();
  insert_tables();
  for (int i = 0; i < 5; ++i)
  {
    MYSQL_STMT *stmt = mysql_stmt_init(mysql_);
    ASSERT_TRUE(NULL != stmt);
    snprintf(ps_select_str, 100, "select * from %s", tables[i]);
    int ret = mysql_stmt_prepare(stmt, ps_select_str, strlen(ps_select_str));
    ASSERT_EQ(ret, 0);
    ret = mysql_stmt_execute(stmt);
    ASSERT_EQ(ret, 0);
    ret = mysql_stmt_store_result(stmt);
    ASSERT_EQ(ret, 0);
    MYSQL_RES *res = mysql_stmt_result_metadata(stmt);
    ASSERT_TRUE(NULL != res);
    for (int j = 0; j < 3; ++j)
    {
      MYSQL_FIELD *field = res->fields + j;
      ASSERT_EQ(field->charsetnr, charset[i]);
    }
  }
}

TEST_F(ObSqlCharsetTest, test_create_table_abnormal)
{
  test_create_table_abnormal();
}

TEST_F(ObSqlCharsetTest, test_create_table_normal)
{
  test_create_table_normal();
}

TEST_F(ObSqlCharsetTest, test_result_charset)
{
  test_result_charset();
}

TEST_F(ObSqlCharsetTest, test_result_charset_ps)
{
  test_result_charset_ps();
}

int main(int argc, char* argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
