/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_session_test.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include <mysql/mysql.h>
#include <gtest/gtest.h>
#include <sys/time.h>
static const char* HOST = "10.232.36.42";
static int PORT = 45447;
static int SESSION_COUNT = 10000;
static const char* USER = "admin";
static const char* PASSWD = "admin";
class ObSessionTest: public ::testing::Test
{
  public:
    ObSessionTest();
    virtual ~ObSessionTest();
    virtual void SetUp();
    virtual void TearDown();
  private:
    // disallow copy
    ObSessionTest(const ObSessionTest &other);
    ObSessionTest& operator=(const ObSessionTest &other);
  protected:
    void prepare_select(int s);
    void prepare_replace(int s);
  protected:
    // data members
    MYSQL *my_;
};

ObSessionTest::ObSessionTest()
  :my_(NULL)
{
}

ObSessionTest::~ObSessionTest()
{
}

void ObSessionTest::SetUp()
{
}

void ObSessionTest::TearDown()
{
}

#define ASSERT_QUERY_RET(ret)\
  do                         \
  {                          \
  if (0 != ret)              \
  {                                             \
    fprintf(stderr, "%s\n", mysql_stmt_error(stmt)); \
  }                                             \
  ASSERT_EQ(0, ret);                            \
  }while (0)

void ObSessionTest::prepare_select(int s)
{
  MYSQL_STMT *stmt = mysql_stmt_init(&my_[s]);
  ASSERT_TRUE(stmt);
  static const char* SELECT_QUERY = "select * from __first_tablet_entry where table_name = ?";
  // 1. prepare
  int ret = mysql_stmt_prepare(stmt, SELECT_QUERY, strlen(SELECT_QUERY));
  ASSERT_QUERY_RET(ret);
  int param_count = (int)mysql_stmt_param_count(stmt);
  ASSERT_EQ(1, param_count);
  MYSQL_RES *prepare_meta = mysql_stmt_result_metadata(stmt);
  ASSERT_TRUE(prepare_meta);
  int num_fields = mysql_num_fields(prepare_meta);
  ASSERT_EQ(24, num_fields);
}

void ObSessionTest::prepare_replace(int s)
{
  MYSQL_STMT *stmt = mysql_stmt_init(&my_[s]);
  ASSERT_TRUE(stmt);
  static const char* SELECT_QUERY = "replace into __first_tablet_entry (table_name) values(?)";
  // 1. prepare
  int ret = mysql_stmt_prepare(stmt, SELECT_QUERY, strlen(SELECT_QUERY));
  ASSERT_QUERY_RET(ret);
  int param_count = (int)mysql_stmt_param_count(stmt);
  ASSERT_EQ(1, param_count);
}

TEST_F(ObSessionTest, basic_test)
{
  my_ = new(std::nothrow) MYSQL[SESSION_COUNT];
  ASSERT_TRUE(my_);
  for (int s = 0; s < SESSION_COUNT; ++s)
  {
    ASSERT_TRUE(NULL != mysql_init(&my_[s]));
    fprintf(stderr, "[%d] Connecting server %s:%d...\n", s, HOST, PORT);
    if (!mysql_real_connect(&my_[s], HOST, USER, PASSWD, "test", PORT, NULL, 0))
    {
      fprintf(stderr, "Failed to connect to database: Error: %s\n",
              mysql_error(&my_[s]));
      ASSERT_TRUE(0);
    }
    prepare_select(s);
    prepare_replace(s);
  }
  sleep(10);

  for (int s = 0; s < SESSION_COUNT; ++s)
  {
    mysql_close(&my_[s]);
  }
  delete [] my_;
  my_ = NULL;
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc,argv);
  if (0 != mysql_library_init(0, NULL, NULL))
  {
    fprintf(stderr, "could not init mysql library\n");
    exit(1);
  }
  int ch = 0;
  while (-1 != (ch = getopt(argc, argv, "H:P:N:u:p:")))
  {
    switch(ch)
    {
      case 'H':
        HOST = optarg;
        break;
      case 'P':
        PORT = atoi(optarg);
        break;
      case 'u':
        USER = optarg;
        break;
      case 'p':
        PASSWD = optarg;
        break;
      case 'N':
        SESSION_COUNT = atoi(optarg);
        break;
      case '?':
        fprintf(stderr, "option `%c' missing argument\n", optopt);
        exit(0);
      default:
        break;
    }
  }
  int ret = RUN_ALL_TESTS();
  mysql_library_end();
  return ret;
}
