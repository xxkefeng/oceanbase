/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* test_remote_sstable_scan.cpp is for ...
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/

#include "sql/ob_remote_sstable_scan.h"
#include "mock_chunk_server.h"
#include "chunkserver/ob_sql_rpc_stub.h"
#include "common/ob_base_client.h"
#include "sstable/ob_sstable_scan_param.h"

#include <iostream>
#include <gtest/gtest.h>
#include <gmock/gmock.h>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

using ::testing::_;
using ::testing::Invoke;

namespace oceanbase
{
namespace sql
{
namespace test
{

class TestObRemoteSSTableScan : public ::testing::Test
{
protected:
  static void SetUpTestCase();
  static void TearDownTestCase();

protected:
  static MockChunkServer *mock_chunk_server_;
  static tbsys::CThread *chunk_server_thread_;

  static ObBaseClient *base_client_;
  static ThreadSpecificBuffer *sql_rpc_buffer_;
};

class MockSqlRpcStub : public chunkserver::ObSqlRpcStub
{
public:
  explicit MockSqlRpcStub(chunkserver::ObSqlRpcStub *sql_rpc_stub)
    : sql_rpc_stub_(sql_rpc_stub)
  {
  }

  MOCK_CONST_METHOD5(sstable_scan, int(const int64_t timeout, const ObServer &server,
        const sstable::ObSSTableScanParam &param, ObNewScanner &new_scanner,
        int64_t &session_id));
  MOCK_CONST_METHOD4(get_next_session_scanner, int (const int64_t timeout,
        const ObServer &server, const int64_t session_id, ObNewScanner &new_scanner));

  MOCK_CONST_METHOD3(end_next_session, int (const int64_t timeout, ObServer &server,
        const int64_t session_id));

  void delegate() {
    ON_CALL(*this , sstable_scan(_,_,_,_,_)).WillByDefault(Invoke(sql_rpc_stub_,
            &chunkserver::ObSqlRpcStub::sstable_scan));
    ON_CALL(*this , get_next_session_scanner(_,_,_,_)).WillByDefault(Invoke(sql_rpc_stub_,
            &chunkserver::ObSqlRpcStub::get_next_session_scanner));
    ON_CALL(*this , end_next_session(_,_,_)).WillByDefault(Invoke(sql_rpc_stub_,
            &chunkserver::ObSqlRpcStub::end_next_session));
  }

  chunkserver::ObSqlRpcStub *sql_rpc_stub_;
};

////////////////////////////////////////////

MockChunkServer *TestObRemoteSSTableScan::mock_chunk_server_ = NULL;
tbsys::CThread *TestObRemoteSSTableScan::chunk_server_thread_ = NULL;

ObBaseClient *TestObRemoteSSTableScan::base_client_ = NULL;
ThreadSpecificBuffer *TestObRemoteSSTableScan::sql_rpc_buffer_ = NULL;

void TestObRemoteSSTableScan::SetUpTestCase()
{
  // Memory leak is ok.
  mock_chunk_server_ = new MockChunkServer();
  mock_chunk_server_->initialize();

  chunk_server_thread_ = new tbsys::CThread();
  MockServerRunner *runner = new MockServerRunner(*mock_chunk_server_);

  chunk_server_thread_->start(runner, NULL);

  ObServer server;
  base_client_ = new ObBaseClient();
  int rc = base_client_->initialize(server);
  if (OB_SUCCESS != rc)
  {
    TBSYS_LOG(ERROR, "client initialize failed, rc %d", rc);
    return;
  }
  sql_rpc_buffer_ = new ThreadSpecificBuffer(2 << 20);
}

void TestObRemoteSSTableScan::TearDownTestCase()
{
  // Wait a while, let server complete OB_SESSION_END packet before stop.
  usleep(100000);

  mock_chunk_server_->stop();
  chunk_server_thread_->join();
}

TEST_F(TestObRemoteSSTableScan, read_all)
{
  chunkserver::ObSqlRpcStub rpc_stub;

  int rc = rpc_stub.init(sql_rpc_buffer_, &base_client_->get_client_mgr());
  ASSERT_EQ(OB_SUCCESS, rc);

  MockSqlRpcStub mock_rpc_stub(&rpc_stub);
  mock_rpc_stub.delegate();

  EXPECT_CALL(mock_rpc_stub, sstable_scan(_,_,_,_,_)).Times(1);
  EXPECT_CALL(mock_rpc_stub, get_next_session_scanner(_,_,_,_)).Times(2);
  EXPECT_CALL(mock_rpc_stub, end_next_session(_,_,_)).Times(0);

  ObRemoteSSTableScan scan;

  ObServer chunk_server;
  chunk_server.set_ipv4_addr("127.0.0.1", MockChunkServer::CHUNK_SERVER_PORT);
  sstable::ObSSTableScanParam scan_param;
  
  rc = scan.set_param(scan_param, mock_rpc_stub, chunk_server, 10 * 1000 * 1000);
  ASSERT_EQ(OB_SUCCESS, rc);

  rc = scan.open();
  ASSERT_EQ(OB_SUCCESS, rc);

  const ObRow *row;
  while (OB_SUCCESS == (rc = scan.get_next_row(row)))
  {
    ;
  }
  ASSERT_EQ(OB_ITER_END, rc);

  rc = scan.close();
  ASSERT_EQ(OB_SUCCESS, rc);
}

TEST_F(TestObRemoteSSTableScan, read_part)
{
  chunkserver::ObSqlRpcStub rpc_stub;

  int rc = rpc_stub.init(sql_rpc_buffer_, &base_client_->get_client_mgr());
  ASSERT_EQ(OB_SUCCESS, rc);

  MockSqlRpcStub mock_rpc_stub(&rpc_stub);
  mock_rpc_stub.delegate();

  EXPECT_CALL(mock_rpc_stub, sstable_scan(_,_,_,_,_)).Times(1);
  EXPECT_CALL(mock_rpc_stub, get_next_session_scanner(_,_,_,_)).Times(1);
  EXPECT_CALL(mock_rpc_stub, end_next_session(_,_,_)).Times(1);

  ObRemoteSSTableScan scan;

  ObServer chunk_server;
  chunk_server.set_ipv4_addr("127.0.0.1", MockChunkServer::CHUNK_SERVER_PORT);
  sstable::ObSSTableScanParam scan_param;
  
  rc = scan.set_param(scan_param, mock_rpc_stub, chunk_server, 10 * 1000 * 1000);
  ASSERT_EQ(OB_SUCCESS, rc);

  rc = scan.open();
  ASSERT_EQ(OB_SUCCESS, rc);

  const ObRow *row;
  ASSERT_EQ(OB_SUCCESS, scan.get_next_row(row));
  ASSERT_EQ(OB_SUCCESS, scan.get_next_row(row));

  rc = scan.close();
  ASSERT_EQ(OB_SUCCESS, rc);
}

} // end namespace test
} // end namespace sql
} // end namespace oceanbase

int main(int argc, char** argv)
{
  ob_init_memory_pool();

  // testing::InitGoogleTest(&argc, argv);
  ::testing::InitGoogleMock(&argc, argv);
  return RUN_ALL_TESTS();
}
