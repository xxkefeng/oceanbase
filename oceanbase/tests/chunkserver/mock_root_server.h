/**
 * (C) 2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * mock_root_server.h
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef __OCEANBASE_CHUNKSERVER_MOCK_ROOT_SERVER_H__
#define __OCEANBASE_CHUNKSERVER_MOCK_ROOT_SERVER_H__

#include <gtest/gtest.h>
#include "tbnet.h"
#include "tbsys.h"

#include "common/ob_array.h"
#include "common/ob_tbnet_callback.h"
#include "common/ob_packet_factory.h"
#include "common/ob_base_server.h"
#include "common/ob_single_server.h"
#include "common/ob_malloc.h"
#include "common/ob_result.h"
#include "common/ob_client_manager.h"
#include "common/ob_tablet_info.h"
#include "chunkserver/ob_chunk_server.h"
#include "rootserver/ob_root_callback.h"
#include "index/ob_tablet_distribution.h"
#include "test_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

const int32_t MOCK_SERVER_LISTEN_PORT = 8866;

namespace oceanbase
{
  namespace tests
  {
    namespace chunkserver
    {
      inline ObString cstring_to_obstring(char *str)
      {
        return ObString(static_cast<int32_t>(strlen(str)),
              static_cast<int32_t>(strlen(str)), str);
      }

      class MockRootServer : public ObSingleServer
      {
        public:
          MockRootServer() : schema_(NULL)
          {
          }
          virtual ~MockRootServer()
          {
          }
          virtual int initialize()
          {
            set_batch_process(false);
            set_listen_port(MOCK_SERVER_LISTEN_PORT);
            set_dev_name("bond0");
            set_default_queue_size(100);
            set_thread_count(1);

            memset(&server_handler_, 0, sizeof(easy_io_handler_pt));
            server_handler_.encode = ObTbnetCallback::encode;
            server_handler_.decode = ObTbnetCallback::decode;
            server_handler_.process = rootserver::ObRootCallback::process;
            //server_handler_.batch_process = ObTbnetCallback::batch_process;
            server_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
            server_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
            server_handler_.user_data = this;

            ObSingleServer::initialize();

            return OB_SUCCESS;
          }

          void set_data_tablet_array(const ObArray<ObTabletDistItem>& data_tablet_array)
          {
            data_tablet_array_ = &data_tablet_array;
          }

          void set_index_tablet_array(const ObArray<ObTabletDistItem>& index_tablet_array)
          {
            index_tablet_array_ = &index_tablet_array;
          }

          void set_schema(const ObSchemaManagerV2 &schema)
          {
            schema_ = &schema;
          }

          virtual int do_request(ObPacket* base_packet)
          {
            int ret = OB_SUCCESS;
            ObPacket* ob_packet = base_packet;
            int32_t packet_code = ob_packet->get_packet_code();
            int32_t version = ob_packet->get_api_version();
            int32_t channel_id = ob_packet->get_channel_id();
            ret = ob_packet->deserialize();

            TBSYS_LOG(INFO, "recv packet with packet_code[%d] version[%d] channel_id[%d]",
                packet_code, version, channel_id);

            if (OB_SUCCESS == ret)
            {
              switch (packet_code)
              {
              case OB_HEARTBEAT:
                handle_heartbeat(ob_packet);
                break;
              case OB_SERVER_REGISTER:
                handle_server_register(ob_packet);
                break;
              case OB_REPORT_TABLETS:
                handle_report_tablets(ob_packet);
                break;
              case OB_REPORT_CAPACITY_INFO:
                handle_report_capacity_info(ob_packet);
                break;
              case OB_WAITING_JOB_DONE:
                handle_waiting_job_done(ob_packet);
                break;
              case OB_SCAN_REQUEST:
                handle_scan_roottable(ob_packet);
                break ;
              case OB_CS_REPORT_SAMPLES:
                handle_report_samples(ob_packet);
                break;
              case OB_CS_SAMPLE_TABLE_DONE:
                handle_sample_tablet_done(ob_packet);
                break;
              case OB_CS_BUILD_INDEX_DONE:
                handle_build_index_done(ob_packet);
                break;
              case OB_FETCH_SCHEMA:
                handle_fetch_schema(ob_packet);
                break;
              default:
                break;
              };
            }

            return ret;
          }

          int handle_heartbeat(ObPacket* packet)
          {
            UNUSED(packet);
            return OB_SUCCESS;
          }

          int handle_server_register(ObPacket *packet)
          {
            int ret = OB_SUCCESS;
            easy_request_t* connection = packet->get_request();
            ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
            if (NULL != thread_buffer)
            {
              thread_buffer->reset();
              ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
              ObResultCode rc;

              rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position());

              int status = 1;
              serialization::encode_vi32(out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position(), status);

              ret = send_response(OB_SERVER_REGISTER_RESPONSE, 1, out_buffer, connection,
                  packet->get_channel_id());
            }
            else
            {
              ret = OB_ERROR;
            }

            return ret;
          }

          int handle_fetch_schema(ObPacket *packet)
          {
            int ret = OB_SUCCESS;
            easy_request_t* connection = packet->get_request();
            ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
            if (NULL != thread_buffer)
            {
              thread_buffer->reset();
              ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
              ObResultCode rc;

              if (NULL == schema_)
              {
                TBSYS_LOG(WARN, "schema not set");
                rc.result_code_ = OB_ERROR;
              }

              rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position());

              if (NULL != schema_)
              {
                schema_->serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position());
              }

              ret = send_response(OB_FETCH_STATS_RESPONSE, 1, out_buffer, connection,
                  packet->get_channel_id());
            }
            else
            {
              ret = OB_ERROR;
            }

            return ret;
          }

          int handle_report_capacity_info(ObPacket *packet)
          {
            int ret = OB_SUCCESS;
            easy_request_t* connection = packet->get_request();
            ThreadSpecificBuffer::Buffer* thread_buffer = response_packet_buffer_.get_buffer();
            if (NULL != thread_buffer)
            {
              thread_buffer->reset();
              ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());
              ObResultCode rc;

              rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                  out_buffer.get_position());

              ret = send_response(OB_REPORT_CAPACITY_INFO_RESPONSE, 1, out_buffer, connection,
                  packet->get_channel_id());
            }
            else
            {
              ret = OB_ERROR;
            }

            return ret;
          }

          int handle_report_tablets(ObPacket* ob_packet)
          {
            int ret = OB_SUCCESS;

            ObDataBuffer* data = ob_packet->get_buffer();
            if (NULL == data)
            {
              TBSYS_LOG(ERROR, "data is NUll should not reach this");
              ret = OB_ERROR;
            }
            else
            {
              ObResultCode result_msg;
              ObServer server;
              ObTabletReportInfoList tablet_list;
              int64_t time_stamp = 0;

              if (OB_SUCCESS == ret)
              {
                ret = server.deserialize(data->get_data(), data->get_capacity(), 
                                         data->get_position());
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "server.deserialize error");
                }
              }
              if (OB_SUCCESS == ret)
              {
                ret = tablet_list.deserialize(data->get_data(), data->get_capacity(), 
                                              data->get_position());
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "tablet_list.deserialize error");
                }
                TBSYS_LOG(INFO, "tablets size: %ld", tablet_list.get_tablet_size());
                const ObTabletReportInfo* tablets = tablet_list.get_tablet();
                EXPECT_TRUE(NULL != tablets);
                for (int64_t i = 0; i < tablet_list.get_tablet_size(); ++i)
                {
                  TBSYS_LOG(INFO, "the %ld-th tablet: version=%ld, range=(%s, %s]\n", i,
                      tablets[i].tablet_location_.tablet_version_,
                      to_cstring(tablets[i].tablet_info_.range_.start_key_),
                      to_cstring(tablets[i].tablet_info_.range_.end_key_));
                }
              }
              if (OB_SUCCESS == ret)
              {
                ret = serialization::decode_vi64(data->get_data(), data->get_capacity(), 
                                                 data->get_position(), &time_stamp);
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "time_stamp.deserialize error");
                }
                TBSYS_LOG(INFO, "timestamp: %ld", time_stamp);
              }

              easy_request_t* connection = ob_packet->get_request();
              ThreadSpecificBuffer::Buffer* thread_buffer =
                response_packet_buffer_.get_buffer();
              if (NULL != thread_buffer)
              {
                thread_buffer->reset();
                ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

                result_msg.result_code_ = ret;
                result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                    out_buffer.get_position());

                TBSYS_LOG(DEBUG, "handle tablets report packet");

                int32_t version = 1;
                int32_t channel_id = ob_packet->get_channel_id();
                ret = send_response(OB_REPORT_TABLETS_RESPONSE, version, out_buffer, 
                                    connection, channel_id);
              }
              else
              {
                TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
                ret = OB_ERROR;
              }
            }

            return ret;
          }

          int handle_waiting_job_done(ObPacket* ob_packet)
          {
            int ret = OB_SUCCESS;

            ObDataBuffer* data_buffer = ob_packet->get_buffer();
            if (NULL == data_buffer)
            {
              TBSYS_LOG(ERROR, "data_buffer is NUll should not reach this");
              ret = OB_ERROR;
            }
            else
            {
              easy_request_t* connection = ob_packet->get_request();
              ThreadSpecificBuffer::Buffer* thread_buffer =
                response_packet_buffer_.get_buffer();
              if (NULL != thread_buffer)
              {
                thread_buffer->reset();
                ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

                ObResultCode response;
                response.result_code_ = OB_SUCCESS;
                response.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                    out_buffer.get_position());

                TBSYS_LOG(DEBUG, "handle wait job done");

                int32_t version = 1;
                int32_t channel_id = ob_packet->get_channel_id();
                ret = send_response(OB_WAITING_JOB_DONE_RESPONSE, version, out_buffer, 
                                    connection, channel_id);
                TBSYS_LOG(DEBUG, "handle wait job done");
              }
              else
              {
                TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
                ret = OB_ERROR;
              }
            }

            return ret;
          }

          int handle_scan_roottable(ObPacket* ob_packet)
          {
            static struct {
              char port_[64];
              char ipv4_[64];
              char tablet_version_[64];
              char tablet_status_[64];
            } column_names[] = {
              {"1_port", "1_ipv4", "1_tablet_version", "1_tablet_status"},
              {"2_port", "2_ipv4", "2_tablet_version", "2_tablet_status"},
              {"3_port", "3_ipv4", "3_tablet_version", "3_tablet_status"}};

            int ret = OB_SUCCESS;

            ObDataBuffer* data = ob_packet->get_buffer();
            if (NULL == data)
            {
              TBSYS_LOG(ERROR, "data is NUll should not reach this");
              ret = OB_ERROR;
            }
            else
            {
              uint64_t table_id = OB_INVALID_ID;
              const ObArray<ObTabletDistItem>* tablet_array = NULL;
              ObResultCode result_msg;
              ObCellInfo out_cell;
              ObNewRange tablet_range;
              tablet_range.set_whole_range();
              scan_param_.reset();
              scanner_.reset();
              scanner_.set_range(tablet_range);
              scanner_.set_data_version(1);

              if (OB_SUCCESS == ret)
              {
                ret = scan_param_.deserialize(data->get_data(), data->get_capacity(), 
                                              data->get_position());
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "scan_param_.deserialize error");
                }
                TBSYS_LOG(INFO, "scan table_id=%lu", scan_param_.get_table_id());
              }

              if (OB_SUCCESS == ret)
              {
                EXPECT_TRUE(NULL != data_tablet_array_);
                EXPECT_TRUE(NULL != index_tablet_array_);
                table_id = scan_param_.get_table_id();
                tablet_array = ((uint64_t)SSTableBuilder::table_id == table_id) ? data_tablet_array_ : index_tablet_array_;
                for (int64_t i = 0; i < tablet_array->count(); ++i)
                {
                  out_cell.row_key_ = tablet_array->at(i).tablet_range_.end_key_;
                  for (int64_t k = 0; k < tablet_array->at(i).locations_count_; k++)
                  {
                    out_cell.column_name_ = cstring_to_obstring(column_names[k].port_);
                    out_cell.value_.set_int(tablet_array->at(i).locations_[k].location_.chunkserver_.get_port());
                    if (OB_SUCCESS != (ret = scanner_.add_cell(out_cell)))
                    {
                      break;
                    }

                    out_cell.column_name_ = cstring_to_obstring(column_names[k].ipv4_);
                    out_cell.value_.set_int(tablet_array->at(i).locations_[k].location_.chunkserver_.get_ipv4());
                    if (OB_SUCCESS != (ret = scanner_.add_cell(out_cell)))
                    {
                      break;
                    }

                    out_cell.column_name_ = cstring_to_obstring(column_names[k].tablet_version_);
                    out_cell.value_.set_int(tablet_array->at(i).locations_[k].location_.tablet_version_);
                    if (OB_SUCCESS != (ret = scanner_.add_cell(out_cell)))
                    {
                      break;
                    }

                    out_cell.column_name_ = cstring_to_obstring(column_names[k].tablet_status_);
                    out_cell.value_.set_int(tablet_array->at(i).locations_[k].tablet_status_);
                    if (OB_SUCCESS != (ret = scanner_.add_cell(out_cell)))
                    {
                      break;
                    }
                  }
                }
              }

              easy_request_t* connection = ob_packet->get_request();
              ThreadSpecificBuffer::Buffer* thread_buffer =
                response_packet_buffer_.get_buffer();
              if (NULL != thread_buffer)
              {
                thread_buffer->reset();
                ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

                result_msg.result_code_ = ret;
                result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                    out_buffer.get_position());

                ret = scanner_.serialize(out_buffer.get_data(),
                    out_buffer.get_capacity(), out_buffer.get_position());
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "serialize ObScanner failed.");
                }

                TBSYS_LOG(DEBUG, "handle tablets report packet");

                int32_t version = 1;
                int32_t channel_id = ob_packet->get_channel_id();
                ret = send_response(OB_SCAN_RESPONSE, version, out_buffer, 
                                    connection, channel_id);
              }
              else
              {
                TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
                ret = OB_ERROR;
              }
            }

            return ret;
          }

          int handle_report_samples(ObPacket* ob_packet)
          {
            int ret = OB_SUCCESS;

            ObDataBuffer* data = ob_packet->get_buffer();
            if (NULL == data)
            {
              TBSYS_LOG(ERROR, "data is NUll should not reach this");
              ret = OB_ERROR;
            }
            else
            {
              ObResultCode result_msg;
              ObServer server;
              new_scanner_.reuse();

              if (OB_SUCCESS == ret)
              {
                ret = server.deserialize(data->get_data(), data->get_capacity(), 
                                         data->get_position());
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "server.deserialize error");
                }
                TBSYS_LOG(INFO, "server: %s", to_cstring(server));
              }

              if (OB_SUCCESS == ret)
              {
                ret = new_scanner_.deserialize(data->get_data(), data->get_capacity(), 
                                               data->get_position());
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "scanner_.deserialize error");
                }
                TBSYS_LOG(INFO, "scanner size: %ld, row_num=%ld", 
                          new_scanner_.get_size(), new_scanner_.get_row_num());
                new_scanner_.dump();
                new_scanner_.dump_all(TBSYS_LOG_LEVEL_INFO);
              }

              easy_request_t* connection = ob_packet->get_request();
              ThreadSpecificBuffer::Buffer* thread_buffer =
                response_packet_buffer_.get_buffer();
              if (NULL != thread_buffer)
              {
                thread_buffer->reset();
                ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

                result_msg.result_code_ = ret;
                result_msg.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                    out_buffer.get_position());

                TBSYS_LOG(DEBUG, "handle tablets report packet");

                int32_t version = 1;
                int32_t channel_id = ob_packet->get_channel_id();
                ret = send_response(OB_CS_REPORT_SAMPLES_RESPONSE, version, out_buffer, 
                                    connection, channel_id);
              }
              else
              {
                TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
                ret = OB_ERROR;
              }
            }

            return ret;
          }

          int handle_sample_tablet_done(ObPacket* ob_packet)
          {
            int ret = OB_SUCCESS;

            ObDataBuffer* data = ob_packet->get_buffer();
            if (NULL == data)
            {
              TBSYS_LOG(ERROR, "data is NUll should not reach this");
              ret = OB_ERROR;
            }
            else
            {
              ObServer server;
              bool is_succ = false;
              int64_t index_table_id = 0;

              if (OB_SUCCESS == ret)
              {
                ret = serialization::decode_bool(data->get_data(), data->get_capacity(), 
                                                 data->get_position(), &is_succ);
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "is_succ.deserialize error");
                }
                TBSYS_LOG(INFO, "is_succ: %d", is_succ);
              }

              if (OB_SUCCESS == ret)
              {
                ret = server.deserialize(data->get_data(), data->get_capacity(), 
                                         data->get_position());
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "server.deserialize error");
                }
                TBSYS_LOG(INFO, "server: %s", to_cstring(server));
              }

              if (OB_SUCCESS == ret)
              {
                ret = serialization::decode_vi64(data->get_data(), data->get_capacity(), 
                                                 data->get_position(), &index_table_id);
                if (ret != OB_SUCCESS)
                {
                  TBSYS_LOG(ERROR, "index_table_id.deserialize error");
                }
                TBSYS_LOG(INFO, "index_table_id: %ld", index_table_id);
              }

              easy_request_t* connection = ob_packet->get_request();
              ThreadSpecificBuffer::Buffer* thread_buffer =
                response_packet_buffer_.get_buffer();
              if (NULL != thread_buffer)
              {
                thread_buffer->reset();
                ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

                ObResultCode response;
                response.result_code_ = OB_SUCCESS;
                response.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                    out_buffer.get_position());

                TBSYS_LOG(DEBUG, "handle sample tablet done");

                int32_t version = 1;
                int32_t channel_id = ob_packet->get_channel_id();
                ret = send_response(OB_CS_SAMPLE_TABLE_DONE_RESPONSE, version, out_buffer, 
                                    connection, channel_id);
                TBSYS_LOG(DEBUG, "handle sample tablet done");
              }
              else
              {
                TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
                ret = OB_ERROR;
              }
            }

            return ret;
          }


          int handle_build_index_done(ObPacket* ob_packet)
          {
            int ret = OB_SUCCESS;

            ObDataBuffer* data_buffer = ob_packet->get_buffer();
            if (NULL == data_buffer)
            {
              TBSYS_LOG(ERROR, "data_buffer is NUll should not reach this");
              ret = OB_ERROR;
            }
            else
            {
              easy_request_t* connection = ob_packet->get_request();
              ThreadSpecificBuffer::Buffer* thread_buffer =
                response_packet_buffer_.get_buffer();
              if (NULL != thread_buffer)
              {
                thread_buffer->reset();
                ObDataBuffer out_buffer(thread_buffer->current(), thread_buffer->remain());

                ObResultCode response;
                response.result_code_ = OB_SUCCESS;
                response.serialize(out_buffer.get_data(), out_buffer.get_capacity(),
                    out_buffer.get_position());

                TBSYS_LOG(DEBUG, "handle build index done");

                int32_t version = 1;
                int32_t channel_id = ob_packet->get_channel_id();
                ret = send_response(OB_CS_BUILD_INDEX_DONE_RESPONSE, version, out_buffer, 
                                    connection, channel_id);
                TBSYS_LOG(DEBUG, "handle build index done");
              }
              else
              {
                TBSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
                ret = OB_ERROR;
              }
            }

            return ret;
          }

        private:
          ThreadSpecificBuffer response_packet_buffer_;
          const ObArray<ObTabletDistItem>* data_tablet_array_;
          const ObArray<ObTabletDistItem>* index_tablet_array_;
          const ObSchemaManagerV2 *schema_;
          ObScanParam scan_param_;
          ObScanner scanner_;
          ObNewScanner new_scanner_;
      };

      class MockRootServerRunner : public tbsys::Runnable
      {
        public:
          void set_tablet_array(const ObArray<ObTabletDistItem>& data_tablet_array,
                                const ObArray<ObTabletDistItem>& index_tablet_array)
          {
            mock_server_.set_data_tablet_array(data_tablet_array);
            mock_server_.set_index_tablet_array(index_tablet_array);
          }

          MockRootServer &get_mock_server() { return mock_server_; }

          virtual void run(tbsys::CThread *thread, void *arg)
          {
            UNUSED(thread);
            UNUSED(arg);
            mock_server_.start();
          }
        private:
          MockRootServer mock_server_;
      };
    }
  }
}

#endif //__MOCK_ROOT_SERVER_H__

