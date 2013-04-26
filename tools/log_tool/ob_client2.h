#include "common/ob_define.h"
#include "common/ob_result.h"
#include "common/utility.h"
#include "common/ob_packet_factory.h"
#include "common/ob_client_manager.h"
#if USE_LIBEASY
#include "common/ob_tbnet_callback.h"
#include "easy_io_struct.h"
#include "easy_io.h"
#endif

using namespace oceanbase::common;

struct Dummy
{
  int serialize(char* buf, int64_t len, int64_t& pos) const
  {
    UNUSED(buf); UNUSED(len); UNUSED(pos);
    return OB_SUCCESS;
  }
  int deserialize(char* buf, int64_t len, int64_t& pos)
  {
    UNUSED(buf); UNUSED(len); UNUSED(pos);
    return OB_SUCCESS;
  }
};

template <class T>
int uni_serialize(const T &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return data.serialize(buf, data_len, pos);
};

template <class T>
int uni_deserialize(T &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return data.deserialize(buf, data_len, pos);
};

template <>
int uni_serialize<uint64_t>(const uint64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::encode_vi64(buf, data_len, pos, (int64_t)data);
};

template <>
int uni_serialize<int64_t>(const int64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::encode_vi64(buf, data_len, pos, data);
};

template <>
int uni_deserialize<uint64_t>(uint64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::decode_vi64(buf, data_len, pos, (int64_t*)&data);
};

template <>
int uni_deserialize<int64_t>(int64_t &data, char *buf, const int64_t data_len, int64_t& pos)
{
  return serialization::decode_vi64(buf, data_len, pos, &data);
};

template <>
int uni_serialize<ObDataBuffer>(const ObDataBuffer &data, char *buf, const int64_t data_len, int64_t& pos)
{
  int err = OB_SUCCESS;
  if (NULL == buf || pos <= 0 || pos > data_len)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (pos + data.get_position() > data_len)
  {
    err = OB_BUF_NOT_ENOUGH;
  }
  else
  {
    memcpy(buf + pos, data.get_data(), data.get_position());
    pos += data.get_position();
  }
  return err;
};

template <>
int uni_deserialize<ObDataBuffer>(ObDataBuffer& data, char *buf, const int64_t data_len, int64_t& pos)
{
  UNUSED(data);
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  return OB_NOT_SUPPORTED;
};

ObDataBuffer& __get_thread_buffer(ThreadSpecificBuffer& thread_buf, ObDataBuffer& buf)
{
  ThreadSpecificBuffer::Buffer* my_buffer = thread_buf.get_buffer();
  my_buffer->reset();
  buf.set_data(my_buffer->current(), my_buffer->remain());
  return buf;
}

template <class Input, class Output>
int send_request_with_rt(int64_t& rt, ObClientManager* client_mgr, const ObServer& server,
                 const int32_t version, const int pcode, const Input &param, Output &result,
                 ObDataBuffer& buf, const int64_t timeout)
{
  int err = OB_SUCCESS;
  int64_t start_time = tbsys::CTimeUtil::getTime();
  err = send_request(client_mgr, server, version, pcode, param, result, buf, timeout);
  rt = tbsys::CTimeUtil::getTime() - start_time;
  return err;
}

template <class Input, class Output>
int send_request(int64_t* rt, ObClientManager* client_mgr, const ObServer& server,
                 const int32_t version, const int pcode, const Input &param, Output &result,
                 ObDataBuffer& buf, const int64_t timeout)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObResultCode result_code;
  int64_t start_time = tbsys::CTimeUtil::getTime();

  if (OB_SUCCESS != (err = uni_serialize(param, buf.get_data(), buf.get_capacity(), buf.get_position())))
  {
    TBSYS_LOG(ERROR, "serialize()=>%d", err);
  }
  else if (OB_SUCCESS != (err = client_mgr->send_request(server, pcode, version, timeout, buf)))
  {
    TBSYS_LOG(WARN, "failed to send request, ret=%d", err);
  }
  else if (OB_SUCCESS != (err = result_code.deserialize(buf.get_data(), buf.get_position(), pos)))
  {
    TBSYS_LOG(ERROR, "deserialize result_code failed:pos[%ld], ret[%d]", pos, err);
  }
  else if (OB_SUCCESS != (err = result_code.result_code_))
  {
    TBSYS_LOG(DEBUG, "result_code.result_code = %d", err);
  }
  else if (OB_SUCCESS != (err = uni_deserialize(result, buf.get_data(), buf.get_position(), pos)))
  {
    TBSYS_LOG(ERROR, "deserialize result data failed:pos[%ld], ret[%d]", pos, err);
  }
  else if (NULL != rt)
  {
    *rt = tbsys::CTimeUtil::getTime() - start_time;
  }
  return err;
}

const int64_t DEFAULT_VERSION = 1;
const int64_t DEFAULT_TIMEOUT = 5 * 1000*1000;
#if !USE_LIBEASY
class TbnetBaseClient
{
  public:
    const static int64_t MAX_N_TRANSPORT = 256;
    TbnetBaseClient(): n_transport_(0) {}
    virtual ~TbnetBaseClient(){
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = destroy()))
      {
        TBSYS_LOG(ERROR, "destroy()=>%d", err);
      }
    }
  public:
    virtual int initialize(int64_t n_transport = 1) {
      int err = OB_SUCCESS;
      streamer_.setPacketFactory(&factory_);
      TBSYS_LOG(INFO, "client[n_transport=%ld]", n_transport);
      if (0 >= n_transport)
      {
        err = OB_INVALID_ARGUMENT;
      }
      for(int64_t i = 0; i < n_transport; i++)
      {
        if (OB_SUCCESS != (err = client_[i].initialize(&transport_[i], &streamer_)))
        {
          TBSYS_LOG(ERROR, "client_mgr.initialize()=>%d", err);
        }
        else if (!transport_[i].start())
        {
          err = OB_ERROR;
          TBSYS_LOG(ERROR, "transport.start()=>%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        n_transport_ = n_transport;
      }
      return err;
    }

    virtual int destroy() {
      for(int64_t i = 0; i < n_transport_; i++)
      {
        transport_[i].stop();
      }
      return wait();
    }

    virtual int wait(){
      int err = OB_SUCCESS;
      for(int64_t i = 0; OB_SUCCESS == err && i < n_transport_; i++)
      {
        err = transport_[i].wait();
      }
      return err;
    }

    // ObClientManager * get_rpc() {
    //   return &client_;
    // }

    template <class Server, class Input, class Output>
    int send_request(const Server server_spec, const int pcode,
                     const Input &param, Output &result, int64_t id = 0, int64_t* rt = NULL,
                     const int32_t version=DEFAULT_VERSION, const int64_t timeout=DEFAULT_TIMEOUT) {
      int err = OB_SUCCESS;
      ObDataBuffer buf;
      ObServer server;
      if (OB_SUCCESS != (err = to_server(server, server_spec)))
      {
        TBSYS_LOG(ERROR, "invalid server");
      }
      else if (OB_SUCCESS != (err = ::send_request(rt, &client_[id % n_transport_], server, version, pcode, param, result, __get_thread_buffer(rpc_buffer_, buf), timeout)))
      {
      }
      return err;
    }
  protected:
    int64_t n_transport_;
    tbnet::DefaultPacketStreamer streamer_;
    ObPacketFactory factory_;
    tbnet::Transport transport_[MAX_N_TRANSPORT];
    ObClientManager client_[MAX_N_TRANSPORT];
    ThreadSpecificBuffer rpc_buffer_;
};
typedef TbnetBaseClient BaseClient;
#else
int process(easy_request_t* r)
{
  int ret = EASY_OK;
  if (NULL == r || NULL == r->ipacket)
  {
    ret = EASY_BREAK;
  }
  else
  {
    ret = EASY_AGAIN;
  }
  return ret;
}

class EasyBaseClient
{
  public:
    const static int64_t MAX_N_CLIENT = 256;
    EasyBaseClient(): n_client_(0), io_thread_count_(0) {
      memset(eio_, 0, sizeof(eio_));
    }
    virtual ~EasyBaseClient(){
      int err = OB_SUCCESS;
      if (OB_SUCCESS != (err = destroy()))
      {
        TBSYS_LOG(ERROR, "destroy()=>%d", err);
      }
    }
  public:
    int init_server_handler(easy_io_handler_pt& server_handler) {
      int err = OB_SUCCESS;
      memset(&server_handler, 0, sizeof(easy_io_handler_pt));
      server_handler.encode = ObTbnetCallback::encode;
      server_handler.decode = ObTbnetCallback::decode;
      server_handler.process = process;
      server_handler.get_packet_id = ObTbnetCallback::get_packet_id;
      server_handler.on_disconnect = ObTbnetCallback::on_disconnect;
      server_handler.user_data = this;
      return err;
    }
    virtual int initialize(int64_t io_thread_count = 1, int64_t n_client = 1) {
      int err = OB_SUCCESS;
      TBSYS_LOG(INFO, "client[io_thread_count=%ld, n_client=%ld]", io_thread_count, n_client);
      if (0 >= io_thread_count)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = init_server_handler(server_handler_)))
      {}
      for(int64_t i = 0; OB_SUCCESS == err && i < n_client; i++)
      {
        if (NULL == (eio_[i] = easy_eio_create(eio_[i], (int)io_thread_count)))
        {
          err = OB_CONN_ERROR;
        }
        else if (OB_SUCCESS != (err = client_[i].initialize(eio_[i], &server_handler_)))
        {
          TBSYS_LOG(ERROR, "client_mgr.initialize()=>%d", err);
        }
        else if (OB_SUCCESS != (err = easy_eio_start(eio_[i])))
        {
          TBSYS_LOG(ERROR, "easy_eio_start(i=%ld)=>%d", i, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        n_client_ = n_client;
        io_thread_count_ = io_thread_count;
      }
      return err;
    }

    virtual int destroy() {
      int err = OB_SUCCESS;
      for(int64_t i = 0; i < n_client_; i++)
      {
        easy_eio_stop(eio_[i]);
        easy_eio_wait(eio_[i]);
        easy_eio_destroy(eio_[i]);
      }
      for(int64_t i = 0; i < n_client_; i++)
      {
        //client_[i].destroy();
      }
      return err;
    }

    virtual int wait(){
      int err = OB_SUCCESS;
      for(int64_t i = 0; i < n_client_; i++)
      {
        easy_eio_wait(eio_[i]);
      }
      return err;
    }

    // ObClientManager * get_rpc() {
    //   return &client_;
    // }

    template <class Server, class Input, class Output>
    int send_request(const Server server_spec, const int pcode,
                     const Input &param, Output &result, int64_t id = 0, int64_t* rt = NULL,
                     const int32_t version=DEFAULT_VERSION, const int64_t timeout=DEFAULT_TIMEOUT) {
      int err = OB_SUCCESS;
      ObDataBuffer buf;
      ObServer server;
      if (OB_SUCCESS != (err = to_server(server, server_spec)))
      {
        TBSYS_LOG(ERROR, "invalid server(%s)", to_cstring(server_spec));
      }
      else if (OB_SUCCESS != (err = ::send_request(rt, &client_[id % n_client_], server, version, pcode, param, result, __get_thread_buffer(rpc_buffer_, buf), timeout)))
      {
      }
      return err;
    }
  protected:
    int64_t n_client_;
    int64_t io_thread_count_;
    easy_io_t *eio_[MAX_N_CLIENT];
    easy_io_handler_pt server_handler_;
    ObClientManager client_[MAX_N_CLIENT];
    ThreadSpecificBuffer rpc_buffer_;
};
typedef EasyBaseClient BaseClient;
#endif
Dummy _dummy_;
