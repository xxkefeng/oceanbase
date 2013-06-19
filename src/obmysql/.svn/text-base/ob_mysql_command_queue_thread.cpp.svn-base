#include "ob_mysql_command_queue_thread.h"
#include "common/ob_atomic.h"
#include "common/ob_profile_type.h"
#include "obmysql/ob_mysql_define.h"
#include "common/ob_profile_fill_log.h"

using namespace oceanbase::common;
namespace  oceanbase
{
  namespace obmysql
  {
    ObMySQLCommandQueueThread::ObMySQLCommandQueueThread()
      :host_()
    {
      _stop = 0;
      wait_finish_ = false;
      waiting_ = false;
      queue_.init();
      handler_ = NULL;
    }

    ObMySQLCommandQueueThread::~ObMySQLCommandQueueThread()
    {
      stop();
    }

    void ObMySQLCommandQueueThread::set_self_to_thread_queue(const ObServer & host)
    {
      host_ = host;
    }
    void ObMySQLCommandQueueThread::setThreadParameter(int thread_count, ObMySQLPacketQueueHandler* handler,
                                                       void* args)
    {
      setThreadCount(thread_count);
      handler_ = handler;
      UNUSED(args);
    }
    void ObMySQLCommandQueueThread::set_ip_port(const IpPort & ip_port)
    {
      ip_port_ = ip_port;
    }

    bool ObMySQLCommandQueueThread::push(ObMySQLCommandPacket* packet, int max_queue_len, bool block)
    {
      if (max_queue_len > 0 && queue_.size() >= max_queue_len)
      {
        pushcond_.lock();
        waiting_ = true;
        while (_stop == false && queue_.size() >= max_queue_len && block)
        {
          pushcond_.wait(1000);
        }
        waiting_ = false;
        if (queue_.size() >= max_queue_len && !block)
        {
          pushcond_.unlock();
          TBSYS_LOG(WARN, "can not push packet into queue, queue size(%d) >= max_queue_len(%d)",
                    queue_.size(), max_queue_len);
          return false;
        }
        else
        {
          pushcond_.unlock();

          if (_stop)
          {
            return true;
          }
        }
      }
      cond_.lock();
      queue_.push(packet);
      cond_.unlock();
      cond_.signal();
      return true;
    }

    void ObMySQLCommandQueueThread::stop(bool wait_finish)
    {
      cond_.lock();
      _stop = true;
      wait_finish_ = wait_finish;
      cond_.broadcast();
      cond_.unlock();
    }

    void ObMySQLCommandQueueThread::run(tbsys::CThread* thread,void* args)
    {
      UNUSED(thread);
      UNUSED(args);
      ObServer *host = GET_TSI_MULT(ObServer, TSI_COMMON_OBSERVER_1);
      *host = host_;
      ObMySQLCommandPacket* packet = NULL;
      while (!_stop)
      {
        cond_.lock();
        while (!_stop && queue_.size() == 0)
        {
          cond_.wait();
        }
        if (_stop)
        {
          cond_.unlock();
          break;
        }

        packet = queue_.pop();
        cond_.unlock();

        if (waiting_)
        {
          pushcond_.lock();
          pushcond_.signal();
          pushcond_.unlock();
        }

        if (packet == NULL) continue;

        if (handler_)
        {
          int64_t pop_time = tbsys::CTimeUtil::getTime();
          // 这个时候还没有source_chid和chid
          uint32_t trace_seq = atomic_inc(&(SeqGenerator::seq_generator_));
          TraceId *generated_id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
          (generated_id->id).seq_ = trace_seq;
          (generated_id->id).ip_ = ip_port_.ip_;
          (generated_id->id).port_ = ip_port_.port_;
          PFILL_SET_TRACE_ID(generated_id->uval_);
          uint8_t pcode = packet->get_type();
          if (pcode != COM_DELETE_SESSION)
          {
            PFILL_SET_WAIT_SQL_QUEUE_TIME(pop_time - packet->get_receive_ts());
            PFILL_SET_PCODE(pcode);
            if (pcode == COM_STMT_EXECUTE)
            {
              PFILL_SET_SQL("EXEC", 4);
            }
            else
            {
              PFILL_SET_SQL(packet->get_command().ptr(), packet->get_command().length());
            }
          }
          PFILL_ITEM_START(handle_sql_time);
          handler_->handle_packet_queue(packet, args);
          PFILL_ITEM_END(handle_sql_time);
          PFILL_PRINT();
          PFILL_CLEAR_LOG();
        }
      }

      cond_.lock();
      while (queue_.size() > 0)
      {
        packet = queue_.pop();
        cond_.unlock();
        if (handler_ && wait_finish_)
        {
          int64_t pop_time = tbsys::CTimeUtil::getTime();
          uint32_t trace_seq = atomic_inc(&(SeqGenerator::seq_generator_));
          TraceId *generated_id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
          (generated_id->id).seq_ = trace_seq;
          (generated_id->id).ip_ = ip_port_.ip_;
          (generated_id->id).port_ = ip_port_.port_;
          PFILL_SET_TRACE_ID(generated_id->uval_);
          if (packet->get_type() != COM_DELETE_SESSION)
          {
            PFILL_SET_WAIT_SQL_QUEUE_TIME(pop_time - packet->get_receive_ts());
            PFILL_SET_PCODE(packet->get_type());
          }
          PFILL_ITEM_START(handle_sql_time);
          handler_->handle_packet_queue(packet, args);
          PFILL_ITEM_END(handle_sql_time);
        }
        cond_.lock();
      }
      cond_.unlock();
    }
  }
}
