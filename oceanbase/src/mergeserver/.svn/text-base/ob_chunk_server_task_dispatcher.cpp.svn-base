#include "ob_chunk_server_task_dispatcher.h"
#include "ob_ms_tsi.h"
#include "common/ob_malloc.h"
#include "common/ob_crc64.h"
#include "common/murmur_hash.h"

using namespace oceanbase;
using namespace oceanbase::mergeserver;
using namespace oceanbase::common;

ObChunkServerTaskDispatcher ObChunkServerTaskDispatcher::task_dispacher_;

ObChunkServerTaskDispatcher * ObChunkServerTaskDispatcher::get_instance()
{
  return &task_dispacher_;
}

ObChunkServerTaskDispatcher::ObChunkServerTaskDispatcher()
{
  local_ip_ = 0;
  using_new_balance_ = false;
}

void ObChunkServerTaskDispatcher::set_factor(const bool use_new_method)
{
  using_new_balance_ = use_new_method;
  TBSYS_LOG(INFO, "open new balance method:on[%d]", use_new_method);
}

ObChunkServerTaskDispatcher::~ObChunkServerTaskDispatcher()
{
}

int ObChunkServerTaskDispatcher::select_cs(const bool open, ObChunkServerItem * replicas_in_out, const int32_t replica_count_in,
    ObMergerServerCounter * counter)
{
  int ret = static_cast<int32_t>(random()%replica_count_in);
  if (open && (NULL != counter))
  {
    int64_t min_count = (((uint64_t)1) << 63) - 1;
    int64_t cur_count = 0;
    for (int32_t i = 0; i < replica_count_in; ++i)
    {
      cur_count = counter->get(replicas_in_out[i].addr_);
      if (0 == cur_count)
      {
        ret = i;
        break;
      }
      if (cur_count < min_count)
      {
        min_count = cur_count;
        ret = i;
      }
    }
  }
  return ret;
}

int ObChunkServerTaskDispatcher::select_cs(ObChunkServerItem * replicas_in_out,
    const int32_t replica_count_in, const int32_t last_query_idx_in)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret && NULL == replicas_in_out)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "parameter replicas_in_out is null");
  }

  if(OB_SUCCESS == ret && replica_count_in <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "replica_count_in should be positive:replica_count_in[%d]", replica_count_in);
  }

  if(OB_SUCCESS == ret && last_query_idx_in >= replica_count_in)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "last_query_idx_in should be less than replica_count_in:last_query_idx_in[%d], replica_count_in[%d]",
      last_query_idx_in, replica_count_in);
  }
  if(OB_SUCCESS == ret)
  {
    ObMergerServerCounter * counter = GET_TSI_MULT(ObMergerServerCounter, SERVER_COUNTER_ID);
    if (NULL == counter)
    {
      TBSYS_LOG(WARN, "get tsi server counter failed:counter[%p]", counter);
    }
    if(last_query_idx_in < 0) // The first time request
    {
      /// select the min request counter server
      ret = select_cs(using_new_balance_, replicas_in_out, replica_count_in, counter);
      replicas_in_out[ret].status_ = ObChunkServerItem::REQUESTED;
    }
    else
    {
      ret = OB_ENTRY_NOT_EXIST;
      for(int i=1;i<=replica_count_in;i++)
      {
        int next = (last_query_idx_in + i) % replica_count_in;
        if(ObChunkServerItem::UNREQUESTED == replicas_in_out[next].status_)
        {
          ret = next;
          replicas_in_out[ret].status_ = ObChunkServerItem::REQUESTED;
          break;
        }
      }
      if(OB_ENTRY_NOT_EXIST == ret)
      {
        TBSYS_LOG(INFO, "There is no chunkserver which is never requested");
      }
    }
  }
  return ret;

}

int ObChunkServerTaskDispatcher::select_cs(ObChunkServerItem * replicas_in_out, const int32_t replica_count_in,
    const int32_t last_query_idx_in, const ObNewRange & tablet_in)
{
  int ret = OB_SUCCESS;
  UNUSED(tablet_in);
  if(OB_SUCCESS == ret && NULL == replicas_in_out)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "parameter replicas_in_out is null");
  }

  if(OB_SUCCESS == ret && replica_count_in <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "replica_count_in should be positive:replica_count_in[%d]", replica_count_in);
  }

  if(OB_SUCCESS == ret && last_query_idx_in >= replica_count_in)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "last_query_idx_in should be less than replica_count_in:last_query_idx_in[%d], replica_count_in[%d]",
      last_query_idx_in, replica_count_in);
  }

  if(OB_SUCCESS == ret &&
    0 > (ret = select_cs(replicas_in_out, replica_count_in, last_query_idx_in)))
  {
    TBSYS_LOG(WARN, "select cs fail: replica_count_in[%d], last_query_idx_in[%d] ret[%d]",
        replica_count_in, last_query_idx_in, ret);
  }

  return ret;
}

int ObChunkServerTaskDispatcher::select_cs(ObChunkServerItem * replicas_in_out, const int32_t replica_count_in,
  const int32_t last_query_idx_in, const ObCellInfo & get_cell_in)
{
  int ret = OB_SUCCESS;
  UNUSED(get_cell_in); 
  if(OB_SUCCESS == ret && NULL == replicas_in_out)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "parameter replicas_in_out is null");
  }

  if(OB_SUCCESS == ret && replica_count_in <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "replica_count_in should be positive:replica_count_in[%d]", replica_count_in);
  }

  if(OB_SUCCESS == ret && last_query_idx_in >= replica_count_in)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "last_query_idx_in should be less than replica_count_in:last_query_idx_in[%d], replica_count_in[%d]",
      last_query_idx_in, replica_count_in);
  }

  if (OB_SUCCESS == ret &&
    0 > (ret = select_cs(replicas_in_out, replica_count_in, last_query_idx_in)))
  {
    TBSYS_LOG(WARN, "select cs fail: replica_count_in[%d], last_query_idx_in[%d] ret[%d]",
        replica_count_in, last_query_idx_in, ret);
  }
  return ret;
}


