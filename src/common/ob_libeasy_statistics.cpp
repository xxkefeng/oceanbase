/* (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License
 *  version 2 as published by the Free Software Foundation.
 *
 * Version: 0.1
 *
 * Authors:
 *    Wu Di <lide.wd@taobao.com>
 */


#include <pthread.h>
#include "ob_libeasy_statistics.h"
#include "tbsys.h"
#include "utility.h"

using namespace oceanbase;
using namespace oceanbase::common;

ObLibEasyStatistics::ObLibEasyStatistics()
  :eio_(NULL)
{
}
ObLibEasyStatistics::~ObLibEasyStatistics()
{
}
void ObLibEasyStatistics::runTimerTask()
{
  TBSYS_LOG(INFO, "START PRINT LIBEASY STATISTICS");
  OB_ASSERT(eio_ != NULL);
  easy_thread_pool_t *tp = eio_->io_thread_pool;
  easy_io_thread_t *ioth = NULL;
  easy_connection_t *c = NULL;
  easy_connection_t *c2 = NULL;
  if (OB_UNLIKELY(NULL == tp))
  {
    TBSYS_LOG(ERROR, "easy_thread_pool_t == NULL, ret=%d", OB_ERR_UNEXPECTED);
  }
  else
  {
    easy_thread_pool_for_each(ioth, tp, 0)
    {
      int64_t pos = 0;
      //ioth->tid : pthread_t
      databuff_printf(buffer_, FD_BUFFER_SIZE, pos, "tid=%ld fds=", ioth->tid);
      easy_list_for_each_entry_safe(c, c2, &(ioth->connected_list), conn_list_node)
      {
        databuff_printf(buffer_, FD_BUFFER_SIZE, pos, "%d,", c->fd);
      }
      TBSYS_LOG(INFO, "%.*s", static_cast<int>(pos), buffer_);
    }
  }
  TBSYS_LOG(INFO, "END PRINT LIBEASY STATISTICS");
}
void ObLibEasyStatistics::init(easy_io_t *eio)
{
  eio_ = eio;
}

