/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_schema_task.cpp for update multi-version schemas form
 * rootserver.
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_schema_task.h"
#include "ob_chunk_server_main.h"
#include "common/ob_schema_manager.h"
#include "common/ob_malloc.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObMergerSchemaTask::ObMergerSchemaTask()
    {
      task_scheduled_ = false;
      schema_manager_ = NULL;
      local_version_ = 0;
      remote_version_ = 0;
    }

    ObMergerSchemaTask::~ObMergerSchemaTask()
    {
    }

    int ObMergerSchemaTask::fetch_new_schema(const int64_t timestamp, const ObSchemaManagerV2 ** manager)
    {
      int ret = OB_SUCCESS;
      if (NULL == manager)
      {
        TBSYS_LOG(WARN, "check shema manager param failed:manager[%p]", manager);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        tbsys::CThreadGuard lock(&schema_lock_);
        if (schema_manager_->get_latest_version() >= timestamp)
        {
          *manager = schema_manager_->get_user_schema(0);
          if (NULL == *manager)
          {
            TBSYS_LOG(WARN, "get latest but local schema failed:schema[%p], latest[%ld]",
              *manager, schema_manager_->get_latest_version());
            ret = OB_INNER_STAT_ERROR;
          }
          else
          {
            TBSYS_LOG(DEBUG, "get new schema is fetched by other thread:schema[%p], latest[%ld]",
              *manager, (*manager)->get_version());
          }
        }
        else
        {
          char * temp = (char *)ob_malloc(sizeof(ObSchemaManagerV2),ObModIds::OB_MS_RPC);
          if (NULL == temp)
          {
            TBSYS_LOG(ERROR, "check ob malloc failed");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            ObSchemaManagerV2 * schema = new(temp) ObSchemaManagerV2;
            if (NULL == schema)
            {
              TBSYS_LOG(ERROR, "check replacement new schema failed:schema[%p]", schema);
              ret = OB_INNER_STAT_ERROR;
            }
            else
            {
              ret = rpc_stub_->fetch_schema(THE_CHUNK_SERVER.get_config().network_timeout, root_server_, 0, false, *schema);
              if (ret != OB_SUCCESS)
              {
                TBSYS_LOG(WARN, "rpc fetch schema failed:version[%ld], ret[%d]", timestamp, ret);
              }
              else
              {
                ret = schema_manager_->add_schema(*schema, manager);
                // maybe failed because of timer thread fetch and add it already
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "add new schema failed:version[%ld], ret[%d]", schema->get_version(), ret);
                  ret = OB_SUCCESS;
                  *manager = schema_manager_->get_user_schema(0);
                  if (NULL == *manager)
                  {
                    TBSYS_LOG(WARN, "get latest schema failed:schema[%p], latest[%ld]",
                        *manager, schema_manager_->get_latest_version());
                    ret = OB_INNER_STAT_ERROR;
                  }
                }
                else
                {
                  TBSYS_LOG(DEBUG, "fetch and add new schema succ:version[%ld]", schema->get_version());
                }
              }
            }
            schema->~ObSchemaManagerV2();
            ob_free(temp);
            temp = NULL;
          }
        }
      }
      return ret;
    }

    void ObMergerSchemaTask::runTimerTask(void)
    {
      int ret = OB_SUCCESS;
      if (true != check_inner_stat())
      {
        TBSYS_LOG(WARN, "check schema timer task inner stat failed");
      }
      else if (remote_version_ > local_version_)
      {
        const ObSchemaManagerV2 * new_schema = NULL;
        ret = fetch_new_schema(remote_version_, &new_schema);
        if ((ret != OB_SUCCESS) || (NULL == new_schema))
        {
          TBSYS_LOG(WARN, "fetch new schema version failed:schema[%p], local[%ld], "
              "new[%ld], ret[%d]", new_schema, local_version_, remote_version_, ret);
        }
        else
        {
          ret = schema_manager_->release_schema(new_schema);
          if (ret != OB_SUCCESS)
          {
            TBSYS_LOG(WARN, "release schema failed:ret[%d]", ret);
          }
          else
          {
            TBSYS_LOG(INFO, "fetch new schema succ:local[%ld], new[%ld]",
                local_version_, remote_version_);
          }
        }
      }
      else
      {
        TBSYS_LOG(WARN, "check new version lt than local version:local[%ld], new[%ld]",
            local_version_, remote_version_);
      }
      unset_scheduled();
    }
  } // end namespace chunkserver
} // end namespace oceanbase
