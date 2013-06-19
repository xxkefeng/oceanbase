/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_schema_task.h for update multi-version schemas form
 * rootserver. 
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_SCHEMA_TIMER_TASK_H_
#define OCEANBASE_CHUNKSERVER_SCHEMA_TIMER_TASK_H_

#include "common/ob_timer.h"
#include "common/ob_server.h"

namespace oceanbase
{
  namespace common
  {
    class ObMergerSchemaManager;
    class ObGeneralRpcStub;
    class ObSchemaManagerV2;
  }
  namespace chunkserver
  {
    class ObMergerRpcProxy;

    // check and fetch new schema timer task
    class ObMergerSchemaTask : public common::ObTimerTask
    {
    public:
      ObMergerSchemaTask();
      ~ObMergerSchemaTask();
    
    public:
      // init set rpc and schema manager
      void init(const common::ObServer & root_server, 
          common::ObGeneralRpcStub * rpc_stub,
          common::ObMergerSchemaManager * schema);
      
      // set fetch new version
      void set_version(const int64_t local, const int64_t remote);

      // main routine
      void runTimerTask(void);

      inline bool is_scheduled() const { return task_scheduled_; }
      inline void set_scheduled() { task_scheduled_ = true; }
      inline void unset_scheduled() { task_scheduled_ = false; }
    
    private:
      bool check_inner_stat(void) const; 
      // lock and check whether need fetch new schema
      // param  @timestamp new schema timestamp
      //        @manager the new schema pointer
      int fetch_new_schema(const int64_t timestamp, const common::ObSchemaManagerV2 ** manager);
    
    public:
      bool task_scheduled_;
      volatile int64_t local_version_;
      volatile int64_t remote_version_;
      common::ObServer root_server_;                         // root server addr
      const common::ObGeneralRpcStub * rpc_stub_;            // rpc stub bottom module
      common::ObMergerSchemaManager * schema_manager_;
      tbsys::CThreadMutex schema_lock_;                      // lock for update schema manager
    };
    
    inline void ObMergerSchemaTask::init(const common::ObServer & root_server, 
        common::ObGeneralRpcStub * rpc_stub,
        common::ObMergerSchemaManager * schema)
    {
      local_version_ = 0;
      remote_version_ = 0;
      root_server_ = root_server;
      rpc_stub_ = rpc_stub;
      schema_manager_ = schema;
    }
    
    inline void ObMergerSchemaTask::set_version(const int64_t local, const int64_t server)
    {
      local_version_ = local;
      remote_version_ = server;
    }

    inline bool ObMergerSchemaTask::check_inner_stat(void) const
    {
      return ((NULL != rpc_stub_) && (NULL != schema_manager_));
    }
  }
}

#endif //OCEANBASE_CHUNKSERVER_SCHEMA_TIMER_TASK_H_
