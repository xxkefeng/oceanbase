/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_table_verifier.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_root_table_verifier.h"
#include "ob_root_ms_provider.h"
#include "ob_root_ups_provider.h"
#include "common/roottable/ob_scan_helper_impl.h"
#include "common/ob_table_id_name.h"
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootTableVerifier::ObRootTableVerifier()
  :old_rt_(NULL),
   root_table_rwlock_(NULL),
   schema_service_(NULL),
   schema_manager_(NULL),
   server_manager_(NULL),
   ups_manager_(NULL),
   config_(NULL),
   rpc_stub_(NULL),
   rt_service_(NULL),
   role_mgr_(NULL)
{
}

ObRootTableVerifier::~ObRootTableVerifier()
{
}


void ObRootTableVerifier::set_components(ObRootTable2 &old_rt, tbsys::CRWLock &root_table_rwlock,
                                         common::ObSchemaService &schema_service,
                                         common::ObSchemaManagerV2 &schema_manager,
                                         ObChunkServerManager &server_manager,
                                         ObUpsManager &ups_manager,
                                         ObRootConfig &config,
                                         ObRootRpcStub &rpc_stub,
                                         ObRootTableService &rt_service,
                                         ObRoleMgr &role_manager)
{
  old_rt_ = &old_rt;
  root_table_rwlock_ = &root_table_rwlock;
  schema_service_ = &schema_service;
  schema_manager_ = &schema_manager;
  server_manager_ = &server_manager;
  ups_manager_ = &ups_manager;
  config_ = &config;
  rpc_stub_ = &rpc_stub;
  rt_service_ = &rt_service;
  role_mgr_ = &role_manager;
}


int ObRootTableVerifier::check_integrity()
{
  int ret = OB_SUCCESS;
  if (NULL == old_rt_
      || NULL == root_table_rwlock_
      || NULL == schema_service_
      || NULL == schema_manager_
      || NULL == server_manager_
      || NULL == ups_manager_
      || NULL == config_
      || NULL == rpc_stub_
      || NULL == rt_service_
      || NULL == role_mgr_)
  {
    ret = OB_NOT_INIT;
  }
  return ret;
}

int ObRootTableVerifier::verify_schemas_and_tables()
{
  int ret = OB_SUCCESS;
  ObRootMsProvider ms_provider(*server_manager_);
  ObUps ups_master;
  ups_manager_->get_ups_master(ups_master);
  ObRootUpsProvider ups_provider(ups_master.addr_);
  ObScanHelperImpl scan_helper;
  scan_helper.set_ms_provider(&ms_provider);
  scan_helper.set_rpc_stub(rpc_stub_);
  scan_helper.set_ups_provider(&ups_provider);
  scan_helper.set_scan_timeout(config_->flag_network_timeout_us_.get());
  scan_helper.set_mutate_timeout(config_->flag_network_timeout_us_.get());

  ObTableIdNameIterator tables_id_name;
  ObTableIdName* tid_name = NULL;
  if (OB_SUCCESS != (ret = tables_id_name.init(&scan_helper)))
  {
    TBSYS_LOG(WARN, "failed to init iterator, err=%d", ret);
  }
  else
  {
    int64_t table_count = 0;
    while (OB_SUCCESS == (ret = tables_id_name.next()))
    {
      if (OB_SUCCESS != (ret = tables_id_name.get(&tid_name)))
      {
        TBSYS_LOG(WARN, "failed to get next name, err=%d", ret);
        break;
      }
      else
      {
        ++table_count;
        int err = OB_SUCCESS;
        err = verify_table_schema(tid_name->table_name_);
        if (OB_SUCCESS != err)
        {
          ret = err;
        }
        err = verify_root_table(tid_name->table_id_);
        if (OB_SUCCESS != err)
        {
          ret = err;
        }
      }
    } // end while
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
    if (table_count != schema_manager_->get_table_count())
    {
      TBSYS_LOG(WARN, "[RT_VERIFIER] new_table_count=%ld, old=%ld",
                table_count, schema_manager_->get_table_count());
      ret = OB_ERR_UNEXPECTED;
    }
    else
    {
      TBSYS_LOG(INFO, "[RT_VERIFIER] new_table_count=%ld, old=%ld",
                table_count, schema_manager_->get_table_count());
    }
  }
  return ret;
}

int ObRootTableVerifier::verify_table_schema(const ObString &tname)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* otschema = schema_manager_->get_table_schema(tname);
  TableSchema ntschema;
  if (NULL == otschema)
  {
    TBSYS_LOG(WARN, "failed to get schema from schema_manager");
  }
  else if (OB_SUCCESS != (ret = schema_service_->get_table_schema(tname, ntschema)))
  {
    TBSYS_LOG(WARN, "failed to get schema from schema_service, err=%d", ret);
  }
  else
  {
    if (otschema->get_table_id() != ntschema.table_id_)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "[RT_VERIFIER] table id not match, old_tid=%lu new_tid=%lu",
                otschema->get_table_id(), ntschema.table_id_);
    }
  }
  return ret;
}

int ObRootTableVerifier::verify_root_table(const uint64_t tid)
{
  int ret = OB_SUCCESS;
  UNUSED(tid);
  ObRootMsProvider ms_provider(*server_manager_);
  ObUps ups_master;
  ups_manager_->get_ups_master(ups_master);
  ObRootUpsProvider ups_provider(ups_master.addr_);
  ObScanHelperImpl scan_helper;
  scan_helper.set_ms_provider(&ms_provider);
  scan_helper.set_rpc_stub(rpc_stub_);
  scan_helper.set_ups_provider(&ups_provider);
  scan_helper.set_scan_timeout(config_->flag_network_timeout_us_.get());
  scan_helper.set_mutate_timeout(config_->flag_network_timeout_us_.get());

  ObRootTable3 *new_rt = NULL;
  const ObRootTable3::Value* crow = NULL;
  ObNewRange range_min_max;
  range_min_max.table_id_ = tid;
  range_min_max.set_whole_range();
  range_min_max.border_flag_.set_inclusive_start();
  range_min_max.border_flag_.unset_inclusive_end();
  ObRootTable3::ConstIterator* first = NULL;

  if (OB_SUCCESS != (ret = rt_service_->aquire_root_table(scan_helper, new_rt)))
  {
    TBSYS_LOG(ERROR, "failed to aquire new root table, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = new_rt->search(range_min_max, first)))
  {
    TBSYS_LOG(WARN, "failed to search tablet, err=%d", ret);
  }
  else
  {
    int err = OB_SUCCESS;
    // for each tablet in old root table
    ObRootTable2::const_iterator it;
    const ObTabletInfo* tablet = NULL;
    bool table_found = false;
    int64_t tablet_idx = 0;
    tbsys::CRLockGuard guard(*root_table_rwlock_);
    for (it = old_rt_->begin(); it != old_rt_->sorted_end(); ++it)
    {
      tablet = old_rt_->get_tablet_info(it);
      if (NULL != tablet)
      {
        if (tablet->range_.table_id_ == tid)
        {
          if (!table_found)
          {
            table_found = true;
          }
          if (OB_SUCCESS != (err = first->next(crow)))
          {
            TBSYS_LOG(WARN, "no row found for the new table, err=%d range=%s",
                      err, to_cstring(tablet->range_));
            ret = err;
          }
          else
          {
            verify_tablet(it, tablet, crow);
          }
          ++tablet_idx;
        }
        else
        {
          if (table_found)
          {
            // another table
            break;
          }
        }
      }
    } // end for
    if (OB_ITER_END != (ret = first->next(crow)))
    {
      TBSYS_LOG(WARN, "new root table has more tablet than the old, err=%d", ret);
      if (OB_SUCCESS == ret)
      {
        TBSYS_LOG(WARN, "endkey=%s startkey=%s", to_cstring(crow->get_end_key()),
                  to_cstring(crow->get_start_key()));
      }
    }
    rt_service_->release_root_table(new_rt);
  }
  return ret;
}

int ObRootTableVerifier::verify_tablet(ObRootTable2::const_iterator it, const ObTabletInfo* tablet,
                  const common::ObRootTable3::Value* crow)
{
  int ret = OB_ERR_UNEXPECTED;
  if (tablet->range_.end_key_ != crow->get_end_key())
  {
    TBSYS_LOG(WARN, "endkey not match");
  }
  else if (tablet->range_.start_key_ != crow->get_start_key())
  {
    TBSYS_LOG(WARN, "endkey not match");
  }
  else
  {
    ret = OB_SUCCESS;

    int32_t replica_num = 0;
    for (int32_t i = 0; i < OB_SAFE_COPY_COUNT; i++)
    {
      if (OB_INVALID_INDEX != it->server_info_indexes_[i])
      {
        ObServerStatus *src_cs = server_manager_->get_server_status(it->server_info_indexes_[i]);
        if (NULL != src_cs && ObServerStatus::STATUS_DEAD != src_cs->status_)
        {
          bool found = false;
          for (int32_t j = 0; j < crow->get_max_replica_count(); ++j)
          {
            const ObTabletReplica &replica = crow->get_replica(j);
            if (src_cs->server_ == replica.cs_)
            {
              found = true;
              break;
            }
          }
          if (!found)
          {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "replica in old roottable not found in the new roottable, range=%s cs=%s",
                      to_cstring(tablet->range_), to_cstring(src_cs->server_));
          }
          ++replica_num;
        }
      }
    } // end for
    int32_t new_replica_num = 0;
    for (int32_t j = 0; j < crow->get_max_replica_count(); ++j)
    {
      const ObTabletReplica &replica = crow->get_replica(j);
      if (0 != replica.cs_.get_port()
          && 0 != replica.cs_.get_ipv4())
      {
        ++new_replica_num;
      }
    }
    if (replica_num != new_replica_num)
    {
      TBSYS_LOG(WARN, "replica num not match, range=%s old=%d new=%d",
                to_cstring(tablet->range_), replica_num, new_replica_num);
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}


void ObRootTableVerifier::run(tbsys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  int err = OB_SUCCESS;
  TBSYS_LOG(INFO, "[NOTICE] root table verifier thread start, tid=%ld", syscall(__NR_gettid));
  while (!_stop)
  {
    if (OB_SUCCESS != (err = check_integrity()))
    {
      TBSYS_LOG(ERROR, "root table verifier not init");
    }
    else if (config_->flag_enable_new_root_table_.get()
             && role_mgr_->is_master())
    {
      if (OB_SUCCESS != (err = verify_schemas_and_tables()))
      {
        TBSYS_LOG(ERROR, "[RT_VERIFIER] verify failed");
      }
    }
    for (int i = 0; i < CHECK_INTERVAL_SEC && !_stop; ++i)
    {
      sleep(1);
    }
  }
  TBSYS_LOG(INFO, "[NOTICE] root table verifier thread exit");
}

