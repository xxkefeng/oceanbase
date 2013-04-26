/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_root_table_verifier.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROOT_TABLE_VERIFIER_H
#define _OB_ROOT_TABLE_VERIFIER_H 1
#include <tbsys.h>
#include "ob_root_table2.h"
#include "ob_chunk_server_manager.h"
#include "ob_ups_manager.h"
#include "ob_root_config.h"
#include "ob_root_rpc_stub.h"
#include "ob_root_table2.h"
#include "common/roottable/ob_root_table3.h"
#include "common/ob_schema_service.h"
#include "common/ob_schema.h"
#include "common/roottable/ob_root_table_service.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootTableVerifier: public tbsys::CDefaultRunnable
    {
      public:
        ObRootTableVerifier();
        virtual ~ObRootTableVerifier();

        void set_components(ObRootTable2 &old_rt, tbsys::CRWLock &root_table_rwlock_,
                            common::ObSchemaService &schema_service,
                            common::ObSchemaManagerV2 &schema_manager,
                            ObChunkServerManager &server_manager,
                            ObUpsManager &ups_manager,
                            ObRootConfig &config,
                            ObRootRpcStub &rpc_stub,
                            common::ObRootTableService &rt_service,
                            common::ObRoleMgr &role_manager);

        virtual void run(tbsys::CThread* thread, void* arg);
      private:
        int check_integrity();
        int verify_schemas_and_tables();
        int verify_table_schema(const common::ObString &tname);
        int verify_root_table(const uint64_t tid);
        int verify_tablet(ObRootTable2::const_iterator it, const common::ObTabletInfo* tablet,
                          const common::ObRootTable3::Value* crow);
        // disallow copy
        ObRootTableVerifier(const ObRootTableVerifier &other);
        ObRootTableVerifier& operator=(const ObRootTableVerifier &other);
      private:
        static const int64_t CHECK_INTERVAL_SEC = 600; // 10min
        // data members
        ObRootTable2 *old_rt_;
        tbsys::CRWLock *root_table_rwlock_;
        common::ObSchemaService *schema_service_;
        common::ObSchemaManagerV2 *schema_manager_;
        ObChunkServerManager *server_manager_;
        ObUpsManager *ups_manager_;
        ObRootConfig *config_;
        ObRootRpcStub *rpc_stub_;
        common::ObRootTableService *rt_service_;
        common::ObRoleMgr *role_mgr_;
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_TABLE_VERIFIER_H */

