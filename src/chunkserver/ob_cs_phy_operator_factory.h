/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   Huang Yu <xiaochu.yh@alipay.com>
 *     - some work details if you want
 */
#ifndef __OB_CS_PHY_OPERATOR_FACTORY_H__
#define __OB_CS_PHY_OPERATOR_FACTORY_H__
#include "common/ob_sql_ups_rpc_proxy.h"
#include "sql/ob_phy_operator_factory.h"
#include "ob_tablet_manager.h"
#include "ob_cs_plan_executor.h"

namespace oceanbase
{
  namespace chunkserver
  {
    class ObCsPhyOperatorFactory: public sql::ObPhyOperatorFactory
    {
      public:
        ObCsPhyOperatorFactory():tablet_mgr_(NULL), ups_rpc_proxy_(NULL), inited_(false), plan_context_(NULL),schema_mgr_(NULL) {}
        virtual ~ObCsPhyOperatorFactory(){}
        virtual sql::ObPhyOperator *get_one(sql::ObPhyOperatorType phy_operator_type, common::ModuleArena &allocator);
        inline int set_param(ObPlanContext* context, ObTabletManager *tablet_mgr,
          ObSqlUpsRpcProxy *ups_rpc_proxy, const ObSchemaManagerV2 *schema_mgr);
      private:
        ObTabletManager *tablet_mgr_;
        ObSqlUpsRpcProxy *ups_rpc_proxy_;
        bool inited_;
        ObPlanContext* plan_context_;
        const ObSchemaManagerV2 *schema_mgr_;
    };

    inline int ObCsPhyOperatorFactory::set_param(ObPlanContext* context,
      ObTabletManager *tablet_mgr, ObSqlUpsRpcProxy *ups_rpc_proxy, const ObSchemaManagerV2 *schema_mgr)
    {
      int ret = OB_SUCCESS;
      if (NULL == context || NULL == tablet_mgr || NULL == ups_rpc_proxy || NULL == schema_mgr)
      {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(ERROR, "context=%p tablet_mgr=%p ups_rpc_proxy=%p schema_mgr=%p must not NULL",
          context, tablet_mgr, ups_rpc_proxy, schema_mgr);
      }
      else
      {
        plan_context_ = context;
        ups_rpc_proxy_ = ups_rpc_proxy;
        tablet_mgr_ = tablet_mgr;
        inited_ = true;
        schema_mgr_ = schema_mgr;
      }
      return ret;
    }
  }; // end namespace chunkserver
}; // end namespace oceanbase

#endif /* __OB_CS_PHY_OPERATOR_FACTORY_H__ */

