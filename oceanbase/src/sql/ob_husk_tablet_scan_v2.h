/*
 * (C) 1999-2013 Alibaba Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_husk_tablet_scan_v2.h,  05/31/2013 10:07:30 AM Yu Huang Exp $
 * 
 * Author:  
 *   Huang Yu <xiaochu.yh@alipay.com>
 * Description:  
 *   Direct Chunkserver to make a CS scan
 * 
 */
#ifndef _OB_HUSK_TABLET_SCAN_V2_H_
#define _OB_HUSK_TABLET_SCAN_V2_H_
#include "ob_phy_operator.h"
#include "common/ob_define.h"
#include "ob_sql_scan_simple_param.h"
#include "ob_plan_context.h"

namespace oceanbase
{
  namespace sql
  {
    using namespace oceanbase::common;

    class ObHuskTabletScanV2 : public ObPhyOperator 
    {
      public:
        ObHuskTabletScanV2()
        {
        }

        virtual ~ObHuskTabletScanV2()
        {
        }

        virtual int open()
        {
          return OB_NOT_IMPLEMENT;
        }
        
        virtual int close()
        {
          return OB_NOT_IMPLEMENT;
        }
        
        virtual int get_next_row(const common::ObRow *&row)
        {
          UNUSED(row);
          return OB_NOT_IMPLEMENT;
        }
        
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator)
        {
          UNUSED(child_idx);
          UNUSED(child_operator);
          return OB_NOT_IMPLEMENT;
        }
        
        virtual int64_t to_string(char* buf, const int64_t buf_len) const
        {
          int64_t pos = 0;
          databuff_printf(buf, buf_len, pos, "HuskTabletScan()\n");
          return pos;
        }
        
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const
        {
          UNUSED(row_desc);
          return OB_NOT_IMPLEMENT;
        }

        virtual enum ObPhyOperatorType get_type() const
        {
          return PHY_TABLET_SCAN_V2;
        }

        ObSqlScanSimpleParam& get_scan_param()
        {
          return scan_param_;
        }
        
        const ObSqlScanSimpleParam& get_scan_param() const 
        {
          return scan_param_;
        }
        VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
      protected:
        ObSqlScanSimpleParam scan_param_;
    };
  }
}

#endif //_OB_HUSK_TABLET_SCAN_V2_H_
