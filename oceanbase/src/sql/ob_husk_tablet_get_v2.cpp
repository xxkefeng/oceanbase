/*
 * (C) 1999-2013 Alibaba Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 * Version:  ob_husk_tablet_get_v2.cpp,  05/31/2013 12:55:39 PM Yu Huang Exp $
 * 
 * Author:  
 *   Huang Yu <xiaochu.yh@alipay.com>
 * Description:  
 *   
 * 
 */

#include "ob_husk_tablet_get_v2.h"
using namespace oceanbase::sql;

DEFINE_SERIALIZE(ObHuskTabletGetV2)
{
  int ret = get_param_.serialize(buf, buf_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "failed to serialize get_param_, ret=%d", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObHuskTabletGetV2)
{
  int ret = get_param_.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "failed to deserialize get_param_, ret=%d", ret);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObHuskTabletGetV2)
{
  return get_param_.get_serialize_size();
}




