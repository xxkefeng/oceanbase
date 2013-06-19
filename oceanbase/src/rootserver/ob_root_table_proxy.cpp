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
  *   zhidong <xielun.szd@taobao.com>
  *     - some work details if you want
  */

#include "ob_root_table_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

void ObRootTableProxy::set_server_manager(ObChunkServerManager * server_manager)
{
  chunk_server_manager_ = server_manager;
}

