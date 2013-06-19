/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   sunzhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#ifndef OB_ROOT_BOOTSTRAP_STATE_H_
#define OB_ROOT_BOOTSTRAP_STATE_H_

#include "common/ob_spin_lock.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObBootState
    {
    public:
      enum State
      {
        OB_BOOT_NO_META = 0,
        OB_BOOT_OK = 1,
        OB_BOOT_STRAP = 2,
        OB_BOOT_RECOVER = 3,
      };
    public:
      ObBootState();
      void set_boot_ok();
      void set_boot_strap();
      void set_boot_recover();
      bool can_boot_strap() const;
      bool can_boot_recover() const;
      const char * to_cstring() const;
      ObBootState::State get_boot_state() const;
    private:
      mutable common::ObSpinLock lock_;
      State state_;
    };
  }
}

#endif // OB_ROOT_BOOTSTRAP_STATE_H_
