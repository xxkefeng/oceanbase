#include "ob_bootstrap_state.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

ObBootState::ObBootState():state_(OB_BOOT_NO_META)
{
}

void ObBootState::set_boot_ok()
{
  ObSpinLockGuard guard(lock_);
  state_ = OB_BOOT_OK;
}

ObBootState::State ObBootState::get_boot_state() const
{
  return state_;
}

void ObBootState::set_boot_strap()
{
  ObSpinLockGuard guard(lock_);
  state_ = OB_BOOT_STRAP;
}

void ObBootState::set_boot_recover()
{
  ObSpinLockGuard guard(lock_);
  state_ = OB_BOOT_RECOVER;
}

bool ObBootState::can_boot_strap() const
{
  ObSpinLockGuard guard(lock_);
  return OB_BOOT_NO_META == state_;
}

bool ObBootState::can_boot_recover() const
{
  ObSpinLockGuard guard(lock_);
  return OB_BOOT_NO_META == state_;
}

const char* ObBootState::to_cstring() const
{
  ObSpinLockGuard guard(lock_);
  const char* ret = "UNKNOWN";
  switch(state_)
  {
    case OB_BOOT_NO_META:
      ret = "BOOT_NO_META";
      break;
    case OB_BOOT_OK:
      ret = "BOOT_OK";
      break;
    case OB_BOOT_STRAP:
      ret = "BOOT_STRAP";
      break;
    case OB_BOOT_RECOVER:
      ret = "BOOT_RECOVER";
      break;
    default:
      break;
  }
  return ret;
}

