////===================================================================
 //
 // ob_lock_mgr.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-08-30 by Yubai (yubai.lk@taobao.com) 
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 // 
 // Change Log
 //
////====================================================================

#include "ob_lock_mgr.h"
#include "ob_sessionctx_factory.h"

namespace oceanbase
{
  namespace updateserver
  {
    int IRowUnlocker::cb_func(const bool rollback, void *data, BaseSessionCtx &session)
    {
      UNUSED(rollback);
      return unlock((TEValue*)data, session);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    RowExclusiveUnlocker::RowExclusiveUnlocker()
    {
    }

    RowExclusiveUnlocker::~RowExclusiveUnlocker()
    {
    }

    int RowExclusiveUnlocker::unlock(TEValue *value, BaseSessionCtx &session)
    {
      int ret = OB_SUCCESS;
      if (NULL == value)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = value->row_lock.exclusive_unlock(session.get_session_descriptor())))
      {
        TBSYS_LOG(ERROR, "exclusive unlock row fail sd=%u %s value=%p", session.get_session_descriptor(), value->log_str(), value);
      }
      else
      {
        TBSYS_LOG(DEBUG, "exclusive unlock row succ sd=%u %s value=%p", session.get_session_descriptor(), value->log_str(), value);
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    RPLockInfo::RPLockInfo(RPSessionCtx &session_ctx) : ILockInfo(READ_COMMITED),
                                                        session_ctx_(session_ctx),
                                                        row_exclusive_unlocker_(),
                                                        callback_mgr_()
    {
    }

    RPLockInfo::~RPLockInfo()
    {
    }

    int RPLockInfo::on_trans_begin()
    {
      return OB_SUCCESS;
    }

    int RPLockInfo::on_read_begin(const TEKey &key, TEValue &value)
    {
      UNUSED(key);
      UNUSED(value);
      return OB_SUCCESS;
    }

    int RPLockInfo::on_write_begin(const TEKey &key, TEValue &value)
    {
      int ret = OB_SUCCESS;
      uint32_t sd = session_ctx_.get_session_descriptor();
      if (!value.row_lock.is_exclusive_locked_by(sd))
      {
        int64_t session_end_time = session_ctx_.get_session_start_time() + session_ctx_.get_session_timeout();
        session_end_time = (0 <= session_end_time) ? session_end_time : INT64_MAX;
        int64_t stmt_end_time = session_ctx_.get_stmt_start_time() + session_ctx_.get_stmt_timeout();
        stmt_end_time = (0 <= stmt_end_time) ? stmt_end_time : INT64_MAX;
        int64_t end_time = std::min(session_end_time, stmt_end_time);
        if (OB_SUCCESS == (ret = value.row_lock.exclusive_lock(sd, end_time, session_ctx_.is_alive())))
        {
          if (OB_SUCCESS != (ret = callback_mgr_.add_callback_info(session_ctx_, &row_exclusive_unlocker_, &value)))
          {
            row_exclusive_unlocker_.unlock(&value, session_ctx_);
          }
          else
          {
            TBSYS_LOG(DEBUG, "exclusive lock row succ sd=%u %s %s value=%p",
                            session_ctx_.get_session_descriptor(), key.log_str(), value.log_str(), &value);
          }
        }
        else
        {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
        }
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(USER_ERROR, "Exclusive lock conflict \'%s\' for key \'PRIMARY\'", to_cstring(key.row_key));
      }
      return ret;
    }

    void RPLockInfo::on_trans_end()
    {
      // do nothing
    }

    void RPLockInfo::on_precommit_end()
    {
      bool rollback = false; // do not care
      callback_mgr_.callback(rollback, session_ctx_);
    }

    int RPLockInfo::cb_func(const bool rollback, void *data, BaseSessionCtx &session)
    {
      UNUSED(data);
      return callback_mgr_.callback(rollback, session);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    RCLockInfo::RCLockInfo(RWSessionCtx &session_ctx) : ILockInfo(READ_COMMITED),
                                                        session_ctx_(session_ctx),
                                                        row_exclusive_unlocker_(),
                                                        callback_mgr_()
    {
    }

    RCLockInfo::~RCLockInfo()
    {
    }

    int RCLockInfo::on_trans_begin()
    {
      return OB_SUCCESS;
    }

    int RCLockInfo::on_read_begin(const TEKey &key, TEValue &value)
    {
      UNUSED(key);
      UNUSED(value);
      return OB_SUCCESS;
    }

    int RCLockInfo::on_write_begin(const TEKey &key, TEValue &value)
    {
      int ret = OB_SUCCESS;
      uint32_t sd = session_ctx_.get_session_descriptor();
      if (!value.row_lock.is_exclusive_locked_by(sd))
      {
        int64_t session_end_time = session_ctx_.get_session_start_time() + session_ctx_.get_session_timeout();
        session_end_time = (0 <= session_end_time) ? session_end_time : INT64_MAX;
        int64_t stmt_end_time = session_ctx_.get_stmt_start_time() + session_ctx_.get_stmt_timeout();
        stmt_end_time = (0 <= stmt_end_time) ? stmt_end_time : INT64_MAX;
        int64_t end_time = std::min(session_end_time, stmt_end_time);
        if (OB_SUCCESS == (ret = value.row_lock.exclusive_lock(sd, end_time, session_ctx_.is_alive())))
        {
          if (OB_SUCCESS != (ret = callback_mgr_.add_callback_info(session_ctx_, &row_exclusive_unlocker_, &value)))
          {
            row_exclusive_unlocker_.unlock(&value, session_ctx_);
          }
          else
          {
            TBSYS_LOG(DEBUG, "exclusive lock row succ sd=%u %s %s value=%p",
                            session_ctx_.get_session_descriptor(), key.log_str(), value.log_str(), &value);
          }
        }
        else
        {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
        }
      }
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(USER_ERROR, "Exclusive lock conflict \'%s\' for key \'PRIMARY\'", to_cstring(key.row_key));
      }
      return ret;
    }

    void RCLockInfo::on_trans_end()
    {
      // do nothing
    }

    void RCLockInfo::on_precommit_end()
    {
      // do nothing
    }

    int RCLockInfo::cb_func(const bool rollback, void *data, BaseSessionCtx &session)
    {
      UNUSED(data);
      return callback_mgr_.callback(rollback, session);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    LockMgr::LockMgr()
    {
    }

    LockMgr::~LockMgr()
    {
    }

    ILockInfo *LockMgr::assign(const IsolationLevel level, RWSessionCtx &session_ctx)
    {
      void *buffer = NULL;
      ILockInfo *ret = NULL;
      switch (level)
      {
      case NO_LOCK:
        buffer = session_ctx.alloc(sizeof(RPLockInfo));
        if (NULL != buffer)
        {
          ret = new(buffer) RPLockInfo(session_ctx);
        }
        break;
      case READ_COMMITED:
        buffer = session_ctx.alloc(sizeof(RCLockInfo));
        if (NULL != buffer)
        {
          ret = new(buffer) RCLockInfo(session_ctx);
        }
        break;
      default:
        TBSYS_LOG(WARN, "isolation level=%d not support", level);
        break;
      }
      if (NULL != ret)
      {
        int tmp_ret = session_ctx.add_callback_info(session_ctx, ret, NULL);
        if (OB_SUCCESS != tmp_ret)
        {
          TBSYS_LOG(WARN, "add lock callback info to session ctx fail ret=%d", tmp_ret);
          ret = NULL;
        }
      }
      return ret;
    }
  }
}

