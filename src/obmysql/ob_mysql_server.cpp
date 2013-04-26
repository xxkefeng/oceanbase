#include "easy_io.h"
#include "ob_mysql_server.h"
#include "ob_mysql_state.h"
#include "common/ob_tbnet_callback.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_atomic.h"
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_trace_log.h"
#include "ob_mysql_util.h"
#include "packet/ob_mysql_resheader_packet.h"
#include "packet/ob_mysql_eof_packet.h"
#include "packet/ob_mysql_ok_packet.h"
#include "packet/ob_mysql_row_packet.h"
#include "packet/ob_mysql_field_packet.h"
#include "packet/ob_mysql_error_packet.h"
#include "packet/ob_mysql_spr_packet.h"
#include "sql/ob_sql_context.h"
#include "common/ob_schema_manager.h"
#include "common/hash/ob_hashutils.h"
#include "common/ob_privilege.h"
#include "common/ob_row.h"
#include "common/location/ob_tablet_location_cache_proxy.h"
#include "common/ob_obj_cast.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::sql;

namespace oceanbase
{
  namespace obmysql
  {
    __thread uint8_t number = 0;
    ObMySQLServer::ObMySQLServer(): io_thread_count_(1), work_thread_count_(10),
                                    task_queue_size_(100), port_(3100),
                                    stop_(false), eio_(NULL),
                                    self_()
    {
    }

    ObMySQLServer::~ObMySQLServer()
    {
      if (NULL != eio_)
      {
        easy_eio_destroy(eio_);
        eio_ = NULL;
      }
    }
    common::DefaultBlockAllocator* ObMySQLServer::get_block_allocator()
    {
      return &block_allocator_;
    }

    void ObMySQLServer::set_config(const mergeserver::ObMergeServerConfig *config)
    {
      config_ = config;
    }

    void ObMySQLServer::set_env(MergeServerEnv &env)
    {
      env_ = env;
    }

    bool ObMySQLServer::has_too_many_sessions() const
    {
      bool ret = false;
      static const int64_t MS_MEM_RESERVED_SIZE = 1024*1024*1024L; // 1GB
      if ((session_mgr_.size() * ObSQLSessionInfo::APPROX_MEM_USAGE_PER_SESSION)
          >= (ob_get_memory_size_limit() - MS_MEM_RESERVED_SIZE))
      {
        TBSYS_LOG(WARN, "there are too many sessions, session_count=%ld mem_limit=%ld",
                  session_mgr_.size(), ob_get_memory_size_limit());
        ret = true;
      }
      return ret;
    }

    int ObMySQLServer::initialize()
    {
      int ret = OB_SUCCESS;
      TBSYS_LOG(DEBUG, "initialize obmsql");

      if (OB_SUCCESS == ret)
      {
        config_->print();
      }

      //init server handler
      if(OB_SUCCESS == ret)
      {
        memset(&handler_, 0, sizeof(easy_io_handler_pt));
        handler_.encode = ObMySQLCallback::encode;
        handler_.decode = ObMySQLCallback::decode;
        handler_.process = ObMySQLCallback::process;
        handler_.get_packet_id = ObMySQLCallback::get_packet_id;
        handler_.on_disconnect = ObMySQLCallback::on_disconnect;
        handler_.on_connect = ObMySQLCallback::on_connect;
        handler_.cleanup = ObMySQLCallback::clean_up;
        handler_.user_data = this;
      }

      //set server params read from config file
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = set_io_thread_count((int32_t)config_->obmysql_io_thread_count)))
        {
          TBSYS_LOG(ERROR, "set obmysql io_thread_count=%s failed",
                    config_->obmysql_io_thread_count.str());
        }
        else if (OB_SUCCESS != (ret = set_work_thread_count((int32_t)config_->obmysql_work_thread_count)))
        {
          TBSYS_LOG(ERROR, "set obmysql work_thread_count=%s failed",
                    config_->obmysql_work_thread_count.str());
        }
        else if (OB_SUCCESS != (ret = set_task_queue_size((int32_t)config_->obmysql_task_queue_size)))
        {
          TBSYS_LOG(ERROR, "set obmysql task_queue_size=%s failed",
                    config_->obmysql_task_queue_size.str());
        }
        else if (OB_SUCCESS != (ret = set_port((int32_t)config_->obmysql_port)))
        {
          TBSYS_LOG(ERROR, "set obmysql port=%s failed", config_->obmysql_port.str());
        }
      }

      //init session mgr
      if (OB_SUCCESS == ret)
      {
        ret = session_mgr_.create(hash::cal_next_prime(512));
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "create session mgr failed");
        }
      }
      login_handler_.set_obmysql_server(this);
      //start work thread
      if (OB_SUCCESS == ret)
      {
        if (work_thread_count_ > 0)
        {
          IpPort ip_port;
          uint32_t local_ip = tbsys::CNetUtil::getLocalAddr(config_->devname);
          ip_port.ip_ = static_cast<uint16_t>(local_ip & 0x0000FFFF);
          ip_port.port_ = static_cast<uint16_t>(port_);
          self_.set_ipv4_addr(local_ip, port_);
          command_queue_thread_.set_self_to_thread_queue(self_);
          command_queue_thread_.set_ip_port(ip_port);
          command_queue_thread_.setThreadParameter(work_thread_count_, this, NULL);
          TBSYS_LOG(INFO, "obmysql work thread count=%d", work_thread_count_);
          command_queue_thread_.start();
        }
        //start work thread for close request only
        IpPort ip_port;
        uint32_t local_ip = tbsys::CNetUtil::getLocalAddr(config_->devname);
        ip_port.ip_ = static_cast<uint16_t>(local_ip & 0x0000FFFF);
        ip_port.port_ = static_cast<uint16_t>(port_);
        self_.set_ipv4_addr(local_ip, port_);
        close_command_queue_thread_.set_self_to_thread_queue(self_);
        close_command_queue_thread_.set_ip_port(ip_port);
        close_command_queue_thread_.setThreadParameter(1, this, NULL);
        TBSYS_LOG(INFO, "start thread to handle close request");
        close_command_queue_thread_.start();
      }
      return ret;
    }
    int ObMySQLServer::get_privilege(ObPrivilege * p_privilege)
    {
      int ret = OB_SUCCESS;
      int err = OB_SUCCESS;
      ObString get_users = ObString::make_string("SELECT u.user_name,u.user_id,u.pass_word,u.info,u.priv_all,u.priv_alter,u.priv_create,u.priv_create_user,u.priv_delete,u.priv_drop,u.priv_grant_option,u.priv_insert,u.priv_update,u.priv_select,u.priv_replace,u.is_locked  FROM __all_user u");
      ObString get_table_privileges = ObString::make_string("SELECT t.user_id,t.table_id,t.priv_all,t.priv_alter,t.priv_create,t.priv_create_user,t.priv_delete,t.priv_drop,t.priv_grant_option,t.priv_insert,t.priv_update,t.priv_select, t.priv_replace FROM __all_table_privilege t");
      ObString get_privilege_version = ObString::make_string("SELECT value FROM __all_sys_stat where name='ob_current_privilege_version'");
      ObSqlContext context;
      ObSQLSessionInfo session;
      if (OB_SUCCESS != (ret = session.init(block_allocator_)))
      {
        TBSYS_LOG(WARN, "failed to init context, err=%d", ret);
        return ret;
      }
      context.session_info_ = &session;
      int64_t schema_version = 0;
      while (!stop_)
      {
        schema_version = env_.schema_mgr_->get_latest_version();
        context.schema_manager_ = env_.schema_mgr_->get_user_schema(schema_version);
        if (NULL == context.schema_manager_)
        {
          TBSYS_LOG(INFO, "privilege related table schema not ready,schema_version=%ld", schema_version);
          sleep(1);
        }
        else
        {
          if (NULL == context.schema_manager_->get_table_schema(OB_ALL_USER_TABLE_NAME)
              ||
              NULL == context.schema_manager_->get_table_schema(OB_ALL_TABLE_PRIVILEGE_TABLE_NAME))
          {
            err = env_.schema_mgr_->release_schema(context.schema_manager_);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(WARN, "release schema manager failed,ret=%d", err);
            }
            context.schema_manager_ = NULL;
            TBSYS_LOG(INFO, "privilege related table schema not ready,schema_version=%ld", schema_version);
            sleep(1);
          }
          else
          {
            TBSYS_LOG(INFO, "privilege related table schema ready");
            break;
          }
        }
      }
      context.cache_proxy_ = env_.cache_proxy_;    // thread safe singleton
      context.async_rpc_ = env_.async_rpc_;        // thread safe singleton
      context.merger_rpc_proxy_ = env_.rpc_proxy_; // thread safe singleton
      context.rs_rpc_proxy_ = env_.root_rpc_;      // thread safe singleton
      context.merge_service_ = env_.merge_service_;
      context.disable_privilege_check_ = true;
      context.stat_mgr_ = env_.stat_mgr_; // thread safe
      // direct_execute don't check privilege if flag is true, it means we are in system bootstrap progress
      // 获取到了权限表相关schema的时候有可能__users表里面还没有数据
      while (!stop_)
      {
        ObMySQLResultSet result;
        if (OB_SUCCESS != (ret = result.init()))
        {
          TBSYS_LOG(ERROR, "init result set failed, ret = %d", ret);
        }
        else
        {
          context.session_info_->get_mutex().lock(); // !!!lock
          context.session_info_->get_parser_mem_pool().reuse();
          context.session_info_->get_transformer_mem_pool().start_batch_alloc();
          ret = ObSql::direct_execute(get_users, result, context);
          if (OB_SUCCESS == ret)
          {
            if (OB_SUCCESS != (ret = result.open()))
            {
              TBSYS_LOG(WARN, "failed to open result set, ret=%d", ret);
              // release resouce as early as possible, in case YOU FORGET
              err = result.close();
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "failed to close result set,ret=%d", err);
              }
            }
            else
            {
              OB_ASSERT(result.is_with_rows() == true);
              ObMySQLRow row;
              bool has_data = false;
              while (OB_SUCCESS == ret)
              {
                if (OB_SUCCESS != (ret = result.next_row(row)))
                {
                  if (ret == OB_ITER_END)
                  {
                    ret = OB_SUCCESS;
                  }
                  else
                  {
                    TBSYS_LOG(ERROR, "next row from ObMySQLResultSet failed, ret=%d", ret);
                  }
                  break;
                }
                else if(OB_SUCCESS != (ret = p_privilege->add_users_entry(*(row.get_inner_row()))))
                {
                  TBSYS_LOG(ERROR, "add privilege row  to system failed,ret=%d", ret);
                }
                else
                {
                  TBSYS_LOG(INFO, "get one user row, row=%s", to_cstring(*(row.get_inner_row())));
                  has_data = true;
                }
              } // end while
              err = result.close();
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(WARN, "failed to close result set,ret=%d", err);
              }
              if (false == has_data)
              {
                TBSYS_LOG(INFO, "no data in __users,retry");
                cleanup_sql_env(context, result);
                continue;
              }
            }
          }
          else
          {
            TBSYS_LOG(ERROR,"execute sql %.*s, failed,ret=%d", get_users.length(), get_users.ptr(), ret);
          }
          cleanup_sql_env(context, result);
        }
        if (OB_SUCCESS == ret)
        {
          ObMySQLResultSet result2;
          if (OB_SUCCESS != (ret = result2.init()))
          {
            TBSYS_LOG(ERROR, "init result set failed, ret = %d", ret);
          }
          else
          {
            context.session_info_->get_mutex().lock(); // !!!lock
            context.session_info_->get_parser_mem_pool().reuse();
            context.session_info_->get_transformer_mem_pool().start_batch_alloc();
            // direct_execute don't check privilege if flag is true, it means we are in system bootstrap progress
            ret = ObSql::direct_execute(get_table_privileges, result2, context);
            if (OB_SUCCESS == ret)
            {
              if (OB_SUCCESS != (ret = result2.open()))
              {
                TBSYS_LOG(WARN, "failed to open result set, ret=%d", ret);
                err = result2.close();
                if (OB_SUCCESS != err)
                {
                  TBSYS_LOG(WARN, "failed to close result set,ret=%d", err);
                }
              }
              else
              {
                OB_ASSERT(result2.is_with_rows() == true);
                ObMySQLRow row;
                while (OB_SUCCESS == ret)
                {
                  if (OB_SUCCESS != (ret = result2.next_row(row)))
                  {
                    if (ret == OB_ITER_END)
                    {
                      ret = OB_SUCCESS;
                    }
                    else
                    {
                      TBSYS_LOG(ERROR, "next row from ObMySQLResultSet failed, ret=%d", ret);
                    }
                    break;
                  }
                  else if(OB_SUCCESS != (ret = p_privilege->add_table_privileges_entry(*(row.get_inner_row()))))
                  {
                    TBSYS_LOG(ERROR, "add privilege row  to system failed,ret=%d", ret);
                  }
                  else
                  {
                    // do nothing
                  }
                }
                err = result2.close();
                if (OB_SUCCESS != err)
                {
                  TBSYS_LOG(WARN, "failed to close result set,ret=%d", err);
                }
              }
            }
            else
            {
              TBSYS_LOG(ERROR,"execute sql %.*s, failed,ret=%d", get_table_privileges.length(), get_table_privileges.ptr(), ret);
            }
            cleanup_sql_env(context, result2);
          }
        }
        if (OB_SUCCESS == ret)
        {
          ObMySQLResultSet result3;
          if (OB_SUCCESS != (ret = result3.init()))
          {
            TBSYS_LOG(ERROR, "init result set failed, ret = %d", ret);
          }
          else
          {
            context.session_info_->get_mutex().lock(); // !!!lock
            context.session_info_->get_parser_mem_pool().reuse();
            context.session_info_->get_transformer_mem_pool().start_batch_alloc();
            // direct_execute don't check privilege if flag is true, it means we are in system bootstrap progress
            ret = ObSql::direct_execute(get_privilege_version, result3, context);
            if (OB_SUCCESS == ret)
            {
              if (OB_SUCCESS != (ret = result3.open()))
              {
                TBSYS_LOG(WARN, "failed to open result set, ret=%d", ret);
                err = result3.close();
                if (OB_SUCCESS != err)
                {
                  TBSYS_LOG(WARN, "failed to close result set,ret=%d", err);
                }
              }
              else
              {
                OB_ASSERT(result3.is_with_rows() == true);
                ObMySQLRow row;
                const common::ObObj *pcell = NULL;
                // not used
                uint64_t table_id = OB_INVALID_ID;
                uint64_t column_id = OB_INVALID_ID;
                ObExprObj in;
                ObExprObj out;
                while (OB_SUCCESS == ret)
                {
                  if (OB_SUCCESS != (ret = result3.next_row(row)))
                  {
                    if (ret == OB_ITER_END)
                    {
                      ret = OB_SUCCESS;
                    }
                    else
                    {
                      TBSYS_LOG(ERROR, "next row from ObMySQLResultSet failed, ret=%d", ret);
                    }
                    break;
                  }
                  else if(OB_SUCCESS != (ret = row.get_inner_row()->raw_get_cell(0, pcell, table_id, column_id)))
                  {
                    TBSYS_LOG(ERROR, "raw_get_cell failed,ret=%d", ret);
                  }
                  else
                  {
                    in.assign(*pcell);
                    ObObjCastParams params;
                    if (OB_SUCCESS != (ret = OB_OBJ_CAST[ObVarcharType][ObIntType](params, in, out)))
                    {
                      TBSYS_LOG(ERROR, "varchar cast to int failed, ret=%d", ret);
                    }
                    else
                    {
                      p_privilege->set_version(out.get_int());
                    }
                  }
                }
                err = result3.close();
                if (OB_SUCCESS != err)
                {
                  TBSYS_LOG(WARN, "failed to close result set,ret=%d", err);
                }
                if (OB_SUCCESS == ret)
                {
                  TBSYS_LOG(INFO, "privilege related table ready");
                }
                break;
              }
            }
            else
            {
              TBSYS_LOG(ERROR,"execute sql %.*s, failed,ret=%d", get_privilege_version.length(), get_privilege_version.ptr(), ret);
            }
            cleanup_sql_env(context, result3);
          }
        }
      }// while
      if (NULL != context.schema_manager_)
      {
        env_.schema_mgr_->release_schema(context.schema_manager_);
        context.schema_manager_ = NULL;
      }
      return ret;
    }
    int ObMySQLServer::start(bool need_wait)
    {
      int ret = OB_SUCCESS;
      int rc = EASY_OK;
      easy_listen_t* listen = NULL;
      TBSYS_LOG(DEBUG, "obmysql server start....");
      eio_ = easy_eio_create(eio_, io_thread_count_);
      eio_->do_signal = 0;
      eio_->force_destroy_second = OB_CONNECTION_FREE_TIME_S;
      eio_->checkdrc = 1;
      eio_->support_ipv6 = 0;
      if (NULL == eio_)
      {
        ret = OB_ERROR;
        TBSYS_LOG(ERROR, "easy_eio_create error");
      }
      else
      {
        TBSYS_LOG(DEBUG, "obmysql easy_eio_create successful");
        eio_->tcp_defer_accept = 0;
        ret = initialize();
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "initialize failed ret is %d", ret);
        }
        else
        {
          if (NULL == (listen = easy_connection_add_listen(eio_, NULL, port_, &handler_)))
          {
            TBSYS_LOG(ERROR, "easy_connection_add_listen error, port: %d, %s", port_, strerror(errno));
            ret = OB_SERVER_LISTEN_ERROR;
          }
          else
          {
            TBSYS_LOG(INFO, "obmysql listen start, port = %d", port_);
          }
        }

        //start io thread
        if (OB_SUCCESS == ret)
        {
          rc = easy_eio_start(eio_);
          if (EASY_OK == rc)
          {
            ret = OB_SUCCESS;
            TBSYS_LOG(INFO, "obmysql start io thread");
          }
          else
          {
            ret = OB_ERROR;
            TBSYS_LOG(ERROR, "obmysq start failed, call easy_eio_start return %d", rc);
          }

          if (OB_SUCCESS == ret)
          {
            ObPrivilege *p_privilege = reinterpret_cast<ObPrivilege*>(ob_malloc(sizeof(ObPrivilege)));
            if (NULL == p_privilege)
            {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              TBSYS_LOG(ERROR, "ob malloc ObPrivilege failed, ret=%d", ret);
            }
            if (OB_SUCCESS == ret)
            {
              p_privilege = new (p_privilege) ObPrivilege();
              if (OB_SUCCESS != (ret = p_privilege->init()))
              {
                TBSYS_LOG(ERROR, "init ObPrivilege failed, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = get_privilege(p_privilege)))
              {
                TBSYS_LOG(ERROR, "get privilege failed, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = env_.privilege_mgr_->renew_privilege(*p_privilege)))
              {
                // @首先通过心跳更新到了privilege，然后才取得__all_sys_stat表的内容，会造成这种情况
                if (ret == OB_ERR_OLDER_PRIVILEGE_VERSION)
                {
                  TBSYS_LOG(INFO, "initialize privilege information success");
                  login_handler_.set_privilege_manager(env_.privilege_mgr_);
                  ret = OB_SUCCESS;
                }
                else
                {
                  TBSYS_LOG(ERROR, "renew privilege failed, ret=%d", ret);
                }
              }
              else
              {
                TBSYS_LOG(INFO, "initialize privilege information success");
                login_handler_.set_privilege_manager(env_.privilege_mgr_);
              }
            }

            //ret = start_service();
          }

          if (OB_SUCCESS == ret)
          {
            if (need_wait)
            {
              easy_eio_wait(eio_);
            }
          }
        }

        if (need_wait)
        {
          stop();
        }
      }
      return ret;
    }

    void ObMySQLServer::stop()
    {
      if (!stop_)
      {
        stop_ = true;
        destroy();
        if (eio_ != NULL && NULL != eio_->pool)
        {
          easy_eio_stop(eio_);
          easy_eio_wait(eio_);
          easy_eio_destroy(eio_);
          eio_ = NULL;
        }
        TBSYS_LOG(INFO, "server stoped.");
      }
    }

    void ObMySQLServer::wait_for_queue()
    {
      if (work_thread_count_ > 0)
      {
        command_queue_thread_.wait();
        close_command_queue_thread_.wait();
      }
    }

    void ObMySQLServer::wait()
    {
      easy_eio_wait(eio_);
    }

    void ObMySQLServer::destroy()
    {
      if (work_thread_count_ > 0)
      {
        command_queue_thread_.stop();
        close_command_queue_thread_.stop();
        wait_for_queue();
      }
    }

    int ObMySQLServer::login_handler(easy_connection_t* c)
    {
      int ret = OB_SUCCESS;
      ObSQLSessionInfo *session = NULL;
      if (OB_SUCCESS != (ret = login_handler_.login(c, session)))
      {
        TBSYS_LOG(ERROR, "handler login failed ret is %d", ret);
      }
      return ret;
    }

    int ObMySQLServer::load_system_params(sql::ObSQLSessionInfo& session)
    {
      int ret = OB_SUCCESS;
      ObSqlContext context;
      ObMySQLResultSet result;
      char sql_str_buf[OB_MAX_VARCHAR_LENGTH];
      snprintf(sql_str_buf, OB_MAX_VARCHAR_LENGTH, "SELECT name, data_type, value FROM %s;", OB_ALL_SYS_PARAM_TABLE_NAME);
      ObString get_params_sql_str = ObString::make_string(sql_str_buf);

      int64_t schema_version = env_.schema_mgr_->get_latest_version();
      session.get_mutex().lock(); // !!!lock
      context.session_info_ = &session;
      context.session_info_->set_current_result_set(&result);
      context.cache_proxy_ = env_.cache_proxy_;    // thread safe singleton
      context.async_rpc_ = env_.async_rpc_;        // thread safe singleton
      context.merger_rpc_proxy_ = env_.rpc_proxy_; // thread safe singleton
      context.merge_service_ = env_.merge_service_;
      context.rs_rpc_proxy_ = env_.root_rpc_;      // thread safe singleton
      context.disable_privilege_check_ = true;
      context.stat_mgr_ = env_.stat_mgr_;
      if ((context.schema_manager_ = env_.schema_mgr_->get_user_schema(schema_version)) == NULL
        || context.session_info_->get_parser_mem_pool().reuse() != OB_SUCCESS
        || context.session_info_->get_transformer_mem_pool().start_batch_alloc() != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "Fail to get user schema. schema_version=%ld", schema_version);
        ret = OB_ERROR;
      }
      else if ((ret = result.init()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "Init result set failed, ret = %d", ret);
      }
      else if ((ret = ObSql::direct_execute(get_params_sql_str, result, context)) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR,"Execute sql: %s, failed, ret=%d", sql_str_buf, ret);
      }
      else if ((ret = result.open()) != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "Failed to open result set, ret=%d", ret);
        if (result.close() != OB_SUCCESS)
        {
          TBSYS_LOG(WARN, "Failed to close result set");
        }
      }
      else if (result.is_with_rows() != true)
      {
        TBSYS_LOG(WARN, "Sql: '%s', get nothing, ret=%d", sql_str_buf, ret);
      }
      else
      {
        ObMySQLRow sql_row;
        while ((ret = result.next_row(sql_row)) == OB_SUCCESS)
        {
          int ret = OB_SUCCESS;
          const ObRow *row = sql_row.get_inner_row();
          const ObObj *name = NULL;
          const ObObj *type_value = NULL;
          const ObObj *value = NULL;
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          ObString name_str;
          ObObj type;
          int64_t type_val;
          if ((ret = row->raw_get_cell(0, name, table_id, column_id)) != OB_SUCCESS
            || (ret = name->get_varchar(name_str)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "Get system variable name failed, ret=%d", ret);
            break;
          }
          else if ((ret = row->raw_get_cell(1, type_value, table_id, column_id)) != OB_SUCCESS
            || (ret = type_value->get_int(type_val)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "Get system variable type failed, ret=%d", ret);
            break;
          }
          else if ((ret = row->raw_get_cell(2, value, table_id, column_id)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "Get system variable value failed, ret=%d", ret);
            break;
          }
          type.set_type(static_cast<ObObjType>(type_val));
          if ((ret = session.load_system_variable(name_str, type, *value)) != OB_SUCCESS)
          {
            TBSYS_LOG(ERROR, "Add system variable to session failed, ret=%d", ret);
          }
        } // end while
        if (ret == OB_ITER_END)
        {
          ret = OB_SUCCESS;
        }
        else if (ret != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "Get next row from ObMySQLResultSet failed, ret=%d", ret);
        }
        if (result.close() != OB_SUCCESS)
        {
          TBSYS_LOG(ERROR, "Failed to close result set");
        }
      }
      cleanup_sql_env(context, result);
      if (NULL != context.schema_manager_)
      {
        env_.schema_mgr_->release_schema(context.schema_manager_);
        context.schema_manager_ = NULL;
      }
      return ret;
    }

    int ObMySQLServer::set_io_thread_count(const int32_t io_thread_count)
    {
      int ret = OB_SUCCESS;

      if (io_thread_count <= 0)
      {
        TBSYS_LOG(ERROR, "invalid argument io_thread_count = %d", io_thread_count);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        io_thread_count_ = io_thread_count;
      }
      return ret;
    }

    int ObMySQLServer::set_work_thread_count(const int32_t work_thread_count)
    {
      int ret = OB_SUCCESS;

      if (work_thread_count <= 0)
      {
        TBSYS_LOG(ERROR, "invalid argument work_thread_count = %d", work_thread_count);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        work_thread_count_ = work_thread_count;
      }
      return ret;
    }

    int ObMySQLServer::set_task_queue_size(const int32_t task_queue_size)
    {
      int ret = OB_SUCCESS;

      if (task_queue_size <= 0)
      {
        TBSYS_LOG(ERROR, "invalid argument task_queue_size = %d", task_queue_size);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        task_queue_size_ = task_queue_size;
      }
      return ret;
    }

    int ObMySQLServer::set_port(const int32_t port)
    {
      int ret = OB_SUCCESS;

      if (port <= 0)
      {
        TBSYS_LOG(ERROR, "invalid argument port = %d", port);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        port_ = port;
      }
      return ret;
    }

    int ObMySQLServer::submit_session_delete_task(ObMySQLSessionKey key)
    {
      int ret = OB_SUCCESS;
      ObMySQLCommandPacket *del_packet = NULL;
      int64_t length = 32 + sizeof(ObMySQLCommandPacket);
      char buffer[length];
      int64_t pos = 0;
      del_packet = new(buffer)ObMySQLCommandPacket();
      del_packet->set_type(COM_DELETE_SESSION);

      char *data = buffer + sizeof(ObMySQLCommandPacket);

      if (OB_SUCCESS != ObMySQLUtil::store_length(data, length, key, pos))
      {
        TBSYS_LOG(ERROR, "serialize key.fd_ failed, buffer=%p, len=%ld, key=%d, pos=%ld",
                  data, length, key, pos);
      }
      del_packet->set_command(data, 32);

      if (OB_SUCCESS == ret)
      {
        if (!close_command_queue_thread_.push(del_packet, (int)config_->obmysql_task_queue_size, false))
        {
          TBSYS_LOG(WARN, "submit async task to thread queue fail task_queue_size=%s",
                    config_->obmysql_task_queue_size.str());
          ret = OB_ERROR;
        }
        else
        {
          //TBSYS_LOG(INFO, "submit async task succ pcode=%d", pcode);
        }
      }
      return ret;
    }

    int ObMySQLServer::handle_packet(ObMySQLCommandPacket* packet)
    {
      int ret = OB_SUCCESS;

      if (NULL == packet)
      {
        TBSYS_LOG(ERROR, "invalid argument packet is %p", packet);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        uint8_t code = packet->get_type();
        if (COM_STMT_CLOSE == code)
        {
          bool bret = close_command_queue_thread_.push(packet,
                                                       (int)config_->obmysql_task_queue_size,
                                                       false);
          if (true == bret)
          {
            ret = OB_SUCCESS;
          }
          else
          {
            TBSYS_LOG(ERROR, "can not push packet(ptype is %u) to close command queue", code);
            ret = OB_ERROR;
          }
        }
        else
        {
          bool bret = command_queue_thread_.push(packet,
                                                 (int)config_->obmysql_task_queue_size,
                                                 false);
          if (true == bret)
          {
            ret = OB_SUCCESS;
          }
          else
          {
            TBSYS_LOG(WARN, "can not push packet(ptype is %u) to command queue", code);
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }

    int ObMySQLServer::handle_packet_queue(ObMySQLCommandPacket* packet, void* args)
    {
      int ret = OB_SUCCESS;
      static __thread int64_t thread_counter = 0;
      static volatile uint64_t total_counter = 0;
      UNUSED(args);

      // reset error message buffer before query start
      ob_reset_err_msg();
      if (NULL == packet)
      {
        TBSYS_LOG(ERROR, "invalide argument packet is %p", packet);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        uint8_t code = packet->get_type();
        switch(code)
        {
          case COM_QUIT:
            ret = do_com_quit(packet);
            break;
          case COM_QUERY:
            ret = do_com_query(packet);
            break;
          case COM_STMT_PREPARE:
            ret = do_com_prepare(packet);
            break;
          case COM_STMT_EXECUTE:
            ret = do_com_execute(packet);
            break;
          case COM_STMT_CLOSE:
            ret = do_com_close_stmt(packet);
            break;
          case COM_PING:
            ret = do_com_ping(packet);
            break;
          case COM_DELETE_SESSION:
            ret = do_com_delete_session(packet);
            break;
          default:
            TBSYS_LOG(WARN, "unsupport command %u", code);
            //TODO close connection or send error packet
            ret = do_unsupport(packet);
            break;
        }

        if (COM_DELETE_SESSION != code)
        {
          ++thread_counter;
          atomic_inc(&total_counter);
          if (0 == thread_counter % 5000)
          {
            TBSYS_LOG(INFO, "server statistics: current tid=%ld, thread_counter=%ld, total_counter=%lu",
                      syscall(__NR_gettid), thread_counter, total_counter);
          }
        }
      }
      return ret;
    }

    common::ThreadSpecificBuffer::Buffer* ObMySQLServer::get_buffer() const
    {
      return response_buffer_.get_buffer();
    }

    int ObMySQLServer::do_unsupport(ObMySQLCommandPacket* packet)
    {
      int ret = OB_SUCCESS;
      ret = check_param(packet);
      if (OB_SUCCESS == ret)
      {
        easy_addr_t addr = get_easy_addr(packet->get_request());
        ObMySQLErrorPacket epacket;
        char * msg = (char*)"cmd not support yet.";
        ObString message(ObString(static_cast<int32_t>(strlen(msg)),
                                  static_cast<int32_t>(strlen(msg)),
                                  msg));

        epacket.set_message(message);
        epacket.set_oberrcode(OB_NOT_SUPPORTED);
        number = packet->get_packet_header().seq_;
        ++number;
        ret = post_packet(packet->get_request(), &epacket, number);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to send error packet to mysql client(%s) ret is %d",
                    inet_ntoa_r(addr), ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "send error packet to mysql client(%s)", inet_ntoa_r(addr));
        }
      }
      return ret;
    }

    int ObMySQLServer::do_com_delete_session(ObMySQLCommandPacket* packet)
    {
      int ret = OB_SUCCESS;
      uint64_t key = 0;
      char *buffer = const_cast<char*>(packet->get_command().ptr());
      //decode key from command buffer
      if (OB_SUCCESS != (ObMySQLUtil::get_length(buffer, key)))
      {
        TBSYS_LOG(WARN, "decode key from packet buffer failed");
      }

      if (OB_SUCCESS == ret)
      {
        ObMySQLSessionKey seq = static_cast<int>(key);
        ObSQLSessionInfo *expired_session = NULL;
        ret = session_mgr_.get(seq, expired_session);
        if (HASH_EXIST != ret)
        {
          if (-1 == ret)
          {
            TBSYS_LOG(ERROR, "found session error, session key is %d", seq);
          }
          else if (HASH_NOT_EXIST == ret)
          {
            TBSYS_LOG(ERROR, "session not found session key is %d", seq);
          }
        }
        else
        {
          if (OB_SUCCESS == expired_session->get_mutex().trylock()) // make sure there is no one still using the session
          {
            ret = OB_SUCCESS;
            TBSYS_LOG(INFO, "delete expired session, Session key is %d, Session info is %s", seq, to_cstring(*expired_session));
            expired_session->~ObSQLSessionInfo();
            ob_free(expired_session);
            expired_session = NULL;
            ret = session_mgr_.erase(seq);
            if (-1 == ret)
            {
              TBSYS_LOG(ERROR, "erase session failed, key is %d", seq);
            }
            OB_STAT_INC(OBMYSQL, SUCC_LOGOUT_COUNT);
          }
          else
          {
            usleep(10*1000);//sleep 10ms wait other thread unlock session mutex
            ret = submit_session_delete_task(seq);
          }
        }
      }
      return ret;
    }

    int ObMySQLServer::do_com_quit(ObMySQLCommandPacket* packet)
    {
      int ret = OB_SUCCESS;
      ret = check_param(packet);
      if (OB_SUCCESS == ret)
      {
        easy_request_t* req = packet->get_request();
        TBSYS_LOG(INFO, "client quit, peer=%s",
                    inet_ntoa_r(req->ms->c->addr));
        easy_request_wakeup(req);
        // do nothing, the session will be destroyed in MySQLCallback::on_disconnect()
      }
      return ret;
    }

    int ObMySQLServer::do_com_query(ObMySQLCommandPacket* packet)
    {
      int ret = OB_SUCCESS;
      int err = OB_SUCCESS;
      int64_t start_time = tbsys::CTimeUtil::getTime();
      ObBasicStmt::StmtType inner_stmt_type = ObBasicStmt::T_NONE;
      ret = check_param(packet);
      if (OB_SUCCESS == ret)
      {
        ObSqlContext context;
        ObMySQLResultSet result;
        easy_addr_t addr = get_easy_addr(packet->get_request());
        const common::ObString& q = packet->get_command();
        int32_t truncated_length = std::min(q.length(), 384);
        FILL_TRACE_LOG("stmt=\"%.*s\"", truncated_length, q.ptr());
        TBSYS_LOG(INFO, "start query: \"%.*s\" real_query_len=%d, peer=%s",
                  truncated_length, q.ptr(), q.length(), inet_ntoa_r(addr));

        if (OB_SUCCESS != (ret = result.init()))
        {
          TBSYS_LOG(ERROR, "Init result set failed, ret = %d", ret);
        }
        else
        {
          int64_t schema_version = 0;
          number = packet->get_packet_header().seq_;
          ret = init_sql_env(*packet, context, schema_version, result);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "init sql env failed ret is %d", ret);
            result.set_errcode(OB_INIT_SQL_CONTEXT_ERROR);
            if (OB_SUCCESS != (err = send_error_packet(packet, &result)))
            {
              TBSYS_LOG(WARN, "fail to send error packet. err=%d", err);
            }
          }
          else
          {
            // do it
            ret = ObSql::direct_execute(q, result, context);
            FILL_TRACE_LOG("direct_execute");
            // process result
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "direct_execute failed, err=%d stmt=\"%.*s\" ret is %d",
                        ret, q.length(), q.ptr(), ret);
              if (OB_SUCCESS != (err = send_error_packet(packet, &result)))
              {
                TBSYS_LOG(WARN, "fail to send error packet. err=%d", err);
              }
            }
            else
            {
              ret = send_response(packet, &result, TEXT);
            }
            // release the schema after execution
            if (NULL != context.schema_manager_)
            {
              env_.schema_mgr_->release_schema(context.schema_manager_);
              context.schema_manager_ = NULL;
            }
            // release the privilege info after execution
            if (NULL != context.pp_privilege_)
            {
              err = env_.privilege_mgr_->release_privilege(context.pp_privilege_);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(ERROR, "release privilege failed, ret=%d", err);
              }
              context.pp_privilege_ = NULL;
            }
            if (context.session_info_ != NULL)
            {
              context.session_info_->set_warnings_buf();
            }
            inner_stmt_type = result.get_inner_stmt_type();
            cleanup_sql_env(context, result);
          }
        }
        PRINT_TRACE_LOG();
        CLEAR_TRACE_LOG();
        TBSYS_LOG(DEBUG, "end query");
      }

      do_stat(inner_stmt_type, tbsys::CTimeUtil::getTime() - start_time);
      if (OB_SUCCESS == ret)
      {
        OB_STAT_INC(OBMYSQL, SUCC_QUERY_COUNT);
      }
      else
      {
        OB_STAT_INC(OBMYSQL, FAIL_QUERY_COUNT);
      }
      return ret;
    }

    int ObMySQLServer::do_com_prepare(ObMySQLCommandPacket* packet)
    {
      int ret = OB_SUCCESS;
      int err = OB_SUCCESS;
      ret = check_param(packet);
      if (OB_SUCCESS == ret)
      {
        ObSqlContext context;
        ObMySQLResultSet result;
        const common::ObString& q = packet->get_command();
        int32_t truncated_length = std::min(q.length(), 384);
        FILL_TRACE_LOG("stmt=\"%.*s\"", truncated_length, q.ptr());
        TBSYS_LOG(INFO, "start prepare: \"%.*s\" real_query_len=%d",
                  truncated_length, q.ptr(), q.length());
        //1. init session && sql_context
        easy_request_t *req = packet->get_request();
        UNUSED(req);
        int64_t schema_version = 0;
        number = packet->get_packet_header().seq_;
        ret = init_sql_env(*packet, context, schema_version, result);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to init sql env ret is %d", ret);
          result.set_errcode(OB_INIT_SQL_CONTEXT_ERROR);
          if (OB_SUCCESS != (err = send_error_packet(packet, &result)))
          {
            TBSYS_LOG(WARN, "fail to send error packet. err=%d", err);
          }
        }
        else
        {
          ret = ObSql::stmt_prepare(packet->get_command(), result, context);
          FILL_TRACE_LOG("stmt_prepare query_result=(%s)", to_cstring(result));
          // release the schema after execute
          if (NULL != context.schema_manager_)
          {
            env_.schema_mgr_->release_schema(context.schema_manager_);
            context.schema_manager_ = NULL;
          }
          // release the privilege info after parsing
          if (NULL != context.pp_privilege_)
          {
            int err = env_.privilege_mgr_->release_privilege(context.pp_privilege_);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(ERROR, "release privilege failed, ret=%d", err);
            }
            context.pp_privilege_ = NULL;
          }

          // process result
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "stmt_prepare failed, err=%d stmt=\"%.*s\" ret is %d",
                      ret, q.length(), q.ptr(), ret);
            if (OB_SUCCESS != (err = send_error_packet(packet, &result)))
            {
              TBSYS_LOG(WARN, "fail to to send error packet. err=%d", err);
            }
          }
          else
          {
            ret = send_stmt_prepare_response(packet, &result);
          }
          if (context.session_info_ != NULL)
          {
            context.session_info_->set_warnings_buf();
          }
          cleanup_sql_env(context, result);
        }
        PRINT_TRACE_LOG();
        CLEAR_TRACE_LOG();
        TBSYS_LOG(DEBUG, "end do_com_prepare");
      }
      if (OB_SUCCESS == ret)
      {
        OB_STAT_INC(OBMYSQL, SUCC_PREPARE_COUNT);
      }
      else
      {
        OB_STAT_INC(OBMYSQL, FAIL_PREPARE_COUNT);
      }
      return ret;
    }

    int ObMySQLServer::do_com_execute(ObMySQLCommandPacket* packet)
    {
      int ret = OB_SUCCESS;
      int err = OB_SUCCESS;
      ObSqlContext context;
      ObMySQLResultSet result;
      ObArray<ObObj> params;
      ObArray<EMySQLFieldType> params_type;
      int64_t schema_version = 0;
      uint32_t stmt_id = 0;
      int64_t start_time = tbsys::CTimeUtil::getTime();
      ObBasicStmt::StmtType inner_stmt_type = ObBasicStmt::T_NONE;

      FILL_TRACE_LOG("start do_com_execute");
      ret = check_param(packet);
      if (OB_SUCCESS == ret)
      {
        //1. init session && sql_context
        number = packet->get_packet_header().seq_;
        ret = init_sql_env(*packet, context, schema_version, result);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to init sql env ret is %d", ret);
          result.set_errcode(OB_INIT_SQL_CONTEXT_ERROR);
          if (OB_SUCCESS != (err = send_error_packet(packet, &result)))
          {
            TBSYS_LOG(WARN, "fail to send error packet. err=%d", err);
          }
        }
        else
        {
          //2. parase input packet get param
          //得到prepare的参数的类型和值
          ret = parse_execute_params(&context, packet, params, stmt_id, params_type);
          FILL_TRACE_LOG("stmt_id=%u params=%s", stmt_id, to_cstring(params));
          if (OB_SUCCESS != ret)
          {
            if (NULL != context.schema_manager_)
            {
              env_.schema_mgr_->release_schema(context.schema_manager_);
              context.schema_manager_ = NULL;
            }
            if (NULL != context.pp_privilege_)
            {
              int err = env_.privilege_mgr_->release_privilege(context.pp_privilege_);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(ERROR, "release privilege failed, ret=%d", err);
              }
              context.pp_privilege_ = NULL;
            }
            TBSYS_LOG(ERROR, "parse params from COM_STMT_EXECUTE failed");
            if (OB_SUCCESS != (err = send_error_packet(packet, &result)))
            {
              TBSYS_LOG(WARN, "fail to send error packet. err=%d", err);
            }
          }
          else
          {
            //TODO read session and call the right function
            ret = ObSql::stmt_execute(stmt_id, params_type, params, result, context);
            FILL_TRACE_LOG("stmt_execute");
            // release the schema manager after parsing
            if (NULL != context.schema_manager_)
            {
              env_.schema_mgr_->release_schema(context.schema_manager_);
              context.schema_manager_ = NULL;
            }
            // release the privilege info after parsing
            if (NULL != context.pp_privilege_)
            {
              err = env_.privilege_mgr_->release_privilege(context.pp_privilege_);
              if (OB_SUCCESS != err)
              {
                TBSYS_LOG(ERROR, "release privilege failed, ret=%d", err);
              }
              context.pp_privilege_ = NULL;
            }
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "do_com_execute failed, err=%d stmt=%u", ret, stmt_id);
              if (OB_SUCCESS != (err = send_error_packet(packet, &result)))
              {
                TBSYS_LOG(WARN, "fail to send error packet. err=%d", err);
              }
            }
            else
            {
              ret = send_response(packet, &result, BINARY);
            }
          }
          if (context.session_info_ != NULL)
          {
            context.session_info_->set_warnings_buf();
          }
          inner_stmt_type = result.get_inner_stmt_type();
          cleanup_sql_env(context, result);
        }
      }
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      TBSYS_LOG(DEBUG, "end do_com_execute");


      do_stat(inner_stmt_type, tbsys::CTimeUtil::getTime() - start_time);
      if (OB_SUCCESS == ret)
      {
        OB_STAT_INC(OBMYSQL, SUCC_EXEC_COUNT);
      }
      else
      {
        OB_STAT_INC(OBMYSQL, FAIL_EXEC_COUNT);
      }
      return ret;
    }

    int ObMySQLServer::do_com_close_stmt(ObMySQLCommandPacket* packet)
    {
      int ret = OB_SUCCESS;
      int err = OB_SUCCESS;
      ObSqlContext context;
      ObMySQLResultSet result;
      int64_t schema_version = 0;
      uint32_t stmt_id = 0;
      TBSYS_LOG(TRACE, "start do_com_close_stmt");
      ret = check_param(packet);
      if (OB_SUCCESS == ret)
      {
        //get stmt id from packet
        char *data = packet->get_command().ptr();
        ObMySQLUtil::get_uint4(data, stmt_id);
        number = packet->get_packet_header().seq_;

        //1. init session && sql_context
        ret = init_sql_env(*packet, context, schema_version, result);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to init sql env ret is %d", ret);
          // No response is sent back to the client.
        }
        else
        {
          ret = ObSql::stmt_close(stmt_id, context);
          FILL_TRACE_LOG("stmt_close stmt_id=%u ret=%d", stmt_id, ret);
          //release after call stmt_close
          if (NULL != context.schema_manager_)
          {
            env_.schema_mgr_->release_schema(context.schema_manager_);
            context.schema_manager_ = NULL;
          }
          // release the privilege info after parsing
          if (NULL != context.pp_privilege_)
          {
            err = env_.privilege_mgr_->release_privilege(context.pp_privilege_);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(ERROR, "release privilege failed, ret=%d", err);
            }
            context.pp_privilege_ = NULL;
          }
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "sql engine close statement(id is %u) faild ret is %d", stmt_id, ret);
            // No response is sent back to the client.
          }
          else
          {
            // No response is sent back to the client.
          }
          if (context.session_info_ != NULL)
          {
            context.session_info_->set_warnings_buf();
          }
          cleanup_sql_env(context, result);
        }
      }
      easy_request_wakeup(packet->get_request());
      PRINT_TRACE_LOG();
      CLEAR_TRACE_LOG();
      TBSYS_LOG(DEBUG, "end do_com_stmt_close");

      if (OB_SUCCESS == ret)
      {
        OB_STAT_INC(OBMYSQL, SUCC_CLOSE_STMT_COUNT);
      }
      else
      {
        OB_STAT_INC(OBMYSQL, FAIL_CLOSE_STMT_COUNT);
      }
      return ret;
    }

    //just return ok with no info
    int ObMySQLServer::do_com_ping(ObMySQLCommandPacket* packet)
    {
      int ret = OB_SUCCESS;
      ret = check_param(packet);
      if (OB_SUCCESS == ret)
      {
        easy_addr_t addr = get_easy_addr(packet->get_request());
        number = packet->get_packet_header().seq_;
        //fake result
        ObMySQLResultSet result;
        result.set_affected_rows(0);
        result.set_warning_count(0);
        result.set_message("server is ok");
        TBSYS_LOG(INFO, "start do_com_ping: \"%.*s\"", packet->get_command().length(), packet->get_command().ptr());
        ret = send_ok_packet(packet, &result);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "send ok packet to client(%s) failed ret is %d",
                    inet_ntoa_r(addr), ret);
        }
      }
      return ret;
    }

    int ObMySQLServer::post_packet(easy_request_t* req, ObMySQLPacket* packet, uint8_t seq)
    {
      int ret = OB_SUCCESS;
      if (NULL == req || NULL == packet)
      {
        TBSYS_LOG(ERROR, "invalid argument req is %p, packet is %p",
                  req, packet);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObDataBuffer buffer;
        int32_t pkt_len = 0;
        int64_t len_pos = 0;
        int64_t pos = 0;

        uint64_t size = packet->get_serialize_size() + OB_MYSQL_PACKET_HEADER_SIZE;
        //同样，还是在message的pool上分配内存
        easy_buf_t* buf = reinterpret_cast<easy_buf_t*>(easy_pool_alloc(req->ms->pool,
                                                                         static_cast<uint32_t>(sizeof(easy_buf_t) + size)));
        char* buff = NULL;
        if (NULL != buf)
        {
          pos += SEQ_OFFSET;
          buff = reinterpret_cast<char*>(buf + 1);
          init_easy_buf(buf, buff, req, size);
          ObMySQLUtil::store_int1(buff, size, seq, pos);
          ret = packet->serialize(buff, size, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "serialize packet failed packet is %p ret is %d", packet, ret);
          }
          else
          {
            len_pos = 0;
            // 写入包的长度
            pkt_len = static_cast<int32_t>(pos - OB_MYSQL_PACKET_HEADER_SIZE);
            ObMySQLUtil::store_int3(buff, size, pkt_len, len_pos);
            buf->pos = buff;
            buf->last = buff + pos;
            buf->end  = buff + size;
            //设置output packet
            req->opacket = reinterpret_cast<void*>(buf);
            //hex_dump(buf->pos,  static_cast<int32_t>(buf->last - buf->pos), true, TBSYS_LOG_LEVEL_INFO);
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "alloc buffer from req->ms->pool failed");
          ret = OB_ERROR;
        }
        easy_request_wakeup(req);
      }
      return ret;
    }

    int ObMySQLServer::do_com_reset_stmt(ObMySQLCommandPacket* packet)
    {
      int ret = OB_SUCCESS;
      if (NULL == packet)
      {
        TBSYS_LOG(ERROR, "invalid argument packet is %p", packet);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        //TODO to be implement
      }
      return ret;
    }

    int ObMySQLServer::send_response(ObMySQLCommandPacket* packet, ObMySQLResultSet* result, MYSQL_PROTOCOL_TYPE type)
    {
      int ret = OB_SUCCESS;
      int err = OB_SUCCESS;
      if (NULL == packet || NULL == result || NULL == packet->get_request())
      {
        TBSYS_LOG(ERROR, "invalid argument packet is %p, result is %p, req is %p",
                  packet, result, packet->get_request());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        FILL_TRACE_LOG("query_result=(%s)", to_cstring(*result));
        easy_request_t* req = packet->get_request();
        easy_addr_t addr = get_easy_addr(req);
        number = packet->get_packet_header().seq_;
        if (OB_SUCCESS != (ret = result->open()))
        {
          ObString err_msg = ob_get_err_msg();
          TBSYS_LOG(WARN, "failed to open result set, err=%d, msg=%.*s", ret, err_msg.length(), err_msg.ptr());
          if (OB_SUCCESS != (err = send_error_packet(packet, result)))
          {
            TBSYS_LOG(WARN, "fail to send error packet. err=%d", err);
          }
          if (OB_SUCCESS != result->close())
          {
            TBSYS_LOG(WARN, "close result set failed");
          }
        }
        else
        {
          FILL_TRACE_LOG("open");
          bool select = result->is_with_rows();
          if (select)
          {
            ret = send_result_set(req, result, type);
            //send error packet if some error when sending resultset
            //except OB_CONNECT_ERROR which means connection lost
            if (OB_SUCCESS != ret && OB_CONNECT_ERROR != ret)
            {
              if (OB_SUCCESS != (err = send_error_packet(packet, result)))
              {
                TBSYS_LOG(ERROR, "send error packet to client(%s), failed,ret=%d", inet_ntoa_r(addr), err);
              }
              FILL_TRACE_LOG("client_res=%d", result->get_errcode());
            }
            else if (OB_SUCCESS == ret)
            {
              FILL_TRACE_LOG("client_res=ok");
            }
          }
          else //send result for update/insert/delete
          {
            ret = send_ok_packet(packet, result);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "send ok packet to client(%s) failed ret is %d",
                        inet_ntoa_r(addr), ret);
            }
            FILL_TRACE_LOG("client_res=ok");
          }
          int ret2 = result->close();
          if (OB_UNLIKELY(OB_SUCCESS != ret2))
          {
            TBSYS_LOG(ERROR, "close result set failed ret is %d", ret2);
            if (OB_SUCCESS == ret)
            {
              ret = ret2;
            }
          }
          FILL_TRACE_LOG("close");
        }
      }
      return ret;
    }

    int ObMySQLServer::init_sql_env(const ObMySQLCommandPacket& packet, ObSqlContext &context,
                                    int64_t &schema_version, ObMySQLResultSet &result)
    {
      int ret = OB_SUCCESS;
      ObMySQLSessionKey key = packet.get_request()->ms->c->seq;
      ObSQLSessionInfo *info = NULL;
      ret = session_mgr_.get(key, info);
      if (HASH_EXIST != ret)
      {
        if (-1 == ret)
        {
          TBSYS_LOG(ERROR, "found session error, session key is %d", key);
        }
        else if (HASH_NOT_EXIST == ret)
        {
          TBSYS_LOG(ERROR, "session not found session key is %d", key);
        }
      }
      else
      {
        info->get_mutex().lock(); // !!!lock
        TBSYS_LOG(DEBUG, "get session, addr=%p key=%d info=%s",
                  info, key, to_cstring(*info));
        ret = OB_SUCCESS;
        context.session_info_ = info;
        context.session_info_->set_current_result_set(&result);

        schema_version = env_.schema_mgr_->get_latest_version();
        context.schema_manager_ = env_.schema_mgr_->get_user_schema(schema_version); // reference count
        const ObPrivilege **pp_privilege = NULL;
        ret = env_.privilege_mgr_->get_newest_privilege(pp_privilege);
        if (OB_SUCCESS != ret)
        {
          info->get_mutex().unlock(); // !!!unlock when error
          TBSYS_LOG(ERROR, "can 't get privilege information, internal server error, ret=%d", ret);
        }
        else if (NULL == context.schema_manager_)
        {
          info->get_mutex().unlock(); // !!!unlock when error
          TBSYS_LOG(WARN, "fail to get user schema. schema_version=%ld", schema_version);
          result.set_message("fail to get user schema.");
          ret = OB_ERROR;
        }
        else
        {
          context.merger_schema_mgr_ = env_.schema_mgr_;
          context.privilege_mgr_ = env_.privilege_mgr_;
          context.cache_proxy_ = env_.cache_proxy_;    // thread safe singleton
          context.async_rpc_ = env_.async_rpc_;        // thread safe singleton
          context.merger_rpc_proxy_ = env_.rpc_proxy_; // thread safe singleton
          context.merge_service_ = env_.merge_service_;
          context.rs_rpc_proxy_ = env_.root_rpc_;      // thread safe singleton
          context.pp_privilege_ = pp_privilege;
          context.stat_mgr_ = env_.stat_mgr_;
          // reuse memory pool for parser
          context.session_info_->get_parser_mem_pool().reuse();
          context.session_info_->get_transformer_mem_pool().start_batch_alloc();
          // note that the session is locked now
        }
        if (ret != OB_SUCCESS)
        {
          if (NULL != context.schema_manager_)
          {
            int err = env_.schema_mgr_->release_schema(context.schema_manager_);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(ERROR, "release schema failed, ret=%d", err);
            }
            context.schema_manager_ = NULL;
          }
          if (NULL != context.pp_privilege_)
          {
            int err = env_.privilege_mgr_->release_privilege(context.pp_privilege_);
            if (OB_SUCCESS != err)
            {
              TBSYS_LOG(ERROR, "release privilege failed, ret=%d", err);
            }
            context.pp_privilege_ = NULL;
          }
        }
      }
      result.set_errcode(ret);
      return ret;
    }

    void ObMySQLServer::cleanup_sql_env(ObSqlContext &context, ObMySQLResultSet &result)
    {
      bool reuse_mem = !result.is_prepare_stmt();
      result.reset();
      OB_ASSERT(context.session_info_);
      context.session_info_->get_parser_mem_pool().reuse();
      context.session_info_->get_transformer_mem_pool().end_batch_alloc(reuse_mem);
      context.session_info_->get_mutex().unlock(); // !!!unlock
      TBSYS_LOG(DEBUG, "stack allocator reuse_mem=%c", reuse_mem?'Y':'N');
#ifdef __OB_MTRACE__
      ob_print_mod_memory_usage();
#endif
    }

    int ObMySQLServer::send_error_packet(ObMySQLCommandPacket* packet, ObMySQLResultSet *result)
    {
      int ret = OB_SUCCESS;
      ObMySQLErrorPacket epacket;
      if (NULL == packet || NULL == packet->get_request())
      {
        TBSYS_LOG(ERROR, "should never reach here invalid argument packet is %p, req is %p",
                  packet, packet->get_request());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        easy_request_t *req = packet->get_request();
        easy_addr_t addr = get_easy_addr(req);
        ObString message = ob_get_err_msg();
        if (message.length() <= 0)
        {
          if (NULL != result && 0 < strlen(result->get_message()))
          {
            message = ObString::make_string(result->get_message());
          }
          else
          {
            message = ObString::make_string("unknown internal error"); // default error message
          }
        }
        if (OB_SUCCESS == (ret = epacket.set_oberrcode(result->get_errcode()))
            && OB_SUCCESS == (ret = epacket.set_message(message)))
        {
          //makesure seq is continuous
          ++number;
          ret = post_packet(req, &epacket, number);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "failed to send error packet to mysql client(%s) ret is %d", inet_ntoa_r(addr), ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "send error packet(seq=%u) to mysql client(%s) succ", number, inet_ntoa_r(addr));
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "set member of MySQL error packet failed");
        }
      }
      return ret;
    }

    int ObMySQLServer::send_spr_packet(ObMySQLCommandPacket* packet, ObMySQLResultSet *result)
    {
      int ret = OB_SUCCESS;
      ObMySQLSPRPacket sprpacket;
      if (NULL == packet || NULL == packet->get_request())
      {
        TBSYS_LOG(ERROR, "should never reach here invalid argument packet is %p, req is %p",
                  packet, packet->get_request());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        easy_request_t *req = packet->get_request();
        easy_addr_t addr = get_easy_addr(req);
        sprpacket.set_statement_id(static_cast<uint32_t>(result->get_statement_id()));
        sprpacket.set_column_num(static_cast<uint16_t>(result->get_field_cnt()));
        sprpacket.set_param_num(
          static_cast<uint16_t>(result->get_param_columns().count()));
        sprpacket.set_warning_count(static_cast<uint16_t>(result->get_warning_count()));

        //makesure seq is continuous
        ++number;
        ret = post_packet(req, &sprpacket, number);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "failed to send error packet to mysql client(%s) ret is %d", inet_ntoa_r(addr), ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "send spr packet to mysql client(%s) succ", inet_ntoa_r(addr));
        }
      }
      return ret;
    }

    int ObMySQLServer::send_ok_packet(ObMySQLCommandPacket* packet, ObMySQLResultSet *result)
    {
      int ret = OB_SUCCESS;
      if (NULL == packet || NULL == packet->get_request())
      {
        TBSYS_LOG(ERROR, "should never reach here invalid argument packet is %p, req is %p",
                  packet, packet->get_request());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        easy_request_t *req = packet->get_request();
        easy_addr_t addr = get_easy_addr(req);
        ObMySQLOKPacket ok;
        ok.set_affected_rows(result->get_affected_rows());
        ok.set_warning_count(static_cast<uint16_t>(result->get_warning_count()));
        //TODO get server status from resultset
        ok.set_server_status(0x22);
        ObString message(ObString(static_cast<int32_t>(strlen(result->get_message())),
                                  static_cast<int32_t>(strlen(result->get_message())),
                                  const_cast<char*>(result->get_message())));
        ok.set_message(message);
        number++;
        req->retcode = EASY_OK;
        ret = post_packet(req, &ok, number);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "post ok packet to client(%s) failed ret is %d",
                    inet_ntoa_r(addr), ret);
        }
        else
        {
          TBSYS_LOG(DEBUG, "send result for update/insert/delete to client(%s) succ",
                    inet_ntoa_r(addr));
        }
      }
      return ret;
    }

    int ObMySQLServer::send_stmt_prepare_response(ObMySQLCommandPacket* packet, ObMySQLResultSet* result)
    {
      int ret = OB_SUCCESS;
      if (NULL == packet || NULL == result || NULL == packet->get_request())
      {
        TBSYS_LOG(ERROR, "should not reache here, invalid argument"
                  "packet is %p, result is %p, req is %p", packet, result, packet->get_request());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        TBSYS_LOG(TRACE, "query result=(%s)", to_cstring(*result));
        easy_request_t *req = packet->get_request();
        easy_addr_t addr = get_easy_addr(req);
        if (OB_SUCCESS != (ret = result->open()))
        {
          ObString err_msg = ob_get_err_msg();
          TBSYS_LOG(WARN, "failed to open result set, err=%d, msg=%.*s", ret, err_msg.length(), err_msg.ptr());
          ret = send_error_packet(packet, result);
          if (OB_SUCCESS != result->close())
          {
            TBSYS_LOG(WARN, "close result set failed");
          }
        }
        else
        {
          //send spr packet following with param && field
          if (result->get_field_cnt() > 0 || result->get_param_cnt() > 0)
          {
            ret = send_stmt_prepare_result_set(packet, result);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "send stmt prepare result set to client %s failed ret is %d", inet_ntoa_r(addr), ret);
            }
          }
          else                  // send only spr packet
          {
            ret = send_spr_packet(packet, result);
          }
          ret = result->close();
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "close result set failed ret is %d", ret);
          }
        }
      }
      return ret;
    }

    int ObMySQLServer::parse_execute_params(ObSqlContext *context, ObMySQLCommandPacket *packet, ObArray<ObObj>& params, uint32_t &stmt_id, ObArray<EMySQLFieldType>& params_type)
    {
      int ret = OB_SUCCESS;
      if (NULL == packet)
      {
        TBSYS_LOG(ERROR, "should not reach here");
      }
      else
      {
        bool nv = false;
        char *payload = packet->get_command().ptr();
        char *start = payload;
        int32_t length = static_cast<int32_t>(packet->get_command().length());
        //hex_dump(payload, length, true, TBSYS_LOG_LEVEL_INFO);
        int8_t bound_flag = 0;
        uint8_t type = 0;
        int8_t flag = 0;
        int64_t param_count = 0;
        ObMySQLUtil::get_uint4(payload, stmt_id);
        payload += 5;           // skip flags && iteration-count

        ObResultSet* rsset = context->session_info_->get_plan(stmt_id);
        if (NULL == rsset)
        {
          TBSYS_LOG(ERROR, "can not find ObResultSet for stmt_id = %u", stmt_id);
          ret = OB_ERROR;
        }
        else
        {
          param_count = context->session_info_->get_plan(stmt_id)->get_param_columns().count();
          const common::ObArray<EMySQLFieldType> &types = context->session_info_->get_plan(stmt_id)->get_params_type();
          TBSYS_LOG(DEBUG, "ps expected param_count=%ld, data_len=%d remain=%ld",
                    param_count, length, payload - start);
          if (param_count > 0 && payload - start < length)
          {
            int64_t bitmap_bytes = (param_count + 7) / 8;
            char *bitmap = payload;
            payload += bitmap_bytes;
            // new-params-bound-flag
            ObMySQLUtil::get_int1(payload, bound_flag);
            TBSYS_LOG(DEBUG, "bitmap_bytes=%ld bound_flag=%hhd bitmap0=%hhu",
                      bitmap_bytes, bound_flag, bitmap[0]);
            if (payload -start <= length)
            {
              char *databuf = NULL;
              if (1 == bound_flag)
              {
                params_type.clear();
                databuf = payload + param_count * 2;
              }
              else
              {
                databuf = payload;
              }

              for (int32_t index = 0; index < param_count && OB_SUCCESS == ret; ++index)
              {
                ObObj param;
                ObObjType ob_type;
                if (1 == bound_flag)
                {
                  // with param types
                  ObMySQLUtil::get_uint1(payload, type);
                  ObMySQLUtil::get_int1(payload, flag);
                  if (OB_SUCCESS != (ret = params_type.push_back(static_cast<EMySQLFieldType>(type))))
                  {
                    TBSYS_LOG(WARN, "failed to push back, err=%d", ret);
                  }
                }
                else
                {
                  if (param_count != types.count())
                  {
                    ret = OB_ERR_WRONG_DYNAMIC_PARAM;
                    TBSYS_LOG(USER_ERROR, "Incorrect arguments number to EXECUTE, need %ld arguments but give %ld",
                              types.count(), param_count);
                  }
                  else
                  {
                    type = static_cast<uint8_t>(types.at(index));
                  }
                }
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = ObMySQLUtil::get_ob_type(ob_type, static_cast<EMySQLFieldType>(type))))
                {
                  TBSYS_LOG(WARN, "cast ob type from MySQL type failed MySQL type is %d", type);
                }
                else
                {
                  param.set_type(ob_type);
                  nv = ObMySQLUtil::update_from_bitmap(param, bitmap, index);
                  if (false == nv)
                  {
                    // is not NULL
                    ret = get_param_value(databuf, param, type);
                    if (OB_SUCCESS != ret)
                    {
                      TBSYS_LOG(WARN, "get param value failed, type=%u i=%d ret is %d", type, index, ret);
                    }
                    else
                    {
                      TBSYS_LOG(DEBUG, "execute with param i=%d type=%d param=%s",
                                index, type, to_cstring(param));
                    }
                  }
                  else
                  {
                    TBSYS_LOG(DEBUG, "execute with param i=%d param=NULL", index);
                  }
                  if (OB_LIKELY(OB_SUCCESS == ret))
                  {
                    if (OB_SUCCESS != (ret = params.push_back(param)))
                    {
                      TBSYS_LOG(WARN, "failed to push back, err=%d", ret);
                    }
                  }
                }
              } // end for
              if (OB_SUCCESS != ret)
              {
                params_type.clear();
              }
            }
          }
          else
          {
            //do nothing
          }
        }
      }
      return ret;
    }

    int ObMySQLServer::get_param_value(char *&data, ObObj &param, uint8_t type)
    {
      int ret = OB_SUCCESS;
      switch(type)
      {
      case MYSQL_TYPE_TINY:
      {
        int8_t value;
        ObMySQLUtil::get_int1(data, value);
        bool bv = (value == 0 ? false : true);
        param.set_bool(bv);
        break;
      }
      case MYSQL_TYPE_SHORT:
      {
        int16_t value = 0;
        ObMySQLUtil::get_int2(data, value);
        param.set_int(value);
        break;
      }
      case MYSQL_TYPE_LONG:
      {
        int32_t value = 0;
        ObMySQLUtil::get_int4(data, value);
        param.set_int(value);
        break;
      }
      case MYSQL_TYPE_LONGLONG:
      {
        int64_t value = 0;
        ObMySQLUtil::get_int8(data, value);
        param.set_int(value);
        break;
      }
      case MYSQL_TYPE_FLOAT:
      {
        float value = 0;
        memcpy(&value, data, sizeof(value));
        data += sizeof(value);
        param.set_float(value);
        break;
      }
      case MYSQL_TYPE_DOUBLE:
      {
        double value = 0;
        memcpy(&value, data, sizeof(value));
        data += sizeof(value);
        param.set_double(value);
        break;
      }
      case MYSQL_TYPE_DATETIME:
      case MYSQL_TYPE_TIMESTAMP:
        //http://dev.mysql.com/doc/internals/en/prepared-statements.html#packet-ProtocolBinary::MYSQL_TYPE_TIMESTAMP
        ret = get_mysql_timestamp_value(data, param);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "parse timestamp value from client failed ret is %d", ret);
        }
        break;
      case MYSQL_TYPE_STRING:
      case MYSQL_TYPE_VARCHAR:
      case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_BLOB:
      {
        ObString str;
        uint64_t length = 0;
        ret = ObMySQLUtil::get_length(data, length);
        if (OB_SUCCESS == ret)
        {
          str.assign(data, static_cast<ObString::obstr_size_t>(length));
          param.set_varchar(str);
          data += length;
        }
        else
        {
          TBSYS_LOG(ERROR, "decode varchar param value failed ret is %d", ret);
        }
        break;
      }
      default:
        TBSYS_LOG(WARN, "unsupport MySQL type %d", type);
        ret = OB_ERROR;
        break;
      }
      return ret;
    }

    int ObMySQLServer::get_mysql_timestamp_value(char *&data, ObObj &param)
    {
      int ret = OB_SUCCESS;
      int8_t length = 0;
      int16_t year = 0;
      int8_t month = 0;
      int8_t day = 0;
      int8_t hour = 0;
      int8_t min = 0;
      int8_t second = 0;
      int32_t microsecond = 0;
      struct tm tmval;
      memset(&tmval, 0, sizeof(tmval));
      ObMySQLUtil::get_int1(data, length);
      ObPreciseDateTime value;
      if (0 == length)
      {
        value = 0;
      }
      else if (4 == length)
      {
        ObMySQLUtil::get_int2(data, year);
        ObMySQLUtil::get_int1(data, month);
        ObMySQLUtil::get_int1(data, day);
      }
      else if (7 == length)
      {
        ObMySQLUtil::get_int2(data, year);
        ObMySQLUtil::get_int1(data, month);
        ObMySQLUtil::get_int1(data, day);
        ObMySQLUtil::get_int1(data, hour);
        ObMySQLUtil::get_int1(data, min);
        ObMySQLUtil::get_int1(data, second);
      }
      else if (11 == length)
      {
        ObMySQLUtil::get_int2(data, year);
        ObMySQLUtil::get_int1(data, month);
        ObMySQLUtil::get_int1(data, day);
        ObMySQLUtil::get_int1(data, hour);
        ObMySQLUtil::get_int1(data, min);
        ObMySQLUtil::get_int1(data, second);
        ObMySQLUtil::get_int4(data, microsecond);
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "invalid mysql timestamp value length %d", length);
      }

      if (0 != length)
      {
        tmval.tm_year = year - 1900;
        tmval.tm_mon = month - 1;
        tmval.tm_mday = day;
        tmval.tm_hour = hour;
        tmval.tm_min = min;
        tmval.tm_sec = second;
        time_t tm = mktime(&tmval);
        value = tm * 1000000 + microsecond;
      }
      param.set_precise_datetime(value);
      TBSYS_LOG(DEBUG, "length is %d, time is %d-%d-%d %d:%d:%d %d value is %ld",
                length, year, month, day, hour, min, second, microsecond, value);
      return ret;
    }

    int ObMySQLServer::send_result_set(easy_request_t *req, ObMySQLResultSet *result, MYSQL_PROTOCOL_TYPE type)
    {
      int ret = OB_SUCCESS;
      int64_t buffer_length = 0;
      int64_t buffer_pos = 0;
      if (NULL == req || NULL == result)
      {
        TBSYS_LOG(ERROR, "invalid argument argument req is %p, result is %p", req, result);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        //基于结果集都是很小的假设每次都在message申请6k内存，尽可能的把所有的数据包都放到这个内存里面去
        //如果能够放下那么直接调用异步发送接口 工作线程不用等待IO线程发包
        //如果发现6k不能放下所有结果，先发送序列化好的包，等待IO线程发送完毕以便复用这个6k的内存
        easy_addr_t addr = get_easy_addr(req);
        easy_buf_t* buf = reinterpret_cast<easy_buf_t*>(easy_pool_alloc(req->ms->pool, OB_MYSQL_PACKET_BUFF_SIZE));
        if (NULL != buf)
        {
          char *data_buffer = reinterpret_cast<char *>(buf + 1);
          buffer_length = OB_MYSQL_PACKET_BUFF_SIZE - sizeof(easy_buf_t);
          init_easy_buf(buf, data_buffer, req, buffer_length);
          if (OB_SUCCESS != (ret = process_resheader_packet(buf, buffer_pos, req, result)))
          {
            TBSYS_LOG(WARN, "process resheasder packet failed dest is %s ret is %d", inet_ntoa_r(addr), ret);
          }
          else if (OB_SUCCESS != (ret = process_field_packets(buf, buffer_pos, req, result, true)))
          {
            TBSYS_LOG(WARN, "process field packet failed dest is %s ret is %d", inet_ntoa_r(addr), ret);
          }
          else if (OB_SUCCESS != (ret = process_eof_packet(buf, buffer_pos, req, result)))
          {
            TBSYS_LOG(WARN, "process field eof packet failed dest is %s ret is %d", inet_ntoa_r(addr), ret);
          }
          else if (OB_SUCCESS != (ret = process_row_packets(buf, buffer_pos,req, result, type)))
          {
            TBSYS_LOG(WARN, "process row packet failed dest is %s ret is %d", inet_ntoa_r(addr), ret);
          }
          else if (OB_SUCCESS != (ret = process_eof_packet(buf, buffer_pos, req, result)))
          {
            TBSYS_LOG(WARN, "process row eof packet failed dest is %s ret is %d", inet_ntoa_r(addr), ret);
          }
          if (OB_SUCCESS == ret)
          {
            TBSYS_LOG(DEBUG, "send result set to client %s", inet_ntoa_r(addr));
            buf->last = buf->pos + buffer_pos;
            req->opacket = reinterpret_cast<void*>(buf);
            ret = post_raw_packet(req);
            if (OB_SUCCESS != ret)
            {
              TBSYS_LOG(ERROR, "post packet to client(%s) failed ret is %d", inet_ntoa_r(addr), ret);
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "alloc buffer from req->ms->pool failed");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int ObMySQLServer::send_stmt_prepare_result_set(ObMySQLCommandPacket *packet, ObMySQLResultSet *result)
    {
      int ret = OB_SUCCESS;
      int64_t buffer_length = 0;
      int64_t buffer_pos = 0;
      if (NULL == packet || NULL == result)
      {
        TBSYS_LOG(ERROR, "invalid argument req is %p, result is %p", packet, result);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        easy_request_t *req = packet->get_request();
        easy_addr_t addr = get_easy_addr(req);
        easy_buf_t* buf = reinterpret_cast<easy_buf_t*>(easy_pool_alloc(req->ms->pool, OB_MYSQL_PACKET_BUFF_SIZE));
        if (NULL != buf && NULL != req)
        {
          char *data_buffer = reinterpret_cast<char *>(buf + 1);
          buffer_length = OB_MYSQL_PACKET_BUFF_SIZE - sizeof(easy_buf_t);
          init_easy_buf(buf, data_buffer, req, buffer_length);
          //send spr packet following with param && field
          if (result->get_field_cnt() > 0 || result->get_param_cnt() > 0)
          {
            ret = process_spr_packet(buf, buffer_pos, req, result);
            if (OB_SUCCESS == ret)
            {
              TBSYS_LOG(DEBUG, "send params%%fields field count is %ld, param count is %ld",
                        result->get_field_cnt(), result->get_param_cnt());
              if (result->get_param_cnt() > 0)
              {
                // 发送param columns
                if (OB_SUCCESS != (ret = process_field_packets(buf, buffer_pos, req, result, false)))
                {
                  TBSYS_LOG(WARN, "process param packets failed dest is %s ret is %d", inet_ntoa_r(addr), ret);
                }
                else if (OB_SUCCESS != (ret = process_eof_packet(buf, buffer_pos, req, result)))
                {
                  TBSYS_LOG(WARN, "process param eof failed dest is %s ret is %d", inet_ntoa_r(addr), ret);
                }
              }

              if (OB_SUCCESS == ret && result->get_field_cnt() > 0)
              {
                if (OB_SUCCESS != (ret = process_field_packets(buf, buffer_pos, req, result, true)))
                {
                  TBSYS_LOG(WARN, "send field packet to client(%s) failed ret is %d", inet_ntoa_r(addr), ret);
                }
                else if (OB_SUCCESS != (ret = process_eof_packet(buf, buffer_pos, req, result)))
                {
                  TBSYS_LOG(WARN, "send field eof packet to client(%s) failed ret is %d", inet_ntoa_r(addr), ret);
                }
              }

              if (OB_SUCCESS == ret)
              {
                TBSYS_LOG(DEBUG, "send result set to client %s", inet_ntoa_r(addr));
                buf->last = data_buffer + buffer_pos;
                req->opacket = reinterpret_cast<void*>(buf);
                ret = post_raw_packet(req);
                if (OB_SUCCESS != ret)
                {
                  TBSYS_LOG(ERROR, "post packet to client(%s) failed ret is %d", inet_ntoa_r(addr), ret);
                }
              }
            }
            else
            {
              TBSYS_LOG(ERROR, "process spr packet failed dest is %s ret is %d", inet_ntoa_r(addr), ret);
            }

            //if error happend during send binary resultset
            //send error packet except OB_CONNECT_ERROR which means connection lost
            if (OB_SUCCESS != ret && OB_CONNECT_ERROR != ret)
            {
              ret = send_error_packet(packet, result);
            }
          }
        }
        else
        {
          TBSYS_LOG(ERROR, "alloc buffer from req->ms->pool failed request is %p", req);
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int ObMySQLServer::process_spr_packet(easy_buf_t *&buff, int64_t &buff_pos,
                                          easy_request_t *req, ObMySQLResultSet *result)
    {
      int ret = OB_SUCCESS;
      if (NULL == buff || NULL == req || NULL == result)
      {
        TBSYS_LOG(ERROR, "invalid argument buff is %p, req is %p, result is %p",
                  buff, req, result);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObMySQLSPRPacket sprpacket;
        sprpacket.set_statement_id(static_cast<uint32_t>(result->get_statement_id()));
        sprpacket.set_column_num(static_cast<uint16_t>(result->get_field_cnt()));
        sprpacket.set_param_num(
          static_cast<uint16_t>(result->get_param_columns().count()));
        sprpacket.set_warning_count(static_cast<uint16_t>(result->get_warning_count()));
        sprpacket.set_seq(static_cast<uint8_t>(number+1));
        ret = process_single_packet(buff, buff_pos, req, &sprpacket);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "process resheader packet failed ret is %d", ret);
        }
      }
      return ret;
    }

    int ObMySQLServer::process_resheader_packet(easy_buf_t *&buff, int64_t &buff_pos,
                                                easy_request_t *req, ObMySQLResultSet *result)
    {
      int ret = OB_SUCCESS;
      if (NULL == buff || NULL == req || NULL == result)
      {
        TBSYS_LOG(ERROR, "invalid argument buff is %p, req is %p, result is %p",
                  buff, req, result);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObMySQLResheaderPacket header;
        header.set_field_count(result->get_field_cnt());
        header.set_seq(static_cast<uint8_t>(number+1));
        ret = process_single_packet(buff, buff_pos, req, &header);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "process resheader packet failed ret is %d", ret);
        }
      }
      return ret;
    }

    int ObMySQLServer::process_field_packets(easy_buf_t *&buff, int64_t &buff_pos,
                              easy_request_t *req, ObMySQLResultSet *result, bool is_field)
    {
      int ret = OB_SUCCESS;
      if (NULL == buff || NULL == req || NULL == result)
      {
        TBSYS_LOG(ERROR, "invalid argument buff is %p, req is %p, result is %p",
                  buff, req, result);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        easy_addr_t addr = get_easy_addr(req);
        ObMySQLField field;
        int (ObMySQLResultSet::*get_next) (ObMySQLField &field);
        if (is_field)
        {
          get_next = &ObMySQLResultSet::next_field;
        }
        else
        {
          get_next = &ObMySQLResultSet::next_param;
        }

        while (OB_SUCCESS ==ret &&
               OB_SUCCESS == (ret = (result->*get_next)(field)))
        {
          ObMySQLFieldPacket fpacket(&field);
          fpacket.set_seq(static_cast<uint8_t>(number+1));
          ret = process_single_packet(buff, buff_pos, req, &fpacket);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "process field packet dest is %s failed ret is %d",
                      inet_ntoa_r(addr), ret);
          }
        }
        if (OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
        }
        else if (OB_CONNECT_ERROR == ret)
        {
          TBSYS_LOG(WARN, "libeasy connection error");
        }
        else
        {
          TBSYS_LOG(WARN, "failed to get next field, err=%d", ret);
          buff->last = buff->pos + buff_pos;
          req->opacket = reinterpret_cast<void*>(buff);
          int sret = send_raw_packet(req);
          if (OB_SUCCESS != sret)
          {
            TBSYS_LOG(ERROR, "send raw packet(dest is %s) failed ret is %d", inet_ntoa_r(addr), sret);
            //发包失败OB_CONN_ERROR 不再发后续的error包了
            ret = sret;
          }
        }
      }
      return ret;
    }

    int ObMySQLServer::process_eof_packet(easy_buf_t *&buff, int64_t &buff_pos,
                                          easy_request_t *req, ObMySQLResultSet *result)
    {
      int ret = OB_SUCCESS;
      if (NULL == buff || NULL == req || NULL == result)
      {
        TBSYS_LOG(ERROR, "invalid argument buff is %p, req is %p, result is %p",
                  buff, req, result);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        easy_addr_t addr = get_easy_addr(req);
        ObMySQLEofPacket eof;
        eof.set_warning_count(static_cast<uint16_t>(result->get_warning_count()));
        eof.set_seq(static_cast<uint8_t>(number+1));
        ret = process_single_packet(buff, buff_pos, req, &eof);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "process eof packet failed dest is %s ret is %d", inet_ntoa_r(addr), ret);
        }
      }
      return ret;
    }

    int ObMySQLServer::process_row_packets(easy_buf_t *&buff, int64_t &buff_pos,
                                           easy_request_t *req, ObMySQLResultSet *result, MYSQL_PROTOCOL_TYPE type)
    {
      int ret = OB_SUCCESS;
      if (NULL == buff || NULL == req || NULL == result)
      {
        TBSYS_LOG(ERROR, "invalid argument buff is %p, req is %p, result is %p",
                  buff, req, result);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        easy_addr_t addr = get_easy_addr(req);
        ObMySQLRow row;
        int64_t row_num = 0;
        while (OB_SUCCESS == ret
               && OB_SUCCESS == (ret = result->next_row(row)))
        {
          row_num++;
          row.set_protocol_type(type);
          ObMySQLRowPacket rpacket(&row);
          rpacket.set_seq(static_cast<uint8_t>(number+1));
          ret = process_single_packet(buff, buff_pos, req, &rpacket);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(ERROR, "process row packet failed, dest is %s ret is %d",
                      inet_ntoa_r(addr), ret);
          }
          else
          {
            TBSYS_LOG(DEBUG, "process row succ, dest is %s, seq=%d row_num=%ld",
                      inet_ntoa_r(addr), number, row_num);
          }
        }
        if (OB_ITER_END != ret)
        {
          result->set_errcode(ret);
          if (OB_CONNECT_ERROR == ret)
          {
            TBSYS_LOG(WARN, "libeasy connection error");
          }
          else
          {
            buff->last = buff->pos + buff_pos;
            req->opacket = reinterpret_cast<void*>(buff);
            int sret = send_raw_packet(req);
            if (OB_SUCCESS != sret)
            {
              TBSYS_LOG(ERROR, "send raw packet(dest is %s) failed ret is %d", inet_ntoa_r(addr), sret);
              //发包失败OB_CONN_ERROR 不再发后续的error包了
              ret = sret;
            }

            if (ret == OB_RESPONSE_TIME_OUT)
            {
              result->set_message("request timeout");
            }
            TBSYS_LOG(WARN, "failed to get next row, err=%d", ret);
          }
        }
        else
        {
          ret = OB_SUCCESS;
        }
      }
      return ret;
    }

    int ObMySQLServer::process_single_packet(easy_buf_t *&buff, int64_t &buff_pos,
                                             easy_request_t *req, ObMySQLPacket *packet)
    {
      int ret = OB_SUCCESS;
      int sret = OB_SUCCESS;
      if (NULL == buff || NULL == packet)
      {
        TBSYS_LOG(ERROR, "invalid argument buff is %p, packet is %p", buff, packet);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        easy_addr_t addr = get_easy_addr(req);
        ret = packet->encode(buff->pos, buff->end - buff->pos, buff_pos);
        if (OB_BUF_NOT_ENOUGH == ret || OB_ARRAY_OUT_OF_RANGE == ret || OB_SIZE_OVERFLOW == ret) //buff not enough to hold this packet
        {
          if (0 != buff_pos) //有数据先发送
          {
            buff->last = buff->pos + buff_pos;
            req->opacket = reinterpret_cast<void*>(buff);
            sret = send_raw_packet(req);
            if (OB_SUCCESS != sret)
            {
              TBSYS_LOG(ERROR, "send raw packet(dest is %s) failed ret is %d", inet_ntoa_r(addr), sret);
              if (OB_CONNECT_ERROR == sret)
              {
                ret = sret;
              }
            }
            else
            {
              //now we can reuse message buffer
              buff->pos = buff->pos - buff_pos;//libeasy 发包后会移动buf->pos 移回去
              buff_pos = 0;
              ret = packet->encode(buff->pos, buff->end - buff->pos, buff_pos);
            }
          }

          if (OB_SUCCESS == sret)
          {
            if (OB_ARRAY_OUT_OF_RANGE == ret || OB_SIZE_OVERFLOW == ret)
            {
              //buffer size > 6k 表示buffer的大小已经是2M了
              if (OB_MYSQL_PACKET_BUFF_SIZE < buff->end - buff->pos)
              {
                TBSYS_LOG(ERROR, "do not support packet larger than %ld ret is %d", OB_MAX_PACKET_LENGTH, ret);
              }
              else
              {
                TBSYS_LOG(WARN, "packet size is larger than 6k, try alloc 2M buffer");
                //alloc a 2m buffer
                buff = reinterpret_cast<easy_buf_t*>(easy_pool_alloc(req->ms->pool, OB_MAX_PACKET_LENGTH));
                if (NULL != buff)
                {
                  char *data_buffer = reinterpret_cast<char *>(buff + 1);
                  int64_t buffer_length = OB_MAX_PACKET_LENGTH - sizeof(easy_buf_t);
                  init_easy_buf(buff, data_buffer, req, buffer_length);
                  ret = packet->encode(buff->pos, buff->end - buff->pos, buff_pos);
                  if (OB_ARRAY_OUT_OF_RANGE == ret || OB_SIZE_OVERFLOW == ret)
                  {
                    TBSYS_LOG(ERROR, "do not support packet larger than %ld ret is %d", OB_MAX_PACKET_LENGTH, ret);
                  }
                  else if (OB_SUCCESS != ret)
                  {
                    TBSYS_LOG(ERROR, "serialize packet(%p) failed ret is %d", packet, ret);
                  }
                }
                else
                {
                  TBSYS_LOG(ERROR, "alloc memory from message failed");
                }
              }
            }
          }
        }

        if (OB_SUCCESS == ret)
        {
          number++; //increase number when packet serialize into buffer
        }
      }
      return ret;
    }

    int ObMySQLServer::send_raw_packet(easy_request_t *req)
    {
      int ret = OB_SUCCESS;
      if (NULL == req)
      {
        TBSYS_LOG(ERROR, "invalid argument req is %p", req);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {

        easy_buf_t *buf = static_cast<easy_buf_t*>(req->opacket);
        if (NULL != buf)
        {
          OB_STAT_INC(OBMYSQL, SQL_QUERY_BYTES, buf->last - buf->pos);
        }
        easy_client_wait_t wait_obj;
        wait_obj.done_count = 0;
        easy_client_wait_init(&wait_obj);
        req->client_wait = &wait_obj;
        req->retcode = EASY_AGAIN;
        //io线程被唤醒，r->opacket被挂过去,send_response->easy_connection_request_done
        easy_request_wakeup(req);
        // IO线程回调 int ObMySQLCallback::process(easy_request_t* r)的时候唤醒工作线程
        wait_client_obj(wait_obj);
        //return OB_CONNECT_ERROR if status eq EASY_CONN_CLOSE
        if (EASY_CONN_CLOSE == wait_obj.status)
        {
          TBSYS_LOG(WARN, "send error happen, quit current query");
          ret = OB_CONNECT_ERROR;
        }
        easy_client_wait_cleanup(&wait_obj);
        req->client_wait = NULL;
      }
      return ret;
    }

    int ObMySQLServer::post_raw_packet(easy_request_t *req)
    {
      int ret = OB_SUCCESS;
      if (NULL == req)
      {
        TBSYS_LOG(ERROR, "invalid argument req is %p", req);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        easy_buf_t *buf = static_cast<easy_buf_t*>(req->opacket);
        if (NULL != buf)
        {
          OB_STAT_INC(OBMYSQL, SQL_QUERY_BYTES, buf->last - buf->pos);
        }
        req->retcode = EASY_OK;
        //io线程被唤醒，r->opacket被挂过去,send_response->easy_connection_request_done
        easy_request_wakeup(req);
      }
      return ret;
    }

    int ObMySQLServer::do_stat(ObBasicStmt::StmtType stmt_type, int64_t consumed_time)
    {
      switch(stmt_type)
      {
        case ObBasicStmt::T_SELECT:
          OB_STAT_INC(OBMYSQL, SQL_SELECT_COUNT);
          OB_STAT_INC(OBMYSQL, SQL_SELECT_TIME, consumed_time);
          break;
        case ObBasicStmt::T_INSERT:
          OB_STAT_INC(OBMYSQL, SQL_INSERT_COUNT);
          OB_STAT_INC(OBMYSQL, SQL_INSERT_TIME, consumed_time);
          break;
        case ObBasicStmt::T_REPLACE:
          OB_STAT_INC(OBMYSQL, SQL_REPLACE_COUNT);
          OB_STAT_INC(OBMYSQL, SQL_REPLACE_TIME, consumed_time);
          break;
        case ObBasicStmt::T_UPDATE:
          OB_STAT_INC(OBMYSQL, SQL_UPDATE_COUNT);
          OB_STAT_INC(OBMYSQL, SQL_UPDATE_TIME, consumed_time);
          break;
        case ObBasicStmt::T_DELETE:
          OB_STAT_INC(OBMYSQL, SQL_DELETE_COUNT);
          OB_STAT_INC(OBMYSQL, SQL_DELETE_TIME, consumed_time);
          break;
        default:
          break;
      }
      return OB_SUCCESS;
    }
  }
}
