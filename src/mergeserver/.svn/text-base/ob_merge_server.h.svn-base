#ifndef OCEANBASE_MERGESERVER_MERGESERVER_H_
#define OCEANBASE_MERGESERVER_MERGESERVER_H_

#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/ob_server.h"
#include "common/ob_timer.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_config_manager.h"
#include "common/ob_privilege_manager.h"
#include "ob_merge_server_service.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServer : public common::ObSingleServer
    {
      public:
        ObMergeServer(ObConfigManager &config_mgr,
                      ObMergeServerConfig &ms_config);
      public:
        void set_privilege_manager(ObPrivilegeManager *privilege_manager);

      public:
        int initialize();
        int start_service();
        void destroy();
        int reload_config();

        int do_request(common::ObPacket* base_packet);
        //
        bool handle_overflow_packet(common::ObPacket* base_packet);
        void handle_timeout_packet(common::ObPacket* base_packet);
      public:
        common::ThreadSpecificBuffer::Buffer* get_response_buffer() const;

        common::ThreadSpecificBuffer* get_rpc_buffer();

        const common::ObServer& get_self() const;

        const common::ObServer& get_root_server() const;

        common::ObTimer& get_timer();

        bool is_stoped() const;

        const common::ObClientManager& get_client_manager() const;

        ObMergeServerConfig& get_config();
        ObConfigManager& get_config_mgr();
        mergeserver::ObMergerRpcProxy  *get_rpc_proxy() const{return service_.get_rpc_proxy();}
        mergeserver::ObMergerRootRpcProxy * get_root_rpc() const{return service_.get_root_rpc();}
        mergeserver::ObMergerAsyncRpcStub   *get_async_rpc() const{return service_.get_async_rpc();}
        common::ObMergerSchemaManager *get_schema_mgr() const{return service_.get_schema_mgr();}
        common::ObTabletLocationCacheProxy *get_cache_proxy() const{return service_.get_cache_proxy();}
        common::ObStatManager* get_stat_manager() const { return service_.get_stat_manager(); }
        
        inline const ObMergeServerService &get_service() const
        {
          return service_;
        }
        inline ObPrivilegeManager* get_privilege_manager()
        {
          return &privilege_mgr_;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObMergeServer);
        int init_root_server();
        int set_self(const char* dev_name, const int32_t port);
        // handle no response request add timeout as process time for monitor info
        void handle_no_response_request(common::ObPacket * base_packet);

      private:
        static const int64_t DEFAULT_LOG_INTERVAL = 100;
        int64_t log_count_;
        int64_t log_interval_count_;
        /* ObMergeServerParams& ms_params_; */
        ObConfigManager& config_mgr_;
        ObMergeServerConfig& ms_config_;
        common::ObTimer task_timer_;
        common::ThreadSpecificBuffer response_buffer_;
        common::ThreadSpecificBuffer rpc_buffer_;
        common::ObClientManager client_manager_;
        common::ObServer self_;
        common::ObServer root_server_;
        ObMergeServerService service_;
        common::ObPrivilegeManager privilege_mgr_;
    };
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVER_H_ */
