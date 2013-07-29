/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-19
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#ifndef OCEANBASE_ROOTSERVER_OB_CHUNK_SERVER_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_CHUNK_SERVER_MANAGER_H_
#include "common/ob_array_helper.h"
#include "common/ob_server.h"
#include "common/ob_array.h"
#include "common/ob_range2.h"
#include "ob_server_balance_info.h"
#include "ob_migrate_info.h"

namespace oceanbase
{
  namespace rootserver
  {
    // shutdown具体执行的操作:shutdown, restart
    enum ShutdownOperation
    {
      SHUTDOWN = 0,
      RESTART,
    };

    struct ObBalanceInfo
    {
      /// total size of all sstables for one particular table in this CS
      int64_t table_sstable_total_size_;
      /// total count of all sstables for one particular table in this CS
      int64_t table_sstable_count_;
      /// the count of currently migrate-in tablets
      int32_t curr_migrate_in_num_;
      /// the count of currently migrate-out tablets
      int32_t curr_migrate_out_num_;
      ObCsMigrateTo migrate_to_;

      ObBalanceInfo();
      ~ObBalanceInfo();
      void reset();
      void reset_for_table();
    };

    const int64_t CHUNK_LEASE_DURATION = 1000000;
    const int16_t CHUNK_SERVER_MAGIC = static_cast<int16_t>(0xCDFF);
    struct ObServerStatus
    {
      enum EStatus {
        STATUS_DEAD = 0,
        STATUS_WAITING_REPORT,
        STATUS_SERVING ,
        STATUS_REPORTING,
        STATUS_REPORTED,
        STATUS_SHUTDOWN,         // will be shut down
      };
      ObServerStatus();
      NEED_SERIALIZE_AND_DESERIALIZE;
      void set_hb_time(int64_t hb_t);
      void set_hb_time_ms(int64_t hb_t);
      bool is_alive(int64_t now, int64_t lease) const;
      bool is_ms_alive(int64_t now, int64_t lease) const;
      void dump(const int32_t index) const;
      const char* get_cs_stat_str() const;

      common::ObServer server_;
      volatile int64_t last_hb_time_;
      volatile int64_t last_hb_time_ms_;  //the last hb time of mergeserver,for compatible,we don't serialize this field
      EStatus ms_status_;
      EStatus status_;

      int32_t port_cs_; //chunk server port
      int32_t port_ms_; //merger server port
      int32_t port_ms_sql_;   /* for ms sql port */

      int32_t hb_retry_times_;        //no need serialize

      ObServerDiskInfo disk_info_; //chunk server disk info
      int64_t register_time_;       // no need serialize
      //used in the new rebalance algorithm, don't serialize
      ObBalanceInfo balance_info_;
      bool wait_restart_;
      bool can_restart_; //all the tablet in this chunkserver has safe copy num replicas, means that this server can be restarted;
    };
    class ObChunkServerManager
    {
      public:
        enum {
          MAX_SERVER_COUNT = 1000,
        };

        typedef ObServerStatus* iterator;
        typedef const ObServerStatus* const_iterator;

        ObChunkServerManager();
        virtual ~ObChunkServerManager();

        iterator begin();
        const_iterator begin() const;
        iterator end();
        const_iterator end() const;
        int64_t size() const;
        iterator find_by_ip(const common::ObServer& server);
        const_iterator find_by_ip(const common::ObServer& server) const;
        const_iterator get_serving_ms() const;
        // regist or receive heartbeat
        int receive_hb(const common::ObServer& server, const int64_t time_stamp,
            const bool is_merge_server = false, const int32_t sql_port = 0, const bool is_regist = false);
        int update_disk_info(const common::ObServer& server, const ObServerDiskInfo& disk_info);
        int get_array_length() const;
        ObServerStatus* get_server_status(const int32_t index);
        const ObServerStatus* get_server_status(const int32_t index) const;
        common::ObServer get_cs(const int32_t index) const;
        int get_server_index(const common::ObServer &server, int32_t &index) const;
        int32_t get_alive_server_count(const bool chunkserver) const;
        int get_next_alive_ms(int32_t & index, common::ObServer & server) const;

        void set_server_down(iterator& it);
        void set_server_down_ms(iterator& it);
        void reset_balance_info(int32_t max_migrate_out_per_cs);
        void reset_balance_info_for_table(int32_t &cs_num, int32_t &shutdown_num);
        bool is_migrate_infos_full() const;
        int add_migrate_info(ObServerStatus& cs, const common::ObNewRange &range, int32_t dest_cs_idx);
        int add_copy_info(ObServerStatus& cs, const common::ObNewRange &range, int32_t dest_cs_idx);
        int32_t get_max_migrate_num() const;

        int shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        void restart_all_cs();
        void cancel_restart_all_cs();
        int cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op);
        bool has_shutting_down_server() const;

        NEED_SERIALIZE_AND_DESERIALIZE;
        ObChunkServerManager& operator= (const ObChunkServerManager& other);
        int serialize_cs(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_ms(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const;
        int serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos) const;
      public:
        int write_to_file(const char* filename);
        int read_from_file(const char* filename, int32_t &cs_num, int32_t &ms_num);
      private:
        ObServerStatus data_holder_[MAX_SERVER_COUNT];
        common::ObArrayHelper<ObServerStatus> servers_;
        ObMigrateInfos migrate_infos_;
    };
  }
}
#endif
