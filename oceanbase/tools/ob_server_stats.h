#ifndef OCEANBASE_TOOLS_SERVER_STATS_H
#define OCEANBASE_TOOLS_SERVER_STATS_H

#include <set>
#include <vector>
#include <string>
#include "common/ob_define.h"
#include "common/ob_statistics.h"
#include "stats.h"
#include "client_rpc.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace tools
  {


    struct Present
    {
      public:
        static const int32_t SERVER_COUNT = common::OB_MAX_MOD_NUMBER;
        static const int64_t SERVER_TYPE_DAILY_MERGE = SERVER_COUNT;
        static const char *rs_map[];
        static const char *ups_map[];
        static const char *cs_map[];
        static const char *ms_map[];
        static const char *common_map[];
        static const char *sql_map[];
        static const char *obmysql_map[];
        static const char *sstable_map[];

      public:
        static const char* field_name(const common::ObStatMod mod, const uint32_t entry) ; 
        static const int64_t field_width(const common::ObStatMod mod, const uint32_t entry);
        static const int64_t mod_size(const common::ObStatMod mod);
        static const char** strmap(const common::ObStatMod mod);
      public:
        Present();

    };

    // dataserver Access Stat. Info
    class ObServerStats : public Stats
    {
      public:
        explicit ObServerStats(ObClientRpcStub &stub, const common::ObStatMod mod)
          : rpc_stub_(stub), show_header_(50), show_date_(1)
        {
          //current_.set_server_type(0);
          mod_ = mod;
        }

        virtual ~ObServerStats() {}
        void add_index_filter(const uint32_t index) { index_filter_.push_back(index); }
        void add_table_filter(const uint64_t table) { table_filter_.insert(table); }
        void set_show_header(const int32_t show) { show_header_ = show; }
        void set_show_date(const int32_t date) { show_date_ = date; }
        void clear_table_filter() { table_filter_.clear(); }
        void clear_index_filter() { index_filter_.clear(); }

      public:
        virtual int32_t output(const int32_t count, const int32_t interval);
        virtual int32_t init();

      protected:
        virtual int32_t calc() ;
        virtual int32_t save() ;
        virtual int32_t refresh() ;

      private:
        void output_header();
        void initialize_empty_value();
        int  calc_hit_ratio(oceanbase::common::ObStat &stat_item,
            const int ratio, const int hit, const int miss);
        int calc_div_value(oceanbase::common::ObStat &stat_item,
            const int div, const int time, const int count);
        int64_t print_value(oceanbase::common::ObStatManager::const_iterator it,
            const uint32_t index) const;

      protected:
        ObClientRpcStub &rpc_stub_;
        oceanbase::common::ObStatManager current_;
        std::vector<uint32_t> index_filter_;
        std::set<uint64_t> table_filter_;
        int32_t show_header_;
        int32_t show_date_;
        common::ObStatMod mod_;
    };


  }
}


#endif //OCEANBASE_TOOLS_SERVER_STATS_H

