#ifndef OCEANBASE_SQL_READ_SIMPLE_PARAM_H_
#define OCEANBASE_SQL_READ_SIMPLE_PARAM_H_

#include "common/ob_define.h"
#include "common/ob_object.h"
#include "common/ob_range.h"
#include "common/ob_rowkey.h"
#include "common/ob_array_helper.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    /// @class ObSqlReadSimpleParam  OB read parameter, API should not concern these parameters,
    ///   and mergeserver will directly ignore these parameters
    class ObSqlReadSimpleParam
    {
    public:
      ObSqlReadSimpleParam();
      virtual ~ObSqlReadSimpleParam();

      void reset(void);

      void set_is_read_consistency(const bool cons) { is_read_master_ = cons;}
      bool get_is_read_consistency() const {return is_read_master_;}

      void set_is_result_cached(const bool cached) {is_result_cached_ = cached;}
      bool get_is_result_cached() const {return is_result_cached_;}

      void set_is_only_static_data(bool is_only_static_data){is_only_static_data_=is_only_static_data;}
      bool get_is_only_static_data() const { return is_only_static_data_;}

      void set_data_version(const int64_t data_version) {data_version_ = data_version;}
      int64_t get_data_version() const { return data_version_; }

      inline int add_column(uint64_t column_id)
      {
        return column_id_list_.push_back(column_id)?common::OB_SUCCESS : common::OB_SIZE_OVERFLOW;
      }
      inline int64_t get_column_id_size() const { return column_id_list_.get_array_index(); }
      inline const uint64_t* const get_column_ids() const { return column_ids_; }
      inline void set_table_id(const uint64_t &renamed_table_id, const uint64_t& table_id) 
      {
        table_id_ = table_id;
        renamed_table_id_ = renamed_table_id;
      }
      inline uint64_t get_table_id() const { return table_id_; }
      inline uint64_t get_renamed_table_id() const { return renamed_table_id_; }
      
      inline void set_request_timeout(const int64_t timeout) {request_timeout_ = timeout; }
      inline int64_t get_request_timeout() const { return request_timeout_; }

      int64_t to_string(char *buf, const int64_t buf_len) const;
      NEED_SERIALIZE_AND_DESERIALIZE;
    protected:
      bool is_read_master_;
      bool is_result_cached_;
      bool is_only_static_data_;
      int64_t data_version_;
      uint64_t table_id_;
      uint64_t renamed_table_id_;//check if this param is needed
      int64_t request_timeout_;
      uint64_t column_ids_[common::OB_MAX_COLUMN_NUMBER]; //普通列, 复合列要在project里面做。refer sstable/ob_sstable_scan_param.h how to serailze
      ObArrayHelper<uint64_t> column_id_list_;
    };
  } /* sql */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_SQL_READ_SIMPLE_PARAM_H_ */
