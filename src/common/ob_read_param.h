#ifndef OCEANBASE_COMMON_READ_PARAM_H_
#define OCEANBASE_COMMON_READ_PARAM_H_

#include "ob_define.h"
#include "ob_object.h"
#include "ob_range.h"
#include "ob_rowkey.h"

namespace oceanbase
{
  namespace common
  {
    /// @class ObReadParam  OB read parameter, API should not concern these parameters,
    ///   and mergeserver will directly ignore these parameters
    class ObReadParam
    {
    public:
      ObReadParam();
      virtual ~ObReadParam();

      /// @fn get data whose timestamp is newer or as new as the given timestamp,
      ///   -# when reading cs, if not setted, the result is decided by the server;
      ///   -# when reading ups, this parameter must be setted
      void set_version_range(const ObVersionRange & range);
      ObVersionRange get_version_range(void) const;

      /// @fn when reading cs, indicating whether the result (including intermediate result,
      /// like sstable block readed from sstable) of this operation should be cached.
      ///
      /// ups just ignores this parameter
      void set_is_result_cached(const bool cached);
      bool get_is_result_cached()const;

      void set_is_read_consistency(const bool cons);
      bool get_is_read_consistency()const;

      void reset(void);

      /// serailize or deserialization
      VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;

    protected:
      // RESERVE_PARAM_FIELD
      int serialize_reserve_param(char * buf, const int64_t buf_len, int64_t & pos) const;
      int deserialize_reserve_param(const char * buf, const int64_t data_len, int64_t & pos);
      int64_t get_reserve_param_serialize_size(void) const;

    protected:
      int8_t is_read_master_;
      int8_t is_result_cached_;
      ObVersionRange version_range_;
    };
  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_READ_PARAM_H_ */
