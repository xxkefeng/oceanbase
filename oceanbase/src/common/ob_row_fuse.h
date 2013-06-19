/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row_fuse.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_ROW_FUSE_H
#define _OB_ROW_FUSE_H 1

#include "common/ob_row.h"
#include "utility.h"

namespace test
{
  class ObRowFuseTest_apply_row_test_Test;
}

namespace oceanbase
{
  namespace common
  {
    class ObRowFuse
    {
      friend class test::ObRowFuseTest_apply_row_test_Test;

      public:
        /*
         * function join_row used in ObTabletFuse, ObTabletGet, ObTabletJoin
         */
        static int join_row(const ObRow *incr_row, const ObRow *sstable_row, ObRow *result);

        /*
         * used in ObMutipleScanMerge and ObMultipleGetMerge
         * @param 
         * row(in param):             the new row, which will be applied to result_row
         * result_row(in_out param):  the base row, which stores the origion value of the rowi
         *                            and also stores the fused result
         * is_row_empty(in_out param):tell whether is result_row is empty before fuse,
         *                            and also stores where the result_row is empty after fuse
         * @param
         */
        static int fuse_row(const ObRow &row, ObRow &result_row, bool &is_row_empty, bool is_ups_row);
      private:
        /*
         * fuse a ups row(row) with a cs row(result_row)
         * or a cs row(row) with a empty row(result_row)
         */
        static int fuse_sstable_row(const ObRow &row, ObRow &result_row, bool &is_row_empty);
        /*
         * fuse two ups rows
         */
        static int fuse_ups_row(const ObRow &row, ObRow &result_row, bool &is_row_empty);
        static int apply_row(const ObRow &row, ObRow &result_row, bool copy);
    };
  }
}

#endif /* _OB_ROW_FUSE_H */

