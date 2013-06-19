/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_table_option.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_TABLE_OPTION_H_
#define OCEANBASE_SQL_OB_TABLE_OPTION_H_
#include "common/ob_object.h"
#include "common/ob_string.h"
#include "common/ob_define.h"

namespace oceanbase
{
  namespace sql
  {
    struct ObTableOption
    {
      ObTableOption()
      {
        tablet_max_size_ = common::OB_DEFAULT_MAX_TABLET_SIZE;
        tablet_block_size_ = common::OB_DEFAULT_SSTABLE_BLOCK_SIZE;
        replica_num_ = common::OB_SAFE_COPY_COUNT;
        character_set_ = common::OB_DEFAULT_CHARACTER_SET;
        use_bloom_filter_ = false;
        read_static_ = false;
      }

      void print_indentation(FILE* fp, int32_t level) const;
      void print(FILE* fp, int32_t level);
      
      int64_t                     tablet_max_size_;
      int64_t                     tablet_block_size_;
      int32_t                     replica_num_;
      int32_t                     character_set_;
      bool                        use_bloom_filter_;
      bool                        read_static_;
      common::ObString            expire_info_;
      common::ObString            compress_method_;
    };

    inline void ObTableOption::print_indentation(FILE* fp, int32_t level) const
    {
      for(int i = 0; i < level; ++i)
        fprintf(fp, "    ");
    }
    inline void ObTableOption::print(FILE* fp, int32_t level)
    {
      if (tablet_max_size_ > 0)
      {
        print_indentation(fp, level);
        fprintf(fp, "tablet_max_size = %ld\n", tablet_max_size_);
      }
      if (tablet_block_size_ > 0)
      {
        print_indentation(fp, level);
        fprintf(fp, "tablet_block_size = %ld\n", tablet_block_size_);
      }
      if (replica_num_ > 0)
      {
        print_indentation(fp, level);
        fprintf(fp, "replica_num = %d\n", replica_num_);
      }
      if (character_set_ > 0)
      {
        print_indentation(fp, level);
        fprintf(fp, "character_set = %d\n", character_set_);
      }
      if (use_bloom_filter_)
      {
        print_indentation(fp, level);
        fprintf(fp, "use_bloom_filter = TRUE\n");
      }
      if (read_static_)
      {
        print_indentation(fp, level);
        fprintf(fp, "read_static = TRUE\n");
      }
      if (expire_info_.length() > 0)
      {
        print_indentation(fp, level);
        fprintf(fp, "expire_info = '%.*s'\n", expire_info_.length(), expire_info_.ptr());
      }
      if (compress_method_.length() > 0)
      {
        print_indentation(fp, level);
        fprintf(fp, "compress_method = '%.*s'\n", compress_method_.length(), compress_method_.ptr());
      }
    }
  }
}

#endif /* OCEANBASE_SQL_OB_TABLE_OPTION_H_ */

