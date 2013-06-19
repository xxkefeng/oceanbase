/**
* (C) 2010-2013 Alibaba Group Holding Limited.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* Version: $Id$
*
* Authors:
*   baihua <bin.lb@alipay.com>
*/
#ifndef OCEANBASE_SQL_OB_SSTABLE_GETTER_H_
#define OCEANBASE_SQL_OB_SSTABLE_GETTER_H_

#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "common/ob_row.h"
#include "common/ob_rowkey.h"
#include "common/ob_array.h"
#include "ob_multi_cg_scanner.h"
#include "sstable/ob_scan_column_indexes.h"
#include "sstable/ob_sstable_reader.h"
#include "ob_sstable_block_getter.h"

namespace oceanbase
{

namespace sstable
{
class ObBlockCache;
class ObBlockIndexCache;
class ObSSTableRowCache;
class ObSSTableReader;
class ObBlockPositionInfo;
class ObBufferHandle;
}

namespace chunkserver
{
struct ObGetThreadContext;
}

namespace sql
{
class ObSqlGetParam;

class ObSSTableGetter : public ObRowIterator
{
  const static int64_t DEFAULT_ROW_BUF_SIZE = 1 << 16; // 64KB
  const static int64_t DEFAULT_BLOCK_BUF_SIZE = 1 << 17; // 128KB
public:
  ObSSTableGetter();
  virtual ~ObSSTableGetter();

  virtual int get_row_desc(const common::ObRowDesc *&row_desc) const
  {
    row_desc = &row_desc_;
    return common::OB_SUCCESS;
  }
  virtual int get_next_row(const common::ObRow *&row);

  // Reset and initialize
  int initialize(const uint64_t table_id,
      const common::ObArray<common::ObRowkey>*  rowkey_list,
      const uint64_t *columns, const int64_t column_size, const int64_t rowkey_cell_count, 
      chunkserver::ObGetThreadContext *get_context,
      bool is_ups_getter);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTableGetter);

  const common::ObRowkey* get_row_key(int64_t idx);

  // Get current row
  int get_row(const common::ObRow *&row);

  // Get row from rowcache
  // On success, return OB_SUCCESS or OB_SEARCH_NOT_FOUND (for rowkey not found)
  int row_from_rowcache(const common::ObRow *&row);
  // Get row from rowcache
  // On success, return OB_SUCCESS or OB_SEARCH_NOT_FOUND (for rowkey not found)
  int row_from_block(const common::ObRow *&row);

  int update_rowcache(bool found_in_block);

  void get_non_exist_row(const common::ObRowkey &key, const common::ObRow *&row);

  bool bloom_filter_may_contain();

  // init %column_index_, %row_desc_, and %rowkey_info_ if necessary.
  int init_by_schema(const sstable::ObSSTableSchema *schema);

  int build_row_desc(const int64_t rowkey_cell_count);

  inline bool is_ups_getter() { return is_ups_getter_; }

  // Init block getter
  // On success, return OB_SUCCESS or OB_SEARCH_NOT_FOUND, see 
  // sql::ObSSTableBlockGetter::initalize.
  int init_block_getter(const sstable::ObBlockPositionInfo &pos);

private:
  uint64_t table_id_;
  const common::ObArray<common::ObRowkey>*  rowkey_list_;
  const uint64_t *columns_;
  int64_t column_size_;

  common::ObRowDesc row_desc_;
  common::ObRowkeyInfo rowkey_info_;
  sstable::ObSimpleColumnIndexes column_index_;

  int64_t cur_idx_; // current index

  // Getter context
  chunkserver::ObGetThreadContext *context_;
  /*
  sstable::ObBlockIndexCache *block_index_cache_;
  sstable::ObBlockCache *block_cache_;
  sstable::ObSSTableRowCache *row_cache_;

  const sstable::ObSSTableReader * const *readers_;
  int64_t reader_size_;
  */

  bool is_ups_getter_;

  // buffer for bloom filter key
  common::ObMemBuf bf_key_;
  // buffer for cached row 
  common::ObMemBuf cache_row_buf_;
  // buffer for block
  common::ObMemBuf block_buf_;

  // Row value for key not exist.
  common::ObRow row_non_exist_;

  ObSSTableBlockGetter block_getter_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_OB_SSTABLE_GETTER_H_
