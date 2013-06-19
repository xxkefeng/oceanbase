/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_compact_cell_iterator.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef OCEANBASE_COMMON_COMPACT_CELL_ITERATOR_H_
#define OCEANBASE_COMMON_COMPACT_CELL_ITERATOR_H_

#include "ob_buffer_helper.h"
#include "ob_cell_meta.h"
#include "ob_object.h"
#include "ob_row.h"
#include "ob_compact_store_type.h"
#include "sstable/ob_scan_column_indexes.h"

namespace oceanbase
{
  namespace common
  {
    /*
     * 紧凑格式读取类
     */
    class ObCompactCellIterator
    {
      public:
        ObCompactCellIterator();
        virtual ~ObCompactCellIterator() { }

      public:
        int init(const char *buf, const enum ObCompactStoreType);
        int init(const ObString &buf, const enum ObCompactStoreType);

        int get_row(ObRow& value, const sstable::ObSimpleColumnIndexes& query_columns);

        /**
         * 从buffer中反序列化得到一行(DENSE格式)
         * @param row_obj_array: row obj array(大小为max rowkey column number + 1,多出来的一个obj是为了反序列化行结束符)
         * @param column_count: [in]rowkey obj array size; [out]实际扫描到的column num
         */
        int get_one_row_dense(
            ObObj* const row_obj_array,
            int64_t& row_column_count);

        /**
         * 从buffer中反序列化得到一行(DENSE_DENSE格式)
         * @param row_obj_array: row obj array(大小为max row column number + 1,多出来的一个obj是为了反序列化行结束符)
         * @param max_row_scan_cnt: max row scan count(优化DENSE_DENSE读,读取的最多列数)
         * @param row_column_count: [in]row array size [out]final get row column count
         */
        int get_one_row_dense_dense(
            ObObj* const row_obj_array,
            const int64_t max_row_scan_count,
            int64_t& row_column_count);

        /**
         * 从buffer中反序列化得到一行(DENSE_SPARSE格式)
         * @param row_obj_array: row obj array(大小为max row column number + 1,多出来的一个obj是为了反序列化行结束符)
         * @param row_column_array: row column array(大小为max row column number + 1)
         * @param row_column_count: [in]row array size [out]final get row column count
         */
        int get_one_row_dense_sparse(
            ObObj* const row_obj_array,
            uint64_t* const row_column_array,
            int64_t& row_column_count);

        /*
         * 移动到下一个cell
         */
        int next_cell();

       /*
         * 取出cell 
         * @param column_id 列id，稠密格式下返回OB_INVALID_ID
         * @param value     cell
         * @param is_row_finished 是否到了一行的结束。到一行结束返回一个特殊的ObObj - OP_END_ROW
         * @param row             行结束时返回这一行的紧凑格式的内存区域
         */
        int get_cell(uint64_t &column_id, const ObObj *&value,
            bool *is_row_finished = NULL, ObString *row = NULL);

        /*
         * 用于稠密格式读取cell
         * 在带有column_id的稀疏格式读取数据使用此函数会出错
         */
        int get_cell(const ObObj *&value, bool *is_row_finished = NULL,
            ObString *row = NULL);

        /*
         * 因为效率问题，此函数过期
         */
        int get_cell(uint64_t &column_id, ObObj &value,
            bool *is_row_finished = NULL, ObString *row = NULL);

        void reset_iter();
        int64_t parsed_size();
        bool is_row_finished();

      private:
        int fill_row(const sstable::ObSimpleColumnIndexes& query_columns, common::ObRow& value, int64_t& filled);
        int get_sparse_rowvalue(int64_t filled, const sstable::ObSimpleColumnIndexes& query_columns, common::ObRow& value);

        virtual int parse_varchar(ObBufferReader &buf_reader, ObObj &value) const;
        int parse(ObBufferReader &buf_reader, ObObj &value, uint64_t *column_id = NULL);
        int parse_decimal(ObBufferReader &buf_reader, ObObj &value) const;

      private:
        ObBufferReader buf_reader_;
        uint64_t column_id_;
        ObObj value_;
	      uint64_t row_start_;
        enum ObCompactStoreType store_type_;
        int32_t step_;
        bool is_row_finished_;
        bool inited_;
    };

    inline bool ObCompactCellIterator::is_row_finished()
    {
      return is_row_finished_;
    }

    inline int64_t ObCompactCellIterator::parsed_size()
    {
      return buf_reader_.pos();
    }

    inline __attribute__((always_inline)) int ObCompactCellIterator::parse_varchar(ObBufferReader &buf_reader, ObObj &value) const
    {
      int ret = OB_SUCCESS;
      const int32_t *varchar_len = NULL;
      ObString str_value;

      ret = buf_reader.get<int32_t>(varchar_len);
      if(OB_SUCCESS == ret)
      {
        str_value.assign_ptr(const_cast<char*>(buf_reader.cur_ptr()), *varchar_len);
        buf_reader.skip(*varchar_len);
        value.set_varchar(str_value);
      }
      return ret;
    }
  }
}

#endif /* OCEANBASE_COMMON_COMPACT_CELL_ITERATOR_H_ */

