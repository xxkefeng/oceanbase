#ifndef TEST_COMPACTSSTABLEV2_COMMON
#define TEST_COMPACTSSTABLEV2_COMMON

#include "gtest/gtest.h"
#include "common/ob_define.h"
#include "common/ob_cell_meta.h"
#include "ob_fileinfo_cache.h"
#include "compactsstablev2/ob_compact_sstable_reader.h"
#include "compactsstablev2/ob_compact_sstable_writer.h"
#include "compactsstablev2/ob_compact_sstable_scanner.h"
#include "compactsstablev2/ob_sstable_block_cache.h"
#include "compactsstablev2/ob_sstable_block_index_cache.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"
#include "sstable/ob_sstable_trailer.h"
#include "sstable/ob_sstable_block_index_builder.h"
#include "sstable/ob_sstable_writer.h"

using namespace oceanbase;

/**
 *make version range
 *@param version_range:version_range
 *@param version_flag:0(vaild version)
 *                    1(invlid major version)
 *                    2(invaild major frozen time)
 *                    3(invalid next transaction id)
 *                    4(invalid next commit log id)
 *                    5(invalid start minor version)
 *                    6(invalid end minor version)
 *                    7(invlid is final minor version)
 */
int make_version_range(compactsstablev2::ObFrozenMinorVersionRange& version_range, const int version_flag);


/**
 *make compressor name
 *@param comp_name:compressor name
 *@param comp_flag:0(不压缩)
 *                 1(invalid)
 *                 2(none)
 *                 3(lzo_1.0)
 *                 4(snappy_1.0)
 *                 5(lz4_1.0)
 */
int make_comp_name(common::ObString& comp_name, const int comp_flag);

/**
 * create compressor/decompressor
 *@param comp_flag:0(不压缩)
 *                 1(invalid)
 *                 2(none)
 *                 3(lzo_1.0)
 *                 4(snappy_1.0)
 *                 5(lz4_1.0)
 */
int make_compressor(ObCompressor*& compressor, const int comp_flag);

/**
 *make file path
 *@param file_path:file path
 *@param file_num: >=0(valid filename)
 *                 -1(invalid filename)
 *                 -2(empty filepath)
 */
int make_file_path(common::ObString& file_path, const int64_t file_num);

/**
 *make column def
 *@param def: column def
 *@param table_id: table id
 *@param column_id: column id
 *@param value_type: value type
 *@param rowkey_seq: rowkey seq
 */
void make_column_def(compactsstablev2::ObSSTableSchemaColumnDef& def, 
    const uint64_t table_id,
    const uint64_t column_id, 
    const int value_type,
    const int64_t rowkey_seq);

/**
 *make schema
 *rowkey: column_id type                 value
 *           1       ObVarcharType        prefix
 *           2       ObIntType            (int)
 *rwovalue: column_id    type    value
 *           3        ObDateTimeType     (clumn_id==2)value;
 *           4        ObPreciseDateTimeType (column_id==2)value;
 *           5        ObVarcharType       (column_id==2)value;
 *           6        ObCreateTimeType     (column_id==2)value;
 *           7        ObModifyTimeType     (column_id==2)value;
 *           8        ObBoolType           (column_id==2)value%2;
 *           9        ObDecimalType        (column_id==2)value;
 *@param shcema: schema
 *@param table_id: table id
 */
int make_schema(compactsstablev2::ObSSTableSchema& schema, const uint64_t table_id);

/**
 *make rowkey
 *@param rowkey: rowkey
 *@param data: rowkey num
 *@param flag: 0(normal)
 *             1(min)
 *             2(max)
 */
int make_rowkey(common::ObRowkey& rowkey, const int64_t data, const int flag);

/**
 *make row
 *@param row: row
 *@param table_id: table id
 *@param data: rowkey num
 *@param flag: 0(normal, write)
 *             1(del row, write)
 *             2(new add, write)
 *             3(no row value, write)
 *             4(normal, scan, dense_dense)
 *             5(normal, scan, dense_sparse)
 *             6(del row, scan, dense_sparse)
 *             7(new add, scan, dense_sparse),
 *             8(no row value, scan, dense_dense)
 *             9(no row value, scan, dense_sparse)
 */
int make_row(common::ObRow& row, const uint64_t table_id, const int64_t data, const int flag);

/**
 *make range
 *@param range: table range
 *@param table_id: table id
 *@param start: stard key id
 *@param end: end key id
 *@param start_flag: 0(min)
 *                   1(uninclusive start)
 *                   2(inclusvie start)
 *@param end_flag: 0(max)
 *                 1(uninclusive end)
 *                 2(inclusive end)
 */
int make_range(common::ObNewRange& range,
    const uint64_t table_id,
    const int64_t start,
    const int64_t end,
    const int start_flag,
    const int end_flag);

/**
 *make scan param
 *@param sstable_scan_param: sstable scan param
 *@param table_id: table id
 *@param start: start
 *@param end: end
 *@param start_flag: 0(min)
 *                   1(uninclusive start)
 *                   2(inclusive start)
 *@param end_flag: 0(max)
 *                 1(uninclusive end)
 *                 2(inclusive end)
 *@param scan_flag: 0(forward)
 *                  1(backward)
 *@param read_flag: 0(sync)
 *                  1(preread)
 *@param column_array: column array
 *@param column_cnt: column count
 *@param not_exit_col_ret_nop: not exit_col_ret_nop
 *@param full_row_scan_flag: 0(full row scan)
 *                           1(not full row scan)
 *@param daily_merge_scan_flag: 0(daily merge scan)
 *                              1(not daily merge scan)
 *@param rowkey_column_cnt: rowkey column count
 */
int make_scan_param(sstable::ObSSTableScanParam& sstable_scan_param,
    const uint64_t table_id,
    const int64_t start,
    const int64_t end,
    const int start_flag,
    const int end_flag,
    const int scan_flag,
    const int read_flag,
    const uint64_t* column_array,
    const int64_t column_cnt,
    const bool not_exit_col_ret_nop,
    const uint64_t full_row_scan_flag,
    const uint64_t daily_merge_scan_flag,
    const int64_t rowkey_column_cnt);

/**
 *create file for test
 *block size=1024,(15row/block),6 block,DENSE_DENSE
 *block size=1024,(11row/block),9 block,DENSE_SPARSE
 *@param file_id:sstable file id
 *@param comp_flag: 0(不压缩)
 *                  1(invalid)
 *                  2(none)
 *                  3(lzo_1.0)
 *                  4(snappy_1.0)
 *                  5(lz4_1.0)
 *@param store_type: DENSE_SPARSE
 *                   DENSE_DENSE
 *@param table_id: table id array
 *@param table_count: table count
 *@param range_start:
 *@param range_end:
 *@param range_start_flag: 0(min)
 *                         1(uninclusive start)
 *                         2(inclusive start)
 *@param range_end_flag: 0(max)
 *                       1(uninclusive end)
 *                       2(inclusive end)
 *@param row_data: row data array
 *@param ext_flag: 0(normal)
 *                 1(del row)
 *                 2(new add)
 *                 3(no row value)
 *@param row_count: row count array
 */
int create_file(const uint64_t file_id,
    const int comp_flag,
    const common::ObCompactStoreType store_type,
    const uint64_t* table_id,
    const int64_t table_count,
    const int64_t* range_start,
    const int64_t* range_end,
    const int* range_start_flag,
    const int* range_end_flag,
    const int64_t* row_data,
    const int* ext_flag,
    const int64_t* row_count);

/**
 * delete file(i.sst)
 * @param i: file id
 */
void delete_file(const int64_t i);

#endif
