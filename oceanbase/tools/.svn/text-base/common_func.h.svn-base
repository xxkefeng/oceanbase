/*
 *  (C) 2007-2010 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License version 2 as
 *  published by the Free Software Foundation.
 *
 *         common_func.h is for what ...
 *
 *  Version: $Id: common_func.h 2010年11月17日 16时18分07秒 qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */


#include "common/ob_range.h"
#include "common/ob_range2.h"
#include "common/ob_scanner.h"
#include "common/ob_new_scanner.h"
#include "sql/ob_sql_scan_param.h"
#include "chunkserver/ob_tablet_image.h"

namespace oceanbase
{

namespace sstable
{
class ObSSTableScanParam;
}

}

int64_t random_number(int64_t min, int64_t max);
int parse_number_range(const char *number_string,
    int32_t *number_array, int32_t &number_array_size);
int parse_range_str(const char* range_str, int hex_format, oceanbase::common::ObNewRange &range);
void dump_scanner(oceanbase::common::ObScanner &scanner);
void dump_scanner(oceanbase::common::ObNewScanner &scanner);
int dump_tablet_info(oceanbase::common::ObScanner &scanner);
int parse_rowkey(const char* rowkey_type_str, const char* rowkey_value_str,
    oceanbase::common::PageArena<char>& allocer, oceanbase::common::ObRowkey &rowkey);
void init_obj_type_map_();
void dump_multi_version_tablet_image(oceanbase::chunkserver::ObMultiVersionTabletImage & image, bool load_sstable);
void dump_tablet_image(oceanbase::chunkserver::ObTabletImage & image, bool load_sstable);
int dump_tablet(const oceanbase::chunkserver::ObTablet & tablet, const bool dump_sstable);


struct QueryParam
{
  int64_t table_id;
  int64_t version;
  int64_t end_version;
  bool is_async_read;
  bool is_result_cached;
  const char* scan_range;
  const char* query_columns;
};

int build_scan_param(const QueryParam& query_param, 
    oceanbase::sql::ObSqlScanParam& scan_param, 
    oceanbase::common::ObRowDesc& row_desc);
int build_scan_param(
    const oceanbase::common::ObNewRange& range,  
    const int32_t* query_column_array, 
    const int32_t query_column_size, 
    const int64_t query_data_version,
    oceanbase::sql::ObSqlScanParam& scan_param,
    oceanbase::common::ObRowDesc& row_desc);
int build_sstable_scan_param(
    const QueryParam &query_param, const QueryParam *local_index_param,
    oceanbase::sstable::ObSSTableScanParam &scan_param,
    oceanbase::common::ObRowDesc& row_desc);

int fill_project(int64_t table_id, int64_t column_id, oceanbase::sql::ObProject& project);
int fill_scan_project(const int64_t table_id, const int32_t* query_column_array, 
    const int32_t query_column_size, oceanbase::sql::ObSqlScanParam& scan_param, 
    oceanbase::common::ObRowDesc& row_desc);
void dump_scanner(oceanbase::common::ObNewScanner &scanner, oceanbase::common::ObRowDesc& row_desc);
void dump_phy_operator(oceanbase::sql::ObPhyOperator &op);

int build_scan_param(const QueryParam& query_param, oceanbase::common::ObScanParam& scan_param);

