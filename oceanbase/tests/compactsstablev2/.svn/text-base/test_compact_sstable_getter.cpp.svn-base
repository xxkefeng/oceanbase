#include "gtest/gtest.h"
#include "common/ob_define.h"
#include "common/ob_cell_meta.h"
#include "ob_fileinfo_cache.h"
#include "compactsstablev2/ob_compact_sstable_reader.h"
#include "compactsstablev2/ob_compact_sstable_writer.h"
#include "compactsstablev2/ob_compact_sstable_getter.h"
#include "compactsstablev2/ob_sstable_block_cache.h"
#include "compactsstablev2/ob_sstable_block_index_cache.h"
#include "sstable/ob_blockcache.h"
#include "sstable/ob_block_index_cache.h"

using namespace oceanbase;
using namespace common;
using namespace compactsstablev2;
using namespace chunkserver;
using namespace sstable;

static FileInfoCache g_fic;
static ObSSTableBlockCache g_block_cache(g_fic);
static ObSSTableBlockIndexCache g_index_cache(g_fic);
static ObSSTableRowCache g_row_cache;


static const int64_t table_count = 1;
static const uint64_t table_id = 1001;
static const uint64_t file_id = 1;

static ObObj s_rowkey_buf_arrays[OB_MAX_ROWKEY_COLUMN_NUMBER * 50];



class TestCompactSSTableGetter : public ::testing::Test
{
  public:

  enum
  {
    FIRST_50_ROW = 1,
    MIDDLE_50_ROW,
    END_50_ROW
  };  

  enum
  {
    GET_FULL_COLUMN = 1,
    GET_4_COLUMN,
    GET_CONTAIN_ROWKEY
  };

  enum
  {
    NORMAL_ROW = 1,
    EMPTY_ROW,
    NOT_EXIST_ROW,
    DEL_ROW,
    ADD_ROW     
  };

  enum
  {
    ROWKEY_MIN = 1,
    ROWKEY_MAX,
    ROWKEY_NOMAL
  };

  enum
  {
    RANGE_MIN = 1,
    RANGE_MAX,
    RANGE_INCLUSIVE,
    RANGE_UNINCLUSIVE
  };
    
  enum
  {
    EXCEPTION_TABLE_NOT_EXIST = 1,
    EXCEPTION_READER_NULL,
    EXCEPTION_ROWCOUNT_0
  };  

  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
    int ret = OB_SUCCESS;

    ret = g_fic.clear();
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = g_block_cache.clear();
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = g_index_cache.clear();
    ASSERT_EQ(OB_SUCCESS, ret);
  
    ret = g_row_cache.clear();
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  static void SetUpTestCase()
  {
    int ret = OB_SUCCESS;
    ret = g_fic.init(1024);
    ASSERT_EQ(OB_SUCCESS, ret);

    //ObSSTableBlockCacheConf block_cache_conf;
    //block_cache_conf.block_cache_memsize_mb = 10;
    const int64_t cache_mem_size = 10 * 1024 * 1024;
    ret = g_block_cache.init(cache_mem_size);
    ASSERT_EQ(OB_SUCCESS, ret);

    //ObSSTableBlockIndexCacheConf index_cache_conf;
    //index_cache_conf.cache_mem_size = 10 * 1024 * 1024;
    const int64_t index_cache_mem_size = 10 * 1024 * 1024;
    ret = g_index_cache.init(index_cache_mem_size);
    ASSERT_EQ(OB_SUCCESS, ret);

    const int64_t row_cache_mem_size = 10 * 1024 * 1024;
    ret = g_row_cache.init(row_cache_mem_size);
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  static void TearDownTestCase()
  {
    int ret = OB_SUCCESS;
    ret = g_fic.destroy();
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = g_block_cache.destroy();
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = g_index_cache.destroy();
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = g_row_cache.destroy();
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  /**
   *make version range
   *@param version_range:version_range
   *@param version_flag:0(vaild version)
   *                    1(invaild major frozen time)
   *                    2(invalid major transaction id)
   *                    3(invalid next commit log id)
   *                    4(invalid start minor version)
   *                    5(invalid end minor version)
   *                    6(invlid is final minor version)
   */
  int make_version_range(ObFrozenMinorVersionRange& version_range,
      const int version_flag)
  {
    int ret = OB_SUCCESS;
    version_range.major_version_ = 10;
    version_range.major_frozen_time_ = 2;
    version_range.next_transaction_id_ = 3;
    version_range.next_commit_log_id_ = 4;
    version_range.start_minor_version_ = 5;
    version_range.end_minor_version_ = 6;
    version_range.is_final_minor_version_ = 1;

    if (0 == version_flag)
    {
    }
    else if (1 == version_flag)
    {
      version_range.major_frozen_time_ = -1;
    }
    else if (2 == version_flag)
    {
      version_range.major_frozen_time_ = -1;
    }
    else if (3 == version_flag)
    {
      version_range.next_transaction_id_ = -1;
    }
    else if (4 == version_flag)
    {
      version_range.next_commit_log_id_ = -1;
    }
    else if (5 == version_flag)
    {
      version_range.start_minor_version_ = -1;
    }
    else if (6 == version_flag)
    {
      version_range.is_final_minor_version_ = -1;
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid version flag:version_flag=%d",
          version_flag);
      ret = OB_ERROR;
    }

    return ret;
  }

  /**
   *make compressor name
   *@param comp_name:compressor name
   *@param comp_flag:0(none),1(invalid),2(lzo_1.0),3(snappy)
   *
   */
  int make_comp_name(ObString& comp_name, const int comp_flag)
  {
    int ret = OB_SUCCESS;
    static char s_comp_name_buf[1024];
    memset(s_comp_name_buf, 0, 1024);

    if (comp_flag < 0 || comp_flag > 3)
    {
      TBSYS_LOG(ERROR, "invlid comp_flag:comp_flag=%d", comp_flag);
      ret = OB_ERROR;
    }
    else if (0 == comp_flag)
    {
      comp_name.assign_ptr(NULL, 0);
    }
    else if (1 == comp_flag)
    {
      memcpy(s_comp_name_buf, "invalid", 10);
      comp_name.assign_ptr(s_comp_name_buf, 10);
    }
    else if (2 == comp_flag)
    {
      memcpy(s_comp_name_buf, "lzo_1.0", 10);
      comp_name.assign_ptr(s_comp_name_buf, 10);
    }
    else if (3 == comp_flag)
    {
      memcpy(s_comp_name_buf, "snappy", 10);
      comp_name.assign_ptr(s_comp_name_buf, 10);
    }
    else
    {
      TBSYS_LOG(ERROR, "invlid comp_flag:comp_flag=%d", comp_flag);
      ret = OB_ERROR;
    }

    return ret;
  }

  /**
   *make file path
   *@param file_path:file path
   *@param file_num:>=0(valid filename),-1(invalid filename)
   *                -2(empty filepath)
   */
  int make_file_path(ObString& file_path, const int64_t file_num)
  {
    int ret = OB_SUCCESS;

    static char s_file_path_buf[1024];
    memset(s_file_path_buf, 0, 1024);

    if (file_num >= 0)
    {
      snprintf(s_file_path_buf, 1024, "%ld%s", file_num, ".sst");
      file_path.assign_ptr(s_file_path_buf, 10);
    }
    else if (-1 == file_num)
    {
      snprintf(s_file_path_buf, 1024, "tt/1.sst");
      file_path.assign_ptr(s_file_path_buf, 10);
    }
    else if (-2 == file_num)
    {
      file_path.assign_ptr(s_file_path_buf, 0);
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid file_num:file_num=%ld", file_num);
      ret = OB_ERROR;
    }

    return ret;
  }

  /**
   *make column def
   *@param def: column def
   *@param table_id: table id
   *@param column_id: column id
   *@param value_type: value type
   *@param rowkey_seq: rowkey seq
   */
  void make_column_def(compactsstablev2::ObSSTableSchemaColumnDef& def, 
      const uint64_t table_id, const uint64_t column_id, 
      const int value_type, const int64_t rowkey_seq)
  {
    def.table_id_ = table_id;
    def.column_id_ = column_id;
    def.column_value_type_ = value_type;
    def.rowkey_seq_ = rowkey_seq;
  }

  /**
   *make schema
   *@param shcema: schema
   *@param table_id: table id
   *@param flag:
   *   --0(rowkey<2-11>,rowvalue<12-21>,ObIntType)
   */
  void make_schema(compactsstablev2::ObSSTableSchema& schema,
      const uint64_t table_id, const int flag)
  {
    int ret = OB_SUCCESS;
    schema.reset();
    compactsstablev2::ObSSTableSchemaColumnDef def;

    if (0 == flag)
    {
      for (int64_t j = 0; j < 10; j ++)
      {
        //rowkey seq:1---10
        //column id:2----11
        make_column_def(def, table_id,
            static_cast<uint64_t>(j + 2), ObIntType, j + 1);
        ret = schema.add_column_def(def);
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      for (int64_t j = 10; j < 20; j ++)
      {
        //rowkey seq:0
        //column id:12----21
        make_column_def(def, table_id, static_cast<uint64_t>(j + 2),
            ObIntType, 0);
        ret = schema.add_column_def(def);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid flag:flag=%d", flag);
      ret = OB_ERROR;
    }
  }

  /**
   *make range
   *@param range: table range
   *@param table_id: table id
   *@param start: stard key id
   *@param end: end key id
   *@param start_flag: 0(min),-1(uninclusive start),-2(inclusvie start)
   *@param end_flag: 0(max),-1(uninclusive end),-2(inclusive end)
   */
  void make_range(ObNewRange& range, const uint64_t table_id,
      const int64_t start, const int64_t end,
      const int start_flag, const int end_flag)
  {
    int ret = OB_SUCCESS;

    static ObObj s_startkey_buf_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
    static ObObj s_endkey_buf_array[OB_MAX_ROWKEY_COLUMN_NUMBER];

    ObObj obj;
    int64_t remain = 0;
    int64_t yushu = 0;

    //range.table_id_
    range.reset();
    range.table_id_ = table_id;

    //range.start_key_
    if (RANGE_MIN == start_flag)
    {
      range.start_key_.set_min_row();
      range.border_flag_.unset_inclusive_start();
      range.border_flag_.set_min_value();
    }
    else
    {
      remain = start;
      for (int64_t i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        s_startkey_buf_array[12 - 3 - i] = obj;
        remain = (remain - yushu) / 10;
      }

      if (RANGE_UNINCLUSIVE == start_flag)
      {
        range.border_flag_.unset_inclusive_start();
      }
      else if (RANGE_INCLUSIVE == start_flag)
      {
        range.border_flag_.set_inclusive_start();
      }
      else
      {
        TBSYS_LOG(ERROR, "invalid end_flag:start_flag=%d", start_flag);
        ret = OB_ERROR;
      }

      range.start_key_.assign(s_startkey_buf_array, 10);
    }

    //end key
    if (RANGE_MAX == end_flag)
    {
      range.end_key_.set_max_row();
      range.border_flag_.unset_inclusive_end();
      range.border_flag_.set_max_value();
    }
    else
    {
      remain = end;
      for (int64_t i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        s_endkey_buf_array[12 - 3 - i] = obj;
        remain = (remain - yushu) / 10;
      }

      if (RANGE_UNINCLUSIVE == end_flag)
      {
        range.border_flag_.unset_inclusive_end();
      }
      else if (RANGE_INCLUSIVE == end_flag)
      {
        range.border_flag_.set_inclusive_end();
      }
      else
      {
        TBSYS_LOG(ERROR, "invalid end_flag:end_flag=%d", end_flag);
        ret = OB_ERROR;
      }
      range.end_key_.assign(s_endkey_buf_array, 10);
    }
  }
  /**
   *make rowkey
   *@param rowkey: rowkey
   *@param data: rowkey num
   *@param flag: 0(normal), 1(min), 2(max)
   */
  void make_rowkey(ObRowkey& rowkey, const int64_t data,
       ObObj* s_rowkey_buf_array, const int flag = ROWKEY_NOMAL)
  {
    int ret = OB_SUCCESS;


    int64_t remain = 0;
    int64_t yushu = 0;
    ObObj obj;
  
    if (ROWKEY_NOMAL == flag)
    {
      remain = data;
      for (int64_t i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        s_rowkey_buf_array[12 - 3 - i] = obj;
        remain = (remain - yushu) / 10;
      }
      rowkey.assign(s_rowkey_buf_array, 10);
    }
    else if (ROWKEY_MIN == flag)
    {
      rowkey.set_min_row();
    }
    else if (ROWKEY_MAX == flag)
    {
      rowkey.set_max_row();
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid flag:flag=%d", flag);
      ret = OB_ERROR;
    }
  }



  void make_row(ObRow& row, const uint64_t table_id, const int64_t data,
      const int flag)
  {
    int ret = OB_SUCCESS;
    static ObRowDesc s_desc;
    s_desc.reset();

    ObObj obj;
    int64_t remain = 0;
    int64_t yushu = 0;
    int64_t i = 0;

    /***** DENSE-DENSE ******/
    if (NORMAL_ROW == flag)
    {//normal
      for (i = 0; i < 20; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      s_desc.set_rowkey_cell_count(10);
      row.set_row_desc(s_desc);

      remain = data;
      for (i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        ret = row.set_cell(table_id, 12 - 1 - i, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
        remain = (remain - yushu) / 10;
      }

      for (i = 10; i < 20; i ++)
      {
        obj.set_int(i - 10);
        ret = row.set_cell(table_id, i + 2, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
    }
    else if (EMPTY_ROW == flag)
    {//empty rowvalue
      for (i = 0; i < 10; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      s_desc.set_rowkey_cell_count(10);
      row.set_row_desc(s_desc);
      row.reset(false, ObRow::DEFAULT_NOP);

      remain = data;
      for (i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        ret = row.set_cell(table_id, 12 - 1 - i, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
        remain = (remain - yushu) / 10;
      }
    }

    /***** DENSE-SPARSE ******/
    else if (DEL_ROW == flag)
    {//del row
      for (i = 0; i < 10; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ret = s_desc.add_column_desc(table_id, OB_ACTION_FLAG_COLUMN_ID);
      ASSERT_EQ(OB_SUCCESS, ret);
      s_desc.set_rowkey_cell_count(10);
      row.set_row_desc(s_desc);

      remain = data;
      for (i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        ret = row.set_cell(table_id, 12 - 1 - i, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
        remain = (remain - yushu) / 10;
      }

      obj.set_ext(ObActionFlag::OP_DEL_ROW);
      ret = row.set_cell(table_id, OB_ACTION_FLAG_COLUMN_ID, obj);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    else if (ADD_ROW == flag)
    {//add row
      for (i = 0; i < 10; i ++)
      {
        ret = s_desc.add_column_desc(table_id, i + 2);
        ASSERT_EQ(OB_SUCCESS, ret);
      }
      ret = s_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
      ASSERT_EQ(OB_SUCCESS, ret);

      ret = s_desc.add_column_desc(table_id, 14);
      ASSERT_EQ(OB_SUCCESS, ret);

      s_desc.set_rowkey_cell_count(10);
      row.set_row_desc(s_desc);

      remain = data;
      for (i = 0; i < 10; i ++)
      {
        yushu = remain % 10;
        obj.set_int(yushu);
        ret = row.set_cell(table_id, 12 - 1 - i, obj);
        ASSERT_EQ(OB_SUCCESS, ret);
        remain = (remain - yushu) / 10;
      }

      obj.set_ext(ObActionFlag::OP_DEL_ROW);
      ret = row.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, obj);
      ASSERT_EQ(OB_SUCCESS, ret);

      //add new col, the 11th column
      obj.set_int(30);
      ret = row.set_cell(table_id, 14, obj);
      ASSERT_EQ(OB_SUCCESS, ret);

      //TBSYS_LOG(WARN, "insert obrow=%s", to_cstring(row));
    }
    else
    {
      TBSYS_LOG(ERROR, "invalid flag:flag=%d", flag);
      ret = OB_ERROR;
    }
  }

  /**
   *make get param 
   *@param sstable_get_param: sstable get param
   *@param table_id: table id
   *@param column_array: column array
   *@param column_cnt: column count
   *@param not_exit_col_ret_nop: not exit_col_ret_nop
   *@param ext_flag: 0(normal get), 1(full row get)
   */
  void make_get_param(sql::ObSqlGetParam *sstable_get_param,
      const uint64_t table_id,
      common::ObRowkey* rowkeys, const int64_t rowkey_cnt)
  {
    int ret = OB_SUCCESS;

    sstable_get_param->reset();

    sstable_get_param->set_table_id(table_id, table_id);

    for (int64_t i = 0; i < rowkey_cnt; i ++)
    {
      ret = sstable_get_param->add_rowkey(rowkeys[i]);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }


  /**
   *create file for test
   *@param file_id:sstable file id
   *@param comp_flag: 0(none),1(invalid),2(lzo_1.0),3(snappy_1.0)
   *@param store_type: DENSE_SPARSE(2),DENSE_DENSE(3)
   *@param table_count: table count
   *@param table_id: table id
   *@param range_start:
   *@param range_end:
   *@param range_start_flag:0(min),-1(uninclusive start),-2(inclu start)
   *@param range_end_flag:0(max), -2(uninclusive end),-2(inclu end)
   *@param ext_data: 0(0),1(hole data),2(del data),3(0),4(0),5(0)
   *@param ext_flag: 0(not exist hole),1(exist one hole),2(del row),
   *                 3(no row value),4(del row + full row)
   *@param row_count: row count array
   */
  void create_sstable(const uint64_t file_id, 
      const int comp_flag, 
      const ObCompactStoreType store_type, 
      const int64_t table_count, 
      const uint64_t* table_id, 
      const int64_t* range_start, 
      const int64_t* range_end,
      const int* range_start_flag, 
      const int* range_end_flag, 
      const int64_t* ext_data, 
      const int* ext_flag, 
      const int64_t* row_count)
  {
    int ret = OB_SUCCESS;

    ObCompactSSTableWriter writer;
    ObFrozenMinorVersionRange version_range;
    ObString comp_name;
    ObNewRange range;
    const int64_t version_flag = 0;
    compactsstablev2::ObSSTableSchema schema;
    const int schema_flag = 0;
    const int64_t block_size = 1024;
    const int64_t def_sstable_size = 0;
    const int64_t min_split_sstable_size = 0;

    ret = make_version_range(version_range, version_flag);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = make_comp_name(comp_name, comp_flag);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObString file_path;
    ret = make_file_path(file_path, file_id);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = writer.set_sstable_param(version_range, store_type,
        table_count, block_size, comp_name,
        def_sstable_size, min_split_sstable_size);
    ASSERT_EQ(OB_SUCCESS, ret);

    ObRow row;
    ObRowkey rowkey;
    int64_t real_start = 0;
    int64_t real_end = 0;
    bool is_split = false;

    for (int64_t i = 0; i < table_count; i ++)
    {
      make_range(range, table_id[i], range_start[i], range_end[i],
          range_start_flag[i], range_end_flag[i]);
      ASSERT_EQ(OB_SUCCESS, ret);

      make_schema(schema, table_id[i], schema_flag);

      ret = writer.set_table_info(table_id[i], schema, range);
      ASSERT_EQ(OB_SUCCESS, ret);

      if (0 == i)
      {
        ret = writer.set_sstable_filepath(file_path);
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      if (RANGE_MIN == range_start_flag[i])
      {
        real_start = 0;
      }
      else if (RANGE_UNINCLUSIVE == range_start_flag[i])
      {
        real_start = range_start[i] + 1;
      }
      else if (RANGE_INCLUSIVE == range_start_flag[i])
      {
        real_start = range_start[i] - 1;
      }
      else
      {
        TBSYS_LOG(WARN, "invalid range_start_flag:range_start_flag=%d",
            range_start_flag[i]);
      }

      if (RANGE_MAX == range_end_flag[i])
      {
        if (0 == row_count[i])
        {
          real_end = -1;
        }
        else
        {
          real_end = row_count[i];
        }
      }
      else if (RANGE_UNINCLUSIVE == range_end_flag[i])
      {
        real_end = range_end[i] - 1;
      }
      else if (RANGE_INCLUSIVE == range_end_flag[i])
      {
        real_end = range_end[i];
      }
      else
      {
        TBSYS_LOG(WARN, "invalid range_end_flag:range_end_flag=%d",
            range_end_flag[i]);
      }

      for (int64_t j = real_start; j < real_end; j ++)
      {
        if (NOT_EXIST_ROW == ext_flag[i])
        {
          if (j == ext_data[i])
          {
            continue;
          }
          else
          {
            make_row(row, table_id[i], j, NORMAL_ROW);
          }
        }
        else if (EMPTY_ROW == ext_flag[i])
        {
          make_row(row, table_id[i], j, EMPTY_ROW);
        }
        else if (DEL_ROW == ext_flag[i])
        {
          make_row(row, table_id[i], j, DEL_ROW);
        }
        else if (ADD_ROW == ext_flag[i])
        {
          make_row(row, table_id[i], j, ADD_ROW);
        }
        else
        {
          make_row(row, table_id[i], j, NORMAL_ROW);
        }

        //TBSYS_LOG(WARN, "i=%ld rowkey = %s", j, to_cstring(row));
        ret = writer.append_row(row, is_split);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_FALSE(is_split);

        row.reset(false, ObRow::DEFAULT_NULL); //reset(rowkey+rowvalue)
      }
    }

    ret = writer.finish();
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  void delete_file(const int64_t i)
  {
    ObString file_path;
    make_file_path(file_path, i);
    remove(file_path.ptr());
  }


  //check the 5,14,17,30 column 
  void check_DENSE_DENSE_row(const common::ObRowkey* rowkey, const ObRow* row_value)
  { 
    const ObObj* obj;
    ObObj cur_obj;
    int ret = OB_SUCCESS;

    ASSERT_EQ(14, row_value->get_column_num());

    check_rowkey(rowkey, row_value);

    cur_obj.set_ext(ObActionFlag::OP_NOP);

    ret = row_value->get_cell(1001, 5, obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    cur_obj.set_int(0);
    ASSERT_TRUE(cur_obj == *obj);

    ret = row_value->get_cell(1001, 14, obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    cur_obj.set_int(2);
    ASSERT_TRUE(cur_obj == *obj);

    ret = row_value->get_cell(1001, 17, obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    cur_obj.set_int(5);
    ASSERT_TRUE(cur_obj == *obj);

    ret = row_value->get_cell(1001, 30, obj);
    ASSERT_EQ(OB_SUCCESS, ret);

    cur_obj.set_null();
    ASSERT_TRUE(cur_obj == *obj);
  }


  void check_row_desc(const ObRow* row_value, const ObCompactStoreType store_type = DENSE_DENSE)
  {
    int ret = OB_SUCCESS;
    const common::ObRowDesc *row_desc = row_value->get_row_desc();  
    uint64_t column_array[1024];
    int64_t column_cnt;
    uint64_t tmp_table_id;
    uint64_t tmp_column_id;
    build_query_columns(column_array, column_cnt);

    for(int64_t i = 0; i<column_cnt; i++)
    {
      ret = row_desc->get_tid_cid(i, tmp_table_id, tmp_column_id);
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(table_id, tmp_table_id);
      ASSERT_EQ(column_array[i], tmp_column_id);
    }
  
    if(store_type == DENSE_SPARSE)
    { 
      ret = row_desc->get_tid_cid(column_cnt, tmp_table_id, tmp_column_id);
      ASSERT_EQ(OB_SUCCESS, ret);
         
      ASSERT_EQ(tmp_column_id, OB_ACTION_FLAG_COLUMN_ID);
    }

    ASSERT_EQ(10, row_desc->get_rowkey_cell_count());
  }


  void check_rowkey(const common::ObRowkey* rowkey, const ObRow* row_value)
  {

    const common::ObRowkey* store_rowkey = NULL; 
    ASSERT_EQ(OB_SUCCESS, row_value->get_rowkey(store_rowkey));   

    //TBSYS_LOG(ERROR, "src=%s store=%s", to_cstring(*rowkey), to_cstring(*store_rowkey));
    ASSERT_EQ(true, *rowkey == *store_rowkey);
  }


  void check_DENSE_SPARSE_row(const common::ObRowkey* rowkey, const ObRow* row_value, int ext_flag)  
  {
    ObObj cur_obj;
    int ret = OB_SUCCESS;
    const ObObj* obj;

    //check column count and rowkey
    ASSERT_EQ(14, row_value->get_column_num());
    check_rowkey(rowkey, row_value);

    cur_obj.set_ext(ObActionFlag::OP_NOP);

    int64_t tmp_value1;
    int64_t tmp_value2;
    cur_obj.get_ext(tmp_value2);


    ret = row_value->get_cell(table_id, 17, obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    obj->get_ext(tmp_value1);
    ASSERT_TRUE(tmp_value1 == tmp_value2);


    ret = row_value->get_cell(table_id, 30, obj);
    ASSERT_EQ(OB_SUCCESS, ret);
    obj->get_ext(tmp_value1);
    ASSERT_TRUE(tmp_value1 == tmp_value2);

    if(DEL_ROW == ext_flag)
    {
      ret = row_value->get_cell(table_id, 14, obj);
      ASSERT_EQ(OB_SUCCESS, ret);
      obj->get_ext(tmp_value1);
      ASSERT_TRUE(tmp_value1 == tmp_value2);

      ret = row_value->get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, obj);
      ASSERT_EQ(OB_SUCCESS, ret);

      cur_obj.set_ext(ObActionFlag::OP_DEL_ROW);

      obj->get_ext(tmp_value1);
      cur_obj.get_ext(tmp_value2);
      ASSERT_EQ(tmp_value1, tmp_value2);
    }
    else if(ADD_ROW == ext_flag)
    {
      ret = row_value->get_cell(table_id, 14, obj);
      ASSERT_EQ(OB_SUCCESS, ret);

      cur_obj.set_int(30);
      ASSERT_TRUE(cur_obj == *obj);

      ret = row_value->get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, obj);
      ASSERT_EQ(OB_SUCCESS, ret);

      cur_obj.set_ext(ObActionFlag::OP_NEW_ADD);
      obj->get_ext(tmp_value1);
      cur_obj.get_ext(tmp_value2);
      ASSERT_EQ(tmp_value1, tmp_value2);
    }
    else
    {
      FAIL();
    }
  }  

  void check_EMPTY_NEXIST_row(const common::ObRowkey* key, const ObRow* row_value, bool is_ups_get)  
  {
    ObObj cur_obj;
    int ret = OB_SUCCESS;
    const ObObj* obj;

    check_rowkey(key, row_value);

    if(is_ups_get)
      cur_obj.set_ext(ObActionFlag::OP_NOP);
    else
      cur_obj.set_null();

    int64_t tmp_value1;
    int64_t tmp_value2;
    cur_obj.get_ext(tmp_value2);

    ASSERT_EQ(14, row_value->get_column_num());

    ret = row_value->get_cell(1001, 14, obj);
    ASSERT_EQ(OB_SUCCESS, ret);

    if(is_ups_get)
    {
      obj->get_ext(tmp_value1);
      ASSERT_EQ(tmp_value1, tmp_value2);
    }
    else
    {
      ASSERT_TRUE(*obj == cur_obj);
    }

    ret = row_value->get_cell(1001, 17, obj);
    ASSERT_EQ(OB_SUCCESS, ret);

    if(is_ups_get)
    {
      obj->get_ext(tmp_value1);
      ASSERT_EQ(tmp_value1, tmp_value2);
    }
    else
    {
      ASSERT_TRUE(*obj == cur_obj);
    }

    ret = row_value->get_cell(1001, 30, obj);
    ASSERT_EQ(OB_SUCCESS, ret);


    if(is_ups_get)
    {
      obj->get_ext(tmp_value1);
      ASSERT_EQ(tmp_value1, tmp_value2);
    }
    else
    {
      ASSERT_TRUE(*obj == cur_obj);
    }
  }  


  void check_rowkey(const ObRowkey* row_key, int64_t exp_val)
  {
    ObRowkey key; 
    
    ObObj s_rowkey_buf_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
    make_rowkey(key, exp_val, s_rowkey_buf_array);
    ASSERT_TRUE(key == *row_key);
  }

  /* flag: 
     GET_FULL_COLUMN = 1,
     GET_4_COLUMN
     FIRST_50_ROW,
     MIDDLE_50_ROW,
     END_50_ROW
     */
  void build_query_rowkeys(common::ObRowkey* rowkeys, int64_t& start, int64_t row_count, int flag)
  {
    int64_t i;
    if(flag == FIRST_50_ROW)
    {
      start = 0;
    }

    if(flag == MIDDLE_50_ROW)
    {
      start = row_count/2;
    }

    if(flag == END_50_ROW)
    { 
      if(row_count - 50 > 0)
        start = row_count - 50;
      else
        start = 0;
    }

    
    for (i=start; i <= row_count && i < start + 49; i ++)
    {
      make_rowkey(rowkeys[i - start], i, s_rowkey_buf_arrays + (i - start) * OB_MAX_ROWKEY_COLUMN_NUMBER);
    }

    
    make_rowkey(rowkeys[i - start], 100000, s_rowkey_buf_arrays + (i - start) * OB_MAX_ROWKEY_COLUMN_NUMBER);
  }


  void build_query_columns(uint64_t* column_array, int64_t& column_cnt)
  {
    for (int64_t i = 0; i < 10; i ++)
    {
      column_array[i] = i + 2;
    }
    column_array[10] = 14;
    column_array[11] = 17;
    column_array[12] = 30;
    column_cnt = 13;
  }



  /* get_colum_flag: 
     GET_FULL_COLUMN = 1,
     GET_4_COLUMN
get_row_flag:
FIRST_50_ROW,
MIDDLE_50_ROW,
END_50_ROW
*/
  void test_normal_getter(const int64_t row_count, const int get_row_flag, const bool is_ups_getter, const int ext_flag, const int64_t ext_data)
  {
    int ret = OB_SUCCESS;
    const int comp_flag = 0;
    const int64_t range_start = 0;
    const int64_t range_end = 0;
    const int range_start_flag = RANGE_MIN;
    const int range_end_flag = RANGE_MAX;
    int64_t start = 0;
    ObCompactStoreType store_type = is_ups_getter? DENSE_SPARSE : DENSE_DENSE;

    create_sstable(file_id, comp_flag, store_type, table_count,
        &table_id, &range_start, &range_end, &range_start_flag, 
        &range_end_flag, &ext_data, &ext_flag, &row_count);

    uint64_t column_array[1024];
    int64_t column_cnt;

    //build query colums
    build_query_columns(column_array, column_cnt);

    //build query rowkey
    common::ObRowkey rowkeys[50];
    build_query_rowkeys(rowkeys, start, row_count, get_row_flag);


    sql::ObSqlGetParam sstable_get_param;

    make_get_param(&sstable_get_param, table_id, rowkeys, 50);
      
    ObCompactSSTableGetter::ObCompactGetThreadContext context;

    ObCompactSSTableReader reader(g_fic);
    ret = reader.init(file_id);
    ASSERT_EQ(OB_SUCCESS ,ret);


    for(int64_t i = 0; i < 50; i++)
    { 
      context.readers_[i] = &reader;
    }
    context.readers_count_ = 50;
    context.block_cache_ = &g_block_cache;
    context.block_index_cache_ = &g_index_cache;
    context.row_cache_ = &g_row_cache;

    ObCompactSSTableGetter getter;
    ret = getter.initialize(&sstable_get_param, column_array, column_cnt, 10, &context, is_ups_getter);
    ASSERT_EQ(OB_SUCCESS, ret);

    const ObRow* row_value = NULL;

    for (int64_t i = start; i < start + 50 && i < row_count; i ++)
    {
      ret = getter.get_next_row(row_value);
      ASSERT_EQ(OB_SUCCESS, ret);
 
      //TBSYS_LOG(WARN, "rowvalue = %s", to_cstring(*row_value));         
      check_row_desc(row_value, store_type);

      if(i == start + 49)
      {
        check_EMPTY_NEXIST_row(&(rowkeys[i - start]), row_value, is_ups_getter);
      }
      else if(DEL_ROW == ext_flag || ADD_ROW == ext_flag)
      {
        check_DENSE_SPARSE_row(&(rowkeys[i - start]), row_value, ext_flag);
      }
      else if((NOT_EXIST_ROW == ext_flag && 15 == i)|| EMPTY_ROW == ext_flag)  
      {
        check_EMPTY_NEXIST_row(&(rowkeys[i - start]), row_value, is_ups_getter);
      }
      else
      {
        check_DENSE_DENSE_row(&(rowkeys[i - start]), row_value);
      }

      
    }

    ret = getter.get_next_row(row_value);
    ASSERT_EQ(OB_ITER_END, ret);

    delete_file(file_id);
  }


  void test_EXCEPTION_getter(const int64_t row_count, const int get_row_flag, const bool is_ups_getter)
  {
    uint64_t column_array[1024];
    int64_t column_cnt;
    int64_t start;
    int ret = OB_SUCCESS;
    sql::ObSqlGetParam sstable_get_param;
    ObCompactSSTableGetter::ObCompactGetThreadContext context;

    //build query colums
    build_query_columns(column_array, column_cnt);

    //build query rowkey
    common::ObRowkey rowkeys[50];
    build_query_rowkeys(rowkeys, start, row_count, get_row_flag);


    context.readers_count_ = 50;
    context.block_cache_ = &g_block_cache;
    context.block_index_cache_ = &g_index_cache;
    context.row_cache_ = &g_row_cache;

    ObCompactSSTableGetter getter;
    column_cnt = 0;
    ret = getter.initialize(&sstable_get_param, column_array, column_cnt, 10, &context, is_ups_getter);
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
    
  }

  void test_READERS_NULL_getter(const int64_t row_count, const int get_row_flag, const bool is_ups_getter)
  {
    uint64_t column_array[1024];
    int64_t column_cnt;
    int64_t start;
    int ret = OB_SUCCESS;
    const ObRow* row_value = NULL;
    ObCompactStoreType store_type = is_ups_getter? DENSE_SPARSE : DENSE_DENSE;

    //build query colums
    build_query_columns(column_array, column_cnt);

    //build query rowkey
    common::ObRowkey rowkeys[50];
    build_query_rowkeys(rowkeys, start, row_count, get_row_flag);


    sql::ObSqlGetParam sstable_get_param;
    make_get_param(&sstable_get_param, table_id, rowkeys, 50);
  
      
    ObCompactSSTableGetter::ObCompactGetThreadContext context;

    for(int64_t i = 0; i < 50; i++)
    {
      context.readers_[i] = NULL;
    }

    context.readers_count_ = 50;
    context.block_cache_ = &g_block_cache;
    context.block_index_cache_ = &g_index_cache;
    context.row_cache_ = &g_row_cache;

    ObCompactSSTableGetter getter;
    ret = getter.initialize(&sstable_get_param, column_array, column_cnt, 10, &context, is_ups_getter);
    ASSERT_EQ(OB_SUCCESS, ret);

    for (int64_t i = start; i < start + 50 ; i ++)
    {
      ret = getter.get_next_row(row_value);
      ASSERT_EQ(OB_SUCCESS, ret);

      //TBSYS_LOG(WARN, "rowvalue = %s", to_cstring(*row_value));         
      check_row_desc(row_value, store_type);

      check_EMPTY_NEXIST_row(&(rowkeys[i - start]), row_value, is_ups_getter);
    }

    ret = getter.get_next_row(row_value);
    ASSERT_EQ(OB_ITER_END, ret);
  }

  void test_multi_reader_getter(const int64_t row_count, const int64_t reader_count, const int get_row_flag, const bool is_ups_getter, const int ext_flag, const int64_t ext_data)
  {
    int ret = OB_SUCCESS;
    const int comp_flag = 0;
    const int64_t range_start = 0;
    const int64_t range_end = 0;
    const int range_start_flag = RANGE_MIN;
    const int range_end_flag = RANGE_MAX;
    int64_t start = 0;
    ObCompactStoreType store_type = is_ups_getter? DENSE_SPARSE : DENSE_DENSE;

    create_sstable(file_id, comp_flag, store_type, table_count,
        &table_id, &range_start, &range_end, &range_start_flag, 
        &range_end_flag, &ext_data, &ext_flag, &row_count);


    create_sstable(file_id + 1, comp_flag, store_type, table_count,
        &table_id, &range_start, &range_end, &range_start_flag, 
        &range_end_flag, &ext_data, &ext_flag, &row_count);

    uint64_t column_array[1024];
    int64_t column_cnt;

    //build query colums
    build_query_columns(column_array, column_cnt);

    //build query rowkey
    common::ObRowkey rowkeys[50];
    build_query_rowkeys(rowkeys, start, row_count, get_row_flag);


    sql::ObSqlGetParam sstable_get_param;

    make_get_param(&sstable_get_param, table_id, rowkeys, 50);
      

    ObCompactSSTableReader reader(g_fic);
    ret = reader.init(file_id);
    ASSERT_EQ(OB_SUCCESS ,ret);

    ObCompactSSTableReader reader2(g_fic);
    ret = reader2.init(file_id + 1);
    ASSERT_EQ(OB_SUCCESS ,ret);

    ObCompactSSTableGetter::ObCompactGetThreadContext context;

    for(int64_t i = 0; i < reader_count; i++)
    {
      if(i < 25)
        context.readers_[i] = &reader;
      else 
        context.readers_[i] = &reader2;
    }

    context.readers_count_ = reader_count;
    context.block_cache_ = &g_block_cache;
    context.block_index_cache_ = &g_index_cache;

    context.row_cache_ = &g_row_cache;

    ObCompactSSTableGetter getter;
    ret = getter.initialize(&sstable_get_param, column_array, column_cnt, 10, &context, is_ups_getter);
    ASSERT_EQ(OB_SUCCESS, ret);

    const ObRow* row_value = NULL;

    for (int64_t i = start; i < start + 50 && i < start + reader_count; i ++)
    {
      ret = getter.get_next_row(row_value);
      ASSERT_EQ(OB_SUCCESS, ret);
 
      //TBSYS_LOG(WARN, "rowvalue = %s", to_cstring(*row_value));         
      check_row_desc(row_value, store_type);

      if(i == start + 49)
      {
        check_EMPTY_NEXIST_row(&(rowkeys[i - start]), row_value, is_ups_getter);
      }
      else if(DEL_ROW == ext_flag || ADD_ROW == ext_flag)
      {
        check_DENSE_SPARSE_row(&(rowkeys[i - start]), row_value, ext_flag);
      }
      else if((NOT_EXIST_ROW == ext_flag && 15 == i)|| EMPTY_ROW == ext_flag)  
      {
        check_EMPTY_NEXIST_row(&(rowkeys[i - start]), row_value, is_ups_getter);
      }
      else
      {
        check_DENSE_DENSE_row(&(rowkeys[i - start]), row_value);
      }
    }

    ret = getter.get_next_row(row_value);
    ASSERT_EQ(OB_ITER_END, ret);

    delete_file(file_id);
    delete_file(file_id + 1);
  }

};


//test normal getter
TEST_F(TestCompactSSTableGetter, DENSE_DENSE_GETTER)
{
  test_normal_getter(200, FIRST_50_ROW, false, 0, 0);
  //test_normal_getter(200, MIDDLE_50_ROW, false, 0, 0);
  //test_normal_getter(200, END_50_ROW, false, 0, 0);
}

//test row cache
TEST_F(TestCompactSSTableGetter, ROW_CACHE_GETTER)
{
  test_normal_getter(200, FIRST_50_ROW, false, 0, 0);
  test_normal_getter(200, FIRST_50_ROW, false, 0, 0);
}


//test one del row 
TEST_F(TestCompactSSTableGetter, ROW_DEL_GETTER)
{
  test_normal_getter(200, FIRST_50_ROW, true, DEL_ROW, 0);
  test_normal_getter(200, MIDDLE_50_ROW, true, DEL_ROW, 0);
  test_normal_getter(200, END_50_ROW, true, DEL_ROW, 0);
}

//test one add row 
TEST_F(TestCompactSSTableGetter, ROW_ADDNEW_GETTER)
{
  test_normal_getter(200, FIRST_50_ROW, true, ADD_ROW, 15);
  test_normal_getter(200, MIDDLE_50_ROW, true, ADD_ROW, 15);
  test_normal_getter(200, END_50_ROW, true, ADD_ROW, 15);
}

//test one empty row 
TEST_F(TestCompactSSTableGetter, ROW_EMPTY_GETTER)
{
  test_normal_getter(200, FIRST_50_ROW, true, EMPTY_ROW, 15);
  test_normal_getter(200, MIDDLE_50_ROW, true, EMPTY_ROW, 15);
  test_normal_getter(200, END_50_ROW, true, EMPTY_ROW, 15);
}

//test one not exist row 
TEST_F(TestCompactSSTableGetter, ROW_NOTEXIST_GETTER)
{
  test_normal_getter(200, FIRST_50_ROW, false, NOT_EXIST_ROW, 15);
  test_normal_getter(200, MIDDLE_50_ROW, false, NOT_EXIST_ROW, 15);
  test_normal_getter(200, END_50_ROW, false, NOT_EXIST_ROW, 15);
}


//test normal getter
TEST_F(TestCompactSSTableGetter, MULTI_READER_GETTER)
{
  test_multi_reader_getter(200, 50, FIRST_50_ROW, false, 0, 0);
  test_multi_reader_getter(200, 50, MIDDLE_50_ROW, false, 0, 0);
  test_multi_reader_getter(200, 50, END_50_ROW, false, 0, 0);
}

//test normal getter
TEST_F(TestCompactSSTableGetter, MULTI_READER_LESS_GETTER)
{
  test_multi_reader_getter(200, 30, FIRST_50_ROW, false, 0, 0);
}


//test normal getter
TEST_F(TestCompactSSTableGetter, MULTI_READER_NULL_GETTER)
{
  test_READERS_NULL_getter(200, FIRST_50_ROW, false);
}


//test normal getter
TEST_F(TestCompactSSTableGetter, EXCEPTION_GETTER)
{
  test_EXCEPTION_getter(200, FIRST_50_ROW, false);
}

int main(int argc, char** argv)
{
  ob_init_memory_pool();
  TBSYS_LOGGER.setLogLevel("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
