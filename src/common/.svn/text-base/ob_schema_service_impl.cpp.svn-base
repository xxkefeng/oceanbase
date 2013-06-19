#include "ob_schema_service_impl.h"
#include "roottable/ob_first_tablet_entry_schema.h"
#include "ob_extra_tables_schema.h"
#include "ob_schema_service.h"
#include "utility.h"
#include "ob_mutator.h"
#include "ob_tsi_factory.h"

using namespace oceanbase;
using namespace common;

#define DEL_ROW(table_name, rowkey) \
if (OB_SUCCESS == ret) \
{ \
  ret = mutator->del_row(table_name, rowkey); \
  if(OB_SUCCESS != ret) \
  { \
    TBSYS_LOG(WARN, "insert del to mutator fail:ret[%d]", ret); \
  } \
}

#define ADD_VARCHAR(table_name, rowkey, column_name, value) \
if (OB_SUCCESS == ret) \
{ \
  ObObj vchar_value; \
  vchar_value.set_varchar(OB_STR(value)); \
  ret = mutator->insert(table_name, rowkey, OB_STR(column_name), vchar_value); \
  if(OB_SUCCESS != ret) \
  { \
    TBSYS_LOG(WARN, "insert value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
}

#define ADD_INT(table_name, rowkey, column_name, value) \
if(OB_SUCCESS == ret) \
{ \
  ObObj int_value; \
  int_value.set_int(value); \
  ret = mutator->insert(table_name, rowkey, OB_STR(column_name), int_value); \
  if(OB_SUCCESS != ret) \
  { \
    TBSYS_LOG(WARN, "insert value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
}
#define ADD_CREATE_TIME(table_name, rowkey, column_name, value) \
if(OB_SUCCESS == ret) \
{ \
  ObObj time_value; \
  time_value.set_createtime(value); \
  ret = mutator->insert(table_name, rowkey, OB_STR(column_name), time_value); \
  if(OB_SUCCESS != ret) \
  { \
    TBSYS_LOG(WARN, "insert value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
}
#define ADD_MODIFY_TIME(table_name, rowkey, column_name, value) \
if(OB_SUCCESS == ret) \
{ \
  ObObj time_value; \
  time_value.set_modifytime(value); \
  ret = mutator->insert(table_name, rowkey, OB_STR(column_name), time_value); \
  if(OB_SUCCESS != ret) \
  { \
    TBSYS_LOG(WARN, "insert value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
}

int ObSchemaServiceImpl::add_join_info(ObMutator* mutator, const TableSchema& table_schema)
{
  int ret = OB_SUCCESS;

  if(NULL == mutator)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "mutator is null");
  }

  JoinInfo join_info;
  ObRowkey rowkey;

  ObObj value[4];


  if(OB_SUCCESS == ret)
  {
    for(int32_t i=0;i<table_schema.join_info_.count();i++)
    {

      ret = table_schema.join_info_.at(i, join_info);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get joininfo from table_schema fail:ret[%d], i[%d]", ret, i);
      }

      value[0].set_int(join_info.left_table_id_);
      value[1].set_int(join_info.left_column_id_);
      value[2].set_int(join_info.right_table_id_);
      value[3].set_int(join_info.right_column_id_);
      rowkey.assign(value, 4);

      //连调需要
      //rowkey列不需要写入，等郁白在UPS端的修改完成以后可以去掉
      //to be delete start
     // ADD_INT(joininfo_table_name, rowkey, "left_table_id", join_info.left_table_id_);
     // ADD_INT(joininfo_table_name, rowkey, "left_column_id", join_info.left_column_id_);
     // ADD_INT(joininfo_table_name, rowkey, "right_table_id", join_info.right_table_id_);
     // ADD_INT(joininfo_table_name, rowkey, "right_column_id", join_info.right_column_id_);
      // to be delete end
      ADD_VARCHAR(joininfo_table_name, rowkey, "left_table_name", join_info.left_table_name_);
      ADD_VARCHAR(joininfo_table_name, rowkey, "left_column_name", join_info.left_column_name_);
      ADD_VARCHAR(joininfo_table_name, rowkey, "right_table_name", join_info.right_table_name_);
      ADD_VARCHAR(joininfo_table_name, rowkey, "right_column_name", join_info.right_column_name_);

    }
  }

  return ret;
}


int ObSchemaServiceImpl::add_column(ObMutator* mutator, const TableSchema& table_schema)
{
  int ret = OB_SUCCESS;

  if(NULL == mutator)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "mutator is null");
  }

  ColumnSchema column;
  ObRowkey rowkey;
  ObString column_name;

  ObObj value[2];
  value[0].set_int(table_schema.table_id_);


  if (OB_SUCCESS == ret)
  {
    for(int32_t i=0;i<table_schema.columns_.count() && OB_SUCCESS == ret;i++)
    {
      ret = table_schema.columns_.at(i, column);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "get column from table_schema fail:ret[%d], i[%d]", ret, i);
      }

      if(OB_SUCCESS == ret)
      {
        column_name.assign_ptr(column.column_name_, static_cast<int32_t>(strlen(column.column_name_)));
        value[1].set_varchar(column_name);
        rowkey.assign(value, 2);
        ADD_INT(column_table_name, rowkey, "column_id", column.column_id_);
        ADD_INT(column_table_name, rowkey, "column_group_id", column.column_group_id_);
        ADD_INT(column_table_name, rowkey, "rowkey_id", column.rowkey_id_);
        ADD_INT(column_table_name, rowkey, "join_table_id", column.join_table_id_);
        ADD_INT(column_table_name, rowkey, "join_column_id", column.join_column_id_);
        ADD_INT(column_table_name, rowkey, "data_type", column.data_type_);
        ADD_INT(column_table_name, rowkey, "data_length", column.data_length_);
        ADD_INT(column_table_name, rowkey, "data_precision", column.data_precision_);
        ADD_INT(column_table_name, rowkey, "data_scale", column.data_scale_);
        ADD_INT(column_table_name, rowkey, "nullable", column.nullable_);
        ADD_INT(column_table_name, rowkey, "length_in_rowkey", column.length_in_rowkey_);
        ADD_INT(column_table_name, rowkey, "order_in_rowkey", column.order_in_rowkey_);
      }
    }
  }

  return ret;
}

ObSchemaServiceImpl::ObSchemaServiceImpl()
  :client_proxy_(NULL), is_id_name_map_inited_(false), only_core_tables_(true)
{
}

ObSchemaServiceImpl::~ObSchemaServiceImpl()
{
  client_proxy_ = NULL;
  is_id_name_map_inited_ = false;
}

bool ObSchemaServiceImpl::check_inner_stat()
{
  bool ret = true;
  tbsys::CThreadGuard guard(&mutex_);
  if(!is_id_name_map_inited_)
  {
    int err = init_id_name_map();
    if(OB_SUCCESS != err)
    {
      ret = false;
      TBSYS_LOG(WARN, "init id name map fail:ret[%d]", err);
    }
    else
    {
      is_id_name_map_inited_ = true;
    }
  }

  if(ret && NULL == client_proxy_)
  {
    TBSYS_LOG(ERROR, "client proxy is NULL");
    ret = false;
  }
  return ret;
}

int ObSchemaServiceImpl::init(ObScanHelper* client_proxy, bool only_core_tables)
{
  int ret = OB_SUCCESS;
  tbsys::CThreadGuard guard(&mutex_);
  if (NULL == client_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "client proxy is null");
  }
  else if (id_name_map_.created())
  {
    if (OB_SUCCESS != (ret = id_name_map_.clear()))
    {
      TBSYS_LOG(WARN, "fail to clear id name hash map. ret=%d", ret);
    }
    else
    {
      is_id_name_map_inited_ = false;
    }
  }
  else if (OB_SUCCESS != (ret = id_name_map_.create(1000)))
  {
    TBSYS_LOG(WARN, "create id_name_map_ fail:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    this->client_proxy_ = client_proxy;
    this->only_core_tables_ = only_core_tables;
  }
  return ret;
}

int ObSchemaServiceImpl::create_table_mutator(const TableSchema& table_schema, ObMutator* mutator)
{
  int ret = OB_SUCCESS;

  ObString table_name;
  table_name.assign_ptr(const_cast<char*>(table_schema.table_name_), static_cast<int32_t>(strlen(table_schema.table_name_)));

  ObObj table_name_value;
  table_name_value.set_varchar(table_name);

  ObRowkey rowkey;
  rowkey.assign(&table_name_value, 1);

  //ADD_VARCHAR(first_tablet_entry_name, rowkey, "table_name", table_schema.table_name_);
  ADD_INT(first_tablet_entry_name, rowkey, "table_id", table_schema.table_id_);
  ADD_INT(first_tablet_entry_name, rowkey, "table_type", table_schema.table_type_);
  ADD_INT(first_tablet_entry_name, rowkey, "load_type", table_schema.load_type_);
  ADD_INT(first_tablet_entry_name, rowkey, "table_def_type", table_schema.table_def_type_);
  ADD_INT(first_tablet_entry_name, rowkey, "rowkey_column_num", table_schema.rowkey_column_num_);
  ADD_INT(first_tablet_entry_name, rowkey, "replica_num", table_schema.replica_num_);
  ADD_INT(first_tablet_entry_name, rowkey, "max_used_column_id", table_schema.max_used_column_id_);
  ADD_INT(first_tablet_entry_name, rowkey, "create_mem_version", table_schema.create_mem_version_);
  ADD_INT(first_tablet_entry_name, rowkey, "tablet_max_size", table_schema.tablet_max_size_);
  ADD_INT(first_tablet_entry_name, rowkey, "tablet_block_size", table_schema.tablet_block_size_);
  ADD_VARCHAR(first_tablet_entry_name, rowkey, "compress_func_name", table_schema.compress_func_name_);

  ADD_INT(first_tablet_entry_name, rowkey, "is_use_bloomfilter", table_schema.is_use_bloomfilter_);
  ADD_INT(first_tablet_entry_name, rowkey, "is_pure_update_table", table_schema.is_pure_update_table_);
  ADD_INT(first_tablet_entry_name, rowkey, "is_read_static", table_schema.is_read_static_);
  ADD_INT(first_tablet_entry_name, rowkey, "rowkey_split", table_schema.rowkey_split_);
  ADD_INT(first_tablet_entry_name, rowkey, "max_rowkey_length", table_schema.max_rowkey_length_);
  ADD_INT(first_tablet_entry_name, rowkey, "merge_write_sstable_version", table_schema.merge_write_sstable_version_);
  ADD_VARCHAR(first_tablet_entry_name, rowkey, "expire_condition", table_schema.expire_condition_);
  ADD_INT(first_tablet_entry_name, rowkey, "create_time_column_id", table_schema.create_time_column_id_);
  ADD_INT(first_tablet_entry_name, rowkey, "modify_time_column_id", table_schema.modify_time_column_id_);
  ADD_INT(first_tablet_entry_name, rowkey, "character_set", table_schema.charset_number_);

  // field for index
  ADD_INT(first_tablet_entry_name, rowkey, "data_table_id", table_schema.data_table_id_);
  ADD_INT(first_tablet_entry_name, rowkey, "index_status", table_schema.index_status_);

  if(OB_SUCCESS == ret)
  {
    ret = add_column(mutator, table_schema);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add column to mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = add_join_info(mutator, table_schema);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "add join info to mutator fail:ret[%d]", ret);
    }
  }

  return ret;
}

int ObSchemaServiceImpl::alter_table_mutator(const AlterTableSchema& table_schema, ObMutator* mutator)
{
  int ret = OB_SUCCESS;
  ObObj value[2];
  value[0].set_int(table_schema.table_id_);
  ObRowkey rowkey;
  ObString column_name;
  uint64_t max_column_id = 0;
  AlterTableSchema::AlterColumnSchema alter_column;
  for (int32_t i = 0; (OB_SUCCESS == ret) && (i < table_schema.get_column_count()); ++i)
  {
    ret = table_schema.columns_.at(i, alter_column);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "get column from table_schema fail:ret[%d], i[%d]", ret, i);
    }
    else
    {
      column_name.assign_ptr(alter_column.column_.column_name_,
          static_cast<int32_t>(strlen(alter_column.column_.column_name_)));
      value[1].set_varchar(column_name);
      rowkey.assign(value, 2);
      switch (alter_column.type_)
      {
        case AlterTableSchema::ADD_COLUMN:
          {
            if (alter_column.column_.column_id_ <= max_column_id)
            {
              TBSYS_LOG(WARN, "check column id failed:column_id[%lu], max[%ld]",
                  alter_column.column_.column_id_, max_column_id);
              ret = OB_INVALID_ARGUMENT;
            }
            else
            {
              max_column_id = alter_column.column_.column_id_;
            }
          }
        case AlterTableSchema::MOD_COLUMN:
          {
            // add column succ
            if (OB_SUCCESS == ret)
            {
              ret = update_column_mutator(mutator, rowkey, alter_column.column_);
            }
            break;
          }
        case AlterTableSchema::DEL_COLUMN:
          {
            DEL_ROW(column_table_name, rowkey);
            break;
          }
        default :
          {
            ret = OB_INVALID_ARGUMENT;
            break;
          }
      }
    }
  }
  // reset table max used column id
  if ((OB_SUCCESS == ret) && (max_column_id != 0))
  {
    ret = reset_column_id_mutator(mutator, table_schema, max_column_id);
  }
  return ret;
}

int ObSchemaServiceImpl::update_column_mutator(ObMutator* mutator, ObRowkey & rowkey, const ColumnSchema & column)
{
  int ret = OB_SUCCESS;
  ADD_INT(column_table_name, rowkey, "column_id", column.column_id_);
  ADD_INT(column_table_name, rowkey, "column_group_id", column.column_group_id_);
  ADD_INT(column_table_name, rowkey, "rowkey_id", column.rowkey_id_);
  ADD_INT(column_table_name, rowkey, "join_table_id", column.join_table_id_);
  ADD_INT(column_table_name, rowkey, "join_column_id", column.join_column_id_);
  ADD_INT(column_table_name, rowkey, "data_type", column.data_type_);
  ADD_INT(column_table_name, rowkey, "data_length", column.data_length_);
  ADD_INT(column_table_name, rowkey, "data_precision", column.data_precision_);
  ADD_INT(column_table_name, rowkey, "data_scale", column.data_scale_);
  ADD_INT(column_table_name, rowkey, "nullable", column.nullable_);
  ADD_INT(column_table_name, rowkey, "length_in_rowkey", column.length_in_rowkey_);
  ADD_INT(column_table_name, rowkey, "order_in_rowkey", column.order_in_rowkey_);
  return ret;
}

int ObSchemaServiceImpl::reset_column_id_mutator(ObMutator* mutator, const AlterTableSchema & schema, const uint64_t max_column_id)
{
  int ret = OB_SUCCESS;
  if ((mutator != NULL) && (max_column_id > OB_APP_MIN_COLUMN_ID))
  {
    ObString table_name;
    table_name.assign_ptr(const_cast<char*>(schema.table_name_), static_cast<int32_t>(strlen(schema.table_name_)));
    ObObj table_name_value;
    table_name_value.set_varchar(table_name);
    ObRowkey rowkey;
    rowkey.assign(&table_name_value, 1);
    ADD_INT(first_tablet_entry_name, rowkey, "max_used_column_id", max_column_id);
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "check input param failed:table_name[%s], max_column_id[%lu]", schema.table_name_, max_column_id);
  }
  return ret;
}

int ObSchemaServiceImpl::create_table(const TableSchema& table_schema)
{
  int ret = OB_SUCCESS;

  if (!table_schema.is_valid())
  {
    TBSYS_LOG(WARN, "invalid table schema, tid=%lu", table_schema.table_id_);
    ret = OB_ERR_INVALID_SCHEMA;
  }
  else if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }

  tbsys::CThreadGuard guard(&mutex_);

  ObMutator* mutator = NULL;

  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific Mutator fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = create_table_mutator(table_schema, mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "create table mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
    }
  }

  ObString table_name;
  table_name.assign_ptr(const_cast<char*>(table_schema.table_name_), static_cast<int32_t>(strlen(table_schema.table_name_)));

  ObString table_name_store;
  if(OB_SUCCESS == ret)
  {
    tbsys::CThreadGuard buf_guard(&string_buf_write_mutex_);
    ret = string_buf_.write_string(table_name, &table_name_store);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "write string fail:ret[%d]", ret);
    }
  }

  int err = 0;
  if(OB_SUCCESS == ret)
  {
    err = id_name_map_.set(table_schema.table_id_, table_name_store);
    if(hash::HASH_INSERT_SUCC != err)
    {
      if(hash::HASH_EXIST == err)
      {
        TBSYS_LOG(ERROR, "bug table exist:table_id[%lu], table_name_store[%.*s]",
          table_schema.table_id_, table_name_store.length(), table_name_store.ptr());
      }
      else
      {
        ret = OB_ERROR;
        TBSYS_LOG(WARN, "id name map set fail:err[%d], table_id[%lu], table_name_store[%.*s]", err,
          table_schema.table_id_, table_name_store.length(), table_name_store.ptr());
      }
    }
  }
  return ret;
}

int ObSchemaServiceImpl::delete_range(
    const ObString& table_name, const SC& rowkey_columns, 
    const SC& cond_columns, const ObObj *cond_values)
{
  int ret = OB_SUCCESS;
  char sql[OB_DEFAULT_SQL_LENGTH];
  int64_t pos = 0;

  SQLQueryResultReader reader;
  ObRow row;

  if (OB_SUCCESS != (ret = build_select_stmt(
          table_name, rowkey_columns, cond_columns, cond_values, 
          sql, OB_DEFAULT_SQL_LENGTH, pos)))
  {
    TBSYS_LOG(WARN, "build_select_stmt ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = reader.query(*client_proxy_, sql)))
  {
    TBSYS_LOG(WARN, "execute query sql[%s] ret=%d", sql, ret);
  }
  else
  {
    while (OB_SUCCESS == (ret = reader.get_next_row(row)))
    {
      pos = 0;
      if(OB_SUCCESS != (ret = build_delete_stmt(table_name, 
              rowkey_columns, row, sql, OB_DEFAULT_SQL_LENGTH, pos)))
      {
        TBSYS_LOG(WARN, "build delete stmt row[%s] fail:ret[%d]", to_cstring(row), ret);
        break;
      }
      else if(OB_SUCCESS != (ret = client_proxy_->modify(sql)))
      {
        TBSYS_LOG(WARN, "add column to table schema fail:ret[%d]", ret);
        break;
      }
    }
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
  }

  return ret;

}

int ObSchemaServiceImpl::drop_table(const ObString& table_name)
{
  int ret = OB_SUCCESS;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }


  uint64_t table_id = 0;
  char sql[OB_DEFAULT_SQL_LENGTH];
  int64_t sql_len = 0;
  ObRow row;
  ObObj cond_value;

  ret = get_table_id(table_name, table_id);
  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "get table id fail:table_name[%.*s]", table_name.length(), table_name.ptr());
  }
  else
  {
    cond_value.set_int(table_id);
  }

  // called after get_table_id() to prevent dead lock
  tbsys::CThreadGuard guard(&mutex_);
  if (OB_SUCCESS == ret)
  {
    if ((sql_len = snprintf(sql, OB_DEFAULT_SQL_LENGTH, 
            "delete from %s where table_name='%.*s'", 
            FIRST_TABLET_TABLE_NAME, 
            table_name.length(), table_name.ptr())) > OB_DEFAULT_SQL_LENGTH - 1)
    {
      TBSYS_LOG(WARN, "sql buffer not enough %s", sql);
      ret = OB_BUF_NOT_ENOUGH;
    }
    else if (OB_SUCCESS != (ret = client_proxy_->modify(sql)))
    {
      TBSYS_LOG(WARN, "execute sql [%s] ret =%d", sql, ret);
    }
    // unfortunately, we donot support delete range now.
    // scan all rows and delete one by one.
    else if(OB_SUCCESS != (ret = delete_range(column_table_name, 
            SC("table_id")("column_name"), SC("table_id"), &cond_value) ))
    {
      TBSYS_LOG(WARN, "delete rwo from first tablet table fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = delete_range(joininfo_table_name,
            SC("left_table_id")("left_column_id")("right_table_id")("right_column_id"),
            SC("left_table_id"), &cond_value) ))
    {
      TBSYS_LOG(WARN, "delete rwo from first tablet table fail:ret[%d]", ret);
    }
    else
    {
      int err = id_name_map_.erase(table_id);
      if(hash::HASH_EXIST != err)
      {
        ret = hash::HASH_NOT_EXIST == err ? OB_ENTRY_NOT_EXIST : OB_SUCCESS;
        TBSYS_LOG(WARN, "id name map erase fail:err[%d], table_id[%lu]", err, table_id);
      }
    }
  }

  return ret;
}

int ObSchemaServiceImpl::init_id_name_map()
{
  int ret = OB_SUCCESS;
  ObTableIdNameIterator iterator;
  if(OB_SUCCESS == ret)
  {
    ret = iterator.init(client_proxy_, only_core_tables_);
    if(OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "init iterator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = init_id_name_map(iterator)))
    {
      TBSYS_LOG(WARN, "failed init id_name_map, err=%d", ret);
    }
  }

  return ret;
}

int ObSchemaServiceImpl::init_id_name_map(ObTableIdNameIterator& iterator)
{
  int ret = OB_SUCCESS;

  ObTableIdName * table_id_name = NULL;
  ObString tmp_str;

  while(OB_SUCCESS == ret && OB_SUCCESS == (ret = iterator.get_next(&table_id_name)))
  {
    if(OB_SUCCESS == ret)
    {
      tbsys::CThreadGuard buf_guard(&string_buf_write_mutex_);
      ret = string_buf_.write_string(table_id_name->table_name_, &tmp_str);
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "write string to string buf fail:ret[%d]", ret);
      }
    }

    int err = 0;
    if(OB_SUCCESS == ret)
    {
      err = id_name_map_.set(table_id_name->table_id_, tmp_str);
      if(hash::HASH_INSERT_SUCC != err)
      {
        ret = hash::HASH_EXIST == err ? OB_ENTRY_EXIST : OB_ERROR;
        TBSYS_LOG(WARN, "id name map set fail:err[%d], table_id[%lu]", err, table_id_name->table_id_);
      }
      else
      {
        TBSYS_LOG(DEBUG, "add id_name_map, tname=%.*s tid=%lu",
          tmp_str.length(), tmp_str.ptr(), table_id_name->table_id_);
      }
    }
  }

  if(OB_ITER_END == ret)
  {
    ret = OB_SUCCESS;
  }

  return ret;
}


int ObSchemaServiceImpl::get_table_name(uint64_t table_id, ObString& table_name)
{
  int ret = OB_SUCCESS;
  TBSYS_LOG(DEBUG, "table_id = %lu", table_id);
  if (OB_FIRST_TABLET_ENTRY_TID ==  table_id)
  {
    table_name = first_tablet_entry_name;
  }
  else if (OB_ALL_ALL_COLUMN_TID == table_id)
  {
    table_name = column_table_name;
  }
  else if (OB_ALL_JOIN_INFO_TID == table_id)
  {
    table_name = joininfo_table_name;
  }
  else if (!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
    int err = id_name_map_.get(table_id, table_name);
    if(hash::HASH_EXIST != err)
    {
      ret = hash::HASH_NOT_EXIST == err ? OB_ENTRY_NOT_EXIST : OB_ERROR;
      TBSYS_LOG(WARN, "id name map get fail:err[%d], table_id[%lu]", err, table_id);
    }
  }
  TBSYS_LOG(DEBUG, "get table_name=%.*s", table_name.length(), table_name.ptr());
  return ret;
}

int ObSchemaServiceImpl::get_table_id(const ObString& table_name, uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  SQLQueryResultReader res;
  char sql[OB_DEFAULT_SQL_LENGTH] ;
  int64_t len = 0;
  ObRow row;
  const ObObj *value = NULL;

  if(!check_inner_stat())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else if ( (len = snprintf(sql, OB_DEFAULT_SQL_LENGTH,  
          "select table_id from %s where table_name='%.*s'", 
          FIRST_TABLET_TABLE_NAME, 
          table_name.length(), 
          table_name.ptr())) >= OB_DEFAULT_SQL_LENGTH - 1)
  {
    TBSYS_LOG(WARN, "sql buffer not enough %s", sql);
    ret = OB_BUF_NOT_ENOUGH;
  }
  else if (OB_SUCCESS != (ret = res.query_one_row(*client_proxy_, sql, row)))
  {
    TBSYS_LOG(WARN, "execute sql %s ret=%d", sql, ret);
  }
  else if (OB_SUCCESS != (ret = row.raw_get_cell(0, value)) || NULL == value)
  {
    TBSYS_LOG(WARN, "row (%s) get cell 0 ret=%d, value=%p", to_cstring(row), ret, value);
  }
  else if (value->get_type() != ObIntType 
      || OB_SUCCESS != (ret = value->get_int(*(int64_t*)&table_id)))
  {
    TBSYS_LOG(WARN, "value (%s) is unexpected.", to_cstring(*value));
  }

  return ret;
}

int ObSchemaServiceImpl::assemble_table(const SQLQueryResultReader& reader, 
    const ObRow& row, TableSchema& table_schema)
{
  int ret = OB_SUCCESS;

  EXTRACT_STRBUF_FIELD(reader, row, "table_name", table_schema.table_name_, OB_MAX_COLUMN_NAME_LENGTH);
  EXTRACT_INT_FIELD(reader, row, "table_id", table_schema.table_id_, uint64_t);
  EXTRACT_INT_FIELD(reader, row, "table_type", table_schema.table_type_, TableSchema::TableType);
  EXTRACT_INT_FIELD(reader, row, "load_type", table_schema.load_type_, TableSchema::LoadType);
  EXTRACT_INT_FIELD(reader, row, "table_def_type", table_schema.table_def_type_, TableSchema::TableDefType);
  EXTRACT_INT_FIELD(reader, row, "rowkey_column_num", table_schema.rowkey_column_num_, int32_t);
  EXTRACT_INT_FIELD(reader, row, "replica_num", table_schema.replica_num_, int32_t);
  EXTRACT_INT_FIELD(reader, row, "max_used_column_id", table_schema.max_used_column_id_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "create_mem_version", table_schema.create_mem_version_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "tablet_max_size", table_schema.tablet_max_size_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "tablet_block_size", table_schema.tablet_block_size_, int64_t);
  if (OB_SUCCESS == ret && table_schema.tablet_block_size_ <= 0)
  {
    TBSYS_LOG(WARN, "set tablet sstable block size to default value:read[%ld]", table_schema.tablet_block_size_);
    table_schema.tablet_block_size_ = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
  }
  EXTRACT_INT_FIELD(reader, row, "max_rowkey_length", table_schema.max_rowkey_length_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "merge_write_sstable_version", table_schema.merge_write_sstable_version_, int64_t);
  EXTRACT_STRBUF_FIELD(reader, row, "compress_func_name", table_schema.compress_func_name_, OB_MAX_COLUMN_NAME_LENGTH);
  EXTRACT_STRBUF_FIELD(reader, row, "expire_condition", table_schema.expire_condition_, OB_MAX_EXPIRE_CONDITION_LENGTH);
  EXTRACT_INT_FIELD(reader, row, "is_use_bloomfilter", table_schema.is_use_bloomfilter_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "is_pure_update_table", table_schema.is_pure_update_table_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "is_read_static", table_schema.is_read_static_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "rowkey_split", table_schema.rowkey_split_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "create_time_column_id", table_schema.create_time_column_id_, uint64_t);
  EXTRACT_INT_FIELD(reader, row, "modify_time_column_id", table_schema.modify_time_column_id_, uint64_t);
  EXTRACT_INT_FIELD(reader, row, "character_set", table_schema.charset_number_, int32_t);
  EXTRACT_INT_FIELD(reader, row, "data_table_id", table_schema.data_table_id_, uint64_t);
  EXTRACT_INT_FIELD(reader, row, "index_status", table_schema.index_status_, TableSchema::IndexStatus);
  return ret;
}

int ObSchemaServiceImpl::assemble_column(const SQLQueryResultReader& reader, 
    const ObRow& row, ColumnSchema& column)
{
  int ret = OB_SUCCESS;

  EXTRACT_STRBUF_FIELD(reader, row, "column_name", column.column_name_, OB_MAX_COLUMN_NAME_LENGTH);
  EXTRACT_INT_FIELD(reader, row, "column_id", column.column_id_, uint64_t);
  EXTRACT_INT_FIELD(reader, row, "column_group_id", column.column_group_id_, uint64_t);
  EXTRACT_INT_FIELD(reader, row, "rowkey_id", column.rowkey_id_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "join_table_id", column.join_table_id_, uint64_t);
  EXTRACT_INT_FIELD(reader, row, "join_column_id", column.join_column_id_, uint64_t);
  EXTRACT_INT_FIELD(reader, row, "data_type", column.data_type_, ColumnType);
  EXTRACT_INT_FIELD(reader, row, "data_length", column.data_length_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "data_precision", column.data_precision_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "data_scale", column.data_scale_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "nullable", column.nullable_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "length_in_rowkey", column.length_in_rowkey_, int64_t);
  EXTRACT_INT_FIELD(reader, row, "order_in_rowkey", column.order_in_rowkey_, int32_t);
  EXTRACT_CREATE_TIME_FIELD(reader, row, "gm_create", column.gm_create_, ObCreateTime);
  EXTRACT_MODIFY_TIME_FIELD(reader, row, "gm_modify", column.gm_modify_, ObModifyTime);

  return ret;
}

int ObSchemaServiceImpl::assemble_join_info(const SQLQueryResultReader& reader, 
    const ObRow& row, JoinInfo& join_info)
{
  int ret = OB_SUCCESS;
  EXTRACT_STRBUF_FIELD(reader, row, "left_table_name", join_info.left_table_name_, OB_MAX_TABLE_NAME_LENGTH);
  EXTRACT_INT_FIELD(reader, row, "left_table_id", join_info.left_table_id_, uint64_t);
  EXTRACT_STRBUF_FIELD(reader, row, "left_column_name", join_info.left_column_name_, OB_MAX_COLUMN_NAME_LENGTH);
  EXTRACT_INT_FIELD(reader, row, "left_column_id", join_info.left_column_id_, uint64_t);
  EXTRACT_STRBUF_FIELD(reader, row, "right_table_name", join_info.right_table_name_, OB_MAX_TABLE_NAME_LENGTH);
  EXTRACT_INT_FIELD(reader, row, "right_table_id", join_info.right_table_id_, uint64_t);
  EXTRACT_STRBUF_FIELD(reader, row, "right_column_name", join_info.right_column_name_, OB_MAX_COLUMN_NAME_LENGTH);
  EXTRACT_INT_FIELD(reader, row, "right_column_id", join_info.right_column_id_, uint64_t);

  return ret;
}

int ObSchemaServiceImpl::get_table_schema(const ObString& table_name, TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  table_schema.clear();

  if (table_name == first_tablet_entry_name)
  {
    ret = ObExtraTablesSchema::first_tablet_entry_schema(table_schema);
  }
  else if (table_name == column_table_name)
  {
    ret = ObExtraTablesSchema::all_all_column_schema(table_schema);
  }
  else if (table_name == joininfo_table_name)
  {
    ret = ObExtraTablesSchema::all_join_info_schema(table_schema);
  }
  else
  {
    if(!check_inner_stat())
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "check inner stat fail");
    }
    else
    {
      ret = fetch_table_schema(table_name, table_schema);
    }
  }

  if(OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "get table schema fail:ret[%d]", ret);
  }

  return ret;
}

int ObSchemaServiceImpl::fetch_table_info(const ObString& table_name, TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  SQLQueryResultReader reader;
  ObRow row;
  char sql[OB_DEFAULT_SQL_LENGTH];
  int64_t sql_len = 0;

  if ( (sql_len = snprintf(sql, OB_DEFAULT_SQL_LENGTH,  
          "select table_name,table_id,"
          "table_type,load_type,table_def_type,rowkey_column_num,replica_num,"
          "max_used_column_id,create_mem_version,tablet_max_size,tablet_block_size,"
          "max_rowkey_length,compress_func_name,expire_condition,is_use_bloomfilter,"
          "is_read_static,merge_write_sstable_version,is_pure_update_table,rowkey_split,"
          "create_time_column_id,modify_time_column_id,character_set,data_table_id,"
          "index_status from %s where table_name = '%.*s'",
          FIRST_TABLET_TABLE_NAME, table_name.length(), table_name.ptr())) >= OB_DEFAULT_SQL_LENGTH - 1)
  {
    TBSYS_LOG(WARN, "sql buffer not enough %s", sql);
    ret = OB_BUF_NOT_ENOUGH;
  }
  else if (OB_SUCCESS != (ret = reader.query_one_row(*client_proxy_, sql, row)))
  {
    TBSYS_LOG(WARN, "execute sql %s ret=%d", sql, ret);
  }
  else if (OB_SUCCESS != (ret = assemble_table(reader, row, table_schema)))
  {
    TBSYS_LOG(WARN, "assemble_table row [%s] ret=%d", to_cstring(row), ret);
  }
  return ret;
}

int ObSchemaServiceImpl::fetch_column_info(const uint64_t table_id, TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  SQLQueryResultReader reader;
  ObRow row;
  char sql[OB_DEFAULT_SQL_LENGTH];
  int64_t sql_len = 0;
  ColumnSchema column;

  if ( (sql_len = snprintf(sql, OB_DEFAULT_SQL_LENGTH,  
          "select column_name,column_id,gm_create,gm_modify,column_group_id,rowkey_id,"
          "join_table_id,join_column_id,data_type,data_length,data_precision,"
          "data_scale,nullable,length_in_rowkey,order_in_rowkey from %s where table_id=%lu",
          OB_ALL_COLUMN_TABLE_NAME, table_id)) >= OB_DEFAULT_SQL_LENGTH - 1)
  {
    TBSYS_LOG(WARN, "sql buffer not enough %s", sql);
    ret = OB_BUF_NOT_ENOUGH;
  }
  else if (OB_SUCCESS != (ret = reader.query(*client_proxy_, sql)))
  {
    TBSYS_LOG(WARN, "execute sql %s ret=%d", sql, ret);
  }
  else
  {
    while (OB_SUCCESS == (ret = reader.get_next_row(row)))
    {
      if(OB_SUCCESS != (ret = assemble_column(reader, row, column)))
      {
        TBSYS_LOG(WARN, "assemble row[%s] fail:ret[%d]", to_cstring(row), ret);
        break;
      }
      else if(OB_SUCCESS != (ret = table_schema.add_column(column)))
      {
        TBSYS_LOG(WARN, "add column to table schema fail:ret[%d]", ret);
        break;
      }
    }
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObSchemaServiceImpl::fetch_join_info(const uint64_t table_id, TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  SQLQueryResultReader reader;
  ObRow row;
  char sql[OB_DEFAULT_SQL_LENGTH];
  int64_t sql_len = 0;
  JoinInfo join_info;

  if ( (sql_len = snprintf(sql, OB_DEFAULT_SQL_LENGTH,  
          "select left_table_id,left_column_id,right_table_id,right_column_id,"
          "left_table_name,left_column_name,right_table_name,right_column_name "
          "from %s where left_table_id = %lu",
          OB_ALL_JOININFO_TABLE_NAME, table_id)) >= OB_DEFAULT_SQL_LENGTH - 1)
  {
    TBSYS_LOG(WARN, "sql buffer not enough %s", sql);
    ret = OB_BUF_NOT_ENOUGH;
  }
  else if (OB_SUCCESS != (ret = reader.query(*client_proxy_, sql)))
  {
    TBSYS_LOG(WARN, "execute sql %s ret=%d", sql, ret);
  }
  else
  {
    while (OB_SUCCESS == (ret = reader.get_next_row(row)))
    {
      if(OB_SUCCESS != (ret = assemble_join_info(reader, row, join_info)))
      {
        TBSYS_LOG(WARN, "assemble row[%s] fail:ret[%d]", to_cstring(row), ret);
        break;
      }
      else if(OB_SUCCESS != (ret = table_schema.add_join_info(join_info)))
      {
        TBSYS_LOG(WARN, "add column to table schema fail:ret[%d]", ret);
        break;
      }
    }
    if (OB_SUCCESS == ret || OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObSchemaServiceImpl::fetch_table_schema(const ObString& table_name, TableSchema& table_schema)
{
  int ret = OB_SUCCESS;

  TBSYS_LOG(TRACE, "fetch_table_schema begin: table_name=%.*s,", table_name.length(), table_name.ptr());


  if(!check_inner_stat())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else if (OB_SUCCESS != (ret = fetch_table_info(table_name, table_schema)))
  {
    TBSYS_LOG(WARN, "fetch_table_info %.*s ret=%d", table_name.length(), table_name.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = fetch_column_info(table_schema.table_id_, table_schema)))
  {
    TBSYS_LOG(WARN, "fetch_table_info %lu ret=%d", table_schema.table_id_, ret);
  }
  else if (OB_SUCCESS != (ret = fetch_join_info(table_schema.table_id_, table_schema)))
  {
    TBSYS_LOG(WARN, "fetch_join_info %lu ret=%d", table_schema.table_id_, ret);
  }
  else if (!table_schema.is_valid())
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "check the table schema is invalid:table_name[%s]", table_schema.table_name_);
  }

  return ret;
}

int ObSchemaServiceImpl::set_max_used_table_id(const uint64_t max_used_tid)
{
  int ret = OB_SUCCESS;
  char  sql[OB_DEFAULT_SQL_LENGTH];
  int64_t pos = 0;
  ObObj rowkey_objs[2];
  rowkey_objs[0].set_int(0); // cluster_id
  rowkey_objs[1].set_varchar(ObString::make_string("ob_max_used_table_id")); // name

  if(!check_inner_stat())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else if (OB_SUCCESS != (ret = databuff_printf(sql, 
          OB_DEFAULT_SQL_LENGTH, pos, "update %s set value= %lu where ", 
          OB_ALL_SYS_STAT_TABLE_NAME, max_used_tid)))
  {
    TBSYS_LOG(WARN, "build update stmt [%s] [%lu] ret=%d", 
        OB_ALL_SYS_STAT_TABLE_NAME, max_used_tid, ret);
  }
  else if (OB_SUCCESS != (ret = build_where_condition( 
          SC("cluster_id")("name"), rowkey_objs , 
          sql, OB_DEFAULT_SQL_LENGTH, pos)))
  {
    TBSYS_LOG(WARN, "build where cond [%s] [%lu] ret=%d", 
        OB_ALL_SYS_STAT_TABLE_NAME, max_used_tid, ret);
  }
  else if (OB_SUCCESS != (ret = client_proxy_->modify(sql)))
  {
    TBSYS_LOG(WARN, "execute sql [%s] ret=%d", sql, ret);
  }
  return ret;
}

int ObSchemaServiceImpl::get_max_used_table_id(uint64_t &max_used_tid)
{
  int ret = OB_SUCCESS;
  SQLQueryResultReader reader;
  ObRow row;
  char sql[OB_DEFAULT_SQL_LENGTH] ;
  int64_t pos = 0;
  const ObObj *value = NULL;
  ObString str_value;

  if(!check_inner_stat())
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  else if ( OB_SUCCESS != (ret = 
        databuff_printf(sql, OB_DEFAULT_SQL_LENGTH, pos, 
          "select value from %s where "
          "cluster_id = 0 and name='ob_max_used_table_id'", 
          OB_ALL_SYS_STAT_TABLE_NAME)))
  {
    TBSYS_LOG(WARN, "build select stmt %s, ret=%d", sql, ret);
  }
  else if (OB_SUCCESS != (ret = reader.query_one_row(*client_proxy_, sql, row)))
  {
    TBSYS_LOG(WARN, "execute sql %s ret=%d", sql, ret);
  }
  else if (OB_SUCCESS != (ret = row.raw_get_cell(0, value)) || NULL == value)
  {
    TBSYS_LOG(WARN, "row (%s) get cell 0 ret=%d, value=%p", to_cstring(row), ret, value);
  }
  else if (value->get_type() != ObVarcharType 
      || OB_SUCCESS != (ret = value->get_varchar(str_value)))
  {
    TBSYS_LOG(WARN, "value (%s) is unexpected.", to_cstring(*value));
  }
  else
  {
    snprintf(sql, OB_DEFAULT_SQL_LENGTH, "%.*s", str_value.length(), str_value.ptr());
    max_used_tid = strtoull(sql, NULL, 10);
  }

  return ret;
}

int ObSchemaServiceImpl::alter_table(const AlterTableSchema & schema)
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  tbsys::CThreadGuard guard(&mutex_);
  ObMutator* mutator = NULL;
  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get thread specific Mutator fail");
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = alter_table_mutator(schema, mutator);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "set alter table mutator fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
    }
    else
    {
      TBSYS_LOG(INFO, "send alter table to ups succ.");
    }
  }
  return ret;
}
int ObSchemaServiceImpl::generate_new_table_name(char* buf, const uint64_t length, const char* table_name, const uint64_t table_name_length)
{
  int ret = OB_SUCCESS;
  if (table_name_length + sizeof(TMP_PREFIX) >= length
      || NULL == buf
      || NULL == table_name)
  {
    TBSYS_LOG(WARN, "invalid buf_len. need size=%ld, exist_buf_size=%ld, buf=%p, table_name=%p",
        table_name_length + 1, length, buf, table_name);
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == ret)
  {
    snprintf(buf, sizeof(TMP_PREFIX), TMP_PREFIX);
    snprintf(buf + sizeof(TMP_PREFIX), table_name_length, table_name);
    buf[sizeof(TMP_PREFIX) + table_name_length] = '\0';
  }
  return ret;
}
int ObSchemaServiceImpl::modify_table_id(const TableSchema& table_schema, const int64_t new_table_id)
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    TBSYS_LOG(WARN, "check inner stat fail");
  }
  char table_name_buf[OB_MAX_TABLE_NAME_LENGTH];
  if (OB_SUCCESS == ret)
  {
    ret = generate_new_table_name(table_name_buf, OB_MAX_TABLE_NAME_LENGTH, table_schema.table_name_, sizeof(table_schema.table_name_));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to genearte new table_name. table_name=%s", table_schema.table_name_);
    }
  }
  common::TableSchema new_table_schema = table_schema;
  if (OB_SUCCESS == ret)
  {
    memcpy(new_table_schema.table_name_, table_name_buf, OB_MAX_TABLE_NAME_LENGTH);
    new_table_schema.table_id_ = new_table_id;
    ret = create_table(new_table_schema);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "fail to create table. table_name=%s", table_name_buf);
    }
  }
  ObMutator* mutator = NULL;
  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS == ret)
    {
      mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
      if(NULL == mutator)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "get thread specific ObMutator fail");
      }
    }

    if(OB_SUCCESS == ret)
    {
      ret = mutator->reset();
      if(OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
      }
    }
    int64_t old_table_id = table_schema.table_id_;
    if (OB_SUCCESS == ret)
    {
      ObRowkey rowkey;
      ObObj rowkey_value;
      ObString table_name;
      table_name.assign_ptr(new_table_schema.table_name_,
          static_cast<int32_t>(strlen(new_table_schema.table_name_)));
      rowkey_value.set_varchar(table_name);
      rowkey.assign(&rowkey_value, 1);
      ObObj value;
      value.set_int(old_table_id);
      ret = mutator->update(OB_FIRST_TABLET_ENTRY_TID, rowkey, first_tablet_entry_cid::TID, value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to add update cell to mutator. ret=%d, rowkey=%s", ret, to_cstring(table_name));
      }
    }
    if (OB_SUCCESS == ret)
    {
      ObRowkey rowkey;
      ObObj rowkey_value;
      ObString table_name;
      table_name.assign_ptr(const_cast<char*>(table_schema.table_name_), static_cast<int32_t>(strlen(table_schema.table_name_)));
      rowkey_value.set_varchar(table_name);
      rowkey.assign(&rowkey_value, 1);
      ObObj value;
      value.set_int(new_table_id);
      ret = mutator->update(OB_FIRST_TABLET_ENTRY_TID, rowkey, first_tablet_entry_cid::TID, value);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to add update cell to mutator. ret=%d, rowkey=%s", ret, to_cstring(table_name));
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(WARN, "apply mutator to update server fail. ret=%d", ret);
    }
  }
  ObString drop_table_name;
  drop_table_name.assign_ptr(new_table_schema.table_name_, static_cast<int32_t>(strlen(new_table_schema.table_name_)));
  if (OB_SUCCESS != drop_table(drop_table_name))
  {
    TBSYS_LOG(WARN, "fail to drop table. table_name=%s", new_table_schema.table_name_);
  }
  return ret;
}

int ObSchemaServiceImpl::modify_index_table_status(const ObString& index_table_name, const int32_t state)
{
  int ret = OB_SUCCESS;
  ObMutator* mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);

  if(!check_inner_stat())
  {
    TBSYS_LOG(WARN, "check inner stat fail");
    ret = OB_ERROR;
  }
  else if(NULL == index_table_name.ptr() || index_table_name.length() <= 0)
  {
    TBSYS_LOG(WARN, "invalid index_table_name, ptr=%p, length=%d", 
              index_table_name.ptr(), index_table_name.length());
    ret = OB_ERROR;
  }
  else if(NULL == mutator)
  {
    TBSYS_LOG(WARN, "get thread specific ObMutator fail");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if(OB_SUCCESS != (ret = mutator->reset()))
  {
    TBSYS_LOG(WARN, "reset ob mutator fail, ret=%d", ret);
  }
  else 
  {
    ObRowkey rowkey;
    ObObj rowkey_value;
    ObObj value;
    rowkey_value.set_varchar(index_table_name);
    rowkey.assign(&rowkey_value, 1);
    value.set_int(state);

    if (OB_SUCCESS != (ret = mutator->update(first_tablet_entry_name, rowkey, 
        OB_STR("index_status"), value)))
    {
      TBSYS_LOG(WARN, "fail to add update cell to mutator, rowkey=%s, ret=%d", 
                to_cstring(index_table_name), ret);
    }
    else if (OB_SUCCESS != (ret = client_proxy_->mutate(*mutator)))
    {
      TBSYS_LOG(WARN, "apply mutator to update server fail, rowkey=%s, ret=%d", 
                to_cstring(index_table_name), ret);
    }
  }

  return ret;
}
