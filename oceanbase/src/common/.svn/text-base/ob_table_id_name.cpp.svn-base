#include "ob_table_id_name.h"
#include "utility.h"
#include "common/ob_row.h"

using namespace oceanbase;
using namespace common;

ObTableIdNameIterator::ObTableIdNameIterator()
   :inited_(false), only_core_tables_(true), 
   table_idx_(-1), client_proxy_(NULL) 
{
}

ObTableIdNameIterator::~ObTableIdNameIterator()
{
  destroy_objects();
}

int ObTableIdNameIterator::scan_tables()
{
  int ret = OB_SUCCESS;
  const char * sql = "select table_name, table_id from __first_tablet_entry";
  if(OB_SUCCESS != (ret = reader_->query(*client_proxy_, sql)))
  {
    TBSYS_LOG(WARN, "nb accessor scan fail:ret[%d]", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "scan first_tablet_entry success. scanner size=%ld", 
        reader_->get_result_set().get_new_scanner().get_row_num());
  }
  return ret;
}

int ObTableIdNameIterator::alloc_objects()
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (NULL == (ptr = ob_malloc(sizeof(SQLQueryResultReader))))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (NULL == (reader_ = new (ptr) (SQLQueryResultReader)))
  {
    ret = OB_NOT_INIT;
  }
  return ret;
}

int ObTableIdNameIterator::destroy_objects()
{
  if (NULL != reader_)
  {
    reader_->~SQLQueryResultReader();
    ob_free(reader_);
    reader_ = NULL;
  }
  return OB_SUCCESS;
}

int ObTableIdNameIterator::init(ObScanHelper* client_proxy, bool only_core_tables)
{
  int ret = OB_SUCCESS;
  only_core_tables_ = only_core_tables;
  client_proxy_ = client_proxy;
  table_idx_ = -1;
  if(NULL == client_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "client_proxy is null");
  }
  else if (OB_SUCCESS != (ret = alloc_objects())) 
  {
    TBSYS_LOG(WARN, "alloc_objects ret=%d", ret);
  }
  else  if (!only_core_tables)
  {
    ret = scan_tables();
  }

  if (OB_SUCCESS == ret) 
  {
    inited_ = true;
  }
  return ret;
}

int ObTableIdNameIterator::get_next(ObTableIdName** table_info)
{
  int ret = OB_SUCCESS;
  ObRow row;
  if(!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "not init. can NOT iterate");
  }
  else
  {
    ++table_idx_;
    if (table_idx_ < 3)
    {
      // get core table, do nothing;
      ret = internal_get(table_info);
    }
    else if (only_core_tables_)
    {
      ret = OB_ITER_END;
    }
    else if (OB_SUCCESS != (ret = reader_->get_next_row(row)))
    {
      if (OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "get_next_row ret=%d", ret);
      }
    }
    else if (OB_SUCCESS != (ret = normal_get(row, table_info)))
    {
      TBSYS_LOG(WARN, "normal_get [%d][%s] ret=%d", table_idx_, to_cstring(row), ret);
    }
  }

  return ret;
}

int ObTableIdNameIterator::internal_get(ObTableIdName** table_info)
{
  int ret = OB_SUCCESS;
  switch(table_idx_)
  {
    case 0:
      table_id_name_.table_name_.assign_ptr(const_cast<char*>(FIRST_TABLET_TABLE_NAME),
          static_cast<int32_t>(strlen(FIRST_TABLET_TABLE_NAME)));
      table_id_name_.table_id_ = OB_FIRST_TABLET_ENTRY_TID;
      *table_info = &table_id_name_;
      break;
    case 1:
      table_id_name_.table_name_.assign_ptr(const_cast<char*>(OB_ALL_COLUMN_TABLE_NAME),
          static_cast<int32_t>(strlen(OB_ALL_COLUMN_TABLE_NAME)));
      table_id_name_.table_id_ = OB_ALL_ALL_COLUMN_TID;
      *table_info = &table_id_name_;
      break;
    case 2:
      table_id_name_.table_name_.assign_ptr(const_cast<char*>(OB_ALL_JOININFO_TABLE_NAME),
          static_cast<int32_t>(strlen(OB_ALL_JOININFO_TABLE_NAME)));
      table_id_name_.table_id_ = OB_ALL_JOIN_INFO_TID;
      *table_info = &table_id_name_;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(ERROR, "unexpected branch");
      break;
  }
  return ret;
}

int ObTableIdNameIterator::normal_get(const ObRow & row, ObTableIdName** table_info)
{
  int ret = OB_SUCCESS;

  EXTRACT_VARCHAR_FIELD((*reader_), row, "table_name", table_id_name_.table_name_);
  EXTRACT_INT_FIELD((*reader_), row, "table_id", table_id_name_.table_id_, uint64_t);
  if (OB_SUCCESS == ret)
  {
    *table_info = &table_id_name_;
  }
  return ret;
}

