/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_compact_cell_iterator.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_compact_cell_iterator.h"
#include "ob_object.h"

using namespace oceanbase;
using namespace common;

ObCompactCellIterator::ObCompactCellIterator() 
  :column_id_(OB_INVALID_ID),
  row_start_(0),
  store_type_(INVALID_COMPACT_STORE_TYPE),
  step_(0),
  is_row_finished_(true),
  inited_(false)
{
  //buf_reader_(reset in init())
  //value_(construct)
}

int ObCompactCellIterator::init(const char *buf, const enum ObCompactStoreType store_type)
{
  int ret = OB_SUCCESS;

  if(NULL == buf)
  { 
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "init buf is null");
  }
  else if (store_type < SPARSE || store_type > DENSE_DENSE)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid row store type: store_type=[%d]", store_type);
  }
  else if(OB_SUCCESS != (ret = buf_reader_.assign(buf)))
  {
    TBSYS_LOG(WARN, "buf read assign fail: ret=[%d]", ret);
  }
  else
  {
    row_start_ = 0;
    store_type_ = store_type;
    step_ = 0;
    is_row_finished_ = true;
    inited_ = true;
  }

  return ret;
}

int ObCompactCellIterator::init(const ObString &buf, const enum ObCompactStoreType store_type)
{
  int ret = OB_SUCCESS;

  if (store_type < SPARSE || store_type > DENSE_DENSE)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid row store type: store_type=[%d]", store_type);
  }
  else if(OB_SUCCESS != (ret = buf_reader_.assign(buf.ptr(), buf.length())))
  {
    TBSYS_LOG(WARN, "buf read assign fail: ret=[%d], ptr=[%p], len=[%d]", ret, buf.ptr(), buf.length());
  }
  else
  {
    row_start_ = 0;
    store_type_ = store_type;
    step_ = 0;
    is_row_finished_ = true;
    inited_ = true;
  }

  return ret;
}


int ObCompactCellIterator::get_one_row_dense(
    ObObj* const row_obj_array,
    int64_t& row_column_count)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; i < row_column_count; i ++)
  {
    if (OB_SUCCESS != (ret = parse(buf_reader_, row_obj_array[i])))
    {
      TBSYS_LOG(WARN, "parse cell from buffer error: i=[%ld], ret=[%d]", i, ret);
      break;
    }
    else if (1 == step_)
    {
      row_column_count = i;
      break;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (1 != step_)
    {
      TBSYS_LOG(WARN, "the column num is overflow or row deserialize error");
      ret = OB_ERROR;
    }
    else
    {
      step_ = 0;
    }
  }

  return ret;
}

int ObCompactCellIterator::get_one_row_dense_dense(
    ObObj* const row_obj_array,
    const int64_t max_scan_count,
    int64_t& row_column_count)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_column_count = 0;

  if (max_scan_count >= row_column_count)
  {
    TBSYS_LOG(WARN, "max scan count is must less than row_column_count: max_scan_count=[%ld], row_column_count=[%ld]", max_scan_count, row_column_count);
    ret = OB_ERROR;
  }

  //rowkey
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < row_column_count; i ++)
   {
      if (OB_SUCCESS != (ret = parse(buf_reader_, row_obj_array[i])))
      {
        TBSYS_LOG(WARN, "parse cell from buffer error: i=[%ld], ret=[%d]", i, ret);
        break;
      }
      else if (1 == step_)
      {
        rowkey_column_count = i;
        break;
      }
    }
  }

  if (OB_SUCCESS == ret && 1 != step_)
  {
    TBSYS_LOG(WARN, "the column num of rowkey is overflow or rowkey deserialize error");
    ret = OB_ERROR;
  }

  //rowvalue
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = rowkey_column_count; i < row_column_count; i ++)
    {
      if (i > max_scan_count)
      {
        row_column_count = i;
        step_ = 0;
        break;
      }
      else if (OB_SUCCESS != (ret = parse(buf_reader_, row_obj_array[i])))
      {
        TBSYS_LOG(WARN, "parse cell from buffer error: i=[%ld], ret=[%d]", i, ret);
        break;
      }
      else if (0 == step_)
      {
        row_column_count = i;
        break;
      }
    }
  }

  if (OB_SUCCESS == ret && 0 != step_)
  {
    TBSYS_LOG(WARN, "the column num of row is overflow or row deserialize error");
    ret = OB_ERROR;
  }

  return ret;
}

int ObCompactCellIterator::get_one_row_dense_sparse(
    ObObj* const row_obj_array,
    uint64_t* const row_column_array,
    int64_t& row_column_count)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_column_count = 0;

  //rowkey
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < row_column_count; i ++)
    {
      if (OB_SUCCESS != (ret = parse(buf_reader_, row_obj_array[i])))
      {
        TBSYS_LOG(WARN, "parase cell from buffer error: i=[%ld], ret=[%d]", i, ret);
        break;
      }
      else if (1 == step_)
      {
        rowkey_column_count = i;
        break;
      }
    }
  }

  if (OB_SUCCESS == ret && 1 != step_)
  {
    TBSYS_LOG(WARN, "the column num of rowkey is overflow or rowkey deserialize error: step_=[%d]", step_);
    ret = OB_ERROR;
  }

  //rowvalue
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = rowkey_column_count; i < row_column_count; i ++)
    {
      if (OB_SUCCESS != (ret = parse(buf_reader_, row_obj_array[i], &row_column_array[i])))
      {
        TBSYS_LOG(WARN, "parse cell from buffer error: i=[%ld], ret=[%d]", i, ret);
        break;
      }
      else if (0 == step_)
      {
        row_column_count = i;
        break;
      }
    }
  }

  if (OB_SUCCESS == ret && 0 != step_)
  {
    TBSYS_LOG(WARN, "the column num of rowvalue is overflow or rowvalue deserialize error: step_=[%d]", step_);
    ret = OB_ERROR;
  }

  return ret;
}

int ObCompactCellIterator::next_cell()
{
  int ret = OB_SUCCESS;

  if(!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "not init yet");
  }
  else if (is_row_finished_)
  {
    is_row_finished_ = false;
    row_start_ = buf_reader_.pos();
  }

  if(OB_SUCCESS == ret)
  {
    column_id_ = OB_INVALID_ID;
    switch(store_type_)
    {
    case SPARSE:
      ret = parse(buf_reader_, value_, &column_id_);
      if(OB_SUCCESS != ret && OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "parse cell fail: ret[%d]", ret);
      }
      break;
    case DENSE:
      ret = parse(buf_reader_, value_);
      if(OB_SUCCESS != ret && OB_ITER_END != ret)
      {
        TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
      }
      break;
    case DENSE_SPARSE:
      if(0 == step_)
      {
        ret = parse(buf_reader_, value_);
        if(OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
        }
      }
      else if(1 == step_)
      {
        ret = parse(buf_reader_, value_, &column_id_);
        if(OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
        }
      }
      break;
    case DENSE_DENSE:
      if(0 == step_)
      {
        ret = parse(buf_reader_, value_);
        if(OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
        }
      }
      else if(1 == step_)
      {
        ret = parse(buf_reader_, value_);
        if(OB_SUCCESS != ret && OB_ITER_END != ret)
        {
          TBSYS_LOG(WARN, "parse cell fail:ret[%d]", ret);
        }
      }
      break;
    default:
      TBSYS_LOG(WARN, "ERROR");
      ret = OB_ERROR;
      break;
    }
  }

  return ret;
}

int ObCompactCellIterator::get_cell(const ObObj *&value, bool *is_row_finished /* = NULL */, ObString *row /* = NULL */)
{
  int ret = OB_SUCCESS;
  uint64_t column_id = OB_INVALID_ID;
  if(OB_SUCCESS != (ret = get_cell(column_id, value, is_row_finished, row)))
  {
    TBSYS_LOG(WARN, "get cell fail:ret[%d]", ret);
  }
  else if(OB_INVALID_ID != column_id)
  {
    ret = OB_NOT_SUPPORTED;
    TBSYS_LOG(WARN, "column_id should be OB_INVALID_ID. this function only is used in dense format!");
  }
  return ret;
}

int ObCompactCellIterator::get_cell(uint64_t &column_id, ObObj &value, bool *is_row_finished, ObString *row)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  if(!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "not init yet");
  }
  else if(OB_SUCCESS != (ret = get_cell(column_id, cell, is_row_finished, row)))
  {
    TBSYS_LOG(WARN, "get cell fail:ret[%d]", ret);
  }
  else
  {
    value = *cell;
  }
  return ret;
}

int ObCompactCellIterator::get_cell(uint64_t &column_id, const ObObj *&value, bool *is_row_finished, ObString *row)
{
  int ret = OB_SUCCESS;
  
  if(!inited_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "not init yet");
  }

  if(OB_SUCCESS == ret)
  {
    column_id = column_id_;
    value = &value_;

    if(NULL != is_row_finished)
    {
      *is_row_finished = is_row_finished_;
    }

    if(is_row_finished_ && NULL != row)
    {
      row->assign_ptr(const_cast<char*>(buf_reader_.buf() + row_start_), (int32_t)(buf_reader_.pos() - row_start_));
    }
  }
  return ret;
}

int ObCompactCellIterator::parse(ObBufferReader &buf_reader, ObObj &value, uint64_t *column_id /* = NULL */)
{
  int ret = OB_SUCCESS;

  const ObCellMeta *cell_meta = NULL;
  ret = buf_reader.get<ObCellMeta>(cell_meta);

  const int8_t *int8_value = NULL;
  const int16_t *int16_value = NULL;
  const int32_t *int32_value = NULL;
  const int64_t *int64_value = NULL;
  const float *float_value = NULL;
  const double *double_value = NULL;
  const bool *bool_value = NULL;
  const ObDateTime *datetime_value = NULL;
  const ObPreciseDateTime *precise_datetime_value = NULL;
  const ObCreateTime *createtime_value = NULL;
  const ObModifyTime *modifytime_value = NULL;
  bool is_add = false;

#define case_clause_with_add(type_name, type, set_type) \
  ret = buf_reader.get<type_name>((const type_name*&)type##_value); \
  if(OB_SUCCESS == ret) \
  { \
    value.set_##set_type(*type##_value, is_add); \
  } \
  break


#define case_clause(type_name, type) \
  ret = buf_reader.get<type_name>((const type_name*&)type##_value); \
  if(OB_SUCCESS == ret) \
  { \
    value.set_##type(*type##_value); \
  } \
  break

  if(OB_SUCCESS == ret)
  {
    is_add = cell_meta->attr_ == ObCellMeta::AR_ADD ? true : false;

    switch(cell_meta->type_)
    {
    case ObCellMeta::TP_NULL:
      value.set_null();
      break;
    case ObCellMeta::TP_INT8:
      case_clause_with_add(int8_t, int8, int);
    case ObCellMeta::TP_INT16:
      case_clause_with_add(int16_t, int16, int);
    case ObCellMeta::TP_INT32:
      case_clause_with_add(int32_t, int32, int);
    case ObCellMeta::TP_INT64:
      case_clause_with_add(int64_t, int64, int);
    case ObCellMeta::TP_DECIMAL:
      if(OB_SUCCESS != (ret = parse_decimal(buf_reader, value)))
      {
        TBSYS_LOG(WARN, "parse number fail:ret[%d]", ret);
      }
      else if(OB_SUCCESS != (ret = value.set_add(is_add)))
      {
        TBSYS_LOG(WARN, "set add fail:ret[%d]", ret);
      }
      break;
    case ObCellMeta::TP_TIME:
      case_clause_with_add(ObDateTime, datetime, datetime);
    case ObCellMeta::TP_PRECISE_TIME:
      case_clause_with_add(ObPreciseDateTime, precise_datetime, precise_datetime);
    case ObCellMeta::TP_CREATE_TIME:
      case_clause(ObCreateTime, createtime);
    case ObCellMeta::TP_MODIFY_TIME:
      case_clause(ObModifyTime, modifytime);
    case ObCellMeta::TP_FLOAT:
      case_clause_with_add(float, float, float);
    case ObCellMeta::TP_DOUBLE:
      case_clause_with_add(double, double, double);
    case ObCellMeta::TP_BOOL:
      case_clause(bool, bool);

    case ObCellMeta::TP_VARCHAR:
      ret = parse_varchar(buf_reader, value);
      break;

    case ObCellMeta::TP_ESCAPE:
      switch(cell_meta->attr_)
      {
      case ObCellMeta::ES_END_ROW:
        value.set_ext(ObActionFlag::OP_END_ROW);
        step_ = !step_;
        is_row_finished_ = true;
        break;
      case ObCellMeta::ES_DEL_ROW:
        value.set_ext(ObActionFlag::OP_DEL_ROW);
        break;
      case ObCellMeta::ES_NOP_ROW:
        value.set_ext(ObActionFlag::OP_NOP);
        break;
      case ObCellMeta::ES_NOT_EXIST_ROW:
        value.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        break;
      case ObCellMeta::ES_VALID:
        value.set_ext(ObActionFlag::OP_VALID);
        break;
      case ObCellMeta::ES_NEW_ADD:
        value.set_ext(ObActionFlag::OP_NEW_ADD);
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        TBSYS_LOG(WARN, "unsupported escape %d", cell_meta->attr_);
      }
      break;
    case ObCellMeta::TP_EXTEND:
      switch(cell_meta->attr_)
      {
      case ObCellMeta::AR_MIN:
        value.set_min_value();
        break;
      case ObCellMeta::AR_MAX:
        value.set_max_value();
        break;
      case ObCellMeta::ET_DEL_ROW:
        value.set_ext(ObActionFlag::OP_DEL_ROW);
        break;
      case ObCellMeta::ET_NEW_ADD:
        value.set_ext(ObActionFlag::OP_NEW_ADD);
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        TBSYS_LOG(WARN, "unsupported extend:cell_meta->attr_=%d",
            cell_meta->attr_);
      }
      break;
    default:
      ret = OB_NOT_SUPPORTED;
      TBSYS_LOG(WARN, "unsupported type %d", cell_meta->type_);
    }
  }

  if(NULL != column_id)
  {
    if(OB_SUCCESS == ret && ObCellMeta::TP_ESCAPE != cell_meta->type_)
    {
      const uint32_t *tmp_column_id = NULL;
      ret = buf_reader.get<uint32_t>(tmp_column_id);
      if(OB_SUCCESS == ret)
      {
        if(*tmp_column_id == OB_COMPACT_COLUMN_INVALID_ID)
        {
          *column_id = OB_INVALID_ID;
        }
        else
        {
          *column_id = *tmp_column_id;
        }
      }
    }
    else
    {
      *column_id = OB_INVALID_ID;
    }
  }

  return ret;
}

int ObCompactCellIterator::parse_decimal(ObBufferReader &buf_reader, ObObj &value) const
{
  int ret = OB_SUCCESS;
  const uint8_t *vscale = NULL;
  const ObDecimalMeta *dec_meta = NULL;
  const uint32_t *word = NULL;
  uint32_t *words = NULL;

  if(OB_SUCCESS != (ret = buf_reader.get<uint8_t>(vscale)))
  {
    TBSYS_LOG(WARN, "read vscale fail:ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = buf_reader.get<ObDecimalMeta>(dec_meta)))
  {
    TBSYS_LOG(WARN, "get decimal meta fail:ret[%d]", ret);
  }
  else
  {
    value.meta_.type_ = ObDecimalType;
    value.meta_.dec_vscale_ = *vscale & ObObj::META_VSCALE_MASK;
    value.meta_.dec_precision_ = dec_meta->dec_precision_;
    value.meta_.dec_scale_ = dec_meta->dec_scale_;
    value.meta_.dec_nwords_ = dec_meta->dec_nwords_;
  }

  uint8_t nwords = value.meta_.dec_nwords_;
  nwords ++;

  if (OB_SUCCESS == ret)
  {
    if (nwords <= 3)
    {
      words = reinterpret_cast<uint32_t*>(&(value.val_len_));
    }
    else
    {
      //@todo, use ob_pool.h to allocate memory
      ret = OB_NOT_IMPLEMENT;
    }
  }

  if(OB_SUCCESS == ret)
  {
    for(int8_t i=0;OB_SUCCESS == ret && i<nwords;i++)
    {
      if(OB_SUCCESS != (ret = buf_reader.get<uint32_t>(word)))
      {
        TBSYS_LOG(WARN, "get word fail:ret[%d]", ret);
      }
      else
      {
        words[i] = *word;
      }
    }
  }

  return ret;
}


int ObCompactCellIterator::fill_row(const sstable::ObSimpleColumnIndexes& query_columns, common::ObRow& value, int64_t& filled)
{
  int ret = OB_SUCCESS;
  int64_t result_index = 0;
  int64_t value_index = 0;
  bool is_row_finished = false;
  int64_t not_null_col_num = query_columns.get_not_null_column_count();
  common::ObObj obj;
  int step = 0;

  while (OB_SUCCESS == ret && filled < not_null_col_num && false == is_row_finished)
  {
    if (OB_SUCCESS != (ret = parse(buf_reader_, obj)))
    {
      TBSYS_LOG(WARN, "row get next cell error: index=[%ld], ret=[%d]", value_index, ret);
    }
    else if (ObExtendType == obj.get_type() &&
        ObActionFlag::OP_END_ROW == obj.get_ext())
    {
      step++;
      if((DENSE_DENSE == store_type_ && 2 == step) || (DENSE_SPARSE == store_type_ && 1 == step))
        break;
      else
        continue;
    }
    else if ((result_index = query_columns.find_by_offset(value_index)) < 0)
    {
      // this column not in query columns, ignore;
    }
    else if (OB_SUCCESS != (ret = value.raw_set_cell(result_index, obj)))
    {
      TBSYS_LOG(ERROR, "cannot get obj at cell:%ld", result_index);
    }
    else
    {
      ++filled;
    }
    ++value_index;
  }

  return ret;
}


int ObCompactCellIterator::get_sparse_rowvalue(int64_t filled, const sstable::ObSimpleColumnIndexes& query_columns, common::ObRow& value)
{
  int ret = OB_SUCCESS;
  int64_t value_index = 0;
  int64_t result_index = 0;
  common::ObObj obj;
  uint64_t table_id = query_columns.get_table_id();
  int64_t not_null_col_num = query_columns.get_not_null_column_count();
  const ObRowDesc * row_desc = value.get_row_desc();
  bool has_delete_row = false;
  int step = 0;

  while(OB_SUCCESS == ret && filled < not_null_col_num)
  {
    if (OB_SUCCESS != (ret = ObCompactCellIterator::parse(buf_reader_, obj, &column_id_)))
    {
      TBSYS_LOG(WARN, "row get next cell error: value_index=[%ld], ret=[%d]", value_index, ret);
    }
    else
    {
      if (ObExtendType == obj.get_type() &&
          ObActionFlag::OP_END_ROW == obj.get_ext())
      {
        if(1 == ++step)
          break;
        else
          continue;
      }
      else
      {
        if(value_index == 0 && OB_ACTION_FLAG_COLUMN_ID == column_id_)
        {
          has_delete_row = true;
        }
        else if(OB_INVALID_INDEX == (result_index = row_desc->get_idx(table_id, column_id_)))
        {

        }
        else if(OB_SUCCESS != (ret = value.raw_set_cell(result_index, obj)))
        {

        }
        else
        {
          filled++;
        }
      }
    }
    ++value_index;
  }

  // set sparse row action flag column;
  if (OB_SUCCESS == ret)
  {
    // normal row
    obj.set_ext(ObActionFlag::OP_VALID);
    // delete row
    if (has_delete_row)
    {
      // delete row without any columns;
      obj.set_ext(ObActionFlag::OP_DEL_ROW);
      // delete row with new columns;
      if (value_index > 1)
      {
        obj.set_ext(ObActionFlag::OP_NEW_ADD);
      }
    }
    value.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, obj);
  }

  return ret;


}



int ObCompactCellIterator::get_row(common::ObRow& value, const sstable::ObSimpleColumnIndexes& query_columns)
{
  int ret = OB_SUCCESS;
  int64_t filled = 0;

  if(OB_SUCCESS != (ret = fill_row(query_columns, value, filled)))
  {
    TBSYS_LOG(WARN, "fill row error: ret=[%d]", ret);
  }
  else if(DENSE_SPARSE == store_type_ && OB_SUCCESS != (ret = get_sparse_rowvalue(filled, query_columns, value)))
  {
    TBSYS_LOG(WARN, "get sparse rowvalue error: ret=[%d]", ret);
  }

  return ret;
}

