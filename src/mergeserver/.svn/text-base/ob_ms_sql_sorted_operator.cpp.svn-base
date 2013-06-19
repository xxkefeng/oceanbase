/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 * ob_merger_sorted_operator.cpp for 
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *
 */
//#include "ob_merger_sorted_operator.h"
#include "ob_ms_sql_sorted_operator.h"
#include <algorithm>
#include "sql/ob_sql_scan_param.h"
#include "common/ob_new_scanner.h"
#include "common/ob_range2.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::mergeserver;
using namespace std;

void ObMsSqlSortedOperator::sharding_result_t::init(ObNewScanner & sharding_res, const ObNewRange & query_range, 
  ObRowkey & last_process_rowkey, const int64_t fullfilled_item_num)
{
  sharding_res_ = &sharding_res;
  sharding_query_range_ = &query_range;
  last_row_key_ = last_process_rowkey;
  fullfilled_item_num_ = fullfilled_item_num;
}

bool ObMsSqlSortedOperator::sharding_result_t::operator <(const sharding_result_t &other) const
{
  bool res = false;
  res = (sharding_query_range_->compare_with_startkey2(*other.sharding_query_range_) < 0);
  return res;
}

ObMsSqlSortedOperator::ObMsSqlSortedOperator() : 
  sharding_result_count_(0),
  seamless_result_count_(0),
  cur_sharding_result_idx_(0),
  user_scan_range_(),
  total_mem_size_used_(0)
{
}


ObMsSqlSortedOperator::~ObMsSqlSortedOperator()
{
}

void ObMsSqlSortedOperator::reset()
{
  sharding_result_count_ = 0;
  cur_sharding_result_idx_ = 0;
  seamless_result_count_  = 0;
  total_mem_size_used_ = 0;
}

int ObMsSqlSortedOperator::set_param(const ObNewRange &user_scan_range)
{
  reset();
  user_scan_range_ = user_scan_range;
  return OB_SUCCESS;
}

// note: keep in mind that "readed" scanners' rowkey buffer were freed already
void ObMsSqlSortedOperator::sort(bool &is_finish, ObNewScanner * last_sharding_res)
{
  /*
   * 1. sort unread scanners
   * 2. check if first sorted unread scanner's start key equals to last seamed end key
   * 3. if seamed, check if finish
   * 4. update sort params
   * 
   * sharding_result_count_: total scanner received
   * cur_sharding_result_idx_: current readable scanner index
   * seamless_result_count_: seamed scanners
   * 
   */
  int64_t seamless_result_idx = 0;
  // sort 'clean' scanners
  std::sort(sharding_result_arr_ + cur_sharding_result_idx_, sharding_result_arr_ + sharding_result_count_);
  if (seamless_result_count_ <= 0)
  {
    // no seamless result
    if ((sharding_result_arr_[0].sharding_query_range_->start_key_  == user_scan_range_.start_key_)
        || (sharding_result_arr_[0].sharding_query_range_->start_key_.is_min_row()))
    {
      seamless_result_count_ = 1;
    }
  }
  
  if (seamless_result_count_ > 0)
  {
    // at lease one seamless result
    for (seamless_result_idx = seamless_result_count_; 
        seamless_result_idx < sharding_result_count_; 
        seamless_result_idx++)
    {
      if (sharding_result_arr_[seamless_result_idx - 1].last_row_key_
          == sharding_result_arr_[seamless_result_idx].sharding_query_range_->start_key_)
      {
        seamless_result_count_ = seamless_result_idx + 1;
        TBSYS_LOG(DEBUG, "prev last_row_key=%s, cur star_key_=%s, seamless=%ld", 
            to_cstring(sharding_result_arr_[seamless_result_idx - 1].last_row_key_),
            to_cstring(sharding_result_arr_[seamless_result_idx].sharding_query_range_->start_key_),
            seamless_result_count_
            );
      }
      else
      {
        break;
      }
    }
  }

  if (seamless_result_count_ > 0) // implicates that startkey matched already
  {
    OB_ASSERT(NULL != last_sharding_res);
    TBSYS_LOG(DEBUG, "last seamless=%s", to_cstring(sharding_result_arr_[seamless_result_count_-1].last_row_key_));
    if (sharding_result_arr_[seamless_result_count_-1].last_row_key_ >= user_scan_range_.end_key_ ||
        sharding_result_arr_[seamless_result_count_-1].last_row_key_.is_max_row())
    {
      /* check last seemless result. 
       * Finish the whole scan if last scanner fullfilled and its end_key_ equals to scan_range's end_key_
       */
      TBSYS_LOG(DEBUG, "biubiu. seamless_result_count_=%ld, start_key=%s, end_key=%s",
          seamless_result_count_, 
          to_cstring(sharding_result_arr_[seamless_result_count_-1].sharding_query_range_->start_key_),
          to_cstring(sharding_result_arr_[seamless_result_count_-1].sharding_query_range_->end_key_));
      is_finish = true;
    }
    else 
    {
      is_finish = false;
    }
  }
}


int ObMsSqlSortedOperator::add_sharding_result(ObNewScanner & sharding_res, const ObNewRange &query_range, 
  bool &is_finish, ObStringBuf &rowkey_buffer)
{
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;
  ObRowkey last_row_key;
  ObRowkey stored_rowkey;
  ObNewRange cs_tablet_range;
  int err = OB_SUCCESS;

  if (sharding_result_count_ >= MAX_SHARDING_RESULT_COUNT)
  {
    TBSYS_LOG(WARN,"array is full [MAX_SHARDING_RESULT_COUNT:%ld,sharding_result_count_:%ld]", 
      MAX_SHARDING_RESULT_COUNT, sharding_result_count_);
    err = OB_ARRAY_OUT_OF_RANGE;
  }
  else if (OB_SUCCESS != (err = sharding_res.get_is_req_fullfilled(is_fullfilled,fullfilled_item_num)))
  {
    TBSYS_LOG(WARN,"fail to get fullfilled info from sharding result [err:%d]", err);
  }

  // prepare last row key
  if (OB_SUCCESS == err)
  {
    if (true == is_fullfilled)
    {
      // last_row_key has to be modified
      if (OB_SUCCESS != (err = sharding_res.get_range(cs_tablet_range)))
      {
        TBSYS_LOG(WARN,"fail to get tablet range from sharding result [err:%d]", err);
      }
      else
      {
        last_row_key = cs_tablet_range.end_key_;

        if (last_row_key.is_max_row())
        {
          TBSYS_LOG(DEBUG, "got max");
        }
        else
        {
          TBSYS_LOG(DEBUG, "got normal: last=%s. endkey=%s", to_cstring(last_row_key), to_cstring(cs_tablet_range.end_key_));
        }
      }
    }
    else if (OB_SUCCESS != (err = sharding_res.get_last_row_key(last_row_key)))
    {
      if (OB_ENTRY_NOT_EXIST == err)
      {
        TBSYS_LOG(WARN, "last row key not exist");
        err = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN,"fail to get last rowkey from sharding result [err:%d]", err);
      }
    }
  }

  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = rowkey_buffer.write_string(last_row_key, &stored_rowkey)))
    {
      TBSYS_LOG(WARN, "fail to store rowkey to buffer. %s", to_cstring(cs_tablet_range.end_key_));
    }
    else
    {
      fullfilled_item_num = sharding_res.get_row_num();
      total_mem_size_used_ += sharding_res.get_used_mem_size();
      sharding_result_arr_[sharding_result_count_].init(sharding_res, query_range, stored_rowkey, fullfilled_item_num);
      sharding_result_count_ ++;
    }
  }
  
  is_finish = false;
  if (OB_SUCCESS == err)
  {
    sort(is_finish, &sharding_res);
  }
  TBSYS_LOG(DEBUG, "add sharding result. sharding_result_count_=%ld, is_finish=%d, err=%d", sharding_result_count_, is_finish, err);
  
  if (1)
  {
    static __thread int64_t total_row_num = 0;
    total_row_num += fullfilled_item_num;
    TBSYS_LOG(DEBUG, "last_rowkey=%s,query_range=%s,cs_tablet_range=%s,is_fullfilled=%d,fullfilled_item_num=%ld,total=%ld, is_finish=%d",
        to_cstring(last_row_key),to_cstring(query_range),to_cstring(cs_tablet_range),
        is_fullfilled,fullfilled_item_num,total_row_num,is_finish);
  }
  return err;
}


///////////////////////////////////////
//////////// Row inferface ////////////
///////////////////////////////////////

int ObMsSqlSortedOperator::get_next_row(oceanbase::common::ObRow &row)
{
  int err = OB_SUCCESS;
  while (OB_SUCCESS == err)
  {
    if (cur_sharding_result_idx_ >= seamless_result_count_)
    {
      err = OB_ITER_END;
    }
    else if (OB_SUCCESS == err)
    {
      if (OB_SUCCESS ==(err = sharding_result_arr_[cur_sharding_result_idx_].sharding_res_->get_next_row(row)))
      {
        break;
      }
      else if (OB_ITER_END == err)
      {
        total_mem_size_used_ -= sharding_result_arr_[cur_sharding_result_idx_].sharding_res_->get_used_mem_size();
        // since this sharding will never be used again, release it!
        sharding_result_arr_[cur_sharding_result_idx_].sharding_res_->clear();
        cur_sharding_result_idx_ ++;
        err = OB_SUCCESS;
      }
      else
      {
        TBSYS_LOG(WARN,"fail to get next cell from ObNewScanner [idx:%ld,err:%d]", cur_sharding_result_idx_, err);
      }
    }
  }
  return err;
}

