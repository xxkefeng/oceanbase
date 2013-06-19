#include "ob_sstable_block_scanner.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    ObSSTableBlockScanner::ObSSTableBlockScanner()
      : is_reverse_scan_(false),
        row_cursor_(NULL),
        row_start_index_(NULL),
        row_last_index_(NULL)
    {
      //blokc_reader_(construct)
      //row_(construct)
    }

    ObSSTableBlockScanner::~ObSSTableBlockScanner()
    {
    }

    int ObSSTableBlockScanner::set_scan_param(
        const ObNewRange& range,
        const bool is_reverse_scan,
        const ObSSTableBlockReader::BlockData& block_data,
        const ObCompactStoreType& row_store_type,
        bool& need_looking_forward)
    {
      int ret = OB_SUCCESS;
      need_looking_forward = true;
      block_reader_.reset();
      ObSSTableBlockReader::const_iterator start_iterator = block_reader_.end();
      ObSSTableBlockReader::const_iterator end_iterator = block_reader_.end();
      is_reverse_scan_ = is_reverse_scan;

      if (!block_data.is_valid())
      {
        TBSYS_LOG(WARN, "invalid block data: block_internal_buf_=[%p], internal_buf_size_=[%ld], data_buf_=[%p], data_buf_size_=[%ld]",
            block_data.internal_buf_, block_data.internal_buf_size_, block_data.data_buf_, block_data.data_buf_size_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = block_reader_.init(block_data, row_store_type)))
      {
        TBSYS_LOG(WARN, "block reader init error: ret=[%d], block_internal_buf_=[%p],"
            "internal_buf_size_=[%ld], data_buf_=[%p], data_buf_size=[%ld], row_store_type=[%d]",
            ret, block_data.internal_buf_, block_data.internal_buf_size_, block_data.data_buf_,
            block_data.data_buf_size_, row_store_type);
      }
      else if (OB_SUCCESS != (ret = locate_start_pos(range, start_iterator, need_looking_forward)))
      {
        TBSYS_LOG(DEBUG, "locate start pos error: ret=[%d], range=[%s], offset=[%d], size=[%d], need_looking_forward=[%d]",
            ret, to_cstring(range), start_iterator->offset_, start_iterator->size_, need_looking_forward);
      }
      else if (OB_SUCCESS != (ret = locate_end_pos(range, end_iterator, need_looking_forward)))
      {
        TBSYS_LOG(DEBUG, "locate end pos error: ret=[%d], range=[%s], offset=[%d], size=[%d], need_looking_forward=[%d]",
            ret, to_cstring(range), end_iterator->offset_, end_iterator->size_, need_looking_forward);
      }
      else if (start_iterator > end_iterator)
      {
        ret = OB_BEYOND_THE_RANGE;
        need_looking_forward = false;
      }
      else
      {
        row_start_index_ = start_iterator;
        row_last_index_ = end_iterator;
        if (!is_reverse_scan_)
        {
          row_cursor_ = row_start_index_;
        }
        else
        {
          row_cursor_ = row_last_index_;
        }
      }

      return ret;
    }

    int ObSSTableBlockScanner::locate_start_pos(
        const common::ObNewRange& range, 
        ObSSTableBlockReader::const_iterator& start_iterator,
        bool& need_looking_forward)
    {
      int ret = OB_SUCCESS;
      start_iterator = block_reader_.end();
      ObRowkey query_start_key = range.start_key_;
      ObRowkey find_start_key;

      if (range.start_key_.is_min_row())
      {
        start_iterator = block_reader_.begin();
      }
      else
      {
        start_iterator = block_reader_.lower_bound(query_start_key);
        if (start_iterator == block_reader_.end())
        {
          ret = OB_BEYOND_THE_RANGE;
          if (is_reverse_scan_)
          {
            need_looking_forward = false;
          }
        }
        else if (OB_SUCCESS != (ret = block_reader_.get_row_key(start_iterator, find_start_key)))
        {
          TBSYS_LOG(WARN, "block reader get row key error: ret=[%d], offset_=[%d], size_=[%d]",
              ret, start_iterator->offset_, start_iterator->size_);
        }
        else
        {
          if (0 == find_start_key.compare(query_start_key) && (!range.border_flag_.inclusive_start()))
          {
            ++ start_iterator;
          }

          if (start_iterator == block_reader_.end())
          {
            ret = OB_BEYOND_THE_RANGE;
          }

          if (is_reverse_scan_)
          {
            if (start_iterator == block_reader_.begin())
            {
              need_looking_forward = true;
            }
            else
            {
              need_looking_forward = false;
            }
          }
          else
          {
            need_looking_forward = true;
          }
        }
      }

      return ret;
    }

    int ObSSTableBlockScanner::locate_end_pos(
        const common::ObNewRange& range, 
        ObSSTableBlockReader::const_iterator& last_iterator,
        bool& need_looking_forward)
    {
      int ret = OB_SUCCESS;
      ObRowkey find_end_key;
      last_iterator = block_reader_.end();
      ObRowkey query_end_key = range.end_key_;

      if (range.end_key_.is_max_row())
      {
        last_iterator = block_reader_.end();
        -- last_iterator;
        if (last_iterator < block_reader_.begin())
        {
          ret = OB_BEYOND_THE_RANGE;
        }
      }
      else
      {
        last_iterator = block_reader_.lower_bound(query_end_key);
        if (last_iterator == block_reader_.end())
        {
          -- last_iterator;
        }
        else
        {
          if (!is_reverse_scan_)
          {
            need_looking_forward = false;
          }
        }

        if (OB_SUCCESS != (ret = block_reader_.get_row_key(last_iterator, find_end_key)))
        {
          TBSYS_LOG(WARN, "block reader get row key error: ret=[%d], offset_=[%d], size_=[%d]",
              ret, last_iterator->offset_, last_iterator->size_);
        }
        else
        {
          if (last_iterator == block_reader_.begin())
          {
            if (query_end_key.compare(find_end_key) < 0)
            {
              ret = OB_BEYOND_THE_RANGE;
            }

            if (0 == find_end_key.compare(query_end_key) && (!range.border_flag_.inclusive_end()))
            {
              ret = OB_BEYOND_THE_RANGE;
            }
          }
          else
          {
            if (query_end_key.compare(find_end_key) < 0)
            {
              -- last_iterator;
            }

            if (0 == query_end_key.compare(find_end_key))
            {
              if (!range.border_flag_.inclusive_end())
              {
                -- last_iterator;
              }
            }

            if (last_iterator < block_reader_.begin())
            {
              ret = OB_BEYOND_THE_RANGE;
            }
          }
        }
      }
      
      return ret;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase
