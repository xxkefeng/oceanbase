#include "ob_sstable_block.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace compactsstablev2
  {
    int ObSSTableBlock::init()
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = alloc_mem(row_buf_, BLOCK_ROW_BUFFER_SIZE)))
      {
        TBSYS_LOG(ERROR, "failed to alloc memory: ret=[%d]", ret);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = alloc_mem(row_index_buf_, BLOCK_ROW_INDEX_BUFFER_SIZE)))
      {
        TBSYS_LOG(ERROR, "faild to alloc memory: ret=[%d]", ret);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        //row buf contain the block header
        row_length_ = SSTABLE_BLOCK_HEADER_SIZE;
        row_buf_size_ = BLOCK_ROW_BUFFER_SIZE;

        //row index buf
        row_index_length_ = 0;
        row_index_buf_size_ = BLOCK_ROW_INDEX_BUFFER_SIZE;
      }

      return ret;
    }

    int ObSSTableBlock::add_row(const ObRowkey& row_key, const ObRow& row_value, uint64_t& row_checksum)
    {
      UNUSED(row_key);
      UNUSED(row_value);
      UNUSED(row_checksum);
      return OB_SUCCESS;
    }

    int ObSSTableBlock::add_row(const common::ObRow& row, uint64_t& row_checksum)
    {
      int ret = OB_SUCCESS;
      char* cur_row_ptr = NULL;
      int64_t cur_row_offset = 0;
      int64_t row_remain_size = 0;
      ObCompactCellWriter row_writer;

      //append row
      while(true)
      {
        cur_row_ptr = get_cur_row_ptr();
        row_remain_size = get_row_remain();

        if (NULL == cur_row_ptr)
        {
          TBSYS_LOG(WARN, "cur_row_ptr==NULL");
          ret = OB_ERROR;
          break;
        }
        else if (row_remain_size <= 0)
        {
          if (OB_SUCCESS != (ret = alloc_double_mem(0)))
          {
            TBSYS_LOG(WARN, "alloc double mem error: ret=[%d]", ret);
            break;
          }
        }
        else if (OB_SUCCESS != (ret = row_writer.init(cur_row_ptr, row_remain_size, row_store_type_)))
        {
          TBSYS_LOG(WARN, "row writer init error: ret=[%d], row_remain_size=[%ld], row_store_type_=[%d]", 
              ret, row_remain_size, row_store_type_);
          break;
        }
        else if (OB_SUCCESS != (ret = row_writer.append_row(row)))
        {
          if (OB_BUF_NOT_ENOUGH == ret)
          {
            if (OB_SUCCESS != (ret = alloc_double_mem(0)))
            {
              TBSYS_LOG(WARN, "alloc double mem error: ret=[%d]", ret);
              break;
            }
          }
          else
          {
            TBSYS_LOG(WARN, "append row error: ret=[%d], row=[%s]", ret, to_cstring(row));
            break;
          }
        }
        else
        {
          row_checksum = ob_crc64_sse42(row_checksum, cur_row_ptr, row_writer.size());
          cur_row_offset = row_length_;
          row_length_ += row_writer.size();
          break;
        }
      }

      //update row index
      if (OB_SUCCESS == ret)
      {
        ObSSTableBlockRowIndex index;
        index.row_offset_ = static_cast<int32_t>(cur_row_offset);
        if (OB_SUCCESS != (ret = add_row_index(index)))
        {
          TBSYS_LOG(WARN, "add row index error: ret=[%d], index.row_offset_=[%d]", ret, index.row_offset_);
        }
      }

      //update block header
      if (OB_SUCCESS == ret)
      {
        block_header_.row_count_ ++;
      }

      return ret;
    }

    int ObSSTableBlock::build_block(char*& buf, int64_t& size)
    {
      int ret = OB_SUCCESS;
      ObSSTableBlockRowIndex index;

      //the last row index
      index.row_offset_ = static_cast<int32_t>(row_length_);
      block_header_.row_index_offset_  = static_cast<int32_t>(row_length_);

      if (OB_SUCCESS != (ret = add_row_index(index)))
      {
        TBSYS_LOG(WARN, "add row index error: ret=[%d], index.row_offset_=[%d]", ret, index.row_offset_);
      }
      else
      {
        //block header--->buf
        memcpy(row_buf_, &block_header_, SSTABLE_BLOCK_HEADER_SIZE);

        char* buf_ptr = NULL;
        while (true)
        {
          if (row_buf_size_ - row_length_ < row_index_length_)
          {
            if (OB_SUCCESS != (ret = alloc_double_mem(0)))
            {
              TBSYS_LOG(WARN, "alloc double mem error: ret=[%d]", ret);
              break;
            }
          }
          else
          {
            buf_ptr = get_cur_row_ptr();
            memcpy(buf_ptr, row_index_buf_, row_index_length_);
            row_length_ += row_index_length_;
            break;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        buf = row_buf_;
        size = row_length_;
      }

      return ret;
    }

    int ObSSTableBlock::add_row_index(const ObSSTableBlockRowIndex& row_index)
    {
      int ret = OB_SUCCESS;
      char* buf_ptr = NULL;

      while(true)
      {
        if (row_index_buf_size_ - row_index_length_ < BLOCK_ROW_INDEX_SIZE)
        {
          if (OB_SUCCESS != (ret = alloc_double_mem(1)))
          {
            TBSYS_LOG(WARN, "alloc double mem error: ret=[%d]", ret);
            break;
          }
        }
        else
        {
          buf_ptr = get_cur_row_index_ptr();
          memcpy(buf_ptr, &row_index, BLOCK_ROW_INDEX_SIZE);
          row_index_length_ += BLOCK_ROW_INDEX_SIZE;
          break;
        }
      }

      return ret;
    }

    int ObSSTableBlock::alloc_mem(char*& buf, const int64_t size)
    {
      int ret = OB_SUCCESS;

      if (size <= 0)
      {
        TBSYS_LOG(WARN, "invalid argument:size=%ld", size);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (buf = reinterpret_cast<char*>(ob_malloc(size, ObModIds::OB_SSTABLE_WRITER))))
      {
        TBSYS_LOG(ERROR, "failed to alloc memory: size=[%ld]", size);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      return ret;
    }

    int ObSSTableBlock::alloc_double_mem(const int32_t flag)
    {
      int ret = OB_SUCCESS;
      char* new_buf = NULL;
      TBSYS_LOG(WARN, "in alloc double meme");

      if (0 == flag)
      {
        if (OB_SUCCESS != (ret = alloc_mem(new_buf, row_buf_size_ * 2)))
        {
          TBSYS_LOG(WARN, "alloc mem error: ret=[%d], row_buf_size_=[%ld]", ret, row_buf_size_);
        }
        else
        {
          memcpy(new_buf, row_buf_, row_length_);
          free_mem(row_buf_);
          row_buf_ = new_buf;
          row_buf_size_ = row_buf_size_ * 2;
        }
      }
      else if (1 == flag)
      {
        if (OB_SUCCESS != (ret = alloc_mem(new_buf, row_index_buf_size_ * 2)))
        {
          TBSYS_LOG(WARN, "allow mem error: ret=[%d], row_index_buf_size_=[%ld]", ret, row_index_buf_size_);
        }
        else
        {
          memcpy(new_buf, row_index_buf_, row_index_length_);
          free_mem(row_index_buf_);
          row_index_buf_ = new_buf;
          row_index_buf_size_ = row_index_buf_size_ * 2;
        }
      }
      else
      {
        TBSYS_LOG(WARN, "invalid flag: flag=[%d]", flag);
      }

      return ret;
    }
  }//end namespace compactsstablev2
}//end namespace oceanbase

