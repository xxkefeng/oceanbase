#ifndef OCEANBASE_SSTABLE_BLOCK_BUILDER_H_
#define OCEANBASE_SSTABLE_BLOCK_BUILDER_H_

#include <tbsys.h>
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_compact_cell_writer.h"
#include "common/ob_row.h"
#include "common/ob_rowkey.h"
#include "ob_sstable_store_struct.h"
#include "common/ob_compact_store_type.h"
#include "common/ob_crc64.h"

class TestSSTableBlockBuilder_construction_Test;
class TestSSTableBlockBuilder_add_row1_Test;
class TestSSTableBlockBuilder_add_row2_Test;
class TestSSTableBlockBuilder_build_block1_Test;
class TestSSTableBlockBuilder_reset1_Test;

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableBlock
    {
    public:
      friend class ::TestSSTableBlockBuilder_construction_Test;
      friend class ::TestSSTableBlockBuilder_add_row1_Test;
      friend class ::TestSSTableBlockBuilder_add_row2_Test;
      friend class ::TestSSTableBlockBuilder_build_block1_Test;
      friend class ::TestSSTableBlockBuilder_reset1_Test;

    public:
      static const int64_t BLOCK_ROW_BUFFER_SIZE = 2 * 1024 * 1024;
      static const int64_t BLOCK_ROW_INDEX_BUFFER_SIZE = 64 * 1024;
      static const int64_t BLOCK_ROW_INDEX_SIZE = sizeof(ObSSTableBlockRowIndex);
      static const int64_t SSTABLE_BLOCK_HEADER_SIZE = sizeof(ObSSTableBlockHeader);
      static const int64_t SSTABLE_BLOCK_BUF_ALIGN_SIZE = 1024;
      
    public:
      ObSSTableBlock(common::ObCompactStoreType row_store_type = common::INVALID_COMPACT_STORE_TYPE)
        : row_store_type_(row_store_type),
          row_buf_(NULL), 
          row_length_(0),
          row_buf_size_(0),
          row_index_buf_(NULL), 
          row_index_length_(0),
          row_index_buf_size_(0)
      {
        //block_header_(construct)
      }

      ~ObSSTableBlock()
      {
        clear();
      }

      int init();

      void reset();

      void clear();
      
      /**
       * this function is not already use
       */
      int add_row(const common::ObRowkey& row_key, const common::ObRow& row_value, uint64_t& row_checksum);

      int add_row(const common::ObRow& row, uint64_t& row_checksum);

      int32_t get_row_count() const;

      int build_block(char*& buf, int64_t& size);

      int64_t get_block_size() const;

      int set_row_store_type(const common::ObCompactStoreType row_store_type);
      
    private:
      int add_row_index(const ObSSTableBlockRowIndex& row_index);

      char* get_cur_row_ptr();

      char* get_cur_row_index_ptr();

      int64_t get_row_remain() const;

      int64_t get_row_index_remain() const;

      int alloc_mem(char*& buf, const int64_t size);

      void free_mem(void* buf);

      /**
       * if the row_buf_ or row_index_buf_ is not enough, double the mem
       * @param flag: 0(row buf), 1(row index buf)
       */
      int alloc_double_mem(const int32_t flag);

    private:     
      ObSSTableBlockHeader block_header_;
      common::ObCompactStoreType row_store_type_;

      char* row_buf_;
      int64_t row_length_;
      int64_t row_buf_size_;

      char* row_index_buf_;
      int64_t row_index_length_;
      int64_t row_index_buf_size_;
    };

    inline void ObSSTableBlock::reset()
    {
      block_header_.reset();
      //row_store_type_  (not reset)
      //row_buf_ (not reset)
      row_length_ = SSTABLE_BLOCK_HEADER_SIZE;
      //row_buf_size_ (not reset)
      //row_index_buf_ (not reset)
      row_index_length_ = 0;
      //row_index_buf_size_ (not reset)
    }

    inline void ObSSTableBlock::clear()
    {
      if (NULL != row_buf_)
      {
        free_mem(row_buf_);
      }

      if (NULL != row_index_buf_)
      {
        free_mem(row_index_buf_);
      }
    }

    inline int32_t ObSSTableBlock::get_row_count() const
    {
      return block_header_.row_count_;
    }

    inline int64_t ObSSTableBlock::get_block_size() const
    {
      //计算大小时，row索引数要加1
      return (row_length_ + row_index_length_ + BLOCK_ROW_INDEX_SIZE);
    }

    inline int ObSSTableBlock::set_row_store_type(const common::ObCompactStoreType row_store_type)
    {
      int ret = common::OB_SUCCESS;

      if (common::DENSE_DENSE != row_store_type && common::DENSE_SPARSE != row_store_type)
      {
        TBSYS_LOG(WARN, "invalid row store type: row_store_type_=[%d]", row_store_type_);
        ret = common::OB_INVALID_ARGUMENT;
      }
      else
      {
        row_store_type_ = row_store_type;
      }

      return ret;
    }

    inline char* ObSSTableBlock::get_cur_row_ptr()
    {
      return row_buf_ + row_length_;
    }

    inline char* ObSSTableBlock::get_cur_row_index_ptr()
    {
      return row_index_buf_ + row_index_length_;
    }

    inline int64_t ObSSTableBlock::get_row_remain() const
    {
      return (row_buf_size_ - row_length_);
    }

    inline int64_t ObSSTableBlock::get_row_index_remain() const
    {
      return (row_index_buf_size_ - row_index_length_);
    }

    inline void ObSSTableBlock::free_mem(void* buf)
    {
      common::ob_free(buf);
      buf = NULL;
    }

  }//end namespace compactsstablev2
}//end namespace oceanbase
#endif

