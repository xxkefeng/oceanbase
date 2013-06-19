#ifndef OCEANBASE_COMPACTSSSTABLV2_OB_SSTABLE_BLOCK_SCANNER_H_
#define OCEANBASE_COMPACTSSTABLEV2_OB_SSTABLE_BLOCK_SCANNER_H_

#include "common/ob_define.h"
#include "common/ob_range2.h"
#include "common/ob_compact_cell_iterator.h"
#include "common/utility.h"
#include "ob_sstable_block_reader.h"

namespace oceanbase
{
  namespace compactsstablev2
  {
    class ObSSTableBlockScanner
    {
    public:
      ObSSTableBlockScanner();
      ~ObSSTableBlockScanner();

      int set_scan_param(const common::ObNewRange& range,
          const bool is_reverse_scan,
          const ObSSTableBlockReader::BlockData& block_data,
          const common::ObCompactStoreType& row_store_type,
          bool& need_looking_forward);

      int get_next_row(common::ObCompactCellIterator*& row);

    private:
      int locate_start_pos(const common::ObNewRange& range, 
          ObSSTableBlockReader::const_iterator& start_iterator,
          bool& need_looking_forward);

      int locate_end_pos(const common::ObNewRange& range, 
          ObSSTableBlockReader::const_iterator& last_iterator,
          bool& need_looking_forward);

      int load_current_row(ObSSTableBlockReader::const_iterator row_index);

      int end_of_block();

      void next_row();

    private:
      bool is_reverse_scan_;
      ObSSTableBlockReader block_reader_;

      ObSSTableBlockReader::const_iterator row_cursor_;
      ObSSTableBlockReader::const_iterator row_start_index_;
      ObSSTableBlockReader::const_iterator row_last_index_;

      common::ObCompactCellIterator row_;
    };

    inline int ObSSTableBlockScanner::load_current_row(ObSSTableBlockReader::const_iterator row_index)
    {
      int ret = common::OB_SUCCESS;

      if (row_index < row_start_index_ || row_index > row_last_index_)
      {
        TBSYS_LOG(WARN, "invalid row index: row_index=[%p], row_start_index_=[%p], row_last_index_=[%p]",
            row_index, row_start_index_, row_last_index_);
        ret = common::OB_ERROR;
      }
      else if (common::OB_SUCCESS != (ret = block_reader_.get_row(row_index, row_)))
      {
        TBSYS_LOG(WARN, "block reader get row error: ret=[%d], row_index->offset_=[%d], row_index->size_=[%d]",
            ret, row_index->offset_, row_index->size_);
        ret = common::OB_ERROR;
      }

      return ret;
    }

    inline int ObSSTableBlockScanner::end_of_block()
    {
      bool ret = false;
      if ((!is_reverse_scan_) && row_cursor_ > row_last_index_)
      {
        ret = true;
      }
      else if (is_reverse_scan_ && row_cursor_ < row_start_index_)
      {
        ret = true;
      }
      return ret;
    }

    inline void ObSSTableBlockScanner::next_row()
    {
      if (!is_reverse_scan_)
      {
        row_cursor_ ++;
      }
      else
      {
        row_cursor_ --;
      }
    }

    inline int ObSSTableBlockScanner::get_next_row(common::ObCompactCellIterator*& row)
    {
      int ret = common::OB_SUCCESS;

      if (end_of_block())
      {
        ret = common::OB_BEYOND_THE_RANGE;
      }
      else if (common::OB_SUCCESS != (ret = load_current_row(row_cursor_)))
      {
        TBSYS_LOG(WARN, "load current row error: ret=[%d], offset_=[%d], size_=[%d]", ret, row_cursor_->offset_, row_cursor_->size_);
      }
      else
      {
        row = &row_;
        next_row();
      }

      return ret;
    }
  }//end namesapce compactsstablev2
}//end namespace oceanbase
#endif
