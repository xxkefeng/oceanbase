#ifndef OCEANBASE_CHUNKSERVER_OB_ROW_ITERATOR_H_
#define OCEANBASE_CHUNKSERVER_OB_ROW_ITERATOR_H_

#include "common/ob_define.h"
#include "common/ob_row.h"
#include "sql/ob_phy_operator.h"

namespace oceanbase 
{
  namespace chunkserver
  {
    class ObResultRow 
    {
      public:
        ObResultRow(common::ObRow* row, int status)
        : result_(row), 
          status_(status)
        {

        }

        ObResultRow()
        : result_(NULL), 
          status_(common::OB_SUCCESS)
        {

        }

        ~ObResultRow() { } 

        inline bool is_success() const 
        {
          return status_ == common::OB_SUCCESS;
        }

        inline bool is_iter_end() const 
        {
          return status_ == common::OB_ITER_END;
        }

        inline bool is_failure() const 
        {
          return !is_iter_end() && !is_success();
        }

        inline bool is_done() const
        {
          return is_iter_end() || !is_success();
        }

        // Obtains the row. Can be called only if there was no error.
        inline const common::ObRow& row() const { return *result_; }

        inline int status() const { return status_; }

      private:
        friend class ObRowCursor;
        const common::ObRow* result_;
        int status_;
    };

    class ObRowCursor
    {
      public:
        explicit ObRowCursor(sql::ObPhyOperator* phy_operator)
        : operator_(phy_operator)
        {

        }

        ObRowCursor(sql::ObPhyOperator* phy_operator, ObResultRow& status)
        : operator_(phy_operator),
          row_cursor_status_(status)
        {

        }

        ~ObRowCursor() { }

        inline void next()
        {
          row_cursor_status_.status_ = operator_->get_next_row(row_cursor_status_.result_);
        }

        inline int close()
        {
          return operator_->close();
        }

        inline int get_row_desc(const common::ObRowDesc*& row_desc) const
        {
          return operator_->get_row_desc(row_desc);
        }

        inline const ObResultRow& get_status() const { return row_cursor_status_; }

        inline sql::ObPhyOperator* get_phyoperator() { return operator_; }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObRowCursor);

        sql::ObPhyOperator* operator_;
        ObResultRow row_cursor_status_;
    };
  } /* chunkserver */
} /* oceanbase */

#endif
