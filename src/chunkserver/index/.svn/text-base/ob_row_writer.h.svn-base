/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 *
 * Authors:
 *   huating <huating.zmq@alipay.com>
 *
 */
#ifndef OCEANBASE_CHUNKSERVER_ROW_WRITER_H_
#define OCEANBASE_CHUNKSERVER_ROW_WRITER_H_

#include <tbsys.h>
#include "ob_row_cursor.h"

namespace oceanbase
{
  namespace chunkserver
  {
    // An abstraction of a 'row sink' that can be written to. Implementations
    // may write data to memtables, sstables, etc.
    class ObRowSink 
    {
     public:
      virtual ~ObRowSink() {}

      virtual int append(const common::ObRow& row) = 0;

     protected:
      ObRowSink() {}

     private:
      DISALLOW_COPY_AND_ASSIGN(ObRowSink);
    };

    class ObRowWriter
    {
      public:
        ObRowWriter(ObRowCursor& row_cursor, ObRowSink& row_sink) 
        : row_cursor_(row_cursor), 
          row_sink_(row_sink)
        {
        }

        ~ObRowWriter() { }

        int write_all()
        {
          int ret = row_cursor_.get_status().status();
          int err = common::OB_SUCCESS;

          while (common::OB_SUCCESS == ret)
          {
            row_cursor_.next();
            if (row_cursor_.get_status().is_success())
            {
              ret = row_sink_.append(row_cursor_.get_status().row());
            }
            else if (row_cursor_.get_status().is_iter_end())
            {
              ret = common::OB_SUCCESS;
              break;
            }
            else
            {
              ret = row_cursor_.get_status().status();
              break;
            }
          }

          err = row_cursor_.close();
          if (OB_SUCCESS != err && OB_SUCCESS == ret)
          {
            ret = err;
          }

          return ret;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObRowWriter);
        ObRowCursor& row_cursor_;
        ObRowSink& row_sink_;
    };

    inline int write_row_cursor(ObRowCursor& cursor, ObRowSink& sink)
    {
      ObRowWriter writer(cursor, sink);
      return writer.write_all();
    }
  } /* chunkserver */
} /* oceanbase */

#endif // OCEANBASE_CHUNKSERVER_ROW_WRITER_H_
