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
#include "ob_tablet_sstable.h"
#include "ob_tablet_local_index_scan.h"
#include "ob_index_builder.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase;
    using namespace common;
    using namespace sql;

    int ObTabletLocalIndexScanner::open(
       ObTable& input_table, const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;
      input_table_ = &input_table;
      root_ = NULL;

      if (!scan_options.is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = init_input_table(scan_options)))
      {
        TBSYS_LOG(WARN, "failed to init input table, ret=%d", ret);
      }
      else if (NULL == scan_options.index_tablet_range_
               && scan_options.index_table_id_ != scan_options.data_tablet_range_->table_id_
               && OB_SUCCESS != (ret = init_projecter(scan_options)))
      {
        TBSYS_LOG(WARN, "failed to init projecter, ret=%d", ret);
      }
      else if (scan_options.need_sort_
               && OB_SUCCESS != (ret = init_sorter(scan_options)))
      {
        TBSYS_LOG(WARN, "failed to init sorter, ret=%d", ret);
      }
      else if (scan_options.row_interval_ > 0
               && OB_SUCCESS != (ret = init_sampler(scan_options)))
      {
        TBSYS_LOG(WARN, "failed to init interval sampler, ret=%d", ret);
      }
      else if (NULL == root_)
      {
        TBSYS_LOG(WARN, "after init, root phyoperator of tablet local index is NULL");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = root_->open()))
      {
        TBSYS_LOG(WARN, "failed to open tablet local index phyoperator, ret=%d", ret);
      }

      if (OB_SUCCESS != ret && NULL != root_)
      {
        root_->close();
      }

      return ret;
    }

    int ObTabletLocalIndexScanner::init_input_table(const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = build_scan_param(scan_options)))
      {
        TBSYS_LOG(WARN, "failed to build scan parameter, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = input_table_->open(scan_options)))
      {
        TBSYS_LOG(WARN, "failed to open tablet table, ret=%d", ret);
      }
      else if (NULL == (root_ = input_table_->get_phyoperator()))
      {
        TBSYS_LOG(WARN, "failed to get phyoperator from input table");
      }

      return ret;
    }

    int ObTabletLocalIndexScanner::build_scan_param(const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;
      scan_param_.reset();

      scan_param_.set_range(*scan_options.data_tablet_range_);
      scan_param_.set_is_result_cached(scan_options.is_result_cached_);
      scan_param_.set_not_exit_col_ret_nop(false);
      scan_param_.set_read_mode(ScanFlag::ASYNCREAD);
      const_cast<ObScanOptions&>(scan_options).scan_param_ = &scan_param_;

      if (OB_SUCCESS != (ret = fill_scan_columns(scan_options)))
      {
        TBSYS_LOG(WARN, "failed to fill scan columns into scan param, ret=%d", ret);
      }
      else if (NULL != scan_options.index_tablet_range_)
      {
        scan_param_.set_local_index_range(*scan_options.index_tablet_range_);
        scan_param_.set_local_index_scan(true);
      }

      return ret;
    }

    int ObTabletLocalIndexScanner::fill_scan_columns(const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* table_schema = NULL;
      const ObColumnSchemaV2* columns = NULL;
      int32_t column_count = 0;
      uint64_t table_id = (NULL == scan_options.index_tablet_range_) ?
        scan_options.data_tablet_range_->table_id_ : scan_options.index_tablet_range_->table_id_;

      /**
       * the column id in index table is the same as the data 
       * table, so add all the columns in index table into scan 
       * param to scan data from data table 
       */
      if (NULL == (table_schema = scan_options.schema_->get_table_schema(table_id)))
      {
        TBSYS_LOG(WARN, "failed to get table schema, table_id_=%lu", table_id);
        ret = OB_ERROR;
      }
      else 
      {
        const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
        const ObRowkeyColumn* rowkey_column = NULL;
        scan_options.scan_param_->set_rowkey_column_count(
          static_cast<int16_t>(rowkey_info.get_size()));

        //add rowkey columns
        for (int64_t i = 0; i < rowkey_info.get_size() && OB_SUCCESS == ret; i++)
        {
          if (NULL == (rowkey_column = rowkey_info.get_column(i)))
          {
            TBSYS_LOG(WARN, "failed to get rowkey column, table_id=%lu", table_id);
            ret = OB_ERROR;
          }
          else if(OB_SUCCESS != (ret = scan_options.scan_param_->add_column(
             rowkey_column->column_id_)))
          {
            TBSYS_LOG(WARN, "scan parameter add rowkey column failed, table_id=%lu, "
                            "column_id=%lu, ret=%d", 
                      table_id, rowkey_column->column_id_, ret);
          }
        }

        //add non-rowkey columns
        if (!scan_options.scan_only_rowkey_ && OB_SUCCESS == ret)
        {
          if (NULL == (columns = scan_options.schema_->get_table_schema(
             table_id, column_count)) || column_count <= 0)
          {
            TBSYS_LOG(WARN,"failed to get tablet schema from schema manager, "
                           "table_id_=%lu, table_column_count=%d", 
                      table_id, column_count);
            ret = OB_ERROR;
          }
          else if (column_count > rowkey_info.get_size())
          {
            for (int64_t i = 0; i < column_count && OB_SUCCESS == ret; i++)
            {
              if (rowkey_info.is_rowkey_column(columns[i].get_id()))
              {
                // ignore rowkey columns;
                continue;
              }
              else if(OB_SUCCESS != (ret = scan_options.scan_param_->add_column(
                 columns[i].get_id())))
              {
                TBSYS_LOG(WARN, "scan parameter add column fail, table_id_=%lu, "
                                "column_id_=%lu, ret=%d", 
                          table_id, columns[i].get_id(), ret);
              }
            }
          }
        }
      }

      return ret;
    }

    int ObTabletLocalIndexScanner::init_projecter(const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* index_table_schema = NULL;
      const ObColumnSchemaV2* columns = NULL;
      int32_t column_count = 0;

      projecter_.clear();
      if (NULL == (columns = scan_options.schema_->get_table_schema(
         scan_options.index_table_id_, column_count)) || column_count <= 0)
      {
        TBSYS_LOG(WARN,"failed to get tablet schema from schema manager, "
                       "index_table_id_=%lu, table_column_count=%d", 
                  scan_options.index_table_id_, column_count);
        ret = OB_ERROR;
      }
      else if (NULL == (index_table_schema = scan_options.schema_->get_table_schema(
         scan_options.index_table_id_)))
      {
        TBSYS_LOG(WARN, "failed to get index table schema, index_table_id_=%lu",
                  scan_options.index_table_id_);
        ret = OB_ERROR;
      }
      else 
      {
        uint64_t data_table_id = scan_options.data_tablet_range_->table_id_;
        const ObRowkeyInfo& rowkey_info = index_table_schema->get_rowkey_info();
        const ObRowkeyColumn* rowkey_column = NULL;
        projecter_.set_rowkey_cell_count(rowkey_info.get_size());

        //add rowkey columns
        for (int64_t i = 0; i < rowkey_info.get_size() && OB_SUCCESS == ret; i++)
        {
          if (NULL == (rowkey_column = rowkey_info.get_column(i)))
          {
            TBSYS_LOG(WARN, "failed to get rowkey column, table_id_=%lu",
                      scan_options.index_table_id_);
            ret = OB_ERROR;
          }
          else if (OB_SUCCESS != (ret = add_project_column(
             data_table_id, scan_options.index_table_id_, rowkey_column->column_id_)))
          {
            TBSYS_LOG(WARN, "failed to add one column into projecter, ret=%d", ret);
          }
        }

        if (OB_SUCCESS == ret && column_count > rowkey_info.get_size())
        {
          //add non-rowkey columns
          for (int64_t i = 0; i < column_count && OB_SUCCESS == ret; i++)
          {
            if (rowkey_info.is_rowkey_column(columns[i].get_id()))
            {
              // ignore rowkey columns;
              continue;
            }
            else if (OB_SUCCESS != (ret = add_project_column(
               data_table_id, scan_options.index_table_id_, columns[i].get_id())))
            {
              TBSYS_LOG(WARN, "failed to add one column into projecter, ret=%d", ret);
            }
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (NULL == root_)
          {
            TBSYS_LOG(WARN, "sstable scanner operator is not initialization");
            ret = OB_ERROR;
          }
          else if (OB_SUCCESS != (ret = projecter_.set_child(0, *root_)))
          {
            TBSYS_LOG(WARN, "failed to set child of projecter, ret=%d", ret);
          }
          else 
          {
            root_ = &projecter_;
          }
        }
      }

      return ret;
    }

    int ObTabletLocalIndexScanner::add_project_column(
       const uint64_t data_table_id, const uint64_t index_table_id, 
       const uint64_t column_id)
    {
      int ret = OB_SUCCESS;
      ObSqlExpression sql_expression;
      ExprItem item;

      sql_expression.reset();
      item.value_.cell_.tid = data_table_id;
      item.value_.cell_.cid = column_id;
      item.type_ = T_REF_COLUMN;
      sql_expression.set_tid_cid(index_table_id, column_id);
      if (OB_SUCCESS != (ret = sql_expression.add_expr_item(item)))
      {
        TBSYS_LOG(WARN, "add_expr_item ret=%d, index_table_id=%lu, "
                        "data_table_id=%lu, column_id=%lu", 
                  ret, index_table_id, data_table_id, column_id);
      }
      else if (OB_SUCCESS != (ret = sql_expression.add_expr_item_end()))
      {
        TBSYS_LOG(WARN, "add_expr_item_end ret=%d, index_table_id=%lu, "
                        "data_table_id=%lu, column_id=%lu", 
                  ret, index_table_id, data_table_id, column_id);
      }
      else if (OB_SUCCESS != (ret = projecter_.add_output_column(sql_expression)))
      {
        TBSYS_LOG(WARN, "add_output_column ret=%d, index_table_id=%lu, "
                        "data_table_id=%lu, column_id=%lu", 
                  ret, index_table_id, data_table_id, column_id);
      }

      return ret;
    }

    int ObTabletLocalIndexScanner::init_sorter(const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* index_table_schema = NULL;

      sorter_.reset();
      if (NULL == (index_table_schema = scan_options.schema_->get_table_schema(
         scan_options.index_table_id_)))
      {
        TBSYS_LOG(WARN, "failed to get index table schema, index_table_id_=%lu",
                  scan_options.index_table_id_);
        ret = OB_ERROR;
      }
      else if (NULL == scan_options.sort_file_setter_)
      {
        TBSYS_LOG(WARN, "sort file setter not set");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = scan_options.sort_file_setter_->setup_sorter(
              sorter_, "cs_tablet_local_index_scan_sort")))
      {
        TBSYS_LOG(WARN, "set sorter file failed, ret %d", ret);
      }
      else 
      {
        const ObRowkeyInfo& rowkey_info = index_table_schema->get_rowkey_info();
        const ObRowkeyColumn* rowkey_column = NULL;

        for (int64_t i = 0; i < rowkey_info.get_size() && OB_SUCCESS == ret; i++)
        {
          if (NULL == (rowkey_column = rowkey_info.get_column(i)))
          {
            TBSYS_LOG(WARN, "failed to get rowkey column, index_table_id_=%lu",
                      scan_options.index_table_id_);
            ret = OB_ERROR;
          }
          else if (OB_SUCCESS != (ret= sorter_.add_sort_column(
             scan_options.index_table_id_, rowkey_column->column_id_, true)))
          {
            TBSYS_LOG(WARN, "failed to add sort column into sorter, "
                            "index_table_id_=%lu, ret=%d", 
                      scan_options.index_table_id_, ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (NULL == root_)
          {
            TBSYS_LOG(WARN, "sstable scanner operator is not initialization");
            ret = OB_ERROR;
          }
          else if (OB_SUCCESS != (ret = sorter_.set_child(0, *root_)))
          {
            TBSYS_LOG(WARN, "failed to set child of sorter, ret=%d", ret);
          }
          else 
          {
            root_ = &sorter_;
          }
        }
      }

      return ret;
    }

    int ObTabletLocalIndexScanner::init_sampler(const ObScanOptions& scan_options)
    {
      int ret = OB_SUCCESS;

      sampler_.clear();
      if (scan_options.row_interval_ <= 0)
      {
        //do nothing, needn't do sample
      }
      else if (OB_SUCCESS != (ret = sampler_.set_row_interval(scan_options.row_interval_)))
      {
        TBSYS_LOG(WARN, "failed to set row interval for interval sampler_, ret=%d", ret);
      }
      else if (NULL == root_)
      {
        TBSYS_LOG(WARN, "sstable scanner operator is not initialization");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = sampler_.set_child(0, *root_)))
      {
        TBSYS_LOG(WARN, "failed to set child of sampler, ret=%d", ret);
      }
      else 
      {
        root_ = &sampler_;
      }

      return ret;
    }
  } /* chunkserver */
} /* oceanbase */
