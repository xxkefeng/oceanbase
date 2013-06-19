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
#include "common/ob_schema.h"
#include "ob_index_operation.h"

namespace oceanbase
{
  namespace rootserver
  {
    using namespace oceanbase;
    using namespace common;
    using namespace sql;

    ObSampleMemtable::ObSampleMemtable()
    {

    }

    ObSampleMemtable::~ObSampleMemtable()
    {

    }

    int ObSampleMemtable::set_row_desc(const common::ObRowDesc& row_desc)
    {
      cur_row_desc_ = row_desc;
      return OB_SUCCESS;
    }

    int ObSampleMemtable::get_row_desc(const ObRowDesc*& row_desc) const
    {
      int ret = OB_SUCCESS;

      if (cur_row_desc_.get_column_num() <= 0)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "cur_row_desc_ is empty");
      }
      else
      {
        row_desc = &cur_row_desc_;
      }

      return ret;      
    }

    int ObSampleMemtable::append_row(const common::ObRow& row)
    {
      int ret = OB_SUCCESS;
      const ObRowStore::StoredRow *stored_row = NULL;

      tbsys::CThreadGuard guard(&mutex_);
      if (OB_SUCCESS == (ret = row_store_.add_row(row, stored_row)))
      {
        row_count_++;
      }

      return ret;
    }

    void ObSampleMemtable::reset()
    {
      row_count_ = 0;
      cur_row_desc_.reset();
      row_store_.reuse();
    }

    int ObSampleMemtable::open()
    {
      cur_row_.set_row_desc(cur_row_desc_);
      return OB_SUCCESS;
    }

    int ObSampleMemtable::get_next_row(const ObRow*& row)
    {
      int ret = OB_SUCCESS;

      ret = row_store_.get_next_row(cur_row_);
      if (OB_ITER_END == ret)
      {
        TBSYS_LOG(DEBUG, "end of iteration");
      }
      else if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "fail to get next row from row store, ret=%d", ret);
      }
      else
      {
        row = &cur_row_;
      }

      return ret;
    }

    int ObSampleMemtable::close()
    {
      return OB_SUCCESS;
    }

    int64_t ObSampleMemtable::to_string(
       char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;

      if (NULL != buf && buf_len > 0)
      {
        databuff_printf(buf, buf_len, pos, "ObSampleMemtable(");
        databuff_printf(buf, buf_len, pos, "row_desc=");
        pos += cur_row_desc_.to_string(buf + pos, buf_len - pos);
        databuff_printf(buf, buf_len, pos, ", row_store=");
        pos += row_store_.to_string(buf + pos, buf_len - pos);
        databuff_printf(buf, buf_len, pos, ")\n");
      }

      return pos;
    }

    ObIndexSampler::ObIndexSampler()
    : root_(NULL)
    {

    }

    ObIndexSampler::~ObIndexSampler()
    {

    }

    void ObIndexSampler::reset()
    {
      cur_row_desc_.reset();
      sample_memtable_.reset();
      sorter_.reset();
      sampler_.clear();
      root_ = NULL;
    }

    int ObIndexSampler::init(
       const ObSchemaManagerV2* schema_mgr, const ObBuildIndexInfo& build_index_info)
    {
      int ret = OB_SUCCESS;

      reset();
      if (OB_SUCCESS != (ret = init_row_desc(schema_mgr, build_index_info)))
      {
        TBSYS_LOG(WARN, "failed to init row desc, ret=%d", ret);
      }

      return ret;
    }

    int ObIndexSampler::open(
       const ObSchemaManagerV2* schema_mgr, const ObBuildIndexInfo build_index_info)
    {
      int ret = OB_SUCCESS;

      if (NULL == schema_mgr || OB_INVALID_ID == build_index_info.data_table_id_
          || OB_INVALID_ID == build_index_info.index_table_id_ 
          || build_index_info.sample_count_ <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, schema_mgr=%p, data_table_id_=%lu, "
                        "index_table_id_=%lu, sample_count=%ld",
                  schema_mgr, build_index_info.data_table_id_, build_index_info.index_table_id_,
                  build_index_info.sample_count_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = init_sample_memtable()))
      {
        TBSYS_LOG(WARN, "failed to init sample memtable, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = init_sorter(schema_mgr, build_index_info)))
      {
        TBSYS_LOG(WARN, "failed to init sorter, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = init_sampler(build_index_info)))
      {
        TBSYS_LOG(WARN, "failed to init sampler, ret=%d", ret);
      }
      else if (NULL == root_)
      {
        TBSYS_LOG(WARN, "after init, root phyoperator of index sampler is NULL");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = root_->open()))
      {
        TBSYS_LOG(WARN, "failed to open index sampler phyoperator, ret=%d", ret);
      }

      if (OB_SUCCESS != ret && NULL != root_)
      {
        root_->close();
      }

      return ret;
    }

    int ObIndexSampler::init_row_desc(
       const ObSchemaManagerV2* schema_mgr, const ObBuildIndexInfo& build_index_info)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* table_schema = NULL;
      cur_row_desc_.reset();

      if (NULL == (table_schema = schema_mgr->get_table_schema(
         build_index_info.index_table_id_)))
      {
        TBSYS_LOG(WARN, "failed to get table schema, table_id_=%lu",
                  build_index_info.index_table_id_);
        ret = OB_ERROR;
      }
      else 
      {
        const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
        const ObRowkeyColumn* rowkey_column = NULL;

        for (int64_t i = 0; i < rowkey_info.get_size() && OB_SUCCESS == ret; i++)
        {
          if (NULL == (rowkey_column = rowkey_info.get_column(i)))
          {
            TBSYS_LOG(WARN, "failed to get rowkey column, table_id_=%lu", 
                      build_index_info.index_table_id_);
            ret = OB_ERROR;
          }
          else if(OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(
             build_index_info.index_table_id_, rowkey_column->column_id_)))
          {
            TBSYS_LOG(WARN, "row desc or sampler add rowkey column failed, table_id=%lu, "
                            "column_id_=%lu, ret=%d", 
                      build_index_info.index_table_id_, rowkey_column->column_id_, ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          cur_row_desc_.set_rowkey_cell_count(rowkey_info.get_size());
        }
      }

      return ret;
    }

    int ObIndexSampler::init_sample_memtable()
    {
      int ret = OB_SUCCESS;

      if (cur_row_desc_.get_rowkey_cell_count() <= 0)
      {
        TBSYS_LOG(WARN, "current row desc isn't initialized, rowkey_cell_count=%ld",
                  cur_row_desc_.get_rowkey_cell_count());
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = sample_memtable_.set_row_desc(cur_row_desc_)))
      {
        TBSYS_LOG(WARN, "failed to set row desc of sample memtable, ret=%d", ret);
      }
      else 
      {
        root_ = &sample_memtable_;
      }

      return ret;
    }

    int ObIndexSampler::init_sorter(
       const ObSchemaManagerV2* schema_mgr, const ObBuildIndexInfo& build_index_info)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* index_table_schema = NULL;

      sorter_.reset();
      if (NULL == (index_table_schema = schema_mgr->get_table_schema(
         build_index_info.index_table_id_)))
      {
        TBSYS_LOG(WARN, "failed to get index table schema, index_table_id_=%lu",
                  build_index_info.index_table_id_);
        ret = OB_ERROR;
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
                      build_index_info.index_table_id_);
            ret = OB_ERROR;
          }
          else if (OB_SUCCESS != (ret= sorter_.add_sort_column(
             build_index_info.index_table_id_, rowkey_column->column_id_, true)))
          {
            TBSYS_LOG(WARN, "failed to add sort column into sorter, "
                            "index_table_id_=%lu, ret=%d", 
                      build_index_info.index_table_id_, ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          if (NULL == root_)
          {
            TBSYS_LOG(WARN, "sample memtable operator is not initialization");
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

    int ObIndexSampler::init_sampler(const ObBuildIndexInfo& build_index_info)
    {
      int ret = OB_SUCCESS;
      int64_t row_count = 0;
      int64_t row_interval = 0;

      sampler_.clear();
      if (build_index_info.sample_count_ <= 0)
      {
        TBSYS_LOG(WARN, "invalid param, sample_count_=%ld", build_index_info.sample_count_);
        ret =OB_INVALID_ARGUMENT;
      }
      else if ((row_count = sample_memtable_.get_row_count()) < 0)
      {
        TBSYS_LOG(WARN, "no row in sample memtable, row_count=%ld", 
                  row_count);
        ret = OB_ERROR;
      }
      else 
      {
        row_interval = row_count / (build_index_info.sample_count_ + 1) + 1;

        if (OB_SUCCESS != (ret = sampler_.set_row_interval(row_interval)))
        {
          TBSYS_LOG(WARN, "failed to set row interval for interval sampler_, ret=%d", ret);
        }
        else if (NULL == root_)
        {
          TBSYS_LOG(WARN, "sample memtable operator is not initialization");
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
      }

      return ret;
    }

    ObIndexOperation::ObIndexOperation()
    : allocator_(common::ObModIds::OB_CS_BUILD_INDEX),
      cur_row_(NULL)
    {
      cur_rowkey_.set_min_row();
    }

    ObIndexOperation::~ObIndexOperation()
    {

    }

    void ObIndexOperation::reset()
    {
      allocator_.reuse();
      index_sampler_.reset();
      cur_rowkey_.set_min_row();
      cur_row_ = NULL;
    }

    int ObIndexOperation::init_sample_table(
       const ObSchemaManagerV2* schema_mgr, const ObBuildIndexInfo& build_index_info)
    {
      build_index_info_ = build_index_info;
      allocator_.reuse();
      cur_rowkey_.set_min_row();
      cur_row_ = NULL;

      return index_sampler_.init(schema_mgr, build_index_info);
    }

    int ObIndexOperation::report_samples(
       const int32_t server_index, const ObNewScanner& sample_scanner)
    {
      int ret = OB_SUCCESS;
      ObRow row;

      row.set_row_desc(index_sampler_.get_cur_row_desc());
      while (OB_SUCCESS == ret)
      {
        ret = const_cast<ObNewScanner&>(sample_scanner).get_next_row(row);
        if (OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
          break;
        }
        else if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(WARN, "fail to get next row from sample scanner, server_index=%d, ret=%d", 
                    server_index, ret);
        }
        else if (OB_SUCCESS != (ret = index_sampler_.append_row(row)))
        {
          TBSYS_LOG(WARN, "fail to append row into index sampler memtable, server_index=%d, ret=%d", 
                    server_index, ret);
        }
      }

      return ret;
    }

    int ObIndexOperation::build_sample_result(const ObSchemaManagerV2* schema_mgr)
    {
      int ret = OB_SUCCESS;

      if (NULL == schema_mgr)
      {
        TBSYS_LOG(WARN, "invalid param, schema_mgr=%p", schema_mgr);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = index_sampler_.open(schema_mgr, build_index_info_)))
      {
        TBSYS_LOG(WARN, "failed to open index sampler, ret=%d", ret);
      }

       return ret;
    }

    int ObIndexOperation::get_next_sample_result(ObTabletInfoList& tablet_list)
    {
      int ret = OB_SUCCESS;
      ObPhyOperator* sampler = NULL;
      const ObRowkey* rowkey = NULL;
      ObTabletInfo tablet_info;
      tablet_list.reset();

      if (NULL == (sampler = index_sampler_.get_phyoperator()))
      {
        TBSYS_LOG(WARN, "after open index sampler, it returns NULL operator");
        ret = OB_ERROR;
      }
      else
      {
        tablet_info.range_.table_id_ = build_index_info_.index_table_id_;

        while (OB_SUCCESS == ret)
        {
          tablet_info.range_.start_key_ = cur_rowkey_;
          tablet_info.range_.border_flag_.unset_inclusive_start();
          if(NULL == cur_row_)
          {
            ret = sampler->get_next_row(cur_row_);
            if(OB_ITER_END == ret)
            {
              tablet_info.range_.end_key_.set_max_row();
              tablet_info.range_.border_flag_.unset_inclusive_end();
              ret = tablet_list.add_tablet(tablet_info);
              if(OB_ARRAY_OUT_OF_RANGE == ret)
              {
                ret = OB_SUCCESS;
              }
              else if(OB_SUCCESS == ret)
              {
                cur_row_ = NULL;
                ret = OB_ITER_END;
              }
              else
              {
                TBSYS_LOG(WARN, "add tablet info into tablet list fail, ret=%d", ret);
              }
              break;
            }
            else if(OB_SUCCESS != ret)
            {
              TBSYS_LOG(WARN, "get next row fail, ret=%d", ret);
              break;
            }
          }

          if(OB_SUCCESS == ret)
          {
            if (OB_SUCCESS != (ret = cur_row_->get_rowkey(rowkey))
                || NULL == rowkey)
            {
              TBSYS_LOG(WARN, "failed to get rowkey from current row, ret=%d", ret);
              break;
            }
            else if (OB_SUCCESS != (ret = rowkey->deep_copy(cur_rowkey_, allocator_)))
            {
              TBSYS_LOG(WARN, "failed to copy last end rowkey, index_table_id=%lu, "
                              "rowkey=%s, ret=%d", 
                        build_index_info_.index_table_id_, to_cstring(*rowkey), ret);
            }
            else
            {
              tablet_info.range_.end_key_ = cur_rowkey_;
              tablet_info.range_.border_flag_.set_inclusive_end();

              ret = tablet_list.add_tablet(tablet_info);
              if(OB_ARRAY_OUT_OF_RANGE == ret)
              {
                ret = OB_SUCCESS;
                break;
              }
              else if(OB_SUCCESS == ret)
              {
                cur_row_ = NULL;
              }
              else
              {
                TBSYS_LOG(WARN, "add tablet info into tablet list fail, ret=%d", ret);
                break;
              }
            }
          }
        }

        if (OB_SUCCESS != ret)
        {
          sampler->close();
        }
      }

      return ret;
    }
  } /* rootserver */
} /* oceanbase */
