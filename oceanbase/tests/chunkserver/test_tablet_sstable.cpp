/**
 * (C) 2010-2013 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *  
 *
 * Authors:
 *   yaoying <yaoying.yyy@alipay.com>
 *
 */
#include "mock_root_server.h"
#include "common/ob_base_client.h"
#include "common/ob_schema_manager.h"
#include "ob_chunk_server_main.h"
#include "common/file_directory_utils.h"
#include "ob_disk_manager.h"
#include "ob_tablet_manager.h"
#include "ob_tablet.h"
#include "chunkserver/index/ob_tablet_sstable.h"
#include "test_helper.h"
#include "../common/test_rowkey_helper.h"

using namespace oceanbase;
using namespace common;
using namespace sstable;
using namespace compactsstablev2;

namespace oceanbase
{
  namespace tests
  {
    namespace chunkserver
    {
      class MemoryPoolInit
      {
        public:
          MemoryPoolInit()
          {
            ob_init_memory_pool();
          } 
          ~MemoryPoolInit(){}
      };
      static  MemoryPoolInit pool_init;//this is for memory_pool init
      static const uint64_t index_table_id = 101;
      static const uint64_t data_table_id = 100;
      static const uint64_t base_column_id = 1001;
      static const uint64_t ROW_NUM = 50000;
      static const uint64_t COL_NUM = 6;
      static const uint64_t ROWKEY_CELL_COUNT = 1;

      static ObSchemaManagerV2* schema = NULL;
      static TabletManagerIniter manager_initer;
      static ObTabletManager tablet_manager; //it may need tobe init
      static ObNewRange tablet_range;
      static ObLocalIndexTabletImage * local_index_tablet_image = NULL;
      static ObTablet*  data_tablet = NULL; 
      static ObDiskManager*  disk_manager = NULL;// get from tablet_manager 
      static ObAppendOptions option;
      static ObTabletSSTableAppender appender_v;

      static CharArena allocator;
      static ObRow     row;
      static ObString  rowkey_str;
      static char      tmp_rowkey[50];
      static ObObj   obj;
      static ObRowDesc row_desc;

      class TestTabletSSTable : public ::testing::Test
      {
        public:
          static void SetUpTestCase()
          {
            int ret = OB_SUCCESS;
            //init version range_
            compactsstablev2::ObFrozenMinorVersionRange version_range;
            version_range.major_version_ = 2;//end init version_range_,???? 
             
            //init tablet_range 
            tablet_range.set_whole_range();
            tablet_range.table_id_ = data_table_id;//end init tablet_range should here be table_id???
            
            //init schema manager
            tbsys::CConfig config;
            schema = new ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
            ASSERT_TRUE(NULL != schema);
            ASSERT_TRUE(schema->parse_from_file("./schema/index_schema.ini", config));
            //
            ObTableSchema* index_table = schema->get_table_schema(index_table_id);
            ASSERT_TRUE(NULL != index_table);
            index_table->set_data_table_id(data_table_id);
            ASSERT_EQ(0, schema->cons_table_to_index_relation());//end init schema manager
            
            //init disk manager,get it from tablet_manager
            disk_manager =  &tablet_manager.get_disk_manager();

            local_index_tablet_image = new ObLocalIndexTabletImage(
                         tablet_manager.get_serving_tablet_image());
            ASSERT_TRUE(NULL != local_index_tablet_image); 
            manager_initer.set_tablet_manager(tablet_manager);
            ret = manager_initer.init(false, false, base_column_id);
            ASSERT_EQ(OB_SUCCESS, ret);
            tablet_manager.get_serving_tablet_image().prepare_for_merge(2);

            //init data_tablet
            data_tablet =  new ObTablet(const_cast<ObTabletImage *>(&(tablet_manager.get_serving_tablet_image().get_last_not_merged_image())));
            //this is simple for create tablet
            bool ret_local = data_tablet->try_create_local_index(data_table_id);
            ASSERT_EQ(ret_local, true);
            data_tablet->set_range(tablet_range);
            data_tablet->set_data_version(2);
            
            //init options.
            option.version_range_ = version_range;
            option.tablet_range_ = tablet_range;
            option.schema_ = schema;
            option.tablet_manager_ = &tablet_manager;
            option.disk_manager_ = disk_manager;
            option.tablet_image_ = local_index_tablet_image;
            option.data_tablet_ = data_tablet;
          }

          static void TearDownTestCase()
          {
            if (NULL != data_tablet)
            {
              delete data_tablet;
              data_tablet = NULL;
            }
            if (NULL != local_index_tablet_image)
            {
              delete local_index_tablet_image;
              local_index_tablet_image = NULL;
            }
            if (NULL != schema)
            {
              delete schema;
              schema = NULL;
            }
          }

        public:
          virtual void SetUp()
          {
          }

          virtual void TearDown()
          {
          }
      };


      TEST_F(TestTabletSSTable, test_open)
      {
        int ret = OB_SUCCESS;
        ret = appender_v.open(option);
        ASSERT_EQ(ret, OB_SUCCESS);
      }

      TEST_F(TestTabletSSTable, test_append_one_row)
      {
        int ret = OB_SUCCESS;
        //init row_desc
        for (uint64_t j = 0; j < COL_NUM; ++j) 
        {
          uint64_t column_id = j + base_column_id;
          row_desc.add_column_desc(data_table_id, column_id);
        }
        row_desc.set_rowkey_cell_count(ROWKEY_CELL_COUNT);
        row.set_row_desc(row_desc);
        //init rowkey   
        uint64_t row_num = 0; 
        sprintf(tmp_rowkey, "row_key_%08ld", row_num);
        rowkey_str.assign(tmp_rowkey, static_cast<int32_t>(strlen(tmp_rowkey)));
        obj.set_varchar(rowkey_str);
        //init row
        row.set_cell(data_table_id, base_column_id, obj);
        for (uint64_t j = 1; j < COL_NUM; ++j) 
        {
          obj.reset();
          obj.set_int(j-1);
          row.set_cell(data_table_id, base_column_id + j, obj);
        }

        ret = appender_v.append(row);
        ASSERT_EQ(ret, OB_SUCCESS);
      }

      TEST_F(TestTabletSSTable, test_append_many_row)
      {
        int ret = OB_SUCCESS;
        //from 1 to ROW_NUM-1
        for (uint64_t i = 1; i < ROW_NUM; ++i)
        {
          sprintf(tmp_rowkey, "row_key_%08ld", i);
          rowkey_str.assign(tmp_rowkey, static_cast<int32_t>(strlen(tmp_rowkey)));
          obj.set_varchar(rowkey_str);
          row.set_cell(data_table_id, base_column_id, obj);
          for (uint64_t k = 1; k < COL_NUM; ++k) 
          {
            obj.reset();
            obj.set_int(i*(COL_NUM - 1) + (k -1));
            row.set_cell(data_table_id, base_column_id + k, obj);
          }
          ret = appender_v.append(row);
          ASSERT_EQ(ret, OB_SUCCESS);
        }
      }

      TEST_F(TestTabletSSTable, test_close)
      {
        int ret = OB_SUCCESS;
        ret = appender_v.close(true);
        ASSERT_TRUE(NULL != data_tablet->get_local_index());
        //this must be invoked ,or the tablet is not visible 
        ret = tablet_manager.get_serving_tablet_image().add_tablet(const_cast<ObTablet*>(data_tablet->get_local_index()), true, true);
        EXPECT_EQ(ret, OB_SUCCESS);

        ObTablet * tmp_tablet = NULL;
        ObMultiVersionTabletImage& tablet_image = tablet_manager.get_serving_tablet_image();
        ret = tablet_image.begin_scan_tablets();
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = tablet_image.get_next_tablet(tmp_tablet);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(NULL != tmp_tablet);

        ASSERT_EQ((int64_t)ROW_NUM, tmp_tablet->get_row_count());
        const ObNewRange & range = tmp_tablet->get_range();
        ASSERT_EQ(data_table_id, range.table_id_);

        ASSERT_EQ(2, tmp_tablet->get_data_version());

        ret = tablet_manager.get_serving_tablet_image().release_tablet(tmp_tablet);
        EXPECT_EQ(ret, OB_SUCCESS);

      }

      TEST_F(TestTabletSSTable, get_and_check)
      {
        int ret = OB_SUCCESS;
        ObGetParam get_param;
        ObCellInfo param_cell;
        ObScanner scanner;
        ObCellInfo ci;

        uint64_t row_num = 0; 
        sprintf(tmp_rowkey, "row_key_%08ld", row_num);
        rowkey_str.assign(tmp_rowkey, static_cast<int32_t>(strlen(tmp_rowkey)));

        param_cell.table_id_ = data_table_id;
        param_cell.row_key_ = make_rowkey(tmp_rowkey, &allocator);
        param_cell.column_id_ = 0;

        ret = get_param.add_cell(param_cell);
        EXPECT_EQ(OB_SUCCESS, ret);

        ret = tablet_manager.get(get_param, scanner);
        ASSERT_EQ(OB_SUCCESS, ret);

        // check result;only get one row here
        int64_t count = 0;
        ObScannerIterator iter;
        for (iter = scanner.begin(); iter != scanner.end(); iter++)
        {
          EXPECT_EQ(OB_SUCCESS, iter.get_cell(ci));
          if ( count == 0)
          {
            EXPECT_EQ((uint64_t)base_column_id, ci.column_id_);
            ObString get_string;
            ci.value_.get_varchar(get_string);
            check_string(rowkey_str, get_string);
          }
          else
          {
            int64_t get_int = 0;
            ci.value_.get_int(get_int);
            EXPECT_EQ((uint64_t)(base_column_id + count), ci.column_id_);
            EXPECT_EQ((int64_t)(row_num * 5 + count -1), get_int);
          }
          ++count;
        }
        EXPECT_EQ((int64_t)COL_NUM, count);
        tablet_manager.end_get();
      }

      TEST_F(TestTabletSSTable, scan_and_check)
      {
        int err = OB_SUCCESS;
        ObScanParam scan_param;
        ObNewRange scan_range;
        ObCellInfo* ci;
        ObCellInfo expected;
        ObString table_name(strlen("data_table") + 1, strlen("data_table") + 1, (char*)"data_table");

        uint64_t start_row_num = 0; 
        sprintf(tmp_rowkey, "row_key_%08ld", start_row_num);
        scan_range.start_key_ = make_rowkey(tmp_rowkey, &allocator);
        uint64_t all_row_num = ROW_NUM; 
        // uint64_t all_row_num = 1; //for debug
        sprintf(tmp_rowkey, "row_key_%08ld", all_row_num -1);
        scan_range.table_id_ = data_table_id;
        scan_range.end_key_ = make_rowkey(tmp_rowkey, &allocator);
        scan_range.border_flag_.set_inclusive_start();
        scan_range.border_flag_.set_inclusive_end();

        scan_param.set(data_table_id, table_name, scan_range);
        for (uint64_t j = 0; j < COL_NUM; ++j)
        {
          scan_param.add_column(base_column_id + j);
        }
        reset_query_thread_local_buffer();

        ObScanner scanner;
        err = tablet_manager.scan(scan_param, scanner);
        EXPECT_EQ(OB_SUCCESS, err);
        // check result
        int64_t count = 0;
        sstable::ObSSTableScanner *seq_scanner = GET_TSI_MULT(sstable::ObSSTableScanner, TSI_CS_SSTABLE_SCANNER_1);
        if (NULL == seq_scanner)
        {
          TBSYS_LOG(ERROR, "failed to get thread local sequence scaner, seq_scanner=%p",
              seq_scanner);
          err= OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          while (OB_SUCCESS == err && OB_SUCCESS == (err = seq_scanner->next_cell()))
          {
            err = seq_scanner->get_cell(&ci);
            EXPECT_EQ(OB_SUCCESS, err);
            uint64_t row_num = count / COL_NUM;
            uint64_t col_id = count % COL_NUM;

            sprintf(tmp_rowkey, "row_key_%08ld", row_num);
            rowkey_str.assign(tmp_rowkey, static_cast<int32_t>(strlen(tmp_rowkey)));
            expected.row_key_ = make_rowkey(tmp_rowkey, &allocator);
            expected.table_id_ = data_table_id;  
            expected.column_id_ = base_column_id + col_id;  
            expected.table_name_ = table_name;

            ObObj exp;
            if (0 == col_id)
            {
               ObString get_string;
               ci->value_.get_varchar(get_string);
               EXPECT_EQ((uint64_t)expected.column_id_, ci->column_id_);
               check_string(rowkey_str, get_string);
               //exp.set_varchar(rowkey_str);
            //TODO check_cell can't apply to varchar??
            } else
            {
              int64_t get_int = 0;
              ci->value_.get_int(get_int);
              EXPECT_EQ( (int64_t)(row_num *5 + col_id -1), get_int);
              exp.set_int(row_num *5 + col_id -1);
              expected.value_ = exp;
              check_cell(expected, *ci);
            }
          ++count;
          }
        }
        EXPECT_EQ((int64_t)((all_row_num) * COL_NUM), count);
        seq_scanner->cleanup();
        tablet_manager.end_scan();
      }

    } /* chunkserver */
  } /* tests */
} /* oceanbase */

class FooEnvironment : public testing::Environment
{
  public:
    virtual void SetUp()
    {
      ::mallopt(M_MMAP_THRESHOLD, 68*1024);
      TBSYS_LOGGER.setLogLevel("WARN");
      prepare_sstable_directroy(12);
    }
    virtual void TearDown()
    {
    }
};

int main(int argc, char** argv)
{
  testing::AddGlobalTestEnvironment(new FooEnvironment);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

