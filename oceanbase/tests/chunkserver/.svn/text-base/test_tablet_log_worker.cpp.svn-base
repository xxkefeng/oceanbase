/**
 * (C) 2010-2011 Taobao Inc.
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU General Public License 
 *  version 2 as published by the Free Software Foundation. 
 *  
 *  test_tablet_log_worker.cpp
 *
 *  Authors:
 *    zian <yunliang.shi@alipay.com>
 *
 */
#include "gtest/gtest.h"
#include "test_helper.h"
#include "chunkserver/ob_chunk_log_worker.h"
#include "chunkserver/ob_chunk_log_manager.h"
#include "common/ob_vector.h"

using namespace oceanbase;
static ObTabletManager tablet_manager;
static TabletManagerIniter manager_initer(tablet_manager);
static ObChunkLogManager log_manager;
static ObChunkLogWorker* log_worker = NULL;
static const uint64_t table_id = 100;

class TestTabletLogWorker : public ::testing::Test
{
  public:
    static void SetUpTestCase()
    {
      system("rm -rf log");
      system("rm -rf tmp");
      char create_dir[100];
      for(int i = 1; i < 16; i++)
      {
        sprintf(create_dir, "mkdir -p ./tmp/%d", i);
        system(create_dir);
      }

      int err = OB_SUCCESS;
      err = manager_initer.init(true, false);
      ASSERT_EQ(OB_SUCCESS, err);
      
      system("mkdir ./log");
      err = log_manager.init(&tablet_manager, "./log", 10240, true);

      log_worker = log_manager.get_log_worker();

      log_worker->set_recovered();
      log_manager.replay_log();
    }

    //test set flag, and delete the tablet
    void test_single_log()
    {
      ObTablet* tablet = NULL;
      ObMultiVersionTabletImage& tablet_image = tablet_manager.get_serving_tablet_image();
      tablet_image.begin_scan_tablets();
        
      int i = 0;
      char range_buf[10000];
      memset(range_buf, 0, sizeof(range_buf));
      int64_t pos = 0;
      int64_t len = 10000;
      
      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      common::ObNewRange del_range;  
      del_range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      del_range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);

      while(true)
      {
        tablet_image.get_next_tablet(tablet);
        if(NULL == tablet)
          break;

        const common::ObNewRange& range = tablet->get_range();     


        ASSERT_FALSE(tablet->is_with_next_brother());
        ASSERT_FALSE(tablet->is_merged());
      
  
        if(5 == i++)
        {
          range.serialize(range_buf, len, pos);
          pos = 0;
          del_range.deserialize(range_buf, len, pos);
          log_worker->remove_tablet(tablet_image.get_last_not_merged_version(), range, tablet->get_disk_no());
        }
        else
        {
          sstable::ObSSTableId sstable_id = tablet->get_sstable_id();
          log_worker->set_with_next_brother(tablet_image.get_last_not_merged_version(), range, 1);
          log_worker->set_tablet_merged(tablet_image.get_last_not_merged_version(), range);
        }

        tablet_image.release_tablet(tablet);
      };     

      ASSERT_EQ(OB_SUCCESS, log_worker->flush_log(OB_LOG_CS_DEL_TABLE));      

      ASSERT_EQ(24, i);

      tablet_image.end_scan_tablets();
      
      ObChunkLogManager log_replay_manager;
      
      log_replay_manager.init(&tablet_manager, "./log", 10240, true);
  
      log_replay_manager.replay_log();

      i = 0;
      tablet_image.begin_scan_tablets();
      while(true)
      {
        tablet_image.get_next_tablet(tablet);
        if(NULL == tablet)
          break;
                
        const common::ObNewRange& range = tablet->get_range();     
        if(del_range == range)
        {
          FAIL();
        }

        ASSERT_TRUE(tablet->is_with_next_brother());
        ASSERT_TRUE(tablet->is_merged());
        
        tablet_image.release_tablet(tablet);
        i++;
      };   

      tablet_image.end_scan_tablets();
      ASSERT_EQ(23, i);  

      uint64_t new_log_file_id = 0;
      log_manager.switch_log_file(new_log_file_id);
      log_manager.write_ckpt_log(new_log_file_id - 1);
      log_manager.do_check_point(new_log_file_id - 1);
    }

    void test_add_tablet()
    {
      ObTablet* tablet = NULL;
      char range_buf[10000];
      memset(range_buf, 0, sizeof(range_buf));
      int64_t pos = 0;
      int64_t len = 10000;
      int ret = OB_SUCCESS;
      ObMultiVersionTabletImage& tablet_image = tablet_manager.get_serving_tablet_image();
      common::ObNewRange del_range;  
      ObObj obj_array[OB_MAX_ROWKEY_COLUMN_NUMBER * 2];
      del_range.start_key_.assign(obj_array, OB_MAX_ROWKEY_COLUMN_NUMBER);
      del_range.end_key_.assign(obj_array + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);

      tablet_image.begin_scan_tablets();
      int i = 0;
      while(true)
      {
        tablet_image.get_next_tablet(tablet);
        
        int32_t disk_no = tablet->get_disk_no();
        
        common::ObNewRange range = tablet->get_range();
        if(2 == i++)
        {
          range.serialize(range_buf, len, pos);
          pos = 0;
          del_range.deserialize(range_buf, len, pos);
          //delete the tablet, but add the tablet by log
          tablet_image.remove_tablet(tablet->get_range(), tablet_image.get_last_not_merged_version(), disk_no);

          
          log_worker->add_tablet(tablet_image.get_last_not_merged_version(), tablet, 0);
          break;       
        }

        tablet_image.release_tablet(tablet);
      };     

      log_worker->flush_log(OB_LOG_CS_DEL_TABLE);
      tablet_image.release_tablet(tablet);


      tablet_image.end_scan_tablets();
      
      ObChunkLogManager log_replay_manager;
      
      log_replay_manager.init(&tablet_manager, "./log", 10240, true);
  
      ret = log_replay_manager.replay_log();
      ASSERT_EQ(OB_SUCCESS, ret);

      ObTablet* recover_tablet = NULL;
      i = 0;
      tablet_image.begin_scan_tablets();
      while(true)
      {
        tablet_image.get_next_tablet(recover_tablet);

        if(NULL == recover_tablet)        
          break;      

        if(2 == i++)
        {
          //delete the tablet, but add the tablet by log
          ASSERT_TRUE(recover_tablet->get_range() == del_range);
          tablet_image.release_tablet(recover_tablet);
          continue;
        }

        tablet_image.release_tablet(recover_tablet);
      };     

      ASSERT_EQ(23, i);
      tablet_image.end_scan_tablets();

      uint64_t new_log_file_id = 0;
      log_manager.switch_log_file(new_log_file_id);
      log_manager.write_ckpt_log(new_log_file_id - 1);
      log_manager.do_check_point(new_log_file_id - 1);
    }
   
    void test_del_table()
    {
      ObTablet* tablet = NULL;
      ObMultiVersionTabletImage& tablet_image = tablet_manager.get_serving_tablet_image();

      log_worker->delete_table(table_id);
      tablet_image.begin_scan_tablets();
      int i = 0;
      while(true)
      {
        tablet_image.get_next_tablet(tablet);
        if(NULL == tablet)
          break;

        i++;
        tablet_image.release_tablet(tablet);
      };     

      ASSERT_EQ(23, i);

      log_worker->flush_log(OB_LOG_CS_DEL_TABLE);
      tablet_image.end_scan_tablets();
      
      ObChunkLogManager log_replay_manager;
      
      log_replay_manager.init(&tablet_manager, "./log", 10240, true);
  
    
      log_replay_manager.replay_log();

      i = 0;
      tablet_image.begin_scan_tablets();
      while(true)
      {
        tablet_image.get_next_tablet(tablet);
        if(NULL == tablet)
          break;
        
        tablet_image.release_tablet(tablet);
        i++;
      };   

      ASSERT_EQ(0, i);  

      uint64_t new_log_file_id = 0;
      log_manager.switch_log_file(new_log_file_id);
      log_manager.write_ckpt_log(new_log_file_id - 1);
      log_manager.do_check_point(new_log_file_id - 1);
    }
};

TEST_F(TestTabletLogWorker, CHECK_LOG_WRITE_REPLAY)
{
  test_single_log();
}

TEST_F(TestTabletLogWorker, CHECK_ADD_TABLET)
{
  test_add_tablet();
}

TEST_F(TestTabletLogWorker, CHECK_DELETE_TABLE)
{
  test_del_table();
}	

class FooEnvironment : public testing::Environment
{
  public:
    virtual void SetUp()
    {
      TBSYS_LOGGER.setLogLevel("ERROR");
      ob_init_memory_pool();
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



