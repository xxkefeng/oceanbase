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
#include "mock_root_server.h"
#include "common/ob_base_client.h"
#include "common/ob_schema_manager.h"
#include "ob_chunk_server_main.h"
#include "common/file_directory_utils.h"
#include "index/ob_tablet_local_index_builder.h"
#include "index/ob_tablet_global_index_builder.h"
#include "ob_disk_manager.h"
#include "ob_tablet_manager.h"
#include "test_helper.h"
#include "fake_rpc_proxy.h"

namespace oceanbase
{
  namespace tests
  {
    namespace chunkserver
    {
      using namespace oceanbase;
      using namespace sql;
      using namespace common;
      using namespace sstable;
      using namespace compactsstablev2;

      static const uint64_t index_table_id = 101;
      static ObSchemaManagerV2* schema = NULL;
      static ObLocalIndexTabletImage* local_index_tablet_image = NULL;
      static ObTabletLocalIndexBuilder* local_index_builder = NULL;
      static ObTabletLocalIndexSampler* index_sampler = NULL;
      static ObGlobalIndexTabletImage* global_index_tablet_image = NULL;
      static ObTabletGlobalIndexBuilder* global_index_builder = NULL;
      static ObTabletDistribution* data_tablet_dist = NULL;
      static ObTabletDistribution* index_tablet_dist = NULL;
      static ObServer self_server;
      static ObSortFileSetter sort_file_setter;
        

      class TestIndexBuilder : public ::testing::Test
      {
        public:
          static void SetUpTestCase()
          {
            int ret = OB_SUCCESS;
            ObTabletManager& tablet_manager = THE_CHUNK_SERVER.get_tablet_manager();
            TabletManagerIniter manager_initer(tablet_manager);

            ret = manager_initer.init(true, false, 1002);
            ASSERT_EQ(OB_SUCCESS, ret);


            //init schema manager
            tbsys::CConfig config;
            schema = new ObSchemaManagerV2(tbsys::CTimeUtil::getTime());
            ASSERT_TRUE(NULL != schema);
            ASSERT_TRUE(schema->parse_from_file("./schema/index_schema.ini", config));
            ObTableSchema* index_table = schema->get_table_schema(index_table_id);
            index_table->set_data_table_id(SSTableBuilder::table_id);
            ASSERT_EQ(0, schema->cons_table_to_index_relation());

            local_index_tablet_image = new ObLocalIndexTabletImage(
               tablet_manager.get_serving_tablet_image());
            ASSERT_TRUE(NULL != local_index_tablet_image);
            local_index_builder = new ObTabletLocalIndexBuilder(
               tablet_manager, *local_index_tablet_image);
            ASSERT_TRUE(NULL != local_index_builder);

            self_server.set_ipv4_addr(tbsys::CNetUtil::getLocalAddr("bond0"), 2600);
            global_index_tablet_image = new ObGlobalIndexTabletImage(
               tablet_manager.get_serving_tablet_image());
            ASSERT_TRUE(NULL != global_index_tablet_image);
            global_index_builder = new ObTabletGlobalIndexBuilder(
               tablet_manager, *global_index_tablet_image);
            ASSERT_TRUE(NULL != global_index_builder);

            sort_file_setter.init(tablet_manager.get_disk_manager(), 2 << 20);

            data_tablet_dist = new ObTabletDistribution(*local_index_tablet_image);
            ASSERT_TRUE(NULL != data_tablet_dist);
            index_tablet_dist = new ObTabletDistribution(*global_index_tablet_image);
            ASSERT_TRUE(NULL != index_tablet_dist);
          }

          static void TearDownTestCase()
          {
            if (NULL != schema)
            {
              delete schema;
              schema = NULL;
            }

            if (NULL != local_index_tablet_image)
            {
              delete local_index_tablet_image;
              local_index_tablet_image =NULL;
            }

            if (NULL != local_index_builder)
            {
              delete local_index_builder;
              local_index_builder =NULL;
            }

            if (NULL != index_sampler)
            {
              delete index_sampler;
              index_sampler =NULL;
            }

            if (NULL != global_index_tablet_image)
            {
              delete global_index_tablet_image;
              global_index_tablet_image =NULL;
            }

            if (NULL != global_index_builder)
            {
              delete global_index_builder;
              global_index_builder =NULL;
            }

            if (NULL != data_tablet_dist)
            {
              delete data_tablet_dist;
              data_tablet_dist =NULL;
            }

            if (NULL != index_tablet_dist)
            {
              delete index_tablet_dist;
              index_tablet_dist =NULL;
            }
          }

        public:
          virtual void SetUp()
          {

          }

          virtual void TearDown()
          {

          }

          void build_data_tablet_dist(ObTabletManager &tablet_manager)
          {
            int ret = OB_SUCCESS;
            ObTabletDistItem tablet_item;
            ObTablet* tablet = NULL;
            ObMultiVersionTabletImage& tablet_image = tablet_manager.get_serving_tablet_image();
            ret = tablet_image.begin_scan_tablets();
            ASSERT_EQ(OB_SUCCESS, ret);

            data_tablet_dist->reset();
            while (true)
            {
              ret = tablet_image.get_next_tablet(tablet);
              if (OB_ITER_END == ret)
              {
                break;
              }
              ASSERT_EQ(OB_SUCCESS, ret);
              ASSERT_TRUE(NULL != tablet);

              tablet_item.tablet_range_ = tablet->get_range();
              tablet_item.locations_count_ = 1;
              tablet_item.locations_[0].tablet_status_ = oceanbase::common::TABLET_AVAILABLE;
              tablet_item.locations_[0].location_.tablet_version_ = 1;
              tablet_item.locations_[0].location_.tablet_seq_ = 0;
              tablet_item.locations_[0].location_.chunkserver_ = self_server;

              ret = data_tablet_dist->add_tablet_item(tablet_item);
              ASSERT_EQ(OB_SUCCESS, ret);
            }
            ret = tablet_image.end_scan_tablets();
            ASSERT_EQ(OB_SUCCESS, ret);            
          }

          void build_index_tablet_dist(ObPhyOperator* phy_operator)
          {
            int ret = OB_SUCCESS;
            const ObRow* row = NULL;
            int64_t row_count = 0;
            const ObRowkey* last_rowkey = &ObRowkey::MIN_ROWKEY;
            ObTabletDistItem tablet_item;
            CharArena allocator_;

            index_tablet_dist->reset();
            ASSERT_TRUE(NULL != phy_operator);
            while (true)
            {
              tablet_item.tablet_range_.table_id_ = index_table_id;
              last_rowkey->deep_copy(tablet_item.tablet_range_.start_key_, allocator_);
              tablet_item.tablet_range_.border_flag_.unset_inclusive_start();
              ret = phy_operator->get_next_row(row);

              if (OB_SUCCESS == ret)
              {
                row_count++;
                ASSERT_TRUE(NULL != row);
                TBSYS_LOG(INFO, "row=[%ld] [%s]", row_count, to_cstring(*row));

                ret = row->get_rowkey(last_rowkey);
                ASSERT_EQ(OB_SUCCESS, ret);
                ASSERT_TRUE(NULL != last_rowkey);
                tablet_item.tablet_range_.end_key_ = *last_rowkey;
                tablet_item.tablet_range_.border_flag_.set_inclusive_end();
              }
              else if (OB_ITER_END == ret)
              {
                tablet_item.tablet_range_.end_key_.set_max_row();
                tablet_item.tablet_range_.border_flag_.unset_inclusive_end();
              }
              else 
              {
                ASSERT_EQ(OB_SUCCESS, ret);
              }
              tablet_item.locations_count_ = 1;
              tablet_item.locations_[0].tablet_status_ = oceanbase::common::TABLET_UNAVAILABLE;
              tablet_item.locations_[0].location_.tablet_version_ = 1;
              tablet_item.locations_[0].location_.tablet_seq_ = 0;
              tablet_item.locations_[0].location_.chunkserver_ = self_server;

              ASSERT_EQ(OB_SUCCESS, index_tablet_dist->add_tablet_item(tablet_item));
              tablet_item.reset();
              allocator_.reuse();
              if (OB_ITER_END == ret)
              {
                break;
              }
            }
            ASSERT_EQ(10, row_count);
            phy_operator->close();
          }

          void delete_local_index(ObMultiVersionTabletImage &tablet_image)
          {
            ObTablet *tablet;
            ASSERT_EQ(OB_SUCCESS, tablet_image.begin_scan_tablets());
            while (OB_ITER_END != tablet_image.get_next_tablet(tablet))
            {
              ASSERT_TRUE(tablet);
              if (tablet->get_local_index())
              {
                TBSYS_LOG(INFO, "delete local index [%s]",
                    to_cstring(*tablet->get_local_index()));
                tablet->delete_local_index();
              }
            }
            ASSERT_EQ(OB_SUCCESS, tablet_image.end_scan_tablets());
          }
      };

      TEST_F(TestIndexBuilder, test_build_tablet_local_index)
      {
        int ret = OB_SUCCESS;
        ObTabletManager& tablet_manager = THE_CHUNK_SERVER.get_tablet_manager();

        ASSERT_TRUE(NULL != local_index_builder);

        ObTablet* tablet = NULL;
        ObMultiVersionTabletImage& tablet_image = tablet_manager.get_serving_tablet_image();
        ret = tablet_image.begin_scan_tablets();
        ASSERT_EQ(OB_SUCCESS, ret);
        while (true)
        {
          ret = tablet_image.get_next_tablet(tablet);
          if (OB_ITER_END == ret)
          {
            break;
          }
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_TRUE(NULL != tablet);
          ret = local_index_builder->build(*schema, *tablet, index_table_id, sort_file_setter);
          ASSERT_EQ(OB_SUCCESS, ret);
        }
        ret = tablet_image.end_scan_tablets();
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      TEST_F(TestIndexBuilder, test_sample_tablet_local_index)
      {
        int ret = OB_SUCCESS;
        ObTabletMemtable media_sample_memtable;
        ObTabletManager& tablet_manager = THE_CHUNK_SERVER.get_tablet_manager();

        ASSERT_TRUE(NULL != local_index_tablet_image);
        index_sampler = new ObTabletLocalIndexSampler(
           tablet_manager, *local_index_tablet_image, media_sample_memtable);
        ASSERT_TRUE(NULL != index_sampler);

        int64_t sample_count = 10;
        int64_t row_interval = 0;
        ObTablet* tablet = NULL;
        ObTablet* index_tablet = NULL;
        ObMultiVersionTabletImage& tablet_image = 
          tablet_manager.get_serving_tablet_image();
        ret = tablet_image.begin_scan_tablets();
        ASSERT_EQ(OB_SUCCESS, ret);
        while (true)
        {
          ret = tablet_image.get_next_tablet(tablet);
          if (OB_ITER_END == ret)
          {
            break;
          }
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_TRUE(NULL != tablet);

          index_tablet = tablet->get_local_index();
          ASSERT_TRUE(NULL != index_tablet);
          row_interval = index_tablet->get_row_count() / sample_count;
          row_interval = (0 == row_interval) ? 1 : row_interval;

          ret = index_sampler->sample_tablet(*schema, tablet->get_range(), 
                                             index_tablet->get_range(), row_interval);
          ASSERT_EQ(OB_SUCCESS, ret);
        }
        ret = tablet_image.end_scan_tablets();
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(media_sample_memtable.get_row_count() > 0);
        ASSERT_EQ(MultSSTableBuilder::SSTABLE_NUM * sample_count, media_sample_memtable.get_row_count());
        ASSERT_EQ(SSTableBuilder::ROW_NUM / sample_count, row_interval);

        ObNewRange index_tablet_range;
        index_tablet_range.set_whole_range();
        index_tablet_range.table_id_ = index_table_id;
        ObPhyOperator* phy_operator = NULL;
        row_interval = media_sample_memtable.get_row_count() / sample_count;
        row_interval = (0 == row_interval) ? 1 : row_interval;
        ASSERT_EQ(MultSSTableBuilder::SSTABLE_NUM * sample_count / sample_count, row_interval);
        ret = index_sampler->sample_memtable(media_sample_memtable, *schema, 
                                            index_tablet_range, row_interval,
                                            sort_file_setter, phy_operator);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_TRUE(NULL != phy_operator);

        build_index_tablet_dist(phy_operator);
      }

      TEST_F(TestIndexBuilder, test_build_tablet_global_index)
      {
        int ret = OB_SUCCESS;
        bool stop = false;

        build_data_tablet_dist(THE_CHUNK_SERVER.get_tablet_manager());

        ASSERT_TRUE(NULL != global_index_builder);

        ObSelectedTabletPredicator selected_tablet_predicator(self_server);
        ObTabletIterator index_tablet_iterator(*index_tablet_dist, selected_tablet_predicator);
        const ObTabletDistItem* tablet_item = NULL;
        ObRpcOptions rpc_option;

        ASSERT_EQ(11, index_tablet_dist->get_tablet_item_count());
        ASSERT_TRUE(index_tablet_dist->has_unavaliable_tablet(self_server));

        while (true)
        {
          ret = index_tablet_iterator.next(tablet_item);
          if (OB_ITER_END == ret)
          {
            break;
          }
          ASSERT_EQ(OB_SUCCESS, ret);

          ret = global_index_builder->build(*data_tablet_dist, *schema, 
                                            *tablet_item, rpc_option, self_server,
                                            sort_file_setter, stop);
          ASSERT_EQ(OB_SUCCESS, ret);

          tablet_item = NULL;
        }

        ASSERT_FALSE(index_tablet_dist->has_unavaliable_tablet(self_server));
      }

      TEST_F(TestIndexBuilder, test_build_tablet_local_index_for_old_sstable_pattern)
      {
        int ret = OB_SUCCESS;
        ObTabletManager& tablet_manager = THE_CHUNK_SERVER.get_tablet_manager();

        ASSERT_TRUE(NULL != local_index_builder);

        ObTablet* tablet = NULL;
        ObMultiVersionTabletImage& tablet_image = tablet_manager.get_serving_tablet_image();
        //delete local index
        delete_local_index(tablet_image);
        ret = tablet_image.begin_scan_tablets();
        ASSERT_EQ(OB_SUCCESS, ret);
        while (true)
        {
          ret = tablet_image.get_next_tablet(tablet);
          if (OB_ITER_END == ret)
          {
            break;
          }
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_TRUE(NULL != tablet);
          //old sstable pattern
          int64_t write_sstable_version = SSTableReader::ROWKEY_SSTABLE_VERSION;
          ret = local_index_builder->build(*schema, *tablet, index_table_id, sort_file_setter, write_sstable_version);
          ASSERT_EQ(OB_SUCCESS, ret);
        }
        ret = tablet_image.end_scan_tablets();
        ASSERT_EQ(OB_SUCCESS, ret);
      }

      TEST_F(TestIndexBuilder, test_build_tablet_global_index_for_old_sstable_pattern)
      {
        int ret = OB_SUCCESS;
        bool stop = false;

        //build_data_tablet_dist(THE_CHUNK_SERVER.get_tablet_manager());
        ObArray<ObTabletDistItem>& tablet_array = 
            const_cast<ObArray<ObTabletDistItem>&>(index_tablet_dist->get_tablet_item_array());
        for (int64_t i = 0; i < tablet_array.count(); ++i)
        {
          tablet_array.at(i).locations_[0].tablet_status_ = oceanbase::common::TABLET_UNAVAILABLE;
        }
        ASSERT_TRUE(NULL != global_index_builder);

        ObSelectedTabletPredicator selected_tablet_predicator(self_server);
        ObTabletIterator index_tablet_iterator(*index_tablet_dist, selected_tablet_predicator);
        const ObTabletDistItem* tablet_item = NULL;
        ObRpcOptions rpc_option;

        ASSERT_EQ(11, index_tablet_dist->get_tablet_item_count());
        ASSERT_TRUE(index_tablet_dist->has_unavaliable_tablet(self_server));

        while (true)
        {
          ret = index_tablet_iterator.next(tablet_item);
          if (OB_ITER_END == ret)
          {
            break;
          }
          ASSERT_EQ(OB_SUCCESS, ret);

          //old sstable pattern
          int64_t write_sstable_version = SSTableReader::ROWKEY_SSTABLE_VERSION;
          ret = global_index_builder->build(*data_tablet_dist, *schema, 
                                            *tablet_item, rpc_option, self_server,
                                            sort_file_setter, stop, write_sstable_version);
          ASSERT_EQ(OB_SUCCESS, ret);

          tablet_item = NULL;
        }

        ASSERT_FALSE(index_tablet_dist->has_unavaliable_tablet(self_server));
      }

      TEST_F(TestIndexBuilder, test_index_builder_thread)
      {
        int ret = OB_SUCCESS;
        const char* dst_addr = "localhost";
        ObTabletManager& tablet_mgr = THE_CHUNK_SERVER.get_tablet_manager();


        //init chunkserver instance
        ObChunkServerMain* cm = ObChunkServerMain::get_instance();
        ObChunkServer& cs = cm->get_chunk_server();
        cs.get_config().port = 2600;
        cs.get_config().root_server_ip.set_value(dst_addr);
        cs.get_config().root_server_port = MOCK_SERVER_LISTEN_PORT;
        cs.get_config().index_builder_thread_num = 1;
        cs.get_config().index_sort_mem_limit = (3 << 20);
        cs.get_config().datadir.set_value("./tmp");
        cs.initialize();

        // init global root server 
        ObServer root_server = cs.get_root_server();
        ASSERT_TRUE(0 != root_server.get_ipv4());
        ASSERT_EQ(MOCK_SERVER_LISTEN_PORT, root_server.get_port());

        // init global client manager
        ObBaseClient client_manager ;
        EXPECT_EQ(OB_SUCCESS, client_manager.initialize(root_server));

        FakeRpcProxy *rpc_proxy = new FakeRpcProxy(1, 10 * 1000 * 1000, root_server);
        cs.set_rpc_proxy(rpc_proxy);

        //start mock rootserver
        tbsys::CThread test_root_server_thread;
        MockRootServerRunner* test_root_server = new MockRootServerRunner();
        ASSERT_TRUE(NULL != test_root_server);

        ObArray<ObTabletDistItem>& tablet_array = 
          const_cast<ObArray<ObTabletDistItem>&>(index_tablet_dist->get_tablet_item_array());
        for (int64_t i = 0; i < tablet_array.count(); ++i)
        {
          tablet_array.at(i).locations_[0].tablet_status_ = oceanbase::common::TABLET_UNAVAILABLE;
        }
        test_root_server->set_tablet_array(data_tablet_dist->get_tablet_item_array(), 
            index_tablet_dist->get_tablet_item_array());
        test_root_server->get_mock_server().set_schema(*schema);
        test_root_server_thread.start(test_root_server, NULL);

        sleep(2);

        ASSERT_EQ(OB_SUCCESS, cs.start(false));
        cs.get_rpc_stub().init(cs.get_thread_specific_rpc_buffer(),
                               &client_manager.get_client_mgr());
        //init schema manager
        cs.get_schema_manager() = new ObMergerSchemaManager();
        ASSERT_TRUE(NULL != cs.get_schema_manager());
        ret = cs.get_schema_manager()->init(false, *schema);
        EXPECT_EQ(0, ret);

        //delete index tablet and local index tablet from tablet image.
        //the test before these tablets into the tablet iamge
        ret = tablet_mgr.get_serving_tablet_image().delete_table(index_table_id);
        ASSERT_EQ(OB_SUCCESS, ret);

        ObMultiVersionTabletImage& tablet_image = tablet_mgr.get_serving_tablet_image();
        delete_local_index(tablet_image);

        //init index builder thread
        ret = tablet_mgr.get_build_index_thread().init();
        EXPECT_EQ(0, ret);

        TBSYS_LOG(INFO, "start build sample");
        //sample
        sleep(1);
        ret = tablet_mgr.get_build_index_thread().start_build_sample(
           SSTableBuilder::table_id, index_table_id, 10);
        EXPECT_EQ(0, ret);
        while (!tablet_mgr.get_build_index_thread().is_finish_build())
        {
          sleep(2);
        }

        delete_local_index(tablet_image);

        fprintf(stderr, "=========== start build global index from local cs ===========\n");
        //build global index
        ret = tablet_mgr.get_build_index_thread().start_build_global_index(
           SSTableBuilder::table_id, index_table_id);
        EXPECT_EQ(0, ret);
        while (!tablet_mgr.get_build_index_thread().is_finish_build())
        {
          sleep(2);
        }

        ObServer bad_addr(ObServer::IPV4, "127.0.0.111", 26000);
        ObArray<ObTabletDistItem> &data_tablet_array = 
          const_cast<ObArray<ObTabletDistItem>&>(data_tablet_dist->get_tablet_item_array());
        for (int64_t i = 0; i < data_tablet_array.count(); i++)
        {
          ObTabletDistItem &item = data_tablet_array.at(i);
          item.locations_count_ = 2;
          int64_t selected = ObTabletLocationSelector()(item);
          int64_t alternative = (selected + 1) % 2;

          item.locations_[selected].location_.chunkserver_ = self_server;
          item.locations_[selected].tablet_status_ = oceanbase::common::TABLET_AVAILABLE;
          item.locations_[selected].location_.tablet_version_ = 1;
          item.locations_[selected].location_.tablet_seq_ = 1;

          item.locations_[alternative].location_.chunkserver_ = bad_addr;
          item.locations_[alternative].tablet_status_ = oceanbase::common::TABLET_AVAILABLE;
          item.locations_[alternative].location_.tablet_version_ = 1;
          item.locations_[alternative].location_.tablet_seq_ = 1;
        }
        
        
        // swap first data tablet location to make global index building retry.
        std::swap(data_tablet_array.at(0).locations_[0],
            data_tablet_array.at(0).locations_[1]);

        // build sample again to make new data tablet distribution available
        ret = tablet_mgr.get_build_index_thread().start_build_sample(
           SSTableBuilder::table_id, index_table_id, 10);
        EXPECT_EQ(0, ret);
        while (!tablet_mgr.get_build_index_thread().is_finish_build())
        {
          sleep(2);
        }

        delete_local_index(tablet_image);

        // Set tablet location and self address to 127.0.0.1 
        for (int64_t i = 0; i < tablet_array.count(); ++i)
        {
          ObServer &server = tablet_array.at(i).locations_[0].location_.chunkserver_;
          server.set_ipv4_addr("127.0.0.1", server.get_port());
          tablet_array.at(i).locations_[0].tablet_status_ = oceanbase::common::TABLET_UNAVAILABLE;
        }
        ObServer &chunk_server = const_cast<ObServer &>(THE_CHUNK_SERVER.get_self());
        chunk_server.set_ipv4_addr("127.0.0.1", chunk_server.get_port());

        fprintf(stderr, "=========== start build global index from remote cs ===========\n");
        //build global index
        ret = tablet_mgr.get_build_index_thread().start_build_global_index(
           SSTableBuilder::table_id, index_table_id);
        EXPECT_EQ(0, ret);
        while (!tablet_mgr.get_build_index_thread().is_finish_build())
        {
          sleep(2);
        }

        ObServer chunk_addr(ObServer::IPV4, "127.0.0.1", 2600);
        for (int64_t i = 0; i < tablet_array.count(); ++i)
        {
          tablet_array.at(i).locations_count_ = 2;
          int64_t selected = ObTabletLocationSelector()(tablet_array.at(i));
          int64_t alternative = (selected + 1) % 2;

          tablet_array.at(i).locations_[selected].location_.chunkserver_ = self_server;
          tablet_array.at(i).locations_[selected].tablet_status_ = oceanbase::common::TABLET_AVAILABLE;

          tablet_array.at(i).locations_[selected].location_.tablet_version_ = 1;
          tablet_array.at(i).locations_[selected].location_.tablet_seq_ = 1;

          tablet_array.at(i).locations_[alternative].location_.chunkserver_ = chunk_addr;
          tablet_array.at(i).locations_[alternative].tablet_status_ = oceanbase::common::TABLET_UNAVAILABLE;

          tablet_array.at(i).locations_[alternative].location_.tablet_version_ = 1;
          tablet_array.at(i).locations_[alternative].location_.tablet_seq_ = 1;
        }
        fprintf(stderr, "=========== start build global index from by tablet migrate ===========\n");
        ret = tablet_mgr.get_build_index_thread().start_build_global_index(
           SSTableBuilder::table_id, index_table_id);
        EXPECT_EQ(0, ret);
        while (!tablet_mgr.get_build_index_thread().is_finish_build())
        {
          sleep(2);
        }

        client_manager.destroy();

        if (NULL != test_root_server)
        {
          test_root_server->get_mock_server().stop();
          test_root_server->get_mock_server().wait();
          delete test_root_server;
        }

        cs.stop();
        cs.wait();
      }

      TEST_F(TestIndexBuilder, test_empty_sstable)
      {
        static ObTabletManager *empty_tablet_manager = new ObTabletManager();
        // ObTabletManager& tablet_manager = THE_CHUNK_SERVER.get_tablet_manager();
        TabletManagerIniter empty_initer(*empty_tablet_manager);

        /*
        tablet_manager.get_serving_tablet_image().delete_table(100);
        tablet_manager.get_serving_tablet_image().delete_table(index_table_id);
        tablet_manager.~ObTabletManager();
        */
        // TabletManagerIniter empty_initer(tablet_manager);
        ASSERT_EQ(OB_SUCCESS, empty_initer.init(true, true, 1002));

        int ret = OB_SUCCESS;
        ObTabletManager &tablet_manager = *empty_tablet_manager;

        // <1> build local index
        ObLocalIndexTabletImage *index_image = new ObLocalIndexTabletImage(
            tablet_manager.get_serving_tablet_image());
        ASSERT_TRUE(NULL != index_image);

        ObTabletLocalIndexBuilder *index_builder = new ObTabletLocalIndexBuilder(
            tablet_manager, *index_image);
        ASSERT_TRUE(NULL != index_builder);

        ObTablet *tablet = NULL;
        ObMultiVersionTabletImage &tablet_image = tablet_manager.get_serving_tablet_image();

        ASSERT_EQ(OB_SUCCESS, tablet_image.begin_scan_tablets());
        while (true)
        {
          ret = tablet_image.get_next_tablet(tablet);
          if (OB_ITER_END == ret)
          {
            break;
          }

          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_TRUE(tablet);

          ASSERT_EQ(OB_SUCCESS,
              index_builder->build(*schema, *tablet, index_table_id, sort_file_setter));
        }
        ASSERT_EQ(OB_SUCCESS, tablet_image.end_scan_tablets());

        // <2> sample
        ObTabletMemtable media_sample_memtable;
        ObTabletLocalIndexSampler *sampler = new ObTabletLocalIndexSampler(
            tablet_manager, *index_image, media_sample_memtable);
        ASSERT_TRUE(sampler);

        int64_t row_interval = 1;

        ObTablet *index_tablet = NULL;
        ASSERT_EQ(OB_SUCCESS, tablet_image.begin_scan_tablets());
        while (true)
        {
          ret = tablet_image.get_next_tablet(tablet);
          if (OB_ITER_END == ret)
          {
            break;
          }

          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_TRUE(tablet);

          index_tablet = tablet->get_local_index();
          ASSERT_TRUE(index_tablet);

          ASSERT_EQ(OB_SUCCESS, sampler->sample_tablet(*schema, tablet->get_range(),
                index_tablet->get_range(), row_interval));
        }
        ASSERT_EQ(OB_SUCCESS, tablet_image.end_scan_tablets());
        ASSERT_EQ(0, media_sample_memtable.get_row_count());

        // <3> build global index
        build_data_tablet_dist(tablet_manager);

        ObGlobalIndexTabletImage *global_image = new ObGlobalIndexTabletImage(
            tablet_manager.get_serving_tablet_image());
        ObTabletGlobalIndexBuilder *tablet_global_index_builder =
          new ObTabletGlobalIndexBuilder(tablet_manager, *global_image);

        ObRpcOptions rpc_option;
        ObTabletDistItem item;
        ObNewRange range;
        range.table_id_ = index_table_id;
        range.start_key_ = ObRowkey::MIN_ROWKEY;
        range.end_key_ = ObRowkey::MAX_ROWKEY;
        range.border_flag_.unset_inclusive_start();
        range.border_flag_.set_inclusive_end();
        item.tablet_range_ = range;
        item.locations_count_ = 1;
        item.locations_[0].tablet_status_ = oceanbase::common::TABLET_UNAVAILABLE;
        item.locations_[0].location_.tablet_version_ = 1;
        item.locations_[0].location_.tablet_seq_ = 0;
        item.locations_[0].location_.chunkserver_ = self_server;

        bool stop = false;
        ret = tablet_global_index_builder->build(*data_tablet_dist, *schema, item, rpc_option,
            self_server, sort_file_setter, stop);

        ASSERT_EQ(OB_SUCCESS, ret);

        tablet = NULL;

        ASSERT_EQ(OB_SUCCESS, global_image->acquire_tablet(range, tablet));
        ASSERT_TRUE(tablet);

        ASSERT_EQ(0, tablet->get_extend_info().row_count_);
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
      ob_init_memory_pool();
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
