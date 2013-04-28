/**
  * (C) 2007-2010 Taobao Inc.
  *
  * This program is free software; you can redistribute it and/or modify
  * it under the terms of the GNU General Public License version 2 as
  * published by the Free Software Foundation.
  *
  * Version: $Id$
  *
  * Authors:
  *   zhidong <xielun.szd@taobao.com>
  *     - some work details if you want
  */

#ifndef OB_ROOT_TABLE_PROXY_H_
#define OB_ROOT_TABLE_PROXY_H_

#include "common/ob_define.h"
#include "common/ob_array.h"

namespace oceanbase
{
  namespace common
  {
    class ObServer;
    class ObGetParam;
    class ObScanParam;
    class ObScanner;
    class ObTabletInfo;
    class ObTabletLocation;
    class ObTabletReportInfo;
    class ObTabletReportInfoList;
  }
  //
  namespace rootserver
  {
    class ObChunkServerManager;
    // this class just supply basic operations on the root_table struct
    class ObRootTableProxy
    {
    public:
      ObRootTableProxy();
      virtual ~ObRootTableProxy();
    public:
      /**
       * server manager may be not needed if not use memory table
       *
       * @param server_manager server manager info for get server index
       *
       * @return
       */
      void set_server_manager(ObChunkServerManager * server_manager);

      /////////////////////////////////////////////////////////////////////////////////////////////
      //                           scan and get root table                                       //
      /////////////////////////////////////////////////////////////////////////////////////////////
      /**
       * scan some tablets location info for api/mergeserver
       *
       * @param scan_param input scan param
       * @param scanner scan result set for tablets' location
       *
       * @return
       */
      virtual int scan(const common::ObScanParam & scan_param, common::ObScanner & scanner) const = 0;
      /**
       * get some tablets location info for api/mergeserver
       *
       * @param get_param input get param
       * @param scanner scan result set for tablets' location
       *
       * @return
       */
      virtual int get(const common::ObGetParam & get_param, common::ObScanner & scanner) const = 0;

      /////////////////////////////////////////////////////////////////////////////////////////////
      //                           replica related operation                                     //
      /////////////////////////////////////////////////////////////////////////////////////////////
      /**
       * remove the tablet replica info for the tablet
       *
       * @param replica include the tablet replica info and the location info
       *
       * @return
       */
      virtual int remove_tablet_replica(const common::ObTabletReportInfo & replica) = 0;
      /**
       * report the tablet replicas list info
       *
       * @param server the reported server addr
       * @param replicas the reported all tablet replicas info list
       *
       * @return
       */
      virtual int report_tablet_replicas(const common::ObServer & server,
                                         const common::ObTabletReportInfoList & replicas) = 0;
      /**
       * migrate the tablet from src to dest server
       *
       * @param keep_src reserve the src tablet or not
       * @param dest_server dest server for migrate to
       * @param replica tablet repalica info and location info
       *
       * @return
       */
      virtual int migrate_tablet_replica(const bool keep_src, const common::ObServer & dest_server,
                                         const common::ObTabletReportInfo & replica) = 0;

      /////////////////////////////////////////////////////////////////////////////////////////////
      //                           server location related                                       //
      /////////////////////////////////////////////////////////////////////////////////////////////
      typedef common::ObArray<common::ObTabletLocation> ObTabletLocationList;
      /**
       * get first tablet location
       *
       * @param location_list output param for the first tablet location info
       *
       * @return
       */
      virtual int get_first_tablet_location(ObTabletLocationList & location_list) const = 0;
      /**
       * if the chunkserver offline check whether the root table is integrated
       *
       * @param security_count the min tablet replica count after offline
       * @param server the server prepared to be offline
       *
       * @return
       */
      virtual int check_server_can_offline(const int64_t security_count, const common::ObServer & server) const = 0;
      /**
       * server offline occured
       *
       * @param server the offline chunkserver
       * @param timestamp the offline timestamp
       *
       * @return
       */
      virtual int server_offline(const common::ObServer & server, const int64_t timestamp) = 0;

      /////////////////////////////////////////////////////////////////////////////////////////////
      //                           table related operation                                       //
      /////////////////////////////////////////////////////////////////////////////////////////////
      /**
       * check the root table is already contains the table
       *
       * @param table_id the table id to be checked
       * @param exist output param for exist result
       *
       * @return
       */
      virtual int table_is_exist(const uint64_t table_id, bool & exist) const = 0;
      /**
       * create table after the empty tablet replica is assigned
       *
       * @param table_id the table id for create
       * @param version the frozen version when create table
       * @param replica the tablet replica location info
       *
       * @return
       */
      virtual int create_table(const uint64_t table_id, const int64_t version,
                               const common::ObTabletLocation & replica) = 0;
      /**
       * create table with exist tablets
       *
       * @param table_id the table id for create
       * @param version the frozen version when create table
       * @param dest_server the tablet replica dest chunkserver
       * @param tablet the tablet replica info
       *
       * @return
       */
      virtual int create_tablet(const uint64_t table_id, const int64_t version,
                                const common::ObServer & dest_server, const common::ObTabletInfo & tablet) = 0;
      /**
       * drop the table
       *
       * @param table_id the table id for drop
       *
       * @return
       */
      virtual int drop_table(const uint64_t table_id) = 0;

      /////////////////////////////////////////////////////////////////////////////////////////////
      //                           tablet related operation                                      //
      /////////////////////////////////////////////////////////////////////////////////////////////
      /**
       * check whether all the tablet is merged
       *
       * @param tablet_version the version in check
       * @param succ the merged result
       *
       * @return
       */
      virtual int tablet_is_merged(const int64_t tablet_version, bool & succ) const = 0;
      /**
       * check whether all the tablet is sercurity
       *
       * @param tablet_version the version in check
       * @param security_count the min replica count for tablet
       * @param succ the check result
       *
       * @return
       */
      virtual int tablet_is_security(const int64_t tablet_version, const int64_t security_count, bool & succ) const = 0;

    public:
      /////////////////////////////////////////////////////////////////////////////////////////////
      //                           root table checkpoint                                         //
      /////////////////////////////////////////////////////////////////////////////////////////////
      /**
       * write all the root table to binrary file for checkpoint
       *
       * @param file the output file path and file name
       *
       * @return
       */
      virtual int write_to_file(const char * file) const = 0;
      /**
       * read the root table from binrary file for recovery
       *
       * @param file the input file path and file name
       *
       * @return
       */
      virtual int read_from_file(const char * file) = 0;

    public:
      /////////////////////////////////////////////////////////////////////////////////////////////
      //                           dump for debug                                                //
      /////////////////////////////////////////////////////////////////////////////////////////////
      /**
       * dump all the tablet info for debug info
       *
       * @return
       */
      virtual void dump_all(void) const = 0;
      /**
       * dump the replica count less than security_count tablet info for debug info
       *
       * @param security_count the min tablet repalica count for check dump
       *
       * @return
       */
      virtual void dump_unusual_tablets(const int64_t security_count) const = 0;
      /**
       * dump tablet replicas belong to the chunkserver
       *
       * @param server the chunkserver info for dump info
       * @param tablet_num tablet replica count in the server serving
       *
       * @return
       */
      virtual void dump_tablets(const common::ObServer & server, int64_t & tablet_num) const = 0;
    private:
      // chunk server manager
      ObChunkServerManager * chunk_server_manager_;
    };
  }
}

#endif //OB_ROOT_TABLE_PROXY_H_
