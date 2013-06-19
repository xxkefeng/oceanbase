AM_CPPFLAGS := $(AM_CPPFLAGS) -I${TBLIB_ROOT}/include/tbsys \
              -I${EASY_ROOT}/include/easy   \
              -I${top_srcdir}/include       \
              -I${top_srcdir}/src

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/sstable/libsstable.a                \
        $(top_builddir)/src/common/libcommon.a                  \
        $(top_builddir)/src/sql/libsql.a                        \
		$(top_builddir)/src/compactsstablev2/libcompactsstablev2.a  \
        $(top_builddir)/src/common/compress/libcomp.a           \
        $(top_builddir)/src/common/btree/libbtree.a             \
        $(top_builddir)/src/common/libcommon.a                  \
        $(top_builddir)/src/sql/libsql.a                        \
        $(top_builddir)/src/sstable/libsstable.a                \
        ${EASY_LIB_PATH}/libeasy.a                              \
        ${TBLIB_ROOT}/lib/libtbsys.a


AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lssl
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libchunkserver.a


libchunkserver_a_SOURCES =                                               \
 chunkserver.conf.template                                               \
                                  $(top_srcdir)/svn_version.cpp          \
 ob_cell_array_helper.h           ob_cell_array_helper.cpp               \
 ob_cell_stream.h                 ob_cell_stream.cpp                     \
 ob_chunk_callback.h              ob_chunk_callback.cpp                  \
 ob_chunk_merge.h                 ob_chunk_merge.cpp                     \
 ob_tablet_merger_v0.h            ob_tablet_merger_v0.cpp                \
 ob_tablet_merger_v2.h            ob_tablet_merger_v2.cpp                \
 ob_chunk_server.h                ob_chunk_server.cpp                    \
 ob_chunk_server_main.h           ob_chunk_server_main.cpp               \
 ob_chunk_server_stat.h           ob_chunk_server_stat.cpp               \
 ob_chunk_service.h               ob_chunk_service.cpp                   \
 ob_disk_manager.h                ob_disk_manager.cpp                    \
 ob_file_recycle.h                ob_file_recycle.cpp                    \
 ob_fileinfo_cache.h              ob_fileinfo_cache.cpp                  \
 ob_join_cache.h                  ob_join_cache.cpp                      \
 ob_multi_tablet_merger.h         ob_multi_tablet_merger.cpp             \
 ob_row_cell_vec.h                ob_row_cell_vec.cpp                    \
 ob_read_ups_balance.h            ob_read_ups_balance.cpp                \
 ob_rpc_proxy.h                   ob_rpc_proxy.cpp                       \
 ob_schema_task.h                 ob_schema_task.cpp                     \
 ob_sql_query_service.h                                                  \
 ob_sql_rpc_stub.h                ob_sql_rpc_stub.cpp                    \
 ob_tablet.h                      ob_tablet.cpp                          \
 ob_tablet_image.h                ob_tablet_image.cpp                    \
 ob_tablet_manager.h              ob_tablet_manager.cpp                  \
 ob_chunk_log_worker.h            ob_chunk_log_worker.cpp                \
 ob_chunk_log_manager.h           ob_chunk_log_manager.cpp               \
 ob_tablet_merge_filter.h         ob_tablet_merge_filter.cpp             \
 ob_ups_blacklist.h               ob_ups_blacklist.cpp                   \
 ob_chunk_server_config.h         ob_chunk_server_config.cpp             \
 ob_bypass_sstable_loader.h       ob_bypass_sstable_loader.cpp           \
 ob_tablet_service.h              ob_tablet_service.cpp                  \
 ob_chunk_reload_config.h         ob_chunk_reload_config.cpp             \
 ob_cs_plan_executor.h            ob_cs_plan_executor.cpp                \
 ob_cs_phy_operator_factory.h     ob_cs_phy_operator_factory.cpp          \
 index/ob_build_index_thread.h          index/ob_build_index_thread.cpp              \
 index/ob_index_tablet_image.h          index/ob_index_tablet_image.cpp              \
 index/ob_index_builder.h               index/ob_index_builder.cpp                   \
 index/ob_local_index_builder.h         index/ob_local_index_builder.cpp             \
 index/ob_local_index_sampler.h         index/ob_local_index_sampler.cpp             \
 index/ob_global_index_builder.h        index/ob_global_index_builder.cpp            \
 index/ob_table.h                       index/ob_table.cpp                           \
 index/ob_memory.h                      index/ob_tablet_predicator.h                 \
 index/ob_row_cursor.h                  index/ob_row_writer.h                        \
 index/ob_tablet_distribution.h         index/ob_tablet_distribution.cpp             \
 index/ob_tablet_local_index_builder.h  index/ob_tablet_local_index_builder.cpp      \
 index/ob_tablet_global_index_builder.h index/ob_tablet_global_index_builder.cpp     \
 index/ob_tablet_local_index_scan.h     index/ob_tablet_local_index_scan.cpp         \
 index/ob_tablet_memtable.h             index/ob_tablet_memtable.cpp                 \
 index/ob_tablet_sstable.h              index/ob_tablet_sstable.cpp                  \
 index/ob_remote_tablet_table.h         index/ob_remote_tablet_table.cpp              

bin_PROGRAMS = chunkserver
chunkserver_SOURCES = ob_disk_path.cpp main.cpp ${libchunkserver_a_SOURCES}

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

#pkgdata_data_DATA=chunkserver.conf.template
#pkgdata_datadir=${prefix}/etc
clean-local:
	-rm -f *.gcov *.gcno *.gcda
