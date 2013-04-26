/*
 *   (C) 2007-2010 Taobao Inc.
 *
 *   Version: 0.1
 *
 *   Authors:
 *      qushan <qushan@taobao.com>
 *        - some work details if you want
 *
 */

#include <new>
#include <malloc.h>
#include "common/ob_define.h"
#include "ob_chunk_server_main.h"
#include "easy_pool.h"

using namespace oceanbase::common;
using namespace oceanbase::chunkserver;

namespace
{
  static const int DEFAULT_MMAP_THRESHOLD = 64 * 1024 + 128;
}


int main(int argc, char* argv[])
{
  ::mallopt(M_MMAP_THRESHOLD, DEFAULT_MMAP_THRESHOLD);
  ob_init_memory_pool();
  //easy_pool_set_allocator(ob_malloc);
  ObChunkServerMain* cm = ObChunkServerMain::get_instance();
  int ret = OB_SUCCESS;
  if (NULL == cm)
  {
    fprintf(stderr, "cannot start chunkserver, new ObChunkServerMain failed\n");
    ret = OB_ERROR;
  }
  else
  {
    ret = cm->start(argc, argv);
    cm->destroy();

    return ObChunkServerMain::restart_server(argc, argv);
  }
  // never get here.
}
