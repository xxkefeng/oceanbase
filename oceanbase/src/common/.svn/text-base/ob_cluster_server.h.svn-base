#ifndef __OB_CLUSTER_SERVER_H_
#define __OB_CLUSTER_SERVER_H_

#include "ob_server.h"
#include "ob_define.h"
#include "ob_vector.h"

namespace oceanbase
{
  namespace common
  {
    static const char * RoleName[] =
    {
      "err",
      "rs",
      "cs",
      "ms",
      "ups"
    };
    struct ObClusterServer
    {
      ObRole role;
      ObServer addr;
      ObClusterServer()
      {
        role = OB_INVALID;
      }
      bool operator < (const ObClusterServer & other) const
      {
        bool bret = false;
        int ret = strcmp(RoleName[role], RoleName[other.role]);
        if (0 == ret)
        {
          bret = addr < other.addr;
        }
        else
        {
          bret = ret < 0;
        }
        return bret;
      }
    };

    template <>
    struct ob_vector_traits<ObClusterServer>
    {
      typedef ObClusterServer * pointee_type;
      typedef ObClusterServer value_type;
      typedef const ObClusterServer const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };
  }
}

#endif //__OB_CLUSTER_SERVER_H_
