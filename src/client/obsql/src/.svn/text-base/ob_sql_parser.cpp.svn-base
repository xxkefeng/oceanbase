#include "common/ob_malloc.h"
#include "ob_sql_parser.h"
#include <strings.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>

using namespace std;
using namespace oceanbase::common;

ObSQLType get_sql_type(const char *q, unsigned long length)
{
  ObSQLType type = OB_SQL_UNKNOWN;
  (void)(length);
  char *nq = strndup(q, length);
  char *iq = strtok(nq, " ");
  if (0 == strncasecmp(OB_SQL_START_TRANSACTION, iq, strlen(OB_SQL_START_TRANSACTION)))
  {
    type = OB_SQL_BEGIN_TRANSACTION;
  }
  else if (0 == strncasecmp(OB_SQL_BEG_TRANSACTION, iq, strlen(OB_SQL_BEG_TRANSACTION)))
  {
    type = OB_SQL_BEGIN_TRANSACTION;
  }
  else if(0 == strncasecmp(OB_SQL_COMMIT, iq, strlen(OB_SQL_COMMIT)))
  {
    type = OB_SQL_END_TRANSACTION;
  }
  else if(0 == strncasecmp(OB_SQL_ROLLBACK, iq, strlen(OB_SQL_ROLLBACK)))
  {
    type = OB_SQL_END_TRANSACTION;
  }
  free(nq);
  return type;
}
