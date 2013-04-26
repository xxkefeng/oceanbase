#ifndef __OB_IMPORT_H__
#define  __OB_IMPORT_H__

#include "common/utility.h"
#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "tokenizer.h"
#include "file_reader.h"
#include "oceanbase_db.h"
#include <vector>

using namespace oceanbase::common;
using namespace oceanbase::api;

struct ColumnDesc {
  std::string name;
  int offset;
  int len;
};

struct RowkeyDesc {
  int offset;
  int type;
  int pos;
};

struct ColumnInfo {
  const ObColumnSchemaV2 *schema;
  int offset;
};

class TestRowBuilder;

class ObRowBuilder {
  public:
    friend class TestRowBuilder;
  public:
    enum RowkeyDataType{
      INT8,
      INT64,
      INT32,
      VARCHAR,
      DATETIME,
      INT16
    };

  static const int kMaxRowkeyDesc = 10;
  public:
    ObRowBuilder(ObSchemaManagerV2 *schema, const char *table_name, int input_column_nr, const RecordDelima &delima, bool has_nop_flag, char nop_flag, bool has_null_flag, char null_flag);
    ~ObRowBuilder();
    
    int set_column_desc(const std::vector<ColumnDesc> &columns);

    //int set_rowkey_desc(const std::vector<RowkeyDesc> &rowkeys);

    bool check_valid();

    int build_tnx(RecordBlock &block, DbTranscation *tnx) const;

    int create_rowkey(ObRowkey &rowkey, TokenInfo *tokens) const;
    int setup_content(RowMutator *mutator, TokenInfo *tokens) const;
    int make_obobj(const ColumnInfo &column_info, ObObj &obj, TokenInfo *tokens) const;
    inline int get_lineno() const
    {
      return atomic_read(&lineno_);
    }

  private:
    ObSchemaManagerV2 *schema_;
    RecordDelima delima_;

    ColumnInfo columns_desc_[OB_MAX_COLUMN_NUMBER];
    int columns_desc_nr_;

    //RowkeyDesc rowkey_desc_[kMaxRowkeyDesc];
    int64_t rowkey_desc_nr_;
    int64_t rowkey_offset_[kMaxRowkeyDesc];

    //int columns_desc_nr_;

    const char *table_name_;

    int input_column_nr_;

    mutable atomic_t lineno_;

    int64_t rowkey_max_size_;
    bool has_nop_flag_;
    char nop_flag_;
    bool has_null_flag_;
    char null_flag_;
};

#endif
