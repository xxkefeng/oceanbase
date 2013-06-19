#include "test_compact_common.h"
//#include "google/profiler.h"

using namespace oceanbase;

static const char* CONFIG_FILE_NAME = "compactsstable_benchmark.config";


struct SrcSSTable
{
  int64_t sstable_size_;
  int64_t sstable_trailer_offset_;
  sstable::ObSSTableTrailer sstable_trailer_;
  sstable::ObSSTableSchema schema_;
  sstable::ObSSTableBlockIndexHeader block_index_header_;
  sstable::ObSSTableBlockIndexItem* block_index_item_;
  common::ObRow* row_array_;
  char* block_buf_;

  SrcSSTable()
    : sstable_size_(0),
      sstable_trailer_offset_(0),
      block_index_item_(NULL),
      row_array_(NULL),
      block_buf_(NULL)
  {
    //sstable_trailer_(construct)
    //table_range_(construct)
    //table_range_buf_(construct)
    //schema_(construct)
    //block_index_header_(construct)
  }

  ~SrcSSTable()
  {
    if (NULL != block_index_item_)
    {
      common::ob_free(block_index_item_);
    }
  }
};

struct PerfTestConfig
{
  const char* file_name_;
  const char* test_name_;
  const char* compress_name_;
  const char* dst_sstable_path_;
  int quiet_;
  int64_t row_count_;

  PerfTestConfig()
    : file_name_(NULL),
      test_name_(NULL),
      compress_name_(NULL),
      quiet_(0),
      row_count_(0)
  {
  }
};

int read_record(const int fd, const int64_t offset, const int64_t size, const char*& buffer);

int load_sstable_file(const PerfTestConfig& config, SrcSSTable& src_sstable);

int get_sstable_size(const char* file_name, int64_t& sstable_size);

int get_sstable_trailer(const int fd, const int64_t trailer_offset, const sstable::ObSSTableTrailer& sstable_trailer);

int get_sstable_schema(const int fd, const sstable::ObSSTableTrailer& sstable_trailer, sstable::ObSSTableSchema& schema);

int get_block_index(const int fd, const sstable::ObSSTableTrailer& sstable_trailer, sstable::ObSSTableBlockIndexHeader& block_index_headero, sstable::ObSSTableBlockIndexItem*& block_index_item);

int get_block(const int fd, const PerfTestConfig& config, SrcSSTable& src_sstable);

int get_block2(const int fd, const PerfTestConfig& config, SrcSSTable& src_sstable, compactsstablev2::ObCompactSSTableWriter& writer);

int get_row(char* buf, int64_t size, int64_t block_id, SrcSSTable& src_sstable);

int get_row2(char* buf, int64_t size, int64_t block_id, SrcSSTable& src_sstable, common::ObRow& row, compactsstablev2::ObCompactSSTableWriter& writer);

int sstable_write_test(const int fd, const PerfTestConfig& config, SrcSSTable& src_sstable);

int sstable_scan_test(const int fd, const PerfTestConfig& config, SrcSSTable& src_sstable);

int make_schema(compactsstablev2::ObSSTableSchema& schema);

