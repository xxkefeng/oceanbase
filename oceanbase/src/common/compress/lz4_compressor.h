#ifndef OCEANBASE_COMMON_COMPRESS_LZ4_COMPRESSOR_H_
#define OCEANBASE_COMMON_COMPRESS_LZ4_COMPRESSOR_H_

#include "ob_compressor.h"
#include "lz4.h"
#include <new>

class LZ4Compressor : public ObCompressor
{
public:
  int compress(const char* src_buffer,
      const int64_t src_data_size,
      char* dst_buffer,
      const int64_t dst_buffer_size,
      int64_t &dst_data_size);

  int decompress(const char* src_buffer,
      const int64_t src_data_size,
      char* dst_buffer,
      const int64_t dst_buffer_size,
      int64_t &dst_data_size);

  const char* get_compressor_name() const;

  int64_t get_max_overflow_size(const int64_t src_data_size) const;

private:
  int trans_errno_(int lzo_errno);
};

extern "C" ObCompressor *create();
extern "C" void destroy(ObCompressor *lz4);

#endif
