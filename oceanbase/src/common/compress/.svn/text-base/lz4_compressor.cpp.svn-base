#include "lz4_compressor.h"
#include <stdio.h>

#define COMPRESSOR_NAME "lz4_1.0"
#define COMPRESS_METHOD LZ4_compress
#define DECOMPRESS_METHOD LZ4_uncompress

int LZ4Compressor::trans_errno_(int lzo_errno)
{
  (void) lzo_errno;
  return 0;
}

int LZ4Compressor::compress(const char *src_buffer,
    const int64_t src_data_size,
    char *dst_buffer,
    const int64_t dst_buffer_size,
    int64_t &dst_data_size)
{
  int ret = COM_E_NOERROR;

  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size)
  {
    ret = COM_E_INVALID_PARAM;
  }
  else if ((src_data_size + get_max_overflow_size(src_data_size)) > dst_buffer_size)
  {
    ret = COM_E_OVERFLOW;
  }
  else if (0 >= (dst_data_size = COMPRESS_METHOD(src_buffer, dst_buffer, static_cast<int32_t>(src_data_size))))
  {
    ret = COM_E_DATAERROR;
  }

  return ret;
}

int LZ4Compressor::decompress(const char *src_buffer,
    const int64_t src_data_size,
    char *dst_buffer,
    const int64_t dst_buffer_size,
    int64_t &dst_data_size)
{
  int ret = COM_E_NOERROR;

  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size)
  {
    ret = COM_E_INVALID_PARAM;
  }
  else if (0 > (dst_data_size = DECOMPRESS_METHOD(src_buffer, dst_buffer, static_cast<int32_t>(dst_buffer_size))))
  {
    ret = COM_E_DATAERROR;
  }

  return ret;
}

int64_t LZ4Compressor::get_max_overflow_size(const int64_t src_data_size) const
{
  return LZ4_compressBound(static_cast<int32_t>(src_data_size));
}

const char *LZ4Compressor::get_compressor_name() const
{
  return COMPRESSOR_NAME;
}

ObCompressor *create()
{
  return(new(std::nothrow) LZ4Compressor());
}

void destroy(ObCompressor *lz4)
{
  if (NULL != lz4)
  {
    delete lz4;
    lz4 = NULL;
  }
}
