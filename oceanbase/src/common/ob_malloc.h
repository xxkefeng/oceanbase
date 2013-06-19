/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_malloc.h is for what ...
 *
 * Version: $id: ob_malloc.h,v 0.1 8/19/2010 9:57a wushi Exp $
 *
 * Authors:
 *   wushi <wushi.ly@taobao.com>
 *   为了便于调试内存错误的bug，如果定义了名为__OB_MALLOC_DIRECT__的环境变量，
 *   内存池将直接调用new和delete分配和释放内存
 *
 */
#ifndef OCEANBASE_COMMON_OB_MALLOC_H_
#define OCEANBASE_COMMON_OB_MALLOC_H_
#include <stdint.h>
#include "ob_define.h"
#include "ob_mod_define.h"
namespace oceanbase
{
  namespace common
  {
    /// @fn int oceanbase/common::ob_init_memory_pool(int64_t block_size)
    ///   初始化全局的内存池，在调用ob_malloc分配内存前必须调用该函数进行初始化
    ///
    /// @param block_size 每次从系统分配的内存块的大小
    int ob_init_memory_pool(int64_t block_size = OB_MALLOC_BLOCK_SIZE);
    void ob_mod_usage_update(const int64_t delta, const int32_t mod_id);
    /// @fn 从全局内存池中分配nbyte的缓冲区
    void *ob_malloc(void *ptr, size_t size); //alloc mem for libeasy easy_pool
    void *ob_malloc(const int64_t nbyte, const int32_t mod_id = 0, int64_t *got_size = NULL);
    void *ob_malloc_emergency(const int64_t nbyte, const int32_t mod_id = 0, int64_t *got_size = NULL);

    /// @fn 释放通过ob_malloc获取的内存
    void ob_free(void *ptr, const int32_t mod_id = 0);
    void ob_safe_free(void *&ptr, const int32_t mod_id = 0);


    /// @fn print memory usage of each module
    void ob_print_mod_memory_usage(bool print_to_std = false);
    int64_t ob_get_mod_memory_usage(int32_t mod_id);

    int64_t ob_get_memory_size_direct_allocated();

    int64_t ob_set_memory_size_limit(const int64_t mem_size_limit);
    int64_t ob_get_memory_size_limit();
    int64_t ob_get_memory_size_handled();


    /// @class  handle buffer allocated form ObBaseMemPool
    /// @author wushi(wushi.ly@taobao.com)  (8/16/2010)
    class ObMemBuffer
    {
    private:
      DISALLOW_COPY_AND_ASSIGN(ObMemBuffer);
    public:
      /// @fn default constructor
      ObMemBuffer();
      /// @fn allocate memory from given mempool
      explicit ObMemBuffer(const int64_t nbyte);
      /// @fn free memory allocated from given mempool
      virtual ~ObMemBuffer();
      /// @fn 重新分配内存
      void *malloc(const int64_t nbyte, const int32_t mod_id = 0);
      /// @fn return memory allocated
      void *get_buffer();
      /// @fn get buffer size
      int64_t get_buffer_size();

    private:
      /// @property memory ptr pointing to memory allocated from memory
      void *buf_ptr_;
      int64_t buf_size_;
      int32_t mod_id_;
    };

    class ObMemBuf
    {
    public:
      ObMemBuf() : buf_ptr_(NULL), buf_size_(DEFAULT_MEMBUF_SIZE)
      {

      }

      explicit ObMemBuf(const int64_t default_size)
      : buf_ptr_(NULL), buf_size_(default_size)
      {

      }

      ~ObMemBuf()
      {
        if (NULL != buf_ptr_)
        {
          ob_free(buf_ptr_, mod_id_);
          buf_ptr_ = NULL;
        }
      }

      inline char* get_buffer()
      {
        return buf_ptr_;
      }

      int64_t get_buffer_size() const
      {
        return buf_size_;
      }

      int ensure_space(const int64_t size, const int32_t mod_id = 0);

    private:
      static const int64_t DEFAULT_MEMBUF_SIZE = 1024 * 1024;  //1M

    private:
      char* buf_ptr_;
      int64_t buf_size_;
      int32_t mod_id_;
    };

    class ObMemBufAllocatorWrapper
    {
      public:
        ObMemBufAllocatorWrapper(ObMemBuf& mem_buf) : mem_buf_(mem_buf) {}
      public:
        inline char* alloc(int64_t sz)
        {
          char* ptr = NULL;
          if (OB_SUCCESS == mem_buf_.ensure_space(sz))
          {
            ptr = mem_buf_.get_buffer();
          }
          return ptr;
        }
        inline void free(char* ptr)
        {
          UNUSED(ptr);
        }
      private:
        ObMemBuf& mem_buf_;
    };


    class ObRawBufAllocatorWrapper
    {
      public:
        ObRawBufAllocatorWrapper(char *mem_buf, int64_t mem_buf_len) : mem_buf_(mem_buf),mem_buf_len_(mem_buf_len) {}
      public:
        inline char* alloc(int64_t sz)
        {
          char* ptr = NULL;
          if (mem_buf_len_ >= sz)
          {
            ptr = mem_buf_;
          }
          return ptr;
        }
        inline void free(char* ptr)
        {
          UNUSED(ptr);
        }
      private:
        char * mem_buf_;
        int64_t mem_buf_len_;
    };
  }
}

#define OB_NEW(T, mod_id, ...)                  \
  ({                                            \
  T* ret = NULL;                                \
  void *buf = ob_malloc(sizeof(T), mod_id);     \
  if (NULL != buf)                              \
  {                                             \
    ret = new(buf) T(__VA_ARGS__);              \
  }                                             \
  ret;                                          \
  })

#define OB_DELETE(T, mod_id, ptr)               \
  do{                                           \
    if (NULL != ptr)                            \
    {                                           \
      ptr->~T();                                \
      ob_free(ptr, mod_id);                     \
      ptr = NULL;                               \
    }                                           \
  } while(0)

#endif /* OCEANBASE_SRC_COMMON_OB_MALLOC_H_ */
