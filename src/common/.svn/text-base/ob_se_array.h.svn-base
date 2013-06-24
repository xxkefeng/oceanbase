/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_se_array.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SE_ARRAY_H
#define _OB_SE_ARRAY_H 1
#include "ob_array.h"
#include "page_arena.h"         // for ModulePageAllocator
namespace oceanbase
{
  namespace common
  {
    template <typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT = ModulePageAllocator>
    class ObSEArray
    {
      public:
        ObSEArray(int64_t block_size = 64*1024, const BlockAllocatorT &alloc = BlockAllocatorT(ObModIds::OB_SE_ARRAY));
        virtual ~ObSEArray();

        // deep copy
        ObSEArray(const ObSEArray &other);
        ObSEArray& operator=(const ObSEArray &other);

        int push_back(const T &obj);
        void pop_back();
        int pop_back(T &obj);

        int at(int64_t idx, T &obj) const;
        T& at(int64_t idx);     // dangerous
        const T& at(int64_t idx) const; // dangerous
        const T& operator[] (int64_t idx) const; // dangerous

        int64_t count() const;
        void clear();
        void reserve(int64_t capacity);
        int64_t to_string(char* buffer, int64_t length) const;
      private:
        // types and constants
      private:
        // function members
        int move_from_local();
        void move_to_local();
      private:
        // data members
        T *ptr_;                // optimize for read
        T local_data_[LOCAL_ARRAY_SIZE];
        int64_t count_;
        ObArray<T, BlockAllocatorT> array_;
    };

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::ObSEArray(int64_t block_size, const BlockAllocatorT &alloc)
      :ptr_(local_data_),
       count_(0),
       array_(block_size, alloc)
    {
      OB_ASSERT(LOCAL_ARRAY_SIZE > 0);
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::~ObSEArray()
    {
      clear();
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::move_from_local()
    {
      int ret = OB_SUCCESS;
      OB_ASSERT(count_ == LOCAL_ARRAY_SIZE);
      OB_ASSERT(0 == array_.count());
      array_.reserve(count_);
      for (int64_t i = 0; i < count_; ++i)
      {
        if (OB_SUCCESS != (ret = array_.push_back(local_data_[i])))
        {
          break;
        }
      }
      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        ptr_ = &array_.at(0);
      }
      return ret;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::push_back(const T &obj)
    {
      int ret = OB_SUCCESS;
      if (count_ < LOCAL_ARRAY_SIZE)
      {
        local_data_[count_++] = obj;
      }
      else
      {
        if (OB_UNLIKELY(count_ == LOCAL_ARRAY_SIZE))
        {
          ret = move_from_local();
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          ret = array_.push_back(obj);
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          ptr_ = &array_.at(0);
          ++count_;
        }
      }
      return ret;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::move_to_local()
    {
      OB_ASSERT(array_.count() == LOCAL_ARRAY_SIZE);
      for (int64_t i = 0; i < LOCAL_ARRAY_SIZE; ++i)
      {
        local_data_[i] = array_.at(i);
      }
      ptr_ = local_data_;
      array_.clear();
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::pop_back()
    {
      if (OB_UNLIKELY(count_ <= 0))
      {
      }
      else if (count_ <= LOCAL_ARRAY_SIZE)
      {
        --count_;
      }
      else
      {
        array_.pop_back();
        if (array_.count() == LOCAL_ARRAY_SIZE)
        {
          move_to_local();
        }
        --count_;
      }
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::pop_back(T &obj)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(count_ <= 0))
      {
        ret = OB_ENTRY_NOT_EXIST;
      }
      else if (count_ <= LOCAL_ARRAY_SIZE)
      {
        obj = local_data_[--count_];
      }
      else
      {
        ret = array_.pop_back(obj);
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          if (array_.count() == LOCAL_ARRAY_SIZE)
          {
            move_to_local();
          }
          --count_;
        }
      }
      return ret;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    int ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::at(int64_t idx, T &obj) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(0 > idx || idx >= count_))
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else
      {
        obj = ptr_[idx];
      }
      return ret;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    T& ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::at(int64_t idx)
    {
      OB_ASSERT(0 <= idx && idx < count_);
      return ptr_[idx];
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    const T& ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::at(int64_t idx) const
    {
      OB_ASSERT(0 <= idx && idx < count_);
      return ptr_[idx];
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    const T& ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::operator[] (int64_t idx) const
    {
      OB_ASSERT(0 <= idx && idx < count_);
      return ptr_[idx];
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    int64_t ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::count() const
    {
      return count_;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::clear()
    {
      if (count_ > LOCAL_ARRAY_SIZE)
      {
        array_.clear();
        ptr_ = local_data_;
      }
      count_ = 0;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    void ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::reserve(int64_t capacity)
    {
      if (capacity > LOCAL_ARRAY_SIZE)
      {
        array_.reserve(capacity);
      }
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    int64_t ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::to_string(char* buffer, int64_t length) const
    {
      int64_t pos = 0;
      for (int64_t i = 0; i < count_; ++i)
      {
        databuff_printf(buffer, length, pos, "<%ld:%s> ", i, to_cstring(ptr_[i]));
      }
      return pos;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>& ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::operator=(const ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT> &other)
    {
      if (OB_LIKELY(this != &other))
      {
        this->clear();
        if (other.count_ <= LOCAL_ARRAY_SIZE)
        {
          for (int64_t i = 0; i < other.count_; ++i)
          {
            local_data_[i] = other.local_data_[i];
          }
        }
        else
        {
          this->array_ = other.array_;
          ptr_ = &array_.at(0);
        }
        this->count_ = other.count_;
      }
      return *this;
    }

    template<typename T, int64_t LOCAL_ARRAY_SIZE, typename BlockAllocatorT>
    ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT>::ObSEArray(const ObSEArray<T, LOCAL_ARRAY_SIZE, BlockAllocatorT> &other)
      :ptr_(local_data_),
       count_(0),
       array_(other.array_.block_size_, other.array_.block_allocator_)
    {
      *this = other;
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_SE_ARRAY_H */
