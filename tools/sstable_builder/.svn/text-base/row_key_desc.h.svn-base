#ifndef OCEANBASE_CHUNKSERVER_ROW_KEY_DESC_H_
#define OCEANBASE_CHUNKSERVER_ROW_KEY_DESC_H_

#include<iostream>
#include<vector>
#include<stdint.h>
namespace oceanbase
{
  namespace chunkserver
  {
    const int32_t MAX_ROW_KEY_ITEM_FAILEDS = 4;
    const int32_t MAX_ROW_KEY_DESC_STRING_LENGTH = 1024;

    const int32_t MAX_ROW_KEY_TYPE = 5;
    enum RowKeyType
    {
      INT8 = 0,
      INT16 = 1,
      INT32 = 2,
      INT64 = 3,
      VARCHAR = 4,
      DATETIME = 5,
    };

    struct RowKeyItem
    {
      int32_t index_; // index in raw data
      RowKeyType type_;
      int32_t size_; // size of rowkey item
      int32_t flag_; // 0 fixed length varchar; 1 variable length varchar
    };

    class RowKeyDesc
    {
      public:
        RowKeyDesc();
        ~RowKeyDesc();
        int parse(const char *desc, const int32_t need_check = 1);
        int to_string(char *buf, int32_t buf_len) const;

        inline bool is_inited() const {return inited_;}
        inline int32_t get_size() const {return size_;}
        inline int32_t get_item_num() const {return item_num_;}
        inline const RowKeyItem* get_item(int32_t i) const;

      private:
        int check(const char *desc) const;

      private:
        RowKeyItem items_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
        int32_t item_num_;
        int32_t size_; // max size of rowkey
        bool inited_;
        DISALLOW_COPY_AND_ASSIGN(RowKeyDesc);
    };

    inline const RowKeyItem* RowKeyDesc::get_item(int32_t i) const
    {
      const RowKeyItem* item = NULL;
      if (i >= 0 && i < item_num_)
      {
        item = items_ + i;
      }
      else
      {
        TBSYS_LOG(ERROR, "row key desc index cannot be %d, item num %d", i, item_num_);
      }
      return item;
    }
  }
}
#endif /* OCEANBASE_CHUNKSERVER_ROW_KEY_DESC_H_ */
