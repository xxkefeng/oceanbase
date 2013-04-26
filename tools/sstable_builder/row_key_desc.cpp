#include "tblog.h"
#include "common/ob_define.h"
#include "common/utility.h"
#include "row_key_desc.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace chunkserver
  {
    RowKeyDesc::RowKeyDesc():item_num_(0),size_(0),inited_(false){}

    RowKeyDesc::~RowKeyDesc() {}

    int RowKeyDesc::to_string(char *buf, int32_t buf_len) const
    {
      int ret = OB_SUCCESS;
      if (!inited_)
      {
        TBSYS_LOG(INFO, "rowkey desc is not inited");
        ret = OB_NOT_INIT;
      }
      else
      {
        int32_t pos = 0;
        for(int32_t i=0; i < item_num_; ++i)
        {
          int count = 0;
          if (0 != i)
          {
            if (buf_len - pos <= 0)
            {
              TBSYS_LOG(ERROR, "buf not enough");
              ret = OB_BUF_NOT_ENOUGH;
              break;
            }
            buf[pos++] = ',';
          }

          const RowKeyItem* item = items_ + i;
          if(item->flag_ == 1)
          {
            count = snprintf(buf+pos, buf_len - pos,"%d-%d-%d-%d",
                item->index_, item->type_, item->size_, item->flag_);
          }
          else
          {
            count = snprintf(buf+pos, buf_len - pos, "%d-%d-%d",
                item->index_, item->type_, item->size_);
          }

          if (count <= 0 || count >= buf_len - pos)
          {
            TBSYS_LOG(ERROR, "failed snprintf, buf pos = %d, buf len = %d", pos, buf_len);
            ret = OB_BUF_NOT_ENOUGH;
            break;
          }
          pos += count;
        }
        buf[buf_len-1] = '\0';
      }
      return ret;
    }

    int RowKeyDesc::check(const char* desc) const
    {
      char buf[MAX_ROW_KEY_DESC_STRING_LENGTH];
      int ret = to_string(buf, MAX_ROW_KEY_DESC_STRING_LENGTH);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(ERROR, "failed to call to_string of rowkey_desc, ret=%d", ret);
      }
      else if (0 != strcmp(desc, buf))
      {
        TBSYS_LOG(ERROR, "rebuild desc[%s] is not as the same as source desc[%s]",
            buf, desc);
        ret = OB_ERROR;
      }
      return ret;
    }

    int RowKeyDesc::parse(const char *desc, int32_t need_check)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }

      char *row_key_desc = strdup(desc);
      char *token = NULL;

      int32_t fields[MAX_ROW_KEY_ITEM_FAILEDS];
      int32_t row_key_item_num = 0;

      while(OB_SUCCESS == ret && (token = strsep(&row_key_desc, ",")) != NULL)
      {
        int32_t count = MAX_ROW_KEY_ITEM_FAILEDS;
        int32_t flag = 0;
        ret = parse_string_to_int_array(token, '-', fields, count);
        if (OB_SUCCESS != ret || (count != 3 && count != 4))
        {
          TBSYS_LOG(ERROR, "failed to parse row_key_desc line %s, count=%d ret=%d",
              token, count, ret);
          if (OB_SUCCESS == ret)
          {
            ret = OB_INVALID_ARGUMENT;
          }
          break;
        }
        // check type
        if (fields[1] < 0 || fields[1] > MAX_ROW_KEY_TYPE)
        {
          TBSYS_LOG(ERROR, "failed to parse row_key_desc line %s, "
              "no proper type found for the 2th fields", token);
          ret = OB_INVALID_ARGUMENT;
          break;
        }

        // check flag
        if (count == 4)
        {
          if (fields[3] == 1)
          {
            flag = 1;
          }
          else
          {
            TBSYS_LOG(ERROR, "failed to parse row_key_desc line %s, "
                "the 4th fields should be 1 or not exist", token);
            ret = OB_INVALID_ARGUMENT;
            break;
          }
        }

        RowKeyItem* item = items_ + row_key_item_num;
        item->index_ = fields[0];
        item->type_ = RowKeyType(fields[1]);
        item->size_ = fields[2];
        item->flag_ = flag;
        size_ += item->size_;

        ++row_key_item_num;
      }

      if (OB_SUCCESS == ret)
      {
        item_num_ = row_key_item_num;
        inited_ = true;
      }

      if (OB_SUCCESS == ret && need_check == 1)
      {
        ret = check(desc);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "check internal failed");
          inited_ = false;
        }
      }

      if (NULL == row_key_desc)
      {
        free(row_key_desc);
      }

      return ret;
    }

  }
}
