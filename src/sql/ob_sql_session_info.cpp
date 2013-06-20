#include "ob_sql_session_info.h"
#include "common/ob_define.h"
#include "common/ob_mod_define.h"
#include "common/ob_obj_cast.h"
#include "common/ob_trace_log.h"
#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSQLSessionInfo::ObSQLSessionInfo()
  :session_id_(OB_INVALID_ID),
   user_id_(OB_INVALID_ID),
   next_stmt_id_(0),
   cur_result_set_(NULL),
   is_autocommit_(true),
   version_provider_(NULL),
   block_allocator_(SMALL_BLOCK_SIZE, common::OB_MALLOC_BLOCK_SIZE, ObMalloc(ObModIds::OB_SQL_SESSION_SBLOCK)),
   name_pool_(ObModIds::OB_SQL_SESSION, OB_COMMON_MEM_BLOCK_SIZE),
   parser_mem_pool_(ObModIds::OB_SQL_PARSER, OB_COMMON_MEM_BLOCK_SIZE),
   id_plan_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
   stmt_name_id_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  var_name_val_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  sys_var_val_map_allocer_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  arena_pointers_(sizeof(ObArenaAllocator), SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_)),
  result_set_pool_(SMALL_BLOCK_SIZE, ObWrapperAllocator(&block_allocator_))
{
}

ObSQLSessionInfo::~ObSQLSessionInfo()
{
  destroy();
}

int64_t ObSQLSessionInfo::to_string(char *buffer, const int64_t length) const
{
  int64_t size = 0;
  size += snprintf(buffer, length, "session_id=%ld", session_id_);
  return size;
}

int ObSQLSessionInfo::init(common::DefaultBlockAllocator &block_allocator)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = id_plan_map_.create(hash::cal_next_prime(128),
                                               &id_plan_map_allocer_,
                                               &block_allocator_)))
  {
    TBSYS_LOG(WARN, "init id-plan map failed, ret=%d", ret);
  }
  else if(OB_SUCCESS != (ret = stmt_name_id_map_.create(hash::cal_next_prime(16),
                                               &stmt_name_id_map_allocer_,
                                               &block_allocator_)))
  {
    TBSYS_LOG(WARN, "init name-id map failed, ret=%d", ret);
  }
  else if(OB_SUCCESS != (ret = var_name_val_map_.create(hash::cal_next_prime(16),
                                               &var_name_val_map_allocer_,
                                               &block_allocator_)))
  {
    TBSYS_LOG(WARN, "init var_value map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = sys_var_val_map_.create(hash::cal_next_prime(64),
                                               &sys_var_val_map_allocer_,
                                               &block_allocator_)))
  {
    TBSYS_LOG(WARN, "init sys_var_value map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = transformer_mem_pool_.init(&block_allocator, OB_COMMON_MEM_BLOCK_SIZE)))
  {
    TBSYS_LOG(WARN, "failed to init transformer mem pool, err=%d", ret);
  }
  else
  {
    block_allocator.set_mod_id(ObModIds::OB_SQL_TRANSFORMER);
  }
  return ret;
}

void ObSQLSessionInfo::destroy()
{
  IdPlanMap::iterator iter;
  for (iter = id_plan_map_.begin(); iter != id_plan_map_.end(); iter++)
  {
    ObResultSet* result_set = NULL;
    if (hash::HASH_EXIST != id_plan_map_.get(iter->first, result_set))
    {
      TBSYS_LOG(WARN, "result_set whose key=[%lu] not found", iter->first);
    }
    else
    {
      result_set_pool_.free(result_set);
      id_plan_map_.erase(iter->first);
    }
  }
  // free all cached page arena
  ObArenaAllocator* arena = NULL;
  while (0 == free_arena_for_transformer_.pop_front(arena))
  {
    arena->~ObArenaAllocator();
    arena_pointers_.free(arena);
    TBSYS_LOG(DEBUG, "destroy allocator, addr=%p", arena);
    OB_STAT_INC(SQL, SQL_PS_ALLOCATOR_COUNT, -1);
  }
}

int ObSQLSessionInfo::store_plan(const ObString& stmt_name, ObResultSet& result_set)
{
  int ret = OB_SUCCESS;
  uint64_t stmt_id = OB_INVALID_ID;
  ObString name;
  ObResultSet *new_res_set = NULL;
  if (id_plan_map_.size() >= MAX_STORED_PLANS_COUNT)
  {
    TBSYS_LOG(USER_ERROR, "too many prepared statements, the max allowed is %ld", MAX_STORED_PLANS_COUNT);
    ret = OB_ERR_TOO_MANY_PS;
  }
  else if ((ret = name_pool_.write_string(stmt_name, &name)) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "fail to save statement name in session name pool");
  }
  else if (name.length() > 0 && plan_exists(name, &stmt_id))
  {
    if (id_plan_map_.get(stmt_id, new_res_set) == hash::HASH_EXIST)
    {
      new_res_set->reset();
    }
    else
    {
      ret = OB_ERR_PREPARE_STMT_UNKNOWN;
      TBSYS_LOG(ERROR, "Can not find stored plan, id = %lu, name = %.*s",
          stmt_id, name.length(), name.ptr());
      stmt_name_id_map_.erase(stmt_name);
    }
  }
  else
  {
    stmt_id = get_new_stmt_id();
    if (name.length() > 0 && stmt_name_id_map_.set(name, stmt_id) != hash::HASH_INSERT_SUCC)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "fail to save statement <name, id> pair");
    }
    else if ((new_res_set = result_set_pool_.alloc()) == NULL)
    {
      TBSYS_LOG(ERROR, "ob malloc for ObResultSet failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (name.length() > 0)
        stmt_name_id_map_.erase(name);
    }
    // from stmt_prepare, there is no statement name
    else if (id_plan_map_.set(stmt_id, new_res_set) != hash::HASH_INSERT_SUCC)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "fail to save prepared plan");
      if (name.length() > 0)
        stmt_name_id_map_.erase(name);
      result_set_pool_.free(new_res_set);
    }
  }
  if (ret == OB_SUCCESS && new_res_set != NULL)
  {
    // params are just place holder, they will be filled when execute
    result_set.set_statement_id(stmt_id);
    result_set.set_statement_name(name);
    FILL_TRACE_LOG("stored_query_result=(%s)", to_cstring(result_set));
    if ((ret = result_set.to_prepare(*new_res_set)) == OB_SUCCESS)
    {
      TBSYS_LOG(DEBUG, "new_res_set=%p output_result=(%s) stored_result=(%s)",
                new_res_set, to_cstring(result_set), to_cstring(*new_res_set));
    }
    else
    {
      new_res_set->reset();
    }
  }
  return ret;
}


int ObSQLSessionInfo::remove_plan(const uint64_t& stmt_id)
{
  int ret = OB_SUCCESS;
  ObResultSet *result_set = NULL;
  if (id_plan_map_.get(stmt_id, result_set) != hash::HASH_EXIST)
  {
    ret = OB_ERR_PREPARE_STMT_UNKNOWN;
    TBSYS_LOG(WARN, "prepare statement id not found, statement id = %ld", stmt_id);
  }
  else
  {
    const ObString& stmt_name = result_set->get_statement_name();
    if (stmt_name.length() > 0 && stmt_name_id_map_.erase(stmt_name) != hash::HASH_EXIST)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "prepare statement name not fount, statement name = %.*s",
          stmt_name.length(), stmt_name.ptr());
    }
    if (ret == OB_SUCCESS && id_plan_map_.erase(stmt_id) != hash::HASH_EXIST)
    {
      ret = OB_ERROR;
      TBSYS_LOG(ERROR, "drop prepare statement error, statement id = %ld", stmt_id);
    }
    if (ret == OB_SUCCESS)
    {
      result_set_pool_.free(result_set); // will free the ps_transformer_allocator to the session
    }
  }
  return ret;
}

bool ObSQLSessionInfo::plan_exists(const ObString& stmt_name, uint64_t *stmt_id)
{
  uint64_t id = OB_INVALID_ID;
  return (stmt_name_id_map_.get(stmt_name, stmt_id ? *stmt_id : id) == hash::HASH_EXIST);
}

ObResultSet* ObSQLSessionInfo::get_plan(const uint64_t& stmt_id) const
{
  ObResultSet * result_set = NULL;
  if (id_plan_map_.get(stmt_id, result_set) != hash::HASH_EXIST)
    result_set = NULL;
  return result_set;
}

ObResultSet* ObSQLSessionInfo::get_plan(const ObString& stmt_name) const
{
  ObResultSet * result_set = NULL;
  uint64_t stmt_id = OB_INVALID_ID;
  if (stmt_name_id_map_.get(stmt_name, stmt_id) != hash::HASH_EXIST)
  {
    result_set = NULL;
  }
  else if (id_plan_map_.get(stmt_id, result_set) != hash::HASH_EXIST)
  {
    result_set = NULL;
  }
  return result_set;
}

int ObSQLSessionInfo::replace_variable(const ObString& var, const ObObj& val)
{
  int ret = OB_SUCCESS;
  ObString tmp_var;
  ObObj tmp_val;
  if (var.length() <= 0)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "Empty variable name");
  }
  else if ((ret = name_pool_.write_string(var, &tmp_var)) != OB_SUCCESS
    || (ret = name_pool_.write_obj(val, &tmp_val)) != OB_SUCCESS
    || ((ret = var_name_val_map_.set(tmp_var, tmp_val, 1)) != hash::HASH_INSERT_SUCC
        && ret != hash::HASH_OVERWRITE_SUCC))
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "Add variable %.*s error", var.length(), var.ptr());
  }
  else
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSQLSessionInfo::remove_variable(const ObString& var)
{
  int ret = OB_SUCCESS;
  if (var.length() <= 0)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "Empty variable name");
  }
  else if (var_name_val_map_.erase(var) != hash::HASH_EXIST)
  {
    ret = OB_ERR_VARIABLE_UNKNOWN;
    TBSYS_LOG(ERROR, "remove variable %.*s error", var.length(), var.ptr());
  }
  else
  {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSQLSessionInfo::update_system_variable(const ObString& var, const ObObj& val)
{
  int ret = OB_SUCCESS;
  ObObj *obj = NULL;
  std::pair<common::ObObj*, common::ObObjType> values;
  if (var.length() <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(ERROR, "Empty variable name");
  }
  else if (sys_var_val_map_.get(var, values) != hash::HASH_EXIST)
  {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "invalid system variable, name=%.*s", var.length(), var.ptr());
  }
  else
  {
    obj = values.first;
    if ((ret = name_pool_.write_obj(val, obj)) != OB_SUCCESS)
    {
      TBSYS_LOG(ERROR, "Update system variable %.*s error", var.length(), var.ptr());
    }
  }
  return ret;
}

int ObSQLSessionInfo::load_system_variable(const ObString& name, const ObObj& type, const ObObj& value)
{
  int ret = OB_SUCCESS;
  char var_buf[OB_MAX_VARCHAR_LENGTH];
  ObString var_str;
  var_str.assign_ptr(var_buf, OB_MAX_VARCHAR_LENGTH);
  ObObj casted_cell;
  casted_cell.set_varchar(var_str);
  const ObObj *res_cell = NULL;
  ObString tmp_name;
  ObObj *val_ptr = NULL;
  if (NULL == (val_ptr = (ObObj*)name_pool_.get_arena().alloc(sizeof(ObObj))))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "no memory");
  }
  else
  {
    val_ptr = new(val_ptr) ObObj();
  }
  if (OB_SUCCESS != ret)
  {
  }
  else if ((ret = name_pool_.write_string(name, &tmp_name)) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "Fail to store variable name, err=%d", ret);
  }
  else if ((ret = obj_cast(value, type, casted_cell, res_cell)) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "Cast variable value failed, err=%d", ret);
  }
  else if ((ret = name_pool_.write_obj(*res_cell, val_ptr)) != OB_SUCCESS)
  {
    TBSYS_LOG(ERROR, "Fail to store variable value, err=%d", ret);
  }
  else if (sys_var_val_map_.set(tmp_name, std::make_pair(val_ptr, type.get_type()), 0) != hash::HASH_INSERT_SUCC)
  {
    ret = OB_ERROR;
    TBSYS_LOG(ERROR, "Load system variable error, err=%d", ret);
  }
  else
  {
    TBSYS_LOG(INFO, "system variable %.*s=%s", name.length(), name.ptr(), to_cstring(*val_ptr));
  }
  return ret;
}

bool ObSQLSessionInfo::variable_exists(const ObString& var)
{
  ObObj val;
  return (var_name_val_map_.get(var, val) == hash::HASH_EXIST);
}

ObObjType ObSQLSessionInfo::get_sys_variable_type(const ObString &var_name)
{
  std::pair<common::ObObj*, common::ObObjType> val;
  int ret = sys_var_val_map_.get(var_name, val);
  OB_ASSERT(ret == hash::HASH_EXIST);
  return val.second;
}
bool ObSQLSessionInfo::sys_variable_exists(const ObString& var)
{
  std::pair<common::ObObj*, common::ObObjType> val;
  return (sys_var_val_map_.get(var, val) == hash::HASH_EXIST);
}

int ObSQLSessionInfo::get_variable_value(const ObString& var, ObObj& val) const
{
  int ret = OB_SUCCESS;
  if (var_name_val_map_.get(var, val) != hash::HASH_EXIST)
    ret = OB_ERR_VARIABLE_UNKNOWN;
  return ret;
}

const ObObj* ObSQLSessionInfo::get_variable_value(const ObString& var) const
{
  return var_name_val_map_.get(var);
}

int ObSQLSessionInfo::get_sys_variable_value(const ObString& var, ObObj& val) const
{
  int ret = OB_SUCCESS;
  std::pair<common::ObObj*, common::ObObjType> values;
  if (sys_var_val_map_.get(var, values) != hash::HASH_EXIST)
  {
    ret = OB_ERR_VARIABLE_UNKNOWN;
  }
  else
  {
    val = *(values.first);
  }
  return ret;
}

const ObObj* ObSQLSessionInfo::get_sys_variable_value(const ObString& var) const
{
  ObObj *obj = NULL;
  std::pair<common::ObObj*, common::ObObjType> values;
  if (sys_var_val_map_.get(var, values) != hash::HASH_EXIST)
  {
    // do nothing
  }
  else
  {
    obj = values.first;
    TBSYS_LOG(DEBUG, "sys_variable=%s name=%.*s addr=%p", to_cstring(*obj), var.length(), var.ptr(), obj);
  }
  return obj;
}

bool ObSQLSessionInfo::is_create_sys_table_disabled() const
{
  bool ret = true;
  ObObj val;
  if (OB_SUCCESS != get_sys_variable_value(ObString::make_string("ob_disable_create_sys_table"), val)
      || OB_SUCCESS != val.get_bool(ret))
  {
    ret = true;
  }
  return ret;
}

int ObSQLSessionInfo::set_username(const ObString & user_name)
{
  int ret = OB_SUCCESS;
  ret = name_pool_.write_string(user_name, &user_name_);
  if (OB_SUCCESS != ret)
  {
    TBSYS_LOG(WARN, "write username to string_buf_ failed,ret=%d", ret);
  }
  return ret;
}

void ObSQLSessionInfo::set_warnings_buf()
{
  tbsys::WarningBuffer *warnings_buf = NULL;
  if ((warnings_buf = tbsys::get_tsi_warning_buffer()) == NULL)
  {
    TBSYS_LOG(WARN, "can not get thread warnings buffer");
    warnings_buf_.reset();
  }
  else if (cur_result_set_ && cur_result_set_->is_show_warnings()
    && warnings_buf->get_readable_warning_count() == 0)
  {
    /* Successful "show warning" statement, skip */
  }
  else
  {
    warnings_buf_ = *warnings_buf;
    warnings_buf->reset();
  }
}

ObArenaAllocator* ObSQLSessionInfo::get_transformer_mem_pool_for_ps()
{
  ObArenaAllocator* ret = NULL;
  if (0 != free_arena_for_transformer_.pop_front(ret))
  {
    // no free arena, new one
    void *ptr = arena_pointers_.alloc();
    if (NULL == ptr)
    {
      TBSYS_LOG(WARN, "no memory");
    }
    else
    {
      ret = new(ptr) ObArenaAllocator(ObModIds::OB_SQL_PS_TRANS);
      TBSYS_LOG(DEBUG, "new allocator, addr=%p session_id=%ld", ret, session_id_);
      OB_STAT_INC(SQL, SQL_PS_ALLOCATOR_COUNT);
    }
  }
  else
  {
    ret->reuse();
    TBSYS_LOG(DEBUG, "reuse allocator, addr=%p session_id=%ld", ret, session_id_);
  }
  return  ret;
}

void ObSQLSessionInfo::free_transformer_mem_pool_for_ps(ObArenaAllocator* arena)
{
  if (OB_LIKELY(NULL != arena))
  {
    if (free_arena_for_transformer_.size() >= MAX_CACHED_ARENA_COUNT)
    {
      arena->~ObArenaAllocator();
      arena_pointers_.free(arena);
      TBSYS_LOG(DEBUG, "destroy allocator, addr=%p", arena);
      OB_STAT_INC(SQL, SQL_PS_ALLOCATOR_COUNT, -1);
    }
    else if (0 != free_arena_for_transformer_.push_front(arena))
    {
      arena->~ObArenaAllocator();
      arena_pointers_.free(arena);
      TBSYS_LOG(DEBUG, "destroy allocator, addr=%p", arena);
      OB_STAT_INC(SQL, SQL_PS_ALLOCATOR_COUNT, -1);
    }
    else
    {
      TBSYS_LOG(DEBUG, "free allocator, addr=%p session_id=%ld", arena, session_id_);
    }
  }
}
