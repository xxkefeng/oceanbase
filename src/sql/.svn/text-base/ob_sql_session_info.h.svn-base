/*
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_mysql_session_info.h is for what ...
 *
 * Version: ***: ob_sql_session_info.h  Mon Oct  8 10:30:53 2012 fangji.hcm Exp $
 *
 * Authors:
 *   Author fangji
 *   Email: fangji.hcm@taobao.com
 *     -some work detail if you want
 *
 */
#ifndef OB_SQL_SESSION_INFO_H_
#define OB_SQL_SESSION_INFO_H_

#include "common/ob_define.h"
#include "common/ob_atomic.h"
#include "common/hash/ob_hashmap.h"
#include "ob_result_set.h"
#include "WarningBuffer.h"
#include "common/ob_stack_allocator.h"
#include "common/ob_range.h"
#include "common/ob_list.h"
#include "common/page_arena.h"
#include "common/ob_pool.h"
#include "common/ob_pooled_allocator.h"
namespace oceanbase
{
  namespace sql
  {
    class ObSQLSessionInfo: public common::ObVersionProvider
    {
      public:
        static const int64_t APPROX_MEM_USAGE_PER_SESSION = 256*1024L; // 256KB ~= 4 * OB_COMMON_MEM_BLOCK_SIZE
        typedef common::ObPooledAllocator<common::hash::HashMapTypes<uint64_t, ObResultSet*>::AllocType, common::ObWrapperAllocator> IdPlanMapAllocer;
        typedef common::hash::ObHashMap<uint64_t,
                                        ObResultSet*,
                                        common::hash::NoPthreadDefendMode,
                                        common::hash::hash_func<uint64_t>,
                                        common::hash::equal_to<uint64_t>,
                                        IdPlanMapAllocer,
                                        common::hash::NormalPointer,
                                        common::ObSmallBlockAllocator<>
                                        > IdPlanMap;
        typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString, uint64_t>::AllocType, common::ObWrapperAllocator> NamePlanIdMapAllocer;
        typedef common::hash::ObHashMap<common::ObString,
                                        uint64_t,
                                        common::hash::NoPthreadDefendMode,
                                        common::hash::hash_func<common::ObString>,
                                        common::hash::equal_to<common::ObString>,
                                        NamePlanIdMapAllocer,
                                        common::hash::NormalPointer,
                                        common::ObSmallBlockAllocator<>
                                        > NamePlanIdMap;
        typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString, common::ObObj>::AllocType, common::ObWrapperAllocator> VarNameValMapAllocer;
        typedef common::hash::ObHashMap<common::ObString,
                                        common::ObObj,
                                        common::hash::NoPthreadDefendMode,
                                        common::hash::hash_func<common::ObString>,
                                        common::hash::equal_to<common::ObString>,
                                        VarNameValMapAllocer,
                                        common::hash::NormalPointer,
                                        common::ObSmallBlockAllocator<>
                                        > VarNameValMap;
        typedef common::ObPooledAllocator<common::hash::HashMapTypes<common::ObString, std::pair<common::ObObj*, common::ObObjType> >::AllocType, common::ObWrapperAllocator> SysVarNameValMapAllocer;
        typedef common::hash::ObHashMap<common::ObString,
                                        std::pair<common::ObObj*, common::ObObjType>,
                                        common::hash::NoPthreadDefendMode,
                                        common::hash::hash_func<common::ObString>,
                                        common::hash::equal_to<common::ObString>,
                                        SysVarNameValMapAllocer,
                                        common::hash::NormalPointer,
                                        common::ObSmallBlockAllocator<>
                                        > SysVarNameValMap;
      public:
        ObSQLSessionInfo();
        ~ObSQLSessionInfo();

        int init(common::DefaultBlockAllocator &block_allocator);
        void destroy();

        void set_session_id(uint64_t id){session_id_ = id;}
        void set_current_result_set(ObResultSet *cur_result_set){cur_result_set_ = cur_result_set;}
        const tbsys::WarningBuffer& get_warnings_buffer() const{return warnings_buf_;}
        uint64_t get_new_stmt_id(){return (common::atomic_inc(&next_stmt_id_));}
        IdPlanMap& get_id_plan_map(){return id_plan_map_;}
        SysVarNameValMap& get_sys_var_val_map(){return sys_var_val_map_;}
        ObResultSet* get_current_result_set(){return cur_result_set_;}
        const common::ObString& get_user_name(){return user_name_;}

        common::ObStringBuf& get_parser_mem_pool(){return parser_mem_pool_;}
        common::StackAllocator& get_transformer_mem_pool(){return transformer_mem_pool_;}
        common::ObArenaAllocator* get_transformer_mem_pool_for_ps();
        void free_transformer_mem_pool_for_ps(common::ObArenaAllocator* arena);

        int store_plan(const common::ObString& stmt_name, ObResultSet& result_set);
        int remove_plan(const uint64_t& stmt_id);
        int replace_variable(const common::ObString& var, const common::ObObj& val);
        int remove_variable(const common::ObString& var);
        int update_system_variable(const common::ObString& var, const common::ObObj& val);
        int load_system_variable(const common::ObString& name, const common::ObObj& type, const common::ObObj& value);
        int get_variable_value(const common::ObString& var, common::ObObj& val) const;
        int get_sys_variable_value(const common::ObString& var, common::ObObj& val) const;
        const common::ObObj* get_variable_value(const common::ObString& var) const;
        const common::ObObj* get_sys_variable_value(const common::ObString& var) const;
        bool variable_exists(const common::ObString& var);
        bool sys_variable_exists(const common::ObString& var);
        bool plan_exists(const common::ObString& stmt_name, uint64_t *stmt_id = NULL);
        ObResultSet* get_plan(const uint64_t& stmt_id) const;
        ObResultSet* get_plan(const common::ObString& stmt_name) const;
        int set_username(const common::ObString & user_name);
        void set_warnings_buf();
        int64_t to_string(char* buffer, const int64_t length) const;
        tbsys::CThreadMutex &get_mutex(){return mutex_;}
        void set_trans_id(const common::ObTransID &trans_id){trans_id_ = trans_id;};
        const common::ObTransID& get_trans_id(){return trans_id_;};
        void set_version_provider(const common::ObVersionProvider *version_provider){version_provider_ = version_provider;};
        const common::ObVersion get_frozen_version() const {return version_provider_->get_frozen_version();};
        /**
         * @pre 系统变量存在的情况下
         * @synopsis 根据变量名，取得这个变量的类型
         *
         * @param var_name
         *
         * @returns
         */
        common::ObObjType get_sys_variable_type(const common::ObString &var_name);
        void set_autocommit(bool autocommit) {is_autocommit_ = autocommit;};
        bool get_autocommit() const {return is_autocommit_;};
        // get system variable value
        bool is_create_sys_table_disabled() const;
      private:
        static const int64_t MAX_STORED_PLANS_COUNT = 10240;
        static const int64_t MAX_CACHED_ARENA_COUNT = 2;
        static const int64_t SMALL_BLOCK_SIZE = 4*1024LL;
      private:
        tbsys::CThreadMutex mutex_; // protect this session
        uint64_t session_id_;
        tbsys::WarningBuffer warnings_buf_;
        uint64_t user_id_;
        common::ObString user_name_;
        uint64_t next_stmt_id_;
        ObResultSet *cur_result_set_;
        common::ObTransID trans_id_;
        bool is_autocommit_;
        const common::ObVersionProvider *version_provider_;

        common::ObSmallBlockAllocator<> block_allocator_;

        common::ObStringBuf name_pool_; // for statement names and variables names
        common::ObStringBuf parser_mem_pool_; // reuse for each parsing
        common::StackAllocator transformer_mem_pool_; // for non-ps transformer

        IdPlanMapAllocer id_plan_map_allocer_;
        IdPlanMap id_plan_map_; // statement-id -> physical-plan
        NamePlanIdMapAllocer stmt_name_id_map_allocer_;
        NamePlanIdMap stmt_name_id_map_; // statement-name -> statement-id
        VarNameValMapAllocer var_name_val_map_allocer_;
        VarNameValMap var_name_val_map_; // user variables
        SysVarNameValMapAllocer sys_var_val_map_allocer_;
        SysVarNameValMap sys_var_val_map_; // system variables

        // PS related
        common::ObPool<common::ObWrapperAllocator> arena_pointers_;
        common::ObList<common::ObArenaAllocator *> free_arena_for_transformer_;
        common::ObPooledAllocator<ObResultSet, common::ObWrapperAllocator> result_set_pool_;
    };
  }
}

#endif
