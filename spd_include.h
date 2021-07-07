/* Copyright (C) 2008-2009 Kentoku Shiba

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#define spider_set_bit(BITMAP, BIT) \
  ((BITMAP)[(BIT) / 8] |= (1 << ((BIT) & 7)))
#define spider_bit_is_set(BITMAP, BIT) \
  (uint) ((BITMAP)[(BIT) / 8] & (1 << ((BIT) & 7)))

/* alter table */
typedef struct st_spider_alter_table
{
  bool               now_create;
  char               *table_name;
  uint               table_name_length;
  char               *tmp_char;
  longlong           tmp_priority;

  char               *tmp_server_name;
  char               *tmp_tgt_table_name;
  char               *tmp_tgt_db;
  char               *tmp_tgt_host;
  char               *tmp_tgt_username;
  char               *tmp_tgt_password;
  char               *tmp_tgt_socket;
  char               *tmp_tgt_wrapper;
  long               tmp_tgt_port;

  uint               tmp_server_name_length;
  uint               tmp_tgt_table_name_length;
  uint               tmp_tgt_db_length;
  uint               tmp_tgt_host_length;
  uint               tmp_tgt_username_length;
  uint               tmp_tgt_password_length;
  uint               tmp_tgt_socket_length;
  uint               tmp_tgt_wrapper_length;
} SPIDER_ALTER_TABLE;

/* database connection */
typedef struct st_spider_conn
{
  char               *conn_key;
  uint               conn_key_length;
  SPIDER_DB_CONN     *db_conn;
  pthread_mutex_t    mta_conn_mutex;
  volatile bool      mta_conn_mutex_unlock_later;
  uint               join_trx;
  int                trx_isolation;
  bool               semi_trx_isolation_chk;
  int                semi_trx_isolation;
  bool               semi_trx_chk;
  bool               semi_trx;
  bool               trx_start;
  bool               table_locked;
  int                table_lock;
  bool               disable_xa;
  bool               disable_reconnect;
  int                autocommit;
  int                sql_log_off;
  THD                *thd;
  HASH               lock_table_hash;
  void               *another_ha_first;
  void               *another_ha_last;
  st_spider_conn     *p_small;
  st_spider_conn     *p_big;
  st_spider_conn     *c_small;
  st_spider_conn     *c_big;
  longlong           priority;
  bool               server_lost;
  bool               ignore_dup_key;
  char               *error_str;
  int                error_length;
  time_t             ping_time;
  CHARSET_INFO       *access_charset;

  char               *tgt_host;
  char               *tgt_username;
  char               *tgt_password;
  char               *tgt_socket;
  char               *tgt_wrapper;
  long               tgt_port;

  uint               tgt_host_length;
  uint               tgt_username_length;
  uint               tgt_password_length;
  uint               tgt_socket_length;
  uint               tgt_wrapper_length;

#ifndef WITHOUT_SPIDER_BG_SEARCH
  volatile
#endif
    void             *quick_target;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  volatile bool      bg_init;
  volatile bool      bg_break;
  volatile bool      bg_kill;
  volatile bool      bg_caller_wait;
  volatile bool      bg_caller_sync_wait;
  volatile bool      bg_search;
  volatile bool      bg_direct_sql;
  THD                *bg_thd;
  pthread_t          bg_thread;
  pthread_cond_t     bg_conn_cond;
  pthread_mutex_t    bg_conn_mutex;
  pthread_cond_t     bg_conn_sync_cond;
  pthread_mutex_t    bg_conn_sync_mutex;
  volatile void      *bg_target;
#endif
} SPIDER_CONN;

#ifdef WITH_PARTITION_STORAGE_ENGINE
typedef struct st_spider_patition_share
{
  char               *table_name;
  uint               table_name_length;
  uint               use_count;
  pthread_mutex_t    sts_mutex;
  pthread_mutex_t    crd_mutex;

  volatile bool      sts_init;
  volatile bool      crd_init;
  volatile time_t    sts_get_time;
  volatile time_t    crd_get_time;
  ulonglong          data_file_length;
  ulonglong          max_data_file_length;
  ulonglong          index_file_length;
  ulonglong          auto_increment_value;
  ha_rows            records;
  ulong              mean_rec_length;
  time_t             check_time;
  time_t             create_time;
  time_t             update_time;

  longlong           *cardinality;
} SPIDER_PARTITION_SHARE;
#endif

typedef struct st_spider_transaction
{
  bool               trx_start;
  bool               trx_xa;
  bool               trx_consistent_snapshot;

  bool               use_consistent_snapshot;
  bool               internal_xa;
  uint               internal_xa_snapshot;

  query_id_t         query_id;
  bool               tmp_flg;

  THD                *thd;
  XID                xid;
  HASH               trx_conn_hash;
  HASH               trx_another_conn_hash;
  HASH               trx_alter_table_hash;
  XID_STATE          internal_xid_state;
  SPIDER_CONN        *join_trx_top;
  ulonglong          spider_thread_id;
  ulonglong          trx_conn_adjustment;
  uint               locked_connections;
  pthread_mutex_t    *udf_table_mutexes;
  CHARSET_INFO       *udf_access_charset;
  String             *udf_set_names;
} SPIDER_TRX;

typedef struct st_spider_share
{
  char               *table_name;
  uint               table_name_length;
  uint               use_count;
  pthread_mutex_t    mutex;
  pthread_mutex_t    sts_mutex;
  pthread_mutex_t    crd_mutex;
  pthread_mutex_t    auto_increment_mutex;
  THR_LOCK           lock;
  TABLE_SHARE        *table_share;

  volatile bool      init;
  volatile bool      init_error;
  volatile time_t    init_error_time;
  volatile bool      sts_init;
  volatile time_t    sts_get_time;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  volatile time_t    bg_sts_try_time;
  volatile double    bg_sts_interval;
  volatile int       bg_sts_mode;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  volatile int       bg_sts_sync;
#endif
  volatile bool      bg_sts_init;
  volatile bool      bg_sts_kill;
  volatile bool      bg_sts_thd_wait;
  THD                *bg_sts_thd;
  pthread_t          bg_sts_thread;
  pthread_cond_t     bg_sts_cond;
  volatile bool      crd_init;
#endif
  volatile time_t    crd_get_time;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  volatile time_t    bg_crd_try_time;
  volatile double    bg_crd_interval;
  volatile int       bg_crd_mode;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  volatile int       bg_crd_sync;
#endif
  volatile bool      bg_crd_init;
  volatile bool      bg_crd_kill;
  volatile bool      bg_crd_thd_wait;
  THD                *bg_crd_thd;
  pthread_t          bg_crd_thread;
  pthread_cond_t     bg_crd_cond;
#endif
  ulonglong          data_file_length;
  ulonglong          max_data_file_length;
  ulonglong          index_file_length;
  volatile bool      auto_increment_init;
  volatile ulonglong auto_increment_lclval;
  ulonglong          auto_increment_value;
  ha_rows            records;
  ulong              mean_rec_length;
  time_t             check_time;
  time_t             create_time;
  time_t             update_time;

  int                bitmap_size;
  String             *table_select;
  String             *key_select;
  String             *key_hint;
  String             *show_table_status;
  String             *show_index;
  String             *set_names;
  String             *table_name_str;
  String             *db_name_str;
  String             *column_name_str;
  CHARSET_INFO       *access_charset;
  longlong           *cardinality;
  uchar              *cardinality_upd;
  longlong           additional_table_flags;

#ifndef WITHOUT_SPIDER_BG_SEARCH
  int                sts_bg_mode;
#endif
  double             sts_interval;
  int                sts_mode;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  int                sts_sync;
#endif
#ifndef WITHOUT_SPIDER_BG_SEARCH
  int                crd_bg_mode;
#endif
  double             crd_interval;
  int                crd_mode;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  int                crd_sync;
#endif
  int                crd_type;
  double             crd_weight;
  longlong           internal_offset;
  longlong           internal_limit;
  longlong           split_read;
  int                init_sql_alloc_size;
  int                reset_sql_alloc;
  int                multi_split_read;
  int                max_order;
  int                semi_table_lock;
  int                semi_table_lock_conn;
  int                selupd_lock_mode;
  int                query_cache;
  int                internal_delayed;
  int                bulk_size;
  int                internal_optimize;
  int                internal_optimize_local;
  double             scan_rate;
  double             read_rate;
  longlong           priority;
  int                net_timeout;
  int                quick_mode;
  longlong           quick_page_size;
  int                low_mem_read;
  int                table_count_mode;
  int                select_column_mode;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  int                bgs_mode;
  longlong           bgs_first_read;
  longlong           bgs_second_read;
#endif
  int                auto_increment_mode;
  int                use_table_charset;
  int                use_pushdown_udf;
  int                direct_dup_insert;

  char               *server_name;
  char               *tgt_table_name;
  char               *tgt_db;
  char               *tgt_host;
  char               *tgt_username;
  char               *tgt_password;
  char               *tgt_socket;
  char               *tgt_wrapper;
  char               *conn_key;
  long               tgt_port;

  uint               server_name_length;
  uint               tgt_table_name_length;
  uint               tgt_db_length;
  uint               tgt_host_length;
  uint               tgt_username_length;
  uint               tgt_password_length;
  uint               tgt_socket_length;
  uint               tgt_wrapper_length;
  uint               conn_key_length;

  SPIDER_ALTER_TABLE alter_table;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  SPIDER_PARTITION_SHARE *partition_share;
#endif
} SPIDER_SHARE;

typedef struct st_spider_init_error_table
{
  char               *table_name;
  uint               table_name_length;
  bool               init_error_with_message;
  char               init_error_msg[MYSQL_ERRMSG_SIZE];
  volatile int       init_error;
  volatile time_t    init_error_time;
} SPIDER_INIT_ERROR_TABLE;

typedef struct st_spider_direct_sql
{
  int                  table_count;
  char                 **db_names;
  char                 **table_names;
  TABLE                **tables;
  int                  *iop;

  char                 *sql;
  ulong                sql_length;

  SPIDER_TRX           *trx;
  SPIDER_CONN          *conn;

  bool                 modified_non_trans_table;

  int                  table_loop_mode;
  longlong             priority;
  int                  net_timeout;
  longlong             bulk_insert_rows;
  int                  connection_channel;

  char                 *server_name;
  char                 *tgt_default_db_name;
  char                 *tgt_host;
  char                 *tgt_username;
  char                 *tgt_password;
  char                 *tgt_socket;
  char                 *tgt_wrapper;
  char                 *conn_key;
  long                 tgt_port;

  uint                 server_name_length;
  uint                 tgt_default_db_name_length;
  uint                 tgt_host_length;
  uint                 tgt_username_length;
  uint                 tgt_password_length;
  uint                 tgt_socket_length;
  uint                 tgt_wrapper_length;
  uint                 conn_key_length;

  pthread_mutex_t               *bg_mutex;
  pthread_cond_t                *bg_cond;
  volatile st_spider_direct_sql *prev;
  volatile st_spider_direct_sql *next;
  void                          *parent;
} SPIDER_DIRECT_SQL;

typedef struct st_spider_bg_direct_sql
{
  longlong                   called_cnt;
  char                       bg_error_msg[MYSQL_ERRMSG_SIZE];
  volatile int               bg_error;
  volatile bool              modified_non_trans_table;
  pthread_mutex_t            bg_mutex;
  pthread_cond_t             bg_cond;
  volatile SPIDER_DIRECT_SQL *direct_sql;
} SPIDER_BG_DIRECT_SQL;

char *spider_create_string(
  const char *str,
  uint length
);
