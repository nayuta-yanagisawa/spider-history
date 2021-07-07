/* Copyright (C) 2008 Kentoku Shiba

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

/* alter table */
typedef struct st_spider_alter_table
{
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
  uint               join_trx;
  int                trx_isolation;
  bool               semi_trx_isolation_chk;
  int                semi_trx_isolation;
  bool               semi_trx_chk;
  bool               semi_trx;
  bool               trx_start;
  int                table_lock;
  bool               disable_xa;
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
} SPIDER_TRX;

typedef struct st_spider_share
{
  char               *table_name;
  uint               table_name_length;
  uint               use_count;
  pthread_mutex_t    mutex;
  pthread_mutex_t    sts_mutex;
  pthread_mutex_t    crd_mutex;
  THR_LOCK           lock;

  volatile bool      init;
  volatile bool      init_error;
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

  String             *table_select;
  String             *key_select;
  String             *key_hint;
  String             *show_table_status;
  String             *show_index;
  longlong           *cardinality;
  bool               *cardinality_upd;

  double             sts_interval;
  int                sts_mode;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  int                sts_sync;
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
#ifndef WITHOUT_SPIDER_BG_SEARCH
  int                bgs_mode;
  longlong           bgs_first_read;
  longlong           bgs_second_read;
#endif

  char               *server_name;
  char               *tgt_table_name;
  char               *tgt_db;
  char               *tgt_host;
  char               *tgt_username;
  char               *tgt_password;
  char               *tgt_socket;
  char               *tgt_wrapper;
  char               *csname;
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
  uint               csname_length;
  uint               conn_key_length;

  SPIDER_ALTER_TABLE alter_table;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  SPIDER_PARTITION_SHARE *partition_share;
#endif
} SPIDER_SHARE;

char *spider_create_string(
  const char *str,
  uint length
);
