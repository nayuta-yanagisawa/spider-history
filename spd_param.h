/* Copyright (C) 2008-2010 Kentoku Shiba

  This program is free software); you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation); version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY); without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program); if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

extern my_bool spider_support_xa;
extern uint spider_table_init_error_interval;
extern int spider_use_table_charset;
extern uint spider_udf_table_lock_mutex_count;
extern uint spider_udf_table_mon_mutex_count;
extern char *spider_remote_access_charset;
extern int spider_remote_autocommit;
extern int spider_remote_sql_log_off;
extern int spider_remote_trx_isolation;
extern my_bool spider_connect_mutex;
extern int spider_udf_ct_bulk_insert_interval;
extern long long spider_udf_ct_bulk_insert_rows;

extern DECLARE_MYSQL_THDVAR_SIMPLE(conn_recycle_mode, unsigned int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(conn_recycle_strict, unsigned int);
extern DECLARE_MYSQL_THDVAR_BASIC(sync_trx_isolation, char);
extern DECLARE_MYSQL_THDVAR_BASIC(use_consistent_snapshot, char);
extern DECLARE_MYSQL_THDVAR_BASIC(internal_xa, char);
extern DECLARE_MYSQL_THDVAR_SIMPLE(internal_xa_snapshot, unsigned int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(force_commit, unsigned int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(internal_offset, long long);
extern DECLARE_MYSQL_THDVAR_SIMPLE(internal_limit, long long);
extern DECLARE_MYSQL_THDVAR_SIMPLE(split_read, long long);
extern DECLARE_MYSQL_THDVAR_SIMPLE(semi_split_read, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(semi_split_read_limit, long long);
extern DECLARE_MYSQL_THDVAR_SIMPLE(init_sql_alloc_size, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(reset_sql_alloc, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(multi_split_read, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(max_order, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(semi_trx_isolation, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(semi_table_lock, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(semi_table_lock_connection, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(block_size, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(selupd_lock_mode, int);
extern DECLARE_MYSQL_THDVAR_BASIC(sync_autocommit, char);
extern DECLARE_MYSQL_THDVAR_BASIC(internal_sql_log_off, char);
extern DECLARE_MYSQL_THDVAR_SIMPLE(bulk_size, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(bulk_update_mode, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(bulk_update_size, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(internal_optimize, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(internal_optimize_local, int);
extern DECLARE_MYSQL_THDVAR_BASIC(use_flash_logs, char);
extern DECLARE_MYSQL_THDVAR_SIMPLE(use_snapshot_with_flush_tables, int);
extern DECLARE_MYSQL_THDVAR_BASIC(use_all_conns_snapshot, char);
extern DECLARE_MYSQL_THDVAR_BASIC(lock_exchange, char);
extern DECLARE_MYSQL_THDVAR_BASIC(internal_unlock, char);
extern DECLARE_MYSQL_THDVAR_BASIC(semi_trx, char);
extern DECLARE_MYSQL_THDVAR_SIMPLE(net_timeout, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(quick_mode, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(quick_page_size, long long);
extern DECLARE_MYSQL_THDVAR_SIMPLE(low_mem_read, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(select_column_mode, int);
#ifndef WITHOUT_SPIDER_BG_SEARCH
extern DECLARE_MYSQL_THDVAR_SIMPLE(bgs_mode, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(bgs_first_read, long long);
extern DECLARE_MYSQL_THDVAR_SIMPLE(bgs_second_read, long long);
#endif
extern DECLARE_MYSQL_THDVAR_SIMPLE(crd_interval, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(crd_mode, int);
#ifdef WITH_PARTITION_STORAGE_ENGINE
extern DECLARE_MYSQL_THDVAR_SIMPLE(crd_sync, int);
#endif
extern DECLARE_MYSQL_THDVAR_SIMPLE(crd_type, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(crd_weight, int);
#ifndef WITHOUT_SPIDER_BG_SEARCH
extern DECLARE_MYSQL_THDVAR_SIMPLE(crd_bg_mode, int);
#endif
extern DECLARE_MYSQL_THDVAR_SIMPLE(sts_interval, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(sts_mode, int);
#ifdef WITH_PARTITION_STORAGE_ENGINE
extern DECLARE_MYSQL_THDVAR_SIMPLE(sts_sync, int);
#endif
#ifndef WITHOUT_SPIDER_BG_SEARCH
extern DECLARE_MYSQL_THDVAR_SIMPLE(sts_bg_mode, int);
#endif
extern DECLARE_MYSQL_THDVAR_SIMPLE(ping_interval_at_trx_start, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(auto_increment_mode, int);
extern DECLARE_MYSQL_THDVAR_BASIC(same_server_link, char);
extern DECLARE_MYSQL_THDVAR_BASIC(local_lock_table, char);
extern DECLARE_MYSQL_THDVAR_SIMPLE(use_pushdown_udf, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(direct_dup_insert, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(udf_ds_bulk_insert_rows, long long);
extern DECLARE_MYSQL_THDVAR_SIMPLE(udf_ds_table_loop_mode, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(connect_retry_interval, long long);
extern DECLARE_MYSQL_THDVAR_SIMPLE(connect_retry_count, int);
extern DECLARE_MYSQL_THDVAR_BASIC(bka_engine, char *);
extern DECLARE_MYSQL_THDVAR_SIMPLE(bka_mode, int);
