/* Copyright (C) 2008 Kentoku Shiba

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
extern DECLARE_MYSQL_THDVAR_SIMPLE(init_sql_alloc_size, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(reset_sql_alloc, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(multi_split_read, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(max_order, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(semi_trx_isolation, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(semi_table_lock, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(block_size, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(selupd_lock_mode, int);
extern DECLARE_MYSQL_THDVAR_BASIC(sync_autocommit, char);
extern DECLARE_MYSQL_THDVAR_BASIC(internal_sql_log_off, char);
extern DECLARE_MYSQL_THDVAR_SIMPLE(bulk_size, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(internal_optimize, int);
extern DECLARE_MYSQL_THDVAR_SIMPLE(internal_optimize_local, int);
extern DECLARE_MYSQL_THDVAR_BASIC(use_flash_logs, char);
extern DECLARE_MYSQL_THDVAR_SIMPLE(use_snapshot_with_flush_tables, int);
extern DECLARE_MYSQL_THDVAR_BASIC(use_all_conns_snapshot, char);
extern DECLARE_MYSQL_THDVAR_BASIC(lock_exchange, char);
extern DECLARE_MYSQL_THDVAR_BASIC(internal_unlock, char);
extern DECLARE_MYSQL_THDVAR_BASIC(semi_trx, char);
