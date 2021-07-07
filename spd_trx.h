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

uchar *spider_xa_lock_get_key(
  SPIDER_XA_LOCK *xa_lock,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
);

int spider_free_trx_conn(
  SPIDER_TRX *trx,
  bool trx_free
);

int spider_free_trx_another_conn(
  SPIDER_TRX *trx,
  bool lock
);

int spider_trx_another_lock_tables(
  SPIDER_TRX *trx
);

int spider_trx_all_flush_tables(
  SPIDER_TRX *trx
);

int spider_trx_all_start_trx(
  SPIDER_TRX *trx
);

int spider_trx_all_flush_logs(
  SPIDER_TRX *trx
);

int spider_free_trx_alloc(
  SPIDER_TRX *trx
);

void spider_free_trx_alter_table_alloc(
  SPIDER_TRX *trx,
  SPIDER_ALTER_TABLE *alter_table
);

int spider_free_trx_alter_table(
  SPIDER_TRX *trx
);

int spider_create_trx_alter_table(
  SPIDER_TRX *trx,
  SPIDER_SHARE *share
);

SPIDER_TRX *spider_get_trx(
  const THD *thd,
  int *error_num
);

int spider_free_trx(
  SPIDER_TRX *trx
);

int spider_check_and_set_trx_isolation(
  SPIDER_CONN *conn
);

int spider_check_and_set_autocommit(
  THD *thd,
  SPIDER_CONN *conn
);

int spider_check_and_set_sql_log_off(
  THD *thd,
  SPIDER_CONN *conn
);

SPIDER_XA_LOCK *spider_xa_lock(
  XID *xid,
  int *error_num
);

int spider_xa_unlock(
  SPIDER_XA_LOCK *xa_lock
);

int spider_start_internal_consistent_snapshot(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn
);

int spider_internal_start_trx(
  ha_spider *spider
);

int spider_internal_xa_commit(
  THD* thd,
  SPIDER_TRX *trx,
  XID* xid,
  TABLE *table_xa,
  TABLE *table_xa_member
);

int spider_internal_xa_rollback(
  THD* thd,
  SPIDER_TRX *trx
);

int spider_internal_xa_prepare(
  THD* thd,
  SPIDER_TRX *trx,
  TABLE *table_xa,
  TABLE *table_xa_member,
  bool internal_xa
);

int spider_internal_xa_recover(
  THD* thd,
  XID* xid_list,
  uint len
);

int spider_internal_xa_commit_by_xid(
  THD* thd,
  SPIDER_TRX *trx,
  XID* xid
);

int spider_internal_xa_rollback_by_xid(
  THD* thd,
  SPIDER_TRX *trx,
  XID* xid
);

int spider_start_consistent_snapshot(
  handlerton *hton,
  THD* thd
);

int spider_commit(
  handlerton *hton,
  THD *thd,
  bool all
);

int spider_rollback(
  handlerton *hton,
  THD *thd,
  bool all
);

int spider_xa_prepare(
  handlerton *hton,
  THD* thd,
  bool all
);

int spider_xa_recover(
  handlerton *hton,
  XID* xid_list,
  uint len
);

int spider_xa_commit_by_xid(
  handlerton *hton,
  XID* xid
);

int spider_xa_rollback_by_xid(
  handlerton *hton,
  XID* xid
);

int spider_end_trx(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn
);

int spider_check_trx_and_get_conn(
  THD *thd,
  ha_spider *spider
);