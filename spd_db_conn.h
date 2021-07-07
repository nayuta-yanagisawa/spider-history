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

#define SPIDER_DB_WRAPPER_STR "mysql"
#define SPIDER_DB_WRAPPER_LEN (sizeof(SPIDER_DB_WRAPPER_STR) - 1)

#define SPIDER_DB_TABLE_LOCK_READ_LOCAL         0
#define SPIDER_DB_TABLE_LOCK_READ               1
#define SPIDER_DB_TABLE_LOCK_LOW_PRIORITY_WRITE 2
#define SPIDER_DB_TABLE_LOCK_WRITE              3

#define SPIDER_SQL_OPEN_PAREN_STR "("
#define SPIDER_SQL_OPEN_PAREN_LEN (sizeof(SPIDER_SQL_OPEN_PAREN_STR) - 1)
#define SPIDER_SQL_CLOSE_PAREN_STR ")"
#define SPIDER_SQL_CLOSE_PAREN_LEN (sizeof(SPIDER_SQL_CLOSE_PAREN_STR) - 1)
#define SPIDER_SQL_UNION_ALL_STR ")union all("
#define SPIDER_SQL_UNION_ALL_LEN (sizeof(SPIDER_SQL_UNION_ALL_STR) - 1)

int spider_db_connect(
  const SPIDER_SHARE *share,
  SPIDER_CONN *conn
);

int spider_db_ping(
  ha_spider *spider
);

void spider_db_disconnect(
  SPIDER_CONN *conn
);

int spider_db_query(
  SPIDER_CONN *conn,
  const char *query,
  uint length
);

int spider_db_errorno(
  SPIDER_CONN *conn
);

int spider_db_set_trx_isolation(
  SPIDER_CONN *conn,
  int trx_isolation
);

int spider_db_set_autocommit(
  SPIDER_CONN *conn,
  bool autocommit
);

int spider_db_set_sql_log_off(
  SPIDER_CONN *conn,
  bool sql_log_off
);

int spider_db_consistent_snapshot(
  SPIDER_CONN *conn
);

int spider_db_start_transaction(
  SPIDER_CONN *conn
);

int spider_db_commit(
  SPIDER_CONN *conn
);

int spider_db_rollback(
  SPIDER_CONN *conn
);

void spider_db_append_xid_str(
  String *tmp_str,
  XID *xid
);

int spider_db_xa_start(
  SPIDER_CONN *conn,
  XID *xid
);

int spider_db_xa_end(
  SPIDER_CONN *conn,
  XID *xid
);

int spider_db_xa_prepare(
  SPIDER_CONN *conn,
  XID *xid
);

int spider_db_xa_commit(
  SPIDER_CONN *conn,
  XID *xid
);

int spider_db_xa_rollback(
  SPIDER_CONN *conn,
  XID *xid
);

int spider_db_lock_tables(
  ha_spider *spider
);

int spider_db_unlock_tables(
  ha_spider *spider
);

void spider_db_append_column_name(
  String *str,
  const Field *field,
  int field_length
);

int spider_db_append_column_value(
  String *str,
  Field *field,
  const uchar *new_ptr
);

int spider_db_append_select(
  ha_spider *spider
);

int spider_db_append_insert(
  ha_spider *spider
);

int spider_db_append_update(
  ha_spider *spider
);

int spider_db_append_delete(
  ha_spider *spider
);

int spider_db_append_truncate(
  ha_spider *spider
);

int spider_db_append_from(
  String *str,
  SPIDER_SHARE *share
);

int spider_db_append_into(
  ha_spider *spider,
  const TABLE *table
);

int spider_db_append_update_set(
  ha_spider *spider,
  TABLE *table
);

int spider_db_append_update_where(
  ha_spider *spider,
  const TABLE *table,
  my_ptrdiff_t ptr_diff
);

int spider_db_append_table_select(
  String *str,
  const TABLE *table,
  SPIDER_SHARE *share
);

int spider_db_append_key_select(
  String *str,
  const KEY *key_info,
  SPIDER_SHARE *share
);

int spider_db_append_minimum_select(
  String *str,
  const TABLE *table,
  SPIDER_SHARE *share
);

int spider_db_append_select_columns(
  ha_spider *spider
);

int spider_db_append_null(
  String *str,
  KEY_PART_INFO *key_part,
  uint key_name_length,
  const key_range *key,
  const uchar **ptr
);

int spider_db_append_key_hint(
  String *str,
  char *hint_str
);

int spider_db_append_key_where(
  const key_range *start_key,
  const key_range *end_key,
  ha_spider *spider
);

int spider_db_append_hint_after_table(
  String *str,
  String *hint
);

int spider_db_append_key_order(
  ha_spider *spider
);

int spider_db_append_limit(
  String *str,
  longlong offset,
  longlong limit
);

int spider_db_append_select_lock(
  ha_spider *spider
);

int spider_db_append_show_table_status(
  SPIDER_SHARE *share
);

void spider_db_free_show_table_status(
  SPIDER_SHARE *share
);

int spider_db_append_show_index(
  SPIDER_SHARE *share
);

void spider_db_free_show_index(
  SPIDER_SHARE *share
);

int spider_db_append_disable_keys(
  ha_spider *spider
);

int spider_db_append_enable_keys(
  ha_spider *spider
);

int spider_db_append_check_table(
  ha_spider *spider,
  HA_CHECK_OPT* check_opt
);

int spider_db_append_repair_table(
  ha_spider *spider,
  HA_CHECK_OPT* check_opt
);

int spider_db_append_analyze_table(
  ha_spider *spider
);

int spider_db_append_optimize_table(
  ha_spider *spider
);

int spider_db_append_flush_tables(
  ha_spider *spider,
  bool lock
);

void spider_db_fetch_row(
  Field *field,
  SPIDER_DB_ROW row,
  ulong *lengths,
  my_ptrdiff_t ptr_diff
);

int spider_db_fetch_table(
  uchar *buf,
  TABLE *table,
  SPIDER_RESULT_LIST *result_list
);

int spider_db_fetch_key(
  uchar *buf,
  TABLE *table,
  const KEY *key_info,
  SPIDER_RESULT_LIST *result_list
);

int spider_db_fetch_minimum_columns(
  uchar *buf,
  TABLE *table,
  SPIDER_RESULT_LIST *result_list
);

void spider_db_free_one_result(
  SPIDER_RESULT_LIST *result_list,
  SPIDER_RESULT *result
);

int spider_db_free_result(
  ha_spider *spider,
  bool final
);

int spider_db_store_result(
  ha_spider *spider,
  TABLE *table
);

int spider_db_fetch(
  uchar *buf,
  ha_spider *spider,
  TABLE *table
);

int spider_db_seek_prev(
  uchar *buf,
  ha_spider *spider,
  TABLE *table
);

int spider_db_seek_next(
  uchar *buf,
  ha_spider *spider,
  TABLE *table
);

int spider_db_seek_last(
  uchar *buf,
  ha_spider *spider,
  TABLE *table
);

int spider_db_seek_first(
  uchar *buf,
  ha_spider *spider,
  TABLE *table
);

void spider_db_set_pos_to_first_row(
  SPIDER_RESULT_LIST *result_list
);

SPIDER_POSITION *spider_db_create_position(
  ha_spider *spider
);

int spider_db_seek_tmp(
  uchar *buf,
  SPIDER_POSITION *pos,
  ha_spider *spider,
  TABLE *table
);

int spider_db_seek_tmp_table(
  uchar *buf,
  SPIDER_POSITION *pos,
  ha_spider *spider,
  TABLE *table
);

int spider_db_seek_tmp_key(
  uchar *buf,
  SPIDER_POSITION *pos,
  ha_spider *spider,
  TABLE *table,
  const KEY *key_info
);

int spider_db_seek_tmp_minimum_columns(
  uchar *buf,
  SPIDER_POSITION *pos,
  ha_spider *spider,
  TABLE *table
);

int spider_db_show_table_status(
  ha_spider *spider,
  int sts_mode
);

int spider_db_show_index(
  ha_spider *spider,
  TABLE *table,
  int crd_mode
);

ha_rows spider_db_explain_select(
  key_range *start_key,
  key_range *end_key,
  ha_spider *spider
);

int spider_db_bulk_insert_init(
  ha_spider *spider,
  const TABLE *table
);

int spider_db_bulk_insert(
  ha_spider *spider,
  TABLE *table,
  bool bulk_end
);

int spider_db_update_auto_increment(
  ha_spider *spider
);

int spider_db_update(
  ha_spider *spider,
  TABLE *table,
  const uchar *old_data
);

int spider_db_delete(
  ha_spider *spider,
  const TABLE *table,
  const uchar *buf
);

int spider_db_delete_all_rows(
  ha_spider *spider
);

int spider_db_disable_keys(
  ha_spider *spider
);

int spider_db_enable_keys(
  ha_spider *spider
);

int spider_db_check_table(
  ha_spider *spider,
  HA_CHECK_OPT* check_opt
);

int spider_db_repair_table(
  ha_spider *spider,
  HA_CHECK_OPT* check_opt
);

int spider_db_analyze_table(
  ha_spider *spider
);

int spider_db_optimize_table(
  ha_spider *spider
);

int spider_db_flush_tables(
  ha_spider *spider,
  bool lock
);

int spider_db_flush_logs(
  ha_spider *spider
);

int spider_db_print_item_type(
  Item *item,
  ha_spider *spider,
  String *str
);

int spider_db_open_item_cond(
  Item_cond *item_cond,
  ha_spider *spider,
  String *str
);

int spider_db_open_item_func(
  Item_func *item_func,
  ha_spider *spider,
  String *str
);

int spider_db_open_item_ident(
  Item_ident *item_ident,
  ha_spider *spider,
  String *str
);

int spider_db_open_item_field(
  Item_field *item_field,
  ha_spider *spider,
  String *str
);

int spider_db_open_item_ref(
  Item_ref *item_ref,
  ha_spider *spider,
  String *str
);

int spider_db_append_condition(
  ha_spider *spider,
  String *str
);
