/* Copyright (C) 2008-2010 Kentoku Shiba

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
#define SPIDER_SQL_COMMA_STR ","
#define SPIDER_SQL_COMMA_LEN (sizeof(SPIDER_SQL_COMMA_STR) - 1)
#define SPIDER_SQL_UNION_ALL_STR ")union all("
#define SPIDER_SQL_UNION_ALL_LEN (sizeof(SPIDER_SQL_UNION_ALL_STR) - 1)

#define SPIDER_SQL_ID_STR "id"
#define SPIDER_SQL_ID_LEN (sizeof(SPIDER_SQL_ID_STR) - 1)
#define SPIDER_SQL_TMP_BKA_ENGINE_STR "memory"
#define SPIDER_SQL_TMP_BKA_ENGINE_LEN (sizeof(SPIDER_SQL_TMP_BKA_ENGINE_STR) - 1)

#define SPIDER_SQL_A_DOT_STR "a."
#define SPIDER_SQL_A_DOT_LEN (sizeof(SPIDER_SQL_A_DOT_STR) - 1)
#define SPIDER_SQL_B_DOT_STR "b."
#define SPIDER_SQL_B_DOT_LEN (sizeof(SPIDER_SQL_B_DOT_STR) - 1)
#define SPIDER_SQL_A_STR "a"
#define SPIDER_SQL_A_LEN (sizeof(SPIDER_SQL_A_STR) - 1)
#define SPIDER_SQL_B_STR "b"
#define SPIDER_SQL_B_LEN (sizeof(SPIDER_SQL_B_STR) - 1)

#define SPIDER_SQL_INT_LEN 20
#define SPIDER_UDF_PING_TABLE_PING_ONLY (1 << 0)
#define SPIDER_UDF_PING_TABLE_USE_WHERE (1 << 1)

int spider_db_connect(
  const SPIDER_SHARE *share,
  SPIDER_CONN *conn,
  int link_idx
);

int spider_db_ping(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
);

void spider_db_disconnect(
  SPIDER_CONN *conn
);

int spider_db_before_query(
  SPIDER_CONN *conn,
  int *need_mon
);

int spider_db_query(
  SPIDER_CONN *conn,
  const char *query,
  uint length,
  int *need_mon
);

int spider_db_errorno(
  SPIDER_CONN *conn
);

int spider_db_set_trx_isolation(
  SPIDER_CONN *conn,
  int trx_isolation,
  int *need_mon
);

int spider_db_set_autocommit(
  SPIDER_CONN *conn,
  bool autocommit,
  int *need_mon
);

int spider_db_set_sql_log_off(
  SPIDER_CONN *conn,
  bool sql_log_off,
  int *need_mon
);

int spider_db_set_names(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
);

int spider_db_query_with_set_names(
  String *sql,
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
);

int spider_db_query_for_bulk_update(
  String *sql,
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
);

size_t spider_db_real_escape_string(
  SPIDER_CONN *conn,
  char *to,
  const char *from,
  size_t from_length
);

int spider_db_consistent_snapshot(
  SPIDER_CONN *conn,
  int *need_mon
);

int spider_db_start_transaction(
  SPIDER_CONN *conn,
  int *need_mon
);

int spider_db_commit(
  SPIDER_CONN *conn
);

int spider_db_rollback(
  SPIDER_CONN *conn
);

int spider_db_append_hex_string(
  String *str,
  uchar *hex_ptr,
  int hex_ptr_length
);

void spider_db_append_xid_str(
  String *tmp_str,
  XID *xid
);

int spider_db_xa_start(
  SPIDER_CONN *conn,
  XID *xid,
  int *need_mon
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
  ha_spider *spider,
  int link_idx
);

int spider_db_unlock_tables(
  ha_spider *spider,
  int link_idx
);

int spider_db_append_name_with_quote_str(
  String *str,
  char *name
);

int spider_db_create_table_names_str(
  SPIDER_SHARE *share
);

void spider_db_free_table_names_str(
  SPIDER_SHARE *share
);

int spider_db_create_column_name_str(
  SPIDER_SHARE *share,
  TABLE_SHARE *table_share
);

void spider_db_free_column_name_str(
  SPIDER_SHARE *share
);

int spider_db_convert_key_hint_str(
  SPIDER_SHARE *share,
  TABLE_SHARE *table_share
);

void spider_db_append_table_name(
  String *str,
  SPIDER_SHARE *share,
  int link_idx
);

void spider_db_append_table_name_with_adjusting(
  String *str,
  SPIDER_SHARE *share,
  int link_idx
);

void spider_db_append_column_name(
  SPIDER_SHARE *share,
  String *str,
  int field_index
);

int spider_db_append_column_value(
  SPIDER_SHARE *share,
  String *str,
  Field *field,
  const uchar *new_ptr
);

int spider_db_append_column_values(
  const key_range *start_key,
  ha_spider *spider,
  String *str
);

int spider_db_append_select(
  ha_spider *spider
);

int spider_db_append_insert(
  ha_spider *spider
);

int spider_db_append_insert_for_recovery(
  String *insert_sql,
  ha_spider *spider,
  const TABLE *table,
  int link_idx
);

int spider_db_append_update(
  String *str,
  ha_spider *spider,
  int link_idx
);

int spider_db_append_delete(
  String *str,
  ha_spider *spider
);

int spider_db_append_truncate(
  ha_spider *spider,
  int link_idx
);

int spider_db_append_from(
  String *str,
  ha_spider *spider,
  int link_idx
);

int spider_db_append_from_with_alias(
  String *str,
  char **table_names,
  uint *table_name_lengths,
  char **table_aliases,
  uint *table_alias_lengths,
  uint table_count,
  int *table_name_pos,
  bool over_write
);

int spider_db_append_into(
  ha_spider *spider,
  const TABLE *table,
  int link_idx
);

int spider_db_append_update_set(
  String *str,
  ha_spider *spider,
  TABLE *table
);

int spider_db_append_update_where(
  String *str,
  ha_spider *spider,
  const TABLE *table,
  my_ptrdiff_t ptr_diff
);

int spider_db_append_table_select(
  String *str,
  const TABLE *table,
  ha_spider *spider
);

int spider_db_append_key_select(
  String *str,
  const KEY *key_info,
  ha_spider *spider
);

int spider_db_append_minimum_select(
  String *str,
  const TABLE *table,
  ha_spider *spider
);

int spider_db_append_select_columns(
  ha_spider *spider
);

int spider_db_append_table_select_with_alias(
  String *str,
  const TABLE *table,
  ha_spider *spider,
  const char *alias,
  uint alias_length
);

int spider_db_append_key_select_with_alias(
  String *str,
  const KEY *key_info,
  ha_spider *spider,
  const char *alias,
  uint alias_length
);

int spider_db_append_minimum_select_with_alias(
  String *str,
  const TABLE *table,
  ha_spider *spider,
  const char *alias,
  uint alias_length
);

int spider_db_append_select_columns_with_alias(
  ha_spider *spider,
  const char *alias,
  uint alias_length
);

int spider_db_append_null(
  SPIDER_SHARE *share,
  String *str,
  KEY_PART_INFO *key_part,
  const key_range *key,
  const uchar **ptr
);

int spider_db_append_null_value(
  String *str,
  KEY_PART_INFO *key_part,
  const uchar **ptr
);

int spider_db_append_key_column_types(
  const key_range *start_key,
  ha_spider *spider,
  String *str
);

int spider_db_append_key_columns(
  const key_range *start_key,
  ha_spider *spider,
  String *str
);

int spider_db_append_key_join_columns_for_bka(
  const key_range *start_key,
  ha_spider *spider,
  String *str,
  char **table_aliases,
  uint *table_alias_lengths
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

int spider_db_append_set_names(
  SPIDER_SHARE *share
);

void spider_db_free_set_names(
  SPIDER_SHARE *share
);

int spider_db_append_disable_keys(
  ha_spider *spider,
  int link_idx
);

int spider_db_append_enable_keys(
  ha_spider *spider,
  int link_idx
);

int spider_db_append_check_table(
  ha_spider *spider,
  int link_idx,
  HA_CHECK_OPT* check_opt
);

int spider_db_append_repair_table(
  ha_spider *spider,
  int link_idx,
  HA_CHECK_OPT* check_opt
);

int spider_db_append_analyze_table(
  ha_spider *spider,
  int link_idx
);

int spider_db_append_optimize_table(
  ha_spider *spider,
  int link_idx
);

int spider_db_append_flush_tables(
  ha_spider *spider,
  bool lock
);

void spider_db_create_tmp_bka_table_name(
  ha_spider *spider,
  char *tmp_table_name,
  int *tmp_table_name_length,
  int link_idx
);

int spider_db_append_create_tmp_bka_table(
  const key_range *start_key,
  ha_spider *spider,
  String *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos,
  CHARSET_INFO *table_charset
);

int spider_db_append_drop_tmp_bka_table(
  ha_spider *spider,
  String *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos,
  int *drop_table_end_pos,
  bool with_semicolon
);

int spider_db_append_insert_tmp_bka_table(
  const key_range *start_key,
  ha_spider *spider,
  String *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos
);

int spider_db_fetch_row(
  SPIDER_SHARE *share,
  Field *field,
  SPIDER_DB_ROW row,
  ulong *lengths,
  my_ptrdiff_t ptr_diff
);

int spider_db_fetch_table(
  ha_spider *spider,
  uchar *buf,
  TABLE *table,
  SPIDER_RESULT_LIST *result_list
);

int spider_db_fetch_key(
  ha_spider *spider,
  uchar *buf,
  TABLE *table,
  const KEY *key_info,
  SPIDER_RESULT_LIST *result_list
);

int spider_db_fetch_minimum_columns(
  ha_spider *spider,
  uchar *buf,
  TABLE *table,
  SPIDER_RESULT_LIST *result_list
);

void spider_db_free_one_result_for_start_next(
  ha_spider *spider
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
  int link_idx,
  TABLE *table
);

int spider_db_next_result(
  SPIDER_CONN *conn
);

void spider_db_discard_result(
  SPIDER_CONN *conn
);

void spider_db_discard_multiple_result(
  SPIDER_CONN *conn
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
  int link_idx,
  TABLE *table
);

int spider_db_seek_last(
  uchar *buf,
  ha_spider *spider,
  int link_idx,
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
  int link_idx,
  int sts_mode
);

void spider_db_set_cardinarity(
  ha_spider *spider,
  TABLE *table
);

int spider_db_show_index(
  ha_spider *spider,
  int link_idx,
  TABLE *table,
  int crd_mode
);

ha_rows spider_db_explain_select(
  key_range *start_key,
  key_range *end_key,
  ha_spider *spider,
  int link_idx
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
  ha_spider *spider,
  int link_idx
);

int spider_db_bulk_update_size_limit(
  ha_spider *spider,
  TABLE *table
);

int spider_db_bulk_update_end(
  ha_spider *spider
);

int spider_db_bulk_update(
  ha_spider *spider,
  TABLE *table,
  my_ptrdiff_t ptr_diff
);

int spider_db_update(
  ha_spider *spider,
  TABLE *table,
  const uchar *old_data
);

int spider_db_bulk_delete(
  ha_spider *spider,
  TABLE *table,
  my_ptrdiff_t ptr_diff
);

int spider_db_delete(
  ha_spider *spider,
  TABLE *table,
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

int spider_db_open_item_row(
  Item_row *item_row,
  ha_spider *spider,
  String *str
);

int spider_db_append_condition(
  ha_spider *spider,
  String *str
);

int spider_db_udf_fetch_row(
  SPIDER_TRX *trx,
  Field *field,
  SPIDER_DB_ROW row,
  ulong *length
);

int spider_db_udf_fetch_table(
  SPIDER_TRX *trx,
  TABLE *table,
  SPIDER_DB_RESULT *result,
  uint set_on,
  uint set_off
);

int spider_db_udf_direct_sql_connect(
  const SPIDER_DIRECT_SQL *direct_sql,
  SPIDER_CONN *conn
);

int spider_db_udf_direct_sql_ping(
  SPIDER_DIRECT_SQL *direct_sql
);

int spider_db_udf_direct_sql(
  SPIDER_DIRECT_SQL *direct_sql
);

int spider_db_udf_direct_sql_select_db(
  SPIDER_DIRECT_SQL *direct_sql,
  SPIDER_CONN *conn
);

int spider_db_udf_direct_sql_set_names(
  SPIDER_DIRECT_SQL *direct_sql,
  SPIDER_TRX *trx,
  SPIDER_CONN *conn
);

int spider_db_udf_check_and_set_set_names(
  SPIDER_TRX *trx
);

int spider_db_udf_append_set_names(
  SPIDER_TRX *trx
);

void spider_db_udf_free_set_names(
  SPIDER_TRX *trx
);

int spider_db_udf_ping_table(
  SPIDER_TABLE_MON_LIST *table_mon_list,
  SPIDER_SHARE *share,
  SPIDER_TRX *trx,
  SPIDER_CONN *conn,
  char *where_clause,
  uint where_clause_length,
  bool ping_only,
  bool use_where,
  longlong limit
);

int spider_db_udf_ping_table_append_mon_next(
  String *str,
  char *child_table_name,
  uint child_table_name_length,
  int link_id,
  char *where_clause,
  uint where_clause_length,
  longlong first_sid,
  int full_mon_count,
  int current_mon_count,
  int success_count,
  int fault_count,
  int flags,
  longlong limit
);

int spider_db_udf_ping_table_append_select(
  String *str,
  SPIDER_SHARE *share,
  SPIDER_TRX *trx,
  String *where_str,
  bool use_where,
  longlong limit
);

int spider_db_udf_ping_table_mon_next(
  THD *thd,
  SPIDER_TABLE_MON *table_mon,
  SPIDER_CONN *conn,
  SPIDER_MON_TABLE_RESULT *mon_table_result,
  char *child_table_name,
  uint child_table_name_length,
  int link_id,
  char *where_clause,
  uint where_clause_length,
  longlong first_sid,
  int full_mon_count,
  int current_mon_count,
  int success_count,
  int fault_count,
  int flags,
  longlong limit
);
