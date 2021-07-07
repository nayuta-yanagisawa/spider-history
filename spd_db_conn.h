/* Copyright (C) 2008-2012 Kentoku Shiba

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

#define SPIDER_DB_INSERT_REPLACE       (1 << 0)
#define SPIDER_DB_INSERT_IGNORE        (1 << 1)
#define SPIDER_DB_INSERT_LOW_PRIORITY  (1 << 2)
#define SPIDER_DB_INSERT_HIGH_PRIORITY (1 << 3)
#define SPIDER_DB_INSERT_DELAYED       (1 << 4)

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
#define SPIDER_SQL_HANDLER_CID_LEN 6
#define SPIDER_SQL_HANDLER_CID_FORMAT "t%05u"
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

int spider_db_conn_queue_action(
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

int spider_db_append_trx_isolation(
  spider_string *str,
  SPIDER_CONN *conn,
  int trx_isolation
);

int spider_db_append_autocommit(
  spider_string *str,
  SPIDER_CONN *conn,
  bool autocommit
);

int spider_db_append_sql_log_off(
  spider_string *str,
  SPIDER_CONN *conn,
  bool sql_log_off
);

int spider_db_append_time_zone(
  spider_string *str,
  SPIDER_CONN *conn,
  Time_zone *time_zone
);

int spider_db_set_names(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
);

int spider_db_query_with_set_names(
  spider_string *sql,
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
);

int spider_db_query_for_bulk_update(
  spider_string *sql,
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

int spider_db_append_start_transaction(
  spider_string *str,
  SPIDER_CONN *conn
);

int spider_db_commit(
  SPIDER_CONN *conn
);

int spider_db_rollback(
  SPIDER_CONN *conn
);

int spider_db_append_hex_string(
  spider_string *str,
  uchar *hex_ptr,
  int hex_ptr_length
);

void spider_db_append_xid_str(
  spider_string *tmp_str,
  XID *xid
);

int spider_db_append_xa_start(
  spider_string *str,
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
  ha_spider *spider,
  int link_idx
);

int spider_db_unlock_tables(
  ha_spider *spider,
  int link_idx
);

int spider_db_append_name_with_quote_str(
  spider_string *str,
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

int spider_db_append_table_name_with_reserve(
  spider_string *str,
  SPIDER_SHARE *share,
  int link_idx
);

void spider_db_append_table_name(
  spider_string *str,
  SPIDER_SHARE *share,
  int link_idx
);

void spider_db_append_table_name_with_adjusting(
  ha_spider *spider,
  spider_string *str,
  SPIDER_SHARE *share,
  int link_idx,
  uint sql_kind
);

void spider_db_append_column_name(
  SPIDER_SHARE *share,
  spider_string *str,
  int field_index
);

int spider_db_append_column_value(
  ha_spider *spider,
  SPIDER_SHARE *share,
  spider_string *str,
  Field *field,
  const uchar *new_ptr
);

int spider_db_append_column_values(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str
);

int spider_db_append_select_str(
  spider_string *str
);

int spider_db_append_select(
  ha_spider *spider
);

int spider_db_append_insert_str(
  spider_string *str,
  int insert_flg
);

int spider_db_append_insert(
  ha_spider *spider
);

int spider_db_append_insert_for_recovery(
  spider_string *insert_sql,
  ha_spider *spider,
  const TABLE *table,
  int link_idx
);

int spider_db_append_update(
  spider_string *str,
  ha_spider *spider,
  int link_idx
);

int spider_db_append_delete(
  spider_string *str,
  ha_spider *spider
);

int spider_db_append_truncate(
  ha_spider *spider,
  int link_idx
);

int spider_db_append_from_str(
  spider_string *str
);

int spider_db_append_from(
  spider_string *str,
  ha_spider *spider,
  int link_idx
);

int spider_db_append_from_with_alias(
  spider_string *str,
  const char **table_names,
  uint *table_name_lengths,
  const char **table_aliases,
  uint *table_alias_lengths,
  uint table_count,
  int *table_name_pos,
  bool over_write
);

int spider_db_append_open_paren_str(
  spider_string *str
);

int spider_db_append_into_str(
  spider_string *str
);

int spider_db_append_values_str(
  spider_string *str
);

int spider_db_append_into(
  ha_spider *spider,
  const TABLE *table,
  int link_idx
);

int spider_db_append_update_set(
  spider_string *str,
  ha_spider *spider,
  TABLE *table
);

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
int spider_db_append_increment_update_set(
  spider_string *str,
  ha_spider *spider,
  TABLE *table
);
#endif
#endif

int spider_db_append_update_where(
  spider_string *str,
  ha_spider *spider,
  const TABLE *table,
  my_ptrdiff_t ptr_diff
);

int spider_db_append_table_select(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider
);

int spider_db_append_key_select(
  spider_string *str,
  const KEY *key_info,
  ha_spider *spider
);

int spider_db_append_minimum_select(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider
);

int spider_db_append_minimum_select_without_quote(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider
);

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
int spider_db_append_minimum_select_by_field_idx_list(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider,
  uint32 *field_idxs,
  size_t field_idxs_num
);
#endif
#endif

int spider_db_append_select_columns(
  ha_spider *spider
);

int spider_db_append_table_select_with_alias(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider,
  const char *alias,
  uint alias_length
);

int spider_db_append_key_select_with_alias(
  spider_string *str,
  const KEY *key_info,
  ha_spider *spider,
  const char *alias,
  uint alias_length
);

int spider_db_append_minimum_select_with_alias(
  spider_string *str,
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
  ha_spider *spider,
  SPIDER_SHARE *share,
  spider_string *str,
  KEY_PART_INFO *key_part,
  const key_range *key,
  const uchar **ptr,
  uint sql_kind
);

int spider_db_append_null_value(
  spider_string *str,
  KEY_PART_INFO *key_part,
  const uchar **ptr
);

int spider_db_append_key_column_types(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str
);

int spider_db_append_table_columns(
  spider_string *str,
  TABLE_SHARE *table_share
);

int spider_db_append_key_columns(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str
);

int spider_db_append_key_join_columns_for_bka(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str,
  const char **table_aliases,
  uint *table_alias_lengths
);

int spider_db_append_key_hint(
  spider_string *str,
  char *hint_str
);

int spider_db_append_key_where_internal(
  const key_range *start_key,
  const key_range *end_key,
  ha_spider *spider,
  uint sql_kind,
  bool update_sql
);

int spider_db_append_key_where(
  const key_range *start_key,
  const key_range *end_key,
  ha_spider *spider
);

int spider_db_append_match_against(
  spider_string *str,
  ha_spider *spider,
  st_spider_ft_info  *ft_info,
  const char *alias,
  uint alias_length
);

int spider_db_append_match_select(
  spider_string *str,
  ha_spider *spider,
  const char *alias,
  uint alias_length
);

int spider_db_append_match_fetch(
  ha_spider *spider,
  st_spider_ft_info *ft_first,
  st_spider_ft_info *ft_current,
  SPIDER_DB_ROW *row,
  ulong **lengths
);

int spider_db_append_match_where(
  ha_spider *spider
);

int spider_db_append_hint_after_table(
  ha_spider *spider,
  spider_string *str,
  spider_string *hint
);

int spider_db_append_key_order_str(
  spider_string *str,
  KEY *key_info,
  int start_pos,
  bool desc_flg
);

int spider_db_append_key_order_with_alias(
  ha_spider *spider,
  bool update_sql,
  const char *alias,
  uint alias_length
);

int spider_db_append_key_order(
  ha_spider *spider,
  bool update_sql
);

int spider_db_append_limit_internal(
  ha_spider *spider,
  spider_string *str,
  longlong offset,
  longlong limit,
  uint sql_kind
);

int spider_db_append_limit(
  ha_spider *spider,
  spider_string *str,
  longlong offset,
  longlong limit
);

int spider_db_append_select_lock_str(
  spider_string *str,
  int lock_mode
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

int spider_db_append_show_records(
  SPIDER_SHARE *share
);

void spider_db_free_show_records(
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
  spider_string *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos,
  CHARSET_INFO *table_charset
);

int spider_db_append_drop_tmp_bka_table(
  ha_spider *spider,
  spider_string *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos,
  int *drop_table_end_pos,
  bool with_semicolon
);

int spider_db_append_insert_tmp_bka_table(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos
);

void spider_db_append_handler_next(
  ha_spider *spider
);

void spider_db_get_row_and_lengths_from_tmp_tbl_rec(
  SPIDER_RESULT *current,
  SPIDER_DB_ROW *row,
  ulong **lengths
);

int spider_db_get_row_and_lengths_from_tmp_tbl(
  SPIDER_RESULT *current,
  SPIDER_DB_ROW *row,
  ulong **lengths
);

int spider_db_get_row_and_lengths_from_tmp_tbl_pos(
  SPIDER_POSITION *pos,
  SPIDER_DB_ROW *row,
  ulong **lengths
);

int spider_db_fetch_row(
  SPIDER_SHARE *share,
  Field *field,
  SPIDER_DB_ROW row,
  ulong *lengths,
  my_ptrdiff_t ptr_diff
);

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
int spider_db_fetch_hs_row(
  SPIDER_SHARE *share,
  Field *field,
  const SPIDER_HS_STRING_REF &hs_row,
  my_ptrdiff_t ptr_diff
);
#endif

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

void spider_db_create_position(
  ha_spider *spider,
  SPIDER_POSITION *pos
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

int spider_db_show_records(
  ha_spider *spider,
  int link_idx
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

#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
int spider_db_direct_update(
  ha_spider *spider,
  TABLE *table,
  KEY_MULTI_RANGE *ranges,
  uint range_count,
  uint *update_rows
);
#endif

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

#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
int spider_db_direct_delete(
  ha_spider *spider,
  TABLE *table,
  KEY_MULTI_RANGE *ranges,
  uint range_count,
  uint *delete_rows
);
#endif

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
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_open_item_cond(
  Item_cond *item_cond,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_open_item_func(
  Item_func *item_func,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_open_item_ident(
  Item_ident *item_ident,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_open_item_field(
  Item_field *item_field,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_open_item_ref(
  Item_ref *item_ref,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_open_item_row(
  Item_row *item_row,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_open_item_string(
  Item *item,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_open_item_int(
  Item *item,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_open_item_cache(
  Item_cache *item_cache,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_append_condition_internal(
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length,
  uint sql_kind
);

int spider_db_append_condition(
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

int spider_db_append_update_columns(
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
);

uint spider_db_check_ft_idx(
  Item_func *item_func,
  ha_spider *spider
);

int spider_db_udf_fetch_row(
  SPIDER_TRX *trx,
  Field *field,
  SPIDER_DB_ROW row,
  ulong *length
);

#ifdef HAVE_HANDLERSOCKET
int spider_db_udf_fetch_hs_row(
  SPIDER_TRX *trx,
  Field *field,
  const SPIDER_HS_STRING_REF &hs_row
);
#endif

int spider_db_udf_fetch_table(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn,
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
  spider_string *str,
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
  spider_string *str,
  SPIDER_SHARE *share,
  SPIDER_TRX *trx,
  spider_string *where_str,
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

int spider_db_udf_copy_key_row(
  spider_string *str,
  spider_string *source_str,
  Field *field,
  ulong *row_pos,
  ulong *length,
  const char *joint_str,
  const int joint_length
);

int spider_db_udf_copy_row(
  spider_string *str,
  Field *field,
  SPIDER_DB_ROW row,
  ulong *length
);

int spider_db_udf_copy_rows(
  spider_string *str,
  TABLE *table,
  SPIDER_DB_RESULT *result,
  ulong **last_row_pos,
  ulong **last_lengths
);

int spider_db_udf_copy_tables(
  SPIDER_COPY_TABLES *copy_tables,
  ha_spider *spider,
  TABLE *table,
  longlong bulk_insert_rows
);

void spider_db_reset_handler_open(
  SPIDER_CONN *conn
);

int spider_db_open_handler(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
);

int spider_db_close_handler(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx,
  uint tgt_conn_kind
);

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
int spider_db_hs_request_buf_find(
  ha_spider *spider,
  int link_idx
);

int spider_db_hs_request_buf_insert(
  ha_spider *spider,
  int link_idx
);

int spider_db_hs_request_buf_update(
  ha_spider *spider,
  int link_idx
);

int spider_db_hs_request_buf_delete(
  ha_spider *spider,
  int link_idx
);
#endif

spider_string *spider_db_add_str_dynamic(
  DYNAMIC_ARRAY *array,
  uint array_id,
  const char *array_func_name,
  const char *array_file_name,
  ulong array_line_no,
  uint *strs_pos,
  const char *str,
  uint str_len
);

void spider_db_free_str_dynamic(
  DYNAMIC_ARRAY *array
);

void spider_db_clear_dynamic(
  DYNAMIC_ARRAY *array
);
