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

#ifdef USE_PRAGMA_INTERFACE
#pragma interface
#endif

#define SPIDER_CONNECT_INFO_MAX_LEN 64
#define SPIDER_LONGLONG_LEN 20
#define SPIDER_MAX_KEY_LENGTH 16384

#define SPIDER_SET_CONNS_PARAM(param_name, param_val, conns, link_statuses, link_count, link_status) \
  for ( \
    roop_count = spider_conn_link_idx_next(link_statuses, \
      -1, link_count, link_status); \
    roop_count < link_count; \
    roop_count = spider_conn_link_idx_next(link_statuses, \
      roop_count, link_count, link_status) \
  ) { \
    conns[roop_count]->param_name = param_val; \
  }

class ha_spider: public handler
{
public:
  THR_LOCK_DATA      lock;
  SPIDER_SHARE       *share;
  SPIDER_TRX         *trx;
  ulonglong          spider_thread_id;
  ulonglong          trx_conn_adjustment;
  char               **conn_keys;
  SPIDER_CONN        **conns;
  void               **quick_targets;
  int                *need_mons;
  int                search_link_idx;
  SPIDER_RESULT_LIST result_list;
  SPIDER_CONDITION   *condition;
  String             *blob_buff;
  uchar              *searched_bitmap;
  bool               position_bitmap_init;
  uchar              *position_bitmap;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  SPIDER_PARTITION_HANDLER_SHARE *partition_handler_share;
  ha_spider          *pt_handler_share_creator;
#endif
  ha_spider          *next;

  bool               rnd_scan_and_first;
  bool               quick_mode;
  bool               keyread;
  bool               ignore_dup_key;
  bool               write_can_replace;
  bool               insert_with_update;
  bool               low_priority;
  bool               high_priority;
  bool               insert_delayed;
  enum thr_lock_type lock_type;
  int                lock_mode;
  uint               sql_command;
  int                selupd_lock_mode;
  bool               bulk_insert;
  int                bulk_size;
  int                direct_dup_insert;
  int                store_error_num;
  uint               dup_key_idx;
  int                select_column_mode;
  bool               update_request;
  bool               pk_update;
  bool               force_auto_increment;

  ha_spider();
  ha_spider(
    handlerton *hton,
    TABLE_SHARE *table_arg
  );
  const char **bas_ext() const;
  int open(
    const char* name,
    int mode,
    uint test_if_locked
  );
  int close();
  THR_LOCK_DATA **store_lock(
    THD *thd,
    THR_LOCK_DATA **to,
    enum thr_lock_type lock_type
  );
  int external_lock(
    THD *thd,
    int lock_type
  );
  int reset();
  int extra(
    enum ha_extra_function operation
  );
  int index_init(
    uint idx,
    bool sorted
  );
  int index_end();
  int index_read_map(
    uchar *buf,
    const uchar *key,
    key_part_map keypart_map,
    enum ha_rkey_function find_flag
  );
  int index_read_last_map(
    uchar *buf,
    const uchar *key,
    key_part_map keypart_map
  );
  int index_next(
    uchar *buf
  );
  int index_prev(
    uchar *buf
  );
  int index_first(
    uchar *buf
  );
  int index_last(
    uchar *buf
  );
  int index_next_same(
    uchar *buf,
    const uchar *key,
    uint keylen
  );
  int read_range_first(
    const key_range *start_key,
    const key_range *end_key,
    bool eq_range,
    bool sorted
  );
  int read_range_next();
  int read_multi_range_first(
    KEY_MULTI_RANGE **found_range_p,
    KEY_MULTI_RANGE *ranges,
    uint range_count,
    bool sorted,
    HANDLER_BUFFER *buffer
  );
  int read_multi_range_next(
    KEY_MULTI_RANGE **found_range_p
  );
  int rnd_init(
    bool scan
  );
  int rnd_end();
  int rnd_next(
    uchar *buf
  );
  void position(
    const uchar *record
  );
  int rnd_pos(
    uchar *buf,
    uchar *pos
  );
  int info(
    uint flag
  );
  ha_rows records_in_range(
    uint inx,
    key_range *start_key,
    key_range *end_key
  );
  const char *table_type() const;
  ulonglong table_flags() const;
  const char *index_type(
    uint key_number
  );
  ulong index_flags(
    uint idx,
    uint part,
    bool all_parts
  ) const;
  uint max_supported_record_length() const;
  uint max_supported_keys() const;
  uint max_supported_key_parts() const;
  uint max_supported_key_length() const;
  uint max_supported_key_part_length() const;
  uint8 table_cache_type();
  int update_auto_increment();
  void get_auto_increment(
    ulonglong offset,
    ulonglong increment,
    ulonglong nb_desired_values,
    ulonglong *first_value,
    ulonglong *nb_reserved_values
  );
  int reset_auto_increment(
    ulonglong value
  );
  void start_bulk_insert(
    ha_rows rows
  );
  /* call from MySQL */
  int end_bulk_insert();
  /* call from MariaDB */
  int end_bulk_insert(
    bool abort
  );
  int write_row(
    uchar *buf
  );
  bool start_bulk_update();
  int exec_bulk_update(
    uint *dup_key_found
  );
  void end_bulk_update();
  int bulk_update_row(
    const uchar *old_data,
    uchar *new_data,
    uint *dup_key_found
  );
  int update_row(
    const uchar *old_data,
    uchar *new_data
  );
  bool start_bulk_delete();
  int end_bulk_delete();
  int delete_row(
    const uchar *buf
  );
  int delete_all_rows();
  double scan_time();
  double read_time(
    uint index,
    uint ranges,
    ha_rows rows
  );
  const key_map *keys_to_use_for_scanning();
  ha_rows estimate_rows_upper_bound();
  bool get_error_message(
    int error,
    String *buf
  );
  int create(
    const char *name,
    TABLE *form,
    HA_CREATE_INFO *info
  );
  void update_create_info(
    HA_CREATE_INFO* create_info
  );
  int rename_table(
    const char *from,
    const char *to
  );
  int delete_table(
    const char *name
  );
  bool is_crashed() const;
  bool auto_repair() const;
  int disable_indexes(
    uint mode
  );
  int enable_indexes(
    uint mode
  );
  int check(
    THD* thd,
    HA_CHECK_OPT* check_opt
  );
  int repair(
    THD* thd,
    HA_CHECK_OPT* check_opt
  );
  bool check_and_repair(
    THD *thd
  );
  int analyze(
    THD* thd,
    HA_CHECK_OPT* check_opt
  );
  int optimize(
    THD* thd,
    HA_CHECK_OPT* check_opt
  );
  bool is_fatal_error(
    int error_num,
    uint flags
  );
  const COND *cond_push(
    const COND* cond
  );
  void cond_pop();
  st_table *get_table();
  void set_select_column_mode();
#ifdef WITH_PARTITION_STORAGE_ENGINE
  void check_select_column(bool rnd);
#endif
  bool check_and_start_bulk_update(
    spider_bulk_upd_start bulk_upd_start
  );
  int check_and_end_bulk_update(
    spider_bulk_upd_start bulk_upd_start
  );
};
