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

uchar *spider_tbl_get_key(
  SPIDER_SHARE *share,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
);

#ifdef WITH_PARTITION_STORAGE_ENGINE
uchar *spider_pt_share_get_key(
  SPIDER_PARTITION_SHARE *share,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
);
#endif

uchar *spider_ha_get_key(
  ha_spider *spider,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
);

int spider_get_server(
  SPIDER_SHARE *share
);

int spider_free_share_alloc(
  SPIDER_SHARE *share
);

void spider_free_tmp_share_alloc(
  SPIDER_SHARE *share
);

char *spider_get_string_between_quote(
  char *ptr,
  bool alloc
);

int spider_parse_connect_info(
  SPIDER_SHARE *share,
  TABLE *table,
  uint create_table
);

int spider_set_connect_info_default(
  SPIDER_SHARE *share,
#ifdef WITH_PARTITION_STORAGE_ENGINE
  partition_element *part_elem,
  partition_element *sub_elem,
#endif
  TABLE *table
);

int spider_create_conn_key(
  SPIDER_SHARE *share
);

SPIDER_SHARE *spider_get_share(
  const char *table_name,
  TABLE *table,
  const THD *thd,
  ha_spider *spider,
  int *error_num
);

int spider_free_share(
  SPIDER_SHARE *share
);

#ifdef WITH_PARTITION_STORAGE_ENGINE
SPIDER_PARTITION_SHARE *spider_get_pt_share(
  TABLE *table,
  SPIDER_SHARE *share,
  int *error_num
);

int spider_free_pt_share(
  SPIDER_PARTITION_SHARE *partition_share
);

void spider_copy_sts_to_pt_share(
  SPIDER_PARTITION_SHARE *partition_share,
  SPIDER_SHARE *share
);

void spider_copy_sts_to_share(
  SPIDER_SHARE *share,
  SPIDER_PARTITION_SHARE *partition_share
);

void spider_copy_crd_to_pt_share(
  SPIDER_PARTITION_SHARE *partition_share,
  SPIDER_SHARE *share,
  int fields
);

void spider_copy_crd_to_share(
  SPIDER_SHARE *share,
  SPIDER_PARTITION_SHARE *partition_share,
  int fields
);
#endif

int spider_open_all_tables(
  SPIDER_TRX *trx,
  bool lock
);

bool spider_flush_logs(
  handlerton *hton
);

handler* spider_create_handler(
  handlerton *hton,
  TABLE_SHARE *table, 
  MEM_ROOT *mem_root
);

int spider_close_connection(
  handlerton*	hton,
  THD* thd
);

void spider_drop_database(
  handlerton *hton,
  char* path
);

bool spider_show_status(
  handlerton *hton,
  THD *thd,
  stat_print_fn *stat_print,
  enum ha_stat_type stat_type
);

int spider_db_done(
  void *p
);

int spider_panic(
  handlerton *hton,
  ha_panic_function type
);

int spider_db_init(
  void *p
);

char *spider_create_table_name_string(
  const char *table_name,
  const char *part_name,
  const char *sub_name
);

#ifdef WITH_PARTITION_STORAGE_ENGINE
void spider_get_partition_info(
  const char *table_name,
  const TABLE *table,
  partition_element **part_elem,
  partition_element **sub_elem
);
#endif

int spider_get_sts(
  SPIDER_SHARE *share,
  time_t tmp_time,
  ha_spider *spider,
  int sts_sync
);

int spider_get_crd(
  SPIDER_SHARE *share,
  time_t tmp_time,
  ha_spider *spider,
  TABLE *table,
  int crd_sync
);
