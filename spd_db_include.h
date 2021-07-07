/* Copyright (C) 2008-2011 Kentoku Shiba

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

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#define SPIDER_HS_CONN dena::hstcpcli_ptr
#define SPIDER_HS_CONN_CREATE dena::hstcpcli_i::create
#define SPIDER_HS_SOCKARGS dena::socket_args
#define SPIDER_HS_UINT32_INFO dena::uint32_info
#ifndef HANDLERSOCKET_MYSQL_UTIL
#define SPIDER_HS_VECTOR std::vector
#endif
#define SPIDER_HS_STRING_REF dena::string_ref
#endif

#include <mysql.h>

#define SPIDER_DB_CONN MYSQL
#define SPIDER_DB_WRAPPER_MYSQL "mysql"
#define SPIDER_DB_RESULT MYSQL_RES
#define SPIDER_DB_ROW MYSQL_ROW
#define SPIDER_DB_ROW_OFFSET MYSQL_ROW_OFFSET

#define SPIDER_CONN_KIND_MYSQL (1 << 0)
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#define SPIDER_CONN_KIND_HS_READ (1 << 2)
#define SPIDER_CONN_KIND_HS_WRITE (1 << 3)
#endif

#define SPIDER_SQL_KIND_SQL (1 << 0)
#define SPIDER_SQL_KIND_HANDLER (1 << 1)
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#define SPIDER_SQL_KIND_HS (1 << 2)
#endif

enum spider_bulk_upd_start {
  SPD_BU_NOT_START,
  SPD_BU_START_BY_INDEX_OR_RND_INIT,
  SPD_BU_START_BY_BULK_INIT
};

struct st_spider_ft_info;
typedef struct st_spider_position
{
  SPIDER_DB_ROW          row;
  ulong                  *lengths;
  bool                   use_position;
  bool                   mrr_with_cnt;
  uchar                  *position_bitmap;
  st_spider_ft_info      *ft_first;
  st_spider_ft_info      *ft_current;
} SPIDER_POSITION;

typedef struct st_spider_condition
{
  COND                   *cond;
  st_spider_condition    *next;
} SPIDER_CONDITION;

typedef struct st_spider_result
{
  SPIDER_DB_RESULT     *result;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  volatile
#endif
    st_spider_result   *prev;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  volatile
#endif
    st_spider_result   *next;
  SPIDER_DB_ROW_OFFSET first_row;
  SPIDER_POSITION      *first_position; /* for quick mode */
  longlong             record_num;
  bool                 finish_flg;
  bool                 use_position;
} SPIDER_RESULT;

typedef struct st_spider_result_list
{
#ifndef WITHOUT_SPIDER_BG_SEARCH
  volatile
#endif
    SPIDER_RESULT        *first;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  volatile
#endif
    SPIDER_RESULT        *last;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  volatile
#endif
    SPIDER_RESULT        *current;
  KEY                    *key_info;
  int                    key_order;
  String                 sql;
  String                 sql_part;
  String                 ha_sql;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  String                 hs_sql;
  bool                   hs_adding_keys;
#ifndef HANDLERSOCKET_MYSQL_UTIL
  SPIDER_HS_VECTOR<SPIDER_HS_STRING_REF> hs_keys;
  SPIDER_HS_VECTOR<SPIDER_HS_STRING_REF> hs_upds;
#else
  bool                   hs_da_init;
  DYNAMIC_ARRAY          hs_keys;
  DYNAMIC_ARRAY          hs_upds;
#endif
  DYNAMIC_ARRAY          hs_strs;
  uint                   hs_strs_pos;
  int                    hs_limit;
  int                    hs_skip;
  ulonglong              hs_upd_rows;
  bool                   hs_has_result;
  SPIDER_HS_CONN         *hs_conn;
#endif
  String                 *sqls;
  int                    where_pos;
  int                    order_pos;
  int                    limit_pos;
  int                    table_name_pos;
  int                    ha_read_pos;
  int                    ha_next_pos;
  int                    ha_where_pos;
  int                    ha_limit_pos;
  int                    ha_table_name_pos;
  uint                   ha_sql_handler_id;
  bool                   have_sql_kind_backup;
  uint                   *sql_kind_backup;
  uint                   sql_kinds_backup;
  bool                   use_union;
  String                 insert_sql;
  String                 *insert_sqls;
  int                    insert_pos;
  int                    insert_table_name_pos;
  String                 update_sql;
  String                 *update_sqls;
  TABLE                  *upd_tmp_tbl;
  TABLE                  **upd_tmp_tbls;
  TMP_TABLE_PARAM        upd_tmp_tbl_prm;
  TMP_TABLE_PARAM        *upd_tmp_tbl_prms;
  bool                   tmp_table_join;
  uchar                  *tmp_table_join_first;
  bool                   tmp_tables_created;
  uchar                  *tmp_table_created;
  bool                   tmp_table_join_break_after_get_next;
  key_part_map           tmp_table_join_key_part_map;
  String                 tmp_sql;
  String                 *tmp_sqls;
  bool                   tmp_reuse_sql;
  int                    tmp_sql_pos1; /* drop db nm pos at tmp_table_join */
  int                    tmp_sql_pos2; /* create db nm pos at tmp_table_join */
  int                    tmp_sql_pos3; /* insert db nm pos at tmp_table_join */
  int                    tmp_sql_pos4; /* insert val pos at tmp_table_join */
  int                    tmp_sql_pos5; /* end of drop tbl at tmp_table_join */
  bool                   sorted;
  bool                   desc_flg;
  longlong               current_row_num;
  longlong               record_num;
  bool                   finish_flg;
  longlong               limit_num;
  longlong               internal_offset;
  longlong               internal_limit;
  longlong               split_read;
  int                    multi_split_read;
  int                    max_order;
  int                    quick_mode;
  longlong               quick_page_size;
  int                    low_mem_read;
  int                    bulk_update_mode;
  int                    bulk_update_size;
  spider_bulk_upd_start  bulk_update_start;
  bool                   check_direct_order_limit;
  bool                   direct_order_limit;
  bool                   set_split_read;
  longlong               split_read_base;
  double                 semi_split_read;
  longlong               semi_split_read_limit;
  longlong               semi_split_read_base;
  longlong               first_read;
  longlong               second_read;
  int                    set_split_read_count;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  /* 0:nomal 1:store 2:store end */
  volatile
#endif
    int                  quick_phase;
  bool                   keyread;
  int                    lock_type;
  TABLE                  *table;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  volatile int           bgs_error;
  volatile bool          bgs_working;
  /* 0:not use bg 1:first read 2:second read 3:after second read */
  volatile int           bgs_phase;
  volatile longlong      bgs_first_read;
  volatile longlong      bgs_second_read;
  volatile longlong      bgs_split_read;
  volatile
#endif
    SPIDER_RESULT        *bgs_current;
} SPIDER_RESULT_LIST;
