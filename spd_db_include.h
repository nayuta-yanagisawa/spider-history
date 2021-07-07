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
#define SPIDER_HS_RESULT dena::hstresult
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
struct st_spider_result;

class spider_string
{
public:
  bool mem_calc_inited;
  String str;
  uint id;
  const char *func_name;
  const char *file_name;
  ulong line_no;
  uint32 current_alloc_mem;

  spider_string();
  spider_string(
    uint32 length_arg
  );
  spider_string(
    const char *str,
    CHARSET_INFO *cs
  );
  spider_string(
    const char *str,
    uint32 len,
    CHARSET_INFO *cs
  );
  spider_string(
    char *str,
    uint32 len,
    CHARSET_INFO *cs
  );
  spider_string(
    const String &str
  );
  ~spider_string();
  void init_mem_calc(
    uint id,
    const char *func_name,
    const char *file_name,
    ulong line_no
  );
  void mem_calc();
  String *get_str();
  void set_charset(
    CHARSET_INFO *charset_arg
  );
  CHARSET_INFO *charset() const;
  uint32 length() const;
  uint32 alloced_length() const;
  char &operator [] (
    uint32 i
  ) const;
  void length(
    uint32 len
  );
  bool is_empty() const;
  const char *ptr() const;
  char *c_ptr();
  char *c_ptr_quick();
  char *c_ptr_safe();
  LEX_STRING lex_string() const;
  void set(
    String &str,
    uint32 offset,
    uint32 arg_length
  );
  void set(
    char *str,
    uint32 arg_length,
    CHARSET_INFO *cs
  );
  void set(
    const char *str,
    uint32 arg_length,
    CHARSET_INFO *cs
  );
  bool set_ascii(
    const char *str,
    uint32 arg_length
  );
  void set_quick(
    char *str,
    uint32 arg_length,
    CHARSET_INFO *cs
  );
  bool set_int(
    longlong num,
    bool unsigned_flag,
    CHARSET_INFO *cs
  );
  bool set(
    longlong num,
    CHARSET_INFO *cs
  );
  bool set(
    ulonglong num,
    CHARSET_INFO *cs
  );
  bool set_real(
    double num,
    uint decimals,
    CHARSET_INFO *cs
  );
  void chop();
  void free();
  bool alloc(
    uint32 arg_length
  );
  bool real_alloc(
    uint32 arg_length
  );
  bool realloc(
    uint32 arg_length
  );
  void shrink(
    uint32 arg_length
  );
  bool is_alloced();
  spider_string& operator = (
    const String &s
  );
  bool copy();
  bool copy(
    const spider_string &s
  );
  bool copy(
    const String &s
  );
  bool copy(
    const char *s,
    uint32 arg_length,
    CHARSET_INFO *cs
  );
  bool needs_conversion(
    uint32 arg_length,
    CHARSET_INFO *cs_from,
    CHARSET_INFO *cs_to,
    uint32 *offset
  );
  bool copy_aligned(
    const char *s,
    uint32 arg_length,
    uint32 offset,
    CHARSET_INFO *cs
  );
  bool set_or_copy_aligned(
    const char *s,
    uint32 arg_length,
    CHARSET_INFO *cs
  );
  bool copy(
    const char *s,
    uint32 arg_length,
    CHARSET_INFO *csfrom,
    CHARSET_INFO *csto,
    uint *errors
  );
  bool append(
    const spider_string &s
  );
  bool append(
    const String &s
  );
  bool append(
    const char *s
  );
  bool append(
    LEX_STRING *ls
  );
  bool append(
    const char *s,
    uint32 arg_length
  );
  bool append(
    const char *s,
    uint32 arg_length,
    CHARSET_INFO *cs
  );
  bool append_ulonglong(
    ulonglong val
  );
  bool append(
    IO_CACHE *file,
    uint32 arg_length
  );
  bool append_with_prefill(
    const char *s,
    uint32 arg_length,
    uint32 full_length,
    char fill_char
  );
  int strstr(
    const String &search,
    uint32 offset = 0
  );
  int strrstr(
    const String &search,
    uint32 offset = 0
  );
  bool replace(
    uint32 offset,
    uint32 arg_length,
    const char *to,
    uint32 length
  );
  bool replace(
    uint32 offset,
    uint32 arg_length,
    const String &to
  );
  inline bool append(
    char chr
  );
  bool fill(
    uint32 max_length,
    char fill
  );
  void strip_sp();
  uint32 numchars();
  int charpos(
    int i,
    uint32 offset=0
  );
  int reserve(
    uint32 space_needed
  );
  int reserve(
    uint32 space_needed,
    uint32 grow_by
  );
  void q_append(
    const char c
  );
  void q_append(
    const uint32 n
  );
  void q_append(
    double d
  );
  void q_append(
    double *d
  );
  void q_append(
    const char *data,
    uint32 data_len
  );
  void write_at_position(
    int position,
    uint32 value
  );
  void qs_append(
    const char *str,
    uint32 len
  );
  void qs_append(
    double d
  );
  void qs_append(
    double *d
  );
  void qs_append(
    const char c
  );
  void qs_append(
    int i
  );
  void qs_append(
    uint i
  );
  char *prep_append(
    uint32 arg_length,
    uint32 step_alloc
  );
  bool append(
    const char *s,
    uint32 arg_length,
    uint32 step_alloc
  );
  void print(
    String *print
  );
  void swap(
    spider_string &s
  );
  bool uses_buffer_owned_by(
    const String *s
  ) const;
  bool is_ascii() const;
};

typedef struct st_spider_position
{
  SPIDER_DB_ROW          row;
  ulong                  *lengths;
  bool                   use_position;
  bool                   mrr_with_cnt;
  uint                   sql_kind;
  uchar                  *position_bitmap;
  st_spider_ft_info      *ft_first;
  st_spider_ft_info      *ft_current;
  my_off_t               tmp_tbl_pos;
  st_spider_result       *result;
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
  int                  pos_page_size; /* for quick mode */
  longlong             record_num;
  bool                 finish_flg;
  bool                 use_position;
  uint                 field_count; /* for quick mode */
  TABLE                *result_tmp_tbl;
  TMP_TABLE_PARAM      result_tmp_tbl_prm;
  THD                  *result_tmp_tbl_thd;
  uint                 result_tmp_tbl_inited;
  SPIDER_DB_ROW        tmp_tbl_row;
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
  spider_string          sql;
  spider_string          sql_part;
  spider_string          sql_part2;
  spider_string          ha_sql;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  spider_string          hs_sql;
  bool                   hs_adding_keys;
#ifndef HANDLERSOCKET_MYSQL_UTIL
  SPIDER_HS_VECTOR<SPIDER_HS_STRING_REF> hs_keys;
  SPIDER_HS_VECTOR<SPIDER_HS_STRING_REF> hs_upds;
#else
  bool                   hs_da_init;
  DYNAMIC_ARRAY          hs_keys;
  uint                   hs_keys_id;
  const char             *hs_keys_func_name;
  const char             *hs_keys_file_name;
  ulong                  hs_keys_line_no;
  DYNAMIC_ARRAY          hs_upds;
  uint                   hs_upds_id;
  const char             *hs_upds_func_name;
  const char             *hs_upds_file_name;
  ulong                  hs_upds_line_no;
#endif
  DYNAMIC_ARRAY          hs_strs;
  uint                   hs_strs_id;
  const char             *hs_strs_func_name;
  const char             *hs_strs_file_name;
  ulong                  hs_strs_line_no;
  uint                   hs_strs_pos;
  int                    hs_limit;
  int                    hs_skip;
  ulonglong              hs_upd_rows;
  SPIDER_HS_RESULT       hs_result;
  bool                   hs_has_result;
  SPIDER_HS_CONN         *hs_conn;
#endif
  spider_string          *sqls;
  int                    where_pos;
  int                    order_pos;
  int                    limit_pos;
  int                    table_name_pos;
  int                    ha_read_kind;
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
  spider_string          insert_sql;
  spider_string          *insert_sqls;
  int                    insert_pos;
  int                    insert_table_name_pos;
  spider_string          update_sql;
  spider_string          *update_sqls;
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
  spider_string          tmp_sql;
  spider_string          *tmp_sqls;
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
