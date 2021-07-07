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

#include <mysql.h>

#define SPIDER_DB_CONN MYSQL
#define SPIDER_DB_WRAPPER_MYSQL "mysql"
#define SPIDER_DB_RESULT MYSQL_RES
#define SPIDER_DB_ROW MYSQL_ROW
#define SPIDER_DB_ROW_OFFSET MYSQL_ROW_OFFSET

typedef struct st_spider_position
{
  SPIDER_DB_ROW          row;
  ulong                  *lengths;
  bool                   use_position;
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
  String                 *sqls;
  int                    where_pos;
  int                    order_pos;
  int                    limit_pos;
  int                    table_name_pos;
  String                 insert_sql;
  String                 *insert_sqls;
  int                    insert_pos;
  int                    insert_table_name_pos;
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
