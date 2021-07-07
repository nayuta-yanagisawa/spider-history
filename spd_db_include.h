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

#include <mysql.h>

#define SPIDER_DB_CONN MYSQL
#define SPIDER_DB_WRAPPER_MYSQL "mysql"
#define SPIDER_DB_RESULT MYSQL_RES
#define SPIDER_DB_ROW MYSQL_ROW
#define SPIDER_DB_ROW_OFFSET MYSQL_ROW_OFFSET

typedef struct st_spider_result
{
  SPIDER_DB_RESULT     *result;
  st_spider_result     *prev;
  st_spider_result     *next;
  SPIDER_DB_ROW_OFFSET first_row;
  longlong             record_num;
  bool                 finish_flg;
} SPIDER_RESULT;

typedef struct st_spider_result_list
{
  SPIDER_RESULT        *first;
  SPIDER_RESULT        *last;
  SPIDER_RESULT        *current;
  KEY                  *key_info;
  int                  key_order;
  String               sql;
  int                  where_pos;
  int                  order_pos;
  int                  limit_pos;
  String               insert_sql;
  int                  insert_pos;
  bool                 sorted;
  bool                 desc_flg;
  longlong             current_row_num;
  longlong             record_num;
  bool                 finish_flg;
  longlong             internal_offset;
  longlong             internal_limit;
  longlong             split_read;
  int                  multi_split_read;
  int                  max_order;
  bool                 keyread;
  int                  lock_type;
} SPIDER_RESULT_LIST;

typedef struct st_spider_position
{
  SPIDER_DB_ROW        row;
  ulong                *lengths;
} SPIDER_POSITION;
