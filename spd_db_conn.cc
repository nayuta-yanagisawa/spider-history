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

#define MYSQL_SERVER 1
#include "mysql_version.h"
#ifdef HAVE_HANDLERSOCKET
#include "hstcpcli.hpp"
#endif
#if MYSQL_VERSION_ID < 50500
#include "mysql_priv.h"
#include <mysql/plugin.h>
#else
#include "sql_priv.h"
#include "probes_mysql.h"
#include "sql_class.h"
#include "sql_partition.h"
#include "sql_analyse.h"
#include "sql_base.h"
#include "tztime.h"
#endif
#include "sql_common.h"
#include <mysql.h>
#include <errmsg.h>
#include "spd_err.h"
#include "spd_param.h"
#include "spd_db_include.h"
#include "spd_include.h"
#include "spd_sys_table.h"
#include "ha_spider.h"
#include "spd_db_conn.h"
#include "spd_table.h"
#include "spd_trx.h"
#include "spd_conn.h"
#include "spd_direct_sql.h"
#include "spd_ping_table.h"
#include "spd_copy_tables.h"
#include "spd_malloc.h"

extern handlerton *spider_hton_ptr;

#define SPIDER_SQL_NAME_QUOTE_STR "`"
#define SPIDER_SQL_NAME_QUOTE_LEN (sizeof(SPIDER_SQL_NAME_QUOTE_STR) - 1)
const char *name_quote_str = SPIDER_SQL_NAME_QUOTE_STR;
#define SPIDER_SQL_VALUE_QUOTE_STR "'"
#define SPIDER_SQL_VALUE_QUOTE_LEN (sizeof(SPIDER_SQL_VALUE_QUOTE_STR) - 1)
#define SPIDER_SQL_DOT_STR "."
#define SPIDER_SQL_DOT_LEN (sizeof(SPIDER_SQL_DOT_STR) - 1)
#define SPIDER_SQL_SEMICOLON_STR ";"
#define SPIDER_SQL_SEMICOLON_LEN sizeof(SPIDER_SQL_SEMICOLON_STR) - 1
#define SPIDER_SQL_UNDERSCORE_STR "_"
#define SPIDER_SQL_UNDERSCORE_LEN sizeof(SPIDER_SQL_UNDERSCORE_STR) - 1
#define SPIDER_SQL_EQUAL_STR " = "
#define SPIDER_SQL_EQUAL_LEN (sizeof(SPIDER_SQL_EQUAL_STR) - 1)
#define SPIDER_SQL_PF_EQUAL_STR " <=> "
#define SPIDER_SQL_PF_EQUAL_LEN (sizeof(SPIDER_SQL_PF_EQUAL_STR) - 1)
#define SPIDER_SQL_GT_STR " > "
#define SPIDER_SQL_GT_LEN (sizeof(SPIDER_SQL_GT_STR) - 1)
#define SPIDER_SQL_GTEQUAL_STR " >= "
#define SPIDER_SQL_GTEQUAL_LEN (sizeof(SPIDER_SQL_GTEQUAL_STR) - 1)
#define SPIDER_SQL_LT_STR " < "
#define SPIDER_SQL_LT_LEN (sizeof(SPIDER_SQL_LT_STR) - 1)
#define SPIDER_SQL_LTEQUAL_STR " <= "
#define SPIDER_SQL_LTEQUAL_LEN (sizeof(SPIDER_SQL_LTEQUAL_STR) - 1)
#define SPIDER_SQL_NULL_STR "null"
#define SPIDER_SQL_NULL_LEN (sizeof(SPIDER_SQL_NULL_STR) - 1)
#define SPIDER_SQL_IS_NULL_STR " is null"
#define SPIDER_SQL_IS_NULL_LEN (sizeof(SPIDER_SQL_IS_NULL_STR) - 1)
#define SPIDER_SQL_IS_NOT_NULL_STR " is not null"
#define SPIDER_SQL_IS_NOT_NULL_LEN (sizeof(SPIDER_SQL_IS_NOT_NULL_STR) - 1)
#define SPIDER_SQL_IS_TRUE_STR " is true"
#define SPIDER_SQL_IS_TRUE_LEN (sizeof(SPIDER_SQL_IS_TRUE_STR) - 1)
#define SPIDER_SQL_IS_NOT_TRUE_STR " is not true"
#define SPIDER_SQL_IS_NOT_TRUE_LEN (sizeof(SPIDER_SQL_IS_NOT_TRUE_STR) - 1)
#define SPIDER_SQL_IS_FALSE_STR " is false"
#define SPIDER_SQL_IS_FALSE_LEN (sizeof(SPIDER_SQL_IS_FALSE_STR) - 1)
#define SPIDER_SQL_IS_NOT_FALSE_STR " is not false"
#define SPIDER_SQL_IS_NOT_FALSE_LEN (sizeof(SPIDER_SQL_IS_NOT_FALSE_STR) - 1)
#define SPIDER_SQL_LIKE_STR " like "
#define SPIDER_SQL_LIKE_LEN (sizeof(SPIDER_SQL_LIKE_STR) - 1)
#define SPIDER_SQL_IN_STR "in("
#define SPIDER_SQL_IN_LEN (sizeof(SPIDER_SQL_IN_STR) - 1)
#define SPIDER_SQL_NOT_IN_STR "not in("
#define SPIDER_SQL_NOT_IN_LEN (sizeof(SPIDER_SQL_NOT_IN_STR) - 1)
#define SPIDER_SQL_COALESCE_STR "coalesce("
#define SPIDER_SQL_COALESCE_LEN (sizeof(SPIDER_SQL_COALESCE_STR) - 1)
#define SPIDER_SQL_NOT_BETWEEN_STR "not between"
#define SPIDER_SQL_NOT_BETWEEN_LEN (sizeof(SPIDER_SQL_NOT_BETWEEN_STR) - 1)
#define SPIDER_SQL_AND_STR " and "
#define SPIDER_SQL_AND_LEN (sizeof(SPIDER_SQL_AND_STR) - 1)
#define SPIDER_SQL_OR_STR " or "
#define SPIDER_SQL_OR_LEN (sizeof(SPIDER_SQL_OR_STR) - 1)
#define SPIDER_SQL_SPACE_STR " "
#define SPIDER_SQL_SPACE_LEN (sizeof(SPIDER_SQL_SPACE_STR) - 1)
#define SPIDER_SQL_NULL_CHAR_STR ""
#define SPIDER_SQL_NULL_CHAR_LEN (sizeof(SPIDER_SQL_NULL_CHAR_STR) - 1)
#define SPIDER_SQL_HEX_STR "0x"
#define SPIDER_SQL_HEX_LEN (sizeof(SPIDER_SQL_HEX_STR) - 1)
#define SPIDER_SQL_SELECT_STR "select "
#define SPIDER_SQL_SELECT_LEN (sizeof(SPIDER_SQL_SELECT_STR) - 1)
#define SPIDER_SQL_INSERT_STR "insert "
#define SPIDER_SQL_INSERT_LEN (sizeof(SPIDER_SQL_INSERT_STR) - 1)
#define SPIDER_SQL_REPLACE_STR "replace "
#define SPIDER_SQL_REPLACE_LEN (sizeof(SPIDER_SQL_REPLACE_STR) - 1)
#define SPIDER_SQL_UPDATE_STR "update "
#define SPIDER_SQL_UPDATE_LEN (sizeof(SPIDER_SQL_UPDATE_STR) - 1)
#define SPIDER_SQL_DELETE_STR "delete "
#define SPIDER_SQL_DELETE_LEN (sizeof(SPIDER_SQL_DELETE_STR) - 1)
#define SPIDER_SQL_FROM_STR " from "
#define SPIDER_SQL_FROM_LEN (sizeof(SPIDER_SQL_FROM_STR) - 1)
#define SPIDER_SQL_INTO_STR "into "
#define SPIDER_SQL_INTO_LEN (sizeof(SPIDER_SQL_INTO_STR) - 1)
#define SPIDER_SQL_SET_STR " set "
#define SPIDER_SQL_SET_LEN (sizeof(SPIDER_SQL_SET_STR) - 1)
#define SPIDER_SQL_WHERE_STR " where "
#define SPIDER_SQL_WHERE_LEN (sizeof(SPIDER_SQL_WHERE_STR) - 1)
#define SPIDER_SQL_ORDER_STR " order by "
#define SPIDER_SQL_ORDER_LEN (sizeof(SPIDER_SQL_ORDER_STR) - 1)
#define SPIDER_SQL_GROUP_STR " group by "
#define SPIDER_SQL_GROUP_LEN (sizeof(SPIDER_SQL_GROUP_STR) - 1)
#define SPIDER_SQL_DESC_STR " desc"
#define SPIDER_SQL_DESC_LEN (sizeof(SPIDER_SQL_DESC_STR) - 1)
#define SPIDER_SQL_LIMIT_STR " limit "
#define SPIDER_SQL_LIMIT_LEN (sizeof(SPIDER_SQL_LIMIT_STR) - 1)
#define SPIDER_SQL_LIMIT1_STR " limit 1"
#define SPIDER_SQL_LIMIT1_LEN (sizeof(SPIDER_SQL_LIMIT1_STR) - 1)
#define SPIDER_SQL_VALUES_STR ")values"
#define SPIDER_SQL_VALUES_LEN (sizeof(SPIDER_SQL_VALUES_STR) - 1)
#define SPIDER_SQL_SHARED_LOCK_STR " lock in share mode"
#define SPIDER_SQL_SHARED_LOCK_LEN (sizeof(SPIDER_SQL_SHARED_LOCK_STR) - 1)
#define SPIDER_SQL_FOR_UPDATE_STR " for update"
#define SPIDER_SQL_FOR_UPDATE_LEN (sizeof(SPIDER_SQL_FOR_UPDATE_STR) - 1)
#define SPIDER_SQL_LOCK_TABLE_STR "lock tables "
#define SPIDER_SQL_LOCK_TABLE_LEN (sizeof(SPIDER_SQL_LOCK_TABLE_STR) - 1)
#define SPIDER_SQL_UNLOCK_TABLE_STR "unlock tables"
#define SPIDER_SQL_UNLOCK_TABLE_LEN (sizeof(SPIDER_SQL_UNLOCK_TABLE_STR) - 1)
#define SPIDER_SQL_TRUNCATE_TABLE_STR "truncate table "
#define SPIDER_SQL_TRUNCATE_TABLE_LEN (sizeof(SPIDER_SQL_TRUNCATE_TABLE_STR) - 1)
#define SPIDER_SQL_HIGH_PRIORITY_STR "high_priority "
#define SPIDER_SQL_HIGH_PRIORITY_LEN (sizeof(SPIDER_SQL_HIGH_PRIORITY_STR) - 1)
#define SPIDER_SQL_LOW_PRIORITY_STR "low_priority "
#define SPIDER_SQL_LOW_PRIORITY_LEN (sizeof(SPIDER_SQL_LOW_PRIORITY_STR) - 1)
#define SPIDER_SQL_SQL_CACHE_STR "sql_cache "
#define SPIDER_SQL_SQL_CACHE_LEN (sizeof(SPIDER_SQL_SQL_CACHE_STR) - 1)
#define SPIDER_SQL_SQL_NO_CACHE_STR "sql_no_cache "
#define SPIDER_SQL_SQL_NO_CACHE_LEN (sizeof(SPIDER_SQL_SQL_NO_CACHE_STR) - 1)
#define SPIDER_SQL_SQL_IGNORE_STR "ignore "
#define SPIDER_SQL_SQL_IGNORE_LEN (sizeof(SPIDER_SQL_SQL_IGNORE_STR) - 1)
#define SPIDER_SQL_SQL_DELAYED_STR "delayed "
#define SPIDER_SQL_SQL_DELAYED_LEN (sizeof(SPIDER_SQL_SQL_DELAYED_STR) - 1)
#define SPIDER_SQL_SQL_QUICK_MODE_STR "quick "
#define SPIDER_SQL_SQL_QUICK_MODE_LEN (sizeof(SPIDER_SQL_SQL_QUICK_MODE_STR) - 1)
#define SPIDER_SQL_SQL_ALTER_TABLE_STR "alter table "
#define SPIDER_SQL_SQL_ALTER_TABLE_LEN (sizeof(SPIDER_SQL_SQL_ALTER_TABLE_STR) - 1)
#define SPIDER_SQL_SQL_DISABLE_KEYS_STR " disable keys"
#define SPIDER_SQL_SQL_DISABLE_KEYS_LEN (sizeof(SPIDER_SQL_SQL_DISABLE_KEYS_STR) - 1)
#define SPIDER_SQL_SQL_ENABLE_KEYS_STR " enable keys"
#define SPIDER_SQL_SQL_ENABLE_KEYS_LEN (sizeof(SPIDER_SQL_SQL_ENABLE_KEYS_STR) - 1)
#define SPIDER_SQL_SQL_CHECK_TABLE_STR "check table "
#define SPIDER_SQL_SQL_CHECK_TABLE_LEN (sizeof(SPIDER_SQL_SQL_CHECK_TABLE_STR) - 1)
#define SPIDER_SQL_SQL_ANALYZE_STR "analyze "
#define SPIDER_SQL_SQL_ANALYZE_LEN (sizeof(SPIDER_SQL_SQL_ANALYZE_STR) - 1)
#define SPIDER_SQL_SQL_OPTIMIZE_STR "optimize "
#define SPIDER_SQL_SQL_OPTIMIZE_LEN (sizeof(SPIDER_SQL_SQL_OPTIMIZE_STR) - 1)
#define SPIDER_SQL_SQL_REPAIR_STR "repair "
#define SPIDER_SQL_SQL_REPAIR_LEN (sizeof(SPIDER_SQL_SQL_REPAIR_STR) - 1)
#define SPIDER_SQL_SQL_TABLE_STR "table "
#define SPIDER_SQL_SQL_TABLE_LEN (sizeof(SPIDER_SQL_SQL_TABLE_STR) - 1)
#define SPIDER_SQL_SQL_LOCAL_STR "local "
#define SPIDER_SQL_SQL_LOCAL_LEN (sizeof(SPIDER_SQL_SQL_LOCAL_STR) - 1)
#define SPIDER_SQL_SQL_QUICK_STR " quick"
#define SPIDER_SQL_SQL_QUICK_LEN (sizeof(SPIDER_SQL_SQL_QUICK_STR) - 1)
#define SPIDER_SQL_SQL_FAST_STR " fast"
#define SPIDER_SQL_SQL_FAST_LEN (sizeof(SPIDER_SQL_SQL_FAST_STR) - 1)
#define SPIDER_SQL_SQL_MEDIUM_STR " medium"
#define SPIDER_SQL_SQL_MEDIUM_LEN (sizeof(SPIDER_SQL_SQL_MEDIUM_STR) - 1)
#define SPIDER_SQL_SQL_EXTENDED_STR " extended"
#define SPIDER_SQL_SQL_EXTENDED_LEN (sizeof(SPIDER_SQL_SQL_EXTENDED_STR) - 1)
#define SPIDER_SQL_SQL_USE_FRM_STR " use_frm"
#define SPIDER_SQL_SQL_USE_FRM_LEN (sizeof(SPIDER_SQL_SQL_USE_FRM_STR) - 1)
#define SPIDER_SQL_SQL_FORCE_IDX_STR " force index("
#define SPIDER_SQL_SQL_FORCE_IDX_LEN (sizeof(SPIDER_SQL_SQL_FORCE_IDX_STR) - 1)
#define SPIDER_SQL_SQL_USE_IDX_STR " use index("
#define SPIDER_SQL_SQL_USE_IDX_LEN (sizeof(SPIDER_SQL_SQL_USE_IDX_STR) - 1)
#define SPIDER_SQL_SQL_IGNORE_IDX_STR " ignore index("
#define SPIDER_SQL_SQL_IGNORE_IDX_LEN (sizeof(SPIDER_SQL_SQL_IGNORE_IDX_STR) - 1)
#define SPIDER_SQL_MATCH_STR "match("
#define SPIDER_SQL_MATCH_LEN (sizeof(SPIDER_SQL_MATCH_STR) - 1)
#define SPIDER_SQL_AGAINST_STR ")against("
#define SPIDER_SQL_AGAINST_LEN (sizeof(SPIDER_SQL_AGAINST_STR) - 1)
#define SPIDER_SQL_IN_BOOLEAN_MODE_STR " in boolean mode"
#define SPIDER_SQL_IN_BOOLEAN_MODE_LEN (sizeof(SPIDER_SQL_IN_BOOLEAN_MODE_STR) - 1)
#define SPIDER_SQL_WITH_QUERY_EXPANSION_STR " with query expansion"
#define SPIDER_SQL_WITH_QUERY_EXPANSION_LEN (sizeof(SPIDER_SQL_WITH_QUERY_EXPANSION_STR) - 1)
#define SPIDER_SQL_CAST_STR "cast("
#define SPIDER_SQL_CAST_LEN (sizeof(SPIDER_SQL_CAST_STR) - 1)
#define SPIDER_SQL_AS_SIGNED_STR " as signed"
#define SPIDER_SQL_AS_SIGNED_LEN (sizeof(SPIDER_SQL_AS_SIGNED_STR) - 1)
#define SPIDER_SQL_AS_UNSIGNED_STR " as unsigned"
#define SPIDER_SQL_AS_UNSIGNED_LEN (sizeof(SPIDER_SQL_AS_UNSIGNED_STR) - 1)
#define SPIDER_SQL_AS_DECIMAL_STR " as decimal"
#define SPIDER_SQL_AS_DECIMAL_LEN (sizeof(SPIDER_SQL_AS_DECIMAL_STR) - 1)
#define SPIDER_SQL_AS_CHAR_STR " as char"
#define SPIDER_SQL_AS_CHAR_LEN (sizeof(SPIDER_SQL_AS_CHAR_STR) - 1)
#define SPIDER_SQL_AS_DATE_STR " as date"
#define SPIDER_SQL_AS_DATE_LEN (sizeof(SPIDER_SQL_AS_DATE_STR) - 1)
#define SPIDER_SQL_AS_TIME_STR " as time"
#define SPIDER_SQL_AS_TIME_LEN (sizeof(SPIDER_SQL_AS_TIME_STR) - 1)
#define SPIDER_SQL_AS_DATETIME_STR " as datetime"
#define SPIDER_SQL_AS_DATETIME_LEN (sizeof(SPIDER_SQL_AS_DATETIME_STR) - 1)
#define SPIDER_SQL_AS_BINARY_STR " as binary"
#define SPIDER_SQL_AS_BINARY_LEN (sizeof(SPIDER_SQL_AS_BINARY_STR) - 1)
#define SPIDER_SQL_AS_STR "as "
#define SPIDER_SQL_AS_LEN (sizeof(SPIDER_SQL_AS_STR) - 1)
#define SPIDER_SQL_DATE_ADD_STR "adddate"
#define SPIDER_SQL_DATE_ADD_LEN (sizeof(SPIDER_SQL_DATE_ADD_STR) - 1)
#define SPIDER_SQL_HANDLER_STR "handler "
#define SPIDER_SQL_HANDLER_LEN (sizeof(SPIDER_SQL_HANDLER_STR) - 1)
#define SPIDER_SQL_OPEN_STR " open "
#define SPIDER_SQL_OPEN_LEN (sizeof(SPIDER_SQL_OPEN_STR) - 1)
#define SPIDER_SQL_CLOSE_STR " close "
#define SPIDER_SQL_CLOSE_LEN (sizeof(SPIDER_SQL_CLOSE_STR) - 1)
#define SPIDER_SQL_READ_STR " read "
#define SPIDER_SQL_READ_LEN (sizeof(SPIDER_SQL_READ_STR) - 1)
#define SPIDER_SQL_FIRST_STR " first "
#define SPIDER_SQL_FIRST_LEN (sizeof(SPIDER_SQL_FIRST_STR) - 1)
#define SPIDER_SQL_NEXT_STR " next  "
#define SPIDER_SQL_NEXT_LEN (sizeof(SPIDER_SQL_NEXT_STR) - 1)
#define SPIDER_SQL_PREV_STR " prev  "
#define SPIDER_SQL_PREV_LEN (sizeof(SPIDER_SQL_PREV_STR) - 1)
#define SPIDER_SQL_LAST_STR " last  "
#define SPIDER_SQL_LAST_LEN (sizeof(SPIDER_SQL_LAST_STR) - 1)
#define SPIDER_SQL_MBR_STR "mbr"
#define SPIDER_SQL_MBR_LEN (sizeof(SPIDER_SQL_MBR_STR) - 1)
#define SPIDER_SQL_MBR_CONTAIN_STR "mbrcontains("
#define SPIDER_SQL_MBR_CONTAIN_LEN (sizeof(SPIDER_SQL_MBR_CONTAIN_STR) - 1)
#define SPIDER_SQL_MBR_INTERSECT_STR "mbrintersects("
#define SPIDER_SQL_MBR_INTERSECT_LEN (sizeof(SPIDER_SQL_MBR_INTERSECT_STR) - 1)
#define SPIDER_SQL_MBR_WITHIN_STR "mbrwithin("
#define SPIDER_SQL_MBR_WITHIN_LEN (sizeof(SPIDER_SQL_MBR_WITHIN_STR) - 1)
#define SPIDER_SQL_MBR_DISJOINT_STR "mbrdisjoint("
#define SPIDER_SQL_MBR_DISJOINT_LEN (sizeof(SPIDER_SQL_MBR_DISJOINT_STR) - 1)
#define SPIDER_SQL_MBR_EQUAL_STR "mbrequal("
#define SPIDER_SQL_MBR_EQUAL_LEN (sizeof(SPIDER_SQL_MBR_EQUAL_STR) - 1)
uchar SPIDER_SQL_LINESTRING_HEAD_STR[] =
  {0x00,0x00,0x00,0x00,0x01,0x02,0x00,0x00,0x00,0x02,0x00,0x00,0x00};
#define SPIDER_SQL_LINESTRING_HEAD_LEN sizeof(SPIDER_SQL_LINESTRING_HEAD_STR)

#ifdef ITEM_FUNC_CASE_PARAMS_ARE_PUBLIC
#define SPIDER_SQL_CASE_STR "case "
#define SPIDER_SQL_CASE_LEN (sizeof(SPIDER_SQL_CASE_STR) - 1)
#define SPIDER_SQL_WHEN_STR " when "
#define SPIDER_SQL_WHEN_LEN (sizeof(SPIDER_SQL_WHEN_STR) - 1)
#define SPIDER_SQL_THEN_STR " then "
#define SPIDER_SQL_THEN_LEN (sizeof(SPIDER_SQL_THEN_STR) - 1)
#define SPIDER_SQL_ELSE_STR " else "
#define SPIDER_SQL_ELSE_LEN (sizeof(SPIDER_SQL_ELSE_STR) - 1)
#define SPIDER_SQL_END_STR " end"
#define SPIDER_SQL_END_LEN (sizeof(SPIDER_SQL_END_STR) - 1)
#endif

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#define SPIDER_SQL_HS_EQUAL_STR "="
#define SPIDER_SQL_HS_EQUAL_LEN (sizeof(SPIDER_SQL_HS_EQUAL_STR) - 1)
#define SPIDER_SQL_HS_GT_STR ">"
#define SPIDER_SQL_HS_GT_LEN (sizeof(SPIDER_SQL_HS_GT_STR) - 1)
#define SPIDER_SQL_HS_GTEQUAL_STR ">="
#define SPIDER_SQL_HS_GTEQUAL_LEN (sizeof(SPIDER_SQL_HS_GTEQUAL_STR) - 1)
#define SPIDER_SQL_HS_LT_STR "<"
#define SPIDER_SQL_HS_LT_LEN (sizeof(SPIDER_SQL_HS_LT_STR) - 1)
#define SPIDER_SQL_HS_LTEQUAL_STR "<="
#define SPIDER_SQL_HS_LTEQUAL_LEN (sizeof(SPIDER_SQL_HS_LTEQUAL_STR) - 1)
#define SPIDER_SQL_HS_INSERT_STR "+"
#define SPIDER_SQL_HS_INSERT_LEN (sizeof(SPIDER_SQL_HS_INSERT_STR) - 1)
#define SPIDER_SQL_HS_UPDATE_STR "U"
#define SPIDER_SQL_HS_UPDATE_LEN (sizeof(SPIDER_SQL_HS_UPDATE_STR) - 1)
#define SPIDER_SQL_HS_DELETE_STR "D"
#define SPIDER_SQL_HS_DELETE_LEN (sizeof(SPIDER_SQL_HS_DELETE_STR) - 1)
#define SPIDER_SQL_HS_INCREMENT_STR "+"
#define SPIDER_SQL_HS_INCREMENT_LEN (sizeof(SPIDER_SQL_HS_INCREMENT_STR) - 1)
#define SPIDER_SQL_HS_DECREMENT_STR "-"
#define SPIDER_SQL_HS_DECREMENT_LEN (sizeof(SPIDER_SQL_HS_DECREMENT_STR) - 1)
#endif

#define SPIDER_SQL_ISO_READ_UNCOMMITTED_STR "set session transaction isolation level read uncommitted"
#define SPIDER_SQL_ISO_READ_UNCOMMITTED_LEN sizeof(SPIDER_SQL_ISO_READ_UNCOMMITTED_STR) - 1
#define SPIDER_SQL_ISO_READ_COMMITTED_STR "set session transaction isolation level read committed"
#define SPIDER_SQL_ISO_READ_COMMITTED_LEN sizeof(SPIDER_SQL_ISO_READ_COMMITTED_STR) - 1
#define SPIDER_SQL_ISO_REPEATABLE_READ_STR "set session transaction isolation level repeatable read"
#define SPIDER_SQL_ISO_REPEATABLE_READ_LEN sizeof(SPIDER_SQL_ISO_REPEATABLE_READ_STR) - 1
#define SPIDER_SQL_ISO_SERIALIZABLE_STR "set session transaction isolation level serializable"
#define SPIDER_SQL_ISO_SERIALIZABLE_LEN sizeof(SPIDER_SQL_ISO_SERIALIZABLE_STR) - 1

#define SPIDER_SQL_START_CONSISTENT_SNAPSHOT_STR "start transaction with consistent snapshot"
#define SPIDER_SQL_START_CONSISTENT_SNAPSHOT_LEN sizeof(SPIDER_SQL_START_CONSISTENT_SNAPSHOT_STR) - 1
#define SPIDER_SQL_START_TRANSACTION_STR "start transaction"
#define SPIDER_SQL_START_TRANSACTION_LEN sizeof(SPIDER_SQL_START_TRANSACTION_STR) - 1

#define SPIDER_SQL_AUTOCOMMIT_OFF_STR "set session autocommit = 0"
#define SPIDER_SQL_AUTOCOMMIT_OFF_LEN sizeof(SPIDER_SQL_AUTOCOMMIT_OFF_STR) - 1
#define SPIDER_SQL_AUTOCOMMIT_ON_STR "set session autocommit = 1"
#define SPIDER_SQL_AUTOCOMMIT_ON_LEN sizeof(SPIDER_SQL_AUTOCOMMIT_ON_STR) - 1

#define SPIDER_SQL_SQL_LOG_OFF_STR "set session sql_log_off = 0"
#define SPIDER_SQL_SQL_LOG_OFF_LEN sizeof(SPIDER_SQL_SQL_LOG_OFF_STR) - 1
#define SPIDER_SQL_SQL_LOG_ON_STR "set session sql_log_off = 1"
#define SPIDER_SQL_SQL_LOG_ON_LEN sizeof(SPIDER_SQL_SQL_LOG_ON_STR) - 1

#define SPIDER_SQL_TIME_ZONE_STR "set session time_zone = '"
#define SPIDER_SQL_TIME_ZONE_LEN sizeof(SPIDER_SQL_TIME_ZONE_STR) - 1

#define SPIDER_SQL_COMMIT_STR "commit"
#define SPIDER_SQL_COMMIT_LEN sizeof(SPIDER_SQL_COMMIT_STR) - 1
#define SPIDER_SQL_ROLLBACK_STR "rollback"
#define SPIDER_SQL_ROLLBACK_LEN sizeof(SPIDER_SQL_ROLLBACK_STR) - 1

#define SPIDER_SQL_XA_START_STR "xa start "
#define SPIDER_SQL_XA_START_LEN sizeof(SPIDER_SQL_XA_START_STR) - 1
#define SPIDER_SQL_XA_END_STR "xa end "
#define SPIDER_SQL_XA_END_LEN sizeof(SPIDER_SQL_XA_END_STR) - 1
#define SPIDER_SQL_XA_PREPARE_STR "xa prepare "
#define SPIDER_SQL_XA_PREPARE_LEN sizeof(SPIDER_SQL_XA_PREPARE_STR) - 1
#define SPIDER_SQL_XA_COMMIT_STR "xa commit "
#define SPIDER_SQL_XA_COMMIT_LEN sizeof(SPIDER_SQL_XA_COMMIT_STR) - 1
#define SPIDER_SQL_XA_ROLLBACK_STR "xa rollback "
#define SPIDER_SQL_XA_ROLLBACK_LEN sizeof(SPIDER_SQL_XA_ROLLBACK_STR) - 1

#define SPIDER_SQL_SHOW_TABLE_STATUS_STR "show table status from "
#define SPIDER_SQL_SHOW_TABLE_STATUS_LEN sizeof(SPIDER_SQL_SHOW_TABLE_STATUS_STR) - 1
#define SPIDER_SQL_SELECT_TABLES_STATUS_STR "select `table_rows`,`avg_row_length`,`data_length`,`max_data_length`,`index_length`,`auto_increment`,`create_time`,`update_time`,`check_time` from `information_schema`.`tables` where `table_schema` = "
#define SPIDER_SQL_SELECT_TABLES_STATUS_LEN sizeof(SPIDER_SQL_SELECT_TABLES_STATUS_STR) - 1
#define SPIDER_SQL_SHOW_RECORDS_STR "select count(*) from "
#define SPIDER_SQL_SHOW_RECORDS_LEN sizeof(SPIDER_SQL_SHOW_RECORDS_STR) - 1
#define SPIDER_SQL_SHOW_INDEX_STR "show index from "
#define SPIDER_SQL_SHOW_INDEX_LEN sizeof(SPIDER_SQL_SHOW_INDEX_STR) - 1
#define SPIDER_SQL_SELECT_STATISTICS_STR "select `column_name`,`cardinality` from `information_schema`.`statistics` where `table_schema` = "
#define SPIDER_SQL_SELECT_STATISTICS_LEN sizeof(SPIDER_SQL_SELECT_STATISTICS_STR) - 1
#define SPIDER_SQL_TABLE_NAME_STR "`table_name`"
#define SPIDER_SQL_TABLE_NAME_LEN sizeof(SPIDER_SQL_TABLE_NAME_STR) - 1
#define SPIDER_SQL_COLUMN_NAME_STR "`column_name`"
#define SPIDER_SQL_COLUMN_NAME_LEN sizeof(SPIDER_SQL_COLUMN_NAME_STR) - 1
#define SPIDER_SQL_EXPLAIN_SELECT_STR "explain select 1 "
#define SPIDER_SQL_EXPLAIN_SELECT_LEN sizeof(SPIDER_SQL_EXPLAIN_SELECT_STR) - 1
#define SPIDER_SQL_SET_NAMES_STR "set names "
#define SPIDER_SQL_SET_NAMES_LEN sizeof(SPIDER_SQL_SET_NAMES_STR) - 1
#define SPIDER_SQL_FLUSH_TABLES_STR "flush tables"
#define SPIDER_SQL_FLUSH_TABLES_LEN sizeof(SPIDER_SQL_FLUSH_TABLES_STR) - 1
#define SPIDER_SQL_WITH_READ_LOCK_STR " with read lock"
#define SPIDER_SQL_WITH_READ_LOCK_LEN sizeof(SPIDER_SQL_WITH_READ_LOCK_STR) - 1
#define SPIDER_SQL_FLUSH_LOGS_STR "flush logs"
#define SPIDER_SQL_FLUSH_LOGS_LEN sizeof(SPIDER_SQL_FLUSH_LOGS_STR) - 1
#define SPIDER_SQL_ONE_STR "1"
#define SPIDER_SQL_ONE_LEN sizeof(SPIDER_SQL_ONE_STR) - 1

#define SPIDER_SQL_DROP_TMP_STR "drop temporary table if exists "
#define SPIDER_SQL_DROP_TMP_LEN (sizeof(SPIDER_SQL_DROP_TMP_STR) - 1)
#define SPIDER_SQL_CREATE_TMP_STR "create temporary table "
#define SPIDER_SQL_CREATE_TMP_LEN (sizeof(SPIDER_SQL_CREATE_TMP_STR) - 1)
#define SPIDER_SQL_TMP_BKA_STR "tmp_spider_bka_"
#define SPIDER_SQL_TMP_BKA_LEN (sizeof(SPIDER_SQL_TMP_BKA_STR) - 1)
#define SPIDER_SQL_ENGINE_STR ")engine="
#define SPIDER_SQL_ENGINE_LEN (sizeof(SPIDER_SQL_ENGINE_STR) - 1)
#define SPIDER_SQL_DEF_CHARSET_STR " default charset="
#define SPIDER_SQL_DEF_CHARSET_LEN (sizeof(SPIDER_SQL_DEF_CHARSET_STR) - 1)
#define SPIDER_SQL_ID_TYPE_STR " bigint"
#define SPIDER_SQL_ID_TYPE_LEN (sizeof(SPIDER_SQL_ID_TYPE_STR) - 1)

#define SPIDER_SQL_PING_TABLE_STR "spider_ping_table("
#define SPIDER_SQL_PING_TABLE_LEN (sizeof(SPIDER_SQL_PING_TABLE_STR) - 1)

pthread_mutex_t spider_open_conn_mutex;

const char *spider_db_table_lock_str[] =
{
  " read local,",
  " read,",
  " low_priority write,",
  " write,"
};
const int spider_db_table_lock_len[] =
{
  sizeof(" read local,") - 1,
  sizeof(" read,") - 1,
  sizeof(" low_priority write,") - 1,
  sizeof(" write,") - 1
};

int spider_db_connect(
  const SPIDER_SHARE *share,
  SPIDER_CONN *conn,
  int link_idx
) {
  int error_num, connect_retry_count;
  THD* thd = current_thd;
  longlong connect_retry_interval;
  my_bool connect_mutex = spider_param_connect_mutex();
  DBUG_ENTER("spider_db_connect");
  DBUG_ASSERT(conn->conn_kind != SPIDER_CONN_KIND_MYSQL || conn->need_mon);
  DBUG_PRINT("info",("spider link_idx=%d", link_idx));

  if (thd)
  {
    conn->connect_timeout = spider_param_connect_timeout(thd,
      share->connect_timeouts[link_idx]);
    conn->net_read_timeout = spider_param_net_read_timeout(thd,
      share->net_read_timeouts[link_idx]);
    conn->net_write_timeout = spider_param_net_write_timeout(thd,
      share->net_write_timeouts[link_idx]);
    connect_retry_interval = spider_param_connect_retry_interval(thd);
    connect_retry_count = spider_param_connect_retry_count(thd);
  } else {
    conn->connect_timeout = spider_param_connect_timeout(NULL,
      share->connect_timeouts[link_idx]);
    conn->net_read_timeout = spider_param_net_read_timeout(NULL,
      share->net_read_timeouts[link_idx]);
    conn->net_write_timeout = spider_param_net_write_timeout(NULL,
      share->net_write_timeouts[link_idx]);
    connect_retry_interval = spider_param_connect_retry_interval(NULL);
    connect_retry_count = spider_param_connect_retry_count(NULL);
  }
  DBUG_PRINT("info",("spider connect_timeout=%u", conn->connect_timeout));
  DBUG_PRINT("info",("spider net_read_timeout=%u", conn->net_read_timeout));
  DBUG_PRINT("info",("spider net_write_timeout=%u", conn->net_write_timeout));

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if ((error_num = spider_reset_conn_setted_parameter(conn, thd)))
      DBUG_RETURN(error_num);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  }
#endif

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    while (TRUE)
    {
      if (!(conn->db_conn = mysql_init(NULL)))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);

      mysql_options(conn->db_conn, MYSQL_OPT_READ_TIMEOUT,
        &conn->net_read_timeout);
      mysql_options(conn->db_conn, MYSQL_OPT_WRITE_TIMEOUT,
        &conn->net_write_timeout);
      mysql_options(conn->db_conn, MYSQL_OPT_CONNECT_TIMEOUT,
        &conn->connect_timeout);

      if (
        conn->tgt_ssl_ca_length |
        conn->tgt_ssl_capath_length |
        conn->tgt_ssl_cert_length |
        conn->tgt_ssl_key_length
      ) {
        mysql_ssl_set(conn->db_conn, conn->tgt_ssl_key, conn->tgt_ssl_cert,
          conn->tgt_ssl_ca, conn->tgt_ssl_capath, conn->tgt_ssl_cipher);
        if (conn->tgt_ssl_vsc)
        {
          my_bool verify_flg = TRUE;
          mysql_options(conn->db_conn, MYSQL_OPT_SSL_VERIFY_SERVER_CERT,
            &verify_flg);
        }
      }

      if (conn->tgt_default_file)
      {
        DBUG_PRINT("info",("spider tgt_default_file=%s",
          conn->tgt_default_file));
        mysql_options(conn->db_conn, MYSQL_READ_DEFAULT_FILE,
          conn->tgt_default_file);
      }
      if (conn->tgt_default_group)
      {
        DBUG_PRINT("info",("spider tgt_default_group=%s",
          conn->tgt_default_group));
        mysql_options(conn->db_conn, MYSQL_READ_DEFAULT_GROUP,
          conn->tgt_default_group);
      }

      if (connect_mutex)
        pthread_mutex_lock(&spider_open_conn_mutex);
      /* tgt_db not use */
      if (!mysql_real_connect(conn->db_conn,
                              share->tgt_hosts[link_idx],
                              share->tgt_usernames[link_idx],
                              share->tgt_passwords[link_idx],
                              NULL,
                              share->tgt_ports[link_idx],
                              share->tgt_sockets[link_idx],
                              CLIENT_MULTI_STATEMENTS)
      ) {
        if (connect_mutex)
          pthread_mutex_unlock(&spider_open_conn_mutex);
        error_num = mysql_errno(conn->db_conn);
        spider_db_disconnect(conn);
        if (
          (
            error_num != CR_CONN_HOST_ERROR &&
            error_num != CR_CONNECTION_ERROR
          ) ||
          !connect_retry_count
        ) {
          *conn->need_mon = ER_CONNECT_TO_FOREIGN_DATA_SOURCE;
          my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
            share->server_names[link_idx]);
          DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
        }
        connect_retry_count--;
        my_sleep((ulong) connect_retry_interval);
      } else {
        if (connect_mutex)
          pthread_mutex_unlock(&spider_open_conn_mutex);
        break;
      }
    }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
    SPIDER_HS_SOCKARGS sockargs;
    sockargs.timeout = conn->connect_timeout;
    sockargs.recv_timeout = conn->net_read_timeout;
    sockargs.send_timeout = conn->net_write_timeout;
    if (conn->hs_sock)
    {
      sockargs.family = AF_UNIX;
      sockargs.set_unix_domain(conn->hs_sock);
    } else {
      char port_str[6];
      my_sprintf(port_str, (port_str, "%05ld", conn->hs_port));
      if (sockargs.resolve(conn->tgt_host, port_str) != 0)
      {
        my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
          conn->tgt_host);
        DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
      }
    }
    conn->hs_conn = SPIDER_HS_CONN_CREATE(sockargs);
#ifndef HANDLERSOCKET_MYSQL_UTIL
    if (!(conn->hs_conn.operator->()))
#else
    if (!(conn->hs_conn))
#endif
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    while (conn->hs_conn->get_error_code())
    {
      if (!connect_retry_count)
      {
        my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
          conn->tgt_host);
        DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
      }
      connect_retry_count--;
      my_sleep((ulong) connect_retry_interval);
      conn->hs_conn->reconnect();
    }
  }
#endif
  conn->opened_handlers = 0;
  spider_db_reset_handler_open(conn);
  DBUG_RETURN(0);
}

int spider_db_ping(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
) {
  int error_num;
  DBUG_ENTER("spider_db_ping");
#ifndef DBUG_OFF
  if (spider->trx->thd)
    DBUG_PRINT("info", ("spider thd->query_id is %lld",
      spider->trx->thd->query_id));
#endif
  if (!conn->mta_conn_mutex_lock_already)
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[link_idx];
  }
  DBUG_ASSERT(conn->mta_conn_mutex_file_pos.file_name);
  if (conn->server_lost || conn->queued_connect)
  {
    if ((error_num = spider_db_connect(spider->share, conn,
      spider->conn_link_idx[link_idx])))
    {
      if (!conn->mta_conn_mutex_unlock_later)
      {
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      }
      DBUG_RETURN(error_num);
    }
    conn->server_lost = FALSE;
    conn->queued_connect = FALSE;
  }
  if ((error_num = simple_command(conn->db_conn, COM_PING, 0, 0, 0)))
  {
    spider_db_disconnect(conn);
    if ((error_num = spider_db_connect(spider->share, conn,
      spider->conn_link_idx[link_idx])))
    {
      DBUG_PRINT("info", ("spider conn=%p SERVER_LOST", conn));
      conn->server_lost = TRUE;
      if (!conn->mta_conn_mutex_unlock_later)
      {
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      }
      DBUG_RETURN(error_num);
    }
    if((error_num = simple_command(conn->db_conn, COM_PING, 0, 0, 0)))
    {
      spider_db_disconnect(conn);
      DBUG_PRINT("info", ("spider conn=%p SERVER_LOST", conn));
      conn->server_lost = TRUE;
      if (!conn->mta_conn_mutex_unlock_later)
      {
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      }
      DBUG_RETURN(error_num);
    }
  }
  conn->ping_time = (time_t) time((time_t*) 0);
  if (!conn->mta_conn_mutex_unlock_later)
  {
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

void spider_db_disconnect(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_db_disconnect");
  DBUG_PRINT("info",("spider conn=%p", conn));
  DBUG_PRINT("info",("spider conn->conn_kind=%u", conn->conn_kind));
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    DBUG_PRINT("info",("spider conn->db_conn=%p", conn->db_conn));
    if (conn->db_conn)
    {
      mysql_close(conn->db_conn);
      conn->db_conn = NULL;
    }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
#ifndef HANDLERSOCKET_MYSQL_UTIL
    if (conn->hs_conn.operator->())
#else
    DBUG_PRINT("info",("spider conn->hs_conn=%p", conn->hs_conn));
    if (conn->hs_conn)
#endif
    {
      conn->hs_conn->close();
#ifndef HANDLERSOCKET_MYSQL_UTIL
      SPIDER_HS_CONN hs_conn;
      hs_conn = conn->hs_conn;
#else
      delete conn->hs_conn;
      conn->hs_conn = NULL;
#endif
    }
  }
#endif
  DBUG_VOID_RETURN;
}

int spider_db_conn_queue_action(
  SPIDER_CONN *conn
) {
  int error_num;
  char sql_buf[MAX_FIELD_WIDTH * 2];
  spider_string sql_str(sql_buf, sizeof(sql_buf), system_charset_info);
  DBUG_ENTER("spider_db_conn_queue_action");
  DBUG_PRINT("info", ("spider conn=%p", conn));
  sql_str.init_calc_mem(106);
  sql_str.length(0);
  if (conn->queued_connect)
  {
    if ((error_num = spider_db_connect(conn->queued_connect_share, conn,
      conn->queued_connect_link_idx)))
    {
      DBUG_PRINT("info", ("spider conn=%p SERVER_LOST", conn));
      conn->server_lost = TRUE;
      DBUG_RETURN(error_num);
    }
    conn->server_lost = FALSE;
    conn->queued_connect = FALSE;
  }

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if (conn->queued_ping)
    {
      if ((error_num = spider_db_ping(conn->queued_ping_spider, conn,
        conn->queued_ping_link_idx)))
        DBUG_RETURN(error_num);
      conn->queued_ping = FALSE;
    }

    if (conn->server_lost)
    {
      DBUG_PRINT("info", ("spider no reconnect queue"));
      DBUG_RETURN(CR_SERVER_GONE_ERROR);
    }

    if (conn->queued_net_timeout)
    {
      my_net_set_read_timeout(&conn->db_conn->net, conn->net_read_timeout);
      my_net_set_write_timeout(&conn->db_conn->net, conn->net_write_timeout);
      conn->queued_net_timeout = FALSE;
    }
    if (
      (
        conn->queued_trx_isolation &&
        !conn->queued_semi_trx_isolation &&
        conn->queued_trx_isolation_val != conn->trx_isolation &&
        (error_num = spider_db_append_trx_isolation(&sql_str, conn,
          conn->queued_trx_isolation_val))
      ) ||
      (
        conn->queued_semi_trx_isolation &&
        conn->queued_semi_trx_isolation_val != conn->trx_isolation &&
        (error_num = spider_db_append_trx_isolation(&sql_str, conn,
          conn->queued_semi_trx_isolation_val))
      ) ||
      (
        conn->queued_autocommit &&
        (
          (conn->queued_autocommit_val && conn->autocommit != 1) ||
          (!conn->queued_autocommit_val && conn->autocommit != 0)
        ) &&
        (error_num = spider_db_append_autocommit(&sql_str, conn,
          conn->queued_autocommit_val))
      ) ||
      (
        conn->queued_sql_log_off &&
        (
          (conn->queued_sql_log_off_val && conn->sql_log_off != 1) ||
          (!conn->queued_sql_log_off_val && conn->sql_log_off != 0)
        ) &&
        (error_num = spider_db_append_sql_log_off(&sql_str, conn,
          conn->queued_sql_log_off_val))
      ) ||
      (
        conn->queued_time_zone &&
        conn->queued_time_zone_val != conn->time_zone &&
        (error_num = spider_db_append_time_zone(&sql_str, conn,
          conn->queued_time_zone_val))
      ) ||
      (
        conn->queued_trx_start &&
        (error_num = spider_db_append_start_transaction(&sql_str, conn))
      ) ||
      (
        conn->queued_xa_start &&
        (error_num = spider_db_append_xa_start(&sql_str, conn,
          conn->queued_xa_start_xid))
      )
    )
      DBUG_RETURN(error_num);
    if (sql_str.length())
    {
      if ((error_num = mysql_real_query(conn->db_conn, sql_str.ptr(),
        sql_str.length())))
        DBUG_RETURN(error_num);
      SPIDER_DB_RESULT *result;
      do {
        if ((result = mysql_store_result(conn->db_conn)))
          mysql_free_result(result);
        else if ((error_num = mysql_errno(conn->db_conn)))
          break;
      } while (!(error_num = spider_db_next_result(conn)));
      if (error_num > 0)
        DBUG_RETURN(error_num);
    }

    if (
      conn->queued_trx_isolation &&
      !conn->queued_semi_trx_isolation &&
      conn->queued_trx_isolation_val != conn->trx_isolation
    ) {
      conn->trx_isolation = conn->queued_trx_isolation_val;
      DBUG_PRINT("info", ("spider conn->trx_isolation=%d",
        conn->trx_isolation));
    }

    if (
      conn->queued_semi_trx_isolation &&
      conn->queued_semi_trx_isolation_val != conn->trx_isolation
    ) {
      conn->semi_trx_isolation = conn->queued_semi_trx_isolation_val;
      DBUG_PRINT("info", ("spider conn->semi_trx_isolation=%d",
        conn->semi_trx_isolation));
      conn->trx_isolation = thd_tx_isolation(conn->thd);
      DBUG_PRINT("info", ("spider conn->trx_isolation=%d",
        conn->trx_isolation));
    }

    if (conn->queued_autocommit)
    {
      if (conn->queued_autocommit_val && conn->autocommit != 1)
      {
        conn->autocommit = 1;
      } else if (!conn->queued_autocommit_val && conn->autocommit != 0)
      {
        conn->autocommit = 0;
      }
      DBUG_PRINT("info", ("spider conn->autocommit=%d",
        conn->autocommit));
    }

    if (conn->queued_sql_log_off)
    {
      if (conn->queued_sql_log_off_val && conn->sql_log_off != 1)
      {
        conn->sql_log_off = 1;
      } else if (!conn->queued_sql_log_off_val && conn->sql_log_off != 0)
      {
        conn->sql_log_off = 0;
      }
      DBUG_PRINT("info", ("spider conn->sql_log_off=%d",
        conn->sql_log_off));
    }

    if (
      conn->queued_time_zone &&
      conn->queued_time_zone_val != conn->time_zone
    ) {
      conn->time_zone = conn->queued_time_zone_val;
      DBUG_PRINT("info", ("spider conn->time_zone=%p",
        conn->time_zone));
    }
    spider_conn_clear_queue(conn);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else if (conn->server_lost)
  {
    DBUG_PRINT("info", ("spider no connect queue"));
    DBUG_RETURN(CR_SERVER_GONE_ERROR);
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_before_query(
  SPIDER_CONN *conn,
  int *need_mon
) {
  int error_num;
  DBUG_ENTER("spider_db_before_query");
  DBUG_ASSERT(need_mon);
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (conn->bg_search)
    spider_bg_conn_break(conn, NULL);
#endif
  if (!conn->mta_conn_mutex_lock_already)
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = need_mon;
  }
  DBUG_ASSERT(conn->mta_conn_mutex_file_pos.file_name);
  if ((error_num = spider_db_conn_queue_action(conn)))
    DBUG_RETURN(error_num);
  if (conn->server_lost)
    DBUG_RETURN(CR_SERVER_GONE_ERROR);
  if (conn->quick_target)
  {
    bool tmp_mta_conn_mutex_unlock_later;
    ha_spider *spider = (ha_spider*) conn->quick_target;
    SPIDER_RESULT_LIST *result_list = &spider->result_list;
    if (result_list->quick_mode == 2)
    {
      result_list->quick_phase = 1;
      tmp_mta_conn_mutex_unlock_later = conn->mta_conn_mutex_unlock_later;
      conn->mta_conn_mutex_unlock_later = TRUE;
      while (conn->quick_target)
      {
        if (
          (error_num = spider_db_store_result(spider, conn->link_idx,
            result_list->table)) &&
          error_num != HA_ERR_END_OF_FILE
        ) {
          conn->mta_conn_mutex_unlock_later = tmp_mta_conn_mutex_unlock_later;
          DBUG_RETURN(error_num);
        }
      }
      conn->mta_conn_mutex_unlock_later = tmp_mta_conn_mutex_unlock_later;
      result_list->quick_phase = 2;
    } else {
      mysql_free_result(result_list->bgs_current->result);
      result_list->bgs_current->result = NULL;
      conn->quick_target = NULL;
      spider->quick_targets[conn->link_idx] = NULL;
    }
  }
  DBUG_RETURN(0);
}

int spider_db_query(
  SPIDER_CONN *conn,
  const char *query,
  uint length,
  int *need_mon
) {
  int error_num;
  DBUG_ENTER("spider_db_query");
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    DBUG_PRINT("info", ("spider conn->db_conn %p", conn->db_conn));
    if ((error_num = spider_db_before_query(conn, need_mon)))
      DBUG_RETURN(error_num);
#ifndef DBUG_OFF
    spider_string tmp_query_str(sizeof(char) * (length + 1));
    tmp_query_str.init_calc_mem(107);
    char *tmp_query = (char *) tmp_query_str.c_ptr_safe();
    memcpy(tmp_query, query, length);
    tmp_query[length] = '\0';
    query = (const char *) tmp_query;
#endif
    if ((error_num = mysql_real_query(conn->db_conn, query, length)))
      DBUG_RETURN(error_num);
    DBUG_RETURN(0);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
#ifndef HANDLERSOCKET_MYSQL_UTIL
    DBUG_PRINT("info", ("spider conn->hs_conn %p",
      conn->hs_conn.operator->()));
#else
    DBUG_PRINT("info", ("spider conn->hs_conn %p",
      conn->hs_conn));
#endif
    if (conn->queued_net_timeout)
    {
      if (conn->hs_conn->set_timeout(conn->net_write_timeout,
        conn->net_read_timeout))
        DBUG_RETURN(ER_SPIDER_HS_NUM);
      conn->queued_net_timeout = FALSE;
    }

    if (
/*
      conn->hs_conn->set_timeout(conn->net_write_timeout,
        conn->net_read_timeout) ||
*/
      conn->hs_conn->request_send() < 0
    )
      DBUG_RETURN(ER_SPIDER_HS_NUM);
    DBUG_RETURN(0);
  }
#endif
}

int spider_db_errorno(
  SPIDER_CONN *conn
) {
  int error_num;
  DBUG_ENTER("spider_db_errorno");
  DBUG_ASSERT(conn->need_mon);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if (conn->server_lost)
    {
      *conn->need_mon = ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM;
      my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
        ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
      if (!conn->mta_conn_mutex_unlock_later)
      {
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      }
      DBUG_RETURN(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM);
    }
    if ((error_num = mysql_errno(conn->db_conn)))
    {
      DBUG_PRINT("info",("spider error_num = %d", error_num));
      if (
        error_num == CR_SERVER_GONE_ERROR ||
        error_num == CR_SERVER_LOST
      ) {
        spider_db_disconnect(conn);
        DBUG_PRINT("info", ("spider conn=%p SERVER_LOST", conn));
        conn->server_lost = TRUE;
        if (conn->disable_reconnect)
        {
          *conn->need_mon = ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM;
          my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
            ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
        }
        if (!conn->mta_conn_mutex_unlock_later)
        {
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
        }
        DBUG_RETURN(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM);
      } else if (
        conn->ignore_dup_key &&
        (error_num == ER_DUP_ENTRY || error_num == ER_DUP_KEY)
      ) {
        conn->error_str = (char*) mysql_error(conn->db_conn);
        conn->error_length = strlen(conn->error_str);
        if (!conn->mta_conn_mutex_unlock_later)
        {
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
        }
        DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
      } else if (
        error_num == ER_XAER_NOTA &&
        current_thd &&
        spider_param_force_commit(current_thd) == 1
      ) {
        push_warning(current_thd, MYSQL_ERROR::WARN_LEVEL_WARN,
          error_num, mysql_error(conn->db_conn));
        if (!conn->mta_conn_mutex_unlock_later)
        {
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
        }
        DBUG_RETURN(error_num);
      }
      *conn->need_mon = error_num;
      my_message(error_num, mysql_error(conn->db_conn), MYF(0));
      if (!conn->mta_conn_mutex_unlock_later)
      {
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      }
      DBUG_RETURN(error_num);
    }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
#ifndef HANDLERSOCKET_MYSQL_UTIL
    my_printf_error(ER_SPIDER_HS_NUM, ER_SPIDER_HS_STR, MYF(0),
      conn->hs_conn->get_error_code(), conn->hs_conn->get_error().c_str());
#else
    my_printf_error(ER_SPIDER_HS_NUM, ER_SPIDER_HS_STR, MYF(0),
      conn->hs_conn->get_error_code(), conn->hs_conn->get_error().c_ptr());
#endif
    *conn->need_mon = ER_SPIDER_HS_NUM;
    DBUG_PRINT("info", ("spider conn=%p SERVER_LOST", conn));
    conn->server_lost = TRUE;
    if (!conn->mta_conn_mutex_unlock_later)
    {
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
    DBUG_RETURN(ER_SPIDER_HS_NUM);
  }
#endif
  if (!conn->mta_conn_mutex_unlock_later)
  {
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

int spider_db_set_trx_isolation(
  SPIDER_CONN *conn,
  int trx_isolation,
  int *need_mon
) {
  DBUG_ENTER("spider_db_set_trx_isolation");
  switch (trx_isolation)
  {
    case ISO_READ_UNCOMMITTED:
      if (spider_db_query(
        conn,
        SPIDER_SQL_ISO_READ_UNCOMMITTED_STR,
        SPIDER_SQL_ISO_READ_UNCOMMITTED_LEN,
        need_mon)
      )
        DBUG_RETURN(spider_db_errorno(conn));
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      break;
    case ISO_READ_COMMITTED:
      if (spider_db_query(
        conn,
        SPIDER_SQL_ISO_READ_COMMITTED_STR,
        SPIDER_SQL_ISO_READ_COMMITTED_LEN,
        need_mon)
      )
        DBUG_RETURN(spider_db_errorno(conn));
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      break;
    case ISO_REPEATABLE_READ:
      if (spider_db_query(
        conn,
        SPIDER_SQL_ISO_REPEATABLE_READ_STR,
        SPIDER_SQL_ISO_REPEATABLE_READ_LEN,
        need_mon)
      )
        DBUG_RETURN(spider_db_errorno(conn));
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      break;
    case ISO_SERIALIZABLE:
      if (spider_db_query(
        conn,
        SPIDER_SQL_ISO_SERIALIZABLE_STR,
        SPIDER_SQL_ISO_SERIALIZABLE_LEN,
        need_mon)
      )
        DBUG_RETURN(spider_db_errorno(conn));
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      break;
    default:
      DBUG_RETURN(HA_ERR_UNSUPPORTED);
  }
  DBUG_RETURN(0);
}

int spider_db_append_trx_isolation(
  spider_string *str,
  SPIDER_CONN *conn,
  int trx_isolation
) {
  DBUG_ENTER("spider_db_append_trx_isolation");
  if (str->reserve(SPIDER_SQL_SEMICOLON_LEN +
    SPIDER_SQL_ISO_READ_UNCOMMITTED_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if (str->length())
  {
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }
  switch (trx_isolation)
  {
    case ISO_READ_UNCOMMITTED:
      str->q_append(SPIDER_SQL_ISO_READ_UNCOMMITTED_STR,
        SPIDER_SQL_ISO_READ_UNCOMMITTED_LEN);
      break;
    case ISO_READ_COMMITTED:
      str->q_append(SPIDER_SQL_ISO_READ_COMMITTED_STR,
        SPIDER_SQL_ISO_READ_COMMITTED_LEN);
      break;
    case ISO_REPEATABLE_READ:
      str->q_append(SPIDER_SQL_ISO_REPEATABLE_READ_STR,
        SPIDER_SQL_ISO_REPEATABLE_READ_LEN);
      break;
    case ISO_SERIALIZABLE:
      str->q_append(SPIDER_SQL_ISO_SERIALIZABLE_STR,
        SPIDER_SQL_ISO_SERIALIZABLE_LEN);
      break;
    default:
      DBUG_RETURN(HA_ERR_UNSUPPORTED);
  }
  DBUG_RETURN(0);
}

int spider_db_append_autocommit(
  spider_string *str,
  SPIDER_CONN *conn,
  bool autocommit
) {
  DBUG_ENTER("spider_db_append_autocommit");
  if (str->reserve(SPIDER_SQL_SEMICOLON_LEN + SPIDER_SQL_AUTOCOMMIT_OFF_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if (str->length())
  {
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }
  if (autocommit)
  {
    str->q_append(SPIDER_SQL_AUTOCOMMIT_ON_STR,
      SPIDER_SQL_AUTOCOMMIT_ON_LEN);
  } else {
    str->q_append(SPIDER_SQL_AUTOCOMMIT_OFF_STR,
      SPIDER_SQL_AUTOCOMMIT_OFF_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_sql_log_off(
  spider_string *str,
  SPIDER_CONN *conn,
  bool sql_log_off
) {
  DBUG_ENTER("spider_db_append_sql_log_off");
  if (str->reserve(SPIDER_SQL_SEMICOLON_LEN + SPIDER_SQL_SQL_LOG_OFF_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if (str->length())
  {
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }
  if (sql_log_off)
  {
    str->q_append(SPIDER_SQL_SQL_LOG_ON_STR, SPIDER_SQL_SQL_LOG_ON_LEN);
  } else {
    str->q_append(SPIDER_SQL_SQL_LOG_OFF_STR, SPIDER_SQL_SQL_LOG_OFF_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_time_zone(
  spider_string *str,
  SPIDER_CONN *conn,
  Time_zone *time_zone
) {
  const String *tz_str = time_zone->get_name();
  DBUG_ENTER("spider_db_append_time_zone");
  if (str->reserve(SPIDER_SQL_SEMICOLON_LEN + SPIDER_SQL_TIME_ZONE_LEN +
    tz_str->length() + SPIDER_SQL_VALUE_QUOTE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if (str->length())
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  str->q_append(SPIDER_SQL_TIME_ZONE_STR, SPIDER_SQL_TIME_ZONE_LEN);
  str->q_append(tz_str->ptr(), tz_str->length());
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  DBUG_RETURN(0);
}

int spider_db_set_names(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
) {
  bool tmp_mta_conn_mutex_lock_already;
  SPIDER_SHARE *share = spider->share;
  int base_link_idx = link_idx;
  DBUG_ENTER("spider_db_set_names");
  link_idx = spider->conn_link_idx[base_link_idx];
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if (!conn->mta_conn_mutex_lock_already)
    {
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[base_link_idx];
    }
    DBUG_ASSERT(conn->mta_conn_mutex_file_pos.file_name);
    if (
      !conn->access_charset ||
      share->access_charset->cset != conn->access_charset->cset
    ) {
      tmp_mta_conn_mutex_lock_already = conn->mta_conn_mutex_lock_already;
      conn->mta_conn_mutex_lock_already = TRUE;
      if (
        spider_db_before_query(conn, &spider->need_mons[base_link_idx]) ||
        mysql_set_character_set(
          conn->db_conn,
          share->access_charset->csname)
      ) {
        conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
        DBUG_RETURN(spider_db_errorno(conn));
      }
      conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
      conn->access_charset = share->access_charset;
    }
    if (
      spider_param_use_default_database(spider->trx->thd) &&
      (
        !conn->default_database.length() ||
        conn->default_database.length() !=
          share->tgt_dbs_lengths[link_idx] ||
        memcmp(share->tgt_dbs[link_idx], conn->default_database.ptr(),
          share->tgt_dbs_lengths[link_idx])
      )
    ) {
      DBUG_PRINT("info",("spider link_idx=%d db=%s", link_idx,
        share->tgt_dbs[link_idx]));
      tmp_mta_conn_mutex_lock_already = conn->mta_conn_mutex_lock_already;
      conn->mta_conn_mutex_lock_already = TRUE;
      if (
        spider_db_before_query(conn, &spider->need_mons[base_link_idx]) ||
        mysql_select_db(
          conn->db_conn,
          share->tgt_dbs[link_idx])
      ) {
        conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
        DBUG_RETURN(spider_db_errorno(conn));
      }
      conn->default_database.length(0);
      if (conn->default_database.reserve(
        share->tgt_dbs_lengths[link_idx] + 1))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      conn->default_database.q_append(share->tgt_dbs[link_idx],
        share->tgt_dbs_lengths[link_idx] + 1);
      conn->default_database.length(share->tgt_dbs_lengths[link_idx]);
      conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
    }
    if (!conn->mta_conn_mutex_unlock_later)
    {
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_query_with_set_names(
  spider_string *sql,
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
) {
  int error_num;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_query_with_set_names");

  pthread_mutex_lock(&conn->mta_conn_mutex);
  SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  if ((error_num = spider_db_set_names(spider, conn, link_idx)))
  {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    if (
      share->monitoring_kind[link_idx] &&
      spider->need_mons[link_idx]
    ) {
      error_num = spider_ping_table_mon_from_table(
          spider->trx,
          spider->trx->thd,
          share,
          (uint32) share->monitoring_sid[link_idx],
          share->table_name,
          share->table_name_length,
          spider->conn_link_idx[link_idx],
          NULL,
          0,
          share->monitoring_kind[link_idx],
          share->monitoring_limit[link_idx],
          TRUE
        );
    }
    DBUG_RETURN(error_num);
  }
  spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
    share);
  if (spider_db_query(
    conn,
    sql->ptr(),
    sql->length(),
    &spider->need_mons[link_idx])
  ) {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    error_num = spider_db_errorno(conn);
    if (
      share->monitoring_kind[link_idx] &&
      spider->need_mons[link_idx]
    ) {
      error_num = spider_ping_table_mon_from_table(
          spider->trx,
          spider->trx->thd,
          share,
          (uint32) share->monitoring_sid[link_idx],
          share->table_name,
          share->table_name_length,
          spider->conn_link_idx[link_idx],
          NULL,
          0,
          share->monitoring_kind[link_idx],
          share->monitoring_limit[link_idx],
          TRUE
        );
    }
    DBUG_RETURN(error_num);
  }
  conn->mta_conn_mutex_lock_already = FALSE;
  conn->mta_conn_mutex_unlock_later = FALSE;
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_query_for_bulk_update(
  spider_string *sql,
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
) {
  int error_num;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_query_for_bulk_update");

  pthread_mutex_lock(&conn->mta_conn_mutex);
  SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  if ((error_num = spider_db_set_names(spider, conn, link_idx)))
  {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    if (
      share->monitoring_kind[link_idx] &&
      spider->need_mons[link_idx]
    ) {
      error_num = spider_ping_table_mon_from_table(
          spider->trx,
          spider->trx->thd,
          share,
          (uint32) share->monitoring_sid[link_idx],
          share->table_name,
          share->table_name_length,
          spider->conn_link_idx[link_idx],
          NULL,
          0,
          share->monitoring_kind[link_idx],
          share->monitoring_limit[link_idx],
          TRUE
        );
    }
    DBUG_RETURN(error_num);
  }
  spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
    share);
  if (spider_db_query(
    conn,
    sql->ptr(),
    sql->length(),
    &spider->need_mons[link_idx])
  ) {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    error_num = spider_db_errorno(conn);
    if (
      error_num != ER_DUP_ENTRY &&
      error_num != ER_DUP_KEY &&
      error_num != HA_ERR_FOUND_DUPP_KEY &&
      share->monitoring_kind[link_idx] &&
      spider->need_mons[link_idx]
    ) {
      error_num = spider_ping_table_mon_from_table(
          spider->trx,
          spider->trx->thd,
          share,
          (uint32) share->monitoring_sid[link_idx],
          share->table_name,
          share->table_name_length,
          spider->conn_link_idx[link_idx],
          NULL,
          0,
          share->monitoring_kind[link_idx],
          share->monitoring_limit[link_idx],
          TRUE
        );
    }
    DBUG_RETURN(error_num);
  }
  while (!(error_num = spider_db_next_result(conn)))
  {
    ;
  }
  if (
    error_num > 0 &&
    error_num != ER_DUP_ENTRY &&
    error_num != ER_DUP_KEY &&
    error_num != HA_ERR_FOUND_DUPP_KEY
  ) {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    if (
      share->monitoring_kind[link_idx] &&
      spider->need_mons[link_idx]
    ) {
      error_num = spider_ping_table_mon_from_table(
          spider->trx,
          spider->trx->thd,
          share,
          (uint32) share->monitoring_sid[link_idx],
          share->table_name,
          share->table_name_length,
          spider->conn_link_idx[link_idx],
          NULL,
          0,
          share->monitoring_kind[link_idx],
          share->monitoring_limit[link_idx],
          TRUE
        );
    }
    DBUG_RETURN(error_num);
  }
  conn->mta_conn_mutex_lock_already = FALSE;
  conn->mta_conn_mutex_unlock_later = FALSE;
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

size_t spider_db_real_escape_string(
  SPIDER_CONN *conn,
  char *to,
  const char *from,
  size_t from_length
) {
  SPIDER_DB_CONN *db_conn = conn->db_conn;
  DBUG_ENTER("spider_db_real_escape_string");
  if (db_conn->server_status & SERVER_STATUS_NO_BACKSLASH_ESCAPES)
    DBUG_RETURN(escape_quotes_for_mysql(db_conn->charset, to, 0,
      from, from_length));
  DBUG_RETURN(escape_string_for_mysql(db_conn->charset, to, 0,
    from, from_length));
}

int spider_db_consistent_snapshot(
  SPIDER_CONN *conn,
  int *need_mon
) {
  DBUG_ENTER("spider_db_consistent_snapshot");
  if (spider_db_query(
    conn,
    SPIDER_SQL_START_CONSISTENT_SNAPSHOT_STR,
    SPIDER_SQL_START_CONSISTENT_SNAPSHOT_LEN,
    need_mon)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  conn->trx_start = TRUE;
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_start_transaction(
  SPIDER_CONN *conn,
  int *need_mon
) {
  DBUG_ENTER("spider_db_start_transaction");
  if (spider_db_query(
    conn,
    SPIDER_SQL_START_TRANSACTION_STR,
    SPIDER_SQL_START_TRANSACTION_LEN,
    need_mon)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  conn->trx_start = TRUE;
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_append_start_transaction(
  spider_string *str,
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_db_append_start_transaction");
  if (str->reserve(SPIDER_SQL_SEMICOLON_LEN +
    SPIDER_SQL_START_TRANSACTION_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if (str->length())
  {
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }
  str->q_append(SPIDER_SQL_START_TRANSACTION_STR,
    SPIDER_SQL_START_TRANSACTION_LEN);
  DBUG_RETURN(0);
}

int spider_db_commit(
  SPIDER_CONN *conn
) {
  int need_mon = 0;
  DBUG_ENTER("spider_db_commit");
  if (!conn->queued_trx_start)
  {
    if (conn->use_for_active_standby && conn->server_lost)
    {
      my_message(ER_SPIDER_LINK_IS_FAILOVER_NUM,
        ER_SPIDER_LINK_IS_FAILOVER_STR, MYF(0));
      DBUG_RETURN(ER_SPIDER_LINK_IS_FAILOVER_NUM);
    }
    if (spider_db_query(
      conn,
      SPIDER_SQL_COMMIT_STR,
      SPIDER_SQL_COMMIT_LEN,
      &need_mon)
    )
      DBUG_RETURN(spider_db_errorno(conn));
    conn->trx_start = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  } else
    conn->trx_start = FALSE;
  DBUG_RETURN(0);
}

int spider_db_rollback(
  SPIDER_CONN *conn
) {
  int error_num, need_mon = 0;
  bool is_error;
  DBUG_ENTER("spider_db_rollback");
  if (!conn->queued_trx_start)
  {
    if (spider_db_query(
      conn,
      SPIDER_SQL_ROLLBACK_STR,
      SPIDER_SQL_ROLLBACK_LEN,
      &need_mon)
    ) {
      is_error = conn->thd->is_error();
      conn->mta_conn_mutex_unlock_later = TRUE;
      error_num = spider_db_errorno(conn);
      if (
        error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
        !is_error
      )
        conn->thd->clear_error();
      else {
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
    }
    conn->trx_start = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  } else
    conn->trx_start = FALSE;
  DBUG_RETURN(0);
}

int spider_db_append_hex_string(
  spider_string *str,
  uchar *hex_ptr,
  int hex_ptr_length
) {
  uchar *end_ptr;
  char *str_ptr;
  DBUG_ENTER("spider_db_append_hex_string");
  if (hex_ptr_length)
  {
    if (str->reserve(SPIDER_SQL_HEX_LEN + hex_ptr_length * 2))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_HEX_STR, SPIDER_SQL_HEX_LEN);
    str_ptr = (char *) str->ptr() + str->length();
    for (end_ptr = hex_ptr + hex_ptr_length; hex_ptr < end_ptr; hex_ptr++)
    {
      *str_ptr++ = _dig_vec_upper[(*hex_ptr) >> 4];
      *str_ptr++ = _dig_vec_upper[(*hex_ptr) & 0x0F];
    }
    str->length(str->length() + hex_ptr_length * 2);
  } else {
    if (str->reserve((SPIDER_SQL_VALUE_QUOTE_LEN) * 2))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  }
  DBUG_RETURN(0);
}

void spider_db_append_xid_str(
  spider_string *tmp_str,
  XID *xid
) {
  char format_id[sizeof(long) + 3];
  uint format_id_length;
  DBUG_ENTER("spider_db_append_xid_str");

  format_id_length =
    my_sprintf(format_id, (format_id, "0x%lx", xid->formatID));
  spider_db_append_hex_string(tmp_str, (uchar *) xid->data, xid->gtrid_length);
/*
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  tmp_str->q_append(xid->data, xid->gtrid_length);
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
*/
  tmp_str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  spider_db_append_hex_string(tmp_str,
    (uchar *) xid->data + xid->gtrid_length, xid->bqual_length);
/*
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  tmp_str->q_append(xid->data + xid->gtrid_length, xid->bqual_length);
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
*/
  tmp_str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  tmp_str->q_append(format_id, format_id_length);
#ifndef DBUG_OFF
  ((char *) tmp_str->ptr())[tmp_str->length()] = '\0';
#endif

  DBUG_VOID_RETURN;
}

int spider_db_append_xa_start(
  spider_string *str,
  SPIDER_CONN *conn,
  XID *xid
) {
  DBUG_ENTER("spider_db_append_xa_start");
  if (str->reserve(SPIDER_SQL_SEMICOLON_LEN +
    SPIDER_SQL_XA_START_LEN + XIDDATASIZE + sizeof(long) + 9))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if (str->length())
  {
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }
  str->q_append(SPIDER_SQL_XA_START_STR, SPIDER_SQL_XA_START_LEN);
  spider_db_append_xid_str(str, xid);
  DBUG_RETURN(0);
}

int spider_db_xa_end(
  SPIDER_CONN *conn,
  XID *xid
) {
  int need_mon = 0;
  char sql_buf[SPIDER_SQL_XA_END_LEN + XIDDATASIZE + sizeof(long) + 9];
  spider_string sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_end");
  sql_str.init_calc_mem(108);

  if (!conn->queued_xa_start)
  {
    sql_str.length(0);
    sql_str.q_append(SPIDER_SQL_XA_END_STR, SPIDER_SQL_XA_END_LEN);
    spider_db_append_xid_str(&sql_str, xid);
    if (spider_db_query(
      conn,
      sql_str.ptr(),
      sql_str.length(),
      &need_mon)
    )
      DBUG_RETURN(spider_db_errorno(conn));
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

int spider_db_xa_prepare(
  SPIDER_CONN *conn,
  XID *xid
) {
  int need_mon = 0;
  char sql_buf[SPIDER_SQL_XA_PREPARE_LEN + XIDDATASIZE + sizeof(long) + 9];
  spider_string sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_prepare");
  sql_str.init_calc_mem(109);

  if (!conn->queued_xa_start)
  {
    if (conn->use_for_active_standby && conn->server_lost)
    {
      my_message(ER_SPIDER_LINK_IS_FAILOVER_NUM,
        ER_SPIDER_LINK_IS_FAILOVER_STR, MYF(0));
      DBUG_RETURN(ER_SPIDER_LINK_IS_FAILOVER_NUM);
    }
    sql_str.length(0);
    sql_str.q_append(SPIDER_SQL_XA_PREPARE_STR, SPIDER_SQL_XA_PREPARE_LEN);
    spider_db_append_xid_str(&sql_str, xid);
    if (spider_db_query(
      conn,
      sql_str.ptr(),
      sql_str.length(),
      &need_mon)
    )
      DBUG_RETURN(spider_db_errorno(conn));
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

int spider_db_xa_commit(
  SPIDER_CONN *conn,
  XID *xid
) {
  int need_mon = 0;
  char sql_buf[SPIDER_SQL_XA_COMMIT_LEN + XIDDATASIZE + sizeof(long) + 9];
  spider_string sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_commit");
  sql_str.init_calc_mem(110);

  if (!conn->queued_xa_start)
  {
    sql_str.length(0);
    sql_str.q_append(SPIDER_SQL_XA_COMMIT_STR, SPIDER_SQL_XA_COMMIT_LEN);
    spider_db_append_xid_str(&sql_str, xid);
    if (spider_db_query(
      conn,
      sql_str.ptr(),
      sql_str.length(),
      &need_mon)
    )
      DBUG_RETURN(spider_db_errorno(conn));
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

int spider_db_xa_rollback(
  SPIDER_CONN *conn,
  XID *xid
) {
  int need_mon = 0;
  char sql_buf[SPIDER_SQL_XA_ROLLBACK_LEN + XIDDATASIZE + sizeof(long) + 9];
  spider_string sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_rollback");
  sql_str.init_calc_mem(111);

  if (!conn->queued_xa_start)
  {
    sql_str.length(0);
    sql_str.q_append(SPIDER_SQL_XA_ROLLBACK_STR, SPIDER_SQL_XA_ROLLBACK_LEN);
    spider_db_append_xid_str(&sql_str, xid);
    if (spider_db_query(
      conn,
      sql_str.ptr(),
      sql_str.length(),
      &need_mon)
    )
      DBUG_RETURN(spider_db_errorno(conn));
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

int spider_db_lock_tables(
  ha_spider *spider,
  int link_idx
) {
  int error_num;
  ha_spider *tmp_spider;
  int lock_type;
  uint conn_link_idx;
  int tmp_link_idx;
  SPIDER_CONN *conn = spider->conns[link_idx];
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sqls[link_idx];
  SPIDER_LINK_FOR_HASH *tmp_link_for_hash;
  DBUG_ENTER("spider_db_lock_tables");
  str->length(0);
  if (str->reserve(SPIDER_SQL_LOCK_TABLE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_LOCK_TABLE_STR, SPIDER_SQL_LOCK_TABLE_LEN);
  while ((tmp_link_for_hash =
    (SPIDER_LINK_FOR_HASH *) my_hash_element(&conn->lock_table_hash, 0)))
  {
    tmp_spider = tmp_link_for_hash->spider;
    tmp_link_idx = tmp_link_for_hash->link_idx;
    switch (tmp_spider->lock_type)
    {
      case TL_READ:
        lock_type = SPIDER_DB_TABLE_LOCK_READ_LOCAL;
        break;
      case TL_READ_NO_INSERT:
        lock_type = SPIDER_DB_TABLE_LOCK_READ;
        break;
      case TL_WRITE_LOW_PRIORITY:
        lock_type = SPIDER_DB_TABLE_LOCK_LOW_PRIORITY_WRITE;
        break;
      case TL_WRITE:
        lock_type = SPIDER_DB_TABLE_LOCK_WRITE;
        break;
      default:
        // no lock
        DBUG_PRINT("info",("spider lock_type=%d", tmp_spider->lock_type));
        DBUG_RETURN(0);
    }
    if (str->reserve(SPIDER_SQL_NAME_QUOTE_LEN))
    {
      my_hash_reset(&conn->lock_table_hash);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    conn_link_idx = tmp_spider->conn_link_idx[tmp_link_idx];
    if (&tmp_spider->share->db_names_str[conn_link_idx])
    {
      if (
        str->append(tmp_spider->share->db_names_str[conn_link_idx].ptr(),
          tmp_spider->share->db_names_str[conn_link_idx].length(),
          tmp_spider->share->access_charset) ||
        str->reserve((SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_DOT_LEN)
      ) {
        my_hash_reset(&conn->lock_table_hash);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    } else {
      if (
        str->append(tmp_spider->share->tgt_dbs[conn_link_idx],
          tmp_spider->share->tgt_dbs_lengths[conn_link_idx],
          system_charset_info) ||
        str->reserve((SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_DOT_LEN)
      ) {
        my_hash_reset(&conn->lock_table_hash);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    }
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    if (&tmp_spider->share->table_names_str[conn_link_idx])
    {
      if (
        str->append(tmp_spider->share->table_names_str[conn_link_idx].ptr(),
          tmp_spider->share->table_names_str[conn_link_idx].length(),
          tmp_spider->share->access_charset) ||
        str->reserve(SPIDER_SQL_NAME_QUOTE_LEN +
          spider_db_table_lock_len[lock_type])
      ) {
        my_hash_reset(&conn->lock_table_hash);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    } else {
      if (
        str->append(tmp_spider->share->tgt_table_names[conn_link_idx],
          tmp_spider->share->tgt_table_names_lengths[conn_link_idx],
          system_charset_info) ||
        str->reserve(SPIDER_SQL_NAME_QUOTE_LEN +
          spider_db_table_lock_len[lock_type])
      ) {
        my_hash_reset(&conn->lock_table_hash);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    }
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    str->q_append(spider_db_table_lock_str[lock_type],
      spider_db_table_lock_len[lock_type]);
    my_hash_delete(&conn->lock_table_hash, (uchar*) tmp_link_for_hash);
  }
  pthread_mutex_lock(&conn->mta_conn_mutex);
  SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  if ((error_num = spider_db_set_names(spider, conn, link_idx)))
  {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(error_num);
  }
  spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
    spider->share);
  if (spider_db_query(
    conn,
    str->ptr(),
    str->length() - SPIDER_SQL_COMMA_LEN,
    &spider->need_mons[link_idx])
  ) {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    DBUG_RETURN(spider_db_errorno(conn));
  }
  if (!conn->table_locked)
  {
    conn->table_locked = TRUE;
    spider->trx->locked_connections++;
  }
  conn->mta_conn_mutex_lock_already = FALSE;
  conn->mta_conn_mutex_unlock_later = FALSE;
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_unlock_tables(
  ha_spider *spider,
  int link_idx
) {
  SPIDER_CONN *conn = spider->conns[link_idx];
  DBUG_ENTER("spider_db_unlock_tables");
  if (conn->table_locked)
  {
    conn->table_locked = FALSE;
    spider->trx->locked_connections--;
  }
  spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
    spider->share);
  if (spider_db_query(
    conn,
    SPIDER_SQL_UNLOCK_TABLE_STR,
    SPIDER_SQL_UNLOCK_TABLE_LEN,
    &spider->need_mons[link_idx])
  )
    DBUG_RETURN(spider_db_errorno(conn));
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_append_name_with_quote_str(
  spider_string *str,
  char *name
) {
  int length = strlen(name);
  char *name_end, head_code;
  DBUG_ENTER("spider_db_append_name_with_quote_str");
  for (name_end = name + length; name < name_end; name += length)
  {
    head_code = *name;
    if (!(length = my_mbcharlen(system_charset_info, (uchar) head_code)))
    {
      my_message(ER_SPIDER_WRONG_CHARACTER_IN_NAME_NUM,
        ER_SPIDER_WRONG_CHARACTER_IN_NAME_STR, MYF(0));
      DBUG_RETURN(ER_SPIDER_WRONG_CHARACTER_IN_NAME_NUM);
    }
    if (length == 1 && head_code == *name_quote_str)
    {
      if (str->reserve((SPIDER_SQL_NAME_QUOTE_LEN) * 2))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    } else {
      if (str->append(name, length, system_charset_info))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_create_table_names_str(
  SPIDER_SHARE *share
) {
  int error_num, roop_count;
  uint table_nm_len, db_nm_len;
  spider_string *str, *first_tbl_nm_str, *first_db_nm_str, *first_db_tbl_str;
  char *first_tbl_nm, *first_db_nm;
  DBUG_ENTER("spider_db_create_table_names_str");
  share->table_names_str = NULL;
  share->db_names_str = NULL;
  share->db_table_str = NULL;
  if (
    !(share->table_names_str = new spider_string[share->all_link_count]) ||
    !(share->db_names_str = new spider_string[share->all_link_count]) ||
    !(share->db_table_str = new spider_string[share->all_link_count])
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error;
  }

  share->same_db_table_name = TRUE;
  first_tbl_nm = share->tgt_table_names[0];
  first_db_nm = share->tgt_dbs[0];
  table_nm_len = share->tgt_table_names_lengths[0];
  db_nm_len = share->tgt_dbs_lengths[0];
  first_tbl_nm_str = &share->table_names_str[0];
  first_db_nm_str = &share->db_names_str[0];
  first_db_tbl_str = &share->db_table_str[0];
  for (roop_count = 0; roop_count < (int) share->all_link_count; roop_count++)
  {
    share->table_names_str[roop_count].init_calc_mem(86);
    share->db_names_str[roop_count].init_calc_mem(87);
    share->db_table_str[roop_count].init_calc_mem(88);
    str = &share->table_names_str[roop_count];
    if (
      roop_count != 0 &&
      share->same_db_table_name &&
      share->tgt_table_names_lengths[roop_count] == table_nm_len &&
      !memcmp(first_tbl_nm, share->tgt_table_names[roop_count],
        table_nm_len)
    ) {
      if (str->copy(*first_tbl_nm_str))
      {
        error_num = HA_ERR_OUT_OF_MEM;
        goto error;
      }
    } else {
      str->set_charset(share->access_charset);
      if ((error_num = spider_db_append_name_with_quote_str(str,
        share->tgt_table_names[roop_count])))
        goto error;
      if (roop_count)
      {
        share->same_db_table_name = FALSE;
        DBUG_PRINT("info", ("spider found different table name %s",
          share->tgt_table_names[roop_count]));
        if (str->length() > share->table_nm_max_length)
          share->table_nm_max_length = str->length();
      } else
        share->table_nm_max_length = str->length();
    }

    str = &share->db_names_str[roop_count];
    if (
      roop_count != 0 &&
      share->same_db_table_name &&
      share->tgt_dbs_lengths[roop_count] == db_nm_len &&
      !memcmp(first_db_nm, share->tgt_dbs[roop_count],
        db_nm_len)
    ) {
      if (str->copy(*first_db_nm_str))
      {
        error_num = HA_ERR_OUT_OF_MEM;
        goto error;
      }
    } else {
      str->set_charset(share->access_charset);
      if ((error_num = spider_db_append_name_with_quote_str(str,
        share->tgt_dbs[roop_count])))
        goto error;
      if (roop_count)
      {
        share->same_db_table_name = FALSE;
        DBUG_PRINT("info", ("spider found different db name %s",
          share->tgt_dbs[roop_count]));
        if (str->length() > share->db_nm_max_length)
          share->db_nm_max_length = str->length();
      } else
        share->db_nm_max_length = str->length();
    }

    str = &share->db_table_str[roop_count];
    if (
      roop_count != 0 &&
      share->same_db_table_name
    ) {
      if (str->copy(*first_db_tbl_str))
      {
        error_num = HA_ERR_OUT_OF_MEM;
        goto error;
      }
    } else {
      str->set_charset(share->access_charset);
      if ((error_num = spider_db_append_table_name_with_reserve(str,
        share, roop_count)))
        goto error;
    }
  }
  DBUG_RETURN(0);

error:
  if (share->db_table_str)
  {
    delete [] share->db_table_str;
    share->db_table_str = NULL;
  }
  if (share->db_names_str)
  {
    delete [] share->db_names_str;
    share->db_names_str = NULL;
  }
  if (share->table_names_str)
  {
    delete [] share->table_names_str;
    share->table_names_str = NULL;
  }
  DBUG_RETURN(error_num);
}

void spider_db_free_table_names_str(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_free_table_names_str");
  if (share->db_table_str)
  {
    delete [] share->db_table_str;
    share->db_table_str = NULL;
  }
  if (share->db_names_str)
  {
    delete [] share->db_names_str;
    share->db_names_str = NULL;
  }
  if (share->table_names_str)
  {
    delete [] share->table_names_str;
    share->table_names_str = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_db_create_column_name_str(
  SPIDER_SHARE *share,
  TABLE_SHARE *table_share
) {
  spider_string *str;
  int error_num;
  Field **field;
  DBUG_ENTER("spider_db_create_column_name_str");
  if (
    table_share->fields &&
    !(share->column_name_str = new spider_string[table_share->fields])
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  for (field = table_share->field, str = share->column_name_str;
   *field; field++, str++)
  {
    str->init_calc_mem(89);
    str->set_charset(share->access_charset);
    if ((error_num = spider_db_append_name_with_quote_str(str,
      (char *) (*field)->field_name)))
      goto error;
  }
  DBUG_RETURN(0);

error:
  if (share->column_name_str)
  {
    delete [] share->column_name_str;
    share->column_name_str = NULL;
  }
  DBUG_RETURN(error_num);
}

void spider_db_free_column_name_str(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_free_column_name_str");
  if (share->column_name_str)
  {
    delete [] share->column_name_str;
    share->column_name_str = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_db_convert_key_hint_str(
  SPIDER_SHARE *share,
  TABLE_SHARE *table_share
) {
  char buf[MAX_FIELD_WIDTH];
  spider_string tmp_str(buf, MAX_FIELD_WIDTH, share->access_charset),
    *key_hint;
  int roop_count;
  DBUG_ENTER("spider_db_convert_key_hint_str");
  tmp_str.init_calc_mem(112);
  if (share->access_charset->cset != system_charset_info->cset)
  {
    /* need convertion */
    for (roop_count = 0, key_hint = share->key_hint;
      roop_count < (int) table_share->keys; roop_count++, key_hint++)
    {
      tmp_str.length(0);
      if (tmp_str.append(key_hint->ptr(), key_hint->length(),
        system_charset_info))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      key_hint->length(0);
      if (key_hint->append(tmp_str))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_append_table_name_with_reserve(
  spider_string *str,
  SPIDER_SHARE *share,
  int link_idx
) {
  DBUG_ENTER("spider_db_append_table_name_with_reserve");
  if (str->reserve(SPIDER_SQL_NAME_QUOTE_LEN * 4 +
    share->db_names_str[link_idx].length() + SPIDER_SQL_DOT_LEN +
    share->table_names_str[link_idx].length()))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  spider_db_append_table_name(str, share, link_idx);
  DBUG_RETURN(0);
}

void spider_db_append_table_name(
  spider_string *str,
  SPIDER_SHARE *share,
  int link_idx
) {
  DBUG_ENTER("spider_db_append_table_name");
  DBUG_ASSERT(str->alloced_length() >= str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_DOT_LEN +
    share->db_names_str[link_idx].length() +
    share->table_names_str[link_idx].length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_names_str[link_idx].ptr(),
    share->db_names_str[link_idx].length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_names_str[link_idx].ptr(),
    share->table_names_str[link_idx].length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  DBUG_VOID_RETURN;
}

void spider_db_append_table_name_with_adjusting(
  ha_spider *spider,
  spider_string *str,
  SPIDER_SHARE *share,
  int link_idx,
  uint sql_kind
) {
  int length;
  DBUG_ENTER("spider_db_append_table_name_with_adjusting");
  if (sql_kind == SPIDER_SQL_KIND_SQL)
  {
    spider_db_append_table_name(str, share, spider->conn_link_idx[link_idx]);
    length =
      share->db_nm_max_length -
      share->db_names_str[spider->conn_link_idx[link_idx]].length() +
      share->table_nm_max_length -
      share->table_names_str[spider->conn_link_idx[link_idx]].length();
    memset((char *) str->ptr() + str->length(), ' ', length);
    str->length(str->length() + length);
  } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
  {
    str->q_append(spider->m_handler_cid[link_idx], SPIDER_SQL_HANDLER_CID_LEN);
  }
  DBUG_VOID_RETURN;
}

void spider_db_append_column_name(
  SPIDER_SHARE *share,
  spider_string *str,
  int field_index
) {
  DBUG_ENTER("spider_db_append_column_name");
  DBUG_ASSERT(str->alloced_length() >= str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
    share->column_name_str[field_index].length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->column_name_str[field_index].ptr(),
    share->column_name_str[field_index].length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  DBUG_VOID_RETURN;
}

int spider_db_append_column_value(
  ha_spider *spider,
  SPIDER_SHARE *share,
  spider_string *str,
  Field *field,
  const uchar *new_ptr
) {
  char buf[MAX_FIELD_WIDTH];
  spider_string tmp_str(buf, MAX_FIELD_WIDTH, &my_charset_bin);
  String *ptr;
  uint length;
  DBUG_ENTER("spider_db_append_column_value");
  tmp_str.init_calc_mem(113);

  if (new_ptr)
  {
    if (
      field->type() == MYSQL_TYPE_BLOB ||
      field->real_type() == MYSQL_TYPE_VARCHAR
    ) {
      length = uint2korr(new_ptr);
      tmp_str.set_quick((char *) new_ptr + HA_KEY_BLOB_LENGTH, length,
        &my_charset_bin);
      ptr = tmp_str.get_str();
    } else if (field->type() == MYSQL_TYPE_GEOMETRY)
    {
/*
      uint mlength = SIZEOF_STORED_DOUBLE, lcnt;
      uchar *dest = (uchar *) buf;
      const uchar *source;
      for (lcnt = 0; lcnt < 4; lcnt++)
      {
        mlength = SIZEOF_STORED_DOUBLE;
        source = new_ptr + mlength + SIZEOF_STORED_DOUBLE * lcnt;
        while (mlength--)
          *dest++ = *--source;
      }
      tmp_str.length(SIZEOF_STORED_DOUBLE * lcnt);
*/
      double xmin, xmax, ymin, ymax;
/*
      float8store(buf,xmin);
      float8store(buf+8,xmax);
      float8store(buf+16,ymin);
      float8store(buf+24,ymax);
      memcpy(&xmin,new_ptr,sizeof(xmin));
      memcpy(&xmax,new_ptr + 8,sizeof(xmax));
      memcpy(&ymin,new_ptr + 16,sizeof(ymin));
      memcpy(&ymax,new_ptr + 24,sizeof(ymax));
      float8get(xmin, buf);
      float8get(xmax, buf + 8);
      float8get(ymin, buf + 16);
      float8get(ymax, buf + 24);
      DBUG_PRINT("info", ("spider geo is %f %f %f %f",
        xmin, xmax, ymin, ymax));
      DBUG_PRINT("info", ("spider geo is %.14g %.14g %.14g %.14g",
        xmin, xmax, ymin, ymax));
*/
      float8get(xmin, new_ptr);
      float8get(xmax, new_ptr + 8);
      float8get(ymin, new_ptr + 16);
      float8get(ymax, new_ptr + 24);
      DBUG_PRINT("info", ("spider geo is %f %f %f %f",
        xmin, xmax, ymin, ymax));
/*
      float8get(xmin, new_ptr + SIZEOF_STORED_DOUBLE * 4);
      float8get(xmax, new_ptr + SIZEOF_STORED_DOUBLE * 5);
      float8get(ymin, new_ptr + SIZEOF_STORED_DOUBLE * 6);
      float8get(ymax, new_ptr + SIZEOF_STORED_DOUBLE * 7);
      DBUG_PRINT("info", ("spider geo is %f %f %f %f",
        xmin, xmax, ymin, ymax));
      float8get(xmin, new_ptr + SIZEOF_STORED_DOUBLE * 8);
      float8get(xmax, new_ptr + SIZEOF_STORED_DOUBLE * 9);
      float8get(ymin, new_ptr + SIZEOF_STORED_DOUBLE * 10);
      float8get(ymax, new_ptr + SIZEOF_STORED_DOUBLE * 11);
      DBUG_PRINT("info", ("spider geo is %f %f %f %f",
        xmin, xmax, ymin, ymax));
      float8get(xmin, new_ptr + SIZEOF_STORED_DOUBLE * 12);
      float8get(xmax, new_ptr + SIZEOF_STORED_DOUBLE * 13);
      float8get(ymin, new_ptr + SIZEOF_STORED_DOUBLE * 14);
      float8get(ymax, new_ptr + SIZEOF_STORED_DOUBLE * 15);
      DBUG_PRINT("info", ("spider geo is %f %f %f %f",
        xmin, xmax, ymin, ymax));
*/
/*
      tmp_str.set_quick((char *) new_ptr, SIZEOF_STORED_DOUBLE * 4,
        &my_charset_bin);
*/
      tmp_str.length(0);
      tmp_str.q_append((char *) SPIDER_SQL_LINESTRING_HEAD_STR,
        SPIDER_SQL_LINESTRING_HEAD_LEN);
      tmp_str.q_append((char *) new_ptr, SIZEOF_STORED_DOUBLE);
      tmp_str.q_append((char *) new_ptr + SIZEOF_STORED_DOUBLE * 2,
        SIZEOF_STORED_DOUBLE);
      tmp_str.q_append((char *) new_ptr + SIZEOF_STORED_DOUBLE,
        SIZEOF_STORED_DOUBLE);
      tmp_str.q_append((char *) new_ptr + SIZEOF_STORED_DOUBLE * 3,
        SIZEOF_STORED_DOUBLE);
      ptr = tmp_str.get_str();
    } else {
      ptr = field->val_str(tmp_str.get_str(), new_ptr);
      tmp_str.mem_calc();
    }
  } else {
    ptr = field->val_str(tmp_str.get_str());
    tmp_str.mem_calc();
  }
  DBUG_PRINT("info", ("spider field->type() is %d", field->type()));
  DBUG_PRINT("info", ("spider ptr->length() is %d", ptr->length()));
/*
  if (
    field->type() == MYSQL_TYPE_BIT ||
    (field->type() >= MYSQL_TYPE_TINY_BLOB &&
      field->type() <= MYSQL_TYPE_BLOB)
  ) {
    uchar *hex_ptr = (uchar *) ptr->ptr(), *end_ptr;
    char *str_ptr;
    DBUG_PRINT("info", ("spider HEX"));
    if (str->reserve(SPIDER_SQL_HEX_LEN + ptr->length() * 2))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_HEX_STR, SPIDER_SQL_HEX_LEN);
    str_ptr = (char *) str->ptr() + str->length();
    for (end_ptr = hex_ptr + ptr->length(); hex_ptr < end_ptr; hex_ptr++)
    {
      *str_ptr++ = _dig_vec_upper[(*hex_ptr) >> 4];
      *str_ptr++ = _dig_vec_upper[(*hex_ptr) & 0x0F];
    }
    str->length(str->length() + ptr->length() * 2);
  } else 
*/
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (str)
  {
#endif
    if (field->result_type() == STRING_RESULT)
    {
      DBUG_PRINT("info", ("spider STRING_RESULT"));
      if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
      if (
        field->type() == MYSQL_TYPE_VARCHAR ||
        (field->type() >= MYSQL_TYPE_ENUM &&
          field->type() <= MYSQL_TYPE_GEOMETRY)
      ) {
        DBUG_PRINT("info", ("spider append_escaped"));
        char buf2[MAX_FIELD_WIDTH];
        spider_string tmp_str2(buf2, MAX_FIELD_WIDTH, share->access_charset);
        tmp_str2.init_calc_mem(114);
        tmp_str2.length(0);
        if (
          tmp_str2.append(ptr->ptr(), ptr->length(), field->charset()) ||
          str->reserve(tmp_str2.length() * 2) ||
          append_escaped(str->get_str(), tmp_str2.get_str())
        )
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->mem_calc();
      } else if (str->append(*ptr))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    } else if (field->str_needs_quotes())
    {
      if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN * 2 + ptr->length() * 2 + 2))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
      append_escaped(str->get_str(), ptr);
      str->mem_calc();
      str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    } else if (str->append(*ptr))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
    SPIDER_RESULT_LIST *result_list = &spider->result_list;
    spider_string *hs_str;
    if (!(hs_str = spider_db_add_str_dynamic(&result_list->hs_strs,
      SPIDER_CALC_MEM_ID(result_list->hs_strs),
      SPIDER_CALC_MEM_FUNC(result_list->hs_strs),
      SPIDER_CALC_MEM_FILE(result_list->hs_strs),
      SPIDER_CALC_MEM_LINE(result_list->hs_strs),
      &result_list->hs_strs_pos, ptr->ptr(), ptr->length())))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    if (result_list->hs_adding_keys)
    {
      DBUG_PRINT("info", ("spider add to key:%s", hs_str->c_ptr_safe()));
#ifndef HANDLERSOCKET_MYSQL_UTIL
      result_list->hs_keys.push_back(SPIDER_HS_STRING_REF(hs_str->ptr(),
        hs_str->length()));
#else
      SPIDER_HS_STRING_REF string_r =
        SPIDER_HS_STRING_REF(hs_str->ptr(), hs_str->length());
      uint old_elements = result_list->hs_keys.max_element;
      if (insert_dynamic(&result_list->hs_keys, (uchar *) &string_r))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      if (result_list->hs_keys.max_element > old_elements)
      {
        spider_alloc_calc_mem(spider_current_trx,
          result_list->hs_keys,
          (result_list->hs_keys.max_element - old_elements) *
          result_list->hs_keys.size_of_element);
      }
#endif
    } else {
      DBUG_PRINT("info", ("spider add to upd:%s", hs_str->c_ptr_safe()));
#ifndef HANDLERSOCKET_MYSQL_UTIL
      result_list->hs_upds.push_back(SPIDER_HS_STRING_REF(hs_str->ptr(),
        hs_str->length()));
#else
      SPIDER_HS_STRING_REF string_r =
        SPIDER_HS_STRING_REF(hs_str->ptr(), hs_str->length());
      uint old_elements = result_list->hs_upds.max_element;
      if (insert_dynamic(&result_list->hs_upds, (uchar *) &string_r))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      if (result_list->hs_upds.max_element > old_elements)
      {
        spider_alloc_calc_mem(spider_current_trx,
          result_list->hs_upds,
          (result_list->hs_upds.max_element - old_elements) *
          result_list->hs_upds.size_of_element);
      }
#endif
    }
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_append_column_values(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str
) {
  int error_num;
  const uchar *ptr;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  KEY *key_info = result_list->key_info;
  uint length;
  uint store_length;
  key_part_map full_key_part_map = make_prev_keypart_map(key_info->key_parts);
  key_part_map start_key_part_map;
  KEY_PART_INFO *key_part;
  Field *field;
  DBUG_ENTER("spider_db_append_column_values");

  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider key_info->key_parts=%u", key_info->key_parts));
  DBUG_PRINT("info", ("spider full_key_part_map=%lu", full_key_part_map));
  DBUG_PRINT("info", ("spider start_key_part_map=%lu", start_key_part_map));

  if (!start_key_part_map)
    DBUG_RETURN(0);

  for (
    key_part = key_info->key_part,
    length = 0,
    store_length = key_part->store_length;
    start_key_part_map;
    start_key_part_map >>= 1,
    key_part++,
    length += store_length,
    store_length = key_part->store_length
  ) {
    ptr = start_key->key + length;
    field = key_part->field;
    if ((error_num = spider_db_append_null_value(str, key_part, &ptr)))
    {
      if (error_num > 0)
        DBUG_RETURN(error_num);
    } else {
      if (spider_db_append_column_value(spider, share, str, field, ptr))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }

    if (str->reserve(SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);

  DBUG_RETURN(0);
}

int spider_db_append_select_str(
  spider_string *str
) {
  DBUG_ENTER("spider_db_append_select");
  if (str->reserve(SPIDER_SQL_SELECT_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SELECT_STR, SPIDER_SQL_SELECT_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_select(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str;
  DBUG_ENTER("spider_db_append_select");

  if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
  {
    str = &result_list->sql;
    if (str->reserve(SPIDER_SQL_SELECT_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SELECT_STR, SPIDER_SQL_SELECT_LEN);
    if (result_list->lock_type != F_WRLCK && spider->lock_mode < 1)
    {
      /* no lock */
      if (spider->share->query_cache == 1)
      {
        if (str->reserve(SPIDER_SQL_SQL_CACHE_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_SQL_CACHE_STR, SPIDER_SQL_SQL_CACHE_LEN);
      } else if (spider->share->query_cache == 2)
      {
        if (str->reserve(SPIDER_SQL_SQL_NO_CACHE_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_SQL_NO_CACHE_STR,
          SPIDER_SQL_SQL_NO_CACHE_LEN);
      }
    }
    if (spider->high_priority)
    {
      if (str->reserve(SPIDER_SQL_HIGH_PRIORITY_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_HIGH_PRIORITY_STR,
        SPIDER_SQL_HIGH_PRIORITY_LEN);
    }
  }
  if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
  {
    str = &result_list->ha_sql;
    if (str->reserve(SPIDER_SQL_HANDLER_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_HANDLER_STR, SPIDER_SQL_HANDLER_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_insert_str(
  spider_string *str,
  int insert_flg
) {
  DBUG_ENTER("spider_db_append_insert_str");
  if (insert_flg & SPIDER_DB_INSERT_REPLACE)
  {
    if (str->reserve(SPIDER_SQL_REPLACE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_REPLACE_STR, SPIDER_SQL_REPLACE_LEN);
  } else {
    if (str->reserve(SPIDER_SQL_INSERT_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_INSERT_STR, SPIDER_SQL_INSERT_LEN);
  }
  if (insert_flg & SPIDER_DB_INSERT_LOW_PRIORITY)
  {
    if (str->reserve(SPIDER_SQL_LOW_PRIORITY_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_LOW_PRIORITY_STR, SPIDER_SQL_LOW_PRIORITY_LEN);
  }
  else if (insert_flg & SPIDER_DB_INSERT_DELAYED)
  {
    if (str->reserve(SPIDER_SQL_SQL_DELAYED_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_DELAYED_STR, SPIDER_SQL_SQL_DELAYED_LEN);
  }
  else if (insert_flg & SPIDER_DB_INSERT_HIGH_PRIORITY)
  {
    if (str->reserve(SPIDER_SQL_HIGH_PRIORITY_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_HIGH_PRIORITY_STR, SPIDER_SQL_HIGH_PRIORITY_LEN);
  }
  if (insert_flg & SPIDER_DB_INSERT_IGNORE)
  {
    if (str->reserve(SPIDER_SQL_SQL_IGNORE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_IGNORE_STR, SPIDER_SQL_SQL_IGNORE_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_insert(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->insert_sql;
  DBUG_ENTER("spider_db_append_insert");

  if (
    (
      spider->write_can_replace ||
      /* for direct_dup_insert without patch for partition */
      spider->sql_command == SQLCOM_REPLACE ||
      spider->sql_command == SQLCOM_REPLACE_SELECT
    ) &&
    spider->direct_dup_insert
  ) {
    if (str->reserve(SPIDER_SQL_REPLACE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_REPLACE_STR, SPIDER_SQL_REPLACE_LEN);
  } else {
    if (str->reserve(SPIDER_SQL_INSERT_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_INSERT_STR, SPIDER_SQL_INSERT_LEN);
  }
  if (spider->low_priority)
  {
    if (str->reserve(SPIDER_SQL_LOW_PRIORITY_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_LOW_PRIORITY_STR, SPIDER_SQL_LOW_PRIORITY_LEN);
  }
  else if (spider->insert_delayed)
  {
    if (spider->share->internal_delayed)
    {
      if (str->reserve(SPIDER_SQL_SQL_DELAYED_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_SQL_DELAYED_STR, SPIDER_SQL_SQL_DELAYED_LEN);
    }
  }
  else if (
    spider->lock_type >= TL_WRITE &&
    !spider->write_can_replace &&
    /* for direct_dup_insert without patch for partition */
    spider->sql_command != SQLCOM_REPLACE &&
    spider->sql_command != SQLCOM_REPLACE_SELECT
  ) {
    if (str->reserve(SPIDER_SQL_HIGH_PRIORITY_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_HIGH_PRIORITY_STR, SPIDER_SQL_HIGH_PRIORITY_LEN);
  }
  if (
    spider->ignore_dup_key &&
    spider->direct_dup_insert &&
    !spider->write_can_replace &&
    !spider->insert_with_update &&
    /* for direct_dup_insert without patch for partition */
    spider->sql_command != SQLCOM_REPLACE &&
    spider->sql_command != SQLCOM_REPLACE_SELECT
  ) {
    if (str->reserve(SPIDER_SQL_SQL_IGNORE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_IGNORE_STR, SPIDER_SQL_SQL_IGNORE_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_insert_for_recovery(
  spider_string *insert_sql,
  ha_spider *spider,
  const TABLE *table,
  int link_idx
) {
  SPIDER_SHARE *share = spider->share;
  Field **field;
  uint field_name_length = 0;
  bool add_value = FALSE;
  DBUG_ENTER("spider_db_append_insert_for_recovery");
  if (insert_sql->reserve(
    SPIDER_SQL_INSERT_LEN + SPIDER_SQL_SQL_IGNORE_LEN +
    SPIDER_SQL_INTO_LEN + share->db_nm_max_length +
    SPIDER_SQL_DOT_LEN + share->table_nm_max_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  insert_sql->q_append(SPIDER_SQL_INSERT_STR, SPIDER_SQL_INSERT_LEN);
  insert_sql->q_append(SPIDER_SQL_SQL_IGNORE_STR, SPIDER_SQL_SQL_IGNORE_LEN);
  insert_sql->q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  spider_db_append_table_name(insert_sql, share,
    spider->conn_link_idx[link_idx]);
  insert_sql->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  for (field = table->field; *field; field++)
  {
    field_name_length =
      share->column_name_str[(*field)->field_index].length();
    if (insert_sql->reserve(field_name_length +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    spider_db_append_column_name(share, insert_sql, (*field)->field_index);
    insert_sql->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  if (field_name_length)
    insert_sql->length(insert_sql->length() - SPIDER_SQL_COMMA_LEN);
  if (insert_sql->reserve(SPIDER_SQL_VALUES_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  insert_sql->q_append(SPIDER_SQL_VALUES_STR, SPIDER_SQL_VALUES_LEN);
  insert_sql->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  for (field = table->field; *field; field++)
  {
    add_value = TRUE;
    if ((*field)->is_null())
    {
      if (insert_sql->reserve(SPIDER_SQL_NULL_LEN + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      insert_sql->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
    } else {
      if (
        spider_db_append_column_value(spider, share, insert_sql, *field, NULL) ||
        insert_sql->reserve(SPIDER_SQL_COMMA_LEN)
      )
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    insert_sql->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  if (add_value)
    insert_sql->length(insert_sql->length() - SPIDER_SQL_COMMA_LEN);
  if (insert_sql->reserve(SPIDER_SQL_CLOSE_PAREN_LEN, SPIDER_SQL_COMMA_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  insert_sql->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_update(
  spider_string *str,
  ha_spider *spider,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_update");

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
    (
      spider->do_direct_update &&
      (spider->direct_update_kinds & SPIDER_SQL_KIND_SQL)
    ) ||
    (
      !spider->do_direct_update &&
#endif
      (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
    )
#endif
  ) {
#endif
    if (str->reserve(SPIDER_SQL_UPDATE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_UPDATE_STR, SPIDER_SQL_UPDATE_LEN);
    if (spider->low_priority)
    {
      if (str->reserve(SPIDER_SQL_LOW_PRIORITY_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_LOW_PRIORITY_STR, SPIDER_SQL_LOW_PRIORITY_LEN);
    }
    if (
      spider->ignore_dup_key &&
      !spider->insert_with_update
    ) {
      if (str->reserve(SPIDER_SQL_SQL_IGNORE_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_SQL_IGNORE_STR, SPIDER_SQL_SQL_IGNORE_LEN);
    }
    if (str->reserve(share->db_nm_max_length +
      SPIDER_SQL_DOT_LEN + share->table_nm_max_length +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    result_list->table_name_pos = str->length();
    spider_db_append_table_name_with_adjusting(spider, str, share, link_idx,
      SPIDER_SQL_KIND_SQL);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_append_delete(
  spider_string *str,
  ha_spider *spider
) {
  DBUG_ENTER("spider_db_append_delete");
  DBUG_PRINT("info",("spider do_direct_update=%s",
    spider->do_direct_update ? "TRUE" : "FALSE"));
  DBUG_PRINT("info",("spider direct_update_kinds=%u",
    spider->direct_update_kinds));

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
    (
      spider->do_direct_update &&
      (spider->direct_update_kinds & SPIDER_SQL_KIND_SQL)
    ) ||
    (
      !spider->do_direct_update &&
#endif
      (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
    )
#endif
  ) {
#endif
    if (str->reserve(SPIDER_SQL_DELETE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_DELETE_STR, SPIDER_SQL_DELETE_LEN);
    if (spider->low_priority)
    {
      if (str->reserve(SPIDER_SQL_LOW_PRIORITY_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_LOW_PRIORITY_STR, SPIDER_SQL_LOW_PRIORITY_LEN);
    }
    if (spider->quick_mode)
    {
      if (str->reserve(SPIDER_SQL_SQL_QUICK_MODE_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_SQL_QUICK_MODE_STR,
        SPIDER_SQL_SQL_QUICK_MODE_LEN);
    }
    if (spider->ignore_dup_key)
    {
      if (str->reserve(SPIDER_SQL_SQL_IGNORE_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_SQL_IGNORE_STR, SPIDER_SQL_SQL_IGNORE_LEN);
    }
    str->length(str->length() - 1);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_append_truncate(
  ha_spider *spider,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->update_sql;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_truncate");

  if (str->reserve(SPIDER_SQL_TRUNCATE_TABLE_LEN + share->db_nm_max_length +
    SPIDER_SQL_DOT_LEN + share->table_nm_max_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_TRUNCATE_TABLE_STR, SPIDER_SQL_TRUNCATE_TABLE_LEN);
  result_list->table_name_pos = str->length();
  spider_db_append_table_name_with_adjusting(spider, str, share, link_idx,
    SPIDER_SQL_KIND_SQL);
  DBUG_RETURN(0);
}

int spider_db_append_from_str(
  spider_string *str
) {
  DBUG_ENTER("spider_db_append_from_str");
  if (str->reserve(SPIDER_SQL_FROM_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_from(
  spider_string *str,
  ha_spider *spider,
  int link_idx
) {
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_append_from");
  if (str)
  {
    if (str->reserve(SPIDER_SQL_FROM_LEN + share->db_nm_max_length +
      SPIDER_SQL_DOT_LEN + share->table_nm_max_length +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
    result_list->table_name_pos = str->length();
    spider_db_append_table_name_with_adjusting(spider, str, share, link_idx,
      SPIDER_SQL_KIND_SQL);
  } else if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
  {
    str = &result_list->ha_sql;
    result_list->ha_table_name_pos = str->length();
    result_list->ha_sql_handler_id = spider->m_handler_id[link_idx];
    if (str->reserve(SPIDER_SQL_HANDLER_CID_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(spider->m_handler_cid[link_idx], SPIDER_SQL_HANDLER_CID_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_from_with_alias(
  spider_string *str,
  const char **table_names,
  uint *table_name_lengths,
  const char **table_aliases,
  uint *table_alias_lengths,
  uint table_count,
  int *table_name_pos,
  bool over_write
) {
  uint roop_count, length = 0;
  DBUG_ENTER("spider_db_append_from_with_alias");
  if (!over_write)
  {
    for (roop_count = 0; roop_count < table_count; roop_count++)
      length += table_name_lengths[roop_count] + SPIDER_SQL_SPACE_LEN +
        table_alias_lengths[roop_count] + SPIDER_SQL_COMMA_LEN;
    if (str->reserve(SPIDER_SQL_FROM_LEN + length))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
    *table_name_pos = str->length();
  }
  for (roop_count = 0; roop_count < table_count; roop_count++)
  {
    str->q_append(table_names[roop_count], table_name_lengths[roop_count]);
    str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    str->q_append(table_aliases[roop_count], table_alias_lengths[roop_count]);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_open_paren_str(
  spider_string *str
) {
  DBUG_ENTER("spider_db_append_open_paren_str");
  if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_into_str(
  spider_string *str
) {
  DBUG_ENTER("spider_db_append_into_str");
  if (str->reserve(SPIDER_SQL_INTO_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_values_str(
  spider_string *str
) {
  DBUG_ENTER("spider_db_append_values_str");
  if (str->reserve(SPIDER_SQL_VALUES_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_VALUES_STR, SPIDER_SQL_VALUES_LEN);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_into(
  ha_spider *spider,
  const TABLE *table,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->insert_sql;
  SPIDER_SHARE *share = spider->share;
  Field **field;
  uint field_name_length = 0;
  DBUG_ENTER("spider_db_append_into");
  if (str->reserve(SPIDER_SQL_INTO_LEN + share->db_nm_max_length +
    SPIDER_SQL_DOT_LEN + share->table_nm_max_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  result_list->insert_table_name_pos = str->length();
  spider_db_append_table_name_with_adjusting(spider, str, share, link_idx,
    SPIDER_SQL_KIND_SQL);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  for (field = table->field; *field; field++)
  {
    if (
      bitmap_is_set(table->write_set, (*field)->field_index) ||
      bitmap_is_set(table->read_set, (*field)->field_index)
    ) {
      field_name_length =
        share->column_name_str[(*field)->field_index].length();
      if (str->reserve(field_name_length +
        (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      spider_db_append_column_name(share, str, (*field)->field_index);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
  }
  if (field_name_length)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  if (str->reserve(SPIDER_SQL_VALUES_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_VALUES_STR, SPIDER_SQL_VALUES_LEN);
  result_list->insert_pos = str->length();
  DBUG_RETURN(0);
}

int spider_db_append_update_set(
  spider_string *str,
  ha_spider *spider,
  TABLE *table
) {
  uint field_name_length;
  SPIDER_SHARE *share = spider->share;
  Field **fields;
  DBUG_ENTER("spider_db_append_update_set");
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
    !spider->do_direct_update &&
#endif
    (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
  ) {
#endif
    DBUG_PRINT("info",("spider add set for SPIDER_SQL_KIND_SQL"));
    if (str->reserve(SPIDER_SQL_SET_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SET_STR, SPIDER_SQL_SET_LEN);
    for (fields = table->field; *fields; fields++)
    {
      if (bitmap_is_set(table->write_set, (*fields)->field_index))
      {
        field_name_length =
          share->column_name_str[(*fields)->field_index].length();
        if ((*fields)->is_null())
        {
          if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) *
            2 + SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_NULL_LEN +
            SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, (*fields)->field_index);
          str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
          str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
        } else {
          if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) *
            2 + SPIDER_SQL_EQUAL_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, (*fields)->field_index);
          str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
#ifndef DBUG_OFF
          my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table,
            table->read_set);
#endif
          if (
            spider_db_append_column_value(spider, share, str, *fields, NULL) ||
            str->reserve(SPIDER_SQL_COMMA_LEN)
          ) {
#ifndef DBUG_OFF
            dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          }
#ifndef DBUG_OFF
          dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
        }
        str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      }
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  }
#endif

#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
  if (
    spider->do_direct_update &&
    spider->direct_update_kinds == SPIDER_SQL_KIND_SQL &&
    spider->direct_update_fields
  ) {
    DBUG_PRINT("info",("spider add set for DU ITEMS"));
    if (str->reserve(SPIDER_SQL_SET_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SET_STR, SPIDER_SQL_SET_LEN);
    DBUG_RETURN(spider_db_append_update_columns(spider, str, NULL, 0));
  }

  if (
    spider->do_direct_update &&
    (spider->direct_update_kinds & SPIDER_SQL_KIND_SQL)
  ) {
    DBUG_PRINT("info",("spider add set for DU SPIDER_SQL_KIND_SQL"));
    size_t roop_count;
    Field *field;
    if (str->reserve(SPIDER_SQL_SET_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SET_STR, SPIDER_SQL_SET_LEN);
    for (roop_count = 0; roop_count < spider->hs_pushed_ret_fields_num;
      roop_count++)
    {
      Field *top_table_field =
        spider->get_top_table_field(spider->hs_pushed_ret_fields[roop_count]);
      if (!(field = spider->field_exchange(top_table_field)))
        continue;
      field_name_length =
        share->column_name_str[field->field_index].length();
      if (top_table_field->is_null())
      {
        if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) *
          2 + SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_NULL_LEN +
          SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(share, str, field->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
        str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
      } else {
        if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) *
          2 + SPIDER_SQL_EQUAL_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(share, str, field->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
#ifndef DBUG_OFF
        my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table,
          table->read_set);
#endif
        if (
          spider_db_append_column_value(spider, share, str, top_table_field,
            NULL) ||
          str->reserve(SPIDER_SQL_COMMA_LEN)
        ) {
#ifndef DBUG_OFF
          dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
#ifndef DBUG_OFF
        dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
      }
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  }
#endif

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
  if (
    spider->do_direct_update &&
    (spider->direct_update_kinds & SPIDER_SQL_KIND_HS)
  ) {
    DBUG_PRINT("info",("spider add set for DU SPIDER_SQL_KIND_HS"));
    size_t roop_count;
    Field *field;
    SPIDER_RESULT_LIST *result_list = &spider->result_list;
    result_list->hs_adding_keys = FALSE;
    for (roop_count = 0; roop_count < spider->hs_pushed_ret_fields_num;
      roop_count++)
    {
      Field *top_table_field =
        spider->get_top_table_field(spider->hs_pushed_ret_fields[roop_count]);
      if (!(field = spider->field_exchange(top_table_field)))
        continue;
      if (top_table_field->is_null())
      {
#ifndef HANDLERSOCKET_MYSQL_UTIL
        spider->result_list.hs_upds.push_back(SPIDER_HS_STRING_REF());
#else
        SPIDER_HS_STRING_REF string_r = SPIDER_HS_STRING_REF();
        uint old_elements = spider->result_list.hs_upds.max_element;
        if (
          insert_dynamic(&spider->result_list.hs_upds, (uchar *) &string_r)
        )
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        if (spider->result_list.hs_upds.max_element > old_elements)
        {
          spider_alloc_calc_mem(spider_current_trx,
            spider->result_list.hs_upds,
            (spider->result_list.hs_upds.max_element - old_elements) *
            spider->result_list.hs_upds.size_of_element);
        }
#endif
      } else {
        if (spider_db_append_column_value(spider, share, NULL, top_table_field,
          NULL))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    }
  }
#endif
#endif
  DBUG_RETURN(0);
}

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
int spider_db_append_increment_update_set(
  spider_string *str,
  ha_spider *spider,
  TABLE *table
) {
  uint field_name_length;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_append_increment_update_set");
  if (spider->direct_update_kinds & SPIDER_SQL_KIND_SQL)
  {
    DBUG_PRINT("info",("spider add set for DU SPIDER_SQL_KIND_SQL"));
    uint roop_count;
    Field *field;
    if (str->reserve(SPIDER_SQL_SET_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SET_STR, SPIDER_SQL_SET_LEN);
#ifndef HANDLERSOCKET_MYSQL_UTIL
    const SPIDER_HS_STRING_REF *value = &result_list->hs_upds[0];
    for (roop_count = 0; roop_count < result_list->hs_upds.size();
      roop_count++)
#else
    const SPIDER_HS_STRING_REF *value =
      (SPIDER_HS_STRING_REF *) result_list->hs_upds.buffer;
    for (roop_count = 0; roop_count < result_list->hs_upds.elements;
      roop_count++)
#endif
    {
      if (
        value[roop_count].size() == 1 &&
        *(value[roop_count].begin()) == '0'
      )
        continue;

      Field *top_table_field =
        spider->get_top_table_field(spider->hs_pushed_ret_fields[roop_count]);
      if (!(field = spider->field_exchange(top_table_field)))
        continue;
      field_name_length =
        share->column_name_str[field->field_index].length();

      if (str->reserve(field_name_length * 2 + (SPIDER_SQL_NAME_QUOTE_LEN) *
        4 + SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_HS_INCREMENT_LEN +
        SPIDER_SQL_COMMA_LEN + value[roop_count].size()))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);

      spider_db_append_column_name(share, str, field->field_index);
      str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
      spider_db_append_column_name(share, str, field->field_index);
      if (spider->hs_increment)
        str->q_append(SPIDER_SQL_HS_INCREMENT_STR,
          SPIDER_SQL_HS_INCREMENT_LEN);
      else
        str->q_append(SPIDER_SQL_HS_DECREMENT_STR,
          SPIDER_SQL_HS_DECREMENT_LEN);
      str->q_append(value[roop_count].begin(), value[roop_count].size());
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  }

  if (spider->direct_update_kinds & SPIDER_SQL_KIND_HS)
  {
    DBUG_PRINT("info",("spider add set for DU SPIDER_SQL_KIND_HS"));
    /* nothing to do */
  }
  DBUG_RETURN(0);
}
#endif
#endif

int spider_db_append_update_where(
  spider_string *str,
  ha_spider *spider,
  const TABLE *table,
  my_ptrdiff_t ptr_diff
) {
  uint field_name_length;
  SPIDER_SHARE *share = spider->share;
  Field **field;
  DBUG_ENTER("spider_db_append_update_where");
  if (str->reserve(SPIDER_SQL_WHERE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
  for (field = table->field; *field; field++)
  {
    if (
      table->s->primary_key == MAX_KEY ||
      bitmap_is_set(table->read_set, (*field)->field_index)
    ) {
      field_name_length =
        share->column_name_str[(*field)->field_index].length();
      if ((*field)->is_null(ptr_diff))
      {
        if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
          SPIDER_SQL_IS_NULL_LEN + SPIDER_SQL_AND_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(share, str, (*field)->field_index);
        str->q_append(SPIDER_SQL_IS_NULL_STR, SPIDER_SQL_IS_NULL_LEN);
      } else {
        if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
          SPIDER_SQL_EQUAL_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(share, str, (*field)->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
        (*field)->move_field_offset(ptr_diff);
        if (
          spider_db_append_column_value(spider, share, str, *field, NULL) ||
          str->reserve(SPIDER_SQL_AND_LEN)
        )
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        (*field)->move_field_offset(-ptr_diff);
      }
      str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
    }
  }
  str->length(str->length() - SPIDER_SQL_AND_LEN);
  if (str->reserve(SPIDER_SQL_LIMIT1_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_LIMIT1_STR, SPIDER_SQL_LIMIT1_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_table_select(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider
) {
  SPIDER_SHARE *share = spider->share;
  Field **field;
  int field_length;
  DBUG_ENTER("spider_db_append_table_select");
  for (field = table->field; *field; field++)
  {
    field_length = share->column_name_str[(*field)->field_index].length();
    if (str->reserve(field_length +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    spider_db_append_column_name(share, str, (*field)->field_index);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(spider_db_append_from(str, spider, 0));
}

int spider_db_append_key_select(
  spider_string *str,
  const KEY *key_info,
  ha_spider *spider
) {
  SPIDER_SHARE *share = spider->share;
  KEY_PART_INFO *key_part;
  Field *field;
  uint part_num;
  int field_length;
  DBUG_ENTER("spider_db_append_key_select");
  for (key_part = key_info->key_part, part_num = 0;
    part_num < key_info->key_parts; key_part++, part_num++)
  {
    field = key_part->field;
    field_length = share->column_name_str[field->field_index].length();
    if (str->reserve(field_length +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    spider_db_append_column_name(share, str, field->field_index);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(spider_db_append_from(str, spider, 0));
}

int spider_db_append_minimum_select(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider
) {
  SPIDER_SHARE *share = spider->share;
  Field **field;
  int field_length;
  bool appended = FALSE;
  DBUG_ENTER("spider_db_append_minimum_select");
  for (field = table->field; *field; field++)
  {
    if (
      spider_bit_is_set(spider->searched_bitmap, (*field)->field_index) |
      bitmap_is_set(table->read_set, (*field)->field_index) |
      bitmap_is_set(table->write_set, (*field)->field_index)
    ) {
      field_length = share->column_name_str[(*field)->field_index].length();
      if (str->reserve(field_length +
        (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      spider_db_append_column_name(share, str, (*field)->field_index);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      appended = TRUE;
    }
  }
  if (appended)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  else {
    if (str->reserve(SPIDER_SQL_ONE_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_ONE_STR, SPIDER_SQL_ONE_LEN);
  }
  DBUG_RETURN(spider_db_append_from(str, spider, 0));
}

int spider_db_append_minimum_select_without_quote(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider
) {
  SPIDER_SHARE *share = spider->share;
  Field **field;
  int field_length;
  bool appended = FALSE;
  DBUG_ENTER("spider_db_append_minimum_select_without_quote");
  for (field = table->field; *field; field++)
  {
    if (
      spider_bit_is_set(spider->searched_bitmap, (*field)->field_index) |
      bitmap_is_set(table->read_set, (*field)->field_index) |
      bitmap_is_set(table->write_set, (*field)->field_index)
    ) {
      field_length = share->column_name_str[(*field)->field_index].length();
      if (str->reserve(field_length + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(share->column_name_str[(*field)->field_index].ptr(),
        share->column_name_str[(*field)->field_index].length());
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      appended = TRUE;
    }
  }
  if (appended)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
int spider_db_append_minimum_select_by_field_idx_list(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider,
  uint32 *field_idxs,
  size_t field_idxs_num
) {
  SPIDER_SHARE *share = spider->share;
  Field *field;
  int roop_count, field_length;
  bool appended = FALSE;
  DBUG_ENTER("spider_db_append_minimum_select_by_field_idx_list");
  for (roop_count = 0; roop_count < (int) field_idxs_num; roop_count++)
  {
    field = spider->get_top_table_field(field_idxs[roop_count]);
    if ((field = spider->field_exchange(field)))
    {
      field_length = share->column_name_str[field->field_index].length();
      if (str->reserve(field_length + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(share->column_name_str[field->field_index].ptr(),
        share->column_name_str[field->field_index].length());
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      appended = TRUE;
    }
  }
  if (appended)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}
#endif
#endif

int spider_db_append_select_columns(
  ha_spider *spider
) {
  int error_num;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_append_select_columns");
  if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
  {
    if ((error_num = spider_db_append_match_select(&result_list->sql, spider,
      NULL, 0)))
      DBUG_RETURN(error_num);
    if (!spider->select_column_mode)
    {
      if (result_list->keyread)
      {
        result_list->table_name_pos = result_list->sql.length() +
          share->key_select_pos[spider->active_index];
        if (result_list->sql.append(share->key_select[spider->active_index]))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      } else {
        result_list->table_name_pos = result_list->sql.length() +
          share->table_select_pos;
        if (result_list->sql.append(*(share->table_select)))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    } else {
      if ((error_num = spider_db_append_minimum_select(
        &result_list->sql, spider->get_table(), spider)))
        DBUG_RETURN(error_num);
    }
  }
  if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
  {
    if ((error_num = spider_db_append_from(NULL, spider, 0)))
      DBUG_RETURN(error_num);
  }
  DBUG_RETURN(0);
}

int spider_db_append_table_select_with_alias(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider,
  const char *alias,
  uint alias_length
) {
  SPIDER_SHARE *share = spider->share;
  Field **field;
  int field_length;
  DBUG_ENTER("spider_db_append_table_select_with_alias");
  for (field = table->field; *field; field++)
  {
    field_length = share->column_name_str[(*field)->field_index].length();
    if (str->reserve(alias_length + field_length +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(alias, alias_length);
    spider_db_append_column_name(share, str, (*field)->field_index);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_key_select_with_alias(
  spider_string *str,
  const KEY *key_info,
  ha_spider *spider,
  const char *alias,
  uint alias_length
) {
  SPIDER_SHARE *share = spider->share;
  KEY_PART_INFO *key_part;
  Field *field;
  uint part_num;
  int field_length;
  DBUG_ENTER("spider_db_append_key_select_with_alias");
  for (key_part = key_info->key_part, part_num = 0;
    part_num < key_info->key_parts; key_part++, part_num++)
  {
    field = key_part->field;
    field_length = share->column_name_str[field->field_index].length();
    if (str->reserve(alias_length + field_length +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(alias, alias_length);
    spider_db_append_column_name(share, str, field->field_index);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_minimum_select_with_alias(
  spider_string *str,
  const TABLE *table,
  ha_spider *spider,
  const char *alias,
  uint alias_length
) {
  SPIDER_SHARE *share = spider->share;
  Field **field;
  int field_length;
  bool appended = FALSE;
  DBUG_ENTER("spider_db_append_minimum_select_with_alias");
  for (field = table->field; *field; field++)
  {
    if (
      spider_bit_is_set(spider->searched_bitmap, (*field)->field_index) |
      bitmap_is_set(table->read_set, (*field)->field_index) |
      bitmap_is_set(table->write_set, (*field)->field_index)
    ) {
      field_length = share->column_name_str[(*field)->field_index].length();
      if (str->reserve(alias_length + field_length +
        (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(alias, alias_length);
      spider_db_append_column_name(share, str, (*field)->field_index);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      appended = TRUE;
    }
  }
  if (appended)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  else {
    if (str->reserve(SPIDER_SQL_ONE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_ONE_STR, SPIDER_SQL_ONE_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_select_columns_with_alias(
  ha_spider *spider,
  const char *alias,
  uint alias_length
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_append_select_columns_with_alias");
  if ((error_num = spider_db_append_match_select(&result_list->sql, spider,
    alias, alias_length)))
    DBUG_RETURN(error_num);
  if (!spider->select_column_mode)
  {
    if (result_list->keyread)
      DBUG_RETURN(spider_db_append_key_select_with_alias(
        &result_list->sql, result_list->key_info, spider,
        alias, alias_length));
    else
      DBUG_RETURN(spider_db_append_table_select_with_alias(
        &result_list->sql, spider->get_table(), spider, alias, alias_length));
  }
  DBUG_RETURN(spider_db_append_minimum_select_with_alias(
    &result_list->sql, spider->get_table(), spider, alias, alias_length));
}

int spider_db_append_null(
  ha_spider *spider,
  SPIDER_SHARE *share,
  spider_string *str,
  KEY_PART_INFO *key_part,
  const key_range *key,
  const uchar **ptr,
  uint sql_kind
) {
  DBUG_ENTER("spider_db_append_null");
  if (key_part->null_bit)
  {
    if (*(*ptr)++)
    {
      switch (sql_kind)
      {
        case SPIDER_SQL_KIND_HANDLER:
          str = &spider->result_list.sql_part;
          if (key->flag == HA_READ_KEY_EXACT)
          {
            if (str->reserve(SPIDER_SQL_IS_NULL_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_IS_NULL_STR, SPIDER_SQL_IS_NULL_LEN);
          } else {
            str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
            spider->result_list.ha_next_pos = str->length();
            if (str->reserve(SPIDER_SQL_FIRST_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_FIRST_STR, SPIDER_SQL_FIRST_LEN);
            spider->result_list.ha_read_kind = 1;
          }
          str = &spider->result_list.sql_part2;
          /* fall through */
        case SPIDER_SQL_KIND_SQL:
          if (key->flag == HA_READ_KEY_EXACT)
          {
            if (str->reserve(SPIDER_SQL_IS_NULL_LEN +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
              share->column_name_str[key_part->field->field_index].length()))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            spider_db_append_column_name(share, str,
              key_part->field->field_index);
            str->q_append(SPIDER_SQL_IS_NULL_STR,
              SPIDER_SQL_IS_NULL_LEN);
          } else {
            if (str->reserve(SPIDER_SQL_IS_NOT_NULL_LEN +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
              share->column_name_str[key_part->field->field_index].length()))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            spider_db_append_column_name(share, str,
              key_part->field->field_index);
            str->q_append(SPIDER_SQL_IS_NOT_NULL_STR,
              SPIDER_SQL_IS_NOT_NULL_LEN);
          }
          break;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
        case SPIDER_SQL_KIND_HS:
        {
#ifndef HANDLERSOCKET_MYSQL_UTIL
          spider->result_list.hs_keys.push_back(SPIDER_HS_STRING_REF());
#else
          SPIDER_HS_STRING_REF string_r = SPIDER_HS_STRING_REF();
          uint old_elements = spider->result_list.hs_keys.max_element;
          if (
            insert_dynamic(&spider->result_list.hs_keys, (uchar *) &string_r)
          )
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          if (spider->result_list.hs_keys.max_element > old_elements)
          {
            spider_alloc_calc_mem(spider_current_trx,
              spider->result_list.hs_keys,
              (spider->result_list.hs_keys.max_element - old_elements) *
              spider->result_list.hs_keys.size_of_element);
          }
#endif
        }
#endif
        default:
          break;
      }
      DBUG_RETURN(-1);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_append_null_value(
  spider_string *str,
  KEY_PART_INFO *key_part,
  const uchar **ptr
) {
  DBUG_ENTER("spider_db_append_null_value");
  if (key_part->null_bit)
  {
    if (*(*ptr)++)
    {
      if (str->reserve(SPIDER_SQL_NULL_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
      DBUG_RETURN(-1);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_append_key_column_types(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  KEY *key_info = result_list->key_info;
  uint key_name_length, key_count;
  key_part_map full_key_part_map = make_prev_keypart_map(key_info->key_parts);
  key_part_map start_key_part_map;
  KEY_PART_INFO *key_part;
  Field *field;
  char tmp_buf[MAX_FIELD_WIDTH];
  spider_string tmp_str(tmp_buf, sizeof(tmp_buf), system_charset_info);
  DBUG_ENTER("spider_db_append_key_column_types");
  tmp_str.init_calc_mem(115);

  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider key_info->key_parts=%u", key_info->key_parts));
  DBUG_PRINT("info", ("spider full_key_part_map=%lu", full_key_part_map));
  DBUG_PRINT("info", ("spider start_key_part_map=%lu", start_key_part_map));

  if (!start_key_part_map)
    DBUG_RETURN(0);

  for (
    key_part = key_info->key_part,
    key_count = 0;
    start_key_part_map;
    start_key_part_map >>= 1,
    key_part++,
    key_count++
  ) {
    field = key_part->field;
    key_name_length = my_sprintf(tmp_buf, (tmp_buf, "c%u", key_count));
    if (str->reserve(key_name_length + SPIDER_SQL_SPACE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(tmp_buf, key_name_length);
    str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);

    if (tmp_str.ptr() != tmp_buf)
      tmp_str.set(tmp_buf, sizeof(tmp_buf), system_charset_info);
    else
      tmp_str.set_charset(system_charset_info);
    field->sql_type(*tmp_str.get_str());
    tmp_str.mem_calc();
    str->append(tmp_str);

    if (str->reserve(SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);

  DBUG_RETURN(0);
}

int spider_db_append_table_columns(
  spider_string *str,
  TABLE_SHARE *table_share
) {
  int error_num;
  Field **field;
  DBUG_ENTER("spider_db_append_table_columns");
  for (field = table_share->field; *field; field++)
  {
    if ((error_num = spider_db_append_name_with_quote_str(str,
      (char *) (*field)->field_name)))
      DBUG_RETURN(error_num);
    if (str->reserve(SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_key_columns(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  KEY *key_info = result_list->key_info;
  uint key_name_length, key_count;
  key_part_map full_key_part_map = make_prev_keypart_map(key_info->key_parts);
  key_part_map start_key_part_map;
  char tmp_buf[MAX_FIELD_WIDTH];
  DBUG_ENTER("spider_db_append_key_columns");

  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider key_info->key_parts=%u", key_info->key_parts));
  DBUG_PRINT("info", ("spider full_key_part_map=%lu", full_key_part_map));
  DBUG_PRINT("info", ("spider start_key_part_map=%lu", start_key_part_map));

  if (!start_key_part_map)
    DBUG_RETURN(0);

  for (
    key_count = 0;
    start_key_part_map;
    start_key_part_map >>= 1,
    key_count++
  ) {
    key_name_length = my_sprintf(tmp_buf, (tmp_buf, "c%u", key_count));
    if (str->reserve(key_name_length + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(tmp_buf, key_name_length);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);

  DBUG_RETURN(0);
}

int spider_db_append_key_join_columns_for_bka(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str,
  const char **table_aliases,
  uint *table_alias_lengths
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  KEY *key_info = result_list->key_info;
  uint length, key_name_length, key_count;
  key_part_map full_key_part_map = make_prev_keypart_map(key_info->key_parts);
  key_part_map start_key_part_map;
  KEY_PART_INFO *key_part;
  Field *field;
  char tmp_buf[MAX_FIELD_WIDTH];
  bool where_pos = ((int) str->length() == spider->result_list.where_pos);
  DBUG_ENTER("spider_db_append_key_join_columns_for_bka");

  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider key_info->key_parts=%u", key_info->key_parts));
  DBUG_PRINT("info", ("spider full_key_part_map=%lu", full_key_part_map));
  DBUG_PRINT("info", ("spider start_key_part_map=%lu", start_key_part_map));

  if (!start_key_part_map)
    DBUG_RETURN(0);

  if (where_pos)
  {
    if (str->reserve(SPIDER_SQL_WHERE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
  } else {
    if (str->reserve(SPIDER_SQL_AND_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
  }

  for (
    key_part = key_info->key_part,
    key_count = 0;
    start_key_part_map;
    start_key_part_map >>= 1,
    key_part++,
    key_count++
  ) {
    field = key_part->field;
    key_name_length = share->column_name_str[field->field_index].length();
    length = my_sprintf(tmp_buf, (tmp_buf, "c%u", key_count));
    if (str->reserve(length + table_alias_lengths[0] + key_name_length +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
      table_alias_lengths[1] + SPIDER_SQL_PF_EQUAL_LEN + SPIDER_SQL_AND_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(table_aliases[0], table_alias_lengths[0]);
    str->q_append(tmp_buf, length);
    str->q_append(SPIDER_SQL_PF_EQUAL_STR, SPIDER_SQL_PF_EQUAL_LEN);
    str->q_append(table_aliases[1], table_alias_lengths[1]);
    spider_db_append_column_name(share, str, field->field_index);
    str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
  }
  str->length(str->length() - SPIDER_SQL_AND_LEN);

  DBUG_RETURN(0);
}

int spider_db_append_key_hint(
  spider_string *str,
  char *hint_str
) {
  int hint_str_len = strlen(hint_str);
  DBUG_ENTER("spider_db_append_key_hint");
  if (hint_str_len >= 2 &&
    (hint_str[0] == 'f' || hint_str[0] == 'F') && hint_str[1] == ' '
  ) {
    if (str->reserve(hint_str_len - 2 +
      SPIDER_SQL_SQL_FORCE_IDX_LEN + SPIDER_SQL_CLOSE_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    hint_str += 2;
    str->q_append(SPIDER_SQL_SQL_FORCE_IDX_STR, SPIDER_SQL_SQL_FORCE_IDX_LEN);
    str->q_append(hint_str, hint_str_len - 2);
    str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  } else if (hint_str_len >= 2 &&
    (hint_str[0] == 'u' || hint_str[0] == 'U') && hint_str[1] == ' '
  ) {
    if (str->reserve(hint_str_len - 2 +
      SPIDER_SQL_SQL_USE_IDX_LEN + SPIDER_SQL_CLOSE_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    hint_str += 2;
    str->q_append(SPIDER_SQL_SQL_USE_IDX_STR, SPIDER_SQL_SQL_USE_IDX_LEN);
    str->q_append(hint_str, hint_str_len - 2);
    str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  } else if (hint_str_len >= 3 &&
    (hint_str[0] == 'i' || hint_str[0] == 'I') &&
    (hint_str[1] == 'g' || hint_str[1] == 'G') && hint_str[2] == ' '
  ) {
    if (str->reserve(hint_str_len - 3 +
      SPIDER_SQL_SQL_IGNORE_IDX_LEN + SPIDER_SQL_CLOSE_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    hint_str += 3;
    str->q_append(
      SPIDER_SQL_SQL_IGNORE_IDX_STR, SPIDER_SQL_SQL_IGNORE_IDX_LEN);
    str->q_append(hint_str, hint_str_len - 3);
    str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  } else if (str->reserve(hint_str_len + SPIDER_SQL_SPACE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  else
  {
    str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    str->q_append(hint_str, hint_str_len);
  }
  DBUG_RETURN(0);
}

int spider_db_append_hint_after_table(
  ha_spider *spider,
  spider_string *str,
  spider_string *hint
) {
  DBUG_ENTER("spider_db_append_hint_after_table");
  if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
  {
    if (str->append(*hint))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  DBUG_RETURN(0);
}

int spider_db_append_key_where_internal(
  const key_range *start_key,
  const key_range *end_key,
  ha_spider *spider,
  uint sql_kind,
  bool update_sql
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
#ifndef DBUG_OFF
  TABLE *table = spider->get_table();
#endif
  spider_string *str, *str_part = NULL, *str_part2 = NULL;
  KEY *key_info = result_list->key_info;
  int error_num;
  uint key_name_length;
  key_part_map full_key_part_map;
  key_part_map start_key_part_map;
  key_part_map end_key_part_map;
  key_part_map tgt_key_part_map;
  int key_count;
  uint length;
  uint store_length;
  const uchar *ptr, *another_ptr;
  const key_range *use_key, *another_key;
  KEY_PART_INFO *key_part;
  Field *field;
  bool use_both = TRUE, key_eq, set_order;
  DBUG_ENTER("spider_db_append_key_where_internal");
  if (key_info)
    full_key_part_map = make_prev_keypart_map(key_info->key_parts);
  else
    full_key_part_map = 0;
  if (sql_kind == SPIDER_SQL_KIND_SQL)
  {
    if (update_sql)
      str = &result_list->update_sql;
    else
      str = &result_list->sql;
    set_order = FALSE;
  } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
  {
    str = &result_list->ha_sql;
    result_list->ha_read_pos = str->length();
    str_part = &result_list->sql_part;
    str_part2 = &result_list->sql_part2;
    str_part->length(0);
    str_part2->length(0);
    set_order = TRUE;
  }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  else {
    str = &result_list->hs_sql;
    str->length(0);
    result_list->hs_adding_keys = TRUE;
    set_order = FALSE;
  }
#endif

  if (start_key)
    start_key_part_map = start_key->keypart_map & full_key_part_map;
  else {
    start_key_part_map = 0;
    use_both = FALSE;
  }
  if (end_key)
    end_key_part_map = end_key->keypart_map & full_key_part_map;
  else {
    end_key_part_map = 0;
    use_both = FALSE;
  }
  DBUG_PRINT("info", ("spider key_info->key_parts=%u", key_info ?
    key_info->key_parts : 0));
  DBUG_PRINT("info", ("spider full_key_part_map=%lu", full_key_part_map));
  DBUG_PRINT("info", ("spider start_key_part_map=%lu", start_key_part_map));
  DBUG_PRINT("info", ("spider end_key_part_map=%lu", end_key_part_map));

#ifndef DBUG_OFF
  my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table, table->read_set);
#endif

  if (sql_kind == SPIDER_SQL_KIND_HANDLER)
  {
    char *key_name = key_info->name;
    key_name_length = strlen(key_name);
    str = &result_list->ha_sql;
    if (start_key_part_map || end_key_part_map)
    {
      if (str->reserve(SPIDER_SQL_READ_LEN + (SPIDER_SQL_NAME_QUOTE_LEN * 2) +
        key_name_length))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_READ_STR, SPIDER_SQL_READ_LEN);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      str->q_append(key_name, key_name_length);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      result_list->ha_next_pos = str->length();
      if (str_part->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str_part->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
      result_list->ha_read_kind = 0;
    } else if (!result_list->desc_flg)
    {
      if (str->reserve(SPIDER_SQL_READ_LEN + (SPIDER_SQL_NAME_QUOTE_LEN * 2) +
        key_name_length + SPIDER_SQL_FIRST_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_READ_STR, SPIDER_SQL_READ_LEN);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      str->q_append(key_name, key_name_length);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      result_list->ha_next_pos = str->length();
      str->q_append(SPIDER_SQL_FIRST_STR, SPIDER_SQL_FIRST_LEN);
      result_list->ha_read_kind = 1;
    } else {
      if (str->reserve(SPIDER_SQL_READ_LEN + (SPIDER_SQL_NAME_QUOTE_LEN * 2) +
        key_name_length + SPIDER_SQL_LAST_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_READ_STR, SPIDER_SQL_READ_LEN);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      str->q_append(key_name, key_name_length);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      result_list->ha_next_pos = str->length();
      str->q_append(SPIDER_SQL_LAST_STR, SPIDER_SQL_LAST_LEN);
      result_list->ha_read_kind = 2;
    }
  }
  if (!start_key_part_map && !end_key_part_map)
    goto end;
  else if (start_key_part_map >= end_key_part_map)
  {
    use_key = start_key;
    another_key = end_key;
    tgt_key_part_map = start_key_part_map;
  } else {
    use_key = end_key;
    another_key = start_key;
    tgt_key_part_map = end_key_part_map;
  }
  DBUG_PRINT("info", ("spider tgt_key_part_map=%lu", tgt_key_part_map));

  if (sql_kind == SPIDER_SQL_KIND_SQL)
  {
    if (str->reserve(SPIDER_SQL_WHERE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
  } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
  {
    if (str_part2->reserve(SPIDER_SQL_WHERE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str_part2->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
  }

  for (
    key_part = key_info->key_part,
    length = 0,
    key_count = 0;
    tgt_key_part_map;
    length += store_length,
    tgt_key_part_map >>= 1,
    start_key_part_map >>= 1,
    end_key_part_map >>= 1,
    key_part++,
    key_count++
  ) {
    store_length = key_part->store_length;
    field = key_part->field;
    key_name_length = share->column_name_str[field->field_index].length();
    ptr = use_key->key + length;
    if (use_both)
    {
      another_ptr = another_key->key + length;
      if (
        start_key_part_map &&
        end_key_part_map &&
        !memcmp(ptr, another_ptr, store_length)
      )
        key_eq = TRUE;
      else {
        key_eq = FALSE;
        DBUG_PRINT("info", ("spider *another_ptr=%u", *another_ptr));
#ifndef DBUG_OFF
        if (
          start_key_part_map &&
          end_key_part_map
        )
          DBUG_PRINT("info", ("spider memcmp=%d",
            memcmp(ptr, another_ptr, store_length)));
#endif
      }
    } else {
      if (tgt_key_part_map > 1)
        key_eq = TRUE;
      else
        key_eq = FALSE;
    }
    if (
      (key_eq && use_key == start_key) ||
      (!key_eq && start_key_part_map)
    ) {
      ptr = start_key->key + length;
      if (
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
        (
          sql_kind != SPIDER_SQL_KIND_HS &&
#endif
          (error_num = spider_db_append_null(spider, share, str, key_part,
            start_key, &ptr, sql_kind))
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
        ) ||
        (
          sql_kind == SPIDER_SQL_KIND_HS &&
          (error_num = spider_db_append_null(spider, share, NULL, key_part,
            start_key, &ptr, sql_kind))
        )
#endif
      ) {
        if (error_num > 0)
          DBUG_RETURN(error_num);
        if (
          !set_order &&
          start_key->flag != HA_READ_KEY_EXACT &&
          sql_kind == SPIDER_SQL_KIND_SQL
        ) {
          result_list->key_order = key_count;
          set_order = TRUE;
        }
      } else if (key_eq)
      {
        DBUG_PRINT("info", ("spider key_eq"));
        if (sql_kind == SPIDER_SQL_KIND_SQL)
        {
          if (str->reserve(store_length + key_name_length +
            (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
            SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_AND_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
          if (spider_db_append_column_value(spider, share, str, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
        {
          if (str_part2->reserve(store_length + key_name_length +
            (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
            SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_AND_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str_part2, field->field_index);
          str_part2->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
          if (spider_db_append_column_value(spider, share, str_part2, field,
            ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);

          if (spider_db_append_column_value(spider, share, str_part, field,
            ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
        else {
          if (spider_db_append_column_value(spider, share, NULL, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
#endif
      } else {
        DBUG_PRINT("info", ("spider start_key->flag=%d", start_key->flag));
        switch (start_key->flag)
        {
          case HA_READ_KEY_EXACT:
            if (sql_kind == SPIDER_SQL_KIND_SQL)
            {
              if (str->reserve(store_length + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                SPIDER_SQL_EQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              spider_db_append_column_name(share, str, field->field_index);
              str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
              if (spider_db_append_column_value(spider, share, str, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
            {
              if (str_part2->reserve(store_length + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                SPIDER_SQL_EQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              spider_db_append_column_name(share, str_part2,
                field->field_index);
              str_part2->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
              if (spider_db_append_column_value(spider, share, str_part2,
                field, ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);

              if (str->reserve(SPIDER_SQL_EQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
              if (spider_db_append_column_value(spider, share, str_part, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
            else {
              if (spider_db_append_column_value(spider, share, NULL, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              if (str->reserve(SPIDER_SQL_HS_EQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_HS_EQUAL_STR, SPIDER_SQL_HS_EQUAL_LEN);
            }
#endif
            break;
          case HA_READ_AFTER_KEY:
            if (sql_kind == SPIDER_SQL_KIND_SQL)
            {
              if (str->reserve(store_length + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                SPIDER_SQL_GT_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              spider_db_append_column_name(share, str, field->field_index);
              str->q_append(SPIDER_SQL_GT_STR, SPIDER_SQL_GT_LEN);
              if (spider_db_append_column_value(spider, share, str, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              if (use_both)
                start_key_part_map = 0;
              if (!set_order)
              {
                result_list->key_order = key_count;
                set_order = TRUE;
              }
            } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
            {
              if (str_part2->reserve(store_length + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                SPIDER_SQL_GT_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              spider_db_append_column_name(share, str_part2,
                field->field_index);
              str_part2->q_append(SPIDER_SQL_GT_STR, SPIDER_SQL_GT_LEN);
              if (spider_db_append_column_value(spider, share, str_part2,
                field, ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);

              if (str->reserve(SPIDER_SQL_GT_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_GT_STR, SPIDER_SQL_GT_LEN);
              if (spider_db_append_column_value(spider, share, str_part, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
            else {
              if (spider_db_append_column_value(spider, share, NULL, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              if (str->reserve(SPIDER_SQL_HS_GT_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_HS_GT_STR, SPIDER_SQL_HS_GT_LEN);
            }
#endif
            break;
          case HA_READ_BEFORE_KEY:
            result_list->desc_flg = TRUE;
            if (sql_kind == SPIDER_SQL_KIND_SQL)
            {
              if (str->reserve(store_length + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                SPIDER_SQL_LT_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              spider_db_append_column_name(share, str, field->field_index);
              str->q_append(SPIDER_SQL_LT_STR, SPIDER_SQL_LT_LEN);
              if (spider_db_append_column_value(spider, share, str, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              if (use_both)
                start_key_part_map = 0;
              if (!set_order)
              {
                result_list->key_order = key_count;
                set_order = TRUE;
              }
            } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
            {
              if (str_part2->reserve(store_length + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                SPIDER_SQL_LT_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              spider_db_append_column_name(share, str_part2,
                field->field_index);
              str_part2->q_append(SPIDER_SQL_LT_STR, SPIDER_SQL_LT_LEN);
              if (spider_db_append_column_value(spider, share, str_part2,
                field, ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);

              if (str->reserve(SPIDER_SQL_LT_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_LT_STR, SPIDER_SQL_LT_LEN);
              if (spider_db_append_column_value(spider, share, str_part, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
            else {
              if (spider_db_append_column_value(spider, share, NULL, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              if (str->reserve(SPIDER_SQL_HS_LT_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_HS_LT_STR, SPIDER_SQL_HS_LT_LEN);
            }
#endif
            break;
          case HA_READ_PREFIX_LAST:
            result_list->limit_num = 1;
            /* fall through */
          case HA_READ_KEY_OR_PREV:
          case HA_READ_PREFIX_LAST_OR_PREV:
            result_list->desc_flg = TRUE;
            if (sql_kind == SPIDER_SQL_KIND_SQL)
            {
              if (str->reserve(store_length + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                SPIDER_SQL_LTEQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              spider_db_append_column_name(share, str, field->field_index);
              str->q_append(SPIDER_SQL_LTEQUAL_STR, SPIDER_SQL_LTEQUAL_LEN);
              if (spider_db_append_column_value(spider, share, str, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              if (!set_order)
              {
                result_list->key_order = key_count;
                set_order = TRUE;
              }
            } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
            {
              if (str_part2->reserve(store_length + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                SPIDER_SQL_LTEQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              spider_db_append_column_name(share, str_part2,
                field->field_index);
              str_part2->q_append(SPIDER_SQL_LTEQUAL_STR,
                SPIDER_SQL_LTEQUAL_LEN);
              if (spider_db_append_column_value(spider, share, str_part2,
                field, ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);

              if (str->reserve(SPIDER_SQL_LTEQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_LTEQUAL_STR, SPIDER_SQL_LTEQUAL_LEN);
              if (spider_db_append_column_value(spider, share, str_part, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
            else {
              if (spider_db_append_column_value(spider, share, NULL, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              if (str->reserve(SPIDER_SQL_HS_LTEQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_HS_LTEQUAL_STR,
                SPIDER_SQL_HS_LTEQUAL_LEN);
            }
#endif
            break;
          case HA_READ_MBR_CONTAIN:
            if (str->reserve(SPIDER_SQL_MBR_CONTAIN_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_MBR_CONTAIN_STR,
              SPIDER_SQL_MBR_CONTAIN_LEN);
            if (
              spider_db_append_column_value(spider, share, str, field, ptr) ||
              str->reserve(SPIDER_SQL_COMMA_LEN + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_CLOSE_PAREN_LEN)
            )
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_CLOSE_PAREN_STR,
              SPIDER_SQL_CLOSE_PAREN_LEN);
            break;
          case HA_READ_MBR_INTERSECT:
            if (str->reserve(SPIDER_SQL_MBR_INTERSECT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_MBR_INTERSECT_STR,
              SPIDER_SQL_MBR_INTERSECT_LEN);
            if (
              spider_db_append_column_value(spider, share, str, field, ptr) ||
              str->reserve(SPIDER_SQL_COMMA_LEN + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_CLOSE_PAREN_LEN)
            )
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_CLOSE_PAREN_STR,
              SPIDER_SQL_CLOSE_PAREN_LEN);
            break;
          case HA_READ_MBR_WITHIN:
            if (str->reserve(SPIDER_SQL_MBR_WITHIN_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_MBR_WITHIN_STR,
              SPIDER_SQL_MBR_WITHIN_LEN);
            if (
              spider_db_append_column_value(spider, share, str, field, ptr) ||
              str->reserve(SPIDER_SQL_COMMA_LEN + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_CLOSE_PAREN_LEN)
            )
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_CLOSE_PAREN_STR,
              SPIDER_SQL_CLOSE_PAREN_LEN);
            break;
          case HA_READ_MBR_DISJOINT:
            if (str->reserve(SPIDER_SQL_MBR_DISJOINT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_MBR_DISJOINT_STR,
              SPIDER_SQL_MBR_DISJOINT_LEN);
            if (
              spider_db_append_column_value(spider, share, str, field, ptr) ||
              str->reserve(SPIDER_SQL_COMMA_LEN + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_CLOSE_PAREN_LEN)
            )
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_CLOSE_PAREN_STR,
              SPIDER_SQL_CLOSE_PAREN_LEN);
            break;
          case HA_READ_MBR_EQUAL:
            if (str->reserve(SPIDER_SQL_MBR_EQUAL_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_MBR_EQUAL_STR, SPIDER_SQL_MBR_EQUAL_LEN);
            if (
              spider_db_append_column_value(spider, share, str, field, ptr) ||
              str->reserve(SPIDER_SQL_COMMA_LEN + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_CLOSE_PAREN_LEN)
            )
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_CLOSE_PAREN_STR,
              SPIDER_SQL_CLOSE_PAREN_LEN);
            break;
          default:
            if (sql_kind == SPIDER_SQL_KIND_SQL)
            {
              if (str->reserve(store_length + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                SPIDER_SQL_GTEQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              spider_db_append_column_name(share, str, field->field_index);
              str->q_append(SPIDER_SQL_GTEQUAL_STR, SPIDER_SQL_GTEQUAL_LEN);
              if (spider_db_append_column_value(spider, share, str, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              if (!set_order)
              {
                result_list->key_order = key_count;
                set_order = TRUE;
              }
            } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
            {
              if (str_part2->reserve(store_length + key_name_length +
                (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                SPIDER_SQL_GTEQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              spider_db_append_column_name(share, str_part2,
                field->field_index);
              str_part2->q_append(SPIDER_SQL_GTEQUAL_STR,
                SPIDER_SQL_GTEQUAL_LEN);
              if (spider_db_append_column_value(spider, share, str_part2,
                field, ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);

              if (str->reserve(SPIDER_SQL_GTEQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_GTEQUAL_STR, SPIDER_SQL_GTEQUAL_LEN);
              if (spider_db_append_column_value(spider, share, str_part, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
            else {
              if (spider_db_append_column_value(spider, share, NULL, field,
                ptr))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              if (str->reserve(SPIDER_SQL_HS_GTEQUAL_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_HS_GTEQUAL_STR,
                SPIDER_SQL_HS_GTEQUAL_LEN);
            }
#endif
            break;
        }
      }
      if (sql_kind == SPIDER_SQL_KIND_SQL)
      {
        if (str->reserve(SPIDER_SQL_AND_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_AND_STR,
          SPIDER_SQL_AND_LEN);
      } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
      {
        if (str_part2->reserve(SPIDER_SQL_AND_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str_part2->q_append(SPIDER_SQL_AND_STR,
          SPIDER_SQL_AND_LEN);

        if (str_part->reserve(SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str_part->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      }
    }

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (sql_kind != SPIDER_SQL_KIND_HS)
    {
#endif
      if (
        (key_eq && use_key == end_key) ||
        (!key_eq && end_key_part_map)
      ) {
        ptr = end_key->key + length;
        if ((error_num = spider_db_append_null(spider, share, str, key_part,
          end_key, &ptr, sql_kind)))
        {
          if (error_num > 0)
            DBUG_RETURN(error_num);
          if (
            !set_order &&
            end_key->flag != HA_READ_KEY_EXACT &&
            sql_kind == SPIDER_SQL_KIND_SQL
          ) {
            result_list->key_order = key_count;
            set_order = TRUE;
          }
        } else if (key_eq)
        {
          DBUG_PRINT("info", ("spider key_eq"));
          if (sql_kind == SPIDER_SQL_KIND_SQL)
          {
            if (str->reserve(store_length + key_name_length +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
              SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_AND_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
            if (spider_db_append_column_value(spider, share, str, field, ptr))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          } else {
            if (str_part2->reserve(store_length + key_name_length +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
              SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_AND_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            spider_db_append_column_name(share, str_part2, field->field_index);
            str_part2->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
            if (spider_db_append_column_value(spider, share, str_part2,
              field, ptr))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);

            if (str->reserve(SPIDER_SQL_EQUAL_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
            if (spider_db_append_column_value(spider, share, str_part, field,
              ptr))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          }
        } else {
          DBUG_PRINT("info", ("spider end_key->flag=%d", end_key->flag));
          switch (end_key->flag)
          {
            case HA_READ_BEFORE_KEY:
              if (sql_kind == SPIDER_SQL_KIND_SQL)
              {
                if (str->reserve(store_length + key_name_length +
                  (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                  SPIDER_SQL_LT_LEN))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                spider_db_append_column_name(share, str, field->field_index);
                str->q_append(SPIDER_SQL_LT_STR, SPIDER_SQL_LT_LEN);
                if (spider_db_append_column_value(spider, share, str, field,
                  ptr))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                if (use_both)
                  end_key_part_map = 0;
                if (!set_order)
                {
                  result_list->key_order = key_count;
                  set_order = TRUE;
                }
              } else {
                if (str_part2->reserve(store_length + key_name_length +
                  (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                  SPIDER_SQL_LT_LEN))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                spider_db_append_column_name(share, str_part2,
                  field->field_index);
                str_part2->q_append(SPIDER_SQL_LT_STR, SPIDER_SQL_LT_LEN);
                if (spider_db_append_column_value(spider, share, str_part2,
                  field, ptr))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);

                if (str->reserve(SPIDER_SQL_LT_LEN))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                str->q_append(SPIDER_SQL_LT_STR, SPIDER_SQL_LT_LEN);
                if (spider_db_append_column_value(spider, share, str_part,
                  field, ptr))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              }
              break;
            default:
              if (sql_kind == SPIDER_SQL_KIND_SQL)
              {
                if (str->reserve(store_length + key_name_length +
                  (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                  SPIDER_SQL_LTEQUAL_LEN))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                spider_db_append_column_name(share, str, field->field_index);
                str->q_append(SPIDER_SQL_LTEQUAL_STR, SPIDER_SQL_LTEQUAL_LEN);
                if (spider_db_append_column_value(spider, share, str, field,
                  ptr))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                if (!set_order)
                {
                  result_list->key_order = key_count;
                  set_order = TRUE;
                }
              } else {
                if (str_part2->reserve(store_length + key_name_length +
                  (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
                  SPIDER_SQL_LTEQUAL_LEN))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                spider_db_append_column_name(share, str_part2,
                  field->field_index);
                str_part2->q_append(SPIDER_SQL_LTEQUAL_STR,
                  SPIDER_SQL_LTEQUAL_LEN);
                if (spider_db_append_column_value(spider, share, str_part2,
                  field, ptr))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);

                if (str->reserve(SPIDER_SQL_LTEQUAL_LEN))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                str->q_append(SPIDER_SQL_LTEQUAL_STR, SPIDER_SQL_LTEQUAL_LEN);
                if (spider_db_append_column_value(spider, share, str_part,
                  field, ptr))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              }
              break;
          }
        }
        if (sql_kind == SPIDER_SQL_KIND_SQL)
        {
          if (str->reserve(SPIDER_SQL_AND_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_AND_STR,
            SPIDER_SQL_AND_LEN);
        } else {
          if (str_part2->reserve(SPIDER_SQL_AND_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str_part2->q_append(SPIDER_SQL_AND_STR,
            SPIDER_SQL_AND_LEN);

          if (str_part->reserve(SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str_part->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
      if (use_both && (!start_key_part_map || !end_key_part_map))
        break;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    }
#endif
  }
  if (sql_kind == SPIDER_SQL_KIND_SQL)
  {
    str->length(str->length() - SPIDER_SQL_AND_LEN);
    if (!set_order)
      result_list->key_order = key_count;
  } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
  {
    str_part2->length(str_part2->length() - SPIDER_SQL_AND_LEN);

    str_part->length(str_part->length() - SPIDER_SQL_COMMA_LEN);
    if (!result_list->ha_read_kind)
      str_part->q_append(SPIDER_SQL_CLOSE_PAREN_STR,
        SPIDER_SQL_CLOSE_PAREN_LEN);
    if (str->append(*str_part))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    uint clause_length = str->length() - result_list->ha_next_pos;
    if (clause_length < SPIDER_SQL_NEXT_LEN)
    {
      int roop_count;
      clause_length = SPIDER_SQL_NEXT_LEN - clause_length;
      if (str->reserve(clause_length))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      for (roop_count = 0; roop_count < (int) clause_length; roop_count++)
        str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    }
  }

end:
  /* use condition */
  if (spider_db_append_condition_internal(spider, str, NULL, 0, sql_kind))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if (sql_kind == SPIDER_SQL_KIND_SQL)
    result_list->order_pos = str->length();
#ifndef DBUG_OFF
  dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
  DBUG_RETURN(0);
}

int spider_db_append_key_where(
  const key_range *start_key,
  const key_range *end_key,
  ha_spider *spider
) {
  int error_num;
  DBUG_ENTER("spider_db_append_key_where");
  if ((spider->sql_kinds & SPIDER_SQL_KIND_SQL))
  {
    DBUG_PRINT("info",("spider call internal by SPIDER_SQL_KIND_SQL"));
    if ((error_num = spider_db_append_key_where_internal(start_key, end_key,
      spider, SPIDER_SQL_KIND_SQL, FALSE)))
      DBUG_RETURN(error_num);
  }
  if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
  {
    DBUG_PRINT("info",("spider call internal by SPIDER_SQL_KIND_HANDLER"));
    if ((error_num = spider_db_append_key_where_internal(start_key, end_key,
      spider, SPIDER_SQL_KIND_HANDLER, FALSE)))
      DBUG_RETURN(error_num);
  }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if ((spider->sql_kinds & SPIDER_SQL_KIND_HS))
  {
    DBUG_PRINT("info",("spider call internal by SPIDER_SQL_KIND_HS"));
    if ((error_num = spider_db_append_key_where_internal(start_key, end_key,
      spider, SPIDER_SQL_KIND_HS, FALSE)))
      DBUG_RETURN(error_num);
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_append_match_against(
  spider_string *str,
  ha_spider *spider,
  st_spider_ft_info  *ft_info,
  const char *alias,
  uint alias_length
) {
  SPIDER_SHARE *share = spider->share;
  TABLE *table = spider->get_table();
  String *ft_init_key;
  KEY *key_info;
  uint key_name_length;
  int key_count;
  KEY_PART_INFO *key_part;
  Field *field;
  DBUG_ENTER("spider_db_append_match_against");

  if (str->reserve(SPIDER_SQL_MATCH_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_MATCH_STR, SPIDER_SQL_MATCH_LEN);

  ft_init_key = ft_info->key;
  key_info = &table->key_info[ft_info->inx];
  DBUG_PRINT("info", ("spider key_info->key_parts=%u",
    key_info->key_parts));

  for (
    key_part = key_info->key_part,
    key_count = 0;
    key_count < (int) key_info->key_parts;
    key_part++,
    key_count++
  ) {
    field = key_part->field;
    key_name_length = share->column_name_str[field->field_index].length();
    if (alias_length)
    {
      if (str->reserve(alias_length + key_name_length +
        (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(alias, alias_length);
    } else {
      if (str->reserve(key_name_length +
        (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    spider_db_append_column_name(share, str, field->field_index);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  if (str->reserve(SPIDER_SQL_AGAINST_LEN + SPIDER_SQL_VALUE_QUOTE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_AGAINST_STR, SPIDER_SQL_AGAINST_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);

  char buf[MAX_FIELD_WIDTH];
  spider_string tmp_str(buf, MAX_FIELD_WIDTH, share->access_charset);
  tmp_str.init_calc_mem(116);
  tmp_str.length(0);
  if (
    tmp_str.append(ft_init_key->ptr(), ft_init_key->length(),
      ft_init_key->charset()) ||
    str->reserve(tmp_str.length() * 2) ||
    append_escaped(str->get_str(), tmp_str.get_str())
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->mem_calc();

  if (str->reserve(
    SPIDER_SQL_VALUE_QUOTE_LEN + SPIDER_SQL_CLOSE_PAREN_LEN +
    ((ft_info->flags & FT_BOOL) ? SPIDER_SQL_IN_BOOLEAN_MODE_LEN : 0) +
    ((ft_info->flags & FT_EXPAND) ?
      SPIDER_SQL_WITH_QUERY_EXPANSION_LEN : 0)
  ))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  if (ft_info->flags & FT_BOOL)
    str->q_append(SPIDER_SQL_IN_BOOLEAN_MODE_STR,
      SPIDER_SQL_IN_BOOLEAN_MODE_LEN);
  if (ft_info->flags & FT_EXPAND)
    str->q_append(SPIDER_SQL_WITH_QUERY_EXPANSION_STR,
      SPIDER_SQL_WITH_QUERY_EXPANSION_LEN);
  str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_match_select(
  spider_string *str,
  ha_spider *spider,
  const char *alias,
  uint alias_length
) {
  int error_num;
  DBUG_ENTER("spider_db_append_match_select");
  if (spider->ft_current)
  {
    st_spider_ft_info *ft_info = spider->ft_first;
    while (TRUE)
    {
      if ((error_num = spider_db_append_match_against(str, spider, ft_info,
        alias, alias_length)))
        DBUG_RETURN(error_num);
      if (str->reserve(SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      if (ft_info == spider->ft_current)
        break;
      ft_info = ft_info->next;
    }
  }
  DBUG_RETURN(0);
}

int spider_db_append_match_fetch(
  ha_spider *spider,
  st_spider_ft_info *ft_first,
  st_spider_ft_info *ft_current,
  SPIDER_DB_ROW *row,
  ulong **lengths
) {
  DBUG_ENTER("spider_db_append_match_fetch");
  if (ft_current)
  {
    st_spider_ft_info *ft_info = ft_first;
    while (TRUE)
    {
      DBUG_PRINT("info",("spider ft_info=%p", ft_info));
      if (**row)
        ft_info->score = (float) atof(**row);
      else
        DBUG_RETURN(ER_SPIDER_UNKNOWN_NUM);
      (*row)++;
      (*lengths)++;
      if (ft_info == ft_current)
        break;
      ft_info = ft_info->next;
    }
  }
  DBUG_RETURN(0);
}

int spider_db_append_match_where(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sql;
  bool first = TRUE;
  st_spider_ft_info *ft_info = spider->ft_first;
  DBUG_ENTER("spider_db_append_match_where");

  if (spider->ft_current)
  {
    while (TRUE)
    {
      if (ft_info->used_in_where)
      {
        if (first)
        {
          if (str->reserve(SPIDER_SQL_WHERE_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
          first = FALSE;
        }
        if ((error_num = spider_db_append_match_against(str, spider, ft_info,
          NULL, 0)))
          DBUG_RETURN(error_num);
        if (str->reserve(SPIDER_SQL_AND_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
      }

      if (ft_info == spider->ft_current)
        break;
      ft_info = ft_info->next;
    }
    if (!first)
      str->length(str->length() - SPIDER_SQL_AND_LEN);
  }

  /* use condition */
  if (spider_db_append_condition(spider, str, NULL, 0))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  result_list->order_pos = str->length();
  DBUG_RETURN(0);
}

int spider_db_append_key_order_str(
  spider_string *str,
  KEY *key_info,
  int start_pos,
  bool desc_flg
) {
  int length, error_num;
  KEY_PART_INFO *key_part;
  Field *field;
  DBUG_ENTER("spider_db_append_key_order_str");

  if ((int) key_info->key_parts > start_pos)
  {
    if (str->reserve(SPIDER_SQL_ORDER_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
    if (desc_flg == TRUE)
    {
      for (
        key_part = key_info->key_part + start_pos,
        length = 0;
        length + start_pos < (int) key_info->key_parts;
        key_part++,
        length++
      ) {
        field = key_part->field;
        if ((error_num = spider_db_append_name_with_quote_str(str,
          (char *) field->field_name)))
          DBUG_RETURN(error_num);
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (str->reserve(SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        } else {
          if (str->reserve(SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
    } else {
      for (
        key_part = key_info->key_part + start_pos,
        length = 0;
        length + start_pos < (int) key_info->key_parts;
        key_part++,
        length++
      ) {
        field = key_part->field;
        if ((error_num = spider_db_append_name_with_quote_str(str,
          (char *) field->field_name)))
          DBUG_RETURN(error_num);
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (str->reserve(SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        } else {
          if (str->reserve(SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_key_order_with_alias(
  ha_spider *spider,
  bool update_sql,
  const char *alias,
  uint alias_length
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  spider_string *str;
  KEY *key_info = result_list->key_info;
  int length;
  KEY_PART_INFO *key_part;
  Field *field;
  uint key_name_length;
  DBUG_ENTER("spider_db_append_key_order_with_alias");
  if (update_sql)
    str = &result_list->update_sql;
  else
    str = &result_list->sql;

  result_list->ha_limit_pos = result_list->ha_sql.length();
  if (
    (
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
      (
        spider->do_direct_update &&
        (update_sql || !(spider->direct_update_kinds & SPIDER_SQL_KIND_SQL))
      ) ||
      (
        !spider->do_direct_update &&
#endif
#endif
        !(spider->sql_kinds & SPIDER_SQL_KIND_SQL)
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
      )
#endif
#endif
    ) &&
    !result_list->direct_order_limit
  )
    DBUG_RETURN(0);

  if (result_list->direct_order_limit)
  {
    int error_num;
    ORDER *order;
    st_select_lex *select_lex;
    longlong select_limit;
    longlong offset_limit;
    spider_get_select_limit(spider, &select_lex, &select_limit,
      &offset_limit);
    if (str->reserve(SPIDER_SQL_ORDER_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
    for (order = (ORDER *) select_lex->order_list.first; order;
      order = order->next)
    {
      if ((error_num =
        spider_db_print_item_type((*order->item), spider, str, alias,
          alias_length)))
      {
        DBUG_PRINT("info",("spider error=%d", error_num));
        DBUG_RETURN(error_num);
      }
      if (order->asc)
      {
        if (str->reserve(SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      } else {
        if (str->reserve(SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
        str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      }
    }
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  } else {
    if (result_list->sorted == TRUE)
    {
      if (result_list->desc_flg == TRUE)
      {
        for (
          key_part = key_info->key_part + result_list->key_order,
          length = 1;
          length + result_list->key_order < (int) key_info->key_parts &&
          length < result_list->max_order;
          key_part++,
          length++
        ) {
          field = key_part->field;
          key_name_length =
            share->column_name_str[field->field_index].length();
          if (length == 1)
          {
            if (str->reserve(SPIDER_SQL_ORDER_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
          }
          if (key_part->key_part_flag & HA_REVERSE_SORT)
          {
            if (str->reserve(alias_length + key_name_length +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(alias, alias_length);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
          } else {
            if (str->reserve(alias_length + key_name_length +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
              SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(alias, alias_length);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
          }
        }
        if (
          length + result_list->key_order <= (int) key_info->key_parts &&
          length <= result_list->max_order
        ) {
          field = key_part->field;
          key_name_length =
            share->column_name_str[field->field_index].length();
          if (length == 1)
          {
            if (str->reserve(SPIDER_SQL_ORDER_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
          }
          if (key_part->key_part_flag & HA_REVERSE_SORT)
          {
            if (str->reserve(alias_length + key_name_length +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(alias, alias_length);
            spider_db_append_column_name(share, str, field->field_index);
          } else {
            if (str->reserve(alias_length + key_name_length +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_DESC_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(alias, alias_length);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          }
        }
      } else {
        for (
          key_part = key_info->key_part + result_list->key_order,
          length = 1;
          length + result_list->key_order < (int) key_info->key_parts &&
          length < result_list->max_order;
          key_part++,
          length++
        ) {
          field = key_part->field;
          key_name_length =
            share->column_name_str[field->field_index].length();
          if (length == 1)
          {
            if (str->reserve(SPIDER_SQL_ORDER_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
          }
          if (key_part->key_part_flag & HA_REVERSE_SORT)
          {
            if (str->reserve(alias_length + key_name_length +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
              SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(alias, alias_length);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
          } else {
            if (str->reserve(alias_length + key_name_length +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(alias, alias_length);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
          }
        }
        if (
          length + result_list->key_order <= (int) key_info->key_parts &&
          length <= result_list->max_order
        ) {
          field = key_part->field;
          key_name_length =
            share->column_name_str[field->field_index].length();
          if (length == 1)
          {
            if (str->reserve(SPIDER_SQL_ORDER_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
          }
          if (key_part->key_part_flag & HA_REVERSE_SORT)
          {
            if (str->reserve(alias_length + key_name_length +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_DESC_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(alias, alias_length);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          } else {
            if (str->reserve(alias_length + key_name_length +
              (SPIDER_SQL_NAME_QUOTE_LEN) * 2))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(alias, alias_length);
            spider_db_append_column_name(share, str, field->field_index);
          }
        }
      }
    }
  }
  result_list->limit_pos = str->length();
  DBUG_RETURN(0);
}

int spider_db_append_key_order(
  ha_spider *spider,
  bool update_sql
) {
  DBUG_ENTER("spider_db_append_key_order");
  DBUG_RETURN(spider_db_append_key_order_with_alias(spider, update_sql,
    NULL, 0));
}

int spider_db_append_limit_internal(
  ha_spider *spider,
  spider_string *str,
  longlong offset,
  longlong limit,
  uint sql_kind
) {
  char buf[SPIDER_LONGLONG_LEN + 1];
  uint32 length;
  DBUG_ENTER("spider_db_append_limit_internal");
  DBUG_PRINT("info", ("spider offset=%lld", offset));
  DBUG_PRINT("info", ("spider limit=%lld", limit));
  DBUG_PRINT("info", ("spider sql_kind=%u", sql_kind));
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (sql_kind != SPIDER_SQL_KIND_HS)
  {
#endif
    if (offset || limit < 9223372036854775807LL)
    {
      if (str->reserve(SPIDER_SQL_LIMIT_LEN + SPIDER_SQL_COMMA_LEN +
        ((SPIDER_LONGLONG_LEN) * 2)))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_LIMIT_STR, SPIDER_SQL_LIMIT_LEN);
      if (offset)
      {
        length = (uint32) (my_charset_bin.cset->longlong10_to_str)(
          &my_charset_bin, buf, SPIDER_LONGLONG_LEN + 1, -10, offset);
        str->q_append(buf, length);
        str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      }
      length = (uint32) (my_charset_bin.cset->longlong10_to_str)(
        &my_charset_bin, buf, SPIDER_LONGLONG_LEN + 1, -10, limit);
      str->q_append(buf, length);
    }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
    spider->result_list.hs_skip = (int) offset;
    spider->result_list.hs_limit = (int) limit;
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_append_limit(
  ha_spider *spider,
  spider_string *str,
  longlong offset,
  longlong limit
) {
  int error_num;
  DBUG_ENTER("spider_db_append_limit");
  if (spider)
  {
    if (
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
      (
        !spider->do_direct_update ||
        !spider->direct_update_kinds
      ) &&
#endif
      spider->result_list.direct_order_limit
    ) {
      DBUG_PRINT("info", ("spider direct_order_limit"));
      st_select_lex *select_lex;
      longlong select_limit;
      longlong offset_limit;
      spider_get_select_limit(spider, &select_lex, &select_limit,
        &offset_limit);
      if ((error_num = spider_db_append_limit_internal(spider, str, 0,
        select_limit + offset_limit, SPIDER_SQL_KIND_SQL)))
        DBUG_RETURN(error_num);
      spider->result_list.internal_limit = 0;
      DBUG_RETURN(0);
    }

    if (
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
      (
        spider->do_direct_update &&
        (spider->direct_update_kinds & SPIDER_SQL_KIND_SQL)
      ) ||
      (
        !spider->do_direct_update &&
#endif
#endif
        (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
      )
#endif
#endif
    ) {
      DBUG_PRINT("info", ("spider SPIDER_SQL_KIND_SQL"));
      if ((error_num = spider_db_append_limit_internal(spider, str, offset,
        limit, SPIDER_SQL_KIND_SQL)))
        DBUG_RETURN(error_num);
    }
    if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
    {
      DBUG_PRINT("info", ("spider SPIDER_SQL_KIND_HANDLER"));
      if ((error_num = spider_db_append_limit_internal(spider,
        &spider->result_list.ha_sql, offset,
        limit, SPIDER_SQL_KIND_HANDLER)))
        DBUG_RETURN(error_num);
    }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
      (
        spider->do_direct_update &&
        (spider->direct_update_kinds & SPIDER_SQL_KIND_HS)
      ) ||
      (
        !spider->do_direct_update &&
#endif
        (spider->sql_kinds & SPIDER_SQL_KIND_HS)
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
      )
#endif
    ) {
      DBUG_PRINT("info", ("spider SPIDER_SQL_KIND_HS"));
      if ((error_num = spider_db_append_limit_internal(spider, NULL, offset,
        limit, SPIDER_SQL_KIND_HS)))
        DBUG_RETURN(error_num);
    }
#endif
  } else {
    DBUG_PRINT("info", ("spider default"));
    if ((error_num = spider_db_append_limit_internal(NULL, str, offset,
      limit, SPIDER_SQL_KIND_SQL)))
      DBUG_RETURN(error_num);
  }
  DBUG_RETURN(0);
}

int spider_db_append_select_lock_str(
  spider_string *str,
  int lock_mode
) {
  DBUG_ENTER("spider_db_append_select_lock_str");
  if (lock_mode == SPIDER_LOCK_MODE_EXCLUSIVE)
  {
    if (str->reserve(SPIDER_SQL_FOR_UPDATE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FOR_UPDATE_STR, SPIDER_SQL_FOR_UPDATE_LEN);
  } else if (lock_mode == SPIDER_LOCK_MODE_SHARED)
  {
    if (str->reserve(SPIDER_SQL_SHARED_LOCK_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SHARED_LOCK_STR, SPIDER_SQL_SHARED_LOCK_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_select_lock(
  ha_spider *spider
) {
  int lock_mode = spider_conn_lock_mode(spider);
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sql;
  DBUG_ENTER("spider_db_append_lock");
  if (!(spider->sql_kinds & SPIDER_SQL_KIND_SQL))
    DBUG_RETURN(0);

  if (lock_mode == SPIDER_LOCK_MODE_EXCLUSIVE)
  {
    if (str->reserve(SPIDER_SQL_FOR_UPDATE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FOR_UPDATE_STR, SPIDER_SQL_FOR_UPDATE_LEN);
  } else if (lock_mode == SPIDER_LOCK_MODE_SHARED)
  {
    if (str->reserve(SPIDER_SQL_SHARED_LOCK_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SHARED_LOCK_STR, SPIDER_SQL_SHARED_LOCK_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_show_table_status(
  SPIDER_SHARE *share
) {
  int roop_count;
  spider_string *str;
  DBUG_ENTER("spider_db_append_show_table_status");
  if (!(share->show_table_status =
    new spider_string[2 * share->all_link_count]))
    goto error;

  for (roop_count = 0; roop_count < (int) share->all_link_count; roop_count++)
  {
    share->show_table_status[0 + (2 * roop_count)].init_calc_mem(90);
    share->show_table_status[1 + (2 * roop_count)].init_calc_mem(91);
    if (
      share->show_table_status[0 + (2 * roop_count)].reserve(
        SPIDER_SQL_SHOW_TABLE_STATUS_LEN +
        share->db_names_str[roop_count].length() +
        SPIDER_SQL_LIKE_LEN + share->table_names_str[roop_count].length() +
        ((SPIDER_SQL_NAME_QUOTE_LEN) * 2) +
        ((SPIDER_SQL_VALUE_QUOTE_LEN) * 2)) ||
      share->show_table_status[1 + (2 * roop_count)].reserve(
        SPIDER_SQL_SELECT_TABLES_STATUS_LEN +
        share->db_names_str[roop_count].length() +
        SPIDER_SQL_AND_LEN + SPIDER_SQL_TABLE_NAME_LEN + SPIDER_SQL_EQUAL_LEN +
        share->table_names_str[roop_count].length() +
        ((SPIDER_SQL_VALUE_QUOTE_LEN) * 4))
    )
      goto error;
    str = &share->show_table_status[0 + (2 * roop_count)];
    str->q_append(
      SPIDER_SQL_SHOW_TABLE_STATUS_STR, SPIDER_SQL_SHOW_TABLE_STATUS_LEN);
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    str->q_append(share->db_names_str[roop_count].ptr(),
      share->db_names_str[roop_count].length());
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    str->q_append(SPIDER_SQL_LIKE_STR, SPIDER_SQL_LIKE_LEN);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    str->q_append(share->table_names_str[roop_count].ptr(),
      share->table_names_str[roop_count].length());
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    str = &share->show_table_status[1 + (2 * roop_count)];
    str->q_append(
      SPIDER_SQL_SELECT_TABLES_STATUS_STR,
      SPIDER_SQL_SELECT_TABLES_STATUS_LEN);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    str->q_append(share->db_names_str[roop_count].ptr(),
      share->db_names_str[roop_count].length());
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
    str->q_append(SPIDER_SQL_TABLE_NAME_STR, SPIDER_SQL_TABLE_NAME_LEN);
    str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    str->q_append(share->table_names_str[roop_count].ptr(),
      share->table_names_str[roop_count].length());
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  }
  DBUG_RETURN(0);

error:
  if (share->show_table_status)
  {
    delete [] share->show_table_status;
    share->show_table_status = NULL;
  }
  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
}

void spider_db_free_show_table_status(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_free_show_table_status");
  if (share->show_table_status)
  {
    delete [] share->show_table_status;
    share->show_table_status = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_db_append_show_records(
  SPIDER_SHARE *share
) {
  int roop_count;
  spider_string *str;
  DBUG_ENTER("spider_db_append_show_records");
  if (!(share->show_records = new spider_string[share->all_link_count]))
    goto error;

  for (roop_count = 0; roop_count < (int) share->all_link_count; roop_count++)
  {
    share->show_records[roop_count].init_calc_mem(92);
    if (
      share->show_records[roop_count].reserve(
        SPIDER_SQL_SHOW_RECORDS_LEN +
        share->db_names_str[roop_count].length() +
        SPIDER_SQL_DOT_LEN +
        share->table_names_str[roop_count].length() +
        ((SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    )
      goto error;
    str = &share->show_records[roop_count];
    str->q_append(SPIDER_SQL_SHOW_RECORDS_STR, SPIDER_SQL_SHOW_RECORDS_LEN);
    spider_db_append_table_name(str, share, roop_count);
  }
  DBUG_RETURN(0);

error:
  if (share->show_records)
  {
    delete [] share->show_records;
    share->show_records = NULL;
  }
  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
}

void spider_db_free_show_records(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_free_show_records");
  if (share->show_records)
  {
    delete [] share->show_records;
    share->show_records = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_db_append_show_index(
  SPIDER_SHARE *share
) {
  int roop_count;
  spider_string *str;
  DBUG_ENTER("spider_db_append_show_index");
  if (!(share->show_index = new spider_string[2 * share->all_link_count]))
    goto error;

  for (roop_count = 0; roop_count < (int) share->all_link_count; roop_count++)
  {
    share->show_index[0 + (2 * roop_count)].init_calc_mem(93);
    share->show_index[1 + (2 * roop_count)].init_calc_mem(94);
    if (
      share->show_index[0 + (2 * roop_count)].reserve(
        SPIDER_SQL_SHOW_INDEX_LEN + share->db_names_str[roop_count].length() +
        SPIDER_SQL_DOT_LEN +
        share->table_names_str[roop_count].length() +
        ((SPIDER_SQL_NAME_QUOTE_LEN) * 4)) ||
      share->show_index[1 + (2 * roop_count)].reserve(
        SPIDER_SQL_SELECT_STATISTICS_LEN +
        share->db_names_str[roop_count].length() +
        SPIDER_SQL_AND_LEN + SPIDER_SQL_TABLE_NAME_LEN + SPIDER_SQL_EQUAL_LEN +
        share->table_names_str[roop_count].length() +
        ((SPIDER_SQL_VALUE_QUOTE_LEN) * 4) +
        SPIDER_SQL_GROUP_LEN + SPIDER_SQL_COLUMN_NAME_LEN)
    )
      goto error;
    str = &share->show_index[0 + (2 * roop_count)];
    str->q_append(
      SPIDER_SQL_SHOW_INDEX_STR, SPIDER_SQL_SHOW_INDEX_LEN);
    spider_db_append_table_name(str, share, roop_count);
    str = &share->show_index[1 + (2 * roop_count)];
    str->q_append(
      SPIDER_SQL_SELECT_STATISTICS_STR, SPIDER_SQL_SELECT_STATISTICS_LEN);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    str->q_append(share->db_names_str[roop_count].ptr(),
      share->db_names_str[roop_count].length());
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
    str->q_append(SPIDER_SQL_TABLE_NAME_STR, SPIDER_SQL_TABLE_NAME_LEN);
    str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    str->q_append(share->table_names_str[roop_count].ptr(),
      share->table_names_str[roop_count].length());
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    str->q_append(SPIDER_SQL_GROUP_STR, SPIDER_SQL_GROUP_LEN);
    str->q_append(SPIDER_SQL_COLUMN_NAME_STR, SPIDER_SQL_COLUMN_NAME_LEN);
  }
  DBUG_RETURN(0);

error:
  if (share->show_index)
  {
    delete [] share->show_index;
    share->show_index = NULL;
  }
  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
}

void spider_db_free_show_index(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_free_show_index");
  if (share->show_index)
  {
    delete [] share->show_index;
    share->show_index = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_db_append_set_names(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_append_set_names");
  DBUG_RETURN(0);
}

void spider_db_free_set_names(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_free_set_names");
  DBUG_VOID_RETURN;
}

int spider_db_append_disable_keys(
  ha_spider *spider,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_disable_keys");

  if (str->reserve(SPIDER_SQL_SQL_ALTER_TABLE_LEN +
    share->db_names_str[spider->conn_link_idx[link_idx]].length() +
    SPIDER_SQL_DOT_LEN +
    share->table_names_str[spider->conn_link_idx[link_idx]].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_SQL_DISABLE_KEYS_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ALTER_TABLE_STR,
    SPIDER_SQL_SQL_ALTER_TABLE_LEN);
  spider_db_append_table_name(str, share, spider->conn_link_idx[link_idx]);
  str->q_append(SPIDER_SQL_SQL_DISABLE_KEYS_STR,
    SPIDER_SQL_SQL_DISABLE_KEYS_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_enable_keys(
  ha_spider *spider,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_enable_keys");

  if (str->reserve(SPIDER_SQL_SQL_ALTER_TABLE_LEN +
    share->db_names_str[spider->conn_link_idx[link_idx]].length() +
    SPIDER_SQL_DOT_LEN +
    share->table_names_str[spider->conn_link_idx[link_idx]].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_SQL_ENABLE_KEYS_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ALTER_TABLE_STR,
    SPIDER_SQL_SQL_ALTER_TABLE_LEN);
  spider_db_append_table_name(str, share, spider->conn_link_idx[link_idx]);
  str->q_append(SPIDER_SQL_SQL_ENABLE_KEYS_STR,
    SPIDER_SQL_SQL_ENABLE_KEYS_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_check_table(
  ha_spider *spider,
  int link_idx,
  HA_CHECK_OPT* check_opt
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_check_table");

  if (str->reserve(SPIDER_SQL_SQL_CHECK_TABLE_LEN +
    share->db_names_str[spider->conn_link_idx[link_idx]].length() +
    SPIDER_SQL_DOT_LEN +
    share->table_names_str[spider->conn_link_idx[link_idx]].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_CHECK_TABLE_STR,
    SPIDER_SQL_SQL_CHECK_TABLE_LEN);
  spider_db_append_table_name(str, share, spider->conn_link_idx[link_idx]);
  if (check_opt->flags & T_QUICK)
  {
    if (str->reserve(SPIDER_SQL_SQL_QUICK_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_QUICK_STR, SPIDER_SQL_SQL_QUICK_LEN);
  }
  if (check_opt->flags & T_FAST)
  {
    if (str->reserve(SPIDER_SQL_SQL_FAST_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_FAST_STR, SPIDER_SQL_SQL_FAST_LEN);
  }
  if (check_opt->flags & T_MEDIUM)
  {
    if (str->reserve(SPIDER_SQL_SQL_MEDIUM_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_MEDIUM_STR, SPIDER_SQL_SQL_MEDIUM_LEN);
  }
  if (check_opt->flags & T_EXTEND)
  {
    if (str->reserve(SPIDER_SQL_SQL_EXTENDED_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_EXTENDED_STR, SPIDER_SQL_SQL_EXTENDED_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_repair_table(
  ha_spider *spider,
  int link_idx,
  HA_CHECK_OPT* check_opt
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  int local_length = spider_param_internal_optimize_local(spider->trx->thd,
    share->internal_optimize_local) * SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_db_append_repair_table");

  if (str->reserve(SPIDER_SQL_SQL_REPAIR_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length +
    share->db_names_str[spider->conn_link_idx[link_idx]].length() +
    SPIDER_SQL_DOT_LEN +
    share->table_names_str[spider->conn_link_idx[link_idx]].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_REPAIR_STR, SPIDER_SQL_SQL_REPAIR_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  spider_db_append_table_name(str, share, spider->conn_link_idx[link_idx]);
  if (check_opt->flags & T_QUICK)
  {
    if (str->reserve(SPIDER_SQL_SQL_QUICK_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_QUICK_STR, SPIDER_SQL_SQL_QUICK_LEN);
  }
  if (check_opt->flags & T_EXTEND)
  {
    if (str->reserve(SPIDER_SQL_SQL_EXTENDED_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_EXTENDED_STR, SPIDER_SQL_SQL_EXTENDED_LEN);
  }
  if (check_opt->sql_flags & TT_USEFRM)
  {
    if (str->reserve(SPIDER_SQL_SQL_USE_FRM_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_USE_FRM_STR, SPIDER_SQL_SQL_USE_FRM_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_analyze_table(
  ha_spider *spider,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  int local_length = spider_param_internal_optimize_local(spider->trx->thd,
    share->internal_optimize_local) * SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_db_append_analyze_table");

  if (str->reserve(SPIDER_SQL_SQL_ANALYZE_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length +
    share->db_names_str[spider->conn_link_idx[link_idx]].length() +
    SPIDER_SQL_DOT_LEN +
    share->table_names_str[spider->conn_link_idx[link_idx]].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ANALYZE_STR, SPIDER_SQL_SQL_ANALYZE_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  spider_db_append_table_name(str, share, spider->conn_link_idx[link_idx]);
  DBUG_RETURN(0);
}

int spider_db_append_optimize_table(
  ha_spider *spider,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  int local_length = spider_param_internal_optimize_local(spider->trx->thd,
    share->internal_optimize_local) * SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_db_append_optimize_table");

  if (str->reserve(SPIDER_SQL_SQL_OPTIMIZE_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length +
    share->db_names_str[spider->conn_link_idx[link_idx]].length() +
    SPIDER_SQL_DOT_LEN +
    share->table_names_str[spider->conn_link_idx[link_idx]].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_OPTIMIZE_STR, SPIDER_SQL_SQL_OPTIMIZE_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  spider_db_append_table_name(str, share, spider->conn_link_idx[link_idx]);
  DBUG_RETURN(0);
}

int spider_db_append_flush_tables(
  ha_spider *spider,
  bool lock
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sql;
  DBUG_ENTER("spider_db_append_flush_tables");
  if (lock)
  {
    if (str->reserve(SPIDER_SQL_FLUSH_TABLES_LEN +
      SPIDER_SQL_WITH_READ_LOCK_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FLUSH_TABLES_STR, SPIDER_SQL_FLUSH_TABLES_LEN);
    str->q_append(SPIDER_SQL_WITH_READ_LOCK_STR,
      SPIDER_SQL_WITH_READ_LOCK_LEN);
  } else {
    if (str->reserve(SPIDER_SQL_FLUSH_TABLES_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FLUSH_TABLES_STR, SPIDER_SQL_FLUSH_TABLES_LEN);
  }
  DBUG_RETURN(0);
}

void spider_db_create_tmp_bka_table_name(
  ha_spider *spider,
  char *tmp_table_name,
  int *tmp_table_name_length,
  int link_idx
) {
  SPIDER_SHARE *share = spider->share;
  uint adjust_length =
    share->db_nm_max_length - share->db_names_str[spider->conn_link_idx[
      link_idx]].length() +
    share->table_nm_max_length - share->table_names_str[spider->conn_link_idx[
      link_idx]].length(),
    length;
  DBUG_ENTER("spider_db_create_tmp_bka_table_name");
  *tmp_table_name_length = share->db_nm_max_length +
    share->table_nm_max_length;
  memset(tmp_table_name, ' ', adjust_length);
  tmp_table_name += adjust_length;
  memcpy(tmp_table_name, share->db_names_str[link_idx].c_ptr(),
    share->db_names_str[link_idx].length());
  tmp_table_name += share->db_names_str[link_idx].length();
  length = my_sprintf(tmp_table_name, (tmp_table_name,
    "%s%s%p%s", SPIDER_SQL_DOT_STR, SPIDER_SQL_TMP_BKA_STR, spider,
    SPIDER_SQL_UNDERSCORE_STR));
  *tmp_table_name_length += length;
  tmp_table_name += length;
  memcpy(tmp_table_name,
    share->table_names_str[spider->conn_link_idx[link_idx]].c_ptr(),
    share->table_names_str[spider->conn_link_idx[link_idx]].length());
  DBUG_VOID_RETURN;
}

int spider_db_append_create_tmp_bka_table(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos,
  CHARSET_INFO *table_charset
) {
  int error_num;
  SPIDER_SHARE *share = spider->share;
  THD *thd = spider->trx->thd;
  char *bka_engine = spider_param_bka_engine(thd, share->bka_engine);
  uint bka_engine_length = strlen(bka_engine),
    cset_length = strlen(table_charset->csname);
  DBUG_ENTER("spider_db_append_create_tmp_bka_table");
  if (str->reserve(SPIDER_SQL_CREATE_TMP_LEN + tmp_table_name_length +
    SPIDER_SQL_OPEN_PAREN_LEN + SPIDER_SQL_ID_LEN + SPIDER_SQL_ID_TYPE_LEN +
    SPIDER_SQL_COMMA_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_CREATE_TMP_STR, SPIDER_SQL_CREATE_TMP_LEN);
  *db_name_pos = str->length();
  str->q_append(tmp_table_name, tmp_table_name_length);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  str->q_append(SPIDER_SQL_ID_STR, SPIDER_SQL_ID_LEN);
  str->q_append(SPIDER_SQL_ID_TYPE_STR, SPIDER_SQL_ID_TYPE_LEN);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  if ((error_num = spider_db_append_key_column_types(start_key, spider, str)))
    DBUG_RETURN(error_num);
  if (str->reserve(SPIDER_SQL_ENGINE_LEN + bka_engine_length +
    SPIDER_SQL_DEF_CHARSET_LEN + cset_length + SPIDER_SQL_SEMICOLON_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_ENGINE_STR, SPIDER_SQL_ENGINE_LEN);
  str->q_append(bka_engine, bka_engine_length);
  str->q_append(SPIDER_SQL_DEF_CHARSET_STR, SPIDER_SQL_DEF_CHARSET_LEN);
  str->q_append(table_charset->csname, cset_length);
  str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_drop_tmp_bka_table(
  ha_spider *spider,
  spider_string *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos,
  int *drop_table_end_pos,
  bool with_semicolon
) {
  DBUG_ENTER("spider_db_append_drop_tmp_bka_table");
  if (str->reserve(SPIDER_SQL_DROP_TMP_LEN + tmp_table_name_length +
    (with_semicolon ? SPIDER_SQL_SEMICOLON_LEN : 0)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_DROP_TMP_STR, SPIDER_SQL_DROP_TMP_LEN);
  *db_name_pos = str->length();
  str->q_append(tmp_table_name, tmp_table_name_length);
  *drop_table_end_pos = str->length();
  if (with_semicolon)
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_insert_tmp_bka_table(
  const key_range *start_key,
  ha_spider *spider,
  spider_string *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos
) {
  int error_num;
  DBUG_ENTER("spider_db_append_insert_tmp_bka_table");
  if (str->reserve(SPIDER_SQL_INSERT_LEN + SPIDER_SQL_INTO_LEN +
    tmp_table_name_length + SPIDER_SQL_OPEN_PAREN_LEN + SPIDER_SQL_ID_LEN +
    SPIDER_SQL_COMMA_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_INSERT_STR, SPIDER_SQL_INSERT_LEN);
  str->q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  *db_name_pos = str->length();
  str->q_append(tmp_table_name, tmp_table_name_length);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  str->q_append(SPIDER_SQL_ID_STR, SPIDER_SQL_ID_LEN);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  if ((error_num = spider_db_append_key_columns(start_key, spider, str)))
    DBUG_RETURN(error_num);
  if (str->reserve(SPIDER_SQL_VALUES_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_VALUES_STR, SPIDER_SQL_VALUES_LEN);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

void spider_db_append_handler_next(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *sql = &result_list->ha_sql;
  DBUG_ENTER("spider_db_append_handler_next");
  DBUG_PRINT("info",("spider ha_next_pos=%d", result_list->ha_next_pos));
  DBUG_PRINT("info",("spider ha_where_pos=%d", result_list->ha_where_pos));
  sql->length(result_list->ha_next_pos);
  if (result_list->sorted && result_list->desc_flg)
  {
    sql->q_append(SPIDER_SQL_PREV_STR, SPIDER_SQL_PREV_LEN);
    memset((char *) sql->ptr() + sql->length(), ' ',
      result_list->ha_where_pos - result_list->ha_next_pos -
      SPIDER_SQL_PREV_LEN);
  } else {
    sql->q_append(SPIDER_SQL_NEXT_STR, SPIDER_SQL_NEXT_LEN);
    memset((char *) sql->ptr() + sql->length(), ' ',
      result_list->ha_where_pos - result_list->ha_next_pos -
      SPIDER_SQL_NEXT_LEN);
  }
  DBUG_VOID_RETURN;
}

void spider_db_get_row_and_lengths_from_tmp_tbl_rec(
  SPIDER_RESULT *current,
  SPIDER_DB_ROW *row,
  ulong **lengths
) {
  uint roop_count;
  spider_string tmp_str1, tmp_str2;
  const char *row_ptr;
  SPIDER_DB_ROW tmp_row;
  ulong *tmp_lengths;
  DBUG_ENTER("spider_db_get_row_and_lengths_from_tmp_tbl_rec");
  tmp_str1.init_calc_mem(117);
  tmp_str2.init_calc_mem(118);
  current->result_tmp_tbl->field[0]->val_str(tmp_str1.get_str());
  current->result_tmp_tbl->field[1]->val_str(tmp_str2.get_str());
  tmp_str1.mem_calc();
  tmp_str2.mem_calc();
  row_ptr = tmp_str2.ptr();
  tmp_row = current->tmp_tbl_row;
  tmp_lengths = (ulong *) tmp_str1.ptr();
  for (roop_count = 0; roop_count < current->field_count; roop_count++)
  {
    if (*tmp_lengths)
    {
      *tmp_row = (char *) row_ptr;
      row_ptr += *tmp_lengths + 1;
    } else {
      *tmp_row = NULL;
    }
    tmp_row++;
    tmp_lengths++;
  }
  *row = current->tmp_tbl_row;
  *lengths = (ulong *) tmp_str1.ptr();
  DBUG_VOID_RETURN;
}

int spider_db_get_row_and_lengths_from_tmp_tbl(
  SPIDER_RESULT *current,
  SPIDER_DB_ROW *row,
  ulong **lengths
) {
  int error_num;
  DBUG_ENTER("spider_db_get_row_and_lengths_from_tmp_tbl");
  if (current->result_tmp_tbl_inited == 2)
  {
    current->result_tmp_tbl->file->ha_rnd_end();
    current->result_tmp_tbl_inited = 0;
  }
  if (current->result_tmp_tbl_inited == 0)
  {
    current->result_tmp_tbl->file->extra(HA_EXTRA_CACHE);
    if ((error_num = current->result_tmp_tbl->file->ha_rnd_init(TRUE)))
      DBUG_RETURN(error_num);
    current->result_tmp_tbl_inited = 1;
  }
  if (
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50200
    (error_num = current->result_tmp_tbl->file->ha_rnd_next(
      current->result_tmp_tbl->record[0]))
#else
    (error_num = current->result_tmp_tbl->file->rnd_next(
      current->result_tmp_tbl->record[0]))
#endif
  ) {
    DBUG_RETURN(error_num);
  }
  spider_db_get_row_and_lengths_from_tmp_tbl_rec(current, row, lengths);
  DBUG_RETURN(0);
}

int spider_db_get_row_and_lengths_from_tmp_tbl_pos(
  SPIDER_POSITION *pos,
  SPIDER_DB_ROW *row,
  ulong **lengths
) {
  int error_num;
  SPIDER_RESULT *result = pos->result;
  TABLE *tmp_tbl = result->result_tmp_tbl;
  DBUG_ENTER("spider_db_get_row_and_lengths_from_tmp_tbl_pos");
  if (result->result_tmp_tbl_inited == 1)
  {
    tmp_tbl->file->ha_rnd_end();
    result->result_tmp_tbl_inited = 0;
  }
  if (result->result_tmp_tbl_inited == 0)
  {
    if ((error_num = tmp_tbl->file->ha_rnd_init(FALSE)))
      DBUG_RETURN(error_num);
    result->result_tmp_tbl_inited = 2;
  }
  if (
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50200
    (error_num = tmp_tbl->file->ha_rnd_pos(tmp_tbl->record[0],
      (uchar *) &pos->tmp_tbl_pos))
#else
    (error_num = tmp_tbl->file->rnd_pos(tmp_tbl->record[0],
      (uchar *) &pos->tmp_tbl_pos))
#endif
  ) {
    DBUG_RETURN(error_num);
  }
  spider_db_get_row_and_lengths_from_tmp_tbl_rec(result, row, lengths);
  DBUG_RETURN(0);
}

int spider_db_fetch_row(
  SPIDER_SHARE *share,
  Field *field,
  SPIDER_DB_ROW row,
  ulong *length,
  my_ptrdiff_t ptr_diff
) {
  DBUG_ENTER("spider_db_fetch_row");
  DBUG_PRINT("info", ("spider fieldcharset %s", field->charset()->csname));
  field->move_field_offset(ptr_diff);
  if (!*row)
  {
    DBUG_PRINT("info", ("spider field is null"));
    field->set_null();
    field->reset();
  } else {
    field->set_notnull();
    if (field->flags & BLOB_FLAG)
    {
      DBUG_PRINT("info", ("spider blob field"));
      if (
        field->charset() == &my_charset_bin ||
        field->charset()->cset == share->access_charset->cset
      )
        ((Field_blob *)field)->set_ptr(*length, (uchar *) *row);
      else {
        DBUG_PRINT("info", ("spider blob convert"));
        ha_spider *spider = (ha_spider *) field->table->file;
        spider_string *str = &spider->blob_buff[field->field_index];
        str->length(0);
        if (str->append(*row, *length, share->access_charset))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        ((Field_blob *)field)->set_ptr(str->length(), (uchar *) str->ptr());
      }
    } else
      field->store(*row, *length, share->access_charset);
  }
  field->move_field_offset(-ptr_diff);
  DBUG_RETURN(0);
}

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
int spider_db_fetch_hs_row(
  SPIDER_SHARE *share,
  Field *field,
  const SPIDER_HS_STRING_REF &hs_row,
  my_ptrdiff_t ptr_diff
) {
  DBUG_ENTER("spider_db_fetch_hs_row");
  DBUG_PRINT("info", ("spider fieldcharset %s", field->charset()->csname));
  field->move_field_offset(ptr_diff);
  if (!hs_row.begin())
  {
    DBUG_PRINT("info", ("spider field is null"));
    field->set_null();
    field->reset();
  } else {
#ifndef DBUG_OFF
    char buf[MAX_FIELD_WIDTH];
    spider_string tmp_str(buf, MAX_FIELD_WIDTH, field->charset());
    tmp_str.init_calc_mem(119);
    tmp_str.length(0);
    tmp_str.append(hs_row.begin(), hs_row.size(), &my_charset_bin);
    DBUG_PRINT("info", ("spider val=%s", tmp_str.c_ptr_safe()));
#endif
    field->set_notnull();
    if (field->flags & BLOB_FLAG)
    {
      DBUG_PRINT("info", ("spider blob field"));
      ((Field_blob *)field)->set_ptr(hs_row.size(), (uchar *) hs_row.begin());
    } else
      field->store(hs_row.begin(), hs_row.size(), &my_charset_bin);
  }
  field->move_field_offset(-ptr_diff);
  DBUG_RETURN(0);
}
#endif

int spider_db_fetch_table(
  ha_spider *spider,
  uchar *buf,
  TABLE *table,
  SPIDER_RESULT_LIST *result_list
) {
  int error_num;
  SPIDER_SHARE *share = spider->share;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  SPIDER_RESULT *current = (SPIDER_RESULT*) result_list->current;
  SPIDER_DB_ROW row;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  SPIDER_CONN *conn;
  int pos;
  const SPIDER_HS_STRING_REF *hs_row = NULL;
#endif
  ulong *lengths;
  Field **field;
  DBUG_ENTER("spider_db_fetch_table");
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (spider->conn_kind[spider->result_link_idx] == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if (result_list->quick_mode == 0)
    {
      SPIDER_DB_RESULT *result = current->result;
      if (!(row = mysql_fetch_row(result)))
      {
        table->status = STATUS_NOT_FOUND;
        DBUG_RETURN(HA_ERR_END_OF_FILE);
      }
      lengths = mysql_fetch_lengths(result);
    } else {
      if (result_list->current_row_num < result_list->quick_page_size)
      {
        row = current->first_position[result_list->current_row_num].row;
        lengths =
          current->first_position[result_list->current_row_num].lengths;
      } else {
        if ((error_num = spider_db_get_row_and_lengths_from_tmp_tbl(
          current, &row, &lengths)))
        {
          if (error_num == HA_ERR_END_OF_FILE)
            table->status = STATUS_NOT_FOUND;
          DBUG_RETURN(error_num);
        }
      }
    }

    /* for mrr */
    if (spider->mrr_with_cnt)
    {
      DBUG_PRINT("info", ("spider mrr_with_cnt"));
      if (spider->sql_kind[spider->result_link_idx] == SPIDER_SQL_KIND_SQL)
      {
        if (*row)
          spider->multi_range_hit_point = atoi(*row);
        else
          DBUG_RETURN(ER_SPIDER_UNKNOWN_NUM);
        row++;
        lengths++;
      } else
        spider->multi_range_hit_point = 0;
    }

    if ((error_num = spider_db_append_match_fetch(spider,
      spider->ft_first, spider->ft_current, &row, &lengths)))
      DBUG_RETURN(error_num);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
    if (spider->conn_kind[spider->result_link_idx] == SPIDER_CONN_KIND_HS_READ)
      conn = spider->hs_r_conns[spider->result_link_idx];
    else
      conn = spider->hs_w_conns[spider->result_link_idx];
    if (!(hs_row = conn->hs_conn->get_next_row_from_result(
      result_list->hs_result)))
    {
      table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
  }
#endif

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (!hs_row)
  {
#endif
    for (
      field = table->field;
      *field;
      field++,
      lengths++
    ) {
      if ((
        bitmap_is_set(table->read_set, (*field)->field_index) |
        bitmap_is_set(table->write_set, (*field)->field_index)
      )) {
#ifndef DBUG_OFF
        my_bitmap_map *tmp_map =
          dbug_tmp_use_all_columns(table, table->write_set);
#endif
        DBUG_PRINT("info", ("spider bitmap is set %s", (*field)->field_name));
        if ((error_num =
          spider_db_fetch_row(share, *field, row, lengths, ptr_diff)))
          DBUG_RETURN(error_num);
#ifndef DBUG_OFF
        dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
      }
      row++;
    }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
    if (spider->hs_pushed_ret_fields_num == MAX_FIELDS)
    {
#endif
      for (
        pos = 0,
        field = table->field;
        *field;
        field++
      ) {
        if (
          spider_bit_is_set(spider->searched_bitmap, (*field)->field_index) |
          bitmap_is_set(table->read_set, (*field)->field_index) |
          bitmap_is_set(table->write_set, (*field)->field_index)
        ) {
#ifndef DBUG_OFF
          my_bitmap_map *tmp_map =
            dbug_tmp_use_all_columns(table, table->write_set);
#endif
          if ((error_num =
            spider_db_fetch_hs_row(share, *field, hs_row[pos], ptr_diff)))
            DBUG_RETURN(error_num);
#ifndef DBUG_OFF
          dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
          pos++;
        }
      }
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
    } else {
      uint32 *field_idxs = spider->hs_pushed_ret_fields;
      size_t field_idxs_num = spider->hs_pushed_ret_fields_num;
      Field *tf;
      int roop_count;
      for (roop_count = 0, pos = 0; roop_count < (int) field_idxs_num;
        roop_count++)
      {
        tf = spider->get_top_table_field(field_idxs[roop_count]);
        if ((tf = spider->field_exchange(tf)))
        {
#ifndef DBUG_OFF
          my_bitmap_map *tmp_map =
            dbug_tmp_use_all_columns(table, table->write_set);
#endif
          if ((error_num =
            spider_db_fetch_hs_row(share, tf, hs_row[pos], ptr_diff)))
            DBUG_RETURN(error_num);
#ifndef DBUG_OFF
          dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
          pos++;
        }
      }
    }
#endif
  }
#endif
  table->status = 0;
  DBUG_RETURN(0);
}

int spider_db_fetch_key(
  ha_spider *spider,
  uchar *buf,
  TABLE *table,
  const KEY *key_info,
  SPIDER_RESULT_LIST *result_list
) {
  int error_num;
  SPIDER_SHARE *share = spider->share;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  SPIDER_RESULT *current = (SPIDER_RESULT*) result_list->current;
  KEY_PART_INFO *key_part;
  uint part_num;
  SPIDER_DB_ROW row;
  ulong *lengths;
  Field *field;
  DBUG_ENTER("spider_db_fetch_key");
  if (result_list->quick_mode == 0)
  {
    SPIDER_DB_RESULT *result = current->result;
    if (!(row = mysql_fetch_row(result)))
    {
      table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
    lengths = mysql_fetch_lengths(result);
  } else {
    if (result_list->current_row_num < result_list->quick_page_size)
    {
      row = current->first_position[result_list->current_row_num].row;
      lengths = current->first_position[result_list->current_row_num].lengths;
    } else {
      if ((error_num = spider_db_get_row_and_lengths_from_tmp_tbl(
        current, &row, &lengths)))
      {
        if (error_num == HA_ERR_END_OF_FILE)
          table->status = STATUS_NOT_FOUND;
        DBUG_RETURN(error_num);
      }
    }
  }

  /* for mrr */
  if (spider->mrr_with_cnt)
  {
    DBUG_PRINT("info", ("spider mrr_with_cnt"));
    if (*row)
      spider->multi_range_hit_point = atoi(*row);
    else
      DBUG_RETURN(ER_SPIDER_UNKNOWN_NUM);
    row++;
    lengths++;
  }
  if ((error_num = spider_db_append_match_fetch(spider,
    spider->ft_first, spider->ft_current, &row, &lengths)))
    DBUG_RETURN(error_num);

  for (
    key_part = key_info->key_part,
    part_num = 0;
    part_num < key_info->key_parts;
    key_part++,
    part_num++,
    lengths++
  ) {
    field = key_part->field;
    if ((
      bitmap_is_set(table->read_set, field->field_index) |
      bitmap_is_set(table->write_set, field->field_index)
    )) {
#ifndef DBUG_OFF
      my_bitmap_map *tmp_map =
        dbug_tmp_use_all_columns(table, table->write_set);
#endif
      DBUG_PRINT("info", ("spider bitmap is set %s", field->field_name));
      if ((error_num =
        spider_db_fetch_row(share, field, row, lengths, ptr_diff)))
        DBUG_RETURN(error_num);
#ifndef DBUG_OFF
      dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
    }
    row++;
  }
  table->status = 0;
  DBUG_RETURN(0);
}

int spider_db_fetch_minimum_columns(
  ha_spider *spider,
  uchar *buf,
  TABLE *table,
  SPIDER_RESULT_LIST *result_list
) {
  int error_num;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT *current = (SPIDER_RESULT*) result_list->current;
  SPIDER_DB_ROW row;
  ulong *lengths;
  Field **field;
  DBUG_ENTER("spider_db_fetch_minimum_columns");
  if (result_list->quick_mode == 0)
  {
    SPIDER_DB_RESULT *result = current->result;
    if (!(row = mysql_fetch_row(result)))
    {
      table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
    lengths = mysql_fetch_lengths(result);
  } else {
    if (result_list->current_row_num < result_list->quick_page_size)
    {
      row = current->first_position[result_list->current_row_num].row;
      lengths = current->first_position[result_list->current_row_num].lengths;
    } else {
      if ((error_num = spider_db_get_row_and_lengths_from_tmp_tbl(
        current, &row, &lengths)))
      {
        if (error_num == HA_ERR_END_OF_FILE)
          table->status = STATUS_NOT_FOUND;
        DBUG_RETURN(error_num);
      }
    }
  }

  /* for mrr */
  if (spider->mrr_with_cnt)
  {
    DBUG_PRINT("info", ("spider mrr_with_cnt"));
    if (*row)
      spider->multi_range_hit_point = atoi(*row);
    else
      DBUG_RETURN(ER_SPIDER_UNKNOWN_NUM);
    row++;
    lengths++;
  }
  if ((error_num = spider_db_append_match_fetch(spider,
    spider->ft_first, spider->ft_current, &row, &lengths)))
    DBUG_RETURN(error_num);

  for (
    field = table->field;
    *field;
    field++
  ) {
    DBUG_PRINT("info", ("spider field_index %u", (*field)->field_index));
    DBUG_PRINT("info", ("spider searched_bitmap %u",
      spider_bit_is_set(spider->searched_bitmap, (*field)->field_index)));
    DBUG_PRINT("info", ("spider read_set %u",
      bitmap_is_set(table->read_set, (*field)->field_index)));
    DBUG_PRINT("info", ("spider write_set %u",
      bitmap_is_set(table->write_set, (*field)->field_index)));
    if (
      spider_bit_is_set(spider->searched_bitmap, (*field)->field_index) |
      bitmap_is_set(table->read_set, (*field)->field_index) |
      bitmap_is_set(table->write_set, (*field)->field_index)
    ) {
      if ((
        bitmap_is_set(table->read_set, (*field)->field_index) |
        bitmap_is_set(table->write_set, (*field)->field_index)
      )) {
#ifndef DBUG_OFF
        my_bitmap_map *tmp_map =
          dbug_tmp_use_all_columns(table, table->write_set);
#endif
        DBUG_PRINT("info", ("spider bitmap is set %s", (*field)->field_name));
        if ((error_num =
          spider_db_fetch_row(share, *field, row, lengths, ptr_diff)))
          DBUG_RETURN(error_num);
#ifndef DBUG_OFF
        dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
      }
      row++;
      lengths++;
    }
  }
  table->status = 0;
  DBUG_RETURN(0);
}

void spider_db_free_one_result_for_start_next(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_RESULT *result = (SPIDER_RESULT *) result_list->current;
  DBUG_ENTER("spider_db_free_one_result_for_start_next");
  spider_bg_all_conn_break(spider);

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (spider->conn_kind[spider->result_link_idx] == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if (result_list->low_mem_read)
    {
      if (result)
      {
        do {
          spider_db_free_one_result(result_list, result);
          DBUG_PRINT("info",("spider result=%p", result));
          DBUG_PRINT("info",("spider result->finish_flg = FALSE"));
          result->finish_flg = FALSE;
          result = (SPIDER_RESULT *) result->next;
        } while (result && (result->result || result->first_position));
        result = (SPIDER_RESULT *) result_list->current;
        if (
          !result->result &&
          !result->first_position
        )
          result_list->current = result->prev;
      }
    } else {
      while (
        result && result->next &&
        (result->next->result || result->next->first_position)
      ) {
        result_list->current = result->next;
        result = (SPIDER_RESULT *) result->next;
      }
    }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
    if (result_list->hs_has_result)
    {
      if (!(*result_list->hs_conn)->stable_point())
        (*result_list->hs_conn)->response_buf_remove();
      result_list->hs_result.readbuf.clear();
      result_list->hs_has_result = FALSE;
    }
  }
#endif
  DBUG_VOID_RETURN;
}

void spider_db_free_one_result(
  SPIDER_RESULT_LIST *result_list,
  SPIDER_RESULT *result
) {
  DBUG_ENTER("spider_db_free_one_result");
  if (result_list->quick_mode == 0)
  {
    if (
      !result->use_position &&
      result->result
    ) {
      mysql_free_result(result->result);
      result->result = NULL;
    }
  } else {
    int roop_count;
    SPIDER_POSITION *position = result->first_position;
    if (position)
    {
      for (roop_count = 0; roop_count < result->pos_page_size; roop_count++)
      {
        if (
          position[roop_count].row &&
          !position[roop_count].use_position
        ) {
          spider_free(spider_current_trx, position[roop_count].row, MYF(0));
          position[roop_count].row = NULL;
        }
      }
    }
  }
  DBUG_VOID_RETURN;
}

int spider_db_free_result(
  ha_spider *spider,
  bool final
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_RESULT *result;
  SPIDER_RESULT *prev;
  SPIDER_SHARE *share = spider->share;
  SPIDER_TRX *trx = spider->trx;
  SPIDER_POSITION *position;
  int roop_count;
  DBUG_ENTER("spider_db_free_result");
  spider_bg_all_conn_break(spider);
  result = (SPIDER_RESULT*) result_list->first;

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (result_list->hs_has_result)
  {
    DBUG_ASSERT(*result_list->hs_conn);
    if (!(*result_list->hs_conn)->stable_point())
      (*result_list->hs_conn)->response_buf_remove();
    if (result_list->hs_result.readbuf.real_size() > (size_t)
      spider_param_hs_result_free_size(trx->thd, share->hs_result_free_size))
    {
      result_list->hs_result.readbuf.real_free();
      trx->hs_result_free_count++;
    }
    result_list->hs_result.readbuf.clear();
    result_list->hs_has_result = FALSE;
  }
#endif

  if (
    final ||
    spider_param_reset_sql_alloc(trx->thd, share->reset_sql_alloc) == 1
  ) {
    int alloc_size = final ? 0 :
      (spider_param_init_sql_alloc_size(trx->thd, share->init_sql_alloc_size));
    while (result)
    {
      position = result->first_position;
      if (position)
      {
        for (roop_count = 0; roop_count < result->pos_page_size; roop_count++)
        {
          if (position[roop_count].row)
          {
            spider_free(spider_current_trx, position[roop_count].row,
              MYF(0));
          }
        }
        spider_free(spider_current_trx, position, MYF(0));
      }
      if (result->result)
      {
        mysql_free_result(result->result);
      }
      if (result->result_tmp_tbl)
      {
        if (result->result_tmp_tbl_inited)
        {
          result->result_tmp_tbl->file->ha_rnd_end();
          result->result_tmp_tbl_inited = 0;
        }
        spider_rm_sys_tmp_table_for_result(result->result_tmp_tbl_thd,
          result->result_tmp_tbl, &result->result_tmp_tbl_prm);
        result->result_tmp_tbl = NULL;
        result->result_tmp_tbl_thd = NULL;
      }
      prev = result;
      result = (SPIDER_RESULT*) result->next;
      spider_free(spider_current_trx, prev, MYF(0));
    }
    result_list->first = NULL;
    result_list->last = NULL;
    if (!final)
    {
      bool sql_realloc = FALSE;
      int init_sql_alloc_size =
        spider_param_init_sql_alloc_size(trx->thd, share->init_sql_alloc_size);
      if ((int) result_list->sql.alloced_length() > alloc_size * 2)
      {
        result_list->sql.free();
        if (result_list->sql.real_alloc(init_sql_alloc_size))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        sql_realloc = TRUE;
      }
      if ((int) result_list->ha_sql.alloced_length() > alloc_size * 2)
      {
        result_list->ha_sql.free();
        if (result_list->ha_sql.real_alloc(init_sql_alloc_size))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        sql_realloc = TRUE;
      }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      if ((int) result_list->hs_sql.alloced_length() > alloc_size * 2)
      {
        result_list->hs_sql.free();
        if (result_list->hs_sql.real_alloc(init_sql_alloc_size))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        sql_realloc = TRUE;
      }
#endif
      if (sql_realloc)
      {
        for (roop_count = 0; roop_count < (int) share->link_count;
          roop_count++)
        {
          if ((int) result_list->sqls[roop_count].alloced_length() >
            alloc_size * 2)
          {
            result_list->sqls[roop_count].free();
            if (result_list->sqls[roop_count].real_alloc(
              init_sql_alloc_size))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          }
        }
      }
      if ((int) result_list->insert_sql.alloced_length() > alloc_size * 2)
      {
        result_list->insert_sql.free();
        if (result_list->insert_sql.real_alloc(init_sql_alloc_size))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        if (!share->same_db_table_name)
        {
          for (roop_count = 0; roop_count < (int) share->link_count;
            roop_count++)
          {
            if ((int) result_list->insert_sqls[roop_count].alloced_length() >
              alloc_size * 2)
            {
              result_list->insert_sqls[roop_count].free();
              if (result_list->insert_sqls[roop_count].real_alloc(
                init_sql_alloc_size))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            }
          }
        }
      }
      if ((int) result_list->update_sql.alloced_length() > alloc_size * 2)
      {
        result_list->update_sql.free();
        if (result_list->update_sql.real_alloc(init_sql_alloc_size))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        if (!share->same_db_table_name)
        {
          for (roop_count = 0; roop_count < (int) share->link_count;
            roop_count++)
          {
            if ((int) result_list->update_sqls[roop_count].alloced_length() >
              alloc_size * 2)
            {
              result_list->update_sqls[roop_count].free();
              if (result_list->update_sqls[roop_count].real_alloc(
                init_sql_alloc_size))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            }
          }
        }
      }
      result_list->update_sql.length(0);
      for (roop_count = 0; roop_count < (int) share->link_count; roop_count++)
        result_list->update_sqls[roop_count].length(0);

      if ((int) result_list->tmp_sql.alloced_length() > alloc_size * 2)
      {
        result_list->tmp_sql.free();
        if (result_list->tmp_sql.real_alloc(init_sql_alloc_size))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        for (roop_count = 0; roop_count < (int) share->link_count;
          roop_count++)
        {
          if ((int) result_list->tmp_sqls[roop_count].alloced_length() >
            alloc_size * 2)
          {
            result_list->tmp_sqls[roop_count].free();
            if (result_list->tmp_sqls[roop_count].real_alloc(
              init_sql_alloc_size))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          }
        }
      }
    }
  } else {
    while (result)
    {
      position = result->first_position;
      if (position)
      {
        for (roop_count = 0; roop_count < result->pos_page_size; roop_count++)
        {
          if (position[roop_count].row)
          {
            spider_free(spider_current_trx, position[roop_count].row,
              MYF(0));
          }
        }
        spider_free(spider_current_trx, position, MYF(0));
      }
      result->first_position = NULL;
      if (result->result)
      {
        mysql_free_result(result->result);
        result->result = NULL;
      }
      if (result->result_tmp_tbl)
      {
        if (result->result_tmp_tbl_inited)
        {
          result->result_tmp_tbl->file->ha_rnd_end();
          result->result_tmp_tbl_inited = 0;
        }
        spider_rm_sys_tmp_table_for_result(result->result_tmp_tbl_thd,
          result->result_tmp_tbl, &result->result_tmp_tbl_prm);
        result->result_tmp_tbl = NULL;
        result->result_tmp_tbl_thd = NULL;
      }
      result->record_num = 0;
      DBUG_PRINT("info",("spider result->finish_flg = FALSE"));
      result->finish_flg = FALSE;
      result->use_position = FALSE;
      result = (SPIDER_RESULT*) result->next;
    }
  }
  result_list->current = NULL;
  result_list->record_num = 0;
  DBUG_PRINT("info",("spider result_list->finish_flg = FALSE"));
  result_list->finish_flg = FALSE;
  result_list->quick_phase = 0;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  result_list->bgs_phase = 0;
#endif
  DBUG_RETURN(0);
}

int spider_db_store_result(
  ha_spider *spider,
  int link_idx,
  TABLE *table
) {
  int error_num;
  SPIDER_CONN *conn;
  SPIDER_DB_CONN *db_conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_RESULT *current;
  DBUG_ENTER("spider_db_store_result");
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (spider->conn_kind[link_idx] == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    conn = spider->conns[link_idx];
    db_conn = conn->db_conn;
    if (!result_list->current)
    {
      if (!result_list->first)
      {
        if (!(result_list->first = (SPIDER_RESULT *)
          spider_malloc(spider_current_trx, 4, sizeof(*result_list->first),
            MYF(MY_WME | MY_ZEROFILL)))
        ) {
          if (!conn->mta_conn_mutex_unlock_later)
          {
            SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
            pthread_mutex_unlock(&conn->mta_conn_mutex);
          }
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
        TMP_TABLE_PARAM *tmp_tbl_prm = (TMP_TABLE_PARAM *)
          &result_list->first->result_tmp_tbl_prm;
        tmp_tbl_prm->init();
        tmp_tbl_prm->field_count = 2;
        result_list->last = result_list->first;
        result_list->current = result_list->first;
      } else {
        result_list->current = result_list->first;
      }
      result_list->bgs_current = result_list->current;
      current = (SPIDER_RESULT*) result_list->current;
    } else {
      if (
#ifndef WITHOUT_SPIDER_BG_SEARCH
        result_list->bgs_phase > 0 ||
#endif
        result_list->quick_phase > 0
      ) {
        if (result_list->bgs_current == result_list->last)
        {
          if (!(result_list->last = (SPIDER_RESULT *)
            spider_malloc(spider_current_trx, 5, sizeof(*result_list->last),
               MYF(MY_WME | MY_ZEROFILL)))
          ) {
            if (!conn->mta_conn_mutex_unlock_later)
            {
              SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
              pthread_mutex_unlock(&conn->mta_conn_mutex);
            }
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          }
          TMP_TABLE_PARAM *tmp_tbl_prm = (TMP_TABLE_PARAM *)
            &result_list->last->result_tmp_tbl_prm;
          tmp_tbl_prm->init();
          tmp_tbl_prm->field_count = 2;
          result_list->bgs_current->next = result_list->last;
          result_list->last->prev = result_list->bgs_current;
          result_list->bgs_current = result_list->last;
        } else {
          result_list->bgs_current = result_list->bgs_current->next;
        }
        if (
#ifndef WITHOUT_SPIDER_BG_SEARCH
          result_list->bgs_phase == 1 ||
#endif
          result_list->quick_phase == 2
        ) {
          if (result_list->low_mem_read)
          {
            do {
              spider_db_free_one_result(result_list,
                (SPIDER_RESULT*) result_list->current);
              result_list->current = result_list->current->next;
            } while (result_list->current != result_list->bgs_current);
          } else {
            result_list->current = result_list->bgs_current;
          }
          result_list->quick_phase = 0;
        }
        current = (SPIDER_RESULT*) result_list->bgs_current;
      } else {
        if (result_list->current == result_list->last)
        {
          if (!(result_list->last = (SPIDER_RESULT *)
            spider_malloc(spider_current_trx, 6, sizeof(*result_list->last),
              MYF(MY_WME | MY_ZEROFILL)))
          ) {
            if (!conn->mta_conn_mutex_unlock_later)
            {
              SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
              pthread_mutex_unlock(&conn->mta_conn_mutex);
            }
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          }
          TMP_TABLE_PARAM *tmp_tbl_prm = (TMP_TABLE_PARAM *)
            &result_list->last->result_tmp_tbl_prm;
          tmp_tbl_prm->init();
          tmp_tbl_prm->field_count = 2;
          result_list->current->next = result_list->last;
          result_list->last->prev = result_list->current;
          result_list->current = result_list->last;
        } else {
          result_list->current = result_list->current->next;
        }
        result_list->bgs_current = result_list->current;
        current = (SPIDER_RESULT*) result_list->current;
      }
    }

    if (result_list->quick_mode == 0)
    {
      if (!(current->result = mysql_store_result(db_conn)))
      {
        if ((error_num = spider_db_errorno(conn)))
          DBUG_RETURN(error_num);
        else {
          DBUG_PRINT("info",("spider set finish_flg point 1"));
          DBUG_PRINT("info",("spider current->finish_flg = TRUE"));
          DBUG_PRINT("info",("spider result_list->finish_flg = TRUE"));
          current->finish_flg = TRUE;
          result_list->finish_flg = TRUE;
#ifndef WITHOUT_SPIDER_BG_SEARCH
          if (result_list->bgs_phase <= 1)
          {
#endif
            result_list->current_row_num = 0;
            table->status = STATUS_NOT_FOUND;
#ifndef WITHOUT_SPIDER_BG_SEARCH
          }
#endif
          if (!conn->mta_conn_mutex_unlock_later)
          {
            SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
            pthread_mutex_unlock(&conn->mta_conn_mutex);
          }
          DBUG_RETURN(HA_ERR_END_OF_FILE);
        }
      } else {
        if (!conn->mta_conn_mutex_unlock_later)
        {
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
        }
        current->record_num =
          (longlong) mysql_num_rows(current->result);
        result_list->record_num += current->record_num;
        DBUG_PRINT("info",("spider current->record_num=%lld",
          current->record_num));
        DBUG_PRINT("info",("spider result_list->record_num=%lld",
          result_list->record_num));
        if (
          result_list->internal_limit <= result_list->record_num ||
          result_list->split_read > current->record_num
        ) {
          DBUG_PRINT("info",("spider result_list->internal_limit=%lld",
            result_list->internal_limit));
          DBUG_PRINT("info",("spider result_list->split_read=%lld",
            result_list->split_read));
          DBUG_PRINT("info",("spider set finish_flg point 2"));
          DBUG_PRINT("info",("spider current->finish_flg = TRUE"));
          DBUG_PRINT("info",("spider result_list->finish_flg = TRUE"));
          current->finish_flg = TRUE;
          result_list->finish_flg = TRUE;
        }
        current->first_row = current->result->data_cursor;
#ifndef WITHOUT_SPIDER_BG_SEARCH
        if (result_list->bgs_phase <= 1)
        {
#endif
          result_list->current_row_num = 0;
#ifndef WITHOUT_SPIDER_BG_SEARCH
        }
#endif
      }
    } else {
      if (current->prev && current->prev->result)
      {
        current->result = current->prev->result;
        current->prev->result = NULL;
        result_list->limit_num -= current->prev->record_num;
        if (!conn->mta_conn_mutex_unlock_later)
        {
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
        }
      } else {
        if (!(current->result =
          conn->db_conn->methods->use_result(db_conn)))
          DBUG_RETURN(spider_db_errorno(conn));
        conn->quick_target = spider;
        spider->quick_targets[link_idx] = spider;
        if (!conn->mta_conn_mutex_unlock_later)
        {
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
        }
      }
      SPIDER_DB_ROW row;
      if (!(row = mysql_fetch_row(current->result)))
      {
        DBUG_PRINT("info",("spider set finish_flg point 3"));
        DBUG_PRINT("info",("spider current->finish_flg = TRUE"));
        DBUG_PRINT("info",("spider result_list->finish_flg = TRUE"));
        current->finish_flg = TRUE;
        result_list->finish_flg = TRUE;
        mysql_free_result(current->result);
        current->result = NULL;
        conn->quick_target = NULL;
        spider->quick_targets[link_idx] = NULL;
        if (
#ifndef WITHOUT_SPIDER_BG_SEARCH
          result_list->bgs_phase <= 1 &&
#endif
          result_list->quick_phase == 0
        ) {
          result_list->current_row_num = 0;
          table->status = STATUS_NOT_FOUND;
        } else if (result_list->quick_phase > 0)
          DBUG_RETURN(0);
        DBUG_RETURN(HA_ERR_END_OF_FILE);
      }
      SPIDER_DB_ROW tmp_row;
      char *tmp_char;
      ulong *lengths;
      uint field_count = db_conn->field_count;
      int row_size;
      SPIDER_POSITION *position;
      longlong page_size =
        !result_list->quick_page_size ||
        result_list->limit_num < result_list->quick_page_size ?
        result_list->limit_num : result_list->quick_page_size;
      int roop_count = 0;
      int roop_count2;
      current->field_count = field_count;
      if (!(position = (SPIDER_POSITION *)
        spider_bulk_malloc(spider_current_trx, 7, MYF(MY_WME | MY_ZEROFILL),
          &position, sizeof(SPIDER_POSITION) * page_size,
          &tmp_row, sizeof(char*) * field_count,
          NullS))
      )
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      current->pos_page_size = (int) page_size;
      current->first_position = position;
      current->tmp_tbl_row = tmp_row;
      do {
        row_size = field_count;
        lengths = current->result->lengths;
        for (roop_count2 = 0; roop_count2 < (int) field_count; roop_count2++)
        {
          row_size += *lengths;
          lengths++;
        }
        DBUG_PRINT("info",("spider row_size=%d", row_size));
        if (!spider_bulk_malloc(spider_current_trx, 29, MYF(MY_WME),
          &tmp_row, sizeof(char*) * field_count,
          &tmp_char, row_size,
          &lengths, sizeof(ulong) * field_count,
          NullS)
        )
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        memcpy(lengths, current->result->lengths, sizeof(ulong) * field_count);
        position->row = tmp_row;
        position->lengths = lengths;
        for (roop_count2 = 0; roop_count2 < (int) field_count; roop_count2++)
        {
          DBUG_PRINT("info",("spider *lengths=%lu", *lengths));
          if (*row == NULL)
          {
            *tmp_row = NULL;
            *tmp_char = 0;
            tmp_char++;
          } else {
            *tmp_row = tmp_char;
            memcpy(tmp_char, *row, *lengths + 1);
            tmp_char += *lengths + 1;
          }
          tmp_row++;
          lengths++;
          row++;
        }
        position++;
        roop_count++;
      } while (
        page_size > roop_count &&
        (row = mysql_fetch_row(current->result))
      );
      if (
        result_list->quick_mode == 3 &&
        page_size == roop_count &&
        result_list->limit_num > roop_count &&
        (row = mysql_fetch_row(current->result))
      ) {
        THD *thd = current_thd;
        char buf[MAX_FIELD_WIDTH];
        spider_string tmp_str(buf, MAX_FIELD_WIDTH, &my_charset_bin);
        tmp_str.init_calc_mem(120);

        DBUG_PRINT("info",("spider store result to temporary table"));
        DBUG_ASSERT(!current->result_tmp_tbl);
        if (!(current->result_tmp_tbl = spider_mk_sys_tmp_table_for_result(
          thd, table, &current->result_tmp_tbl_prm, "a", "b",
          &my_charset_bin)))
        {
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
        current->result_tmp_tbl_thd = thd;
        TABLE *tmp_tbl = current->result_tmp_tbl;
        tmp_tbl->file->extra(HA_EXTRA_WRITE_CACHE);
        tmp_tbl->file->ha_start_bulk_insert((ha_rows) 0);
        tmp_tbl->field[0]->set_notnull();
        tmp_tbl->field[1]->set_notnull();
        do {
          lengths = current->result->lengths;
          tmp_str.length(0);
          for (roop_count2 = 0; roop_count2 < (int) field_count; roop_count2++)
          {
            if (*lengths)
            {
              if (tmp_str.reserve(*lengths + 1))
              {
                tmp_tbl->file->ha_end_bulk_insert();
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              }
              tmp_str.q_append(*row, *lengths + 1);
            }
            lengths++;
            row++;
          }
          tmp_tbl->field[0]->store(
            (const char *) current->result->lengths,
            sizeof(ulong) * field_count, &my_charset_bin);
          tmp_tbl->field[1]->store(
            tmp_str.ptr(), tmp_str.length(), &my_charset_bin);
          if ((error_num = tmp_tbl->file->ha_write_row(tmp_tbl->record[0])))
          {
            tmp_tbl->file->ha_end_bulk_insert();
            DBUG_RETURN(error_num);
          }
          roop_count++;
        } while (
          result_list->limit_num > roop_count &&
          (row = mysql_fetch_row(current->result))
        );
        tmp_tbl->file->ha_end_bulk_insert();
        page_size = result_list->limit_num;
      }
      current->record_num = roop_count;
      result_list->record_num += roop_count;
      if (
        result_list->internal_limit <= result_list->record_num ||
        page_size > roop_count
      ) {
        DBUG_PRINT("info",("spider set finish_flg point 4"));
        DBUG_PRINT("info",("spider current->finish_flg = TRUE"));
        DBUG_PRINT("info",("spider result_list->finish_flg = TRUE"));
        current->finish_flg = TRUE;
        result_list->finish_flg = TRUE;
        mysql_free_result(current->result);
        current->result = NULL;
        conn->quick_target = NULL;
        spider->quick_targets[link_idx] = NULL;
      } else if (result_list->limit_num == roop_count)
      {
        mysql_free_result(current->result);
        current->result = NULL;
        conn->quick_target = NULL;
        spider->quick_targets[link_idx] = NULL;
      }
      if (
#ifndef WITHOUT_SPIDER_BG_SEARCH
        result_list->bgs_phase <= 1 &&
#endif
        result_list->quick_phase == 0
      ) {
        result_list->current_row_num = 0;
      }
    }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
    size_t num_fields;
    if (spider->conn_kind[link_idx] == SPIDER_CONN_KIND_HS_READ)
      conn = spider->hs_r_conns[link_idx];
    else
      conn = spider->hs_w_conns[link_idx];
    DBUG_PRINT("info",("spider conn=%p", conn));
    if (
      (error_num = conn->hs_conn->response_recv(num_fields)) ||
      (error_num = conn->hs_conn->get_result(result_list->hs_result))
    ) {
#ifndef DBUG_OFF
      if (conn->hs_conn->get_response_end_offset() > 0 &&
        conn->hs_conn->get_readbuf_begin())
      {
        char tmp_buf[MAX_FIELD_WIDTH];
        String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
        tmp_str.length(0);
        tmp_str.append(conn->hs_conn->get_readbuf_begin(),
          conn->hs_conn->get_response_end_offset(), &my_charset_bin);
        DBUG_PRINT("info",("spider hs readbuf01 size=%zu str=%s",
          conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
      }
#endif
      if (error_num > 0)
        conn->hs_conn->response_buf_remove();
      spider_db_errorno(conn);
      DBUG_RETURN(ER_SPIDER_HS_NUM);
    }
#ifndef DBUG_OFF
    if (conn->hs_conn->get_response_end_offset() > 0 &&
      conn->hs_conn->get_readbuf_begin())
    {
      char tmp_buf[MAX_FIELD_WIDTH];
      String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
      tmp_str.length(0);
      tmp_str.append(conn->hs_conn->get_readbuf_begin(),
        conn->hs_conn->get_response_end_offset(), &my_charset_bin);
      DBUG_PRINT("info",("spider hs readbuf02 size=%zu str=%s",
        conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
    }
#endif
    result_list->hs_conn = &conn->hs_conn;
    result_list->hs_has_result = TRUE;
    conn->hs_conn->response_buf_remove();
    if (!conn->mta_conn_mutex_unlock_later)
    {
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_next_result(
  SPIDER_CONN *conn
) {
  int status;
  SPIDER_DB_CONN *db_conn = conn->db_conn;
  DBUG_ENTER("spider_db_next_result");

  if (db_conn->status != MYSQL_STATUS_READY)
  {
    my_message(ER_SPIDER_UNKNOWN_NUM, ER_SPIDER_UNKNOWN_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_UNKNOWN_NUM);
  }

  db_conn->net.last_errno = 0;
  db_conn->net.last_error[0] = '\0';
  strmov(db_conn->net.sqlstate, "00000");
  db_conn->affected_rows = ~(my_ulonglong) 0;

#if MYSQL_VERSION_ID < 50500
  if (db_conn->last_used_con->server_status & SERVER_MORE_RESULTS_EXISTS)
#else
  if (db_conn->server_status & SERVER_MORE_RESULTS_EXISTS)
#endif
  {
    if ((status = db_conn->methods->read_query_result(db_conn)) > 0)
      DBUG_RETURN(spider_db_errorno(conn));
    DBUG_RETURN(status);
  }

  DBUG_RETURN(-1);
}

void spider_db_discard_result(
  SPIDER_CONN *conn
) {
  SPIDER_DB_RESULT *result;
  DBUG_ENTER("spider_db_discard_result");
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if ((result =
      conn->db_conn->methods->use_result(conn->db_conn)))
      mysql_free_result(result);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
    size_t num_fields;
    DBUG_PRINT("info",("spider conn=%p", conn));
    if (conn->hs_conn->response_recv(num_fields) >= 0)
    {
#ifndef DBUG_OFF
      if (conn->hs_conn->get_response_end_offset() > 0 &&
        conn->hs_conn->get_readbuf_begin())
      {
        char tmp_buf[MAX_FIELD_WIDTH];
        String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
        tmp_str.length(0);
        tmp_str.append(conn->hs_conn->get_readbuf_begin(),
          conn->hs_conn->get_response_end_offset(), &my_charset_bin);
        DBUG_PRINT("info",("spider hs readbuf03 size=%zu str=%s",
          conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
      }
#endif
      conn->hs_conn->response_buf_remove();
    }
#ifndef DBUG_OFF
    if (conn->hs_conn->get_response_end_offset() > 0 &&
      conn->hs_conn->get_readbuf_begin())
    {
      char tmp_buf[MAX_FIELD_WIDTH];
      String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
      tmp_str.length(0);
      tmp_str.append(conn->hs_conn->get_readbuf_begin(),
        conn->hs_conn->get_response_end_offset(), &my_charset_bin);
      DBUG_PRINT("info",("spider hs readbuf04 size=%zu str=%s",
        conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
    }
#endif
  }
#endif
  DBUG_VOID_RETURN;
}

void spider_db_discard_multiple_result(
  SPIDER_CONN *conn
) {
  SPIDER_DB_RESULT *result;
  DBUG_ENTER("spider_db_discard_multiple_result");
  do
  {
    if ((result =
      conn->db_conn->methods->use_result(conn->db_conn)))
      mysql_free_result(result);
  } while (!spider_db_next_result(conn));
  DBUG_VOID_RETURN;
}

int spider_db_fetch(
  uchar *buf,
  ha_spider *spider,
  TABLE *table
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_fetch");
  if (spider->sql_kind[spider->result_link_idx] == SPIDER_SQL_KIND_SQL)
  {
    if (!spider->select_column_mode) {
      if (result_list->keyread)
        error_num = spider_db_fetch_key(spider, buf, table,
          result_list->key_info, result_list);
      else
        error_num = spider_db_fetch_table(spider, buf, table,
          result_list);
    } else
      error_num = spider_db_fetch_minimum_columns(spider, buf, table,
        result_list);
  } else {
    error_num = spider_db_fetch_table(spider, buf, table,
      result_list);
  }
  result_list->current_row_num++;
  DBUG_PRINT("info",("spider error_num=%d", error_num));
  DBUG_RETURN(error_num);
}

int spider_db_seek_prev(
  uchar *buf,
  ha_spider *spider,
  TABLE *table
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_seek_prev");
  if (result_list->current_row_num <= 1)
  {
    if (result_list->current == result_list->first)
    {
      table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
    if (result_list->low_mem_read == 1)
    {
      my_message(ER_SPIDER_LOW_MEM_READ_PREV_NUM,
        ER_SPIDER_LOW_MEM_READ_PREV_STR, MYF(0));
      DBUG_RETURN(ER_SPIDER_LOW_MEM_READ_PREV_NUM);
    }
    result_list->current = result_list->current->prev;
    result_list->current_row_num = result_list->current->record_num - 1;
  } else {
    result_list->current_row_num -= 2;
  }
  if (result_list->quick_mode == 0)
    result_list->current->result->data_cursor =
      result_list->current->first_row + result_list->current_row_num;
  DBUG_RETURN(spider_db_fetch(buf, spider, table));
}

int spider_db_seek_next(
  uchar *buf,
  ha_spider *spider,
  int link_idx,
  TABLE *table
) {
  int error_num, tmp_pos;
  spider_string *sql;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn = spider->conns[link_idx];
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_seek_next");
  if (
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    spider->conn_kind[spider->result_link_idx] == SPIDER_CONN_KIND_MYSQL &&
#endif
    result_list->current_row_num >= result_list->current->record_num
  ) {
    DBUG_PRINT("info",("spider result_list->current_row_num=%lld",
      result_list->current_row_num));
    DBUG_PRINT("info",("spider result_list->current->record_num=%lld",
      result_list->current->record_num));
    if (result_list->low_mem_read)
      spider_db_free_one_result(result_list,
        (SPIDER_RESULT*) result_list->current);

    int roop_start, roop_end, roop_count, lock_mode, link_ok;
    lock_mode = spider_conn_lock_mode(spider);
    if (lock_mode)
    {
      /* "for update" or "lock in share mode" */
      link_ok = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_OK);
      roop_start = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_end = spider->share->link_count;
    } else {
      link_ok = link_idx;
      roop_start = link_idx;
      roop_end = link_idx + 1;
    }

#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (result_list->bgs_phase > 0)
    {
      for (roop_count = roop_start; roop_count < roop_end;
        roop_count = spider_conn_link_idx_next(share->link_statuses,
          spider->conn_link_idx, roop_count, share->link_count,
          SPIDER_LINK_STATUS_RECOVERY)
      ) {
        if ((error_num = spider_bg_conn_search(spider, roop_count, roop_start,
          FALSE, FALSE, (roop_count != link_ok))))
          DBUG_RETURN(error_num);
      }
    } else {
#endif
      if (result_list->current == result_list->bgs_current)
      {
        if (result_list->finish_flg)
        {
          table->status = STATUS_NOT_FOUND;
          DBUG_RETURN(HA_ERR_END_OF_FILE);
        }
        spider_next_split_read_param(spider);
        if (
          result_list->quick_mode == 0 ||
          !result_list->current->result
        ) {
          result_list->limit_num =
            result_list->internal_limit - result_list->record_num >=
            result_list->split_read ?
            result_list->split_read :
            result_list->internal_limit - result_list->record_num;
          if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
          {
            result_list->sql.length(result_list->limit_pos);
            if ((error_num = spider_db_append_limit_internal(
              spider, &result_list->sql, result_list->record_num,
              result_list->limit_num, SPIDER_SQL_KIND_SQL)) ||
              (
                !result_list->use_union &&
                (error_num = spider_db_append_select_lock(spider))
              )
            )
              DBUG_RETURN(error_num);
          }
          if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
          {
            spider_db_append_handler_next(spider);
            sql = &result_list->ha_sql;
            sql->length(result_list->ha_limit_pos);
            if ((error_num = spider_db_append_limit_internal(
              spider, &result_list->ha_sql, 0,
              result_list->limit_num, SPIDER_SQL_KIND_HANDLER))
            )
              DBUG_RETURN(error_num);
          }

          for (roop_count = roop_start; roop_count < roop_end;
            roop_count = spider_conn_link_idx_next(share->link_statuses,
              spider->conn_link_idx, roop_count, share->link_count,
              SPIDER_LINK_STATUS_RECOVERY)
          ) {
            conn = spider->conns[roop_count];
            if (spider->sql_kind[roop_count] == SPIDER_SQL_KIND_SQL)
            {
              if (spider->share->same_db_table_name)
                sql = &result_list->sql;
              else {
                sql = &result_list->sqls[roop_count];
                if (sql->copy(result_list->sql))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                tmp_pos = sql->length();
                sql->length(result_list->table_name_pos);
                spider_db_append_table_name_with_adjusting(spider, sql,
                  spider->share, roop_count, SPIDER_SQL_KIND_SQL);
                sql->length(tmp_pos);
              }
            } else {
              if (spider->m_handler_id[roop_count] ==
                result_list->ha_sql_handler_id)
                sql = &result_list->ha_sql;
              else {
                sql = &result_list->sqls[roop_count];
                if (sql->copy(result_list->ha_sql))
                  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
                tmp_pos = sql->length();
                sql->length(result_list->ha_table_name_pos);
                spider_db_append_table_name_with_adjusting(spider, sql,
                  spider->share, roop_count, SPIDER_SQL_KIND_HANDLER);
                sql->length(tmp_pos);
              }
            }
            pthread_mutex_lock(&conn->mta_conn_mutex);
            SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
            conn->need_mon = &spider->need_mons[roop_count];
            conn->mta_conn_mutex_lock_already = TRUE;
            conn->mta_conn_mutex_unlock_later = TRUE;
            if ((error_num = spider_db_set_names(spider, conn, roop_count)))
            {
              conn->mta_conn_mutex_lock_already = FALSE;
              conn->mta_conn_mutex_unlock_later = FALSE;
              SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
              pthread_mutex_unlock(&conn->mta_conn_mutex);
              if (
                share->monitoring_kind[roop_count] &&
                spider->need_mons[roop_count]
              ) {
                error_num = spider_ping_table_mon_from_table(
                    spider->trx,
                    spider->trx->thd,
                    share,
                    (uint32) share->monitoring_sid[roop_count],
                    share->table_name,
                    share->table_name_length,
                    spider->conn_link_idx[roop_count],
                    NULL,
                    0,
                    share->monitoring_kind[roop_count],
                    share->monitoring_limit[roop_count],
                    TRUE
                  );
              }
              DBUG_RETURN(error_num);
            }
            spider_conn_set_timeout_from_share(conn, roop_count,
              spider->trx->thd, share);
            if (spider_db_query(
              conn,
              sql->ptr(),
              sql->length(),
              &spider->need_mons[roop_count])
            ) {
              conn->mta_conn_mutex_lock_already = FALSE;
              conn->mta_conn_mutex_unlock_later = FALSE;
              error_num = spider_db_errorno(conn);
              if (
                share->monitoring_kind[roop_count] &&
                spider->need_mons[roop_count]
              ) {
                error_num = spider_ping_table_mon_from_table(
                    spider->trx,
                    spider->trx->thd,
                    share,
                    (uint32) share->monitoring_sid[roop_count],
                    share->table_name,
                    share->table_name_length,
                    spider->conn_link_idx[roop_count],
                    NULL,
                    0,
                    share->monitoring_kind[roop_count],
                    share->monitoring_limit[roop_count],
                    TRUE
                  );
              }
              DBUG_RETURN(error_num);
            }
            conn->mta_conn_mutex_lock_already = FALSE;
            conn->mta_conn_mutex_unlock_later = FALSE;
            if (roop_count == link_ok)
            {
              if ((error_num = spider_db_store_result(spider, roop_count,
                table)))
              {
                if (
                  error_num != HA_ERR_END_OF_FILE &&
                  share->monitoring_kind[roop_count] &&
                  spider->need_mons[roop_count]
                ) {
                  error_num = spider_ping_table_mon_from_table(
                      spider->trx,
                      spider->trx->thd,
                      share,
                      (uint32) share->monitoring_sid[roop_count],
                      share->table_name,
                      share->table_name_length,
                      spider->conn_link_idx[roop_count],
                      NULL,
                      0,
                      share->monitoring_kind[roop_count],
                      share->monitoring_limit[roop_count],
                      TRUE
                    );
                }
                DBUG_RETURN(error_num);
              }
              spider->result_link_idx = link_ok;
            } else {
              spider_db_discard_result(conn);
              SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
              pthread_mutex_unlock(&conn->mta_conn_mutex);
            }
          }
        } else {
          conn->mta_conn_mutex_unlock_later = TRUE;
          if ((error_num = spider_db_store_result(spider, link_idx, table)))
          {
            conn->mta_conn_mutex_unlock_later = FALSE;
            DBUG_RETURN(error_num);
          }
          conn->mta_conn_mutex_unlock_later = FALSE;
        }
      } else {
        result_list->current = result_list->current->next;
        result_list->current_row_num = 0;
        if (
          result_list->current == result_list->bgs_current &&
          result_list->finish_flg
        ) {
          table->status = STATUS_NOT_FOUND;
          DBUG_RETURN(HA_ERR_END_OF_FILE);
        }
      }
#ifndef WITHOUT_SPIDER_BG_SEARCH
    }
#endif
    DBUG_RETURN(spider_db_fetch(buf, spider, table));
  } else
    DBUG_RETURN(spider_db_fetch(buf, spider, table));
}

int spider_db_seek_last(
  uchar *buf,
  ha_spider *spider,
  int link_idx,
  TABLE *table
) {
  int error_num, tmp_pos;
  spider_string *sql;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_seek_last");
  if (result_list->finish_flg)
  {
    if (result_list->low_mem_read == 1)
    {
      my_message(ER_SPIDER_LOW_MEM_READ_PREV_NUM,
        ER_SPIDER_LOW_MEM_READ_PREV_STR, MYF(0));
      DBUG_RETURN(ER_SPIDER_LOW_MEM_READ_PREV_NUM);
    }
    result_list->current = result_list->last;
    result_list->current_row_num = result_list->current->record_num - 1;
    if (result_list->quick_mode == 0)
      result_list->current->result->data_cursor =
        result_list->current->first_row + result_list->current_row_num;
    DBUG_RETURN(spider_db_fetch(buf, spider, table));
  } else if (!result_list->sorted ||
    result_list->internal_limit <= result_list->record_num * 2)
  {
    if (result_list->low_mem_read == 1)
    {
      my_message(ER_SPIDER_LOW_MEM_READ_PREV_NUM,
        ER_SPIDER_LOW_MEM_READ_PREV_STR, MYF(0));
      DBUG_RETURN(ER_SPIDER_LOW_MEM_READ_PREV_NUM);
    }
    spider_next_split_read_param(spider);
    if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
    {
      spider_db_append_handler_next(spider);
    }
    result_list->sql.length(result_list->limit_pos);
    result_list->ha_sql.length(result_list->ha_limit_pos);
    result_list->limit_num =
      result_list->internal_limit - result_list->record_num;
    if ((error_num = spider_db_append_limit(
      spider, &result_list->sql,
      result_list->internal_offset + result_list->record_num,
      result_list->limit_num)) ||
      (
        !result_list->use_union &&
        (error_num = spider_db_append_select_lock(spider))
      )
    )
      DBUG_RETURN(error_num);

    int roop_start, roop_end, roop_count, lock_mode, link_ok;
    lock_mode = spider_conn_lock_mode(spider);
    if (lock_mode)
    {
      /* "for update" or "lock in share mode" */
      link_ok = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_OK);
      roop_start = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_end = spider->share->link_count;
    } else {
      link_ok = link_idx;
      roop_start = link_idx;
      roop_end = link_idx + 1;
    }
    for (roop_count = roop_start; roop_count < roop_end;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
      conn = spider->conns[roop_count];
      if (spider->sql_kind[roop_count] == SPIDER_SQL_KIND_SQL)
      {
        if (spider->share->same_db_table_name)
          sql = &result_list->sql;
        else {
          sql = &result_list->sqls[roop_count];
          if (sql->copy(result_list->sql))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          tmp_pos = sql->length();
          sql->length(result_list->table_name_pos);
          spider_db_append_table_name_with_adjusting(spider, sql,
            spider->share, roop_count, SPIDER_SQL_KIND_SQL);
          sql->length(tmp_pos);
        }
      } else {
        if (spider->m_handler_id[roop_count] ==
          result_list->ha_sql_handler_id)
          sql = &result_list->ha_sql;
        else {
          sql = &result_list->sqls[roop_count];
          if (sql->copy(result_list->ha_sql))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          tmp_pos = sql->length();
          sql->length(result_list->ha_table_name_pos);
          spider_db_append_table_name_with_adjusting(spider, sql,
            spider->share, roop_count, SPIDER_SQL_KIND_HANDLER);
          sql->length(tmp_pos);
        }
      }
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
        share);
      if (spider_db_query(
        conn,
        sql->ptr(),
        sql->length(),
        &spider->need_mons[roop_count])
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conn);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      if (roop_count == link_ok)
      {
        if ((error_num = spider_db_store_result(spider, roop_count, table)))
        {
          if (
            error_num != HA_ERR_END_OF_FILE &&
            share->monitoring_kind[roop_count] &&
            spider->need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                spider->trx,
                spider->trx->thd,
                share,
                (uint32) share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                spider->conn_link_idx[roop_count],
                NULL,
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        spider->result_link_idx = link_ok;
      } else {
        spider_db_discard_result(conn);
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      }
    }
    result_list->current_row_num = result_list->current->record_num - 1;
    if (result_list->quick_mode == 0)
      result_list->current->result->data_cursor =
        result_list->current->first_row + result_list->current_row_num;
    DBUG_RETURN(spider_db_fetch(buf, spider, table));
  }
  if ((error_num = spider_db_free_result(spider, FALSE)))
    DBUG_RETURN(error_num);
  spider_first_split_read_param(spider);
  result_list->sql.length(result_list->order_pos);
  result_list->desc_flg = !(result_list->desc_flg);
  if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
  {
    sql = &result_list->ha_sql;
    sql->length(result_list->ha_next_pos);
    if (result_list->sorted && result_list->desc_flg)
    {
      sql->q_append(SPIDER_SQL_LAST_STR, SPIDER_SQL_LAST_LEN);
      memset((char *) sql->ptr() + sql->length(), ' ',
        result_list->ha_where_pos - result_list->ha_next_pos -
        SPIDER_SQL_LAST_LEN);
    } else {
      sql->q_append(SPIDER_SQL_FIRST_STR, SPIDER_SQL_FIRST_LEN);
      memset((char *) sql->ptr() + sql->length(), ' ',
        result_list->ha_where_pos - result_list->ha_next_pos -
        SPIDER_SQL_FIRST_LEN);
    }
  }
  result_list->ha_sql.length(result_list->ha_limit_pos);
  result_list->limit_num =
    result_list->internal_limit >= result_list->split_read ?
    result_list->split_read : result_list->internal_limit;
  if (
    (error_num = spider_db_append_key_order(spider, FALSE)) ||
    (error_num = spider_db_append_limit(
      spider, &result_list->sql, result_list->internal_offset,
      result_list->limit_num)) ||
    (
      !result_list->use_union &&
      (error_num = spider_db_append_select_lock(spider))
    )
  )
    DBUG_RETURN(error_num);

  int roop_start, roop_end, roop_count, lock_mode, link_ok;
  lock_mode = spider_conn_lock_mode(spider);
  if (lock_mode)
  {
    /* "for update" or "lock in share mode" */
    link_ok = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_OK);
    roop_start = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_end = spider->share->link_count;
  } else {
    link_ok = link_idx;
    roop_start = link_idx;
    roop_end = link_idx + 1;
  }
  for (roop_count = roop_start; roop_count < roop_end;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
    if (spider->sql_kind[roop_count] == SPIDER_SQL_KIND_SQL)
    {
      if (spider->share->same_db_table_name)
        sql = &result_list->sql;
      else {
        sql = &result_list->sqls[roop_count];
        if (sql->copy(result_list->sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_pos = sql->length();
        sql->length(result_list->table_name_pos);
        spider_db_append_table_name_with_adjusting(spider, sql,
          spider->share, roop_count, SPIDER_SQL_KIND_SQL);
        sql->length(tmp_pos);
      }
    } else {
      if (spider->m_handler_id[roop_count] ==
        result_list->ha_sql_handler_id)
        sql = &result_list->ha_sql;
      else {
        sql = &result_list->sqls[roop_count];
        if (sql->copy(result_list->ha_sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_pos = sql->length();
        sql->length(result_list->ha_table_name_pos);
        spider_db_append_table_name_with_adjusting(spider, sql,
          spider->share, roop_count, SPIDER_SQL_KIND_HANDLER);
        sql->length(tmp_pos);
      }
    }
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[roop_count];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    if ((error_num = spider_db_set_names(spider, conn, roop_count)))
    {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      if (
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            (uint32) share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            spider->conn_link_idx[roop_count],
            NULL,
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
    spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
      share);
    if (spider_db_query(
      conn,
      sql->ptr(),
      sql->length(),
      &spider->need_mons[roop_count])
    ) {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      error_num = spider_db_errorno(conn);
      if (
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            (uint32) share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            spider->conn_link_idx[roop_count],
            NULL,
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    if (roop_count == link_ok)
    {
      if ((error_num = spider_db_store_result(spider, roop_count, table)))
      {
        if (
          error_num != HA_ERR_END_OF_FILE &&
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      spider->result_link_idx = link_ok;
    } else {
      spider_db_discard_result(conn);
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
  DBUG_RETURN(spider_db_fetch(buf, spider, table));
}

int spider_db_seek_first(
  uchar *buf,
  ha_spider *spider,
  TABLE *table
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_seek_first");
  if (
    result_list->current != result_list->first &&
    result_list->low_mem_read == 1
  ) {
    my_message(ER_SPIDER_LOW_MEM_READ_PREV_NUM, ER_SPIDER_LOW_MEM_READ_PREV_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_LOW_MEM_READ_PREV_NUM);
  }
  result_list->current = result_list->first;
  spider_db_set_pos_to_first_row(result_list);
  DBUG_RETURN(spider_db_fetch(buf, spider, table));
}

void spider_db_set_pos_to_first_row(
  SPIDER_RESULT_LIST *result_list
) {
  DBUG_ENTER("spider_db_set_pos_to_first_row");
  result_list->current_row_num = 0;
  if (result_list->quick_mode == 0)
    result_list->current->result->data_cursor =
      result_list->current->first_row;
  DBUG_VOID_RETURN;
}

void spider_db_create_position(
  ha_spider *spider,
  SPIDER_POSITION *pos
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_RESULT *current = (SPIDER_RESULT*) result_list->current;
  DBUG_ENTER("spider_db_create_position");
  if (result_list->quick_mode == 0)
  {
    SPIDER_DB_RESULT *result = current->result;
    pos->row = result->current_row;
    pos->lengths = NULL;
  } else {
    if (result_list->current_row_num < result_list->quick_page_size)
    {
      SPIDER_POSITION *tmp_pos =
        &current->first_position[result_list->current_row_num - 1];
      memcpy(pos, tmp_pos, sizeof(SPIDER_POSITION));
    } else {
      TABLE *tmp_tbl = current->result_tmp_tbl;
      pos->row = NULL;
      pos->lengths = NULL;
      tmp_tbl->file->ref = (uchar *) &pos->tmp_tbl_pos;
      tmp_tbl->file->position(tmp_tbl->record[0]);
    }
  }
  current->use_position = TRUE;
  pos->use_position = TRUE;
  pos->mrr_with_cnt = spider->mrr_with_cnt;
  pos->sql_kind = spider->sql_kind[spider->result_link_idx];
  pos->position_bitmap = spider->position_bitmap;
  pos->ft_first = spider->ft_first;
  pos->ft_current = spider->ft_current;
  pos->result = current;
  DBUG_VOID_RETURN;
}

int spider_db_seek_tmp(
  uchar *buf,
  SPIDER_POSITION *pos,
  ha_spider *spider,
  TABLE *table
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_seek_tmp");
  if (spider->sql_kind[spider->result_link_idx] == SPIDER_SQL_KIND_SQL)
  {
    if (!spider->select_column_mode)
    {
      if (result_list->keyread)
        error_num = spider_db_seek_tmp_key(buf, pos, spider, table,
          result_list->key_info);
      else
        error_num = spider_db_seek_tmp_table(buf, pos, spider, table);
    } else
      error_num = spider_db_seek_tmp_minimum_columns(buf, pos, spider, table);
  } else
    error_num = spider_db_seek_tmp_table(buf, pos, spider, table);

  DBUG_PRINT("info",("spider error_num=%d", error_num));
  DBUG_RETURN(error_num);
}

int spider_db_seek_tmp_table(
  uchar *buf,
  SPIDER_POSITION *pos,
  ha_spider *spider,
  TABLE *table
) {
  int error_num;
  ulong *lengths = pos->lengths;
  Field **field;
  SPIDER_DB_ROW row = pos->row;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  DBUG_ENTER("spider_db_seek_tmp_table");
  if (!row)
  {
    if ((error_num = spider_db_get_row_and_lengths_from_tmp_tbl_pos(pos, &row,
      &lengths)))
      DBUG_RETURN(error_num);
  } else if (!lengths)
  {
    SPIDER_DB_RESULT *result = pos->result->result;
    result->current_row = row;
    lengths = mysql_fetch_lengths(result);
  }

  /* for mrr */
  if (pos->mrr_with_cnt)
  {
    DBUG_PRINT("info", ("spider mrr_with_cnt"));
    if (pos->sql_kind == SPIDER_SQL_KIND_SQL)
    {
      row++;
      lengths++;
    }
  }
  if ((error_num = spider_db_append_match_fetch(spider,
    pos->ft_first, pos->ft_current, &row, &lengths)))
    DBUG_RETURN(error_num);

  for (
    field = table->field;
    *field;
    field++,
    lengths++
  ) {
    if ((
      bitmap_is_set(table->read_set, (*field)->field_index) |
      bitmap_is_set(table->write_set, (*field)->field_index)
    )) {
#ifndef DBUG_OFF
      my_bitmap_map *tmp_map =
        dbug_tmp_use_all_columns(table, table->write_set);
#endif
      DBUG_PRINT("info", ("spider bitmap is set %s", (*field)->field_name));
      if ((error_num =
        spider_db_fetch_row(spider->share, *field, row, lengths, ptr_diff)))
        DBUG_RETURN(error_num);
#ifndef DBUG_OFF
      dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
    }
    row++;
  }
  DBUG_RETURN(0);
}

int spider_db_seek_tmp_key(
  uchar *buf,
  SPIDER_POSITION *pos,
  ha_spider *spider,
  TABLE *table,
  const KEY *key_info
) {
  int error_num;
  KEY_PART_INFO *key_part;
  uint part_num;
  SPIDER_DB_ROW row = pos->row;
  ulong *lengths = pos->lengths;
  Field *field;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  DBUG_ENTER("spider_db_seek_tmp_key");
  if (!row)
  {
    if ((error_num = spider_db_get_row_and_lengths_from_tmp_tbl_pos(pos, &row,
      &lengths)))
      DBUG_RETURN(error_num);
  } else if (!lengths)
  {
    SPIDER_DB_RESULT *result = pos->result->result;
    result->current_row = row;
    lengths = mysql_fetch_lengths(result);
  }

  /* for mrr */
  if (pos->mrr_with_cnt)
  {
    DBUG_PRINT("info", ("spider mrr_with_cnt"));
    row++;
    lengths++;
  }
  if ((error_num = spider_db_append_match_fetch(spider,
    pos->ft_first, pos->ft_current, &row, &lengths)))
    DBUG_RETURN(error_num);

  for (
    key_part = key_info->key_part,
    part_num = 0;
    part_num < key_info->key_parts;
    key_part++,
    part_num++,
    lengths++
  ) {
    field = key_part->field;
    if ((
      bitmap_is_set(table->read_set, field->field_index) |
      bitmap_is_set(table->write_set, field->field_index)
    )) {
#ifndef DBUG_OFF
      my_bitmap_map *tmp_map =
        dbug_tmp_use_all_columns(table, table->write_set);
#endif
      DBUG_PRINT("info", ("spider bitmap is set %s", field->field_name));
      if ((error_num =
        spider_db_fetch_row(spider->share, field, row, lengths, ptr_diff)))
        DBUG_RETURN(error_num);
#ifndef DBUG_OFF
      dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
    }
    row++;
  }
  DBUG_RETURN(0);
}

int spider_db_seek_tmp_minimum_columns(
  uchar *buf,
  SPIDER_POSITION *pos,
  ha_spider *spider,
  TABLE *table
) {
  int error_num;
  ulong *lengths = pos->lengths;
  Field **field;
  SPIDER_DB_ROW row = pos->row;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  DBUG_ENTER("spider_db_seek_tmp_minimum_columns");
  if (!row)
  {
    if ((error_num = spider_db_get_row_and_lengths_from_tmp_tbl_pos(pos, &row,
      &lengths)))
      DBUG_RETURN(error_num);
  } else if (!lengths)
  {
    SPIDER_DB_RESULT *result = pos->result->result;
    result->current_row = row;
    lengths = mysql_fetch_lengths(result);
  }

  /* for mrr */
  if (pos->mrr_with_cnt)
  {
    DBUG_PRINT("info", ("spider mrr_with_cnt"));
    row++;
    lengths++;
  }
  if ((error_num = spider_db_append_match_fetch(spider,
    pos->ft_first, pos->ft_current, &row, &lengths)))
    DBUG_RETURN(error_num);

  for (
    field = table->field;
    *field;
    field++
  ) {
    DBUG_PRINT("info", ("spider field_index %u", (*field)->field_index));
    if (spider_bit_is_set(pos->position_bitmap, (*field)->field_index))
    {
/*
    if ((
      bitmap_is_set(table->read_set, (*field)->field_index) |
      bitmap_is_set(table->write_set, (*field)->field_index)
    )) {
      DBUG_PRINT("info", ("spider read_set %u",
        bitmap_is_set(table->read_set, (*field)->field_index)));
      DBUG_PRINT("info", ("spider write_set %u",
        bitmap_is_set(table->write_set, (*field)->field_index)));
*/
#ifndef DBUG_OFF
      my_bitmap_map *tmp_map =
        dbug_tmp_use_all_columns(table, table->write_set);
#endif
      DBUG_PRINT("info", ("spider bitmap is set %s", (*field)->field_name));
      if ((error_num =
        spider_db_fetch_row(spider->share, *field, row, lengths, ptr_diff)))
        DBUG_RETURN(error_num);
      row++;
      lengths++;
#ifndef DBUG_OFF
      dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
    }
    else if (bitmap_is_set(table->read_set, (*field)->field_index))
    {
      DBUG_PRINT("info", ("spider bitmap is cleared %s",
        (*field)->field_name));
      bitmap_clear_bit(table->read_set, (*field)->field_index);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_show_table_status(
  ha_spider *spider,
  int link_idx,
  int sts_mode
) {
  int error_num;
  SPIDER_CONN *conn = spider->conns[link_idx];
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row;
  SPIDER_SHARE *share = spider->share;
  MYSQL_TIME mysql_time;
#ifdef MARIADB_BASE_VERSION
  uint not_used_uint;
#else
  my_bool not_used_my_bool;
#endif
  int not_used_int;
  long not_used_long;
  DBUG_ENTER("spider_db_show_table_status");
  DBUG_PRINT("info",("spider sts_mode=%d", sts_mode));
  if (sts_mode == 1)
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[link_idx];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    spider_conn_queue_connect_rewrite(share, conn, link_idx);
    spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
      share);
    if (
      (error_num = spider_db_set_names(spider, conn, link_idx)) ||
      (
        spider_db_query(
          conn,
          share->show_table_status[0 + (2 * spider->conn_link_idx[link_idx])].
            ptr(),
          share->show_table_status[0 + (2 * spider->conn_link_idx[link_idx])].
            length(),
          &spider->need_mons[link_idx]) &&
        (error_num = spider_db_errorno(conn))
      )
    ) {
      if (
        error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
        !conn->disable_reconnect
      ) {
        /* retry */
        if ((error_num = spider_db_ping(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        if ((error_num = spider_db_set_names(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
          share);
        if (spider_db_query(
          conn,
          share->show_table_status[0 + (2 * spider->conn_link_idx[link_idx])].
            ptr(),
          share->show_table_status[0 + (2 * spider->conn_link_idx[link_idx])].
            length(),
          &spider->need_mons[link_idx])
        ) {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          DBUG_RETURN(spider_db_errorno(conn));
        }
      } else {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
    }
    if (
      !(res = mysql_store_result(conn->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if (res)
        mysql_free_result(res);
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      if ((error_num = spider_db_errorno(conn)))
        DBUG_RETURN(error_num);
      else {
        my_printf_error(ER_SPIDER_REMOTE_TABLE_NOT_FOUND_NUM,
          ER_SPIDER_REMOTE_TABLE_NOT_FOUND_STR, MYF(0),
          spider->share->db_names_str[spider->conn_link_idx[link_idx]].ptr(),
          spider->share->table_names_str[spider->conn_link_idx[
            link_idx]].ptr());
        DBUG_RETURN(ER_SPIDER_REMOTE_TABLE_NOT_FOUND_NUM);
      }
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    if (mysql_num_fields(res) != 18)
    {
      mysql_free_result(res);
      DBUG_RETURN(ER_QUERY_ON_FOREIGN_DATA_SOURCE);
    }

    if (row[4])
      share->records =
        (ha_rows) my_strtoll10(row[4], (char**) NULL, &error_num);
    else
      share->records = (ha_rows) 0;
    DBUG_PRINT("info",
      ("spider records=%lld", share->records));
    if (row[5])
      share->mean_rec_length =
        (ulong) my_strtoll10(row[5], (char**) NULL, &error_num);
    else
      share->mean_rec_length = 0;
    DBUG_PRINT("info",
      ("spider mean_rec_length=%lu", share->mean_rec_length));
    if (row[6])
      share->data_file_length =
        (ulonglong) my_strtoll10(row[6], (char**) NULL, &error_num);
    else
      share->data_file_length = 0;
    DBUG_PRINT("info",
      ("spider data_file_length=%lld", share->data_file_length));
    if (row[7])
      share->max_data_file_length =
        (ulonglong) my_strtoll10(row[7], (char**) NULL, &error_num);
    else
      share->max_data_file_length = 0;
    DBUG_PRINT("info",
      ("spider max_data_file_length=%lld", share->max_data_file_length));
    if (row[8])
      share->index_file_length =
        (ulonglong) my_strtoll10(row[8], (char**) NULL, &error_num);
    else
      share->index_file_length = 0;
    DBUG_PRINT("info",
      ("spider index_file_length=%lld", share->index_file_length));
    if (row[10])
      share->auto_increment_value =
        (ulonglong) my_strtoll10(row[10], (char**) NULL, &error_num);
    else
      share->auto_increment_value = 1;
    DBUG_PRINT("info",
      ("spider auto_increment_value=%lld", share->auto_increment_value));
    if (row[11])
    {
      str_to_datetime(row[11], strlen(row[11]), &mysql_time, 0,
        &not_used_int);
#ifdef MARIADB_BASE_VERSION
      share->create_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_uint);
#else
      share->create_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
#endif
    } else
      share->create_time = (time_t) 0;
#ifndef DBUG_OFF
    {
      struct tm *ts, tmp_ts;
      char buf[80];
      ts = localtime_r(&share->create_time, &tmp_ts);
      strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", ts);
      DBUG_PRINT("info",("spider create_time=%s", buf));
    }
#endif
    if (row[12])
    {
      str_to_datetime(row[12], strlen(row[12]), &mysql_time, 0,
        &not_used_int);
#ifdef MARIADB_BASE_VERSION
      share->update_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_uint);
#else
      share->update_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
#endif
    } else
      share->update_time = (time_t) 0;
#ifndef DBUG_OFF
    {
      struct tm *ts, tmp_ts;
      char buf[80];
      ts = localtime_r(&share->update_time, &tmp_ts);
      strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", ts);
      DBUG_PRINT("info",("spider update_time=%s", buf));
    }
#endif
    if (row[13])
    {
      str_to_datetime(row[13], strlen(row[13]), &mysql_time, 0,
        &not_used_int);
#ifdef MARIADB_BASE_VERSION
      share->check_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_uint);
#else
      share->check_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
#endif
    } else
      share->check_time = (time_t) 0;
#ifndef DBUG_OFF
    {
      struct tm *ts, tmp_ts;
      char buf[80];
      ts = localtime_r(&share->check_time, &tmp_ts);
      strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", ts);
      DBUG_PRINT("info",("spider check_time=%s", buf));
    }
#endif
    mysql_free_result(res);
  } else {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[link_idx];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    spider_conn_queue_connect_rewrite(share, conn, link_idx);
    spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
      share);
    if (
      (error_num = spider_db_set_names(spider, conn, link_idx)) ||
      (
        spider_db_query(
          conn,
          share->show_table_status[1 + (2 * link_idx)].ptr(),
          share->show_table_status[1 + (2 * link_idx)].length(),
          &spider->need_mons[link_idx]) &&
        (error_num = spider_db_errorno(conn))
      )
    ) {
      if (
        error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
        !conn->disable_reconnect
      ) {
        /* retry */
        if ((error_num = spider_db_ping(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        if ((error_num = spider_db_set_names(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
          share);
        if (spider_db_query(
          conn,
          share->show_table_status[1 + (2 * link_idx)].ptr(),
          share->show_table_status[1 + (2 * link_idx)].length(),
          &spider->need_mons[link_idx])
        ) {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          DBUG_RETURN(spider_db_errorno(conn));
        }
      } else {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
    }
    if (
      !(res = mysql_store_result(conn->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if (res)
        mysql_free_result(res);
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      if ((error_num = spider_db_errorno(conn)))
        DBUG_RETURN(error_num);
      else
        DBUG_RETURN(ER_QUERY_ON_FOREIGN_DATA_SOURCE);
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);

    if (row[0])
      share->records =
        (ha_rows) my_strtoll10(row[0], (char**) NULL, &error_num);
    else
      share->records = (ha_rows) 0;
    DBUG_PRINT("info",
      ("spider records=%lld", share->records));
    if (row[1])
      share->mean_rec_length =
        (ulong) my_strtoll10(row[1], (char**) NULL, &error_num);
    else
      share->mean_rec_length = 0;
    DBUG_PRINT("info",
      ("spider mean_rec_length=%lu", share->mean_rec_length));
    if (row[2])
      share->data_file_length =
        (ulonglong) my_strtoll10(row[2], (char**) NULL, &error_num);
    else
      share->data_file_length = 0;
    DBUG_PRINT("info",
      ("spider data_file_length=%lld", share->data_file_length));
    if (row[3])
      share->max_data_file_length =
        (ulonglong) my_strtoll10(row[3], (char**) NULL, &error_num);
    else
      share->max_data_file_length = 0;
    DBUG_PRINT("info",
      ("spider max_data_file_length=%lld", share->max_data_file_length));
    if (row[4])
      share->index_file_length =
        (ulonglong) my_strtoll10(row[4], (char**) NULL, &error_num);
    else
      share->index_file_length = 0;
    DBUG_PRINT("info",
      ("spider index_file_length=%lld", share->index_file_length));
    if (row[5])
      share->auto_increment_value =
        (ulonglong) my_strtoll10(row[5], (char**) NULL, &error_num);
    else
      share->auto_increment_value = 1;
    DBUG_PRINT("info",
      ("spider auto_increment_value=%lld", share->auto_increment_value));
    if (row[6])
    {
      str_to_datetime(row[6], strlen(row[6]), &mysql_time, 0,
        &not_used_int);
#ifdef MARIADB_BASE_VERSION
      share->create_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_uint);
#else
      share->create_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
#endif
    } else
      share->create_time = (time_t) 0;
#ifndef DBUG_OFF
    {
      struct tm *ts, tmp_ts;
      char buf[80];
      ts = localtime_r(&share->create_time, &tmp_ts);
      strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", ts);
      DBUG_PRINT("info",("spider create_time=%s", buf));
    }
#endif
    if (row[7])
    {
      str_to_datetime(row[7], strlen(row[7]), &mysql_time, 0,
        &not_used_int);
#ifdef MARIADB_BASE_VERSION
      share->update_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_uint);
#else
      share->update_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
#endif
    } else
      share->update_time = (time_t) 0;
#ifndef DBUG_OFF
    {
      struct tm *ts, tmp_ts;
      char buf[80];
      ts = localtime_r(&share->update_time, &tmp_ts);
      strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", ts);
      DBUG_PRINT("info",("spider update_time=%s", buf));
    }
#endif
    if (row[8])
    {
      str_to_datetime(row[8], strlen(row[8]), &mysql_time, 0,
        &not_used_int);
#ifdef MARIADB_BASE_VERSION
      share->check_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_uint);
#else
      share->check_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
#endif
    } else
      share->check_time = (time_t) 0;
#ifndef DBUG_OFF
    {
      struct tm *ts, tmp_ts;
      char buf[80];
      ts = localtime_r(&share->check_time, &tmp_ts);
      strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", ts);
      DBUG_PRINT("info",("spider check_time=%s", buf));
    }
#endif
    mysql_free_result(res);
  }
  DBUG_RETURN(0);
}

int spider_db_show_records(
  ha_spider *spider,
  int link_idx
) {
  int error_num;
  SPIDER_CONN *conn = spider->conns[link_idx];
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_show_records");
  pthread_mutex_lock(&conn->mta_conn_mutex);
  SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  spider_conn_queue_connect_rewrite(share, conn, link_idx);
  spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
    share);
  if (
    (error_num = spider_db_set_names(spider, conn, link_idx)) ||
    (
      spider_db_query(
        conn,
        share->show_records[link_idx].ptr(),
        share->show_records[link_idx].length(),
        &spider->need_mons[link_idx]) &&
      (error_num = spider_db_errorno(conn))
    )
  ) {
    if (
      error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
      !conn->disable_reconnect
    ) {
      /* retry */
      if ((error_num = spider_db_ping(spider, conn, link_idx)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
      if ((error_num = spider_db_set_names(spider, conn, link_idx)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
        share);
      if (spider_db_query(
        conn,
        share->show_records[link_idx].ptr(),
        share->show_records[link_idx].length(),
        &spider->need_mons[link_idx])
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        DBUG_RETURN(spider_db_errorno(conn));
      }
    } else {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(error_num);
    }
  }
  if (
    !(res = mysql_store_result(conn->db_conn)) ||
    !(row = mysql_fetch_row(res))
  ) {
    if (res)
      mysql_free_result(res);
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    if ((error_num = spider_db_errorno(conn)))
      DBUG_RETURN(error_num);
    else
      DBUG_RETURN(ER_QUERY_ON_FOREIGN_DATA_SOURCE);
  }
  conn->mta_conn_mutex_lock_already = FALSE;
  conn->mta_conn_mutex_unlock_later = FALSE;
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);

  if (row[0])
  {
    share->records =
      (ha_rows) my_strtoll10(row[0], (char**) NULL, &error_num);
    spider->trx->direct_aggregate_count++;
  } else
    share->records = (ha_rows) 0;
  DBUG_PRINT("info",
    ("spider records=%lld", share->records));
  mysql_free_result(res);
  DBUG_RETURN(0);
}

void spider_db_set_cardinarity(
  ha_spider *spider,
  TABLE *table
) {
  int roop_count, roop_count2;
  SPIDER_SHARE *share = spider->share;
  KEY *key_info;
  KEY_PART_INFO *key_part;
  Field *field;
  ha_rows rec_per_key;
  DBUG_ENTER("spider_db_set_cardinarity");
  for (roop_count = 0; roop_count < (int) table->s->keys; roop_count++)
  {
    key_info = &table->key_info[roop_count];
    for (roop_count2 = 0; roop_count2 < (int) key_info->key_parts;
      roop_count2++)
    {
      key_part = &key_info->key_part[roop_count2];
      field = key_part->field;
      rec_per_key = (ha_rows) share->records /
        share->cardinality[field->field_index];
      if (rec_per_key > ~(ulong) 0)
        key_info->rec_per_key[roop_count2] = ~(ulong) 0;
      else if (rec_per_key == 0)
        key_info->rec_per_key[roop_count2] = 1;
      else
        key_info->rec_per_key[roop_count2] = (ulong) rec_per_key;
      DBUG_PRINT("info",
        ("spider column id=%d", field->field_index));
      DBUG_PRINT("info",
        ("spider cardinality=%lld",
        share->cardinality[field->field_index]));
      DBUG_PRINT("info",
        ("spider rec_per_key=%lu",
        key_info->rec_per_key[roop_count2]));
    }
  }
  DBUG_VOID_RETURN;
}

int spider_db_show_index(
  ha_spider *spider,
  int link_idx,
  TABLE *table,
  int crd_mode
) {
  int error_num;
  uint num_fields;
  SPIDER_CONN *conn = spider->conns[link_idx];
  SPIDER_SHARE *share = spider->share;
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row = NULL;
  Field *field;
  int roop_count;
  longlong *tmp_cardinality;
  DBUG_ENTER("spider_db_show_index");
  DBUG_PRINT("info",("spider crd_mode=%d", crd_mode));
  if (crd_mode == 1)
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[link_idx];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    spider_conn_queue_connect_rewrite(share, conn, link_idx);
    spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
      share);
    if (
      (error_num = spider_db_set_names(spider, conn, link_idx)) ||
      (
        spider_db_query(
          conn,
          share->show_index[0 + (2 * link_idx)].ptr(),
          share->show_index[0 + (2 * link_idx)].length(),
          &spider->need_mons[link_idx]) &&
        (error_num = spider_db_errorno(conn))
      )
    ) {
      if (
        error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
        !conn->disable_reconnect
      ) {
        /* retry */
        if ((error_num = spider_db_ping(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        if ((error_num = spider_db_set_names(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
          share);
        if (spider_db_query(
          conn,
          share->show_index[0 + (2 * link_idx)].ptr(),
          share->show_index[0 + (2 * link_idx)].length(),
          &spider->need_mons[link_idx])
        ) {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          DBUG_RETURN(spider_db_errorno(conn));
        }
      } else {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
    }
    if (
      !(res = mysql_store_result(conn->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if ((error_num = spider_db_errorno(conn)))
      {
        if (res)
          mysql_free_result(res);
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
      /* no record is ok */
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    num_fields = mysql_num_fields(res);
    if (num_fields < 12 || num_fields > 13)
    {
      mysql_free_result(res);
      DBUG_RETURN(ER_QUERY_ON_FOREIGN_DATA_SOURCE);
    }

    memset((uchar *) share->cardinality_upd, 0,
      sizeof(uchar) * share->bitmap_size);
    while (row)
    {
      if (
        row[4] &&
        row[6] &&
        (field = find_field_in_table_sef(table, row[4]))
      ) {
        if ((share->cardinality[field->field_index] =
          (longlong) my_strtoll10(row[6], (char**) NULL, &error_num)) <= 0)
          share->cardinality[field->field_index] = 1;
        spider_set_bit(share->cardinality_upd, field->field_index);
        DBUG_PRINT("info",
          ("spider col_name=%s", row[4]));
        DBUG_PRINT("info",
          ("spider cardinality=%lld",
          share->cardinality[field->field_index]));
      } else if (row[4])
      {
        DBUG_PRINT("info",
          ("spider skip col_name=%s", row[4]));
      } else {
        mysql_free_result(res);
        DBUG_RETURN(ER_QUERY_ON_FOREIGN_DATA_SOURCE);
      }
      row = mysql_fetch_row(res);
    }
    for (roop_count = 0, tmp_cardinality = share->cardinality;
      roop_count < (int) table->s->fields;
      roop_count++, tmp_cardinality++)
    {
      if (!spider_bit_is_set(share->cardinality_upd, roop_count))
      {
        DBUG_PRINT("info",
          ("spider init column cardinality id=%d", roop_count));
        *tmp_cardinality = 1;
      }
    }
    if (res)
      mysql_free_result(res);
  } else {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[link_idx];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    spider_conn_queue_connect_rewrite(share, conn, link_idx);
    spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
      share);
    if (
      (error_num = spider_db_set_names(spider, conn, link_idx)) ||
      (
        spider_db_query(
          conn,
          share->show_index[1 + (2 * link_idx)].ptr(),
          share->show_index[1 + (2 * link_idx)].length(),
          &spider->need_mons[link_idx]) &&
        (error_num = spider_db_errorno(conn))
      )
    ) {
      if (
        error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
        !conn->disable_reconnect
      ) {
        /* retry */
        if ((error_num = spider_db_ping(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        if ((error_num = spider_db_set_names(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
          share);
        if (spider_db_query(
          conn,
          share->show_index[1 + (2 * link_idx)].ptr(),
          share->show_index[1 + (2 * link_idx)].length(),
          &spider->need_mons[link_idx])
        ) {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          DBUG_RETURN(spider_db_errorno(conn));
        }
      } else {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
    }
    if (
      !(res = mysql_store_result(conn->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if ((error_num = spider_db_errorno(conn)))
      {
        if (res)
          mysql_free_result(res);
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
      /* no record is ok */
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);

    memset((uchar *) share->cardinality_upd, 0,
      sizeof(uchar) * share->bitmap_size);
    while (row)
    {
      if (
        row[0] &&
        row[1] &&
        (field = find_field_in_table_sef(table, row[0]))
      ) {
        if ((share->cardinality[field->field_index] =
          (longlong) my_strtoll10(row[1], (char**) NULL, &error_num)) <= 0)
          share->cardinality[field->field_index] = 1;
        spider_set_bit(share->cardinality_upd, field->field_index);
        DBUG_PRINT("info",
          ("spider col_name=%s", row[0]));
        DBUG_PRINT("info",
          ("spider cardinality=%lld",
          share->cardinality[field->field_index]));
      } else if (row[0])
      {
        DBUG_PRINT("info",
          ("spider skip col_name=%s", row[0]));
      } else {
        mysql_free_result(res);
        DBUG_RETURN(ER_QUERY_ON_FOREIGN_DATA_SOURCE);
      }
      row = mysql_fetch_row(res);
    }
    for (roop_count = 0, tmp_cardinality = share->cardinality;
      roop_count < (int) table->s->fields;
      roop_count++, tmp_cardinality++)
    {
      if (!spider_bit_is_set(share->cardinality_upd, roop_count))
      {
        DBUG_PRINT("info",
          ("spider init column cardinality id=%d", roop_count));
        *tmp_cardinality = 1;
      }
    }
    if (res)
      mysql_free_result(res);
  }
  spider_db_set_cardinarity(spider, table);
  DBUG_RETURN(0);
}

ha_rows spider_db_explain_select(
  key_range *start_key,
  key_range *end_key,
  ha_spider *spider,
  int link_idx
) {
  int error_num;
  SPIDER_CONN *conn = spider->conns[link_idx];
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sqls[link_idx];
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row;
  ha_rows rows;
  DBUG_ENTER("spider_db_explain_select");
  str->length(0);
  if (str->reserve(SPIDER_SQL_EXPLAIN_SELECT_LEN))
  {
    my_errno = HA_ERR_OUT_OF_MEM;
    DBUG_RETURN(HA_POS_ERROR);
  }
  str->q_append(SPIDER_SQL_EXPLAIN_SELECT_STR, SPIDER_SQL_EXPLAIN_SELECT_LEN);
  if (
    (error_num = spider_db_append_from(str, spider, link_idx)) ||
    (error_num = spider_db_append_key_where(start_key, end_key, spider))
  ) {
    my_errno = error_num;
    DBUG_RETURN(HA_POS_ERROR);
  }

  pthread_mutex_lock(&conn->mta_conn_mutex);
  SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  spider_conn_queue_connect_rewrite(spider->share, conn, link_idx);
  spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
    spider->share);
  if (
    (error_num = spider_db_set_names(spider, conn, link_idx)) ||
    (
      spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        &spider->need_mons[link_idx]) &&
      (error_num = spider_db_errorno(conn))
    )
  ) {
    if (
      error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
      !conn->disable_reconnect
    ) {
      /* retry */
      if ((error_num = spider_db_ping(spider, conn, link_idx)))
      {
        if (spider->check_error_mode(error_num))
          my_errno = error_num;
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(HA_POS_ERROR);
      }
      if ((error_num = spider_db_set_names(spider, conn, link_idx)))
      {
        if (spider->check_error_mode(error_num))
          my_errno = error_num;
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(HA_POS_ERROR);
      }
      spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
        spider->share);
      if (spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        &spider->need_mons[link_idx])
      ) {
        error_num = spider_db_errorno(conn);
        if (spider->check_error_mode(error_num))
          my_errno = error_num;
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(HA_POS_ERROR);
      }
    } else {
      if (spider->check_error_mode(error_num))
        my_errno = error_num;
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(HA_POS_ERROR);
    }
  }
  if (
    !(res = mysql_store_result(conn->db_conn)) ||
    !(row = mysql_fetch_row(res))
  ) {
    if (res)
      mysql_free_result(res);
    if ((error_num = spider_db_errorno(conn)))
    {
      if (spider->check_error_mode(error_num))
        my_errno = error_num;
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(HA_POS_ERROR);
    } else {
      my_errno = ER_QUERY_ON_FOREIGN_DATA_SOURCE;
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(HA_POS_ERROR);
    }
  }
  conn->mta_conn_mutex_lock_already = FALSE;
  conn->mta_conn_mutex_unlock_later = FALSE;
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  if (mysql_num_fields(res) != 10)
  {
    mysql_free_result(res);
    my_errno = ER_QUERY_ON_FOREIGN_DATA_SOURCE;
    DBUG_RETURN(HA_POS_ERROR);
  }

  if (row[8])
  {
    rows = (ha_rows) my_strtoll10(row[8], (char**) NULL, &error_num);
  } else
    rows = 0;
  mysql_free_result(res);
  DBUG_RETURN(rows);
}

int spider_db_bulk_insert_init(
  ha_spider *spider,
  const TABLE *table
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  spider_string *str;
  DBUG_ENTER("spider_db_bulk_insert_init");
  spider->sql_kinds = 0;
  spider->result_list.sql.length(0);
  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    if (spider->conns[roop_count])
      spider->conns[roop_count]->ignore_dup_key = spider->ignore_dup_key;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (
      spider_conn_use_handler(spider, spider->lock_mode, roop_count) &&
      (
        !spider->handler_opened(roop_count, SPIDER_CONN_KIND_HS_WRITE) ||
        spider->hs_w_conns[roop_count]->server_lost
      )
    ) {
      if ((error_num = spider_db_open_handler(spider,
        spider->hs_w_conns[roop_count], roop_count)))
      {
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      spider->set_handler_opened(roop_count);
    }
#endif
  }
  spider->result_list.insert_pos = 0;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
  {
#endif
    str = &spider->result_list.insert_sql;
    str->length(0);
    if (
      (error_num = spider_db_append_insert(spider)) ||
      (error_num = spider_db_append_into(spider, table, 0))
    )
      DBUG_RETURN(error_num);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  }
  if (spider->sql_kinds & SPIDER_SQL_KIND_HS)
  {
    spider->result_list.hs_upd_rows = 0;
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_bulk_insert(
  ha_spider *spider,
  TABLE *table,
  bool bulk_end
) {
  int error_num, tmp_pos, first_insert_link_idx = -1;
  bool add_value = FALSE;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  spider_string *str = &result_list->insert_sql, *sql;
  Field **field;
  THD *thd = spider->trx->thd;
  DBUG_ENTER("spider_db_bulk_insert");
  DBUG_PRINT("info",("spider str->length()=%d", str->length()));
  DBUG_PRINT("info",("spider result_list->insert_pos=%d",
    result_list->insert_pos));

  if (!bulk_end)
  {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
    {
#endif
      if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
      {
        str->length(0);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
      str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    }
    result_list->hs_adding_keys = FALSE;
#endif
#ifndef DBUG_OFF
    my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table, table->read_set);
#endif
    for (field = table->field; *field; field++)
    {
      DBUG_PRINT("info",("spider field_index=%u", (*field)->field_index));
      if (
        bitmap_is_set(table->write_set, (*field)->field_index) ||
        bitmap_is_set(table->read_set, (*field)->field_index)
      ) {
        add_value = TRUE;
        DBUG_PRINT("info",("spider is_null()=%s",
          (*field)->is_null() ? "TRUE" : "FALSE"));
        DBUG_PRINT("info",("spider table->next_number_field=%p",
          table->next_number_field));
        DBUG_PRINT("info",("spider *field=%p", *field));
        DBUG_PRINT("info",("spider force_auto_increment=%s",
          (table->next_number_field && spider->force_auto_increment) ?
          "TRUE" : "FALSE"));
        if (
          (*field)->is_null() ||
          (
            table->next_number_field == *field &&
            !table->auto_increment_field_not_null &&
            !spider->force_auto_increment
          )
        ) {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
          if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
          {
#endif
            if (str->reserve(SPIDER_SQL_NULL_LEN + SPIDER_SQL_COMMA_LEN))
            {
#ifndef DBUG_OFF
              dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
              str->length(0);
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            }
            str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
          }
          if (spider->sql_kinds & SPIDER_SQL_KIND_HS)
          {
#ifndef HANDLERSOCKET_MYSQL_UTIL
            spider->result_list.hs_upds.push_back(SPIDER_HS_STRING_REF());
#else
            SPIDER_HS_STRING_REF string_r = SPIDER_HS_STRING_REF();
            uint old_elements = spider->result_list.hs_upds.max_element;
            if (
              insert_dynamic(&spider->result_list.hs_upds,
                (uchar *) &string_r)
            )
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            if (spider->result_list.hs_upds.max_element > old_elements)
            {
              spider_alloc_calc_mem(spider_current_trx,
                spider->result_list.hs_upds,
                (spider->result_list.hs_upds.max_element - old_elements) *
                spider->result_list.hs_upds.size_of_element);
            }
#endif
          }
#endif
        } else {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
          if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
          {
#endif
            if (
              spider_db_append_column_value(spider, share, str, *field,
                NULL) ||
              str->reserve(SPIDER_SQL_COMMA_LEN)
            ) {
#ifndef DBUG_OFF
              dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
              str->length(0);
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
          }
          if (spider->sql_kinds & SPIDER_SQL_KIND_HS)
          {
            spider_db_append_column_value(spider, share, NULL, *field, NULL);
          }
#endif
        }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
        if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
        {
#endif
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
        }
#endif
      }
    }
#ifndef DBUG_OFF
    dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
    {
#endif
      if (add_value)
        str->length(str->length() - SPIDER_SQL_COMMA_LEN);
      if (str->reserve(SPIDER_SQL_CLOSE_PAREN_LEN, SPIDER_SQL_COMMA_LEN))
      {
        str->length(0);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
      str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    }
    if (spider->sql_kinds & SPIDER_SQL_KIND_HS)
    {
      int roop_count2;
      for (
        roop_count2 = spider_conn_link_idx_next(share->link_statuses,
          spider->conn_link_idx, -1, share->link_count,
          SPIDER_LINK_STATUS_RECOVERY);
        roop_count2 < (int) share->link_count;
        roop_count2 = spider_conn_link_idx_next(share->link_statuses,
          spider->conn_link_idx, roop_count2, share->link_count,
          SPIDER_LINK_STATUS_RECOVERY)
      ) {
        if (
          spider->sql_kind[roop_count2] == SPIDER_SQL_KIND_HS &&
          ((error_num = spider_db_hs_request_buf_insert(spider, roop_count2)))
        )
          DBUG_RETURN(error_num);
      }
#ifndef HANDLERSOCKET_MYSQL_UTIL
      result_list->hs_upds.clear();
#else
      result_list->hs_upds.elements = 0;
#endif
      result_list->hs_upd_rows++;
    }
#endif
  }
  DBUG_PRINT("info",("spider str->length()=%d", str->length()));
  DBUG_PRINT("info",("spider result_list->insert_pos=%d",
    result_list->insert_pos));
  if (
    (bulk_end || (int) str->length() >= spider->bulk_size) &&
    (!spider->result_list.insert_pos ||
      (int) str->length() > result_list->insert_pos)
  ) {
    int roop_count2;
    SPIDER_CONN *conn, *first_insert_conn = NULL;
    for (
      roop_count2 = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_count2 < (int) share->link_count;
      roop_count2 = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count2, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      if (spider->conn_kind[roop_count2] == SPIDER_CONN_KIND_MYSQL)
      {
#endif
        conn = spider->conns[roop_count2];
        if (share->same_db_table_name)
          sql = str;
        else {
          sql = &result_list->insert_sqls[roop_count2];
          if (sql->copy(result_list->insert_sql))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          tmp_pos = sql->length();
          sql->length(result_list->insert_table_name_pos);
          spider_db_append_table_name_with_adjusting(spider, sql, share,
            roop_count2, SPIDER_SQL_KIND_SQL);
          sql->length(tmp_pos);
        }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      } else {
        sql = &result_list->hs_sql;
        conn = spider->hs_w_conns[roop_count2];
      }
#endif
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[roop_count2];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count2)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count2] &&
          spider->need_mons[roop_count2]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count2],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count2],
              NULL,
              0,
              share->monitoring_kind[roop_count2],
              share->monitoring_limit[roop_count2],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, roop_count2, spider->trx->thd,
        share);
      if (spider_db_query(
        conn,
        sql->ptr(),
        sql->length() - SPIDER_SQL_COMMA_LEN,
        &spider->need_mons[roop_count2])
      ) {
        str->length(result_list->insert_pos);
        error_num = spider_db_errorno(conn);
        if (error_num == HA_ERR_FOUND_DUPP_KEY)
        {
          uint roop_count;
          int key_name_length;
          int max_length = 0;
          char *key_name;
          DBUG_PRINT("info",("spider error_str=%s", conn->error_str));
          for (roop_count = 0; roop_count < table->s->keys; roop_count++)
          {
            key_name = table->s->key_info[roop_count].name;
            key_name_length = strlen(key_name);
            DBUG_PRINT("info",("spider key_name=%s", key_name));
            if (
              max_length < key_name_length &&
              conn->error_length - 1 >= key_name_length &&
              *(conn->error_str + conn->error_length - 2 -
                key_name_length) == '\'' &&
              !strncasecmp(conn->error_str +
                conn->error_length - 1 - key_name_length,
                key_name, key_name_length)
            ) {
              max_length = key_name_length;
              spider->dup_key_idx = roop_count;
            }
          }
          if (max_length == 0)
            spider->dup_key_idx = (uint) -1;
          DBUG_PRINT("info",("spider dup_key_idx=%d", spider->dup_key_idx));
        }
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          error_num != ER_DUP_ENTRY &&
          error_num != ER_DUP_KEY &&
          error_num != HA_ERR_FOUND_DUPP_KEY &&
          share->monitoring_kind[roop_count2] &&
          spider->need_mons[roop_count2]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count2],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count2],
              NULL,
              0,
              share->monitoring_kind[roop_count2],
              share->monitoring_limit[roop_count2],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      if (conn->conn_kind != SPIDER_CONN_KIND_MYSQL)
      {
        size_t num_fields;
        SPIDER_HS_CONN hs_conn = conn->hs_conn;
        uint roop_count;
        DBUG_PRINT("info",("spider conn=%p", conn));
        DBUG_PRINT("info",("spider result_list->hs_upd_rows=%llu",
          result_list->hs_upd_rows));
        for (roop_count = 0; roop_count < result_list->hs_upd_rows;
          roop_count++)
        {
          if ((error_num = hs_conn->response_recv(num_fields)))
          {
#ifndef DBUG_OFF
            if (conn->hs_conn->get_response_end_offset() > 0 &&
              conn->hs_conn->get_readbuf_begin())
            {
              char tmp_buf[MAX_FIELD_WIDTH];
              String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
              tmp_str.length(0);
              tmp_str.append(conn->hs_conn->get_readbuf_begin(),
                conn->hs_conn->get_response_end_offset(), &my_charset_bin);
              DBUG_PRINT("info",("spider hs readbuf05 size=%zu str=%s",
                conn->hs_conn->get_response_end_offset(),
                tmp_str.c_ptr_safe()));
            }
#endif
            if (error_num > 0)
              hs_conn->response_buf_remove();
            spider_db_errorno(conn);
            DBUG_RETURN(ER_SPIDER_HS_NUM);
          }
#ifndef DBUG_OFF
          if (conn->hs_conn->get_response_end_offset() > 0 &&
            conn->hs_conn->get_readbuf_begin())
          {
            char tmp_buf[MAX_FIELD_WIDTH];
            String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
            tmp_str.length(0);
            tmp_str.append(conn->hs_conn->get_readbuf_begin(),
              conn->hs_conn->get_response_end_offset(), &my_charset_bin);
            DBUG_PRINT("info",("spider hs readbuf06 size=%zu str=%s",
              conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
          }
#endif
          hs_conn->response_buf_remove();
        }
      }
#endif
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      if (first_insert_link_idx == -1)
      {
        first_insert_link_idx = roop_count2;
        first_insert_conn = conn;
      }
    }

    conn = first_insert_conn;
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[first_insert_link_idx];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    str->length(result_list->insert_pos);
    if (table->next_number_field &&
      (
        !table->auto_increment_field_not_null ||
        (
          !table->next_number_field->val_int() &&
          !(thd->variables.sql_mode & MODE_NO_AUTO_VALUE_ON_ZERO)
        )
      )
    ) {
      ulonglong last_insert_id;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
      {
#endif
#if MYSQL_VERSION_ID < 50500
        last_insert_id = conn->db_conn->last_used_con->insert_id;
#else
        last_insert_id = conn->db_conn->insert_id;
#endif
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      } else {
        last_insert_id = 0;
      }
#endif
      table->next_number_field->set_notnull();
      if (
        (error_num = spider_db_update_auto_increment(spider,
          first_insert_link_idx)) ||
        (error_num = table->next_number_field->store(
          last_insert_id, TRUE))
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  if (
    (bulk_end || !spider->bulk_insert) &&
    (error_num = spider_trx_check_link_idx_failed(spider))
  )
    DBUG_RETURN(error_num);
  DBUG_RETURN(0);
}

int spider_db_update_auto_increment(
  ha_spider *spider,
  int link_idx
) {
  int roop_count;
  THD *thd = spider->trx->thd;
  ulonglong last_insert_id, affected_rows;
  SPIDER_SHARE *share = spider->share;
  TABLE *table = spider->get_table();
  SPIDER_DB_CONN *last_used_con;
  int auto_increment_mode = spider_param_auto_increment_mode(thd,
    share->auto_increment_mode);
  DBUG_ENTER("spider_db_update_auto_increment");
  if (
    auto_increment_mode == 2 ||
    (auto_increment_mode == 3 && !table->auto_increment_field_not_null)
  ) {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (spider->conn_kind[link_idx] == SPIDER_CONN_KIND_MYSQL)
    {
#endif
#if MYSQL_VERSION_ID < 50500
      last_used_con = spider->conns[link_idx]->db_conn->last_used_con;
#else
      last_used_con = spider->conns[link_idx]->db_conn;
#endif
      last_insert_id = last_used_con->insert_id;
      affected_rows = last_used_con->affected_rows;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    } else {
      last_insert_id = 0;
      affected_rows = spider->result_list.hs_upd_rows;
    }
#endif
    DBUG_PRINT("info",("spider last_insert_id=%llu", last_insert_id));
    share->auto_increment_value =
      last_insert_id + affected_rows;
/*
    thd->record_first_successful_insert_id_in_cur_stmt(last_insert_id);
*/
    if (
      thd->first_successful_insert_id_in_cur_stmt == 0 ||
      thd->first_successful_insert_id_in_cur_stmt > last_insert_id
    ) {
      bool first_set = (thd->first_successful_insert_id_in_cur_stmt == 0);
      thd->first_successful_insert_id_in_cur_stmt = last_insert_id;
      if (
        table->s->next_number_keypart == 0 &&
        mysql_bin_log.is_open() &&
#if MYSQL_VERSION_ID < 50500
        !thd->current_stmt_binlog_row_based
#else
        !thd->is_current_stmt_binlog_format_row()
#endif
      ) {
        if (
          spider->check_partitioned() &&
          thd->auto_inc_intervals_in_cur_stmt_for_binlog.nb_elements() > 0
        ) {
          DBUG_PRINT("info",("spider table partitioning"));
          Discrete_interval *current =
            thd->auto_inc_intervals_in_cur_stmt_for_binlog.get_current();
          current->replace(last_insert_id, affected_rows, 1);
        } else {
          DBUG_PRINT("info",("spider table"));
          thd->auto_inc_intervals_in_cur_stmt_for_binlog.append(
            last_insert_id, affected_rows, 1);
        }
        if (affected_rows > 1 || !first_set)
        {
          for (roop_count = first_set ? 1 : 0;
            roop_count < (int) affected_rows;
            roop_count++)
            push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_NOTE,
              ER_SPIDER_AUTOINC_VAL_IS_DIFFERENT_NUM,
              ER_SPIDER_AUTOINC_VAL_IS_DIFFERENT_STR);
        }
      }
    } else {
      if (
        table->s->next_number_keypart == 0 &&
        mysql_bin_log.is_open() &&
#if MYSQL_VERSION_ID < 50500
        !thd->current_stmt_binlog_row_based
#else
        !thd->is_current_stmt_binlog_format_row()
#endif
      ) {
        for (roop_count = 0; roop_count < (int) affected_rows; roop_count++)
          push_warning_printf(thd, MYSQL_ERROR::WARN_LEVEL_NOTE,
            ER_SPIDER_AUTOINC_VAL_IS_DIFFERENT_NUM,
            ER_SPIDER_AUTOINC_VAL_IS_DIFFERENT_STR);
      }
    }
  }
  DBUG_RETURN(0);
}

int spider_db_bulk_update_size_limit(
  ha_spider *spider,
  TABLE *table
) {
  int error_num, roop_count;
  THD *thd = spider->trx->thd;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->update_sql, *sql;
  TABLE **tmp_table = result_list->upd_tmp_tbls;
  DBUG_ENTER("spider_db_bulk_update_size_limit");

  if (result_list->bulk_update_mode == 1)
  {
    /* execute bulk updating */
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_count < (int) share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
      if (
        share->same_db_table_name &&
        share->link_statuses[roop_count] != SPIDER_LINK_STATUS_RECOVERY
      ) {
        if ((error_num = spider_db_query_for_bulk_update(
          str, spider, spider->conns[roop_count], roop_count)))
          DBUG_RETURN(error_num);
      } else {
        sql = &result_list->update_sqls[roop_count];
        if ((error_num = spider_db_query_for_bulk_update(
          sql, spider, spider->conns[roop_count], roop_count)))
          DBUG_RETURN(error_num);
        sql->length(0);
      }
      str->length(0);
    }
  } else {
    /* store query to temporary tables */
    bool first = FALSE;
    if (!result_list->upd_tmp_tbl)
    {
      if (!(result_list->upd_tmp_tbl = spider_mk_sys_tmp_table(
        thd, table, &result_list->upd_tmp_tbl_prm, "a", str->charset())))
      {
        error_num = HA_ERR_OUT_OF_MEM;
        goto error_mk_table;
      }
      result_list->upd_tmp_tbl->file->extra(HA_EXTRA_WRITE_CACHE);
      result_list->upd_tmp_tbl->file->ha_start_bulk_insert((ha_rows) 0);
      first = TRUE;
    }
    result_list->upd_tmp_tbl->field[0]->set_notnull();
    result_list->upd_tmp_tbl->field[0]->store(
      str->ptr(), str->length(), str->charset());
    if ((error_num = result_list->upd_tmp_tbl->file->ha_write_row(
      result_list->upd_tmp_tbl->record[0])))
      goto error_write_row;
    str->length(0);

    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_count < (int) share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
      if (
        !share->same_db_table_name ||
        share->link_statuses[roop_count] == SPIDER_LINK_STATUS_RECOVERY
      ) {
        sql = &result_list->update_sqls[roop_count];
        if (first)
        {
          if (
            !tmp_table[roop_count] &&
            !(tmp_table[roop_count] = spider_mk_sys_tmp_table(
              thd, table, &result_list->upd_tmp_tbl_prms[roop_count], "a",
              sql->charset()))
          ) {
            error_num = HA_ERR_OUT_OF_MEM;
            goto error_mk_table;
          }
          tmp_table[roop_count]->file->extra(HA_EXTRA_WRITE_CACHE);
          tmp_table[roop_count]->file->ha_start_bulk_insert((ha_rows) 0);
        }
        if (tmp_table[roop_count])
        {
          tmp_table[roop_count]->field[0]->set_notnull();
          tmp_table[roop_count]->field[0]->store(
            sql->ptr(), sql->length(), sql->charset());
          if ((error_num = tmp_table[roop_count]->file->ha_write_row(
            tmp_table[roop_count]->record[0])))
            goto error_write_row;
        }
        sql->length(0);
      }
    }
  }
  DBUG_RETURN(0);

error_mk_table:
error_write_row:
  for (roop_count = share->link_count - 1; roop_count >= 0; roop_count--)
  {
    if (tmp_table[roop_count])
    {
      tmp_table[roop_count]->file->ha_end_bulk_insert();
      spider_rm_sys_tmp_table(thd, tmp_table[roop_count],
        &result_list->upd_tmp_tbl_prms[roop_count]);
      tmp_table[roop_count] = NULL;
    }
    result_list->update_sqls[roop_count].length(0);
  }
  if (result_list->upd_tmp_tbl)
  {
    spider_rm_sys_tmp_table(thd, result_list->upd_tmp_tbl,
      &result_list->upd_tmp_tbl_prm);
    result_list->upd_tmp_tbl = NULL;
  }
  result_list->update_sql.length(0);
  DBUG_RETURN(error_num);
}

int spider_db_bulk_update_end(
  ha_spider *spider
) {
  int error_num = 0, error_num2, roop_count;
  char buf[MAX_FIELD_WIDTH], buf2[MAX_FIELD_WIDTH];
  THD *thd = spider->trx->thd;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string tmp_str(buf, MAX_FIELD_WIDTH,
    result_list->update_sql.charset()),
    tmp_str2(buf2, MAX_FIELD_WIDTH, &my_charset_bin), *sql;
  TABLE **tmp_table = result_list->upd_tmp_tbls;
  bool is_error = thd->is_error();
  DBUG_ENTER("spider_db_bulk_update_end");
  tmp_str.init_calc_mem(121);
  tmp_str2.init_calc_mem(122);

  if (result_list->upd_tmp_tbl)
  {
    if (
      (error_num2 = result_list->upd_tmp_tbl->file->ha_end_bulk_insert())
    ) {
      error_num = error_num2;
    }

    for (roop_count = 0; roop_count < (int) share->link_count; roop_count++)
    {
      if (
        tmp_table[roop_count] &&
        (error_num2 = tmp_table[roop_count]->file->ha_end_bulk_insert())
      ) {
        error_num = error_num2;
      }
    }

    if (!is_error)
    {
      if (error_num)
        goto error_last_query;

      result_list->upd_tmp_tbl->file->extra(HA_EXTRA_CACHE);
      if ((error_num = result_list->upd_tmp_tbl->file->ha_rnd_init(TRUE)))
      {
        roop_count = -1;
        goto error_rnd_init;
      }

      for (roop_count = 0; roop_count < (int) share->link_count; roop_count++)
      {
        if (tmp_table[roop_count])
        {
          result_list->upd_tmp_tbl->file->extra(HA_EXTRA_CACHE);
          if (
            (error_num = tmp_table[roop_count]->file->ha_rnd_init(TRUE))
          )
            goto error_rnd_init;
        }
      }

      while (
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50200
        !(error_num = result_list->upd_tmp_tbl->file->ha_rnd_next(
          result_list->upd_tmp_tbl->record[0]))
#else
        !(error_num = result_list->upd_tmp_tbl->file->rnd_next(
          result_list->upd_tmp_tbl->record[0]))
#endif
      ) {
        result_list->upd_tmp_tbl->field[0]->val_str(tmp_str.get_str());
        tmp_str.mem_calc();
        for (
          roop_count = spider_conn_link_idx_next(share->link_statuses,
            spider->conn_link_idx, -1, share->link_count,
            SPIDER_LINK_STATUS_RECOVERY);
          roop_count < (int) share->link_count;
          roop_count = spider_conn_link_idx_next(share->link_statuses,
            spider->conn_link_idx, roop_count, share->link_count,
            SPIDER_LINK_STATUS_RECOVERY)
        ) {
          if (
            share->same_db_table_name &&
            share->link_statuses[roop_count] != SPIDER_LINK_STATUS_RECOVERY
          )
            sql = &tmp_str;
          else if (!tmp_table[roop_count])
            continue;
          else {
            if (
#if defined(MARIADB_BASE_VERSION) && MYSQL_VERSION_ID >= 50200
              !(error_num = tmp_table[roop_count]->file->ha_rnd_next(
                tmp_table[roop_count]->record[0]))
#else
              !(error_num = tmp_table[roop_count]->file->rnd_next(
                tmp_table[roop_count]->record[0]))
#endif
            )
              goto error_rnd_next;
            tmp_str2.set_charset(
              result_list->update_sqls[roop_count].charset());
            tmp_table[roop_count]->field[0]->val_str(tmp_str2.get_str());
            tmp_str2.mem_calc();
            sql = &tmp_str2;
          }
          conn = spider->conns[roop_count];
          if ((error_num = spider_db_query_for_bulk_update(
            sql, spider, conn, roop_count)))
            goto error_query;
        }
      }
      if (error_num != HA_ERR_END_OF_FILE)
        goto error_rnd_next;

      for (roop_count = share->link_count - 1; roop_count >= 0; roop_count--)
      {
        if (tmp_table[roop_count])
          tmp_table[roop_count]->file->ha_rnd_end();
      }
      result_list->upd_tmp_tbl->file->ha_rnd_end();
    }
  }

  if (!is_error)
  {
    if (result_list->update_sql.length() > 0)
    {
      for (
        roop_count = spider_conn_link_idx_next(share->link_statuses,
          spider->conn_link_idx, -1, share->link_count,
          SPIDER_LINK_STATUS_RECOVERY);
        roop_count < (int) share->link_count;
        roop_count = spider_conn_link_idx_next(share->link_statuses,
          spider->conn_link_idx, roop_count, share->link_count,
          SPIDER_LINK_STATUS_RECOVERY)
      ) {
        if (
          spider->share->same_db_table_name &&
          share->link_statuses[roop_count] != SPIDER_LINK_STATUS_RECOVERY
        )
          sql = &result_list->update_sql;
        else
          sql = &result_list->update_sqls[roop_count];
        conn = spider->conns[roop_count];
        if ((error_num = spider_db_query_for_bulk_update(
          sql, spider, conn, roop_count)))
          goto error_last_query;
      }
    }
  }
  for (roop_count = share->link_count - 1; roop_count >= 0; roop_count--)
  {
    if (tmp_table[roop_count])
    {
      spider_rm_sys_tmp_table(thd, tmp_table[roop_count],
        &result_list->upd_tmp_tbl_prms[roop_count]);
      tmp_table[roop_count] = NULL;
    }
    result_list->update_sqls[roop_count].length(0);
  }
  if (result_list->upd_tmp_tbl)
  {
    spider_rm_sys_tmp_table(thd, result_list->upd_tmp_tbl,
      &result_list->upd_tmp_tbl_prm);
    result_list->upd_tmp_tbl = NULL;
  }
  result_list->update_sql.length(0);
  DBUG_RETURN(0);

error_query:
error_rnd_next:
  roop_count = share->link_count - 1;
error_rnd_init:
  for (; roop_count >= 0; roop_count--)
  {
    if (tmp_table[roop_count])
      tmp_table[roop_count]->file->ha_rnd_end();
  }
  if (result_list->upd_tmp_tbl)
    result_list->upd_tmp_tbl->file->ha_rnd_end();
error_last_query:
  for (roop_count = share->link_count - 1; roop_count >= 0; roop_count--)
  {
    if (tmp_table[roop_count])
    {
      spider_rm_sys_tmp_table(thd, tmp_table[roop_count],
        &result_list->upd_tmp_tbl_prms[roop_count]);
      tmp_table[roop_count] = NULL;
    }
    result_list->update_sqls[roop_count].length(0);
  }
  if (result_list->upd_tmp_tbl)
  {
    spider_rm_sys_tmp_table(thd, result_list->upd_tmp_tbl,
      &result_list->upd_tmp_tbl_prm);
    result_list->upd_tmp_tbl = NULL;
  }
  result_list->update_sql.length(0);
  DBUG_RETURN(error_num);
}

int spider_db_bulk_update(
  ha_spider *spider,
  TABLE *table,
  my_ptrdiff_t ptr_diff
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->update_sql, *sql;
  int bulk_update_size = result_list->bulk_update_size;
  bool size_over = FALSE;
  DBUG_ENTER("spider_db_bulk_update");

  if (str->length() > 0)
  {
    if (str->reserve(SPIDER_SQL_SEMICOLON_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }

  if (
    (error_num = spider_db_append_update(str, spider, 0)) ||
    (error_num = spider_db_append_update_set(str, spider, table)) ||
    (error_num = spider_db_append_update_where(
      str, spider, table, ptr_diff))
  )
    DBUG_RETURN(error_num);
  if ((int) str->length() >= bulk_update_size)
    size_over = TRUE;

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    if (
      !share->same_db_table_name ||
      share->link_statuses[roop_count] == SPIDER_LINK_STATUS_RECOVERY
    ) {
      sql = &result_list->update_sqls[roop_count];
      if (sql->length() > 0)
      {
        if (sql->reserve(SPIDER_SQL_SEMICOLON_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        sql->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
      }
      if (
        (error_num = spider_db_append_update(sql, spider, roop_count)) ||
        (error_num = spider_db_append_update_set(sql, spider, table)) ||
        (error_num = spider_db_append_update_where(
          sql, spider, table, ptr_diff))
      )
        DBUG_RETURN(error_num);
      if (
        spider->pk_update &&
        share->link_statuses[roop_count] == SPIDER_LINK_STATUS_RECOVERY
      ) {
        if (sql->reserve(SPIDER_SQL_SEMICOLON_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        sql->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
        if ((error_num = spider_db_append_insert_for_recovery(sql, spider,
          table, roop_count)))
          DBUG_RETURN(error_num);
      }
      if ((int) sql->length() >= bulk_update_size)
        size_over = TRUE;
    }
  }

  if (
    size_over &&
    (error_num = spider_db_bulk_update_size_limit(spider, table))
  )
    DBUG_RETURN(error_num);
  DBUG_RETURN(0);
}

int spider_db_update(
  ha_spider *spider,
  TABLE *table,
  const uchar *old_data
) {
  int error_num, roop_count, tmp_pos;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->update_sql, *sql, *insert_sql;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(old_data, table->record[0]);
  DBUG_ENTER("spider_db_update");
  if (result_list->bulk_update_mode)
    DBUG_RETURN(spider_db_bulk_update(spider, table, ptr_diff));

  str->length(0);
  if (
    (error_num = spider_db_append_update(str, spider, 0)) ||
    (error_num = spider_db_append_update_set(str, spider, table)) ||
    (error_num = spider_db_append_update_where(
      str, spider, table, ptr_diff))
  )
    DBUG_RETURN(error_num);

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
    if (spider->share->same_db_table_name || roop_count == 0)
      sql = str;
    else {
      sql = &result_list->update_sqls[roop_count];
      if (sql->copy(result_list->update_sql))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      tmp_pos = sql->length();
      sql->length(result_list->table_name_pos);
      spider_db_append_table_name_with_adjusting(spider, sql, share,
        roop_count, SPIDER_SQL_KIND_SQL);
      sql->length(tmp_pos);
    }
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[roop_count];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    if ((error_num = spider_db_set_names(spider, conn, roop_count)))
    {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      if (
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            (uint32) share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            spider->conn_link_idx[roop_count],
            NULL,
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
    spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
      share);
    if (spider_db_query(
      conn,
      sql->ptr(),
      sql->length(),
      &spider->need_mons[roop_count])
    ) {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      error_num = spider_db_errorno(conn);
      if (
        error_num != ER_DUP_ENTRY &&
        error_num != ER_DUP_KEY &&
        error_num != HA_ERR_FOUND_DUPP_KEY &&
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            (uint32) share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            spider->conn_link_idx[roop_count],
            NULL,
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }

    if (
#if MYSQL_VERSION_ID < 50500
      !conn->db_conn->last_used_con->affected_rows &&
#else
      !conn->db_conn->affected_rows &&
#endif
      share->link_statuses[roop_count] == SPIDER_LINK_STATUS_RECOVERY &&
      spider->pk_update
    ) {
      /* insert */
      insert_sql = &spider->result_list.insert_sqls[roop_count];
      insert_sql->length(0);
      if ((error_num = spider_db_append_insert_for_recovery(insert_sql, spider,
        table, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
        share);
      if (spider_db_query(
        conn,
        insert_sql->ptr(),
        insert_sql->length(),
        &spider->need_mons[roop_count])
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conn);
        if (
          error_num != ER_DUP_ENTRY &&
          error_num != ER_DUP_KEY &&
          error_num != HA_ERR_FOUND_DUPP_KEY &&
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    result_list->update_sqls[roop_count].length(0);
  }
  str->length(0);
  DBUG_RETURN(0);
}

#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
int spider_db_direct_update(
  ha_spider *spider,
  TABLE *table,
  KEY_MULTI_RANGE *ranges,
  uint range_count,
  uint *update_rows
) {
  int error_num, roop_count, tmp_pos;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str, *sql;
  bool counted = FALSE;
  st_select_lex *select_lex;
  longlong select_limit;
  longlong offset_limit;
  DBUG_ENTER("spider_db_direct_update");

  str = &result_list->update_sql;
  str->length(0);
  spider_set_result_list_param(spider);
  result_list->finish_flg = FALSE;
  DBUG_PRINT("info", ("spider do_direct_update=%s",
    spider->do_direct_update ? "TRUE" : "FALSE"));
  DBUG_PRINT("info", ("spider direct_update_kinds=%u",
    spider->direct_update_kinds));
  if (
    (error_num = spider_db_append_update(str, spider, 0)) ||
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    (
      (spider->hs_increment || spider->hs_decrement) &&
      (error_num = spider_db_append_increment_update_set(str, spider, table))
    ) ||
    (
      !spider->hs_increment && !spider->hs_decrement &&
#endif
      (error_num = spider_db_append_update_set(str, spider, table))
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    )
#endif
  )
    DBUG_RETURN(error_num);
  result_list->where_pos = str->length();
  result_list->desc_flg = FALSE;
  result_list->sorted = TRUE;
  if (spider->active_index == MAX_KEY)
    result_list->key_info = NULL;
  else
    result_list->key_info = &table->key_info[spider->active_index];
  spider_get_select_limit(spider, &select_lex, &select_limit, &offset_limit);
  result_list->limit_num =
    result_list->internal_limit >= select_limit ?
    select_limit : result_list->internal_limit;
  result_list->internal_offset += offset_limit;
/*
  result_list->limit_num =
    result_list->internal_limit >= result_list->split_read ?
    result_list->split_read : result_list->internal_limit;
*/
  if (
    (
      (spider->direct_update_kinds & SPIDER_SQL_KIND_SQL) &&
      (error_num = spider_db_append_key_where_internal(
        (ranges && ranges->start_key.key) ? &ranges->start_key : NULL,
        (ranges && ranges->end_key.key) ? &ranges->end_key : NULL,
        spider, SPIDER_SQL_KIND_SQL, TRUE))
    ) ||
    (
      (spider->direct_update_kinds & SPIDER_SQL_KIND_HS) &&
      (error_num = spider_db_append_key_where_internal(
        ranges->start_key.key ? &ranges->start_key : NULL,
        ranges->end_key.key ? &ranges->end_key : NULL,
        spider, SPIDER_SQL_KIND_HS, TRUE))
    ) ||
    (error_num = spider_db_append_key_order(spider, TRUE)) ||
    (error_num = spider_db_append_limit(spider, str,
      result_list->internal_offset, result_list->limit_num))
  )
    DBUG_RETURN(error_num);

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (!spider_bit_is_set(spider->do_hs_direct_update, roop_count))
    {
#endif
      DBUG_PRINT("info", ("spider exec sql"));
      conn = spider->conns[roop_count];
      if (spider->share->same_db_table_name || roop_count == 0)
        sql = str;
      else {
        sql = &result_list->update_sqls[roop_count];
        if (sql->copy(result_list->update_sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_pos = sql->length();
        sql->length(result_list->table_name_pos);
        spider_db_append_table_name_with_adjusting(spider, sql, share,
          roop_count, SPIDER_SQL_KIND_SQL);
        sql->length(tmp_pos);
      }
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    } else {
      DBUG_PRINT("info", ("spider exec hs"));
      conn = spider->hs_w_conns[roop_count];
      sql = &result_list->hs_sql; /* dummy */
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      if ((error_num = spider_db_hs_request_buf_update(spider, roop_count)))
        DBUG_RETURN(error_num);
    }
#endif
    conn->need_mon = &spider->need_mons[roop_count];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    if ((error_num = spider_db_set_names(spider, conn, roop_count)))
    {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      if (
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            (uint32) share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            spider->conn_link_idx[roop_count],
            NULL,
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
    spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
      share);
    if (spider_db_query(
      conn,
      sql->ptr(),
      sql->length(),
      &spider->need_mons[roop_count])
    ) {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      error_num = spider_db_errorno(conn);
      if (
        error_num != ER_DUP_ENTRY &&
        error_num != ER_DUP_KEY &&
        error_num != HA_ERR_FOUND_DUPP_KEY &&
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            (uint32) share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            spider->conn_link_idx[roop_count],
            NULL,
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (!spider_bit_is_set(spider->do_hs_direct_update, roop_count))
    {
      if (!counted)
      {
#endif
        SPIDER_DB_CONN *last_used_con;
#if MYSQL_VERSION_ID < 50500
        last_used_con = spider->conns[roop_count]->db_conn->last_used_con;
#else
        last_used_con = spider->conns[roop_count]->db_conn;
#endif
        *update_rows = (uint) last_used_con->affected_rows;
        DBUG_PRINT("info", ("spider update_rows = %u", *update_rows));
        counted = TRUE;
      }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    } else {
      size_t num_fields;
      DBUG_PRINT("info",("spider conn=%p", conn));
      if ((error_num = conn->hs_conn->response_recv(num_fields)))
      {
#ifndef DBUG_OFF
        if (conn->hs_conn->get_response_end_offset() > 0 &&
          conn->hs_conn->get_readbuf_begin())
        {
          char tmp_buf[MAX_FIELD_WIDTH];
          String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
          tmp_str.length(0);
          tmp_str.append(conn->hs_conn->get_readbuf_begin(),
            conn->hs_conn->get_response_end_offset(), &my_charset_bin);
          DBUG_PRINT("info",("spider hs readbuf07 size=%zu str=%s",
            conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
        }
#endif
        if (error_num > 0)
          conn->hs_conn->response_buf_remove();
        spider_db_errorno(conn);
        DBUG_RETURN(ER_SPIDER_HS_NUM);
      }
#ifndef DBUG_OFF
      if (conn->hs_conn->get_response_end_offset() > 0 &&
        conn->hs_conn->get_readbuf_begin())
      {
        char tmp_buf[MAX_FIELD_WIDTH];
        String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
        tmp_str.length(0);
        tmp_str.append(conn->hs_conn->get_readbuf_begin(),
          conn->hs_conn->get_response_end_offset(), &my_charset_bin);
        DBUG_PRINT("info",("spider hs readbuf08 size=%zu str=%s",
          conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
      }
#endif
      if (!counted)
      {
        const SPIDER_HS_STRING_REF *hs_row;
        if (
          num_fields != 1 ||
          !(hs_row = conn->hs_conn->get_next_row()) ||
          !hs_row->begin()
        ) {
          *update_rows = 0;
        } else {
          *update_rows = (uint) my_strtoll10(hs_row->begin(), (char**) NULL,
            &error_num);
        }
        DBUG_PRINT("info", ("spider update_rows = %u", *update_rows));
        counted = TRUE;
      }
      conn->hs_conn->response_buf_remove();
    }
#endif
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    result_list->update_sqls[roop_count].length(0);
  }
  str->length(0);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifndef HANDLERSOCKET_MYSQL_UTIL
  result_list->hs_keys.clear();
  result_list->hs_upds.clear();
#else
  result_list->hs_keys.elements = 0;
  result_list->hs_upds.elements = 0;
#endif
#endif
  DBUG_RETURN(0);
}
#endif

int spider_db_bulk_delete(
  ha_spider *spider,
  TABLE *table,
  my_ptrdiff_t ptr_diff
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->update_sql, *sql;
  int bulk_update_size = result_list->bulk_update_size;
  bool size_over = FALSE;
  DBUG_ENTER("spider_db_bulk_delete");

  if (str->length() > 0)
  {
    if (str->reserve(SPIDER_SQL_SEMICOLON_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
  }

  if (
    (error_num = spider_db_append_delete(str, spider)) ||
    (error_num = spider_db_append_from(str, spider, 0)) ||
    (error_num = spider_db_append_update_where(
      str, spider, table, ptr_diff))
  )
    DBUG_RETURN(error_num);
  if ((int) str->length() >= bulk_update_size)
    size_over = TRUE;

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    if (
      !share->same_db_table_name ||
      share->link_statuses[roop_count] == SPIDER_LINK_STATUS_RECOVERY
    ) {
      sql = &result_list->update_sqls[roop_count];
      if (sql->length() > 0)
      {
        if (sql->reserve(SPIDER_SQL_SEMICOLON_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        sql->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
      }
      if (
        (error_num = spider_db_append_delete(sql, spider)) ||
        (error_num = spider_db_append_from(sql, spider, 0)) ||
        (error_num = spider_db_append_update_where(
          sql, spider, table, ptr_diff))
      )
        DBUG_RETURN(error_num);
      if ((int) sql->length() >= bulk_update_size)
        size_over = TRUE;
    }
  }

  if (
    size_over &&
    (error_num = spider_db_bulk_update_size_limit(spider, table))
  )
    DBUG_RETURN(error_num);
  DBUG_RETURN(0);
}

int spider_db_delete(
  ha_spider *spider,
  TABLE *table,
  const uchar *buf
) {
  int error_num, roop_count, tmp_pos;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->update_sql, *sql;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  DBUG_ENTER("spider_db_delete");
  if (result_list->bulk_update_mode)
    DBUG_RETURN(spider_db_bulk_delete(spider, table, ptr_diff));

  str->length(0);
  if (
    (error_num = spider_db_append_delete(str, spider)) ||
    (error_num = spider_db_append_from(str, spider, 0)) ||
    (error_num = spider_db_append_update_where(
      str, spider, table, ptr_diff))
  )
    DBUG_RETURN(error_num);

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
    if (spider->share->same_db_table_name || roop_count == 0)
      sql = str;
    else {
      sql = &result_list->update_sqls[roop_count];
      if (sql->copy(result_list->update_sql))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      tmp_pos = sql->length();
      sql->length(result_list->table_name_pos);
      spider_db_append_table_name_with_adjusting(spider, sql, share,
        roop_count, SPIDER_SQL_KIND_SQL);
      sql->length(tmp_pos);
    }
    if ((error_num = spider_db_query_with_set_names(
      sql, spider, conn, roop_count)))
      DBUG_RETURN(error_num);
    result_list->update_sqls[roop_count].length(0);
  }
  str->length(0);
  DBUG_RETURN(0);
}

#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
int spider_db_direct_delete(
  ha_spider *spider,
  TABLE *table,
  KEY_MULTI_RANGE *ranges,
  uint range_count,
  uint *delete_rows
) {
  int error_num, roop_count, tmp_pos;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str, *sql;
  bool counted = FALSE;
  st_select_lex *select_lex;
  longlong select_limit;
  longlong offset_limit;
  DBUG_ENTER("spider_db_direct_delete");

  str = &result_list->update_sql;
  str->length(0);
  spider_set_result_list_param(spider);
  result_list->finish_flg = FALSE;
  if (
    (error_num = spider_db_append_delete(str, spider)) ||
    (error_num = spider_db_append_from(
      (spider->direct_update_kinds & SPIDER_SQL_KIND_SQL) ? str : NULL,
      spider, 0))
  )
    DBUG_RETURN(error_num);
  result_list->where_pos = str->length();
  result_list->desc_flg = FALSE;
  result_list->sorted = TRUE;
  if (spider->active_index == MAX_KEY)
    result_list->key_info = NULL;
  else
    result_list->key_info = &table->key_info[spider->active_index];
  spider_get_select_limit(spider, &select_lex, &select_limit, &offset_limit);
  result_list->limit_num =
    result_list->internal_limit >= select_limit ?
    select_limit : result_list->internal_limit;
  result_list->internal_offset += offset_limit;
/*
  result_list->limit_num =
    result_list->internal_limit >= result_list->split_read ?
    result_list->split_read : result_list->internal_limit;
*/
  if (
    (
      (spider->direct_update_kinds & SPIDER_SQL_KIND_SQL) &&
      (error_num = spider_db_append_key_where_internal(
        (ranges && ranges->start_key.key) ? &ranges->start_key : NULL,
        (ranges && ranges->end_key.key) ? &ranges->end_key : NULL,
        spider, SPIDER_SQL_KIND_SQL, TRUE))
    ) ||
    (
      (spider->direct_update_kinds & SPIDER_SQL_KIND_HS) &&
      (error_num = spider_db_append_key_where_internal(
        ranges->start_key.key ? &ranges->start_key : NULL,
        ranges->end_key.key ? &ranges->end_key : NULL,
        spider, SPIDER_SQL_KIND_HS, TRUE))
    ) ||
    (error_num = spider_db_append_key_order(spider, TRUE)) ||
    (error_num = spider_db_append_limit(spider, str,
      result_list->internal_offset, result_list->limit_num))
  )
    DBUG_RETURN(error_num);

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (!spider_bit_is_set(spider->do_hs_direct_update, roop_count))
    {
#endif
      DBUG_PRINT("info", ("spider exec sql"));
      conn = spider->conns[roop_count];
      if (spider->share->same_db_table_name || roop_count == 0)
        sql = str;
      else {
        sql = &result_list->update_sqls[roop_count];
        if (sql->copy(result_list->update_sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_pos = sql->length();
        sql->length(result_list->table_name_pos);
        spider_db_append_table_name_with_adjusting(spider, sql, share,
          roop_count, SPIDER_SQL_KIND_SQL);
        sql->length(tmp_pos);
      }
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    } else {
      DBUG_PRINT("info", ("spider exec hs"));
      conn = spider->hs_w_conns[roop_count];
      sql = &result_list->hs_sql; /* dummy */
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      if ((error_num = spider_db_hs_request_buf_delete(spider, roop_count)))
        DBUG_RETURN(error_num);
    }
#endif
    conn->need_mon = &spider->need_mons[roop_count];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    if ((error_num = spider_db_set_names(spider, conn, roop_count)))
    {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      if (
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            (uint32) share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            spider->conn_link_idx[roop_count],
            NULL,
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
    spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
      share);
    if (spider_db_query(
      conn,
      sql->ptr(),
      sql->length(),
      &spider->need_mons[roop_count])
    ) {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      error_num = spider_db_errorno(conn);
      if (
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            (uint32) share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            spider->conn_link_idx[roop_count],
            NULL,
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (!spider_bit_is_set(spider->do_hs_direct_update, roop_count))
    {
#endif
      if (!counted)
      {
        SPIDER_DB_CONN *last_used_con;
#if MYSQL_VERSION_ID < 50500
        last_used_con = spider->conns[roop_count]->db_conn->last_used_con;
#else
        last_used_con = spider->conns[roop_count]->db_conn;
#endif
        *delete_rows = (uint) last_used_con->affected_rows;
        DBUG_PRINT("info", ("spider delete_rows = %u", *delete_rows));
        counted = TRUE;
      }
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    } else {
      size_t num_fields;
      DBUG_PRINT("info",("spider conn=%p", conn));
      if ((error_num = conn->hs_conn->response_recv(num_fields)))
      {
#ifndef DBUG_OFF
        if (conn->hs_conn->get_response_end_offset() > 0 &&
          conn->hs_conn->get_readbuf_begin())
        {
          char tmp_buf[MAX_FIELD_WIDTH];
          String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
          tmp_str.length(0);
          tmp_str.append(conn->hs_conn->get_readbuf_begin(),
            conn->hs_conn->get_response_end_offset(), &my_charset_bin);
          DBUG_PRINT("info",("spider hs readbuf09 size=%zu str=%s",
            conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
        }
#endif
        if (error_num > 0)
          conn->hs_conn->response_buf_remove();
        spider_db_errorno(conn);
        DBUG_RETURN(ER_SPIDER_HS_NUM);
      }
#ifndef DBUG_OFF
      if (conn->hs_conn->get_response_end_offset() > 0 &&
        conn->hs_conn->get_readbuf_begin())
      {
        char tmp_buf[MAX_FIELD_WIDTH];
        String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
        tmp_str.length(0);
        tmp_str.append(conn->hs_conn->get_readbuf_begin(),
          conn->hs_conn->get_response_end_offset(), &my_charset_bin);
        DBUG_PRINT("info",("spider hs readbuf10 size=%zu str=%s",
          conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
      }
#endif
      if (!counted)
      {
        const SPIDER_HS_STRING_REF *hs_row;
        if (
          num_fields != 1 ||
          !(hs_row = conn->hs_conn->get_next_row()) ||
          !hs_row->begin()
        ) {
          *delete_rows = 0;
        } else {
          *delete_rows = (uint) my_strtoll10(hs_row->begin(), (char**) NULL,
            &error_num);
        }
        DBUG_PRINT("info", ("spider delete_rows = %u", *delete_rows));
        counted = TRUE;
      }
      conn->hs_conn->response_buf_remove();
    }
#endif
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    result_list->update_sqls[roop_count].length(0);
  }
  str->length(0);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
#ifndef HANDLERSOCKET_MYSQL_UTIL
  result_list->hs_keys.clear();
#else
  result_list->hs_keys.elements = 0;
#endif
#endif
  DBUG_RETURN(0);
}
#endif

int spider_db_delete_all_rows(
  ha_spider *spider
) {
  int error_num, roop_count, tmp_pos;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->update_sql, *sql;
  DBUG_ENTER("spider_db_delete_all_rows");
  str->length(0);
  if (spider->sql_command == SQLCOM_TRUNCATE)
  {
    if ((error_num = spider_db_append_truncate(spider, 0)))
      DBUG_RETURN(error_num);
  } else {
    if (
      (error_num = spider_db_append_delete(str, spider)) ||
      (error_num = spider_db_append_from(str, spider, 0))
    )
      DBUG_RETURN(error_num);
  }

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
    if (spider->share->same_db_table_name || roop_count == 0)
      sql = str;
    else {
      sql = &result_list->update_sqls[roop_count];
      if (sql->copy(result_list->update_sql))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      tmp_pos = sql->length();
      sql->length(result_list->table_name_pos);
      spider_db_append_table_name_with_adjusting(spider, sql, share,
        roop_count, SPIDER_SQL_KIND_SQL);
      sql->length(tmp_pos);
    }
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[roop_count];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
      share);
    if (
      (error_num = spider_db_set_names(spider, conn, roop_count)) ||
      (
        spider_db_query(
          conn,
          sql->ptr(),
          sql->length(),
          &spider->need_mons[roop_count]) &&
        (error_num = spider_db_errorno(conn))
      )
    ) {
      if (
        error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
        !conn->disable_reconnect
      ) {
        /* retry */
        if ((error_num = spider_db_ping(spider, conn, roop_count)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          if (
            share->monitoring_kind[roop_count] &&
            spider->need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                spider->trx,
                spider->trx->thd,
                share,
                (uint32) share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                spider->conn_link_idx[roop_count],
                NULL,
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        if ((error_num = spider_db_set_names(spider, conn, roop_count)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          if (
            share->monitoring_kind[roop_count] &&
            spider->need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                spider->trx,
                spider->trx->thd,
                share,
                (uint32) share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                spider->conn_link_idx[roop_count],
                NULL,
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
          share);
        if (spider_db_query(
          conn,
          sql->ptr(),
          sql->length(),
          &spider->need_mons[roop_count])
        ) {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          error_num = spider_db_errorno(conn);
          if (
            share->monitoring_kind[roop_count] &&
            spider->need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                spider->trx,
                spider->trx->thd,
                share,
                (uint32) share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                spider->conn_link_idx[roop_count],
                NULL,
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      } else {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    result_list->update_sqls[roop_count].length(0);
  }
  str->length(0);
  DBUG_RETURN(0);
}

int spider_db_disable_keys(
  ha_spider *spider
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str;
  DBUG_ENTER("spider_db_disable_keys");
  if (
    spider_param_internal_optimize(spider->trx->thd,
      share->internal_optimize) == 1
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_count < (int) share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_disable_keys(spider, roop_count)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
        share);
      if (spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        &spider->need_mons[roop_count])
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conn);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_enable_keys(
  ha_spider *spider
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str;
  DBUG_ENTER("spider_db_enable_keys");
  if (
    spider_param_internal_optimize(spider->trx->thd,
      share->internal_optimize) == 1
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_count < (int) share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_enable_keys(spider, roop_count)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
        share);
      if (spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        &spider->need_mons[roop_count])
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conn);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_check_table(
  ha_spider *spider,
  HA_CHECK_OPT* check_opt
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str;
  DBUG_ENTER("spider_db_check_table");
  if (
    spider_param_internal_optimize(spider->trx->thd,
      share->internal_optimize) == 1
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_count < (int) share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_check_table(spider, roop_count,
        check_opt)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
        share);
      if (spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        &spider->need_mons[roop_count])
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conn);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_repair_table(
  ha_spider *spider,
  HA_CHECK_OPT* check_opt
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str;
  DBUG_ENTER("spider_db_repair_table");
  if (
    spider_param_internal_optimize(spider->trx->thd,
      share->internal_optimize) == 1
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_count < (int) share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_repair_table(spider, roop_count,
        check_opt)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
        share);
      if (spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        &spider->need_mons[roop_count])
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conn);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_analyze_table(
  ha_spider *spider
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str;
  DBUG_ENTER("spider_db_analyze_table");
  if (
    spider_param_internal_optimize(spider->trx->thd,
      share->internal_optimize) == 1
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_count < (int) share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_analyze_table(spider, roop_count)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
        share);
      if (spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        &spider->need_mons[roop_count])
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conn);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_optimize_table(
  ha_spider *spider
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str;
  DBUG_ENTER("spider_db_optimize_table");
  if (
    spider_param_internal_optimize(spider->trx->thd,
      share->internal_optimize) == 1
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, -1, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY);
      roop_count < (int) share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        spider->conn_link_idx, roop_count, share->link_count,
        SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_optimize_table(spider, roop_count)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
        share);
      if (spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        &spider->need_mons[roop_count])
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conn);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              (uint32) share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              spider->conn_link_idx[roop_count],
              NULL,
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_flush_tables(
  ha_spider *spider,
  bool lock
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  spider_string *str = &result_list->sql;
  DBUG_ENTER("spider_db_flush_tables");
  str->length(0);
  if ((error_num = spider_db_append_flush_tables(spider, lock)))
    DBUG_RETURN(error_num);

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
    spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
      share);
    if (spider_db_query(
      conn,
      str->ptr(),
      str->length(),
      &spider->need_mons[roop_count])
    ) {
      error_num = spider_db_errorno(conn);
      if (
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            (uint32) share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            spider->conn_link_idx[roop_count],
            NULL,
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

int spider_db_flush_logs(
  ha_spider *spider
) {
  int roop_count, error_num;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  DBUG_ENTER("spider_db_flush_logs");
  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, -1, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY);
    roop_count < (int) share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      spider->conn_link_idx, roop_count, share->link_count,
      SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
    spider_conn_set_timeout_from_share(conn, roop_count, spider->trx->thd,
      spider->share);
    if (spider_db_query(
      conn,
      SPIDER_SQL_FLUSH_LOGS_STR,
      SPIDER_SQL_FLUSH_LOGS_LEN,
      &spider->need_mons[roop_count])
    ) {
      error_num = spider_db_errorno(conn);
      if (
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            (uint32) share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            spider->conn_link_idx[roop_count],
            NULL,
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

int spider_db_print_item_type(
  Item *item,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  DBUG_ENTER("spider_db_print_item_type");
  DBUG_PRINT("info",("spider COND type=%d", item->type()));
  switch (item->type())
  {
    case Item::FUNC_ITEM:
      DBUG_RETURN(spider_db_open_item_func((Item_func *) item, spider, str,
        alias, alias_length));
    case Item::COND_ITEM:
      DBUG_RETURN(spider_db_open_item_cond((Item_cond *) item, spider, str,
        alias, alias_length));
    case Item::FIELD_ITEM:
      DBUG_RETURN(spider_db_open_item_field((Item_field *) item, spider, str,
        alias, alias_length));
    case Item::REF_ITEM:
      DBUG_RETURN(spider_db_open_item_ref((Item_ref *) item, spider, str,
        alias, alias_length));
    case Item::ROW_ITEM:
      DBUG_RETURN(spider_db_open_item_row((Item_row *) item, spider, str,
        alias, alias_length));
    case Item::STRING_ITEM:
      DBUG_RETURN(spider_db_open_item_string(item, spider, str,
        alias, alias_length));
    case Item::INT_ITEM:
    case Item::REAL_ITEM:
    case Item::DECIMAL_ITEM:
      DBUG_RETURN(spider_db_open_item_int(item, spider, str,
        alias, alias_length));
    case Item::CACHE_ITEM:
      DBUG_RETURN(spider_db_open_item_cache((Item_cache *)item, spider, str,
        alias, alias_length));
    case Item::SUBSELECT_ITEM:
    case Item::TRIGGER_FIELD_ITEM:
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    default:
      THD *thd = spider->trx->thd;
      SPIDER_SHARE *share = spider->share;
      if (spider_param_skip_default_condition(thd,
        share->skip_default_condition))
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      if (str)
      {
        if (spider->share->access_charset->cset == system_charset_info->cset)
        {
#if MYSQL_VERSION_ID < 50500
          item->print(str->get_str(), QT_IS);
#else
          item->print(str->get_str(), QT_TO_SYSTEM_CHARSET);
#endif
        } else {
          item->print(str->get_str(), QT_ORDINARY);
        }
        str->mem_calc();
      }
      break;
  }
  DBUG_RETURN(0);
}

int spider_db_open_item_cond(
  Item_cond *item_cond,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int error_num = 0;
  List_iterator_fast<Item> lif(*(item_cond->argument_list()));
  Item *item;
  char *func_name = NULL;
  int func_name_length = 0, restart_pos = 0;
  DBUG_ENTER("spider_db_open_item_cond");
  if (str)
  {
    if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  }

restart_first:
  if ((item = lif++))
  {
    if (str)
      restart_pos = str->length();
    if ((error_num = spider_db_print_item_type(item, spider, str,
      alias, alias_length)))
    {
      if (
        str &&
        error_num == ER_SPIDER_COND_SKIP_NUM &&
        item_cond->functype() == Item_func::COND_AND_FUNC
      ) {
        DBUG_PRINT("info",("spider COND skip"));
        str->length(restart_pos);
        goto restart_first;
      }
      DBUG_RETURN(error_num);
    }
  }
  if (error_num)
    DBUG_RETURN(error_num);
  while ((item = lif++))
  {
    if (str)
    {
      restart_pos = str->length();
      if (!func_name)
      {
        func_name = (char*) item_cond->func_name();
        func_name_length = strlen(func_name);
      }
      if (str->reserve(func_name_length + SPIDER_SQL_SPACE_LEN * 2))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
      str->q_append(func_name, func_name_length);
      str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    }

    if ((error_num = spider_db_print_item_type(item, spider, str,
      alias, alias_length)))
    {
      if (
        str &&
        error_num == ER_SPIDER_COND_SKIP_NUM &&
        item_cond->functype() == Item_func::COND_AND_FUNC
      ) {
        DBUG_PRINT("info",("spider COND skip"));
        str->length(restart_pos);
      } else
        DBUG_RETURN(error_num);
    }
  }
  if (str)
  {
    if (str->reserve(SPIDER_SQL_CLOSE_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_open_item_func(
  Item_func *item_func,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int error_num;
  Item *item, **item_list = item_func->arguments();
  uint roop_count, item_count = item_func->argument_count(), start_item = 0;
  const char *func_name = SPIDER_SQL_NULL_CHAR_STR,
    *separete_str = SPIDER_SQL_NULL_CHAR_STR,
    *last_str = SPIDER_SQL_NULL_CHAR_STR;
  int func_name_length = SPIDER_SQL_NULL_CHAR_LEN,
    separete_str_length = SPIDER_SQL_NULL_CHAR_LEN,
    last_str_length = SPIDER_SQL_NULL_CHAR_LEN;
  int use_pushdown_udf;
  DBUG_ENTER("spider_db_open_item_func");
  if (str)
  {
    if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  }
  DBUG_PRINT("info",("spider functype = %d", item_func->functype()));
  switch (item_func->functype())
  {
    case Item_func::ISNULL_FUNC:
      last_str = SPIDER_SQL_IS_NULL_STR;
      last_str_length = SPIDER_SQL_IS_NULL_LEN;
      break;
    case Item_func::ISNOTNULL_FUNC:
      last_str = SPIDER_SQL_IS_NOT_NULL_STR;
      last_str_length = SPIDER_SQL_IS_NOT_NULL_LEN;
      break;
    case Item_func::UNKNOWN_FUNC:
      func_name = (char*) item_func->func_name();
      func_name_length = strlen(func_name);
      DBUG_PRINT("info",("spider func_name = %s", func_name));
      DBUG_PRINT("info",("spider func_name_length = %d", func_name_length));
      if (func_name_length == 1 &&
        (
          !strncasecmp("+", func_name, func_name_length) ||
          !strncasecmp("-", func_name, func_name_length) ||
          !strncasecmp("*", func_name, func_name_length) ||
          !strncasecmp("/", func_name, func_name_length) ||
          !strncasecmp("%", func_name, func_name_length) ||
          !strncasecmp("&", func_name, func_name_length) ||
          !strncasecmp("|", func_name, func_name_length) ||
          !strncasecmp("^", func_name, func_name_length)
        )
      ) {
        /* no action */
        break;
      } else if (func_name_length == 2 &&
        (
          !strncasecmp("<<", func_name, func_name_length) ||
          !strncasecmp(">>", func_name, func_name_length)
        )
      ) {
        /* no action */
        break;
      } else if (func_name_length == 3 &&
        !strncasecmp("div", func_name, func_name_length)
      ) {
        /* no action */
        break;
      } else if (func_name_length == 4)
      {
        if (
          !strncasecmp("rand", func_name, func_name_length) &&
          !item_func->arg_count
        ) {
          if (str)
            str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          DBUG_RETURN(spider_db_open_item_int(item_func, spider, str,
            alias, alias_length));
        } else if (
          !strncasecmp("case", func_name, func_name_length)
        ) {
#ifdef ITEM_FUNC_CASE_PARAMS_ARE_PUBLIC
          Item_func_case *item_func_case = (Item_func_case *) item_func;
          if (str)
          {
            if (str->reserve(SPIDER_SQL_CASE_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_CASE_STR, SPIDER_SQL_CASE_LEN);
          }
          if (item_func_case->first_expr_num != -1)
          {
            if ((error_num = spider_db_print_item_type(
              item_list[item_func_case->first_expr_num], spider, str,
              alias, alias_length)))
              DBUG_RETURN(error_num);
          }
          for (roop_count = 0; roop_count < item_func_case->ncases;
            roop_count += 2)
          {
            if (str)
            {
              if (str->reserve(SPIDER_SQL_WHEN_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_WHEN_STR, SPIDER_SQL_WHEN_LEN);
            }
            if ((error_num = spider_db_print_item_type(
              item_list[roop_count], spider, str, alias, alias_length)))
              DBUG_RETURN(error_num);
            if (str)
            {
              if (str->reserve(SPIDER_SQL_THEN_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_THEN_STR, SPIDER_SQL_THEN_LEN);
            }
            if ((error_num = spider_db_print_item_type(
              item_list[roop_count + 1], spider, str, alias, alias_length)))
              DBUG_RETURN(error_num);
          }
          if (item_func_case->else_expr_num != -1)
          {
            if (str)
            {
              if (str->reserve(SPIDER_SQL_ELSE_LEN))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              str->q_append(SPIDER_SQL_ELSE_STR, SPIDER_SQL_ELSE_LEN);
            }
            if ((error_num = spider_db_print_item_type(
              item_list[item_func_case->else_expr_num], spider, str,
              alias, alias_length)))
              DBUG_RETURN(error_num);
          }
          if (str)
          {
            if (str->reserve(SPIDER_SQL_END_LEN + SPIDER_SQL_CLOSE_PAREN_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_END_STR, SPIDER_SQL_END_LEN);
            str->q_append(SPIDER_SQL_CLOSE_PAREN_STR,
              SPIDER_SQL_CLOSE_PAREN_LEN);
          }
          DBUG_RETURN(0);
#else
          DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
#endif
        }
      } else if (func_name_length == 6 &&
        !strncasecmp("istrue", func_name, func_name_length)
      ) {
        last_str = SPIDER_SQL_IS_TRUE_STR;
        last_str_length = SPIDER_SQL_IS_TRUE_LEN;
        break;
      } else if (func_name_length == 7)
      {
        if (!strncasecmp("isfalse", func_name, func_name_length))
        {
          last_str = SPIDER_SQL_IS_FALSE_STR;
          last_str_length = SPIDER_SQL_IS_FALSE_LEN;
          break;
        } else if (
          !strncasecmp("sysdate", func_name, func_name_length) ||
          !strncasecmp("curdate", func_name, func_name_length) ||
          !strncasecmp("curtime", func_name, func_name_length)
        ) {
          if (str)
            str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          DBUG_RETURN(spider_db_open_item_string(item_func, spider, str,
            alias, alias_length));
        }
      } else if (func_name_length == 8 &&
        (
          !strncasecmp("utc_date", func_name, func_name_length) ||
          !strncasecmp("utc_time", func_name, func_name_length)
        )
      ) {
        if (str)
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
        DBUG_RETURN(spider_db_open_item_string(item_func, spider, str,
          alias, alias_length));
      } else if (func_name_length == 9 &&
        !strncasecmp("isnottrue", func_name, func_name_length)
      ) {
        last_str = SPIDER_SQL_IS_NOT_TRUE_STR;
        last_str_length = SPIDER_SQL_IS_NOT_TRUE_LEN;
        break;
      } else if (func_name_length == 10 &&
        !strncasecmp("isnotfalse", func_name, func_name_length)
      ) {
        last_str = SPIDER_SQL_IS_NOT_FALSE_STR;
        last_str_length = SPIDER_SQL_IS_NOT_FALSE_LEN;
        break;
      } else if (func_name_length == 12)
      {
        if (!strncasecmp("cast_as_date", func_name, func_name_length))
        {
          if (str)
          {
            str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
            if (str->reserve(SPIDER_SQL_CAST_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          }
          last_str = SPIDER_SQL_AS_DATE_STR;
          last_str_length = SPIDER_SQL_AS_DATE_LEN;
          break;
        } else if (!strncasecmp("cast_as_time", func_name, func_name_length))
        {
          if (str)
          {
            str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
            if (str->reserve(SPIDER_SQL_CAST_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          }
          last_str = SPIDER_SQL_AS_TIME_STR;
          last_str_length = SPIDER_SQL_AS_TIME_LEN;
          break;
        }
      } else if (func_name_length == 13 &&
        !strncasecmp("utc_timestamp", func_name, func_name_length)
      ) {
        if (str)
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
        DBUG_RETURN(spider_db_open_item_string(item_func, spider, str,
          alias, alias_length));
      } else if (func_name_length == 14)
      {
        if (!strncasecmp("cast_as_binary", func_name, func_name_length))
        {
          if (str)
          {
            char tmp_buf[MAX_FIELD_WIDTH], *tmp_ptr, *tmp_ptr2;
            spider_string tmp_str(tmp_buf, MAX_FIELD_WIDTH, str->charset());
            tmp_str.init_calc_mem(123);
            tmp_str.length(0);
            str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
            if (str->reserve(SPIDER_SQL_CAST_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
#if MYSQL_VERSION_ID < 50500
            item_func->print(tmp_str.get_str(), QT_IS);
#else
            item_func->print(tmp_str.get_str(), QT_TO_SYSTEM_CHARSET);
#endif
            tmp_str.mem_calc();
            if (tmp_str.reserve(1))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            tmp_ptr = tmp_str.c_ptr_quick();
            DBUG_PRINT("info",("spider tmp_ptr = %s", tmp_ptr));
            while ((tmp_ptr2 = strstr(tmp_ptr, SPIDER_SQL_AS_BINARY_STR)))
              tmp_ptr = tmp_ptr2 + 1;
            last_str = tmp_ptr - 1;
            last_str_length = strlen(last_str) - SPIDER_SQL_CLOSE_PAREN_LEN;
          }
          break;
        } else if (!strncasecmp("cast_as_signed", func_name, func_name_length))
        {
          if (str)
          {
            str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
            if (str->reserve(SPIDER_SQL_CAST_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          }
          last_str = SPIDER_SQL_AS_SIGNED_STR;
          last_str_length = SPIDER_SQL_AS_SIGNED_LEN;
          break;
        }
      } else if (func_name_length == 16)
      {
        if (!strncasecmp("cast_as_unsigned", func_name, func_name_length))
        {
          if (str)
          {
            str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
            if (str->reserve(SPIDER_SQL_CAST_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          }
          last_str = SPIDER_SQL_AS_UNSIGNED_STR;
          last_str_length = SPIDER_SQL_AS_UNSIGNED_LEN;
          break;
        } else if (!strncasecmp("decimal_typecast", func_name,
          func_name_length))
        {
          if (str)
          {
            char tmp_buf[MAX_FIELD_WIDTH], *tmp_ptr, *tmp_ptr2;
            spider_string tmp_str(tmp_buf, MAX_FIELD_WIDTH, str->charset());
            tmp_str.init_calc_mem(124);
            tmp_str.length(0);
            str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
            if (str->reserve(SPIDER_SQL_CAST_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
#if MYSQL_VERSION_ID < 50500
            item_func->print(tmp_str.get_str(), QT_IS);
#else
            item_func->print(tmp_str.get_str(), QT_TO_SYSTEM_CHARSET);
#endif
            tmp_str.mem_calc();
            if (tmp_str.reserve(1))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            tmp_ptr = tmp_str.c_ptr_quick();
            DBUG_PRINT("info",("spider tmp_ptr = %s", tmp_ptr));
            while ((tmp_ptr2 = strstr(tmp_ptr, SPIDER_SQL_AS_DECIMAL_STR)))
              tmp_ptr = tmp_ptr2 + 1;
            last_str = tmp_ptr - 1;
            last_str_length = strlen(last_str) - SPIDER_SQL_CLOSE_PAREN_LEN;
          }
          break;
        } else if (!strncasecmp("cast_as_datetime", func_name,
          func_name_length))
        {
          if (str)
          {
            str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
            if (str->reserve(SPIDER_SQL_CAST_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          }
          last_str = SPIDER_SQL_AS_DATETIME_STR;
          last_str_length = SPIDER_SQL_AS_DATETIME_LEN;
          break;
        }
      } else if (func_name_length == 17)
      {
        if (!strncasecmp("date_add_interval", func_name, func_name_length))
        {
          func_name = SPIDER_SQL_DATE_ADD_STR;
          func_name_length = SPIDER_SQL_DATE_ADD_LEN;
        }
      }
      if (str)
      {
        if (str->reserve(func_name_length + SPIDER_SQL_OPEN_PAREN_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(func_name, func_name_length);
        str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
      }
      func_name = SPIDER_SQL_COMMA_STR;
      func_name_length = SPIDER_SQL_COMMA_LEN;
      separete_str = SPIDER_SQL_COMMA_STR;
      separete_str_length = SPIDER_SQL_COMMA_LEN;
      last_str = SPIDER_SQL_CLOSE_PAREN_STR;
      last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      break;
    case Item_func::NOW_FUNC:
      if (str)
        str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
      DBUG_RETURN(spider_db_open_item_string(item_func, spider, str,
        alias, alias_length));
    case Item_func::CHAR_TYPECAST_FUNC:
      {
        if (str)
        {
          char tmp_buf[MAX_FIELD_WIDTH], *tmp_ptr, *tmp_ptr2;
          spider_string tmp_str(tmp_buf, MAX_FIELD_WIDTH, str->charset());
          tmp_str.init_calc_mem(125);
          tmp_str.length(0);
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          if (str->reserve(SPIDER_SQL_CAST_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
#if MYSQL_VERSION_ID < 50500
          item_func->print(tmp_str.get_str(), QT_IS);
#else
          item_func->print(tmp_str.get_str(), QT_TO_SYSTEM_CHARSET);
#endif
          tmp_str.mem_calc();
          if (tmp_str.reserve(1))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          tmp_ptr = tmp_str.c_ptr_quick();
          DBUG_PRINT("info",("spider tmp_ptr = %s", tmp_ptr));
          while ((tmp_ptr2 = strstr(tmp_ptr, SPIDER_SQL_AS_CHAR_STR)))
            tmp_ptr = tmp_ptr2 + 1;
          last_str = tmp_ptr - 1;
          last_str_length = strlen(last_str) - SPIDER_SQL_CLOSE_PAREN_LEN;
        }
      }
      break;
    case Item_func::NOT_FUNC:
    case Item_func::NEG_FUNC:
      if (str)
      {
        func_name = (char*) item_func->func_name();
        func_name_length = strlen(func_name);
        if (str->reserve(func_name_length + SPIDER_SQL_SPACE_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(func_name, func_name_length);
        str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
      }
      break;
    case Item_func::IN_FUNC:
      if (((Item_func_opt_neg *) item_func)->negated)
      {
        func_name = SPIDER_SQL_NOT_IN_STR;
        func_name_length = SPIDER_SQL_NOT_IN_LEN;
        separete_str = SPIDER_SQL_COMMA_STR;
        separete_str_length = SPIDER_SQL_COMMA_LEN;
        last_str = SPIDER_SQL_CLOSE_PAREN_STR;
        last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      } else {
        func_name = SPIDER_SQL_IN_STR;
        func_name_length = SPIDER_SQL_IN_LEN;
        separete_str = SPIDER_SQL_COMMA_STR;
        separete_str_length = SPIDER_SQL_COMMA_LEN;
        last_str = SPIDER_SQL_CLOSE_PAREN_STR;
        last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      }
      break;
    case Item_func::BETWEEN:
      if (((Item_func_opt_neg *) item_func)->negated)
      {
        func_name = SPIDER_SQL_NOT_BETWEEN_STR;
        func_name_length = SPIDER_SQL_NOT_BETWEEN_LEN;
        separete_str = SPIDER_SQL_AND_STR;
        separete_str_length = SPIDER_SQL_AND_LEN;
      } else {
        func_name = (char*) item_func->func_name();
        func_name_length = strlen(func_name);
        separete_str = SPIDER_SQL_AND_STR;
        separete_str_length = SPIDER_SQL_AND_LEN;
      }
      break;
    case Item_func::UDF_FUNC:
      use_pushdown_udf = spider_param_use_pushdown_udf(spider->trx->thd,
        spider->share->use_pushdown_udf);
      if (!use_pushdown_udf)
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      if (str)
      {
        func_name = (char*) item_func->func_name();
        func_name_length = strlen(func_name);
        DBUG_PRINT("info",("spider func_name = %s", func_name));
        DBUG_PRINT("info",("spider func_name_length = %d", func_name_length));
        if (str->reserve(func_name_length + SPIDER_SQL_OPEN_PAREN_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(func_name, func_name_length);
        str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
      }
      func_name = SPIDER_SQL_COMMA_STR;
      func_name_length = SPIDER_SQL_COMMA_LEN;
      separete_str = SPIDER_SQL_COMMA_STR;
      separete_str_length = SPIDER_SQL_COMMA_LEN;
      last_str = SPIDER_SQL_CLOSE_PAREN_STR;
      last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      break;
#ifdef MARIADB_BASE_VERSION
    case Item_func::XOR_FUNC:
#else
    case Item_func::COND_XOR_FUNC:
#endif
      if (str)
        str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
      DBUG_RETURN(
        spider_db_open_item_cond((Item_cond *) item_func, spider, str,
          alias, alias_length));
    case Item_func::TRIG_COND_FUNC:
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::GUSERVAR_FUNC:
      if (str)
        str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
      if (item_func->result_type() == STRING_RESULT)
        DBUG_RETURN(spider_db_open_item_string(item_func, spider, str,
          alias, alias_length));
      else
        DBUG_RETURN(spider_db_open_item_int(item_func, spider, str,
          alias, alias_length));
    case Item_func::FT_FUNC:
      if (spider_db_check_ft_idx(item_func, spider) == MAX_KEY)
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      start_item = 1;
      if (str)
      {
        if (str->reserve(SPIDER_SQL_MATCH_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_MATCH_STR, SPIDER_SQL_MATCH_LEN);
      }
      separete_str = SPIDER_SQL_COMMA_STR;
      separete_str_length = SPIDER_SQL_COMMA_LEN;
      last_str = SPIDER_SQL_CLOSE_PAREN_STR;
      last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      break;
    case Item_func::SP_EQUALS_FUNC:
      if (str)
      {
        func_name = SPIDER_SQL_MBR_EQUAL_STR;
        func_name_length = SPIDER_SQL_MBR_EQUAL_LEN;
        DBUG_PRINT("info",("spider func_name = %s", func_name));
        DBUG_PRINT("info",("spider func_name_length = %d", func_name_length));
        if (str->reserve(func_name_length))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(func_name, func_name_length);
      }
      func_name = SPIDER_SQL_COMMA_STR;
      func_name_length = SPIDER_SQL_COMMA_LEN;
      separete_str = SPIDER_SQL_COMMA_STR;
      separete_str_length = SPIDER_SQL_COMMA_LEN;
      last_str = SPIDER_SQL_CLOSE_PAREN_STR;
      last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      break;
    case Item_func::SP_DISJOINT_FUNC:
    case Item_func::SP_INTERSECTS_FUNC:
    case Item_func::SP_TOUCHES_FUNC:
    case Item_func::SP_CROSSES_FUNC:
    case Item_func::SP_WITHIN_FUNC:
    case Item_func::SP_CONTAINS_FUNC:
    case Item_func::SP_OVERLAPS_FUNC:
      if (str)
      {
        func_name = (char*) item_func->func_name();
        func_name_length = strlen(func_name);
        DBUG_PRINT("info",("spider func_name = %s", func_name));
        DBUG_PRINT("info",("spider func_name_length = %d", func_name_length));
        if (str->reserve(SPIDER_SQL_MBR_LEN + func_name_length +
          SPIDER_SQL_OPEN_PAREN_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_MBR_STR, SPIDER_SQL_MBR_LEN);
        str->q_append(func_name, func_name_length);
        str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
      }
      func_name = SPIDER_SQL_COMMA_STR;
      func_name_length = SPIDER_SQL_COMMA_LEN;
      separete_str = SPIDER_SQL_COMMA_STR;
      separete_str_length = SPIDER_SQL_COMMA_LEN;
      last_str = SPIDER_SQL_CLOSE_PAREN_STR;
      last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      break;
    case Item_func::EQ_FUNC:
    case Item_func::EQUAL_FUNC:
    case Item_func::NE_FUNC:
    case Item_func::LT_FUNC:
    case Item_func::LE_FUNC:
    case Item_func::GE_FUNC:
    case Item_func::GT_FUNC:
    case Item_func::LIKE_FUNC:
      if (str)
      {
        func_name = (char*) item_func->func_name();
        func_name_length = strlen(func_name);
      }
      break;
    default:
      THD *thd = spider->trx->thd;
      SPIDER_SHARE *share = spider->share;
      if (spider_param_skip_default_condition(thd,
        share->skip_default_condition))
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      if (str)
      {
        func_name = (char*) item_func->func_name();
        func_name_length = strlen(func_name);
      }
      break;
  }
  DBUG_PRINT("info",("spider func_name = %s", func_name));
  DBUG_PRINT("info",("spider func_name_length = %d", func_name_length));
  DBUG_PRINT("info",("spider separete_str = %s", separete_str));
  DBUG_PRINT("info",("spider separete_str_length = %d", separete_str_length));
  DBUG_PRINT("info",("spider last_str = %s", last_str));
  DBUG_PRINT("info",("spider last_str_length = %d", last_str_length));
  if (item_count)
  {
    item_count--;
    for (roop_count = start_item; roop_count < item_count; roop_count++)
    {
      item = item_list[roop_count];
      if ((error_num = spider_db_print_item_type(item, spider, str,
        alias, alias_length)))
        DBUG_RETURN(error_num);
      if (roop_count == 1)
      {
        func_name = separete_str;
        func_name_length = separete_str_length;
      }
      if (str)
      {
        if (str->reserve(func_name_length + SPIDER_SQL_SPACE_LEN * 2))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
        str->q_append(func_name, func_name_length);
        str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
      }
    }
    item = item_list[roop_count];
    if ((error_num = spider_db_print_item_type(item, spider, str,
      alias, alias_length)))
      DBUG_RETURN(error_num);
  }
  if (item_func->functype() == Item_func::FT_FUNC)
  {
    Item_func_match *item_func_match = (Item_func_match *)item_func;
    if (str)
    {
      if (str->reserve(SPIDER_SQL_AGAINST_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_AGAINST_STR, SPIDER_SQL_AGAINST_LEN);
    }
    item = item_list[0];
    if ((error_num = spider_db_print_item_type(item, spider, str,
      alias, alias_length)))
      DBUG_RETURN(error_num);
    if (str)
    {
      if (str->reserve(
        ((item_func_match->flags & FT_BOOL) ?
          SPIDER_SQL_IN_BOOLEAN_MODE_LEN : 0) +
        ((item_func_match->flags & FT_EXPAND) ?
          SPIDER_SQL_WITH_QUERY_EXPANSION_LEN : 0)
      ))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      if (item_func_match->flags & FT_BOOL)
        str->q_append(SPIDER_SQL_IN_BOOLEAN_MODE_STR,
          SPIDER_SQL_IN_BOOLEAN_MODE_LEN);
      if (item_func_match->flags & FT_EXPAND)
        str->q_append(SPIDER_SQL_WITH_QUERY_EXPANSION_STR,
          SPIDER_SQL_WITH_QUERY_EXPANSION_LEN);
    }
  }
  if (str)
  {
    if (str->reserve(last_str_length + SPIDER_SQL_CLOSE_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(last_str, last_str_length);
    str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_open_item_ident(
  Item_ident *item_ident,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int field_name_length;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_open_item_ident");
  if (
    item_ident->cached_field_index != NO_CACHED_FIELD_INDEX &&
    item_ident->cached_table
  ) {
    Field *field = item_ident->cached_table->table->field[
      item_ident->cached_field_index];
    if (!(field = spider->field_exchange(field)))
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    DBUG_PRINT("info",("spider use cached_field_index"));
    if (str)
    {
      if (str->reserve(
        alias_length +
        share->column_name_str[field->field_index].length() +
        (SPIDER_SQL_NAME_QUOTE_LEN) * 2))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(alias, alias_length);
      spider_db_append_column_name(share, str, field->field_index);
    }
    DBUG_RETURN(0);
  }
  if (str)
  {
    if (item_ident->field_name)
      field_name_length = strlen(item_ident->field_name);
    else
      field_name_length = 0;
    if (spider->share->access_charset->cset == system_charset_info->cset)
    {
      if (str->reserve(alias_length + (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
        field_name_length))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(alias, alias_length);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      str->q_append(item_ident->field_name, field_name_length);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    } else {
      if (str->reserve(alias_length + SPIDER_SQL_NAME_QUOTE_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(alias, alias_length);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      str->append(item_ident->field_name, field_name_length,
        system_charset_info);
      if (str->reserve(SPIDER_SQL_NAME_QUOTE_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_open_item_field(
  Item_field *item_field,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  Field *field = item_field->field;
  SPIDER_SHARE *share;
  DBUG_ENTER("spider_db_open_item_field");
  if (field)
  {
    DBUG_PRINT("info",("spider field=%p", field));
    if (!(field = spider->field_exchange(field)))
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    if (field->table->const_table)
    {
      if (str)
      {
        share = spider->share;
        if (str->reserve(
          alias_length +
          share->column_name_str[field->field_index].length() +
          (SPIDER_SQL_NAME_QUOTE_LEN) * 2))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(alias, alias_length);
        spider_db_append_column_name(share, str,
          field->field_index);
      }
      DBUG_RETURN(0);
    }
  }
  DBUG_RETURN(spider_db_open_item_ident(
    (Item_ident *) item_field, spider, str, alias, alias_length));
}

int spider_db_open_item_ref(
  Item_ref *item_ref,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  DBUG_ENTER("spider_db_open_item_ref");
  if (item_ref->ref)
  {
    if (
      (*(item_ref->ref))->type() != Item::CACHE_ITEM &&
      item_ref->ref_type() != Item_ref::VIEW_REF &&
      !item_ref->table_name &&
      item_ref->name &&
      item_ref->alias_name_used
    ) {
      if (str)
      {
        int name_length = strlen(item_ref->name);
        if (str->reserve(SPIDER_SQL_NAME_QUOTE_LEN * 2 + name_length))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
        str->q_append(item_ref->name, name_length);
        str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      }
      DBUG_RETURN(0);
    }
    DBUG_RETURN(spider_db_print_item_type(*(item_ref->ref), spider, str,
      alias, alias_length));
  }
  DBUG_RETURN(spider_db_open_item_ident((Item_ident *) item_ref, spider, str,
    alias, alias_length));
}

int spider_db_open_item_row(
  Item_row *item_row,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int error_num;
  uint roop_count, cols = item_row->cols() - 1;
  Item *item;
  DBUG_ENTER("spider_db_open_item_row");
  for (roop_count = 0; roop_count < cols; roop_count++)
  {
    item = item_row->element_index(roop_count);
    if ((error_num = spider_db_print_item_type(item, spider, str,
      alias, alias_length)))
      DBUG_RETURN(error_num);
    if (str)
    {
      if (str->reserve(SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
  }
  item = item_row->element_index(roop_count);
  if ((error_num = spider_db_print_item_type(item, spider, str,
    alias, alias_length)))
    DBUG_RETURN(error_num);
  DBUG_RETURN(0);
}

int spider_db_open_item_string(
  Item *item,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  DBUG_ENTER("spider_db_open_item_string");
  if (str)
  {
    char tmp_buf[MAX_FIELD_WIDTH];
    spider_string tmp_str(tmp_buf, MAX_FIELD_WIDTH, str->charset());
    String *tmp_str2;
    tmp_str.init_calc_mem(126);
    if (
      !(tmp_str2 = item->val_str(tmp_str.get_str())) ||
      str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN * 2 + tmp_str2->length() * 2)
    )
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    tmp_str.mem_calc();
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    if (
      append_escaped(str->get_str(), tmp_str2) ||
      str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN)
    )
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->mem_calc();
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_open_item_int(
  Item *item,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  DBUG_ENTER("spider_db_open_item_int");
  if (str)
  {
    char tmp_buf[MAX_FIELD_WIDTH];
    spider_string tmp_str(tmp_buf, MAX_FIELD_WIDTH, str->charset());
    tmp_str.init_calc_mem(127);
    if (str->append(*item->val_str(tmp_str.get_str())))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    tmp_str.mem_calc();
  }
  DBUG_RETURN(0);
}

int spider_db_open_item_cache(
  Item_cache *item_cache,
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  DBUG_ENTER("spider_db_open_item_cache");
  if (!item_cache->const_item())
    DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
  if (item_cache->result_type() == STRING_RESULT)
    DBUG_RETURN(spider_db_open_item_string(item_cache, spider, str,
      alias, alias_length));
  DBUG_RETURN(spider_db_open_item_int(item_cache, spider, str,
    alias, alias_length));
}

int spider_db_append_condition_internal(
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length,
  uint sql_kind
) {
  int error_num, restart_pos = 0;
  SPIDER_CONDITION *tmp_cond = spider->condition;
  bool where_pos = FALSE;
  DBUG_ENTER("spider_db_append_condition_internal");
  if (sql_kind == SPIDER_SQL_KIND_SQL)
  {
    if (str)
      where_pos = ((int) str->length() == spider->result_list.where_pos);
  } else if (sql_kind == SPIDER_SQL_KIND_HANDLER)
  {
    if (str)
    {
      where_pos = TRUE;
      str = &spider->result_list.ha_sql;
      if (spider->active_index == MAX_KEY)
      {
        spider->result_list.ha_read_pos = str->length();
        if (str->reserve(SPIDER_SQL_READ_LEN + SPIDER_SQL_FIRST_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_READ_STR, SPIDER_SQL_READ_LEN);
        spider->result_list.ha_next_pos = str->length();
        str->q_append(SPIDER_SQL_FIRST_STR, SPIDER_SQL_FIRST_LEN);
        spider->result_list.sql_part2.length(0);
      }
      spider->result_list.ha_where_pos = str->length();

      if (spider->result_list.sql_part2.length())
      {
        str->append(spider->result_list.sql_part2);
        where_pos = FALSE;
      }
    }
  } else
    DBUG_RETURN(0);

  while (tmp_cond)
  {
    if (str)
    {
      restart_pos = str->length();
      if (where_pos)
      {
        if (str->reserve(SPIDER_SQL_WHERE_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
        where_pos = FALSE;
      } else {
        if (str->reserve(SPIDER_SQL_AND_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
      }
    }
    if ((error_num = spider_db_print_item_type(
      (Item *) tmp_cond->cond, spider, str, alias, alias_length)))
    {
      if (str && error_num == ER_SPIDER_COND_SKIP_NUM)
      {
        DBUG_PRINT("info",("spider COND skip"));
        str->length(restart_pos);
        where_pos = ((int) str->length() == spider->result_list.where_pos);
      } else
        DBUG_RETURN(error_num);
    }
    tmp_cond = tmp_cond->next;
  }
  DBUG_RETURN(0);
}

int spider_db_append_condition(
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int error_num;
  DBUG_ENTER("spider_db_append_condition");
  if (str)
  {
    if (spider->sql_kinds & SPIDER_SQL_KIND_SQL)
    {
      if ((error_num = spider_db_append_condition_internal(spider, str,
        alias, alias_length, SPIDER_SQL_KIND_SQL)))
        DBUG_RETURN(error_num);
    }
    if (spider->sql_kinds & SPIDER_SQL_KIND_HANDLER)
    {
      if ((error_num = spider_db_append_condition_internal(spider, str,
        alias, alias_length, SPIDER_SQL_KIND_HANDLER)))
        DBUG_RETURN(error_num);
    }
  } else {
    if (spider->cond_check)
      DBUG_RETURN(spider->cond_check_error);
    spider->cond_check = TRUE;
    if ((spider->cond_check_error = spider_db_append_condition_internal(spider,
      str, alias, alias_length, SPIDER_SQL_KIND_SQL)))
      DBUG_RETURN(spider->cond_check_error);
  }
  DBUG_RETURN(0);
}

int spider_db_append_update_columns(
  ha_spider *spider,
  spider_string *str,
  const char *alias,
  uint alias_length
) {
  int error_num;
  List_iterator_fast<Item> fi(*spider->direct_update_fields),
    vi(*spider->direct_update_values);
  Item *field, *value;
  DBUG_ENTER("spider_db_append_update_columns");
  while ((field = fi++))
  {
    value = vi++;
    if ((error_num = spider_db_print_item_type(
      (Item *) field, spider, str, alias, alias_length)))
    {
      if (
        error_num == ER_SPIDER_COND_SKIP_NUM &&
        field->type() == Item::FIELD_ITEM &&
        ((Item_field *) field)->field
      )
        continue;
      DBUG_RETURN(error_num);
    }
    if (str)
    {
      if (str->reserve(SPIDER_SQL_EQUAL_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
    }
    if ((error_num = spider_db_print_item_type(
      (Item *) value, spider, str, alias, alias_length)))
      DBUG_RETURN(error_num);
    if (str)
    {
      if (str->reserve(SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
    }
  }
  if (str)
    str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

uint spider_db_check_ft_idx(
  Item_func *item_func,
  ha_spider *spider
) {
  uint roop_count, roop_count2, part_num;
  uint item_count = item_func->argument_count();
  Item **item_list = item_func->arguments();
  Item_field *item_field;
  Field *field;
  TABLE *table = spider->get_table();
  TABLE_SHARE *table_share = table->s;
  KEY *key_info;
  KEY_PART_INFO *key_part;
  bool match1, match2;
  DBUG_ENTER("spider_db_check_ft_idx");

  for (roop_count = 0; roop_count < table_share->keys; roop_count++)
  {
    key_info = &table->key_info[roop_count];
    if (
      key_info->algorithm == HA_KEY_ALG_FULLTEXT &&
      item_count - 1 == key_info->key_parts
    ) {
      match1 = TRUE;
      for (roop_count2 = 1; roop_count2 < item_count; roop_count2++)
      {
        item_field = (Item_field *) item_list[roop_count2];
        field = item_field->field;
        if (!(field = spider->field_exchange(field)))
          DBUG_RETURN(MAX_KEY);
        match2 = FALSE;
        for (key_part = key_info->key_part, part_num = 0;
          part_num < key_info->key_parts; key_part++, part_num++)
        {
          if (key_part->field == field)
          {
            match2 = TRUE;
            break;
          }
        }
        if (!match2)
        {
          match1 = FALSE;
          break;
        }
      }
      if (match1)
        DBUG_RETURN(roop_count);
    }
  }
  DBUG_RETURN(MAX_KEY);
}

int spider_db_udf_fetch_row(
  SPIDER_TRX *trx,
  Field *field,
  SPIDER_DB_ROW row,
  ulong *length
) {
  DBUG_ENTER("spider_db_udf_fetch_row");
  if (!*row)
  {
    field->set_null();
    field->reset();
  } else {
    field->set_notnull();
    field->store(*row, *length, trx->udf_access_charset);
  }
  DBUG_RETURN(0);
}

#ifdef HAVE_HANDLERSOCKET
int spider_db_udf_fetch_hs_row(
  SPIDER_TRX *trx,
  Field *field,
  const SPIDER_HS_STRING_REF &hs_row
) {
  DBUG_ENTER("spider_db_udf_fetch_hs_row");
  if (!hs_row.begin())
  {
    field->set_null();
    field->reset();
  } else {
    field->set_notnull();
    field->store(hs_row.begin(), hs_row.size(), &my_charset_bin);
  }
  DBUG_RETURN(0);
}
#endif

int spider_db_udf_fetch_table(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn,
  TABLE *table,
  SPIDER_DB_RESULT *result,
  uint set_on,
  uint set_off
) {
  int error_num;
  SPIDER_DB_ROW row = NULL;
#ifdef HAVE_HANDLERSOCKET
  const SPIDER_HS_STRING_REF *hs_row = NULL;
#endif
  ulong *lengths = NULL;
  Field **field;
  uint roop_count;
  DBUG_ENTER("spider_db_udf_fetch_table");
#ifdef HAVE_HANDLERSOCKET
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if (!(row = mysql_fetch_row(result)))
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    lengths = mysql_fetch_lengths(result);
#ifdef HAVE_HANDLERSOCKET
  } else {
    if (!(hs_row = conn->hs_conn->get_next_row()))
    {
      table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
  }
#endif

#ifndef DBUG_OFF
  my_bitmap_map *tmp_map =
    dbug_tmp_use_all_columns(table, table->write_set);
#endif
#ifdef HAVE_HANDLERSOCKET
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    for (
      roop_count = 0,
      field = table->field;
      roop_count < set_on;
      roop_count++,
      field++,
      lengths++
    ) {
      if ((error_num =
        spider_db_udf_fetch_row(trx, *field, row, lengths)))
      {
#ifndef DBUG_OFF
        dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
        DBUG_RETURN(error_num);
      }
      row++;
    }
#ifdef HAVE_HANDLERSOCKET
  } else {
    for (
      roop_count = 0,
      field = table->field;
      roop_count < set_on;
      roop_count++,
      field++
    ) {
      if ((error_num =
        spider_db_udf_fetch_hs_row(trx, *field, hs_row[roop_count])))
      {
#ifndef DBUG_OFF
        dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
        DBUG_RETURN(error_num);
      }
    }
  }
#endif

  for (; roop_count < set_off; roop_count++, field++)
    (*field)->set_default();
#ifndef DBUG_OFF
  dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
  table->status = 0;
  DBUG_RETURN(0);
}

int spider_db_udf_direct_sql_connect(
  const SPIDER_DIRECT_SQL *direct_sql,
  SPIDER_CONN *conn
) {
  int error_num, connect_retry_count;
  THD* thd = current_thd;
  longlong connect_retry_interval;
  my_bool connect_mutex = spider_param_connect_mutex();
  DBUG_ENTER("spider_db_udf_direct_sql_connect");

  if (thd)
  {
    conn->connect_timeout = spider_param_connect_timeout(thd,
      direct_sql->connect_timeout);
    conn->net_read_timeout = spider_param_net_read_timeout(thd,
      direct_sql->net_read_timeout);
    conn->net_write_timeout = spider_param_net_write_timeout(thd,
      direct_sql->net_write_timeout);
    connect_retry_interval = spider_param_connect_retry_interval(thd);
    connect_retry_count = spider_param_connect_retry_count(thd);
  } else {
    conn->connect_timeout = spider_param_connect_timeout(NULL,
      direct_sql->connect_timeout);
    conn->net_read_timeout = spider_param_net_read_timeout(NULL,
      direct_sql->net_read_timeout);
    conn->net_write_timeout = spider_param_net_write_timeout(NULL,
      direct_sql->net_write_timeout);
    connect_retry_interval = spider_param_connect_retry_interval(NULL);
    connect_retry_count = spider_param_connect_retry_count(NULL);
  }
  DBUG_PRINT("info",("spider connect_timeout=%u", conn->connect_timeout));
  DBUG_PRINT("info",("spider net_read_timeout=%u", conn->net_read_timeout));
  DBUG_PRINT("info",("spider net_write_timeout=%u", conn->net_write_timeout));

#ifdef HAVE_HANDLERSOCKET
  if (direct_sql->access_mode == 0)
  {
#endif
    if ((error_num = spider_reset_conn_setted_parameter(conn, thd)))
      DBUG_RETURN(error_num);
#ifdef HAVE_HANDLERSOCKET
  }
#endif

#ifdef HAVE_HANDLERSOCKET
  if (direct_sql->access_mode == 0)
  {
#endif
    while (TRUE)
    {
      if (!(conn->db_conn = mysql_init(NULL)))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);

      mysql_options(conn->db_conn, MYSQL_OPT_READ_TIMEOUT,
        &conn->net_read_timeout);
      mysql_options(conn->db_conn, MYSQL_OPT_WRITE_TIMEOUT,
        &conn->net_write_timeout);
      mysql_options(conn->db_conn, MYSQL_OPT_CONNECT_TIMEOUT,
        &conn->connect_timeout);

      if (
        conn->tgt_ssl_ca_length |
        conn->tgt_ssl_capath_length |
        conn->tgt_ssl_cert_length |
        conn->tgt_ssl_key_length
      ) {
        mysql_ssl_set(conn->db_conn, conn->tgt_ssl_key, conn->tgt_ssl_cert,
          conn->tgt_ssl_ca, conn->tgt_ssl_capath, conn->tgt_ssl_cipher);
        if (conn->tgt_ssl_vsc)
        {
          my_bool verify_flg = TRUE;
          mysql_options(conn->db_conn, MYSQL_OPT_SSL_VERIFY_SERVER_CERT,
            &verify_flg);
        }
      }

      if (conn->tgt_default_file)
      {
        DBUG_PRINT("info",("spider tgt_default_file=%s",
          conn->tgt_default_file));
        mysql_options(conn->db_conn, MYSQL_READ_DEFAULT_FILE,
          conn->tgt_default_file);
      }
      if (conn->tgt_default_group)
      {
        DBUG_PRINT("info",("spider tgt_default_group=%s",
          conn->tgt_default_group));
        mysql_options(conn->db_conn, MYSQL_READ_DEFAULT_GROUP,
          conn->tgt_default_group);
      }

      if (connect_mutex)
        pthread_mutex_lock(&spider_open_conn_mutex);
      if (!mysql_real_connect(conn->db_conn,
                              direct_sql->tgt_host,
                              direct_sql->tgt_username,
                              direct_sql->tgt_password,
                              NULL,
                              direct_sql->tgt_port,
                              direct_sql->tgt_socket,
                              CLIENT_MULTI_STATEMENTS)
      ) {
        if (connect_mutex)
          pthread_mutex_unlock(&spider_open_conn_mutex);
        error_num = mysql_errno(conn->db_conn);
        spider_db_disconnect(conn);
        if (
          (
            error_num != CR_CONN_HOST_ERROR &&
            error_num != CR_CONNECTION_ERROR
          ) ||
          !connect_retry_count
        ) {
          my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
            direct_sql->server_name);
          DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
        }
        connect_retry_count--;
        my_sleep((ulong) connect_retry_interval);
      } else {
        if (connect_mutex)
          pthread_mutex_unlock(&spider_open_conn_mutex);
        break;
      }
    }
#ifdef HAVE_HANDLERSOCKET
  } else {
    SPIDER_HS_SOCKARGS sockargs;
    sockargs.timeout = conn->connect_timeout;
    sockargs.recv_timeout = conn->net_read_timeout;
    sockargs.send_timeout = conn->net_write_timeout;
    if (conn->hs_sock)
    {
      sockargs.family = AF_UNIX;
      sockargs.set_unix_domain(conn->hs_sock);
    } else {
      char port_str[6];
      my_sprintf(port_str, (port_str, "%05ld", conn->hs_port));
      if (sockargs.resolve(conn->tgt_host, port_str) != 0)
      {
        my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
          conn->tgt_host);
        DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
      }
    }
    spider_db_disconnect(conn);
    conn->hs_conn = SPIDER_HS_CONN_CREATE(sockargs);
#ifndef HANDLERSOCKET_MYSQL_UTIL
    if (!(conn->hs_conn.operator->()))
#else
    DBUG_PRINT("info",("spider conn->hs_conn=%p", conn->hs_conn));
    if (!(conn->hs_conn))
#endif
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    while (conn->hs_conn->get_error_code())
    {
      if (!connect_retry_count)
      {
        my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
          conn->tgt_host);
        DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
      }
      connect_retry_count--;
      my_sleep((ulong) connect_retry_interval);
      conn->hs_conn->reconnect();
    }
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_udf_direct_sql_ping(
  SPIDER_DIRECT_SQL *direct_sql
) {
  int error_num;
  SPIDER_CONN *conn = direct_sql->conn;
  DBUG_ENTER("spider_db_udf_direct_sql_ping");
  if (conn->server_lost)
  {
    if ((error_num = spider_db_udf_direct_sql_connect(direct_sql, conn)))
      DBUG_RETURN(error_num);
    conn->server_lost = FALSE;
  }
#ifdef HAVE_HANDLERSOCKET
  if (direct_sql->access_mode == 0)
  {
#endif
    if ((error_num = simple_command(conn->db_conn, COM_PING, 0, 0, 0)))
    {
      spider_db_disconnect(conn);
      if ((error_num = spider_db_udf_direct_sql_connect(direct_sql, conn)))
      {
        DBUG_PRINT("info", ("spider conn=%p SERVER_LOST", conn));
        conn->server_lost = TRUE;
        DBUG_RETURN(error_num);
      }
      if((error_num = simple_command(conn->db_conn, COM_PING, 0, 0, 0)))
      {
        spider_db_disconnect(conn);
        DBUG_PRINT("info", ("spider conn=%p SERVER_LOST", conn));
        conn->server_lost = TRUE;
        DBUG_RETURN(error_num);
      }
    }
#ifdef HAVE_HANDLERSOCKET
  }
#endif
  conn->ping_time = (time_t) time((time_t*) 0);
  DBUG_RETURN(0);
}

int spider_db_udf_direct_sql(
  SPIDER_DIRECT_SQL *direct_sql
) {
  int error_num = 0, status = 0, roop_count = 0, need_mon = 0;
  uint udf_table_mutex_index, field_num, set_on, set_off;
  long long roop_count2;
  bool end_of_file;
  SPIDER_TRX *trx = direct_sql->trx;
  THD *thd = trx->thd, *c_thd = current_thd;
  SPIDER_CONN *conn = direct_sql->conn;
  SPIDER_DB_RESULT *result = NULL;
  TABLE *table;
  int bulk_insert_rows = (int) spider_param_udf_ds_bulk_insert_rows(thd,
    direct_sql->bulk_insert_rows);
  int table_loop_mode = spider_param_udf_ds_table_loop_mode(thd,
    direct_sql->table_loop_mode);
  double ping_interval_at_trx_start =
    spider_param_ping_interval_at_trx_start(thd);
  time_t tmp_time = (time_t) time((time_t*) 0);
  bool need_trx_end, insert_start = FALSE;
#ifdef HAVE_HANDLERSOCKET
  size_t num_fields;
#endif
  DBUG_ENTER("spider_db_udf_direct_sql");
  if (c_thd->transaction.stmt.ha_list)
    need_trx_end = FALSE;
  else
    need_trx_end = TRUE;

  if (!conn->disable_reconnect)
  {
    if (
      (
        conn->server_lost ||
        difftime(tmp_time, conn->ping_time) >= ping_interval_at_trx_start
      ) &&
      (error_num = spider_db_udf_direct_sql_ping(direct_sql))
    )
      DBUG_RETURN(error_num);
  } else if (conn->server_lost)
  {
    my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
      ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM);
  }

  conn->need_mon = &need_mon;
  if (
    !(error_num = spider_db_udf_direct_sql_set_names(direct_sql, trx, conn)) &&
    !(error_num = spider_db_udf_direct_sql_select_db(direct_sql, conn))
  ) {
#ifdef HAVE_HANDLERSOCKET
    if (direct_sql->access_mode != 0)
    {
      conn->hs_conn->request_buf_append(direct_sql->sql,
        direct_sql->sql + direct_sql->sql_length);
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      DBUG_ASSERT(conn->mta_conn_mutex_file_pos.file_name);
    }
#endif
    spider_conn_set_timeout_from_direct_sql(conn, thd, direct_sql);
    if (spider_db_query(
      conn,
      direct_sql->sql,
      direct_sql->sql_length,
      &need_mon)
    ) {
      error_num = spider_db_errorno(conn);
      if (error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM)
        my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
          ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
    } else {
      DBUG_PRINT("info",("spider conn=%p", conn));
      if (!direct_sql->table_count)
        roop_count = -1;
      conn->mta_conn_mutex_unlock_later = TRUE;
      do {
        if (roop_count == direct_sql->table_count)
        {
          if (table_loop_mode == 1)
            roop_count--;
          else if (table_loop_mode == 2)
            roop_count = 0;
          else
            roop_count = -1;
        }
        if (
#ifdef HAVE_HANDLERSOCKET
          (
            direct_sql->access_mode == 0 &&
#endif
            (result = conn->db_conn->methods->use_result(conn->db_conn))
#ifdef HAVE_HANDLERSOCKET
          ) ||
          (
            direct_sql->access_mode != 0 &&
            !conn->hs_conn->response_recv(num_fields)
          )
#endif
        ) {
          end_of_file = FALSE;
          if (roop_count >= 0)
          {
            while (!error_num && !end_of_file)
            {
              udf_table_mutex_index = spider_udf_calc_hash(
                direct_sql->db_names[roop_count],
                spider_param_udf_table_lock_mutex_count());
              udf_table_mutex_index += spider_udf_calc_hash(
                direct_sql->table_names[roop_count],
                spider_param_udf_table_lock_mutex_count());
              udf_table_mutex_index %=
                spider_param_udf_table_lock_mutex_count();
              pthread_mutex_lock(
                &trx->udf_table_mutexes[udf_table_mutex_index]);
              table = direct_sql->tables[roop_count];
              table->in_use = c_thd;
              memset((uchar *) table->null_flags, ~(uchar) 0,
                sizeof(uchar) * table->s->null_bytes);
              insert_start = TRUE;

#ifdef HAVE_HANDLERSOCKET
              if (direct_sql->access_mode == 0)
#endif
                field_num = mysql_num_fields(result);
#ifdef HAVE_HANDLERSOCKET
              else
                field_num = (uint) num_fields;
#endif
              if (field_num > table->s->fields)
              {
                set_on = table->s->fields;
                set_off = table->s->fields;
              } else {
                set_on = field_num;
                set_off = table->s->fields;
              }
              for (roop_count2 = 0; roop_count2 < set_on; roop_count2++)
                bitmap_set_bit(table->write_set, roop_count2);
              for (; roop_count2 < set_off; roop_count2++)
                bitmap_clear_bit(table->write_set, roop_count2);

              if (table->file->has_transactions())
              {
                THR_LOCK_DATA *to[2];
                table->file->store_lock(table->in_use, to,
                  TL_WRITE_CONCURRENT_INSERT);
                if ((error_num = table->file->ha_external_lock(table->in_use,
                  F_WRLCK)))
                {
                  table->file->print_error(error_num, MYF(0));
                  break;
                }
              }

              if (direct_sql->iop)
              {
                if (direct_sql->iop[roop_count] == 1)
                  table->file->extra(HA_EXTRA_IGNORE_DUP_KEY);
                else if (direct_sql->iop[roop_count] == 2)
                  table->file->extra(HA_EXTRA_WRITE_CAN_REPLACE);
              }
              table->file->ha_start_bulk_insert(
                (ha_rows) bulk_insert_rows);

              for (roop_count2 = 0;
                roop_count2 < bulk_insert_rows;
                roop_count2++)
              {
                if ((error_num = spider_db_udf_fetch_table(
                  trx, conn, table, result, set_on, set_off)))
                {
                  if (error_num == HA_ERR_END_OF_FILE)
                  {
                    end_of_file = TRUE;
                    error_num = 0;
                  }
                  break;
                }
                if (direct_sql->iop && direct_sql->iop[roop_count] == 2)
                {
                  if ((error_num = spider_sys_replace(table,
                    &direct_sql->modified_non_trans_table)))
                  {
                    table->file->print_error(error_num, MYF(0));
                    break;
                  }
                } else if ((error_num =
                  table->file->ha_write_row(table->record[0])))
                {
                  /* insert */
                  if (
                    !direct_sql->iop || direct_sql->iop[roop_count] != 1 ||
                    table->file->is_fatal_error(error_num, HA_CHECK_DUP)
                  ) {
                    DBUG_PRINT("info",("spider error_num=%d", error_num));
                    table->file->print_error(error_num, MYF(0));
                    break;
                  } else
                    error_num = 0;
                }
              }

              if (error_num)
                table->file->ha_end_bulk_insert();
              else
                error_num = table->file->ha_end_bulk_insert();
              if (direct_sql->iop)
              {
                if (direct_sql->iop[roop_count] == 1)
                  table->file->extra(HA_EXTRA_NO_IGNORE_DUP_KEY);
                else if (direct_sql->iop[roop_count] == 2)
                  table->file->extra(HA_EXTRA_WRITE_CANNOT_REPLACE);
              }
              if (table->file->has_transactions())
              {
                table->file->ha_external_lock(table->in_use, F_UNLCK);
              }
              table->file->ha_reset();
              table->in_use = thd;
              pthread_mutex_unlock(
                &trx->udf_table_mutexes[udf_table_mutex_index]);
            }
            if (error_num)
              roop_count = -1;
          }
#ifdef HAVE_HANDLERSOCKET
          if (direct_sql->access_mode == 0)
#endif
            mysql_free_result(result);
#ifdef HAVE_HANDLERSOCKET
          else {
#ifndef DBUG_OFF
            if (conn->hs_conn->get_response_end_offset() > 0 &&
              conn->hs_conn->get_readbuf_begin())
            {
              char tmp_buf[MAX_FIELD_WIDTH];
              String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
              tmp_str.length(0);
              tmp_str.append(conn->hs_conn->get_readbuf_begin(),
                conn->hs_conn->get_response_end_offset(), &my_charset_bin);
              DBUG_PRINT("info",("spider hs readbuf11 size=%zu str=%s",
                conn->hs_conn->get_response_end_offset(),
                tmp_str.c_ptr_safe()));
            }
#endif
            conn->hs_conn->response_buf_remove();
          }
#endif
        } else {
          if ((error_num = spider_db_errorno(conn)))
          {
            if (error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM)
              my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
                ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
            break;
          }
        }
#ifdef HAVE_HANDLERSOCKET
        if (direct_sql->access_mode == 0)
        {
#endif
          if ((status = spider_db_next_result(conn)) > 0)
          {
            error_num = status;
            break;
          }
#ifdef HAVE_HANDLERSOCKET
        } else {
          if (conn->hs_conn->stable_point())
          {
            break;
          }
        }
#endif
        if (roop_count >= 0)
          roop_count++;
      } while (status == 0);
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
  if (need_trx_end && insert_start)
  {
    if (error_num)
      (void) ha_rollback_trans(c_thd, 0);
    else {
      if ((error_num = ha_commit_trans(c_thd, 0)))
        my_error(error_num, MYF(0));
    }
  }
  DBUG_RETURN(error_num);
}

int spider_db_udf_direct_sql_select_db(
  SPIDER_DIRECT_SQL *direct_sql,
  SPIDER_CONN *conn
) {
  int error_num, need_mon = 0;
  bool tmp_mta_conn_mutex_lock_already;
  SPIDER_DB_CONN *db_conn = conn->db_conn;
  DBUG_ENTER("spider_db_udf_direct_sql_select_db");
#ifdef HAVE_HANDLERSOCKET
  if (direct_sql->access_mode == 0)
  {
#endif
    if (!conn->mta_conn_mutex_lock_already)
    {
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &need_mon;
    }
    DBUG_ASSERT(conn->mta_conn_mutex_file_pos.file_name);
    if (
      !conn->default_database.length() ||
      conn->default_database.length() !=
        direct_sql->tgt_default_db_name_length ||
      memcmp(direct_sql->tgt_default_db_name, conn->default_database.ptr(),
        direct_sql->tgt_default_db_name_length)
    ) {
      tmp_mta_conn_mutex_lock_already = conn->mta_conn_mutex_lock_already;
      conn->mta_conn_mutex_lock_already = TRUE;
      if (
        (
          spider_db_before_query(conn, &need_mon) ||
          mysql_select_db(
            db_conn,
            direct_sql->tgt_default_db_name)
        ) &&
        (error_num = spider_db_errorno(conn))
      ) {
        if (
          error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
          !conn->disable_reconnect
        )
          my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
            ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
        conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
        DBUG_RETURN(error_num);
      }
      conn->default_database.length(0);
      if (conn->default_database.reserve(
        direct_sql->tgt_default_db_name_length + 1))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      conn->default_database.q_append(direct_sql->tgt_default_db_name,
        direct_sql->tgt_default_db_name_length + 1);
      conn->default_database.length(direct_sql->tgt_default_db_name_length);
      conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
    }
    if (!conn->mta_conn_mutex_unlock_later)
    {
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
#ifdef HAVE_HANDLERSOCKET
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_udf_direct_sql_set_names(
  SPIDER_DIRECT_SQL *direct_sql,
  SPIDER_TRX *trx,
  SPIDER_CONN *conn
) {
  int error_num, need_mon = 0;
  bool tmp_mta_conn_mutex_lock_already;
  DBUG_ENTER("spider_db_udf_direct_sql_set_names");
#ifdef HAVE_HANDLERSOCKET
  if (direct_sql->access_mode == 0)
  {
#endif
    if (!conn->mta_conn_mutex_lock_already)
    {
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &need_mon;
    }
    DBUG_ASSERT(conn->mta_conn_mutex_file_pos.file_name);
    if (
      !conn->access_charset ||
      trx->udf_access_charset->cset != conn->access_charset->cset
    ) {
      tmp_mta_conn_mutex_lock_already = conn->mta_conn_mutex_lock_already;
      conn->mta_conn_mutex_lock_already = TRUE;
      if (
        (
          spider_db_before_query(conn, &need_mon) ||
          mysql_set_character_set(
            conn->db_conn,
            trx->udf_access_charset->csname)
        ) &&
        (error_num = spider_db_errorno(conn))
      ) {
        if (
          error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
          !conn->disable_reconnect
        ) {
          my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
            ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
        }
        conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
        DBUG_RETURN(error_num);
      }
      conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
      conn->access_charset = trx->udf_access_charset;
    }
    if (!conn->mta_conn_mutex_unlock_later)
    {
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
#ifdef HAVE_HANDLERSOCKET
  }
#endif
  DBUG_RETURN(0);
}

int spider_db_udf_check_and_set_set_names(
  SPIDER_TRX *trx
) {
  int error_num;
  DBUG_ENTER("spider_db_udf_check_and_set_set_names");
  if (
    !trx->udf_access_charset ||
    trx->udf_access_charset->cset !=
      trx->thd->variables.character_set_client->cset)
  {
    trx->udf_access_charset = trx->thd->variables.character_set_client;
    if ((error_num = spider_db_udf_append_set_names(trx)))
      DBUG_RETURN(error_num);
  }
  DBUG_RETURN(0);
}

int spider_db_udf_append_set_names(
  SPIDER_TRX *trx
) {
  DBUG_ENTER("spider_db_udf_append_set_names");
  DBUG_RETURN(0);
}

void spider_db_udf_free_set_names(
  SPIDER_TRX *trx
) {
  DBUG_ENTER("spider_db_udf_free_set_names");
  DBUG_VOID_RETURN;
}

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
) {
  int error_num, need_mon = 0;
  uint tmp_conn_link_idx = 0;
  ha_spider spider;
  DBUG_ENTER("spider_db_udf_ping_table");
  if (!pthread_mutex_trylock(&table_mon_list->monitor_mutex))
  {
    spider.share = share;
    spider.trx = trx;
    spider.need_mons = &need_mon;
    spider.conn_link_idx = &tmp_conn_link_idx;
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &need_mon;
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    if ((error_num = spider_db_ping(&spider, conn, 0)))
    {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      table_mon_list->last_mon_result = error_num;
      pthread_mutex_unlock(&table_mon_list->monitor_mutex);
      my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
        share->server_names[0]);
      DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    if (!ping_only)
    {
      int init_sql_alloc_size =
        spider_param_init_sql_alloc_size(trx->thd, share->init_sql_alloc_size);
#ifdef _MSC_VER
      spider_string sql_str(init_sql_alloc_size);
      sql_str.set_charset(system_charset_info);
      spider_string where_str(init_sql_alloc_size);
      where_str.set_charset(system_charset_info);
#else
      char sql_buf[init_sql_alloc_size], where_buf[init_sql_alloc_size];
      spider_string sql_str(sql_buf, sizeof(sql_buf),
        system_charset_info);
      spider_string where_str(where_buf, sizeof(where_buf),
        system_charset_info);
#endif
      sql_str.init_calc_mem(128);
      where_str.init_calc_mem(129);
      sql_str.length(0);
      where_str.length(0);
      if (
        use_where &&
        where_str.append(where_clause, where_clause_length,
          trx->thd->variables.character_set_client)
      ) {
        table_mon_list->last_mon_result = HA_ERR_OUT_OF_MEM;
        pthread_mutex_unlock(&table_mon_list->monitor_mutex);
        my_error(HA_ERR_OUT_OF_MEM, MYF(0));
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
      share->access_charset = system_charset_info;
      if ((error_num = spider_db_udf_ping_table_append_select(&sql_str,
        share, trx, &where_str, use_where, limit)))
      {
        table_mon_list->last_mon_result = error_num;
        pthread_mutex_unlock(&table_mon_list->monitor_mutex);
        my_error(error_num, MYF(0));
        DBUG_RETURN(error_num);
      }
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &need_mon;
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(&spider, conn, 0)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        table_mon_list->last_mon_result = error_num;
        pthread_mutex_unlock(&table_mon_list->monitor_mutex);
        DBUG_PRINT("info",("spider error_num=%d", error_num));
        DBUG_RETURN(error_num);
      }
      spider_conn_set_timeout_from_share(conn, 0, trx->thd, share);
      if (spider_db_query(
        conn,
        sql_str.ptr(),
        sql_str.length(),
        &need_mon)
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conn);
        table_mon_list->last_mon_result = error_num;
        pthread_mutex_unlock(&table_mon_list->monitor_mutex);
        DBUG_PRINT("info",("spider error_num=%d", error_num));
        DBUG_RETURN(error_num);
      }
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      spider_db_discard_result(conn);
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
    table_mon_list->last_mon_result = 0;
    pthread_mutex_unlock(&table_mon_list->monitor_mutex);
  } else {
    pthread_mutex_lock(&table_mon_list->monitor_mutex);
    error_num = table_mon_list->last_mon_result;
    pthread_mutex_unlock(&table_mon_list->monitor_mutex);
    DBUG_RETURN(error_num);
  }

  DBUG_RETURN(0);
}

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
) {
  char limit_str[SPIDER_SQL_INT_LEN], sid_str[SPIDER_SQL_INT_LEN];
  int limit_str_length, sid_str_length;
  spider_string child_table_name_str(child_table_name,
    child_table_name_length + 1, str->charset());
  spider_string where_clause_str(where_clause ? where_clause : "",
    where_clause_length + 1, str->charset());
  DBUG_ENTER("spider_db_udf_ping_table_append_mon_next");
  child_table_name_str.init_calc_mem(130);
  where_clause_str.init_calc_mem(131);
  child_table_name_str.length(child_table_name_length);
  where_clause_str.length(where_clause_length);
  limit_str_length = my_sprintf(limit_str, (limit_str, "%lld", limit));
  sid_str_length = my_sprintf(sid_str, (sid_str, "%lld", first_sid));
  if (str->reserve(
    SPIDER_SQL_SELECT_LEN +
    SPIDER_SQL_PING_TABLE_LEN +
    (child_table_name_length * 2) +
    (SPIDER_SQL_INT_LEN * 6) +
    sid_str_length +
    limit_str_length +
    (where_clause_length * 2) +
    (SPIDER_SQL_VALUE_QUOTE_LEN * 4) +
    (SPIDER_SQL_COMMA_LEN * 9) +
    SPIDER_SQL_CLOSE_PAREN_LEN
  ))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SELECT_STR, SPIDER_SQL_SELECT_LEN);
  str->q_append(SPIDER_SQL_PING_TABLE_STR, SPIDER_SQL_PING_TABLE_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  append_escaped(str->get_str(), child_table_name_str.get_str());
  str->mem_calc();
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->qs_append(link_id);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->qs_append(flags);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->q_append(limit_str, limit_str_length);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  append_escaped(str->get_str(), where_clause_str.get_str());
  str->mem_calc();
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->q_append(sid_str, sid_str_length);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->qs_append(full_mon_count);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->qs_append(current_mon_count);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->qs_append(success_count);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->qs_append(fault_count);
  str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_db_udf_ping_table_append_select(
  spider_string *str,
  SPIDER_SHARE *share,
  SPIDER_TRX *trx,
  spider_string *where_str,
  bool use_where,
  longlong limit
) {
  int error_num;
  char limit_str[SPIDER_SQL_INT_LEN];
  int limit_str_length;
  DBUG_ENTER("spider_db_udf_ping_table_append_select");
  if (str->reserve(SPIDER_SQL_SELECT_LEN + SPIDER_SQL_ONE_LEN +
    SPIDER_SQL_FROM_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SELECT_STR, SPIDER_SQL_SELECT_LEN);
  str->q_append(SPIDER_SQL_ONE_STR, SPIDER_SQL_ONE_LEN);
  str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
  if ((error_num = spider_db_append_name_with_quote_str(str,
    share->tgt_dbs[0])))
    DBUG_RETURN(error_num);
  if (str->reserve(SPIDER_SQL_DOT_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  if ((error_num = spider_db_append_name_with_quote_str(str,
    share->tgt_table_names[0])))
    DBUG_RETURN(error_num);

  limit_str_length = my_sprintf(limit_str, (limit_str, "%lld", limit));
  if (str->reserve(
    (use_where ? (where_str->length() * 2) : 0) +
    SPIDER_SQL_LIMIT_LEN + limit_str_length
  ))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if (use_where)
    append_escaped(str->get_str(), where_str->get_str());
  str->mem_calc();
  str->q_append(SPIDER_SQL_LIMIT_STR, SPIDER_SQL_LIMIT_LEN);
  str->q_append(limit_str, limit_str_length);
  DBUG_RETURN(0);
}

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
) {
  int error_num, need_mon = 0;
  uint tmp_conn_link_idx = 0;
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row;
  SPIDER_SHARE *share = table_mon->share;
  int init_sql_alloc_size =
    spider_param_init_sql_alloc_size(thd, share->init_sql_alloc_size);
#ifdef _MSC_VER
  spider_string sql_str(init_sql_alloc_size);
  sql_str.set_charset(thd->variables.character_set_client);
#else
  char sql_buf[init_sql_alloc_size];
  spider_string sql_str(sql_buf, sizeof(sql_buf),
    thd->variables.character_set_client);
#endif
  ha_spider spider;
  SPIDER_TRX trx;
  DBUG_ENTER("spider_db_udf_ping_table_mon_next");
  sql_str.init_calc_mem(132);
  sql_str.length(0);
  trx.thd = thd;
  spider.share = share;
  spider.trx = &trx;
  spider.need_mons = &need_mon;
  spider.conn_link_idx = &tmp_conn_link_idx;

  share->access_charset = thd->variables.character_set_client;
  if ((error_num = spider_db_udf_ping_table_append_mon_next(&sql_str,
    child_table_name, child_table_name_length, link_id, where_clause,
    where_clause_length, first_sid, full_mon_count, current_mon_count,
    success_count, fault_count, flags, limit)))
  {
    my_error(error_num, MYF(0));
    DBUG_RETURN(error_num);
  }

  pthread_mutex_lock(&conn->mta_conn_mutex);
  SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
  conn->need_mon = &need_mon;
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  if ((error_num = spider_db_ping(&spider, conn, 0)))
  {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
      share->server_names[0]);
    DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
  }
  if ((error_num = spider_db_set_names(&spider, conn, 0)))
  {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(error_num);
  }
  spider_conn_set_timeout_from_share(conn, 0, thd, share);
  if (spider_db_query(
    conn,
    sql_str.ptr(),
    sql_str.length(),
    &need_mon)
  ) {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    DBUG_RETURN(spider_db_errorno(conn));
  }
  if (
    !(res = mysql_store_result(conn->db_conn)) ||
    !(row = mysql_fetch_row(res))
  ) {
    if (res)
      mysql_free_result(res);
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    if ((error_num = spider_db_errorno(conn)))
      DBUG_RETURN(error_num);
    my_error(HA_ERR_OUT_OF_MEM, MYF(0));
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  conn->mta_conn_mutex_lock_already = FALSE;
  conn->mta_conn_mutex_unlock_later = FALSE;
  SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  if (mysql_num_fields(res) != 1)
  {
    mysql_free_result(res);
    my_printf_error(ER_SPIDER_UNKNOWN_NUM,
      ER_SPIDER_UNKNOWN_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_UNKNOWN_NUM);
  }
  if (row[0])
    mon_table_result->result_status = atoi(row[0]);
  else
    mon_table_result->result_status = SPIDER_LINK_MON_OK;
  DBUG_PRINT("info",
    ("spider result_status=%d", mon_table_result->result_status));
  mysql_free_result(res);
  DBUG_RETURN(0);
}

int spider_db_udf_copy_key_row(
  spider_string *str,
  spider_string *source_str,
  Field *field,
  ulong *row_pos,
  ulong *length,
  const char *joint_str,
  const int joint_length
) {
  int error_num;
  DBUG_ENTER("spider_db_udf_copy_key_row");
  if ((error_num = spider_db_append_name_with_quote_str(str,
    (char *) field->field_name)))
    DBUG_RETURN(error_num);
  if (str->reserve(joint_length + *length + SPIDER_SQL_AND_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(joint_str, joint_length);
  str->q_append(source_str->ptr() + *row_pos, *length);
  str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
  DBUG_RETURN(0);
}

int spider_db_udf_copy_row(
  spider_string *str,
  Field *field,
  SPIDER_DB_ROW row,
  ulong *length
) {
  DBUG_ENTER("spider_db_udf_copy_row");
  if (!*row)
  {
    if (str->reserve(SPIDER_SQL_NULL_LEN + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
  } else if (field->str_needs_quotes())
  {
    spider_string tmp_str(*row, *length + 1, str->charset());
    tmp_str.init_calc_mem(133);
    tmp_str.length(*length);
    if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN * 2 + *length * 2 + 2 +
      SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    append_escaped(str->get_str(), tmp_str.get_str());
    str->mem_calc();
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  } else {
    if (str->reserve(*length + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(*row, *length);
  }
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_db_udf_copy_rows(
  spider_string *str,
  TABLE *table,
  SPIDER_DB_RESULT *result,
  ulong **last_row_pos,
  ulong **last_lengths
) {
  int error_num;
  Field **field;
  SPIDER_DB_ROW row;
  ulong *lengths, *lengths2, *row_pos2;
  DBUG_ENTER("spider_db_udf_copy_rows");
  if (!(row = mysql_fetch_row(result)))
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  lengths = mysql_fetch_lengths(result);
  row_pos2 = *last_row_pos;
  lengths2 = *last_lengths;

  for (
    field = table->field;
    *field;
    field++,
    lengths++,
    lengths2++
  ) {
    *row_pos2 = str->length();
    if ((error_num =
      spider_db_udf_copy_row(str, *field, row, lengths)))
      DBUG_RETURN(error_num);
    *lengths2 = str->length() - *row_pos2 - SPIDER_SQL_COMMA_LEN;
    row++;
    row_pos2++;
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(0);
}

int spider_db_udf_copy_tables(
  SPIDER_COPY_TABLES *copy_tables,
  ha_spider *spider,
  TABLE *table,
  longlong bulk_insert_rows
) {
  int error_num = 0, roop_count, roop_count2;
  bool end_of_file = FALSE;
  ulong *last_lengths, *last_row_pos = NULL;
  ha_spider *tmp_spider;
  SPIDER_CONN *tmp_conn;
  int all_link_cnt =
    copy_tables->link_idx_count[0] + copy_tables->link_idx_count[1];
  SPIDER_COPY_TABLE_CONN *src_tbl_conn = copy_tables->table_conn[0];
  SPIDER_COPY_TABLE_CONN *dst_tbl_conn;
  spider_string *select_sql = &src_tbl_conn->select_sql;
  spider_string *insert_sql;
  KEY *key_info = &table->key_info[table->s->primary_key];
  KEY_PART_INFO *key_part = key_info->key_part;
  int bulk_insert_interval;
  DBUG_ENTER("spider_db_udf_copy_tables");
  if (!(last_row_pos = (ulong *)
    spider_bulk_malloc(spider_current_trx, 30, MYF(MY_WME),
      &last_row_pos, sizeof(ulong) * table->s->fields,
      &last_lengths, sizeof(ulong) * table->s->fields,
      NullS))
  ) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    goto error;
  }
  while (!end_of_file)
  {
    if (copy_tables->trx->thd->killed)
    {
      my_error(ER_QUERY_INTERRUPTED, MYF(0));
      error_num = ER_QUERY_INTERRUPTED;
      goto error_killed;
    }
    if (copy_tables->use_transaction)
    {
      for (roop_count = 0; roop_count < all_link_cnt; roop_count++)
      {
        tmp_spider = &spider[roop_count];
        tmp_conn = tmp_spider->conns[0];
        if (!tmp_conn->trx_start)
        {
          if (spider_db_ping(tmp_spider, tmp_conn, 0))
          {
            my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
              tmp_spider->share->server_names[0]);
            error_num = ER_CONNECT_TO_FOREIGN_DATA_SOURCE;
            goto error_db_ping;
          }
          if (
            (error_num = spider_db_set_names(tmp_spider, tmp_conn, 0)) ||
            (error_num = spider_db_start_transaction(tmp_conn,
              tmp_spider->need_mons))
          )
            goto error_start_transaction;
        }
      }
    } else {
      for (roop_count = 0; roop_count < all_link_cnt; roop_count++)
      {
        tmp_spider = &spider[roop_count];
        tmp_conn = tmp_spider->conns[0];
        uint old_elements = tmp_conn->lock_table_hash.array.max_element;
        if (my_hash_insert(&tmp_conn->lock_table_hash,
          (uchar*) tmp_spider->link_for_hash))
        {
          my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
          error_num = ER_OUT_OF_RESOURCES;
          goto error_lock_table_hash;
        }
        if (tmp_conn->lock_table_hash.array.max_element > old_elements)
        {
          spider_alloc_calc_mem(spider_current_trx,
            tmp_conn->lock_table_hash,
            (tmp_conn->lock_table_hash.array.max_element - old_elements) *
            tmp_conn->lock_table_hash.array.size_of_element);
        }
        tmp_conn->table_lock = 2;
      }
      for (roop_count = 0; roop_count < all_link_cnt; roop_count++)
      {
        tmp_spider = &spider[roop_count];
        tmp_conn = tmp_spider->conns[0];
        if (spider_db_ping(tmp_spider, tmp_conn, 0))
        {
          my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
            tmp_spider->share->server_names[0]);
          error_num = ER_CONNECT_TO_FOREIGN_DATA_SOURCE;
          goto error_db_ping;
        }
        if (
          tmp_conn->lock_table_hash.records &&
          (
            (error_num = spider_db_set_names(tmp_spider, tmp_conn, 0)) ||
            (error_num = spider_db_lock_tables(tmp_spider, 0))
          )
        ) {
          tmp_conn->table_lock = 0;
          if (error_num == HA_ERR_OUT_OF_MEM)
            my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
          goto error_lock_tables;
        }
        tmp_conn->table_lock = 1;
      }
    }

    tmp_conn = src_tbl_conn->conn;
    spider_conn_set_timeout_from_share(tmp_conn, 0,
      copy_tables->trx->thd, src_tbl_conn->share);
    if (spider_db_query(
      tmp_conn,
      select_sql->ptr(),
      select_sql->length(),
      &src_tbl_conn->need_mon)
    ) {
      error_num = spider_db_errorno(tmp_conn);
      if (error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM)
        my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
          ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
      goto error_db_query;
    } else {
      SPIDER_DB_RESULT *result;
      if ((result =
        tmp_conn->db_conn->methods->use_result(tmp_conn->db_conn)))
      {
        insert_sql = &copy_tables->table_conn[1]->insert_sql;
        roop_count = 0;
        while (!(error_num = spider_db_udf_copy_rows(
          insert_sql, table, result, &last_row_pos, &last_lengths)))
        {
          if (insert_sql->reserve(SPIDER_SQL_CLOSE_PAREN_LEN +
            SPIDER_SQL_COMMA_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
          {
            my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
            error_num = ER_OUT_OF_RESOURCES;
            goto error_db_query;
          }
          insert_sql->q_append(SPIDER_SQL_CLOSE_PAREN_STR,
            SPIDER_SQL_CLOSE_PAREN_LEN);
          insert_sql->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
          insert_sql->q_append(SPIDER_SQL_OPEN_PAREN_STR,
            SPIDER_SQL_OPEN_PAREN_LEN);

          roop_count++;
        }
        insert_sql->length(insert_sql->length() -
          SPIDER_SQL_COMMA_LEN - SPIDER_SQL_OPEN_PAREN_LEN);
        if (error_num == HA_ERR_END_OF_FILE)
        {
          if (roop_count < copy_tables->bulk_insert_rows)
          {
            end_of_file = TRUE;
            if (roop_count)
              error_num = 0;
          } else {
            /* add next where clause */
            select_sql->length(src_tbl_conn->where_pos);
            if (select_sql->reserve(SPIDER_SQL_WHERE_LEN +
              SPIDER_SQL_OPEN_PAREN_LEN))
            {
              my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
              mysql_free_result(result);
              SPIDER_CLEAR_FILE_POS(&tmp_conn->mta_conn_mutex_file_pos);
              pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
              error_num = ER_OUT_OF_RESOURCES;
              goto error_db_query;
            }
            select_sql->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
            select_sql->q_append(SPIDER_SQL_OPEN_PAREN_STR,
              SPIDER_SQL_OPEN_PAREN_LEN);
            Field *field;
            for (roop_count = key_info->key_parts - 1;
              roop_count >= 0; roop_count--)
            {
              for (roop_count2 = 0; roop_count2 < roop_count; roop_count2++)
              {
                field = key_part[roop_count2].field;
                if (spider_db_udf_copy_key_row(select_sql, insert_sql,
                  field, &last_row_pos[field->field_index],
                  &last_lengths[field->field_index],
                  SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN))
                {
                  if (error_num == HA_ERR_OUT_OF_MEM)
                    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
                  mysql_free_result(result);
                  SPIDER_CLEAR_FILE_POS(&tmp_conn->mta_conn_mutex_file_pos);
                  pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
                  goto error_db_query;
                }
              }
              field = key_part[roop_count2].field;
              if (spider_db_udf_copy_key_row(select_sql, insert_sql,
                field, &last_row_pos[field->field_index],
                &last_lengths[field->field_index],
                SPIDER_SQL_GT_STR, SPIDER_SQL_GT_LEN))
              {
                if (error_num == HA_ERR_OUT_OF_MEM)
                  my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
                mysql_free_result(result);
                SPIDER_CLEAR_FILE_POS(&tmp_conn->mta_conn_mutex_file_pos);
                pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
                goto error_db_query;
              }
              select_sql->length(select_sql->length() - SPIDER_SQL_AND_LEN);
              if (select_sql->reserve(SPIDER_SQL_CLOSE_PAREN_LEN +
                SPIDER_SQL_OR_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
              {
                my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
                mysql_free_result(result);
                SPIDER_CLEAR_FILE_POS(&tmp_conn->mta_conn_mutex_file_pos);
                pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
                error_num = ER_OUT_OF_RESOURCES;
                goto error_db_query;
              }
              select_sql->q_append(SPIDER_SQL_CLOSE_PAREN_STR,
                SPIDER_SQL_CLOSE_PAREN_LEN);
              select_sql->q_append(SPIDER_SQL_OR_STR, SPIDER_SQL_OR_LEN);
              select_sql->q_append(SPIDER_SQL_OPEN_PAREN_STR,
                SPIDER_SQL_OPEN_PAREN_LEN);
            }
            select_sql->length(select_sql->length() - SPIDER_SQL_OR_LEN -
              SPIDER_SQL_OPEN_PAREN_LEN);
            bulk_insert_rows = spider_param_udf_ct_bulk_insert_rows(
              copy_tables->bulk_insert_rows);
            if (
              spider_db_append_key_order_str(select_sql, key_info, 0, FALSE) ||
              spider_db_append_limit(spider, select_sql, 0,
                bulk_insert_rows) ||
              (
                copy_tables->use_transaction &&
                spider_db_append_select_lock_str(select_sql,
                  SPIDER_LOCK_MODE_SHARED)
              )
            ) {
              my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
              mysql_free_result(result);
              SPIDER_CLEAR_FILE_POS(&tmp_conn->mta_conn_mutex_file_pos);
              pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
              error_num = ER_OUT_OF_RESOURCES;
              goto error_db_query;
            }
            error_num = 0;
          }
        } else {
          if (error_num == HA_ERR_OUT_OF_MEM)
            my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
          mysql_free_result(result);
          SPIDER_CLEAR_FILE_POS(&tmp_conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
          goto error_db_query;
        }
        mysql_free_result(result);
        SPIDER_CLEAR_FILE_POS(&tmp_conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
      } else {
        if ((error_num = spider_db_errorno(tmp_conn)))
        {
          if (error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM)
            my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
              ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
          goto error_db_query;
        }
        error_num = HA_ERR_END_OF_FILE;
        end_of_file = TRUE;
        SPIDER_CLEAR_FILE_POS(&tmp_conn->mta_conn_mutex_file_pos);
        pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
      }
    }

    if (!error_num)
    {
      dst_tbl_conn = copy_tables->table_conn[1];
      insert_sql = &dst_tbl_conn->insert_sql;
      int values_length = insert_sql->length() - dst_tbl_conn->values_pos;
      const char *values_ptr = insert_sql->ptr() + dst_tbl_conn->values_pos;
      for (dst_tbl_conn = dst_tbl_conn->next; dst_tbl_conn;
        dst_tbl_conn = dst_tbl_conn->next)
      {
        insert_sql = &dst_tbl_conn->insert_sql;
        if (insert_sql->reserve(values_length))
        {
          my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
          error_num = ER_OUT_OF_RESOURCES;
          goto error_db_query;
        }
        insert_sql->q_append(values_ptr, values_length);
      }

#ifndef WITHOUT_SPIDER_BG_SEARCH
      if (copy_tables->bg_mode)
      {
        for (dst_tbl_conn = copy_tables->table_conn[1]; dst_tbl_conn;
          dst_tbl_conn = dst_tbl_conn->next)
        {
          if (spider_udf_bg_copy_exec_sql(dst_tbl_conn))
          {
            my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
            error_num = ER_OUT_OF_RESOURCES;
            goto error_db_query;
          }
        }
      } else {
#endif
        for (dst_tbl_conn = copy_tables->table_conn[1]; dst_tbl_conn;
          dst_tbl_conn = dst_tbl_conn->next)
        {
          tmp_conn = dst_tbl_conn->conn;
          insert_sql = &dst_tbl_conn->insert_sql;
          spider_conn_set_timeout_from_share(tmp_conn, 0,
            copy_tables->trx->thd, dst_tbl_conn->share);
          if (spider_db_query(
            tmp_conn,
            insert_sql->ptr(),
            insert_sql->length(),
            &dst_tbl_conn->need_mon)
          ) {
            error_num = spider_db_errorno(tmp_conn);
            if (error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM)
              my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
                ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
            goto error_db_query;
          } else {
            SPIDER_CLEAR_FILE_POS(&tmp_conn->mta_conn_mutex_file_pos);
            pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
          }
        }
#ifndef WITHOUT_SPIDER_BG_SEARCH
      }
#endif

#ifndef WITHOUT_SPIDER_BG_SEARCH
      if (copy_tables->bg_mode)
      {
        for (dst_tbl_conn = copy_tables->table_conn[1]; dst_tbl_conn;
          dst_tbl_conn = dst_tbl_conn->next)
        {
          tmp_conn = dst_tbl_conn->conn;
          if (tmp_conn->bg_exec_sql)
          {
            /* wait */
            pthread_mutex_lock(&tmp_conn->bg_conn_mutex);
            pthread_mutex_unlock(&tmp_conn->bg_conn_mutex);
          }

          if (dst_tbl_conn->bg_error_num)
          {
            if (error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM)
              my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
                ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
            goto error_db_query;
          }
        }
      }
#endif
    }

    if (copy_tables->use_transaction)
    {
      for (roop_count = 0; roop_count < all_link_cnt; roop_count++)
      {
        tmp_spider = &spider[roop_count];
        tmp_conn = tmp_spider->conns[0];
        if (tmp_conn->trx_start)
        {
          if ((error_num = spider_db_commit(tmp_conn)))
            goto error_commit;
        }
      }
    } else {
      for (roop_count = 0; roop_count < all_link_cnt; roop_count++)
      {
        tmp_spider = &spider[roop_count];
        tmp_conn = tmp_spider->conns[0];
        if (tmp_conn->table_lock == 1)
        {
          tmp_conn->table_lock = 0;
          if ((error_num = spider_db_unlock_tables(tmp_spider, 0)))
            goto error_unlock_tables;
        }
      }
    }
    if (!end_of_file)
    {
      for (dst_tbl_conn = copy_tables->table_conn[1]; dst_tbl_conn;
        dst_tbl_conn = dst_tbl_conn->next)
      {
        insert_sql = &dst_tbl_conn->insert_sql;
        insert_sql->length(dst_tbl_conn->values_pos);
      }
      DBUG_PRINT("info",("spider sleep"));
      bulk_insert_interval = spider_param_udf_ct_bulk_insert_interval(
        copy_tables->bulk_insert_interval);
      my_sleep(bulk_insert_interval);
    }
  }
  spider_free(spider_current_trx, last_row_pos, MYF(0));
  DBUG_RETURN(0);

error_db_query:
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (copy_tables->bg_mode)
  {
    for (dst_tbl_conn = copy_tables->table_conn[1]; dst_tbl_conn;
      dst_tbl_conn = dst_tbl_conn->next)
    {
      tmp_conn = dst_tbl_conn->conn;
      if (tmp_conn->bg_exec_sql)
      {
        /* wait */
        pthread_mutex_lock(&tmp_conn->bg_conn_mutex);
        pthread_mutex_unlock(&tmp_conn->bg_conn_mutex);
      }
    }
  }
#endif
error_unlock_tables:
error_commit:
error_lock_tables:
error_lock_table_hash:
error_start_transaction:
error_db_ping:
error_killed:
  if (copy_tables->use_transaction)
  {
    for (roop_count = 0; roop_count < all_link_cnt; roop_count++)
    {
      tmp_spider = &spider[roop_count];
      tmp_conn = tmp_spider->conns[0];
      if (tmp_conn->trx_start)
        spider_db_rollback(tmp_conn);
    }
  } else {
    if (copy_tables->trx->locked_connections)
    {
      for (roop_count = 0; roop_count < all_link_cnt; roop_count++)
      {
        tmp_spider = &spider[roop_count];
        tmp_conn = tmp_spider->conns[0];
        if (tmp_conn->table_lock == 1)
        {
          tmp_conn->table_lock = 0;
          spider_db_unlock_tables(tmp_spider, 0);
        }
      }
    }
  }
error:
  if (last_row_pos)
  {
    spider_free(spider_current_trx, last_row_pos, MYF(0));
  }
  DBUG_RETURN(error_num);
}

void spider_db_reset_handler_open(
  SPIDER_CONN *conn
) {
  ha_spider *tmp_spider;
  int tmp_link_idx;
  SPIDER_LINK_FOR_HASH **tmp_link_for_hash;
  DBUG_ENTER("spider_db_reset_handler_open");
  while ((tmp_link_for_hash =
    (SPIDER_LINK_FOR_HASH **) pop_dynamic(&conn->handler_open_array)))
  {
    tmp_spider = (*tmp_link_for_hash)->spider;
    tmp_link_idx = (*tmp_link_for_hash)->link_idx;
    tmp_spider->clear_handler_opened(tmp_link_idx, conn->conn_kind);
  }
  DBUG_VOID_RETURN;
}

int spider_db_open_handler(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
) {
  int error_num;
  bool tmp_mta_conn_mutex_lock_already;
  bool tmp_mta_conn_mutex_unlock_later;
  SPIDER_SHARE *share = spider->share;
  spider_string *sql = &spider->result_list.sql;
  uint *handler_id_ptr =
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    conn->conn_kind == SPIDER_CONN_KIND_MYSQL ?
#endif
      &spider->m_handler_id[link_idx]
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
      : conn->conn_kind == SPIDER_CONN_KIND_HS_READ ?
        &spider->r_handler_id[link_idx] :
        &spider->w_handler_id[link_idx]
#endif
    ;
  DBUG_ENTER("spider_db_open_handler");
  if (!conn->mta_conn_mutex_lock_already)
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
    conn->need_mon = &spider->need_mons[link_idx];
  }
  DBUG_ASSERT(conn->mta_conn_mutex_file_pos.file_name);
  tmp_mta_conn_mutex_lock_already = conn->mta_conn_mutex_lock_already;
  conn->mta_conn_mutex_lock_already = TRUE;
  tmp_mta_conn_mutex_unlock_later = conn->mta_conn_mutex_unlock_later;
  conn->mta_conn_mutex_unlock_later = TRUE;
  if (!spider->handler_opened(link_idx, conn->conn_kind))
    *handler_id_ptr = conn->opened_handlers;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
  {
#endif
    if (!spider->handler_opened(link_idx, conn->conn_kind))
      my_sprintf(spider->m_handler_cid[link_idx],
        (spider->m_handler_cid[link_idx], SPIDER_SQL_HANDLER_CID_FORMAT,
        *handler_id_ptr));
    if (sql->reserve(SPIDER_SQL_HANDLER_LEN))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    sql->q_append(SPIDER_SQL_HANDLER_STR, SPIDER_SQL_HANDLER_LEN);
    if ((error_num =
      spider_db_append_table_name_with_reserve(sql, share,
        spider->conn_link_idx[link_idx])))
      goto error;
    if (sql->reserve(SPIDER_SQL_OPEN_LEN + SPIDER_SQL_AS_LEN +
      SPIDER_SQL_HANDLER_CID_LEN))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    sql->q_append(SPIDER_SQL_OPEN_STR, SPIDER_SQL_OPEN_LEN);
    sql->q_append(SPIDER_SQL_AS_STR, SPIDER_SQL_AS_LEN);
    sql->q_append(spider->m_handler_cid[link_idx], SPIDER_SQL_HANDLER_CID_LEN);

    spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
      spider->share);
    if (spider_db_query(
      conn,
      sql->ptr(),
      sql->length(),
      &spider->need_mons[link_idx])
    ) {
      error_num = spider_db_errorno(conn);
      goto error;
    }
    sql->length(0);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  } else {
    size_t num_fields;
    TABLE *table = spider->get_table();
    if ((error_num = spider_db_conn_queue_action(conn)))
    {
      goto error;
    }
    if (
      sql->length() == 0 &&
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
      (
        (
          (
            spider->sql_command == SQLCOM_HS_INSERT ||
            spider->hs_pushed_ret_fields_num == MAX_FIELDS
          ) &&
#endif
          (error_num = spider_db_append_minimum_select_without_quote(sql,
            table, spider))
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
        ) ||
        (
          (
            spider->sql_command != SQLCOM_HS_INSERT &&
            spider->hs_pushed_ret_fields_num < MAX_FIELDS
          ) &&
          (error_num = spider_db_append_minimum_select_by_field_idx_list(sql,
            table, spider, spider->hs_pushed_ret_fields,
            spider->hs_pushed_ret_fields_num))
        )
      )
#endif
    ) {
      goto error;
    }
    if (conn->hs_pre_age != conn->hs_age)
    {
      if (conn->hs_conn->reconnect())
      {
#ifndef HANDLERSOCKET_MYSQL_UTIL
        my_printf_error(ER_SPIDER_HS_NUM, ER_SPIDER_HS_STR, MYF(0),
          conn->hs_conn->get_error_code(),
          conn->hs_conn->get_error().c_str());
#else
        my_printf_error(ER_SPIDER_HS_NUM, ER_SPIDER_HS_STR, MYF(0),
          conn->hs_conn->get_error_code(),
          conn->hs_conn->get_error().c_ptr());
#endif
        if (!conn->mta_conn_mutex_unlock_later)
        {
          SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
          pthread_mutex_unlock(&conn->mta_conn_mutex);
        }
        spider->need_mons[link_idx] = ER_SPIDER_HS_NUM;
        error_num = ER_SPIDER_HS_NUM;
        goto error;
      }
      conn->opened_handlers = 0;
      spider_db_reset_handler_open(conn);
      conn->hs_age = conn->hs_pre_age;
    }
    if (conn->conn_kind == SPIDER_CONN_KIND_HS_READ)
    {
      if (spider->hs_r_conn_ages[link_idx] != conn->hs_age)
      {
        spider->clear_handler_opened(link_idx, SPIDER_CONN_KIND_HS_READ);
        *handler_id_ptr = conn->opened_handlers;
      }
    } else {
      if (spider->hs_w_conn_ages[link_idx] != conn->hs_age)
      {
        spider->clear_handler_opened(link_idx, SPIDER_CONN_KIND_HS_WRITE);
        *handler_id_ptr = conn->opened_handlers;
      }
    }

    DBUG_PRINT("info",("spider field list=%s", sql->c_ptr_safe()));
    conn->hs_conn->request_buf_open_index(
      *handler_id_ptr,
      share->tgt_dbs[link_idx],
      share->tgt_table_names[link_idx],
      spider->active_index < MAX_KEY ?
        table->s->key_info[spider->active_index].name :
        "0",
      sql->c_ptr_safe()
    );
    spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
      spider->share);
    if (spider_db_query(
      conn,
      sql->ptr(),
      sql->length(),
      &spider->need_mons[link_idx])
    ) {
      error_num = spider_db_errorno(conn);
      goto error;
    }

    DBUG_PRINT("info",("spider conn=%p", conn));
    if ((error_num = conn->hs_conn->response_recv(num_fields)))
    {
#ifndef DBUG_OFF
      if (conn->hs_conn->get_response_end_offset() > 0 &&
        conn->hs_conn->get_readbuf_begin())
      {
        char tmp_buf[MAX_FIELD_WIDTH];
        String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
        tmp_str.length(0);
        tmp_str.append(conn->hs_conn->get_readbuf_begin(),
          conn->hs_conn->get_response_end_offset(), &my_charset_bin);
        DBUG_PRINT("info",("spider hs readbuf12 size=%zu str=%s",
          conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
      }
#endif
      if (error_num > 0)
        conn->hs_conn->response_buf_remove();
      spider_db_errorno(conn);
      error_num = ER_SPIDER_HS_NUM;
      goto error;
    }
#ifndef DBUG_OFF
    if (conn->hs_conn->get_response_end_offset() > 0 &&
      conn->hs_conn->get_readbuf_begin())
    {
      char tmp_buf[MAX_FIELD_WIDTH];
      String tmp_str(tmp_buf, MAX_FIELD_WIDTH, &my_charset_bin);
      tmp_str.length(0);
      tmp_str.append(conn->hs_conn->get_readbuf_begin(),
        conn->hs_conn->get_response_end_offset(), &my_charset_bin);
      DBUG_PRINT("info",("spider hs readbuf13 size=%zu str=%s",
        conn->hs_conn->get_response_end_offset(), tmp_str.c_ptr_safe()));
    }
#endif
    conn->hs_conn->response_buf_remove();
    if (conn->conn_kind == SPIDER_CONN_KIND_HS_READ)
    {
      spider->r_handler_index[link_idx] = spider->active_index;
      spider->hs_r_conn_ages[link_idx] = conn->hs_age;
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
      if (spider->hs_pushed_ret_fields_num < MAX_FIELDS)
      {
        spider->hs_r_ret_fields_num[link_idx] =
          spider->hs_pushed_ret_fields_num;
        memcpy(spider->hs_r_ret_fields[link_idx], spider->hs_pushed_ret_fields,
          sizeof(uint32) * spider->hs_pushed_ret_fields_num);
      }
#endif
    } else {
      spider->w_handler_index[link_idx] = spider->active_index;
      spider->hs_w_conn_ages[link_idx] = conn->hs_age;
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
      if (spider->hs_pushed_ret_fields_num < MAX_FIELDS)
      {
        spider->hs_w_ret_fields_num[link_idx] =
          spider->hs_pushed_ret_fields_num;
        memcpy(spider->hs_w_ret_fields[link_idx], spider->hs_pushed_ret_fields,
          sizeof(uint32) * spider->hs_pushed_ret_fields_num);
      }
#endif
    }
  }
#endif
  if (!spider->handler_opened(link_idx, conn->conn_kind))
  {
    SPIDER_LINK_FOR_HASH *tmp_link_for_hash = &spider->link_for_hash[link_idx];
    DBUG_ASSERT(tmp_link_for_hash->spider == spider);
    DBUG_ASSERT(tmp_link_for_hash->link_idx == link_idx);
    uint old_elements = conn->handler_open_array.max_element;
    if (insert_dynamic(&conn->handler_open_array,
      (uchar*) &tmp_link_for_hash))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    if (conn->handler_open_array.max_element > old_elements)
    {
      spider_alloc_calc_mem(spider_current_trx,
        conn->handler_open_array,
        (conn->handler_open_array.max_element - old_elements) *
        conn->handler_open_array.size_of_element);
    }
    conn->opened_handlers++;
  }
  DBUG_PRINT("info",("spider conn=%p", conn));
  DBUG_PRINT("info",("spider opened_handlers=%u", conn->opened_handlers));
  conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
  conn->mta_conn_mutex_unlock_later = tmp_mta_conn_mutex_unlock_later;
  if (!tmp_mta_conn_mutex_unlock_later)
  {
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);

error:
  conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
  conn->mta_conn_mutex_unlock_later = tmp_mta_conn_mutex_unlock_later;
  if (!tmp_mta_conn_mutex_unlock_later)
  {
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(error_num);
}

int spider_db_close_handler(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx,
  uint tgt_conn_kind
) {
  int error_num;
  bool tmp_mta_conn_mutex_lock_already;
  bool tmp_mta_conn_mutex_unlock_later;
  spider_string *sql = &spider->result_list.sql;
  DBUG_ENTER("spider_db_close_handler");
  DBUG_PRINT("info",("spider conn=%p", conn));
  if (spider->handler_opened(link_idx, tgt_conn_kind))
  {
    if (!conn->mta_conn_mutex_lock_already)
    {
      pthread_mutex_lock(&conn->mta_conn_mutex);
      SPIDER_SET_FILE_POS(&conn->mta_conn_mutex_file_pos);
      conn->need_mon = &spider->need_mons[link_idx];
    }
    DBUG_ASSERT(conn->mta_conn_mutex_file_pos.file_name);
    tmp_mta_conn_mutex_lock_already = conn->mta_conn_mutex_lock_already;
    conn->mta_conn_mutex_lock_already = TRUE;
    tmp_mta_conn_mutex_unlock_later = conn->mta_conn_mutex_unlock_later;
    conn->mta_conn_mutex_unlock_later = TRUE;
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    if (conn->conn_kind == SPIDER_CONN_KIND_MYSQL)
    {
#endif
      sql->length(0);
      if (sql->reserve(SPIDER_SQL_HANDLER_LEN + SPIDER_SQL_CLOSE_LEN +
        SPIDER_SQL_HANDLER_CID_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      sql->q_append(SPIDER_SQL_HANDLER_STR, SPIDER_SQL_HANDLER_LEN);
      sql->q_append(spider->m_handler_cid[link_idx],
        SPIDER_SQL_HANDLER_CID_LEN);
      sql->q_append(SPIDER_SQL_CLOSE_STR, SPIDER_SQL_CLOSE_LEN);

      spider_conn_set_timeout_from_share(conn, link_idx, spider->trx->thd,
        spider->share);
      if (spider_db_query(
        conn,
        sql->ptr(),
        sql->length(),
        &spider->need_mons[link_idx])
      ) {
        error_num = spider_db_errorno(conn);
        goto error;
      }
      sql->length(0);
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
    } else {
/*
      conn->hs_conn->close();
      conn->server_lost = TRUE;
*/
    }
#endif
    uint roop_count, elements = conn->handler_open_array.elements;
    SPIDER_LINK_FOR_HASH *tmp_link_for_hash;
    for (roop_count = 0; roop_count < elements; roop_count++)
    {
      get_dynamic(&conn->handler_open_array, (uchar *) &tmp_link_for_hash,
        roop_count);
      if (tmp_link_for_hash == &spider->link_for_hash[link_idx])
      {
        delete_dynamic_element(&conn->handler_open_array, roop_count);
        break;
      }
    }
    DBUG_ASSERT(roop_count < elements);
    conn->opened_handlers--;
    DBUG_PRINT("info",("spider opened_handlers=%u", conn->opened_handlers));
    conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
    conn->mta_conn_mutex_unlock_later = tmp_mta_conn_mutex_unlock_later;
    if (!tmp_mta_conn_mutex_unlock_later)
    {
      SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
  }
  DBUG_RETURN(0);

error:
  conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
  conn->mta_conn_mutex_unlock_later = tmp_mta_conn_mutex_unlock_later;
  if (!tmp_mta_conn_mutex_unlock_later)
  {
    SPIDER_CLEAR_FILE_POS(&conn->mta_conn_mutex_file_pos);
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(error_num);
}

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
int spider_db_hs_request_buf_find(
  ha_spider *spider,
  int link_idx
) {
  int error_num;
  spider_string *hs_str;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  uint handler_id;
  DBUG_ENTER("spider_db_hs_request_buf_find");
  if (!(hs_str = spider_db_add_str_dynamic(&result_list->hs_strs,
    SPIDER_CALC_MEM_ID(result_list->hs_strs),
    SPIDER_CALC_MEM_FUNC(result_list->hs_strs),
    SPIDER_CALC_MEM_FILE(result_list->hs_strs),
    SPIDER_CALC_MEM_LINE(result_list->hs_strs),
    &result_list->hs_strs_pos, result_list->hs_sql.ptr(),
    result_list->hs_sql.length())))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if (spider->conn_kind[link_idx] == SPIDER_CONN_KIND_HS_READ)
  {
    conn = spider->hs_r_conns[link_idx];
    handler_id = spider->r_handler_id[link_idx];
  } else {
    conn = spider->hs_w_conns[link_idx];
    handler_id = spider->w_handler_id[link_idx];
  }
  if ((error_num = spider_db_conn_queue_action(conn)))
    DBUG_RETURN(error_num);
  conn->hs_conn->request_buf_exec_generic(
    handler_id,
    SPIDER_HS_STRING_REF(hs_str->ptr(), hs_str->length()),
#ifndef HANDLERSOCKET_MYSQL_UTIL
    &result_list->hs_keys[0], result_list->hs_keys.size(),
#else
    (SPIDER_HS_STRING_REF *) result_list->hs_keys.buffer,
    (size_t) result_list->hs_keys.elements,
#endif
    result_list->hs_limit, result_list->hs_skip,
    SPIDER_HS_STRING_REF(), NULL, 0);
  DBUG_RETURN(0);
}

int spider_db_hs_request_buf_insert(
  ha_spider *spider,
  int link_idx
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_hs_request_buf_insert");
  if ((error_num = spider_db_conn_queue_action(spider->hs_w_conns[link_idx])))
    DBUG_RETURN(error_num);
  spider->hs_w_conns[link_idx]->hs_conn->request_buf_exec_generic(
    spider->w_handler_id[link_idx],
    SPIDER_HS_STRING_REF(SPIDER_SQL_HS_INSERT_STR, SPIDER_SQL_HS_INSERT_LEN),
#ifndef HANDLERSOCKET_MYSQL_UTIL
    &result_list->hs_upds[0], result_list->hs_upds.size(),
#else
    (SPIDER_HS_STRING_REF *) result_list->hs_upds.buffer,
    (size_t) result_list->hs_upds.elements,
#endif
    0, 0,
    SPIDER_HS_STRING_REF(), NULL, 0);
  DBUG_RETURN(0);
}

int spider_db_hs_request_buf_update(
  ha_spider *spider,
  int link_idx
) {
  int error_num;
  spider_string *hs_str;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_hs_request_buf_update");
  if (!(hs_str = spider_db_add_str_dynamic(&result_list->hs_strs,
    SPIDER_CALC_MEM_ID(result_list->hs_strs),
    SPIDER_CALC_MEM_FUNC(result_list->hs_strs),
    SPIDER_CALC_MEM_FILE(result_list->hs_strs),
    SPIDER_CALC_MEM_LINE(result_list->hs_strs),
    &result_list->hs_strs_pos, result_list->hs_sql.ptr(),
    result_list->hs_sql.length())))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if ((error_num = spider_db_conn_queue_action(spider->hs_w_conns[link_idx])))
    DBUG_RETURN(error_num);
  spider->hs_w_conns[link_idx]->hs_conn->request_buf_exec_generic(
    spider->w_handler_id[link_idx],
    SPIDER_HS_STRING_REF(hs_str->ptr(), hs_str->length()),
#ifndef HANDLERSOCKET_MYSQL_UTIL
    &result_list->hs_keys[0], result_list->hs_keys.size(),
#else
    (SPIDER_HS_STRING_REF *) result_list->hs_keys.buffer,
    (size_t) result_list->hs_keys.elements,
#endif
    result_list->hs_limit, result_list->hs_skip,
    spider->hs_increment ?
      SPIDER_HS_STRING_REF(SPIDER_SQL_HS_INCREMENT_STR,
        SPIDER_SQL_HS_INCREMENT_LEN) :
      spider->hs_decrement ?
        SPIDER_HS_STRING_REF(SPIDER_SQL_HS_DECREMENT_STR,
          SPIDER_SQL_HS_DECREMENT_LEN) :
        SPIDER_HS_STRING_REF(SPIDER_SQL_HS_UPDATE_STR,
          SPIDER_SQL_HS_UPDATE_LEN),
#ifndef HANDLERSOCKET_MYSQL_UTIL
    &result_list->hs_upds[0], result_list->hs_upds.size()
#else
    (SPIDER_HS_STRING_REF *) result_list->hs_upds.buffer,
    (size_t) result_list->hs_upds.elements
#endif
  );
  DBUG_RETURN(0);
}

int spider_db_hs_request_buf_delete(
  ha_spider *spider,
  int link_idx
) {
  int error_num;
  spider_string *hs_str;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_hs_request_buf_delete");
  if (!(hs_str = spider_db_add_str_dynamic(&result_list->hs_strs,
    SPIDER_CALC_MEM_ID(result_list->hs_strs),
    SPIDER_CALC_MEM_FUNC(result_list->hs_strs),
    SPIDER_CALC_MEM_FILE(result_list->hs_strs),
    SPIDER_CALC_MEM_LINE(result_list->hs_strs),
    &result_list->hs_strs_pos, result_list->hs_sql.ptr(),
    result_list->hs_sql.length())))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if ((error_num = spider_db_conn_queue_action(spider->hs_w_conns[link_idx])))
    DBUG_RETURN(error_num);
  spider->hs_w_conns[link_idx]->hs_conn->request_buf_exec_generic(
    spider->w_handler_id[link_idx],
    SPIDER_HS_STRING_REF(hs_str->ptr(), hs_str->length()),
#ifndef HANDLERSOCKET_MYSQL_UTIL
    &result_list->hs_keys[0], result_list->hs_keys.size(),
#else
    (SPIDER_HS_STRING_REF *) result_list->hs_keys.buffer,
    (size_t) result_list->hs_keys.elements,
#endif
    result_list->hs_limit, result_list->hs_skip,
    SPIDER_HS_STRING_REF(SPIDER_SQL_HS_DELETE_STR, SPIDER_SQL_HS_DELETE_LEN),
    NULL, 0);
  DBUG_RETURN(0);
}
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
) {
  spider_string *element;
  DBUG_ENTER("spider_db_add_str_dynamic");
  if (array->elements <= *strs_pos + 1)
  {
    if (!(element = (spider_string *) spider_malloc(spider_current_trx, 8,
      sizeof(spider_string), MYF(MY_WME | MY_ZEROFILL))))
      DBUG_RETURN(NULL);
    element->init_calc_mem(98);
    element->set_charset(&my_charset_bin);
    if ((element->reserve(str_len + 1)))
    {
      spider_free(spider_current_trx, element, MYF(0));
      DBUG_RETURN(NULL);
    }
    element->q_append(str, str_len);
    uint old_elements = array->max_element;
    if (insert_dynamic(array, (uchar *) &element))
    {
      element->free();
      spider_free(spider_current_trx, element, MYF(0));
      DBUG_RETURN(NULL);
    }
    if (array->max_element > old_elements)
    {
      spider_alloc_calc_mem(spider_current_trx,
        array,
        (array->max_element - old_elements) *
        array->size_of_element);
    }
  } else {
    element = ((spider_string **) array->buffer)[*strs_pos];
    element->length(0);
    if ((element->reserve(str_len + 1)))
      DBUG_RETURN(NULL);
    element->q_append(str, str_len);
  }
  (*strs_pos)++;
  DBUG_RETURN(element);
}

void spider_db_free_str_dynamic(
  DYNAMIC_ARRAY *array
) {
  uint roop_count;
  spider_string *element;
  DBUG_ENTER("spider_db_free_str_dynamic");
  for (roop_count = 0; roop_count < array->elements; roop_count++)
  {
    get_dynamic(array, (uchar *) &element, roop_count);
    element->free();
    spider_free(spider_current_trx, element, MYF(0));
  }
  array->elements = 0;
  DBUG_VOID_RETURN;
}

void spider_db_clear_dynamic(
  DYNAMIC_ARRAY *array
) {
  uint roop_count;
  uchar *element;
  DBUG_ENTER("spider_db_clear_dynamic");
  for (roop_count = 0; roop_count < array->elements; roop_count++)
  {
    get_dynamic(array, (uchar *) &element, roop_count);
    delete element;
  }
  array->elements = 0;
  DBUG_VOID_RETURN;
}
