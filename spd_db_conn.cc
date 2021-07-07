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

#define MYSQL_SERVER 1
#include "mysql_priv.h"
#include <mysql/plugin.h>
#include <mysql.h>
#include <errmsg.h>
#include "spd_err.h"
#include "spd_param.h"
#include "spd_db_include.h"
#include "spd_include.h"
#include "ha_spider.h"
#include "spd_db_conn.h"
#include "spd_conn.h"

#define SPIDER_SQL_NAME_QUOTE_STR "`"
#define SPIDER_SQL_NAME_QUOTE_LEN (sizeof(SPIDER_SQL_NAME_QUOTE_STR) - 1)
const char *name_quote_str = SPIDER_SQL_NAME_QUOTE_STR;
#define SPIDER_SQL_VALUE_QUOTE_STR "'"
#define SPIDER_SQL_VALUE_QUOTE_LEN (sizeof(SPIDER_SQL_VALUE_QUOTE_STR) - 1)
#define SPIDER_SQL_DOT_STR "."
#define SPIDER_SQL_DOT_LEN (sizeof(SPIDER_SQL_DOT_STR) - 1)
#define SPIDER_SQL_COMMA_STR ","
#define SPIDER_SQL_COMMA_LEN (sizeof(SPIDER_SQL_COMMA_STR) - 1)
#define SPIDER_SQL_EQUAL_STR " = "
#define SPIDER_SQL_EQUAL_LEN (sizeof(SPIDER_SQL_EQUAL_STR) - 1)
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
  SPIDER_CONN *conn
) {
  uint net_timeout;
  THD* thd = current_thd;
  DBUG_ENTER("spider_mysql_real_connect");

  if (thd)
    net_timeout = THDVAR(thd, net_timeout) == -1 ?
      share->net_timeout : THDVAR(thd, net_timeout);
  else
    net_timeout = share->net_timeout;
  DBUG_PRINT("info",("spider net_timeout=%u", net_timeout));

  if (!(conn->db_conn = mysql_init(NULL)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  mysql_options(conn->db_conn, MYSQL_OPT_READ_TIMEOUT,
    &net_timeout);
  mysql_options(conn->db_conn, MYSQL_OPT_CONNECT_TIMEOUT,
    &net_timeout);


  /* tgt_db not use */
  if (!mysql_real_connect(conn->db_conn,
                          share->tgt_host,
                          share->tgt_username,
                          share->tgt_password,
                          NULL,
                          share->tgt_port,
                          share->tgt_socket,
                          0)
  ) {
    spider_db_disconnect(conn);
    my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0), share->server_name);
    DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
  }
  DBUG_RETURN(0);
}

int spider_db_ping(
  ha_spider *spider
) {
  int error_num;
  SPIDER_CONN *conn = spider->conn;
  DBUG_ENTER("spider_db_ping");
#ifndef DBUG_OFF
  if (spider->trx->thd)
    DBUG_PRINT("info", ("spider thd->query_id is %lld",
      spider->trx->thd->query_id));
#endif
  if (conn->server_lost)
  {
    conn->trx_isolation = -1;
    conn->autocommit = -1;
    conn->sql_log_off = -1;
    conn->access_charset = NULL;
    if ((error_num = spider_db_connect(spider->share, conn)))
      DBUG_RETURN(error_num);
    conn->server_lost = FALSE;
  }
  if ((error_num = simple_command(conn->db_conn, COM_PING, 0, 0, 0)))
  {
    conn->trx_isolation = -1;
    conn->autocommit = -1;
    conn->sql_log_off = -1;
    conn->access_charset = NULL;
    spider_db_disconnect(conn);
    if ((error_num = spider_db_connect(spider->share, conn)))
    {
      conn->server_lost = TRUE;
      DBUG_RETURN(error_num);
    }
    if((error_num = simple_command(conn->db_conn, COM_PING, 0, 0, 0)))
    {
      spider_db_disconnect(conn);
      conn->server_lost = TRUE;
      DBUG_RETURN(error_num);
    }
  }
  conn->ping_time = (time_t) time((time_t*) 0);
  DBUG_RETURN(0);
}

void spider_db_disconnect(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_db_disconnect");
  if (conn->db_conn)
  {
    mysql_close(conn->db_conn);
    conn->db_conn = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_db_query(
  SPIDER_CONN *conn,
  const char *query,
  uint length
) {
  DBUG_ENTER("spider_db_query");
  if (conn->server_lost)
    DBUG_RETURN(CR_SERVER_GONE_ERROR);
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (conn->bg_search)
    spider_bg_conn_break(conn, NULL);
#endif
  if (conn->quick_target)
  {
    int error_num;
    ha_spider *spider = (ha_spider*) conn->quick_target;
    SPIDER_RESULT_LIST *result_list = &spider->result_list;
    if (result_list->quick_mode == 2)
    {
      result_list->quick_phase = 1;
      while (conn->quick_target)
      {
        if ((error_num = spider_db_store_result(spider, result_list->table)))
          DBUG_RETURN(error_num);
      }
      result_list->quick_phase = 2;
    } else {
      mysql_free_result(result_list->bgs_current->result);
      result_list->bgs_current->result = NULL;
      conn->quick_target = NULL;
    }
  }
  DBUG_RETURN(mysql_real_query(conn->db_conn, query, length));
}

int spider_db_errorno(
  SPIDER_CONN *conn
) {
  int error_num;
  DBUG_ENTER("spider_db_errorno");
  if (conn->server_lost)
  {
    my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
      ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM);
  }
  if (error_num = mysql_errno(conn->db_conn))
  {
    DBUG_PRINT("info",("spider error_num = %d", error_num));
    if (
      error_num == CR_SERVER_GONE_ERROR ||
      error_num == CR_SERVER_LOST
    ) {
      spider_db_disconnect(conn);
      conn->server_lost = TRUE;
      if (conn->disable_reconnect)
        my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
          ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
      DBUG_RETURN(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM);
    } else if (
      conn->ignore_dup_key &&
      (error_num == ER_DUP_ENTRY || error_num == ER_DUP_KEY)
    ) {
      conn->error_str = (char*) mysql_error(conn->db_conn);
      conn->error_length = strlen(conn->error_str);
      DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    } else if (
      error_num == ER_XAER_NOTA &&
      current_thd &&
      THDVAR(current_thd, force_commit) == 1
    ) {
      push_warning(current_thd, MYSQL_ERROR::WARN_LEVEL_WARN,
        error_num, mysql_error(conn->db_conn));
      DBUG_RETURN(error_num);
    }
    my_message(error_num, mysql_error(conn->db_conn), MYF(0));
    DBUG_RETURN(error_num);
  }
  DBUG_RETURN(0);
}

int spider_db_set_trx_isolation(
  SPIDER_CONN *conn,
  int trx_isolation
) {
  DBUG_ENTER("spider_db_set_trx_isolation");
  switch (trx_isolation)
  {
    case ISO_READ_UNCOMMITTED:
      if (spider_db_query(
        conn,
        SPIDER_SQL_ISO_READ_UNCOMMITTED_STR,
        SPIDER_SQL_ISO_READ_UNCOMMITTED_LEN)
      )
        DBUG_RETURN(spider_db_errorno(conn));
      break;
    case ISO_READ_COMMITTED:
      if (spider_db_query(
        conn,
        SPIDER_SQL_ISO_READ_COMMITTED_STR,
        SPIDER_SQL_ISO_READ_COMMITTED_LEN)
      )
        DBUG_RETURN(spider_db_errorno(conn));
      break;
    case ISO_REPEATABLE_READ:
      if (spider_db_query(
        conn,
        SPIDER_SQL_ISO_REPEATABLE_READ_STR,
        SPIDER_SQL_ISO_REPEATABLE_READ_LEN)
      )
        DBUG_RETURN(spider_db_errorno(conn));
      break;
    case ISO_SERIALIZABLE:
      if (spider_db_query(
        conn,
        SPIDER_SQL_ISO_SERIALIZABLE_STR,
        SPIDER_SQL_ISO_SERIALIZABLE_LEN)
      )
        DBUG_RETURN(spider_db_errorno(conn));
      break;
    default:
      DBUG_RETURN(HA_ERR_UNSUPPORTED);
  }
  DBUG_RETURN(0);
}

int spider_db_set_autocommit(
  SPIDER_CONN *conn,
  bool autocommit
) {
  DBUG_ENTER("spider_db_set_autocommit");
  if (autocommit)
  {
    if (spider_db_query(
      conn,
      SPIDER_SQL_AUTOCOMMIT_ON_STR,
      SPIDER_SQL_AUTOCOMMIT_ON_LEN)
    )
      DBUG_RETURN(spider_db_errorno(conn));
  } else {
    if (spider_db_query(
      conn,
      SPIDER_SQL_AUTOCOMMIT_OFF_STR,
      SPIDER_SQL_AUTOCOMMIT_OFF_LEN)
    )
      DBUG_RETURN(spider_db_errorno(conn));
  }
  DBUG_RETURN(0);
}

int spider_db_set_sql_log_off(
  SPIDER_CONN *conn,
  bool sql_log_off
) {
  DBUG_ENTER("spider_db_set_sql_log_off");
  if (sql_log_off)
  {
    if (spider_db_query(
      conn,
      SPIDER_SQL_SQL_LOG_ON_STR,
      SPIDER_SQL_SQL_LOG_ON_LEN)
    )
      DBUG_RETURN(spider_db_errorno(conn));
  } else {
    if (spider_db_query(
      conn,
      SPIDER_SQL_SQL_LOG_OFF_STR,
      SPIDER_SQL_SQL_LOG_OFF_LEN)
    )
      DBUG_RETURN(spider_db_errorno(conn));
  }
  DBUG_RETURN(0);
}

int spider_db_set_names(
  SPIDER_SHARE *share,
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_db_set_names");
  if (share->access_charset != conn->access_charset)
  {
    if (spider_db_query(
      conn,
      share->set_names->ptr(),
      share->set_names->length())
    )
      DBUG_RETURN(spider_db_errorno(conn));
    conn->access_charset = share->access_charset;
  }
  DBUG_RETURN(0);
}

int spider_db_consistent_snapshot(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_db_consistent_snapshot");
  if (spider_db_query(
    conn,
    SPIDER_SQL_START_CONSISTENT_SNAPSHOT_STR,
    SPIDER_SQL_START_CONSISTENT_SNAPSHOT_LEN)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  conn->trx_start = TRUE;
  DBUG_RETURN(0);
}

int spider_db_start_transaction(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_db_start_transaction");
  if (spider_db_query(
    conn,
    SPIDER_SQL_START_TRANSACTION_STR,
    SPIDER_SQL_START_TRANSACTION_LEN)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  conn->trx_start = TRUE;
  DBUG_RETURN(0);
}

int spider_db_commit(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_db_commit");
  if (spider_db_query(
    conn,
    SPIDER_SQL_COMMIT_STR,
    SPIDER_SQL_COMMIT_LEN)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  conn->trx_start = FALSE;
  DBUG_RETURN(0);
}

int spider_db_rollback(
  SPIDER_CONN *conn
) {
  int error_num;
  bool is_error;
  DBUG_ENTER("spider_db_rollback");
  if (spider_db_query(
    conn,
    SPIDER_SQL_ROLLBACK_STR,
    SPIDER_SQL_ROLLBACK_LEN)
  ) {
    is_error = conn->thd->is_error();
    error_num = spider_db_errorno(conn);
    if (
      error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
      !is_error
    )
      conn->thd->clear_error();
    else
      DBUG_RETURN(error_num);
  }
  conn->trx_start = FALSE;
  DBUG_RETURN(0);
}

int spider_db_append_hex_string(
  String *str,
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
  String *tmp_str,
  XID *xid
) {
  char format_id[sizeof(long) + 3];
  uint format_id_length;
  DBUG_ENTER("spider_db_append_xid_str");

  format_id_length =
    my_sprintf(format_id, (format_id, "0x%lx", xid->formatID));
  VOID(spider_db_append_hex_string(tmp_str, (uchar *) xid->data,
    xid->gtrid_length));
/*
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  tmp_str->q_append(xid->data, xid->gtrid_length);
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
*/
  tmp_str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  VOID(spider_db_append_hex_string(tmp_str,
    (uchar *) xid->data + xid->gtrid_length, xid->bqual_length));
/*
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  tmp_str->q_append(xid->data + xid->gtrid_length, xid->bqual_length);
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
*/
  tmp_str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  tmp_str->q_append(format_id, format_id_length);

  DBUG_VOID_RETURN;
}

int spider_db_xa_start(
  SPIDER_CONN *conn,
  XID *xid
) {
  char sql_buf[SPIDER_SQL_XA_START_LEN + XIDDATASIZE + sizeof(long) + 9];
  String sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_start");

  sql_str.length(0);
  sql_str.q_append(SPIDER_SQL_XA_START_STR, SPIDER_SQL_XA_START_LEN);
  spider_db_append_xid_str(&sql_str, xid);
  if (spider_db_query(
    conn,
    sql_str.ptr(),
    sql_str.length())
  )
    DBUG_RETURN(spider_db_errorno(conn));
  DBUG_RETURN(0);
}

int spider_db_xa_end(
  SPIDER_CONN *conn,
  XID *xid
) {
  char sql_buf[SPIDER_SQL_XA_END_LEN + XIDDATASIZE + sizeof(long) + 9];
  String sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_end");

  sql_str.length(0);
  sql_str.q_append(SPIDER_SQL_XA_END_STR, SPIDER_SQL_XA_END_LEN);
  spider_db_append_xid_str(&sql_str, xid);
  if (spider_db_query(
    conn,
    sql_str.ptr(),
    sql_str.length())
  )
    DBUG_RETURN(spider_db_errorno(conn));
  DBUG_RETURN(0);
}

int spider_db_xa_prepare(
  SPIDER_CONN *conn,
  XID *xid
) {
  char sql_buf[SPIDER_SQL_XA_PREPARE_LEN + XIDDATASIZE + sizeof(long) + 9];
  String sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_prepare");

  sql_str.length(0);
  sql_str.q_append(SPIDER_SQL_XA_PREPARE_STR, SPIDER_SQL_XA_PREPARE_LEN);
  spider_db_append_xid_str(&sql_str, xid);
  if (spider_db_query(
    conn,
    sql_str.ptr(),
    sql_str.length())
  )
    DBUG_RETURN(spider_db_errorno(conn));
  DBUG_RETURN(0);
}

int spider_db_xa_commit(
  SPIDER_CONN *conn,
  XID *xid
) {
  char sql_buf[SPIDER_SQL_XA_COMMIT_LEN + XIDDATASIZE + sizeof(long) + 9];
  String sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_commit");

  sql_str.length(0);
  sql_str.q_append(SPIDER_SQL_XA_COMMIT_STR, SPIDER_SQL_XA_COMMIT_LEN);
  spider_db_append_xid_str(&sql_str, xid);
  if (spider_db_query(
    conn,
    sql_str.ptr(),
    sql_str.length())
  )
    DBUG_RETURN(spider_db_errorno(conn));
  DBUG_RETURN(0);
}

int spider_db_xa_rollback(
  SPIDER_CONN *conn,
  XID *xid
) {
  char sql_buf[SPIDER_SQL_XA_ROLLBACK_LEN + XIDDATASIZE + sizeof(long) + 9];
  String sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_rollback");

  sql_str.length(0);
  sql_str.q_append(SPIDER_SQL_XA_ROLLBACK_STR, SPIDER_SQL_XA_ROLLBACK_LEN);
  spider_db_append_xid_str(&sql_str, xid);
  if (spider_db_query(
    conn,
    sql_str.ptr(),
    sql_str.length())
  )
    DBUG_RETURN(spider_db_errorno(conn));
  DBUG_RETURN(0);
}

int spider_db_lock_tables(
  ha_spider *spider
) {
  int error_num;
  ha_spider *tmp_spider;
  int lock_type;
  SPIDER_CONN *conn = spider->conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_lock_tables");
  str->length(0);
  if (str->reserve(SPIDER_SQL_LOCK_TABLE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_LOCK_TABLE_STR, SPIDER_SQL_LOCK_TABLE_LEN);
  while ((tmp_spider = (ha_spider*) hash_element(&conn->lock_table_hash, 0)))
  {
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
    if (tmp_spider->share->db_name_str)
    {
      if (
        str->append(tmp_spider->share->db_name_str->ptr(),
          tmp_spider->share->db_name_str->length(),
          tmp_spider->share->access_charset) ||
        str->reserve((SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_DOT_LEN)
      ) {
        my_hash_reset(&conn->lock_table_hash);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    } else {
      if (
        str->append(tmp_spider->share->tgt_db,
          tmp_spider->share->tgt_db_length,
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
    if (tmp_spider->share->table_name_str)
    {
      if (
        str->append(tmp_spider->share->table_name_str->ptr(),
          tmp_spider->share->table_name_str->length(),
          tmp_spider->share->access_charset) ||
        str->reserve(SPIDER_SQL_NAME_QUOTE_LEN +
          spider_db_table_lock_len[lock_type])
      ) {
        my_hash_reset(&conn->lock_table_hash);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    } else {
      if (
        str->append(tmp_spider->share->tgt_table_name,
          tmp_spider->share->tgt_table_name_length,
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
    hash_delete(&conn->lock_table_hash, (uchar*) tmp_spider);
  }
  if ((error_num = spider_db_set_names(spider->share, conn)))
    DBUG_RETURN(error_num);
  if (spider_db_query(
    conn,
    str->ptr(),
    str->length() - SPIDER_SQL_COMMA_LEN)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  if (!conn->table_locked)
  {
    conn->table_locked = TRUE;
    spider->trx->locked_connections++;
  }
  DBUG_RETURN(0);
}

int spider_db_unlock_tables(
  ha_spider *spider
) {
  SPIDER_CONN *conn = spider->conn;
  DBUG_ENTER("spider_db_unlock_tables");
  if (conn->table_locked)
  {
    conn->table_locked = FALSE;
    spider->trx->locked_connections--;
  }
  if (spider_db_query(
    conn,
    SPIDER_SQL_UNLOCK_TABLE_STR,
    SPIDER_SQL_UNLOCK_TABLE_LEN)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  DBUG_RETURN(0);
}

int spider_db_append_name_with_quote_str(
  String *str,
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

int spider_db_create_table_name_str(
  SPIDER_SHARE *share
) {
  String *str;
  int error_num;
  DBUG_ENTER("spider_db_create_table_name_str");
  if (
    !(share->table_name_str = new String[1]) ||
    !(share->db_name_str = new String[1])
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str = share->table_name_str;
  str->set_charset(share->access_charset);
  if ((error_num = spider_db_append_name_with_quote_str(str,
    share->tgt_table_name)))
    goto error;
  str = share->db_name_str;
  str->set_charset(share->access_charset);
  if ((error_num = spider_db_append_name_with_quote_str(str,
    share->tgt_db)))
    goto error;
  DBUG_RETURN(0);

error:
  if (share->db_name_str)
  {
    delete [] share->db_name_str;
    share->db_name_str = NULL;
  }
  if (share->table_name_str)
  {
    delete [] share->table_name_str;
    share->table_name_str = NULL;
  }
  DBUG_RETURN(error_num);
}

void spider_db_free_table_name_str(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_free_table_name_str");
  if (share->db_name_str)
  {
    delete [] share->db_name_str;
    share->db_name_str = NULL;
  }
  if (share->table_name_str)
  {
    delete [] share->table_name_str;
    share->table_name_str = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_db_create_column_name_str(
  SPIDER_SHARE *share,
  TABLE_SHARE *table_share
) {
  String *str;
  int error_num;
  Field **field;
  DBUG_ENTER("spider_db_create_column_name_str");
  if (
    table_share->fields &&
    !(share->column_name_str = new String[table_share->fields])
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  for (field = table_share->field, str = share->column_name_str;
   *field; field++, str++)
  {
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
  String tmp_str(buf, MAX_FIELD_WIDTH, share->access_charset), *key_hint;
  int roop_count;
  DBUG_ENTER("spider_db_convert_key_hint_str");
  if (share->access_charset->cset != system_charset_info->cset)
  {
    /* need convertion */
    for (roop_count = 0, key_hint = share->key_hint;
      roop_count < table_share->keys; roop_count++, key_hint++)
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

void spider_db_append_column_name(
  SPIDER_SHARE *share,
  String *str,
  int field_index
) {
  DBUG_ENTER("spider_db_append_column_name");
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->column_name_str[field_index].ptr(),
    share->column_name_str[field_index].length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  DBUG_VOID_RETURN;
}

int spider_db_append_column_value(
  SPIDER_SHARE *share,
  String *str,
  Field *field,
  const uchar *new_ptr
) {
  char buf[MAX_FIELD_WIDTH];
  String tmp_str(buf, MAX_FIELD_WIDTH, &my_charset_bin), *ptr;
  DBUG_ENTER("spider_db_append_column_value");

  if (new_ptr)
    ptr = field->val_str(&tmp_str, new_ptr);
  else
    ptr = field->val_str(&tmp_str);
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
  if (field->result_type() == STRING_RESULT)
  {
    DBUG_PRINT("info", ("spider STRING_RESULT"));
    if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    if (
      field->type() == MYSQL_TYPE_VARCHAR ||
      (field->type() >= MYSQL_TYPE_ENUM &&
        field->type() <= MYSQL_TYPE_STRING)
    ) {
      DBUG_PRINT("info", ("spider append_escaped"));
      char buf2[MAX_FIELD_WIDTH];
      String tmp_str2(buf2, MAX_FIELD_WIDTH, share->access_charset);
      tmp_str2.length(0);
      if (tmp_str2.append(ptr->ptr(), ptr->length(), field->charset()) ||
        str->reserve(tmp_str2.length() * 2) ||
        append_escaped(str, &tmp_str2))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
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
    append_escaped(str, ptr);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  } else if (str->append(*ptr))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  DBUG_RETURN(0);
}

int spider_db_append_select(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_append_select");

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
      str->q_append(SPIDER_SQL_SQL_NO_CACHE_STR, SPIDER_SQL_SQL_NO_CACHE_LEN);
    }
  }
  if (spider->high_priority)
  {
    if (str->reserve(SPIDER_SQL_HIGH_PRIORITY_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_HIGH_PRIORITY_STR, SPIDER_SQL_HIGH_PRIORITY_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_insert(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->insert_sql;
  DBUG_ENTER("spider_db_append_insert");

  if (spider->write_can_replace)
  {
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
  else if (spider->lock_type >= TL_WRITE && !spider->write_can_replace)
  {
    if (str->reserve(SPIDER_SQL_HIGH_PRIORITY_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_HIGH_PRIORITY_STR, SPIDER_SQL_HIGH_PRIORITY_LEN);
  }
  if (
    spider->ignore_dup_key &&
    !spider->write_can_replace &&
    !spider->insert_with_update
  ) {
    if (str->reserve(SPIDER_SQL_SQL_IGNORE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_IGNORE_STR, SPIDER_SQL_SQL_IGNORE_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_update(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_update");

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
  if (str->reserve(share->db_name_str->length() +
    SPIDER_SQL_DOT_LEN + share->table_name_str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_delete(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_append_delete");

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
  DBUG_RETURN(0);
}

int spider_db_append_truncate(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_truncate");

  if (str->reserve(SPIDER_SQL_TRUNCATE_TABLE_LEN + share->db_name_str->length() +
    SPIDER_SQL_DOT_LEN + share->table_name_str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_TRUNCATE_TABLE_STR, SPIDER_SQL_TRUNCATE_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_from(
  String *str,
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_append_from");
  if (str->reserve(SPIDER_SQL_FROM_LEN + share->db_name_str->length() +
    SPIDER_SQL_DOT_LEN + share->table_name_str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_into(
  ha_spider *spider,
  const TABLE *table
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->insert_sql;
  SPIDER_SHARE *share = spider->share;
  Field **field;
  uint field_name_length = 0;
  DBUG_ENTER("spider_db_append_into");
  if (str->reserve(SPIDER_SQL_INTO_LEN + share->db_name_str->length() +
    SPIDER_SQL_DOT_LEN + share->table_name_str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  for (field = table->field; *field; field++)
  {
    if (bitmap_is_set(table->write_set, (*field)->field_index))
    {
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
  ha_spider *spider,
  TABLE *table
) {
  int error_num;
  uint field_name_length;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  String *str = &result_list->sql;
  Field **field;
  DBUG_ENTER("spider_db_append_update_set");
  if (str->reserve(SPIDER_SQL_SET_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SET_STR, SPIDER_SQL_SET_LEN);
  for (field = table->field; *field; field++)
  {
    if (bitmap_is_set(table->write_set, (*field)->field_index))
    {
      field_name_length =
        share->column_name_str[(*field)->field_index].length();
      if ((*field)->is_null())
      {
        if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
          SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_NULL_LEN +
          SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(share, str, (*field)->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
        str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
      } else {
        if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
          SPIDER_SQL_EQUAL_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(share, str, (*field)->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
#ifndef DBUG_OFF
        my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table,
          table->read_set);
#endif
        if (
          spider_db_append_column_value(share, str, *field, NULL) ||
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
  DBUG_RETURN(0);
}

int spider_db_append_update_where(
  ha_spider *spider,
  const TABLE *table,
  my_ptrdiff_t ptr_diff
) {
  int error_num;
  uint field_name_length;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  String *str = &result_list->sql;
  Field **field;
  DBUG_ENTER("spider_db_append_update_where");
  if (str->reserve(SPIDER_SQL_WHERE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);
  for (field = table->field; *field; field++)
  {
    if (bitmap_is_set(table->read_set, (*field)->field_index))
    {
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
          spider_db_append_column_value(share, str, *field, NULL) ||
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
  String *str,
  const TABLE *table,
  SPIDER_SHARE *share
) {
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
  DBUG_RETURN(spider_db_append_from(str, share));
}

int spider_db_append_key_select(
  String *str,
  const KEY *key_info,
  SPIDER_SHARE *share
) {
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
  DBUG_RETURN(spider_db_append_from(str, share));
}

int spider_db_append_minimum_select(
  String *str,
  const TABLE *table,
  SPIDER_SHARE *share
) {
  Field **field;
  int field_length;
  bool appended = FALSE;
  DBUG_ENTER("spider_db_append_minimum_select");
  for (field = table->field; *field; field++)
  {
    if ((
      bitmap_is_set(table->read_set, (*field)->field_index) |
      bitmap_is_set(table->write_set, (*field)->field_index)
    )) {
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
  DBUG_RETURN(spider_db_append_from(str, share));
}

int spider_db_append_select_columns(
  ha_spider *spider
) {
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_append_select_columns");
  if (!spider->select_column_mode)
  {
    if (result_list->keyread)
    {
      if (result_list->sql.append(share->key_select[spider->active_index]))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    } else {
      if (result_list->sql.append(*(share->table_select)))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    DBUG_RETURN(0);
  }
  DBUG_RETURN(spider_db_append_minimum_select(
    &result_list->sql, spider->get_table(), share));
}

int spider_db_append_null(
  SPIDER_SHARE *share,
  String *str,
  KEY_PART_INFO *key_part,
  const key_range *key,
  const uchar **ptr
) {
  DBUG_ENTER("spider_db_append_null");
  if (key_part->null_bit)
  {
    if (*(*ptr)++)
    {
      if (key->flag == HA_READ_KEY_EXACT)
      {
        if (str->reserve(SPIDER_SQL_IS_NULL_LEN +
          share->column_name_str[key_part->field->field_index].length()))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(share, str, key_part->field->field_index);
        str->q_append(SPIDER_SQL_IS_NULL_STR,
          SPIDER_SQL_IS_NULL_LEN);
      } else {
        if (str->reserve(SPIDER_SQL_IS_NOT_NULL_LEN +
          share->column_name_str[key_part->field->field_index].length()))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(share, str, key_part->field->field_index);
        str->q_append(SPIDER_SQL_IS_NOT_NULL_STR,
          SPIDER_SQL_IS_NOT_NULL_LEN);
      }
      DBUG_RETURN(-1);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_append_key_hint(
  String *str,
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
  String *str,
  String *hint
) {
  DBUG_ENTER("spider_db_append_hint_after_table");
  if (str->append(*hint))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  DBUG_RETURN(0);
}

int spider_db_append_key_where(
  const key_range *start_key,
  const key_range *end_key,
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  TABLE *table = spider->get_table();
  String *str = &result_list->sql;
  KEY *key_info = result_list->key_info;
  int error_num;
  uint key_name_length;
  key_part_map full_key_part_map = make_prev_keypart_map(key_info->key_parts);
  key_part_map start_key_part_map;
  key_part_map end_key_part_map;
  key_part_map tgt_key_part_map;
  int key_count;
  uint length;
  uint store_length;
  const uchar *ptr;
  const key_range *use_key;
  KEY_PART_INFO *key_part;
  Field *field;
  DBUG_ENTER("spider_db_append_key_where");

  if (start_key)
    start_key_part_map = start_key->keypart_map & full_key_part_map;
  else
    start_key_part_map = 0;
  if (end_key)
    end_key_part_map = end_key->keypart_map & full_key_part_map;
  else
    end_key_part_map = 0;
  DBUG_PRINT("info", ("spider key_info->key_parts=%lu", key_info->key_parts));
  DBUG_PRINT("info", ("spider full_key_part_map=%lu", full_key_part_map));
  DBUG_PRINT("info", ("spider start_key_part_map=%lu", start_key_part_map));
  DBUG_PRINT("info", ("spider end_key_part_map=%lu", end_key_part_map));

#ifndef DBUG_OFF
  my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table, table->read_set);
#endif
/*
  if (spider->condition)
  {
*/
    /* use condition */
/*
    if (spider_db_append_condition(spider, str))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);

    if (start_key_part_map >= end_key_part_map)
      tgt_key_part_map = start_key_part_map;
    else
      tgt_key_part_map = end_key_part_map;
    for (
      key_count = 0;
      tgt_key_part_map > 1;
      tgt_key_part_map >>= 1
    ) {
      key_count++;
    }
    if (
      key_count > 0 &&
      start_key_part_map >= end_key_part_map &&
      start_key->flag == HA_READ_KEY_EXACT
    )
      key_count++;
    result_list->key_order = key_count;
    goto end;
  }
*/

  if (!start_key_part_map && !end_key_part_map)
    goto end;
  else if (start_key_part_map >= end_key_part_map)
  {
    use_key = start_key;
    tgt_key_part_map = start_key_part_map;
  } else {
    use_key = end_key;
    tgt_key_part_map = end_key_part_map;
  }
  DBUG_PRINT("info", ("spider tgt_key_part_map=%lu", tgt_key_part_map));

  if (str->reserve(SPIDER_SQL_WHERE_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_WHERE_STR, SPIDER_SQL_WHERE_LEN);

  for (
    key_part = key_info->key_part,
    store_length = key_part->store_length,
    length = 0,
    key_count = 0,
    ptr = use_key->key;
    tgt_key_part_map > 1;
    length += store_length,
    ptr = use_key->key + length,
    tgt_key_part_map >>= 1,
    key_part++,
    key_count++,
    store_length = key_part->store_length
  ) {
    field = key_part->field;
    key_name_length = share->column_name_str[field->field_index].length();
    if (key_part->null_bit && *ptr++)
    {
      if (str->reserve(SPIDER_SQL_IS_NULL_LEN + key_name_length +
        SPIDER_SQL_AND_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      spider_db_append_column_name(share, str, field->field_index);
      str->q_append(SPIDER_SQL_IS_NULL_STR,
        SPIDER_SQL_IS_NULL_LEN);
    } else {
      if (str->reserve(store_length + key_name_length +
        SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_AND_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      spider_db_append_column_name(share, str, field->field_index);
      str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
      if (spider_db_append_column_value(share, str, field, ptr) ||
        str->reserve(SPIDER_SQL_AND_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    str->q_append(SPIDER_SQL_AND_STR,
      SPIDER_SQL_AND_LEN);
  }

  /* last one */
  field = key_part->field;
  key_name_length = share->column_name_str[field->field_index].length();
  result_list->key_order = key_count;
  if (start_key_part_map >= end_key_part_map)
  {
    ptr = start_key->key + length;
    if ((error_num = spider_db_append_null(share, str, key_part,
      start_key, &ptr)))
    {
      if (error_num > 0)
        DBUG_RETURN(error_num);
    } else {
      DBUG_PRINT("info", ("spider start_key->flag=%d", start_key->flag));
      switch (start_key->flag)
      {
        case HA_READ_KEY_EXACT:
          if (str->reserve(store_length + key_name_length +
            SPIDER_SQL_EQUAL_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
          if (spider_db_append_column_value(share, str, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          result_list->key_order++;
          break;
        case HA_READ_AFTER_KEY:
          if (str->reserve(store_length + key_name_length +
            SPIDER_SQL_GT_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_GT_STR, SPIDER_SQL_GT_LEN);
          if (spider_db_append_column_value(share, str, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          break;
        default:
          if (str->reserve(store_length + key_name_length +
            SPIDER_SQL_GTEQUAL_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_GTEQUAL_STR, SPIDER_SQL_GTEQUAL_LEN);
          if (spider_db_append_column_value(share, str, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          break;
      }
    }
  }
  if (start_key_part_map == end_key_part_map)
  {
    if (str->reserve(SPIDER_SQL_AND_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_AND_STR,
      SPIDER_SQL_AND_LEN);
  }
  if (end_key_part_map >= start_key_part_map)
  {
    ptr = end_key->key + length;
    if ((error_num = spider_db_append_null(share, str, key_part,
      end_key, &ptr)))
    {
      if (error_num > 0)
        DBUG_RETURN(error_num);
    } else {
      DBUG_PRINT("info", ("spider end_key->flag=%d", end_key->flag));
      switch (end_key->flag)
      {
        case HA_READ_BEFORE_KEY:
          if (str->reserve(store_length + key_name_length +
            SPIDER_SQL_LT_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_LT_STR, SPIDER_SQL_LT_LEN);
          if (spider_db_append_column_value(share, str, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          break;
        default:
          if (str->reserve(store_length + key_name_length +
            SPIDER_SQL_LTEQUAL_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_LTEQUAL_STR, SPIDER_SQL_LTEQUAL_LEN);
          if (spider_db_append_column_value(share, str, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          break;
      }
    }
  }

end:
  /* use condition */
  if (spider_db_append_condition(spider, str))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  result_list->order_pos = str->length();
#ifndef DBUG_OFF
  dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
  DBUG_RETURN(0);
}

int spider_db_append_key_order(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  String *str = &result_list->sql;
  KEY *key_info = result_list->key_info;
  int length;
  KEY_PART_INFO *key_part;
  Field *field;
  uint key_name_length;
  DBUG_ENTER("spider_db_append_key_order");

  if (result_list->sorted == TRUE)
  {
    if (result_list->desc_flg == TRUE)
    {
      for (
        key_part = key_info->key_part + result_list->key_order,
        length = 1;
        length + result_list->key_order < key_info->key_parts &&
        length < result_list->max_order;
        key_part++,
        length++
      ) {
        field = key_part->field;
        key_name_length = share->column_name_str[field->field_index].length();
        if (length == 1)
        {
          if (str->reserve(SPIDER_SQL_ORDER_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
        }
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (str->reserve(key_name_length + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        } else {
          if (str->reserve(key_name_length +
            SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
      if (
        length + result_list->key_order <= key_info->key_parts &&
        length <= result_list->max_order
      ) {
        field = key_part->field;
        key_name_length = share->column_name_str[field->field_index].length();
        if (length == 1)
        {
          if (str->reserve(SPIDER_SQL_ORDER_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
        }
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (str->reserve(key_name_length))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
        } else {
          if (str->reserve(key_name_length + SPIDER_SQL_DESC_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
        }
      }
    } else {
      for (
        key_part = key_info->key_part + result_list->key_order,
        length = 1;
        length + result_list->key_order < key_info->key_parts &&
        length < result_list->max_order;
        key_part++,
        length++
      ) {
        field = key_part->field;
        key_name_length = share->column_name_str[field->field_index].length();
        if (length == 1)
        {
          if (str->reserve(SPIDER_SQL_ORDER_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
        }
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (str->reserve(key_name_length +
            SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        } else {
          if (str->reserve(key_name_length + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
      if (
        length + result_list->key_order <= key_info->key_parts &&
        length <= result_list->max_order
      ) {
        field = key_part->field;
        key_name_length = share->column_name_str[field->field_index].length();
        if (length == 1)
        {
          if (str->reserve(SPIDER_SQL_ORDER_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
        }
        if (key_part->key_part_flag & HA_REVERSE_SORT)
        {
          if (str->reserve(key_name_length + SPIDER_SQL_DESC_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
        } else {
          if (str->reserve(key_name_length))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(share, str, field->field_index);
        }
      }
    }
  }
  result_list->limit_pos = str->length();
  DBUG_RETURN(0);
}

int spider_db_append_limit(
  String *str,
  longlong offset,
  longlong limit
) {
  char buf[SPIDER_LONGLONG_LEN + 1];
  uint32 length;
  DBUG_ENTER("spider_db_append_limit");
  if (str->reserve(SPIDER_SQL_LIMIT_LEN + SPIDER_SQL_COMMA_LEN +
    ((SPIDER_LONGLONG_LEN) * 2)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_LIMIT_STR, SPIDER_SQL_LIMIT_LEN);
  length = (uint32) (my_charset_bin.cset->longlong10_to_str)(
    &my_charset_bin, buf, SPIDER_LONGLONG_LEN + 1, -10, offset);
  str->q_append(buf, length);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  length = (uint32) (my_charset_bin.cset->longlong10_to_str)(
    &my_charset_bin, buf, SPIDER_LONGLONG_LEN + 1, -10, limit);
  str->q_append(buf, length);
  DBUG_RETURN(0);
}

int spider_db_append_select_lock(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_append_lock");
  if (result_list->lock_type == F_WRLCK || spider->lock_mode == 2)
  {
    if (str->reserve(SPIDER_SQL_FOR_UPDATE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_FOR_UPDATE_STR, SPIDER_SQL_FOR_UPDATE_LEN);
  } else if (spider->lock_mode == 1)
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
  String *str;
  DBUG_ENTER("spider_db_append_show_table_status");
  if (
    !(share->show_table_status = new String[2]) ||
    share->show_table_status[0].reserve(
      SPIDER_SQL_SHOW_TABLE_STATUS_LEN + share->db_name_str->length() +
      SPIDER_SQL_LIKE_LEN + share->table_name_str->length() +
      ((SPIDER_SQL_NAME_QUOTE_LEN) * 2) +
      ((SPIDER_SQL_VALUE_QUOTE_LEN) * 2)) ||
    share->show_table_status[1].reserve(
      SPIDER_SQL_SELECT_TABLES_STATUS_LEN + share->db_name_str->length() +
      SPIDER_SQL_AND_LEN + SPIDER_SQL_TABLE_NAME_LEN + SPIDER_SQL_EQUAL_LEN +
      share->table_name_str->length() + ((SPIDER_SQL_VALUE_QUOTE_LEN) * 4))
  )
    goto error;
  str = &share->show_table_status[0];
  str->q_append(
    SPIDER_SQL_SHOW_TABLE_STATUS_STR, SPIDER_SQL_SHOW_TABLE_STATUS_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_LIKE_STR, SPIDER_SQL_LIKE_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str = &share->show_table_status[1];
  str->q_append(
    SPIDER_SQL_SELECT_TABLES_STATUS_STR, SPIDER_SQL_SELECT_TABLES_STATUS_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
  str->q_append(SPIDER_SQL_TABLE_NAME_STR, SPIDER_SQL_TABLE_NAME_LEN);
  str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
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

int spider_db_append_show_index(
  SPIDER_SHARE *share
) {
  String *str;
  DBUG_ENTER("spider_db_append_show_index");
  if (
    !(share->show_index = new String[2]) ||
    share->show_index[0].reserve(
      SPIDER_SQL_SHOW_INDEX_LEN + share->db_name_str->length() +
      SPIDER_SQL_DOT_LEN +
      share->table_name_str->length() + ((SPIDER_SQL_NAME_QUOTE_LEN) * 4)) ||
    share->show_index[1].reserve(
      SPIDER_SQL_SELECT_STATISTICS_LEN + share->db_name_str->length() +
      SPIDER_SQL_AND_LEN + SPIDER_SQL_TABLE_NAME_LEN + SPIDER_SQL_EQUAL_LEN +
      share->table_name_str->length() + ((SPIDER_SQL_VALUE_QUOTE_LEN) * 4) +
      SPIDER_SQL_GROUP_LEN + SPIDER_SQL_COLUMN_NAME_LEN)
  )
    goto error;
  str = &share->show_index[0];
  str->q_append(
    SPIDER_SQL_SHOW_INDEX_STR, SPIDER_SQL_SHOW_INDEX_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str = &share->show_index[1];
  str->q_append(
    SPIDER_SQL_SELECT_STATISTICS_STR, SPIDER_SQL_SELECT_STATISTICS_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
  str->q_append(SPIDER_SQL_TABLE_NAME_STR, SPIDER_SQL_TABLE_NAME_LEN);
  str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(SPIDER_SQL_GROUP_STR, SPIDER_SQL_GROUP_LEN);
  str->q_append(SPIDER_SQL_COLUMN_NAME_STR, SPIDER_SQL_COLUMN_NAME_LEN);
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
  String *str;
  char *csname = (char *) share->access_charset->csname;
  int csname_length = strlen(csname);
  DBUG_ENTER("spider_db_append_set_names");
  if (
    !(share->set_names = new String[1]) ||
    share->set_names->reserve(
      SPIDER_SQL_SET_NAMES_LEN +
      csname_length)
  )
    goto error;
  str = share->set_names;
  str->q_append(
    SPIDER_SQL_SET_NAMES_STR, SPIDER_SQL_SET_NAMES_LEN);
  str->q_append(csname, csname_length);
  DBUG_RETURN(0);

error:
  if (share->set_names)
  {
    delete [] share->set_names;
    share->set_names = NULL;
  }
  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
}

void spider_db_free_set_names(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_free_set_names");
  if (share->set_names)
  {
    delete [] share->set_names;
    share->set_names = NULL;
  }
  DBUG_VOID_RETURN;
}

int spider_db_append_disable_keys(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_disable_keys");

  if (str->reserve(SPIDER_SQL_SQL_ALTER_TABLE_LEN + share->db_name_str->length() +
    SPIDER_SQL_DOT_LEN + share->table_name_str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_SQL_DISABLE_KEYS_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ALTER_TABLE_STR,
    SPIDER_SQL_SQL_ALTER_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_SQL_DISABLE_KEYS_STR,
    SPIDER_SQL_SQL_DISABLE_KEYS_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_enable_keys(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_enable_keys");

  if (str->reserve(SPIDER_SQL_SQL_ALTER_TABLE_LEN + share->db_name_str->length() +
    SPIDER_SQL_DOT_LEN + share->table_name_str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_SQL_ENABLE_KEYS_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ALTER_TABLE_STR,
    SPIDER_SQL_SQL_ALTER_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_SQL_ENABLE_KEYS_STR,
    SPIDER_SQL_SQL_ENABLE_KEYS_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_check_table(
  ha_spider *spider,
  HA_CHECK_OPT* check_opt
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_check_table");

  if (str->reserve(SPIDER_SQL_SQL_CHECK_TABLE_LEN + share->db_name_str->length() +
    SPIDER_SQL_DOT_LEN + share->table_name_str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_CHECK_TABLE_STR,
    SPIDER_SQL_SQL_CHECK_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
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
  HA_CHECK_OPT* check_opt
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  SPIDER_SHARE *share = spider->share;
  int local_length = (THDVAR(spider->trx->thd, internal_optimize_local) == -1 ?
    share->internal_optimize_local :
    THDVAR(spider->trx->thd, internal_optimize_local)) *
    SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_db_append_repair_table");

  if (str->reserve(SPIDER_SQL_SQL_REPAIR_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length + share->db_name_str->length() +
    SPIDER_SQL_DOT_LEN + share->table_name_str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_REPAIR_STR, SPIDER_SQL_SQL_REPAIR_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
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
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  SPIDER_SHARE *share = spider->share;
  int local_length = (THDVAR(spider->trx->thd, internal_optimize_local) == -1 ?
    share->internal_optimize_local :
    THDVAR(spider->trx->thd, internal_optimize_local)) *
    SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_db_append_analyze_table");

  if (str->reserve(SPIDER_SQL_SQL_ANALYZE_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length + share->db_name_str->length() +
    SPIDER_SQL_DOT_LEN + share->table_name_str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ANALYZE_STR, SPIDER_SQL_SQL_ANALYZE_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_optimize_table(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  SPIDER_SHARE *share = spider->share;
  int local_length = (THDVAR(spider->trx->thd, internal_optimize_local) == -1 ?
    share->internal_optimize_local :
    THDVAR(spider->trx->thd, internal_optimize_local)) *
    SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_db_append_optimize_table");

  if (str->reserve(SPIDER_SQL_SQL_OPTIMIZE_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length + share->db_name_str->length() +
    SPIDER_SQL_DOT_LEN + share->table_name_str->length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_OPTIMIZE_STR, SPIDER_SQL_SQL_OPTIMIZE_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->db_name_str->ptr(), share->db_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->table_name_str->ptr(), share->table_name_str->length());
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_flush_tables(
  ha_spider *spider,
  bool lock
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
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
        String *str = &spider->blob_buff[field->field_index];
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

int spider_db_fetch_table(
  SPIDER_SHARE *share,
  uchar *buf,
  TABLE *table,
  SPIDER_RESULT_LIST *result_list
) {
  int error_num;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  SPIDER_RESULT *current = (SPIDER_RESULT*) result_list->current;
  SPIDER_DB_ROW row;
  ulong *lengths;
  Field **field;
  DBUG_ENTER("spider_db_fetch_table");
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
    row = current->first_position[result_list->current_row_num].row;
    lengths = current->first_position[result_list->current_row_num].lengths;
  }
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
  table->status = 0;
  DBUG_RETURN(0);
}

int spider_db_fetch_key(
  SPIDER_SHARE *share,
  uchar *buf,
  TABLE *table,
  const KEY *key_info,
  SPIDER_RESULT_LIST *result_list
) {
  int error_num;
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
    row = current->first_position[result_list->current_row_num].row;
    lengths = current->first_position[result_list->current_row_num].lengths;
  }
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
  SPIDER_SHARE *share,
  uchar *buf,
  TABLE *table,
  SPIDER_RESULT_LIST *result_list
) {
  int error_num;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
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
    row = current->first_position[result_list->current_row_num].row;
    lengths = current->first_position[result_list->current_row_num].lengths;
  }
  for (
    field = table->field;
    *field;
    field++
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
      row++;
      lengths++;
#ifndef DBUG_OFF
      dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
    }
  }
  table->status = 0;
  DBUG_RETURN(0);
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
    int roopCount;
    SPIDER_POSITION *position = result->first_position;
    if (position)
    {
      for (roopCount = 0; roopCount < result->record_num; roopCount++)
      {
        if (
          position[roopCount].row &&
          !position[roopCount].use_position
        ) {
          my_free(position[roopCount].row, MYF(0));
          position[roopCount].row = NULL;
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
  SPIDER_RESULT *result = (SPIDER_RESULT*) result_list->first;
  SPIDER_RESULT *prev;
  SPIDER_SHARE *share = spider->share;
  SPIDER_TRX *trx = spider->trx;
  SPIDER_CONN *conn = spider->conn;
  SPIDER_POSITION *position;
  int roopCount;
  DBUG_ENTER("spider_db_free_result");
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (conn && result_list->bgs_working)
    spider_bg_conn_break(conn, spider);
#endif
  if (conn && conn->quick_target == spider)
    conn->quick_target = NULL;
  if (
    final ||
    THDVAR(trx->thd, reset_sql_alloc) == 1 ||
    (THDVAR(trx->thd, reset_sql_alloc) == -1 && share->reset_sql_alloc == 1)
  ) {
    int alloc_size = final ? 0 :
      (THDVAR(trx->thd, init_sql_alloc_size) < 0 ?
      share->init_sql_alloc_size :
      THDVAR(trx->thd, init_sql_alloc_size));
    while (result)
    {
      position = result->first_position;
      if (position)
      {
        for (roopCount = 0; roopCount < result->record_num; roopCount++)
        {
          if (position[roopCount].row)
            my_free(position[roopCount].row, MYF(0));
        }
        my_free(position, MYF(0));
      }
      if (result->result)
      {
        mysql_free_result(result->result);
      }
      prev = result;
      result = (SPIDER_RESULT*) result->next;
      my_free(prev, MYF(0));
    }
    result_list->first = NULL;
    result_list->last = NULL;
    if (
      !final &&
      result_list->sql.alloced_length() > alloc_size * 2
    ) {
      result_list->sql.free();
      if(result_list->sql.real_alloc(
        THDVAR(trx->thd, init_sql_alloc_size) < 0 ?
        share->init_sql_alloc_size :
        THDVAR(trx->thd, init_sql_alloc_size)))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    if (
      !final &&
      result_list->insert_sql.alloced_length() > alloc_size * 2
    ) {
      result_list->insert_sql.free();
      if(result_list->insert_sql.real_alloc(
        THDVAR(trx->thd, init_sql_alloc_size) < 0 ?
        share->init_sql_alloc_size :
        THDVAR(trx->thd, init_sql_alloc_size)))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
  } else {
    while (result)
    {
      position = result->first_position;
      if (position)
      {
        for (roopCount = 0; roopCount < result->record_num; roopCount++)
        {
          if (position[roopCount].row)
            my_free(position[roopCount].row, MYF(0));
        }
        my_free(position, MYF(0));
      }
      result->first_position = NULL;
      if (result->result)
      {
        mysql_free_result(result->result);
        result->result = NULL;
      }
      result->record_num = 0;
      result->finish_flg = FALSE;
      result->use_position = FALSE;
      result = (SPIDER_RESULT*) result->next;
    }
  }
  result_list->current = NULL;
  result_list->record_num = 0;
  result_list->finish_flg = FALSE;
  result_list->quick_phase = 0;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  result_list->bgs_phase = 0;
#endif
  DBUG_RETURN(0);
}

int spider_db_store_result(
  ha_spider *spider,
  TABLE *table
) {
  int error_num;
  SPIDER_CONN *conn = spider->conn;
  SPIDER_DB_CONN *db_conn = conn->db_conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_RESULT *current;
  DBUG_ENTER("spider_db_store_result");
  if (!result_list->current)
  {
    if (!result_list->first)
    {
      if (!(result_list->first = (SPIDER_RESULT *)
        my_malloc(sizeof(*result_list->first), MYF(MY_WME | MY_ZEROFILL)))
      )
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
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
          my_malloc(sizeof(*result_list->last), MYF(MY_WME | MY_ZEROFILL)))
        )
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
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
          my_malloc(sizeof(*result_list->last), MYF(MY_WME | MY_ZEROFILL)))
        )
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
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
        DBUG_RETURN(HA_ERR_END_OF_FILE);
      }
    } else {
      current->record_num =
        (longlong) mysql_num_rows(current->result);
      result_list->record_num += current->record_num;
      if (
        result_list->internal_limit <= result_list->record_num ||
        result_list->split_read > current->record_num
      ) {
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
    } else {
      if (!(current->result =
        conn->db_conn->methods->use_result(db_conn)))
        DBUG_RETURN(spider_db_errorno(conn));
      conn->quick_target = spider;
    }
    SPIDER_DB_ROW row;
    if (!(row = mysql_fetch_row(current->result)))
    {
      current->finish_flg = TRUE;
      result_list->finish_flg = TRUE;
      mysql_free_result(current->result);
      current->result = NULL;
      conn->quick_target = NULL;
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
    int roopCount = 0;
    int roopCount2;
    if (!(position = (SPIDER_POSITION *)
      my_malloc(sizeof(SPIDER_POSITION) * (page_size + 1),
      MYF(MY_WME | MY_ZEROFILL)))
    )
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    current->first_position = position;
    do {
      row_size = field_count;
      lengths = current->result->lengths;
      for (roopCount2 = 0; roopCount2 < field_count; roopCount2++)
      {
        row_size += *lengths;
        lengths++;
      }
      DBUG_PRINT("info",("spider row_size=%d", row_size));
      if (!my_multi_malloc(MYF(MY_WME),
        &tmp_row, sizeof(char*) * field_count,
        &tmp_char, row_size,
        &lengths, sizeof(ulong) * field_count,
        NullS)
      )
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      memcpy(lengths, current->result->lengths, sizeof(ulong) * field_count);
      position->row = tmp_row;
      position->lengths = lengths;
      for (roopCount2 = 0; roopCount2 < field_count; roopCount2++)
      {
        DBUG_PRINT("info",("spider *lengths=%u", *lengths));
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
      roopCount++;
    } while (
      page_size > roopCount &&
      (row = mysql_fetch_row(current->result))
    );
    current->record_num = roopCount;
    result_list->record_num += roopCount;
    if (
      result_list->internal_limit <= result_list->record_num ||
      page_size > roopCount
    ) {
      current->finish_flg = TRUE;
      result_list->finish_flg = TRUE;
      mysql_free_result(current->result);
      current->result = NULL;
      conn->quick_target = NULL;
    } else if (result_list->limit_num == roopCount)
    {
      mysql_free_result(current->result);
      current->result = NULL;
      conn->quick_target = NULL;
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
  DBUG_RETURN(0);
}

int spider_db_fetch(
  uchar *buf,
  ha_spider *spider,
  TABLE *table
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_fetch");
  if (!spider->select_column_mode)
  {
    if (result_list->keyread)
      error_num = spider_db_fetch_key(spider->share, buf, table,
        result_list->key_info, result_list);
    else
      error_num = spider_db_fetch_table(spider->share, buf, table,
        result_list);
  } else
    error_num = spider_db_fetch_minimum_columns(spider->share, buf, table,
      result_list);
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
      my_message(ER_SPIDER_LOW_MEM_READ_PREV_NUM, ER_SPIDER_LOW_MEM_READ_PREV_STR, MYF(0));
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
  TABLE *table
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_seek_next");
  if (result_list->current_row_num >= result_list->current->record_num)
  {
    if (result_list->low_mem_read)
      spider_db_free_one_result(result_list,
        (SPIDER_RESULT*) result_list->current);
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (result_list->bgs_phase > 0)
    {
      if ((error_num = spider_bg_conn_search(spider, FALSE)))
        DBUG_RETURN(error_num);
    } else {
#endif
      if (result_list->current == result_list->bgs_current)
      {
        if (result_list->finish_flg)
        {
          table->status = STATUS_NOT_FOUND;
          DBUG_RETURN(HA_ERR_END_OF_FILE);
        }
        if (
          result_list->quick_mode == 0 ||
          !result_list->current->result
        ) {
          result_list->sql.length(result_list->limit_pos);
          result_list->limit_num =
            result_list->internal_limit - result_list->record_num >=
            result_list->split_read ?
            result_list->split_read :
            result_list->internal_limit - result_list->record_num;
          if ((error_num = spider_db_append_limit(
            &result_list->sql, result_list->record_num,
            result_list->limit_num)) ||
            (error_num = spider_db_append_select_lock(spider))
          )
            DBUG_RETURN(error_num);
          if ((error_num = spider_db_set_names(spider->share, spider->conn)))
            DBUG_RETURN(error_num);
          if (spider_db_query(
            spider->conn,
            result_list->sql.ptr(),
            result_list->sql.length())
          )
            DBUG_RETURN(spider_db_errorno(spider->conn));
        }
        if ((error_num = spider_db_store_result(spider, table)))
          DBUG_RETURN(error_num);
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
  TABLE *table
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_seek_last");
  if (result_list->finish_flg)
  {
    if (result_list->low_mem_read == 1)
    {
      my_message(ER_SPIDER_LOW_MEM_READ_PREV_NUM, ER_SPIDER_LOW_MEM_READ_PREV_STR, MYF(0));
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
      my_message(ER_SPIDER_LOW_MEM_READ_PREV_NUM, ER_SPIDER_LOW_MEM_READ_PREV_STR, MYF(0));
      DBUG_RETURN(ER_SPIDER_LOW_MEM_READ_PREV_NUM);
    }
    result_list->sql.length(result_list->limit_pos);
    result_list->limit_num =
      result_list->internal_limit - result_list->record_num;
    if ((error_num = spider_db_append_limit(
      &result_list->sql,
      result_list->internal_offset + result_list->record_num,
      result_list->limit_num)) ||
      (error_num = spider_db_append_select_lock(spider))
    )
      DBUG_RETURN(error_num);
    if ((error_num = spider_db_set_names(spider->share, spider->conn)))
      DBUG_RETURN(error_num);
    if (spider_db_query(
      spider->conn,
      result_list->sql.ptr(),
      result_list->sql.length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
    if ((error_num = spider_db_store_result(spider, table)))
      DBUG_RETURN(error_num);
    result_list->current_row_num = result_list->current->record_num - 1;
    if (result_list->quick_mode == 0)
      result_list->current->result->data_cursor =
        result_list->current->first_row + result_list->current_row_num;
    DBUG_RETURN(spider_db_fetch(buf, spider, table));
  } else {
    if ((error_num = spider_db_free_result(spider, FALSE)))
      DBUG_RETURN(error_num);
    result_list->sql.length(result_list->order_pos);
    result_list->desc_flg = ~(result_list->desc_flg);
    result_list->limit_num =
      result_list->internal_limit >= result_list->split_read ?
      result_list->split_read : result_list->internal_limit;
    if (
      (error_num = spider_db_append_key_order(spider)) ||
      (error_num = spider_db_append_limit(
        &result_list->sql, result_list->internal_offset,
        result_list->limit_num)) ||
      (error_num = spider_db_append_select_lock(spider))
    )
      DBUG_RETURN(error_num);
    if ((error_num = spider_db_set_names(spider->share, spider->conn)))
      DBUG_RETURN(error_num);
    if (spider_db_query(
      spider->conn,
      result_list->sql.ptr(),
      result_list->sql.length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
    if ((error_num = spider_db_store_result(spider, table)))
      DBUG_RETURN(error_num);
    DBUG_RETURN(spider_db_fetch(buf, spider, table));
  }
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

SPIDER_POSITION *spider_db_create_position(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_RESULT *current = (SPIDER_RESULT*) result_list->current;
  SPIDER_POSITION *pos;
  DBUG_ENTER("spider_db_create_position");
  if (result_list->quick_mode == 0)
  {
    ulong *lengths;
    SPIDER_DB_RESULT *result = current->result;
    if (
      !(pos = (SPIDER_POSITION*) sql_alloc(sizeof(SPIDER_POSITION))) ||
      !(lengths = (ulong*) sql_alloc(sizeof(ulong) * result->field_count))
    )
      DBUG_RETURN(NULL);
    pos->row = result->current_row;
    memcpy(lengths, result->lengths, sizeof(ulong) * result->field_count);
    pos->lengths = lengths;
  } else {
    pos = &current->first_position[result_list->current_row_num - 1];
  }
  current->use_position = TRUE;
  pos->use_position = TRUE;
  DBUG_RETURN(pos);
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
  if (!spider->select_column_mode)
  {
    if (result_list->keyread)
      error_num = spider_db_seek_tmp_key(buf, pos, spider, table,
        result_list->key_info);
    else
      error_num = spider_db_seek_tmp_table(buf, pos, spider, table);
  } else
    error_num = spider_db_seek_tmp_minimum_columns(buf, pos, spider, table);

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
  ulong *lengths;
  Field **field;
  SPIDER_DB_ROW row = pos->row;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  DBUG_ENTER("spider_db_seek_tmp_table");
  for (
    field = table->field,
    lengths = pos->lengths;
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
  ulong *lengths;
  Field *field;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  DBUG_ENTER("spider_db_seek_tmp_key");
  for (
    key_part = key_info->key_part,
    part_num = 0,
    lengths = pos->lengths;
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
  ulong *lengths;
  Field **field;
  SPIDER_DB_ROW row = pos->row;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  DBUG_ENTER("spider_db_seek_tmp_minimum_columns");
  for (
    field = table->field,
    lengths = pos->lengths;
    *field;
    field++
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
      row++;
      lengths++;
#ifndef DBUG_OFF
      dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
    }
  }
  DBUG_RETURN(0);
}

int spider_db_show_table_status(
  ha_spider *spider,
  int sts_mode
) {
  int error_num;
  SPIDER_CONN *conn = spider->conn;
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row;
  SPIDER_SHARE *share = spider->share;
  MYSQL_TIME mysql_time;
  my_bool not_used_my_bool;
  int not_used_int;
  long not_used_long;
  DBUG_ENTER("spider_db_show_table_status");
  DBUG_PRINT("info",("spider sts_mode=%d", sts_mode));
  if (sts_mode == 1)
  {
    if (
      (error_num = spider_db_set_names(share, conn)) ||
      (
        spider_db_query(
          conn,
          share->show_table_status[0].ptr(),
          share->show_table_status[0].length()) &&
        (error_num = spider_db_errorno(conn))
      )
    ) {
      if (
        error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
        !conn->disable_reconnect
      ) {
        /* retry */
        if ((error_num = spider_db_ping(spider)))
          DBUG_RETURN(error_num);
        if ((error_num = spider_db_set_names(share, conn)))
          DBUG_RETURN(error_num);
        if (spider_db_query(
          conn,
          share->show_table_status[0].ptr(),
          share->show_table_status[0].length())
        )
          DBUG_RETURN(spider_db_errorno(conn));
      } else
        DBUG_RETURN(error_num);
    }
    if (
      !(res = mysql_store_result(conn->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if (res)
        mysql_free_result(res);
      if ((error_num = spider_db_errorno(conn)))
        DBUG_RETURN(error_num);
      else {
        my_printf_error(ER_SPIDER_REMOTE_TABLE_NOT_FOUND_NUM,
          ER_SPIDER_REMOTE_TABLE_NOT_FOUND_STR, MYF(0),
          spider->share->db_name_str->ptr(),
          spider->share->table_name_str->ptr());
        DBUG_RETURN(ER_SPIDER_REMOTE_TABLE_NOT_FOUND_NUM);
      }
    }
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
      ("spider mean_rec_length=%lld", share->mean_rec_length));
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
      share->auto_increment_value = 0;
    DBUG_PRINT("info",
      ("spider auto_increment_value=%lld", share->auto_increment_value));
    if (row[11])
    {
      VOID(str_to_datetime(row[11], strlen(row[11]), &mysql_time, 0,
        &not_used_int));
      share->create_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
    } else
      share->create_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider create_time=%lld", share->create_time));
    if (row[12])
    {
      VOID(str_to_datetime(row[12], strlen(row[12]), &mysql_time, 0,
        &not_used_int));
      share->update_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
    } else
      share->update_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider update_time=%lld", share->update_time));
    if (row[13])
    {
      VOID(str_to_datetime(row[13], strlen(row[13]), &mysql_time, 0,
        &not_used_int));
      share->check_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
    } else
      share->check_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider check_time=%lld", share->check_time));
    mysql_free_result(res);
  } else {
    if (
      (error_num = spider_db_set_names(share, conn)) ||
      (
        spider_db_query(
          conn,
          share->show_table_status[1].ptr(),
          share->show_table_status[1].length()) &&
        (error_num = spider_db_errorno(conn))
      )
    ) {
      if (
        error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
        !conn->disable_reconnect
      ) {
        /* retry */
        if ((error_num = spider_db_ping(spider)))
          DBUG_RETURN(error_num);
        if ((error_num = spider_db_set_names(share, conn)))
          DBUG_RETURN(error_num);
        if (spider_db_query(
          conn,
          share->show_table_status[1].ptr(),
          share->show_table_status[1].length())
        )
          DBUG_RETURN(spider_db_errorno(conn));
      } else
        DBUG_RETURN(error_num);
    }
    if (
      !(res = mysql_store_result(conn->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if (res)
        mysql_free_result(res);
      if ((error_num = spider_db_errorno(conn)))
        DBUG_RETURN(error_num);
      else
        DBUG_RETURN(ER_QUERY_ON_FOREIGN_DATA_SOURCE);
    }

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
      ("spider mean_rec_length=%lld", share->mean_rec_length));
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
      share->auto_increment_value = 0;
    DBUG_PRINT("info",
      ("spider auto_increment_value=%lld", share->auto_increment_value));
    if (row[6])
    {
      VOID(str_to_datetime(row[6], strlen(row[6]), &mysql_time, 0,
        &not_used_int));
      share->create_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
    } else
      share->create_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider create_time=%lld", share->create_time));
    if (row[7])
    {
      VOID(str_to_datetime(row[7], strlen(row[7]), &mysql_time, 0,
        &not_used_int));
      share->update_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
    } else
      share->update_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider update_time=%lld", share->update_time));
    if (row[8])
    {
      VOID(str_to_datetime(row[8], strlen(row[8]), &mysql_time, 0,
        &not_used_int));
      share->check_time = (time_t) my_system_gmt_sec(&mysql_time,
        &not_used_long, &not_used_my_bool);
    } else
      share->check_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider check_time=%lld", share->check_time));
    mysql_free_result(res);
  }
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
  for (roop_count = 0; roop_count < table->s->keys; roop_count++)
  {
    key_info = &table->key_info[roop_count];
    for (roop_count2 = 0; roop_count2 < key_info->key_parts; roop_count2++)
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
        key_info->rec_per_key[roop_count2] = rec_per_key;
      DBUG_PRINT("info",
        ("spider column id=%d", field->field_index));
      DBUG_PRINT("info",
        ("spider cardinality=%lld",
        share->cardinality[field->field_index]));
      DBUG_PRINT("info",
        ("spider rec_per_key=%lld",
        key_info->rec_per_key[roop_count2]));
    }
  }
  DBUG_VOID_RETURN;
}

int spider_db_show_index(
  ha_spider *spider,
  TABLE *table,
  int crd_mode
) {
  int error_num;
  SPIDER_CONN *conn = spider->conn;
  SPIDER_SHARE *share = spider->share;
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row;
  Field *field;
  int roop_count;
  longlong *tmp_cardinality;
  DBUG_ENTER("spider_db_show_index");
  DBUG_PRINT("info",("spider crd_mode=%d", crd_mode));
  if (crd_mode == 1)
  {
    if (
      (error_num = spider_db_set_names(share, conn)) ||
      (
        spider_db_query(
          conn,
          share->show_index[0].ptr(),
          share->show_index[0].length()) &&
        (error_num = spider_db_errorno(conn))
      )
    ) {
      if (
        error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
        !conn->disable_reconnect
      ) {
        /* retry */
        if ((error_num = spider_db_ping(spider)))
          DBUG_RETURN(error_num);
        if ((error_num = spider_db_set_names(share, conn)))
          DBUG_RETURN(error_num);
        if (spider_db_query(
          conn,
          share->show_index[0].ptr(),
          share->show_index[0].length())
        )
          DBUG_RETURN(spider_db_errorno(conn));
      } else
        DBUG_RETURN(error_num);
    }
    if (
      !(res = mysql_store_result(conn->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if ((error_num = spider_db_errorno(conn)))
      {
        if (res)
          mysql_free_result(res);
        DBUG_RETURN(error_num);
      }
      /* no record is ok */
    }
    if (mysql_num_fields(res) != 12)
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
      roop_count < table->s->fields;
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
    if (
      (error_num = spider_db_set_names(share, conn)) ||
      (
        spider_db_query(
          conn,
          share->show_index[1].ptr(),
          share->show_index[1].length()) &&
        (error_num = spider_db_errorno(conn))
      )
    ) {
      if (
        error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
        !conn->disable_reconnect
      ) {
        /* retry */
        if ((error_num = spider_db_ping(spider)))
          DBUG_RETURN(error_num);
        if ((error_num = spider_db_set_names(share, conn)))
          DBUG_RETURN(error_num);
        if (spider_db_query(
          conn,
          share->show_index[1].ptr(),
          share->show_index[1].length())
        )
          DBUG_RETURN(spider_db_errorno(conn));
      } else
        DBUG_RETURN(error_num);
    }
    if (
      !(res = mysql_store_result(conn->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if ((error_num = spider_db_errorno(conn)))
      {
        if (res)
          mysql_free_result(res);
        DBUG_RETURN(error_num);
      }
      /* no record is ok */
    }

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
      roop_count < table->s->fields;
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
  ha_spider *spider
) {
  int error_num;
  SPIDER_CONN *conn = spider->conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
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
    (error_num = spider_db_append_from(str, spider->share)) ||
    (error_num = spider_db_append_key_where(start_key, end_key, spider))
  ) {
    my_errno = error_num;
    DBUG_RETURN(HA_POS_ERROR);
  }

  if (
    (error_num = spider_db_set_names(spider->share, spider->conn)) ||
    (
      spider_db_query(
        conn,
        str->ptr(),
        str->length()) &&
      (my_errno = spider_db_errorno(conn))
    )
  ) {
    if (
      error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
      !conn->disable_reconnect
    ) {
      /* retry */
      if ((my_errno = spider_db_ping(spider)))
        DBUG_RETURN(my_errno);
      if ((error_num = spider_db_set_names(spider->share, conn)))
        DBUG_RETURN(error_num);
      if (spider_db_query(
        conn,
        str->ptr(),
        str->length())
      ) {
        my_errno = spider_db_errorno(conn);
        DBUG_RETURN(HA_POS_ERROR);
      }
    } else
      DBUG_RETURN(HA_POS_ERROR);
  }
  if (
    !(res = mysql_store_result(conn->db_conn)) ||
    !(row = mysql_fetch_row(res))
  ) {
    if (res)
      mysql_free_result(res);
    if ((error_num = spider_db_errorno(conn)))
    {
      my_errno = error_num;
      DBUG_RETURN(HA_POS_ERROR);
    } else {
      my_errno = ER_QUERY_ON_FOREIGN_DATA_SOURCE;
      DBUG_RETURN(HA_POS_ERROR);
    }
  }
  if (mysql_num_fields(res) != 10)
  {
    mysql_free_result(res);
    my_errno = ER_QUERY_ON_FOREIGN_DATA_SOURCE;
    DBUG_RETURN(HA_POS_ERROR);
  }

  if (row[8])
  {
    rows = (ha_rows) my_strtoll10(row[8], (char**) NULL, &error_num);
  }
  mysql_free_result(res);
  DBUG_RETURN(rows);
}

int spider_db_bulk_insert_init(
  ha_spider *spider,
  const TABLE *table
) {
  int error_num;
  String *str = &spider->result_list.insert_sql;
  DBUG_ENTER("spider_db_bulk_insert_init");
  spider->conn->ignore_dup_key = spider->ignore_dup_key;
  str->length(0);
  spider->result_list.insert_pos = 0;
  if (
    (error_num = spider_db_append_insert(spider)) ||
    (error_num = spider_db_append_into(spider, table))
  )
    DBUG_RETURN(error_num);
  DBUG_RETURN(0);
}

int spider_db_bulk_insert(
  ha_spider *spider,
  TABLE *table,
  bool bulk_end
) {
  int error_num;
  bool add_value = FALSE;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  String *str = &result_list->insert_sql;
  Field **field;
  DBUG_ENTER("spider_db_bulk_insert");
  DBUG_PRINT("info",("spider str->length()=%d", str->length()));
  DBUG_PRINT("info",("spider result_list->insert_pos=%d",
    result_list->insert_pos));
  if (!bulk_end)
  {
    if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
    {
      str->length(0);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
#ifndef DBUG_OFF
    my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table, table->read_set);
#endif
    for (field = table->field; *field; field++)
    {
      if (bitmap_is_set(table->write_set, (*field)->field_index))
      {
        add_value = TRUE;
        if ((*field)->is_null())
        {
          if (str->reserve(SPIDER_SQL_NULL_LEN + SPIDER_SQL_COMMA_LEN))
          {
#ifndef DBUG_OFF
            dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
            str->length(0);
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          }
          str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
        } else {
          if (
            spider_db_append_column_value(share, str, *field, NULL) ||
            str->reserve(SPIDER_SQL_COMMA_LEN)
          ) {
#ifndef DBUG_OFF
            dbug_tmp_restore_column_map(table->read_set, tmp_map);
#endif
            str->length(0);
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          }
        }
        str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
      }
    }
#ifndef DBUG_OFF
    dbug_tmp_restore_column_map(table->read_set, tmp_map);
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
  }
  DBUG_PRINT("info",("spider str->length()=%d", str->length()));
  DBUG_PRINT("info",("spider result_list->insert_pos=%d",
    result_list->insert_pos));
  if (
    (bulk_end || str->length() >= spider->bulk_size) &&
    str->length() > result_list->insert_pos
  ) {
    SPIDER_CONN *conn = spider->conn;
    if ((error_num = spider_db_set_names(spider->share, conn)))
      DBUG_RETURN(error_num);
    if (spider_db_query(
      conn,
      str->ptr(),
      str->length() - SPIDER_SQL_COMMA_LEN)
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
      DBUG_RETURN(error_num);
    }
    str->length(result_list->insert_pos);
    if ((error_num = spider_db_update_auto_increment(spider)))
      DBUG_RETURN(error_num);
    if (table->next_number_field && table->next_number_field->is_null())
    {
      table->next_number_field->set_notnull();
      if((error_num = table->next_number_field->store(
        conn->db_conn->last_used_con->insert_id, TRUE)))
        DBUG_RETURN(error_num);
    }
  }
  DBUG_RETURN(0);
}

int spider_db_update_auto_increment(
  ha_spider *spider
) {
  ulonglong last_insert_id;
  SPIDER_DB_CONN *last_used_con = spider->conn->db_conn->last_used_con;
  DBUG_ENTER("spider_db_update_auto_increment");
  last_insert_id = last_used_con->insert_id;
  spider->share->auto_increment_value =
    last_insert_id + last_used_con->affected_rows;
  spider->trx->thd->record_first_successful_insert_id_in_cur_stmt(
    last_insert_id);
  DBUG_RETURN(0);
}

int spider_db_update(
  ha_spider *spider,
  TABLE *table,
  const uchar *old_data
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  Field **field;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(old_data, table->record[0]);
  DBUG_ENTER("spider_db_update");
  str->length(0);
  if (
    (error_num = spider_db_append_update(spider)) ||
    (error_num = spider_db_append_update_set(spider, table)) ||
    (error_num = spider_db_append_update_where(
      spider, table, ptr_diff))
  )
    DBUG_RETURN(error_num);

  if ((error_num = spider_db_set_names(spider->share, spider->conn)))
    DBUG_RETURN(error_num);
  if (spider_db_query(
    spider->conn,
    str->ptr(),
    str->length())
  ) {
    DBUG_RETURN(spider_db_errorno(spider->conn));
  }
  DBUG_RETURN(0);
}

int spider_db_delete(
  ha_spider *spider,
  const TABLE *table,
  const uchar *buf
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  DBUG_ENTER("spider_db_delete");
  str->length(0);
  if (
    (error_num = spider_db_append_delete(spider)) ||
    (error_num = spider_db_append_from(str, spider->share)) ||
    (error_num = spider_db_append_update_where(
      spider, table, ptr_diff))
  )
    DBUG_RETURN(error_num);

  if ((error_num = spider_db_set_names(spider->share, spider->conn)))
    DBUG_RETURN(error_num);
  if (spider_db_query(
    spider->conn,
    str->ptr(),
    str->length())
  )
    DBUG_RETURN(spider_db_errorno(spider->conn));
  DBUG_RETURN(0);
}

int spider_db_delete_all_rows(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_delete_all_rows");
  str->length(0);
  if (spider->sql_command == SQLCOM_TRUNCATE)
  {
    if ((error_num = spider_db_append_truncate(spider)))
      DBUG_RETURN(error_num);
  } else {
    if (
      (error_num = spider_db_append_delete(spider)) ||
      (error_num = spider_db_append_from(str, spider->share))
    )
      DBUG_RETURN(error_num);
  }

  if (
    (error_num = spider_db_set_names(spider->share, spider->conn)) ||
    (
      spider_db_query(
        spider->conn,
        str->ptr(),
        str->length()) &&
      (error_num = spider_db_errorno(spider->conn))
    )
  ) {
    if (
      error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
      !spider->conn->disable_reconnect
    ) {
      /* retry */
      if ((error_num = spider_db_ping(spider)))
        DBUG_RETURN(error_num);
      if ((error_num = spider_db_set_names(spider->share, spider->conn)))
        DBUG_RETURN(error_num);
      if (spider_db_query(
        spider->conn,
        str->ptr(),
        str->length())
      )
        DBUG_RETURN(spider_db_errorno(spider->conn));
    } else
      DBUG_RETURN(error_num);
  }
  DBUG_RETURN(0);
}

int spider_db_disable_keys(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_disable_keys");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    str->length(0);
    if ((error_num = spider_db_append_disable_keys(spider)))
      DBUG_RETURN(error_num);

    if ((error_num = spider_db_set_names(spider->share, spider->conn)))
      DBUG_RETURN(error_num);
    if (spider_db_query(
      spider->conn,
      str->ptr(),
      str->length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
  }
  DBUG_RETURN(0);
}

int spider_db_enable_keys(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_enable_keys");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    str->length(0);
    if ((error_num = spider_db_append_enable_keys(spider)))
      DBUG_RETURN(error_num);

    if ((error_num = spider_db_set_names(spider->share, spider->conn)))
      DBUG_RETURN(error_num);
    if (spider_db_query(
      spider->conn,
      str->ptr(),
      str->length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
  }
  DBUG_RETURN(0);
}

int spider_db_check_table(
  ha_spider *spider,
  HA_CHECK_OPT* check_opt
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_check_table");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    str->length(0);
    if ((error_num = spider_db_append_check_table(spider, check_opt)))
      DBUG_RETURN(error_num);

    if ((error_num = spider_db_set_names(spider->share, spider->conn)))
      DBUG_RETURN(error_num);
    if (spider_db_query(
      spider->conn,
      str->ptr(),
      str->length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
  }
  DBUG_RETURN(0);
}

int spider_db_repair_table(
  ha_spider *spider,
  HA_CHECK_OPT* check_opt
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_repair_table");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    str->length(0);
    if ((error_num = spider_db_append_repair_table(spider, check_opt)))
      DBUG_RETURN(error_num);

    if ((error_num = spider_db_set_names(spider->share, spider->conn)))
      DBUG_RETURN(error_num);
    if (spider_db_query(
      spider->conn,
      str->ptr(),
      str->length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
  }
  DBUG_RETURN(0);
}

int spider_db_analyze_table(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_analyze_table");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    str->length(0);
    if ((error_num = spider_db_append_analyze_table(spider)))
      DBUG_RETURN(error_num);

    if ((error_num = spider_db_set_names(spider->share, spider->conn)))
      DBUG_RETURN(error_num);
    if (spider_db_query(
      spider->conn,
      str->ptr(),
      str->length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
  }
  DBUG_RETURN(0);
}

int spider_db_optimize_table(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_optimize_table");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    str->length(0);
    if ((error_num = spider_db_append_optimize_table(spider)))
      DBUG_RETURN(error_num);

    if ((error_num = spider_db_set_names(spider->share, spider->conn)))
      DBUG_RETURN(error_num);
    if (spider_db_query(
      spider->conn,
      str->ptr(),
      str->length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
  }
  DBUG_RETURN(0);
}

int spider_db_flush_tables(
  ha_spider *spider,
  bool lock
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_flush_tables");
  str->length(0);
  if ((error_num = spider_db_append_flush_tables(spider, lock)))
    DBUG_RETURN(error_num);

  if (spider_db_query(
    spider->conn,
    str->ptr(),
    str->length())
  )
    DBUG_RETURN(spider_db_errorno(spider->conn));
  DBUG_RETURN(0);
}

int spider_db_flush_logs(
  ha_spider *spider
) {
  DBUG_ENTER("spider_db_flush_logs");
  if (spider_db_query(
    spider->conn,
    SPIDER_SQL_FLUSH_LOGS_STR,
    SPIDER_SQL_FLUSH_LOGS_LEN)
  )
    DBUG_RETURN(spider_db_errorno(spider->conn));
  DBUG_RETURN(0);
}

int spider_db_print_item_type(
  Item *item,
  ha_spider *spider,
  String *str
) {
  DBUG_ENTER("spider_db_print_item_type");
  DBUG_PRINT("info",("spider COND type=%d", item->type()));
  switch (item->type())
  {
    case Item::FUNC_ITEM:
      DBUG_RETURN(spider_db_open_item_func((Item_func *) item, spider, str));
    case Item::COND_ITEM:
      DBUG_RETURN(spider_db_open_item_cond((Item_cond *) item, spider, str));
    case Item::FIELD_ITEM:
      DBUG_RETURN(spider_db_open_item_field((Item_field *) item, spider, str));
    case Item::REF_ITEM:
      DBUG_RETURN(spider_db_open_item_ref((Item_ref *) item, spider, str));
    case Item::ROW_ITEM:
      DBUG_RETURN(spider_db_open_item_row((Item_row *) item, spider, str));
    case Item::SUBSELECT_ITEM:
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    default:
      if (spider->share->access_charset->cset == system_charset_info->cset)
        item->print(str, QT_IS);
      else
        item->print(str, QT_ORDINARY);
      break;
  }
  DBUG_RETURN(0);
}

int spider_db_open_item_cond(
  Item_cond *item_cond,
  ha_spider *spider,
  String *str
) {
  int error_num;
  List_iterator_fast<Item> lif(*(item_cond->argument_list()));
  Item *item;
  char *func_name = NULL;
  int func_name_length, restart_pos;
  DBUG_ENTER("spider_db_open_item_cond");
  if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);

restart_first:
  if ((item = lif++))
  {
    restart_pos = str->length();
    if ((error_num = spider_db_print_item_type(item, spider, str)))
    {
      if (error_num == ER_SPIDER_COND_SKIP_NUM)
      {
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

    if ((error_num = spider_db_print_item_type(item, spider, str)))
    {
      if (error_num == ER_SPIDER_COND_SKIP_NUM)
      {
        DBUG_PRINT("info",("spider COND skip"));
        str->length(restart_pos);
      } else
        DBUG_RETURN(error_num);
    }
  }
  if (str->reserve(SPIDER_SQL_CLOSE_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_db_open_item_func(
  Item_func *item_func,
  ha_spider *spider,
  String *str
) {
  int error_num;
  Item *item, **item_list = item_func->arguments();
  uint roop_count, item_count = item_func->argument_count() - 1;
  char *func_name = SPIDER_SQL_NULL_CHAR_STR,
    *separete_str = SPIDER_SQL_NULL_CHAR_STR,
    *last_str = SPIDER_SQL_NULL_CHAR_STR;
  int func_name_length = SPIDER_SQL_NULL_CHAR_LEN,
    separete_str_length = SPIDER_SQL_NULL_CHAR_LEN,
    last_str_length = SPIDER_SQL_NULL_CHAR_LEN;
  DBUG_ENTER("spider_db_open_item_func");
  if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
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
      } else if (func_name_length == 2 &&
        (
          !strncasecmp("<<", func_name, func_name_length) ||
          !strncasecmp(">>", func_name, func_name_length)
        )
      ) {
        /* no action */
      } else if (func_name_length == 3 &&
        !strncasecmp("div", func_name, func_name_length)
      ) {
        /* no action */
      } else if (func_name_length == 6 &&
        !strncasecmp("istrue", func_name, func_name_length)
      ) {
        last_str = SPIDER_SQL_IS_TRUE_STR;
        last_str_length = SPIDER_SQL_IS_TRUE_LEN;
      } else if (func_name_length == 7 &&
        !strncasecmp("isfalse", func_name, func_name_length)
      ) {
        last_str = SPIDER_SQL_IS_FALSE_STR;
        last_str_length = SPIDER_SQL_IS_FALSE_LEN;
      } else if (func_name_length == 9 &&
        !strncasecmp("isnottrue", func_name, func_name_length)
      ) {
        last_str = SPIDER_SQL_IS_NOT_TRUE_STR;
        last_str_length = SPIDER_SQL_IS_NOT_TRUE_LEN;
      } else if (func_name_length == 10 &&
        !strncasecmp("isnotfalse", func_name, func_name_length)
      ) {
        last_str = SPIDER_SQL_IS_NOT_FALSE_STR;
        last_str_length = SPIDER_SQL_IS_NOT_FALSE_LEN;
      } else {
        if (str->reserve(func_name_length + SPIDER_SQL_OPEN_PAREN_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(func_name, func_name_length);
        str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
        func_name = SPIDER_SQL_COMMA_STR;
        func_name_length = SPIDER_SQL_COMMA_LEN;
        separete_str = SPIDER_SQL_COMMA_STR;
        separete_str_length = SPIDER_SQL_COMMA_LEN;
        last_str = SPIDER_SQL_CLOSE_PAREN_STR;
        last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      }
      break;
    case Item_func::NOT_FUNC:
    case Item_func::NEG_FUNC:
      func_name = (char*) item_func->func_name();
      func_name_length = strlen(func_name);
      if (str->reserve(func_name_length + SPIDER_SQL_SPACE_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(func_name, func_name_length);
      str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
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
      func_name = (char*) item_func->func_name();
      func_name_length = strlen(func_name);
      DBUG_PRINT("info",("spider func_name = %s", func_name));
      DBUG_PRINT("info",("spider func_name_length = %d", func_name_length));
      if (str->reserve(func_name_length + SPIDER_SQL_OPEN_PAREN_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(func_name, func_name_length);
      str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
      func_name = SPIDER_SQL_COMMA_STR;
      func_name_length = SPIDER_SQL_COMMA_LEN;
      separete_str = SPIDER_SQL_COMMA_STR;
      separete_str_length = SPIDER_SQL_COMMA_LEN;
      last_str = SPIDER_SQL_CLOSE_PAREN_STR;
      last_str_length = SPIDER_SQL_CLOSE_PAREN_LEN;
      break;
    case Item_func::COND_XOR_FUNC:
      str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
      DBUG_RETURN(
        spider_db_open_item_cond((Item_cond *) item_func, spider, str));
/*  memo
    case Item_func::EQ_FUNC:
    case Item_func::EQUAL_FUNC:
    case Item_func::NE_FUNC:
    case Item_func::LT_FUNC:
    case Item_func::LE_FUNC:
    case Item_func::GE_FUNC:
    case Item_func::GT_FUNC:
*/
    default:
      func_name = (char*) item_func->func_name();
      func_name_length = strlen(func_name);
      break;
  }
  DBUG_PRINT("info",("spider func_name = %s", func_name));
  DBUG_PRINT("info",("spider func_name_length = %d", func_name_length));
  DBUG_PRINT("info",("spider separete_str = %s", separete_str));
  DBUG_PRINT("info",("spider separete_str_length = %d", separete_str_length));
  DBUG_PRINT("info",("spider last_str = %s", last_str));
  DBUG_PRINT("info",("spider last_str_length = %d", last_str_length));
  for (roop_count = 0; roop_count < item_count; roop_count++)
  {
    item = item_list[roop_count];
    if ((error_num = spider_db_print_item_type(item, spider, str)))
      DBUG_RETURN(error_num);
    if (roop_count == 1)
    {
      func_name = separete_str;
      func_name_length = separete_str_length;
    }
    if (str->reserve(func_name_length + SPIDER_SQL_SPACE_LEN * 2))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
    str->q_append(func_name, func_name_length);
    str->q_append(SPIDER_SQL_SPACE_STR, SPIDER_SQL_SPACE_LEN);
  }
  item = item_list[roop_count];
  if ((error_num = spider_db_print_item_type(item, spider, str)))
    DBUG_RETURN(error_num);
  if (str->reserve(last_str_length + SPIDER_SQL_CLOSE_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(last_str, last_str_length);
  str->q_append(SPIDER_SQL_CLOSE_PAREN_STR, SPIDER_SQL_CLOSE_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_db_open_item_ident(
  Item_ident *item_ident,
  ha_spider *spider,
  String *str
) {
  int field_name_length;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_open_item_ident");
  if (item_ident->cached_field_index != NO_CACHED_FIELD_INDEX)
  {
    DBUG_PRINT("info",("spider use cached_field_index"));
    if (str->reserve(
      share->column_name_str[item_ident->cached_field_index].length() +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 2))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    spider_db_append_column_name(share, str,
      item_ident->cached_field_index);
    DBUG_RETURN(0);
  }
  if (item_ident->field_name)
    field_name_length = strlen(item_ident->field_name);
  else
    field_name_length = 0;
  if (spider->share->access_charset->cset == system_charset_info->cset)
  {
    if (str->reserve((SPIDER_SQL_NAME_QUOTE_LEN) * 2 + field_name_length))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    str->q_append(item_ident->field_name, field_name_length);
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  } else {
    if (str->reserve(SPIDER_SQL_NAME_QUOTE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    str->append(item_ident->field_name, field_name_length,
      system_charset_info);
    if (str->reserve(SPIDER_SQL_NAME_QUOTE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_open_item_field(
  Item_field *item_field,
  ha_spider *spider,
  String *str
) {
  int error_num;
  Field *field = item_field->field;
  SPIDER_SHARE *share;
  DBUG_ENTER("spider_db_open_item_field");
  if (field)
  {
    if (field->table != spider->get_table())
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    if (field->table->const_table)
    {
      share = spider->share;
      if (str->reserve(
        share->column_name_str[field->field_index].length() +
        (SPIDER_SQL_NAME_QUOTE_LEN) * 2))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      spider_db_append_column_name(share, str,
        field->field_index);
      DBUG_RETURN(0);
    }
  }
  DBUG_RETURN(spider_db_open_item_ident(
    (Item_ident *) item_field, spider, str));
}

int spider_db_open_item_ref(
  Item_ref *item_ref,
  ha_spider *spider,
  String *str
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
      int name_length = strlen(item_ref->name);
      if (str->reserve(SPIDER_SQL_NAME_QUOTE_LEN * 2 + name_length))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      str->q_append(item_ref->name, name_length);
      str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
      DBUG_RETURN(0);
    }
    DBUG_RETURN(spider_db_print_item_type(*(item_ref->ref), spider, str));
  }
  DBUG_RETURN(spider_db_open_item_ident((Item_ident *) item_ref, spider, str));
}

int spider_db_open_item_row(
  Item_row *item_row,
  ha_spider *spider,
  String *str
) {
  int error_num;
  uint roop_count, cols = item_row->cols() - 1;
  Item *item;
  DBUG_ENTER("spider_db_open_item_row");
  for (roop_count = 0; roop_count < cols; roop_count++)
  {
    item = item_row->element_index(roop_count);
    if ((error_num = spider_db_print_item_type(item, spider, str)))
      DBUG_RETURN(error_num);
    if (str->reserve(SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  item = item_row->element_index(roop_count);
  if ((error_num = spider_db_print_item_type(item, spider, str)))
    DBUG_RETURN(error_num);
  DBUG_RETURN(0);
}

int spider_db_append_condition(
  ha_spider *spider,
  String *str
) {
  int error_num, restart_pos;
  SPIDER_CONDITION *tmp_cond = spider->condition;
  bool where_pos = (str->length() == spider->result_list.where_pos);
  DBUG_ENTER("spider_db_append_condition");
  while (tmp_cond)
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
    if ((error_num = spider_db_print_item_type(
      (Item *) tmp_cond->cond, spider, str)))
    {
      if (error_num == ER_SPIDER_COND_SKIP_NUM)
      {
        DBUG_PRINT("info",("spider COND skip"));
        str->length(restart_pos);
        where_pos = (str->length() == spider->result_list.where_pos);
      } else
        DBUG_RETURN(error_num);
    }
    tmp_cond = tmp_cond->next;
  }
  DBUG_RETURN(0);
}
