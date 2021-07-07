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

#define SPIDER_SQL_NAME_QUOTE_STR "`"
#define SPIDER_SQL_NAME_QUOTE_LEN (sizeof(SPIDER_SQL_NAME_QUOTE_STR) - 1)
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
#define SPIDER_SQL_LIKE_STR " like "
#define SPIDER_SQL_LIKE_LEN (sizeof(SPIDER_SQL_LIKE_STR) - 1)
#define SPIDER_SQL_AND_STR " and "
#define SPIDER_SQL_AND_LEN (sizeof(SPIDER_SQL_AND_STR) - 1)
#define SPIDER_SQL_SPACE_STR " "
#define SPIDER_SQL_SPACE_LEN (sizeof(SPIDER_SQL_SPACE_STR) - 1)
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
#define SPIDER_SQL_FLUSH_TABLES_STR "flush tables"
#define SPIDER_SQL_FLUSH_TABLES_LEN sizeof(SPIDER_SQL_FLUSH_TABLES_STR) - 1
#define SPIDER_SQL_WITH_READ_LOCK_STR " with read lock"
#define SPIDER_SQL_WITH_READ_LOCK_LEN sizeof(SPIDER_SQL_WITH_READ_LOCK_STR) - 1
#define SPIDER_SQL_FLUSH_LOGS_STR "flush logs"
#define SPIDER_SQL_FLUSH_LOGS_LEN sizeof(SPIDER_SQL_FLUSH_LOGS_STR) - 1

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
  DBUG_ENTER("spider_mysql_real_connect");

  if (!(conn->db_conn = mysql_init(NULL)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  mysql_options(conn->db_conn, MYSQL_SET_CHARSET_NAME, share->csname);

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
  SPIDER_DB_CONN *db_conn = spider->db_conn;
  SPIDER_CONN *conn = spider->conn;
  DBUG_ENTER("spider_db_ping");
  if (conn->server_lost)
  {
    if ((error_num = spider_db_connect(spider->share, conn)))
      DBUG_RETURN(error_num);
    conn->server_lost = FALSE;
  }
  if (
    (error_num = simple_command(
      db_conn, COM_PING, 0, 0, 0)) == CR_SERVER_LOST
  ) {
    spider->conn->trx_isolation = -1;
    spider->conn->autocommit = -1;
    spider->conn->sql_log_off = -1;
    spider_db_disconnect(spider->conn);
    if ((error_num = spider_db_connect(spider->share, conn)))
    {
      conn->server_lost = TRUE;
      DBUG_RETURN(error_num);
    }
    if((error_num = simple_command(db_conn, COM_PING, 0, 0, 0)))
    {
      spider_db_disconnect(spider->conn);
      conn->server_lost = TRUE;
      DBUG_RETURN(error_num);
    }
  }
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
    if (error_num == CR_SERVER_GONE_ERROR || error_num == CR_SERVER_LOST)
    {
      spider_db_disconnect(conn);
      conn->server_lost = TRUE;
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
  DBUG_ENTER("spider_db_rollback");
  if (spider_db_query(
    conn,
    SPIDER_SQL_ROLLBACK_STR,
    SPIDER_SQL_ROLLBACK_LEN)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  conn->trx_start = FALSE;
  DBUG_RETURN(0);
}

void spider_db_append_xid_str(
  String *tmp_str,
  XID *xid
) {
  char format_id[sizeof(long) + 3];
  uint format_id_length;
  DBUG_ENTER("spider_db_append_xid_str");

  format_id_length = my_sprintf(format_id, (format_id, "0x%lx", xid->formatID));  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  tmp_str->q_append(xid->data, xid->gtrid_length);
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  tmp_str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  tmp_str->q_append(xid->data + xid->gtrid_length, xid->bqual_length);
  tmp_str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  tmp_str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  tmp_str->q_append(format_id, format_id_length);

  DBUG_VOID_RETURN;
}

int spider_db_xa_start(
  SPIDER_CONN *conn,
  XID *xid
) {
  char sql_buf[SPIDER_SQL_XA_START_LEN + XIDDATASIZE + sizeof(long) + 9];
  String sql_str(sql_buf, sizeof(sql_buf), system_charset_info);
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
  String sql_str(sql_buf, sizeof(sql_buf), system_charset_info);
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
  String sql_str(sql_buf, sizeof(sql_buf), system_charset_info);
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
  String sql_str(sql_buf, sizeof(sql_buf), system_charset_info);
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
  String sql_str(sql_buf, sizeof(sql_buf), system_charset_info);
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
    if (str->reserve(tmp_spider->share->tgt_db_length + SPIDER_SQL_DOT_LEN +
      tmp_spider->share->tgt_table_name_length +
      ((SPIDER_SQL_NAME_QUOTE_LEN) * 4) + spider_db_table_lock_len[lock_type]))
    {
      my_hash_reset(&conn->lock_table_hash);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    str->q_append(tmp_spider->share->tgt_db, tmp_spider->share->tgt_db_length);
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    str->q_append(tmp_spider->share->tgt_table_name,
      tmp_spider->share->tgt_table_name_length);
    str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
    str->q_append(spider_db_table_lock_str[lock_type],
      spider_db_table_lock_len[lock_type]);
    hash_delete(&conn->lock_table_hash, (uchar*) tmp_spider);
  }
  if (spider_db_query(
    conn,
    str->ptr(),
    str->length() - SPIDER_SQL_COMMA_LEN)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  DBUG_RETURN(0);
}

int spider_db_unlock_tables(
  ha_spider *spider
) {
  SPIDER_CONN *conn = spider->conn;
  DBUG_ENTER("spider_db_unlock_tables");
  if (spider_db_query(
    conn,
    SPIDER_SQL_UNLOCK_TABLE_STR,
    SPIDER_SQL_UNLOCK_TABLE_LEN)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  DBUG_RETURN(0);
}

void spider_db_append_column_name(
  String *str,
  const Field *field,
  int field_length
) {
  DBUG_ENTER("spider_db_append_column_name");
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(field->field_name, field_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  DBUG_VOID_RETURN;
}

int spider_db_append_column_value(
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
  if (field->result_type() == STRING_RESULT)
  {
    if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    if (
      field->type() == MYSQL_TYPE_VARCHAR ||
      (field->type() >= MYSQL_TYPE_ENUM &&
        field->type() <= MYSQL_TYPE_STRING)
    ) {
      char buf2[MAX_FIELD_WIDTH];
      String tmp_str2(buf2, MAX_FIELD_WIDTH, system_charset_info);
      tmp_str2.length(0);
      if (tmp_str2.append(ptr->ptr(), ptr->length(), field->charset()) ||
        str->reserve(tmp_str2.length() * 2) ||
        append_escaped(str, &tmp_str2))
/*
      if (str->reserve(ptr->length() * 2) ||
        append_escaped(str, ptr))
*/
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    } else if (str->append(*ptr))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  }
  else if (str->append(*ptr))
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
  if (str->reserve(share->tgt_db_length +
    SPIDER_SQL_DOT_LEN + share->tgt_table_name_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
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

  if (str->reserve(SPIDER_SQL_TRUNCATE_TABLE_LEN + share->tgt_db_length +
    SPIDER_SQL_DOT_LEN + share->tgt_table_name_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_TRUNCATE_TABLE_STR, SPIDER_SQL_TRUNCATE_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_from(
  String *str,
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_db_append_from");
  if (str->reserve(SPIDER_SQL_FROM_LEN + share->tgt_db_length +
    SPIDER_SQL_DOT_LEN + share->tgt_table_name_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
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
  if (str->reserve(SPIDER_SQL_INTO_LEN + share->tgt_db_length +
    SPIDER_SQL_DOT_LEN + share->tgt_table_name_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  for (field = table->field; *field; field++)
  {
    if (bitmap_is_set(table->write_set, (*field)->field_index))
    {
      field_name_length = strlen((*field)->field_name);
      if (str->reserve(field_name_length +
        (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      spider_db_append_column_name(str, *field, field_name_length);
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
      field_name_length = strlen((*field)->field_name);
      if ((*field)->is_null())
      {
        if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
          SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_NULL_LEN +
          SPIDER_SQL_COMMA_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(str, *field, field_name_length);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
        str->q_append(SPIDER_SQL_NULL_STR, SPIDER_SQL_NULL_LEN);
      } else {
        if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
          SPIDER_SQL_EQUAL_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(str, *field, field_name_length);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
#ifndef DBUG_OFF
        my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table,
          table->read_set);
#endif
        if (
          spider_db_append_column_value(str, *field, NULL) ||
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
      field_name_length = strlen((*field)->field_name);
      if ((*field)->is_null(ptr_diff))
      {
        if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
          SPIDER_SQL_IS_NULL_LEN + SPIDER_SQL_AND_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(str, *field, field_name_length);
        str->q_append(SPIDER_SQL_IS_NULL_STR, SPIDER_SQL_IS_NULL_LEN);
      } else {
        if (str->reserve(field_name_length + (SPIDER_SQL_NAME_QUOTE_LEN) * 2 +
          SPIDER_SQL_EQUAL_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(str, *field, field_name_length);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
        (*field)->move_field_offset(ptr_diff);
        if (
          spider_db_append_column_value(str, *field, NULL) ||
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
    field_length = strlen((*field)->field_name);
    if (str->reserve(field_length +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    spider_db_append_column_name(str, *field, field_length);
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
    field_length = strlen(field->field_name);
    if (str->reserve(field_length +
      (SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    spider_db_append_column_name(str, field, field_length);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);
  DBUG_RETURN(spider_db_append_from(str, share));
}

int spider_db_append_null(
  String *str,
  KEY_PART_INFO *key_part,
  uint key_name_length,
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
        if (str->reserve(SPIDER_SQL_IS_NULL_LEN + key_name_length))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(str, key_part->field,
          key_name_length);
        str->q_append(SPIDER_SQL_IS_NULL_STR,
          SPIDER_SQL_IS_NULL_LEN);
      } else {
        if (str->reserve(SPIDER_SQL_IS_NOT_NULL_LEN + key_name_length))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(str, key_part->field,
          key_name_length);
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

  if (!start_key_part_map && !end_key_part_map)
    DBUG_RETURN(0);
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
    key_name_length = strlen(field->field_name);
    if (key_part->null_bit && *ptr++)
    {
      if (str->reserve(SPIDER_SQL_IS_NULL_LEN + key_name_length +
        SPIDER_SQL_AND_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      spider_db_append_column_name(str, field,
        key_name_length);
      str->q_append(SPIDER_SQL_IS_NULL_STR,
        SPIDER_SQL_IS_NULL_LEN);
    } else {
      if (str->reserve(store_length + key_name_length +
        SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_AND_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      spider_db_append_column_name(str, field, key_name_length);
      str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
      if (spider_db_append_column_value(str, field, ptr) ||
        str->reserve(SPIDER_SQL_AND_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    str->q_append(SPIDER_SQL_AND_STR,
      SPIDER_SQL_AND_LEN);
  }

  /* last one */
  field = key_part->field;
  key_name_length = strlen(field->field_name);
  result_list->key_order = key_count;
  if (start_key_part_map >= end_key_part_map)
  {
    ptr = start_key->key + length;
    if ((error_num = spider_db_append_null(str, key_part, key_name_length,
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
          spider_db_append_column_name(str, field, key_name_length);
          str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
          if (spider_db_append_column_value(str, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          result_list->key_order++;
          break;
        case HA_READ_AFTER_KEY:
          if (str->reserve(store_length + key_name_length +
            SPIDER_SQL_GT_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(str, field, key_name_length);
          str->q_append(SPIDER_SQL_GT_STR, SPIDER_SQL_GT_LEN);
          if (spider_db_append_column_value(str, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          break;
        default:
          if (str->reserve(store_length + key_name_length +
            SPIDER_SQL_GTEQUAL_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(str, field, key_name_length);
          str->q_append(SPIDER_SQL_GTEQUAL_STR, SPIDER_SQL_GTEQUAL_LEN);
          if (spider_db_append_column_value(str, field, ptr))
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
    if ((error_num = spider_db_append_null(str, key_part, key_name_length,
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
          spider_db_append_column_name(str, field, key_name_length);
          str->q_append(SPIDER_SQL_LT_STR, SPIDER_SQL_LT_LEN);
          if (spider_db_append_column_value(str, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          break;
        default:
          if (str->reserve(store_length + key_name_length +
            SPIDER_SQL_LTEQUAL_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(str, field, key_name_length);
          str->q_append(SPIDER_SQL_LTEQUAL_STR, SPIDER_SQL_LTEQUAL_LEN);
          if (spider_db_append_column_value(str, field, ptr))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          break;
      }
    }
  }
  result_list->order_pos = str->length();
  DBUG_RETURN(0);
}

int spider_db_append_key_order(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
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
        key_name_length = strlen(field->field_name);
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
          spider_db_append_column_name(str, field, key_name_length);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        } else {
          if (str->reserve(key_name_length +
            SPIDER_SQL_DESC_LEN + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(str, field, key_name_length);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
      if (
        length + result_list->key_order <= key_info->key_parts &&
        length <= result_list->max_order
      ) {
        field = key_part->field;
        key_name_length = strlen(field->field_name);
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
          spider_db_append_column_name(str, field, key_name_length);
        } else {
          if (str->reserve(key_name_length + SPIDER_SQL_DESC_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(str, field, key_name_length);
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
        key_name_length = strlen(field->field_name);
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
          spider_db_append_column_name(str, field, key_name_length);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        } else {
          if (str->reserve(key_name_length + SPIDER_SQL_COMMA_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(str, field, key_name_length);
          str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
        }
      }
      if (
        length + result_list->key_order <= key_info->key_parts &&
        length <= result_list->max_order
      ) {
        field = key_part->field;
        key_name_length = strlen(field->field_name);
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
          spider_db_append_column_name(str, field, key_name_length);
          str->q_append(SPIDER_SQL_DESC_STR, SPIDER_SQL_DESC_LEN);
        } else {
          if (str->reserve(key_name_length))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          spider_db_append_column_name(str, field, key_name_length);
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
      SPIDER_SQL_SHOW_TABLE_STATUS_LEN + share->tgt_db_length +
      SPIDER_SQL_LIKE_LEN + share->tgt_table_name_length +
      ((SPIDER_SQL_NAME_QUOTE_LEN) * 2) +
      ((SPIDER_SQL_VALUE_QUOTE_LEN) * 2)) ||
    share->show_table_status[1].reserve(
      SPIDER_SQL_SELECT_TABLES_STATUS_LEN + share->tgt_db_length +
      SPIDER_SQL_AND_LEN + SPIDER_SQL_TABLE_NAME_LEN + SPIDER_SQL_EQUAL_LEN +
      share->tgt_table_name_length + ((SPIDER_SQL_VALUE_QUOTE_LEN) * 4))
  )
    goto error;
  str = &share->show_table_status[0];
  str->q_append(
    SPIDER_SQL_SHOW_TABLE_STATUS_STR, SPIDER_SQL_SHOW_TABLE_STATUS_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_LIKE_STR, SPIDER_SQL_LIKE_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str = &share->show_table_status[1];
  str->q_append(
    SPIDER_SQL_SELECT_TABLES_STATUS_STR, SPIDER_SQL_SELECT_TABLES_STATUS_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
  str->q_append(SPIDER_SQL_TABLE_NAME_STR, SPIDER_SQL_TABLE_NAME_LEN);
  str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
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
      SPIDER_SQL_SHOW_INDEX_LEN + share->tgt_db_length + SPIDER_SQL_DOT_LEN +
      share->tgt_table_name_length + ((SPIDER_SQL_NAME_QUOTE_LEN) * 4)) ||
    share->show_index[1].reserve(
      SPIDER_SQL_SELECT_STATISTICS_LEN + share->tgt_db_length +
      SPIDER_SQL_AND_LEN + SPIDER_SQL_TABLE_NAME_LEN + SPIDER_SQL_EQUAL_LEN +
      share->tgt_table_name_length + ((SPIDER_SQL_VALUE_QUOTE_LEN) * 4) +
      SPIDER_SQL_GROUP_LEN + SPIDER_SQL_COLUMN_NAME_LEN)
  )
    goto error;
  str = &share->show_index[0];
  str->q_append(
    SPIDER_SQL_SHOW_INDEX_STR, SPIDER_SQL_SHOW_INDEX_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str = &share->show_index[1];
  str->q_append(
    SPIDER_SQL_SELECT_STATISTICS_STR, SPIDER_SQL_SELECT_STATISTICS_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(SPIDER_SQL_AND_STR, SPIDER_SQL_AND_LEN);
  str->q_append(SPIDER_SQL_TABLE_NAME_STR, SPIDER_SQL_TABLE_NAME_LEN);
  str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
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

int spider_db_append_disable_keys(
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_disable_keys");

  if (str->reserve(SPIDER_SQL_SQL_ALTER_TABLE_LEN + share->tgt_db_length +
    SPIDER_SQL_DOT_LEN + share->tgt_table_name_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_SQL_DISABLE_KEYS_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ALTER_TABLE_STR,
    SPIDER_SQL_SQL_ALTER_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
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

  if (str->reserve(SPIDER_SQL_SQL_ALTER_TABLE_LEN + share->tgt_db_length +
    SPIDER_SQL_DOT_LEN + share->tgt_table_name_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_SQL_ENABLE_KEYS_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ALTER_TABLE_STR,
    SPIDER_SQL_SQL_ALTER_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
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

  if (str->reserve(SPIDER_SQL_SQL_CHECK_TABLE_LEN + share->tgt_db_length +
    SPIDER_SQL_DOT_LEN + share->tgt_table_name_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_CHECK_TABLE_STR,
    SPIDER_SQL_SQL_CHECK_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
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
    local_length + share->tgt_db_length +
    SPIDER_SQL_DOT_LEN + share->tgt_table_name_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_REPAIR_STR, SPIDER_SQL_SQL_REPAIR_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
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
    local_length + share->tgt_db_length +
    SPIDER_SQL_DOT_LEN + share->tgt_table_name_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ANALYZE_STR, SPIDER_SQL_SQL_ANALYZE_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
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
    local_length + share->tgt_db_length +
    SPIDER_SQL_DOT_LEN + share->tgt_table_name_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_OPTIMIZE_STR, SPIDER_SQL_SQL_OPTIMIZE_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_db, share->tgt_db_length);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(SPIDER_SQL_DOT_STR, SPIDER_SQL_DOT_LEN);
  str->q_append(SPIDER_SQL_NAME_QUOTE_STR, SPIDER_SQL_NAME_QUOTE_LEN);
  str->q_append(share->tgt_table_name, share->tgt_table_name_length);
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

void spider_db_fetch_row(
  uchar *buf,
  const TABLE *table,
  Field *field,
  SPIDER_DB_ROW row,
  ulong *length
) {
  DBUG_ENTER("spider_db_fetch_row");
  if (bitmap_is_set(table->read_set, field->field_index))
  {
    my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
    field->move_field_offset(ptr_diff);
    if (!*row)
      field->set_null();
    else {
      field->set_notnull();
      field->store(*row, *length, system_charset_info);
    }
    field->move_field_offset(-ptr_diff);
  }
  DBUG_VOID_RETURN;
}

int spider_db_fetch_table(
  uchar *buf,
  TABLE *table,
  SPIDER_DB_RESULT *result
) {
  SPIDER_DB_ROW row;
  ulong *lengths;
  Field **field;
  DBUG_ENTER("spider_db_fetch_table");
  if (!(row = mysql_fetch_row(result)))
  {
    table->status = STATUS_NOT_FOUND;
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }
#ifndef DBUG_OFF
  my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table, table->write_set);
#endif
  for (
    field = table->field,
    lengths = mysql_fetch_lengths(result);
    *field;
    field++,
    lengths++
  ) {
    spider_db_fetch_row(buf, table, *field, row, lengths);
    row++;
  }
#ifndef DBUG_OFF
  dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
  table->status = 0;
  DBUG_RETURN(0);
}

int spider_db_fetch_key(
  uchar *buf,
  TABLE *table,
  const KEY *key_info,
  SPIDER_DB_RESULT *result
) {
  KEY_PART_INFO *key_part;
  uint part_num;
  SPIDER_DB_ROW row;
  ulong *lengths;
  DBUG_ENTER("spider_db_fetch_key");
  if (!(row = mysql_fetch_row(result)))
  {
    table->status = STATUS_NOT_FOUND;
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }
#ifndef DBUG_OFF
  my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table, table->write_set);
#endif
  for (
    key_part = key_info->key_part,
    part_num = 0,
    lengths = mysql_fetch_lengths(result);
    part_num < key_info->key_parts;
    key_part++,
    part_num++,
    lengths++
  ) {
    spider_db_fetch_row(buf, table, key_part->field, row, lengths);
    row++;
  }
#ifndef DBUG_OFF
  dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
  table->status = 0;
  DBUG_RETURN(0);
}

int spider_db_free_result(
  ha_spider *spider,
  bool final
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_RESULT *result = result_list->first;
  SPIDER_RESULT *prev;
  SPIDER_SHARE *share = spider->share;
  SPIDER_TRX *trx = spider->trx;
  DBUG_ENTER("spider_db_free_result");
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
      if (result->result)
      {
        mysql_free_result(result->result);
      }
      prev = result;
      result = result->next;
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
      if (result->result)
      {
        mysql_free_result(result->result);
        result->result = NULL;
        result->record_num = 0;
        result->finish_flg = FALSE;
      }
      result = result->next;
    }
  }
  result_list->current = NULL;
  result_list->record_num = 0;
  result_list->finish_flg = FALSE;
  DBUG_RETURN(0);
}

int spider_db_store_result(
  ha_spider *spider,
  TABLE *table
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
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
  }

  if (!(result_list->current->result = mysql_store_result(spider->db_conn)))
  {
    if ((error_num = spider_db_errorno(spider->conn)))
      DBUG_RETURN(error_num);
    else {
      result_list->current->finish_flg = TRUE;
      result_list->current_row_num = 0;
      result_list->finish_flg = TRUE;
      table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
  } else {
    result_list->current->record_num =
      (longlong) mysql_num_rows(result_list->current->result);
    result_list->record_num += result_list->current->record_num;
    if (
      result_list->internal_limit >= result_list->record_num ||
      result_list->split_read > result_list->current->record_num
    ) {
      result_list->current->finish_flg = TRUE;
      result_list->finish_flg = TRUE;
    }
    result_list->current_row_num = 0;
    result_list->current->first_row =
      result_list->current->result->data_cursor;
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
  if (result_list->keyread)
    error_num = spider_db_fetch_key(buf, table,
      result_list->key_info, result_list->current->result);
  else
    error_num = spider_db_fetch_table(buf, table,
      result_list->current->result);
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
    result_list->current = result_list->current->prev;
    result_list->current_row_num = result_list->current->record_num - 1;
  } else {
    result_list->current_row_num -= 2;
  }
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
    if (result_list->finish_flg)
    {
      table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
    result_list->sql.length(result_list->limit_pos);
    if ((error_num = spider_db_append_limit(
      &result_list->sql,
      result_list->internal_offset + result_list->record_num,
      result_list->internal_limit - result_list->record_num >=
      result_list->split_read ?
      result_list->split_read :
      result_list->internal_limit - result_list->record_num))
    )
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
    result_list->current = result_list->last;
    result_list->current_row_num = result_list->current->record_num - 1;
    result_list->current->result->data_cursor =
      result_list->current->first_row + result_list->current_row_num;
    DBUG_RETURN(spider_db_fetch(buf, spider, table));
  } else if (!result_list->sorted ||
    result_list->internal_limit <= result_list->record_num * 2)
  {
    result_list->sql.length(result_list->limit_pos);
    if ((error_num = spider_db_append_limit(
      &result_list->sql,
      result_list->internal_offset + result_list->record_num,
      result_list->internal_limit - result_list->record_num))
    )
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
    result_list->current->result->data_cursor =
      result_list->current->first_row + result_list->current_row_num;
    DBUG_RETURN(spider_db_fetch(buf, spider, table));
  } else {
    if ((error_num = spider_db_free_result(spider, FALSE)))
      DBUG_RETURN(error_num);
    result_list->sql.length(result_list->order_pos);
    result_list->desc_flg = ~(result_list->desc_flg);
    if (
      (error_num = spider_db_append_key_order(spider)) ||
      (error_num = spider_db_append_limit(
        &result_list->sql, result_list->internal_offset,
        result_list->internal_limit >= result_list->split_read ?
        result_list->split_read : result_list->internal_limit))
    )
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
  result_list->current = result_list->first;
  result_list->current_row_num = 0;
  result_list->current->result->data_cursor =
    result_list->current->first_row;
  DBUG_RETURN(spider_db_fetch(buf, spider, table));
}

SPIDER_POSITION *spider_db_create_position(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_create_position");
  SPIDER_POSITION *pos = (SPIDER_POSITION*) sql_alloc(sizeof(SPIDER_POSITION));
  pos->row =
    (result_list->current->first_row + result_list->current_row_num - 1)->data;
  pos->lengths = mysql_fetch_lengths(result_list->current->result);
  DBUG_RETURN(pos);
}

int spider_db_seek_tmp(
  uchar *buf,
  SPIDER_POSITION *pos,
  ha_spider *spider,
  TABLE *table
) {
  ulong *lengths;
  Field **field;
  DBUG_ENTER("spider_db_seek_tmp");
#ifndef DBUG_OFF
  my_bitmap_map *tmp_map = dbug_tmp_use_all_columns(table, table->write_set);
#endif
  for (
    field = table->field,
    lengths = pos->lengths;
    *field;
    field++,
    lengths++
  ) {
    spider_db_fetch_row(buf, table, *field, pos->row, lengths);
    pos->row++;
  }
#ifndef DBUG_OFF
  dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
  DBUG_RETURN(0);
}

int spider_db_show_table_status(
  ha_spider *spider
) {
  int error_num;
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_show_table_status");
  if (spider->share->sts_mode == 1)
  {
    if (spider_db_query(
      spider->conn,
      share->show_table_status[0].ptr(),
      share->show_table_status[0].length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
    if (
      !(res = mysql_store_result(spider->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if (res)
        mysql_free_result(res);
      if ((error_num = spider_db_errorno(spider->conn)))
        DBUG_RETURN(error_num);
      else {
        my_printf_error(ER_SPIDER_REMOTE_TABLE_NOT_FOUND_NUM,
          ER_SPIDER_REMOTE_TABLE_NOT_FOUND_STR, MYF(0),
          spider->share->tgt_db, spider->share->tgt_table_name);
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
      share->create_time =
        (time_t) my_strtoll10(row[11], (char**) NULL, &error_num);
    else
      share->create_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider create_time=%lld", share->create_time));
    if (row[12])
      share->update_time =
        (time_t) my_strtoll10(row[12], (char**) NULL, &error_num);
    else
      share->update_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider update_time=%lld", share->update_time));
    if (row[13])
      share->check_time =
        (time_t) my_strtoll10(row[13], (char**) NULL, &error_num);
    else
      share->check_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider check_time=%lld", share->check_time));
    mysql_free_result(res);
  } else {
    if (spider_db_query(
      spider->conn,
      spider->share->show_table_status[1].ptr(),
      spider->share->show_table_status[1].length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
    if (
      !(res = mysql_store_result(spider->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if (res)
        mysql_free_result(res);
      if ((error_num = spider_db_errorno(spider->conn)))
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
      share->create_time =
        (time_t) my_strtoll10(row[6], (char**) NULL, &error_num);
    else
      share->create_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider create_time=%lld", share->create_time));
    if (row[7])
      share->update_time =
        (time_t) my_strtoll10(row[7], (char**) NULL, &error_num);
    else
      share->update_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider update_time=%lld", share->update_time));
    if (row[8])
      share->check_time =
        (time_t) my_strtoll10(row[8], (char**) NULL, &error_num);
    else
      share->check_time = (time_t) 0;
    DBUG_PRINT("info",
      ("spider check_time=%lld", share->check_time));
    mysql_free_result(res);
  }
  DBUG_RETURN(0);
}

int spider_db_show_index(
  ha_spider *spider,
  TABLE *table
) {
  int error_num;
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row;
  Field *field;
  int roop_count;
  longlong *tmp_cardinality;
  bool *tmp_cardinality_upd;
  DBUG_ENTER("spider_db_show_index");
  if (spider->share->crd_mode == 1)
  {
    if (spider_db_query(
      spider->conn,
      spider->share->show_index[0].ptr(),
      spider->share->show_index[0].length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
    if (
      !(res = mysql_store_result(spider->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if ((error_num = spider_db_errorno(spider->conn)))
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

    for (roop_count = 0, tmp_cardinality_upd = spider->share->cardinality_upd;
      roop_count < table->s->fields;
      roop_count++, tmp_cardinality_upd++)
      *tmp_cardinality_upd = FALSE;
    while (row)
    {
      if (
        row[4] &&
        row[6] &&
        (field = find_field_in_table_sef(table, row[4]))
      ) {
        if ((spider->share->cardinality[field->field_index] =
          (longlong) my_strtoll10(row[6], (char**) NULL, &error_num)) <= 0)
          spider->share->cardinality[field->field_index] = 1;
          spider->share->cardinality_upd[field->field_index] = TRUE;
        DBUG_PRINT("info",
          ("spider col_name=%s", row[4]));
        DBUG_PRINT("info",
          ("spider cardinality=%lld",
          spider->share->cardinality[field->field_index]));
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
    for (roop_count = 0, tmp_cardinality = spider->share->cardinality,
      tmp_cardinality_upd = spider->share->cardinality_upd;
      roop_count < table->s->fields;
      roop_count++, tmp_cardinality++, tmp_cardinality_upd++)
    {
      if (!tmp_cardinality_upd)
        *tmp_cardinality = 1;
    }
    if (res)
      mysql_free_result(res);
  } else {
    if (spider_db_query(
      spider->conn,
      spider->share->show_index[1].ptr(),
      spider->share->show_index[1].length())
    )
      DBUG_RETURN(spider_db_errorno(spider->conn));
    if (
      !(res = mysql_store_result(spider->db_conn)) ||
      !(row = mysql_fetch_row(res))
    ) {
      if ((error_num = spider_db_errorno(spider->conn)))
      {
        if (res)
          mysql_free_result(res);
        DBUG_RETURN(error_num);
      }
      /* no record is ok */
    }

    for (roop_count = 0, tmp_cardinality_upd = spider->share->cardinality_upd;
      roop_count < table->s->fields;
      roop_count++, tmp_cardinality_upd++)
      *tmp_cardinality_upd = FALSE;
    while (row)
    {
      if (
        row[0] &&
        row[1] &&
        (field = find_field_in_table_sef(table, row[0]))
      ) {
        if ((spider->share->cardinality[field->field_index] =
          (longlong) my_strtoll10(row[1], (char**) NULL, &error_num)) <= 0)
          spider->share->cardinality[field->field_index] = 1;
          spider->share->cardinality_upd[field->field_index] = TRUE;
        DBUG_PRINT("info",
          ("spider col_name=%s", row[0]));
        DBUG_PRINT("info",
          ("spider cardinality=%lld",
          spider->share->cardinality[field->field_index]));
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
    for (roop_count = 0, tmp_cardinality = spider->share->cardinality,
      tmp_cardinality_upd = spider->share->cardinality_upd;
      roop_count < table->s->fields;
      roop_count++, tmp_cardinality++, tmp_cardinality_upd++)
    {
      if (!tmp_cardinality_upd)
        *tmp_cardinality = 1;
    }
    if (res)
      mysql_free_result(res);
  }
  DBUG_RETURN(0);
}

ha_rows spider_db_explain_select(
  key_range *start_key,
  key_range *end_key,
  ha_spider *spider
) {
  int error_num;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sql;
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row;
  ha_rows rows;
  DBUG_ENTER("spider_db_explain_select");
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

  if (spider_db_query(
    spider->conn,
    str->ptr(),
    str->length())
  ) {
    my_errno = spider_db_errorno(spider->conn);
    DBUG_RETURN(HA_POS_ERROR);
  }
  if (
    !(res = mysql_store_result(spider->db_conn)) ||
    !(row = mysql_fetch_row(res))
  ) {
    if (res)
      mysql_free_result(res);
    if ((error_num = spider_db_errorno(spider->conn)))
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
            spider_db_append_column_value(str, *field, NULL) ||
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
    if (spider_db_query(
      spider->conn,
      str->ptr(),
      str->length() - SPIDER_SQL_COMMA_LEN)
    ) {
      str->length(result_list->insert_pos);
      error_num = spider_db_errorno(spider->conn);
      if (error_num == HA_ERR_FOUND_DUPP_KEY)
      {
        uint roop_count;
        int key_name_length;
        int max_length = 0;
        char *key_name;
        DBUG_PRINT("info",("spider error_str=%s", spider->conn->error_str));
        for (roop_count = 0; roop_count < table->s->keys; roop_count++)
        {
          key_name = table->s->key_info[roop_count].name;
          key_name_length = strlen(key_name);
          DBUG_PRINT("info",("spider key_name=%s", key_name));
          if (
            max_length < key_name_length &&
            spider->conn->error_length - 1 >= key_name_length &&
            *(spider->conn->error_str + spider->conn->error_length - 2 -
              key_name_length) == '\'' &&
            !strncasecmp(spider->conn->error_str +
              spider->conn->error_length - 1 - key_name_length,
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
    if (
      (error_num = spider_db_update_auto_increment(spider)) ||
      (table->next_number_field &&
      (error_num = table->next_number_field->store(
        spider->db_conn->last_used_con->insert_id, TRUE)))
    )
      DBUG_RETURN(error_num);
  }
  DBUG_RETURN(0);
}

int spider_db_update_auto_increment(
  ha_spider *spider
) {
  ulonglong last_insert_id;
  DBUG_ENTER("spider_db_update_auto_increment");
  last_insert_id = spider->db_conn->last_used_con->insert_id;
  spider->share->auto_increment_value =
    last_insert_id + spider->db_conn->last_used_con->affected_rows - 1;
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

  if (spider_db_query(
    spider->conn,
    str->ptr(),
    str->length())
  )
    DBUG_RETURN(spider_db_errorno(spider->conn));
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
