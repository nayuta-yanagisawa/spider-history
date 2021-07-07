/* Copyright (C) 2008-2010 Kentoku Shiba

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
#include "spd_sys_table.h"
#include "ha_spider.h"
#include "spd_db_conn.h"
#include "spd_conn.h"
#include "spd_direct_sql.h"
#include "spd_ping_table.h"
#include "spd_copy_tables.h"

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
  uint net_timeout;
  THD* thd = current_thd;
  longlong connect_retry_interval;
  my_bool connect_mutex = spider_connect_mutex;
  DBUG_ENTER("spider_db_connect");
  DBUG_ASSERT(conn->need_mon);

  if (thd)
  {
    net_timeout = THDVAR(thd, net_timeout) == -1 ?
      share->net_timeout : THDVAR(thd, net_timeout);
    connect_retry_interval = THDVAR(thd, connect_retry_interval);
    connect_retry_count = THDVAR(thd, connect_retry_count);
  } else {
    net_timeout = share->net_timeout;
    connect_retry_interval = 0;
    connect_retry_count = 0;
  }
  DBUG_PRINT("info",("spider net_timeout=%u", net_timeout));

  if ((error_num = spider_reset_conn_setted_parameter(conn)))
    DBUG_RETURN(error_num);

  while (TRUE)
  {
    if (!(conn->db_conn = mysql_init(NULL)))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);

    mysql_options(conn->db_conn, MYSQL_OPT_READ_TIMEOUT,
      &net_timeout);
    mysql_options(conn->db_conn, MYSQL_OPT_CONNECT_TIMEOUT,
      &net_timeout);

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
      DBUG_PRINT("info",("spider tgt_default_file=%s", conn->tgt_default_file));
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
      my_sleep(connect_retry_interval);
    } else {
      if (connect_mutex)
        pthread_mutex_unlock(&spider_open_conn_mutex);
      break;
    }
  }
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
    conn->need_mon = &spider->need_mons[link_idx];
  }
  if (conn->server_lost)
  {
    if ((error_num = spider_db_connect(spider->share, conn, link_idx)))
    {
      if (!conn->mta_conn_mutex_unlock_later)
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(error_num);
    }
    conn->server_lost = FALSE;
  }
  if ((error_num = simple_command(conn->db_conn, COM_PING, 0, 0, 0)))
  {
    spider_db_disconnect(conn);
    if ((error_num = spider_db_connect(spider->share, conn, link_idx)))
    {
      conn->server_lost = TRUE;
      if (!conn->mta_conn_mutex_unlock_later)
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(error_num);
    }
    if((error_num = simple_command(conn->db_conn, COM_PING, 0, 0, 0)))
    {
      spider_db_disconnect(conn);
      conn->server_lost = TRUE;
      if (!conn->mta_conn_mutex_unlock_later)
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(error_num);
    }
  }
  conn->ping_time = (time_t) time((time_t*) 0);
  if (!conn->mta_conn_mutex_unlock_later)
    pthread_mutex_unlock(&conn->mta_conn_mutex);
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

int spider_db_before_query(
  SPIDER_CONN *conn,
  int *need_mon
) {
  DBUG_ENTER("spider_db_before_query");
  DBUG_ASSERT(need_mon);
  if (conn->server_lost)
    DBUG_RETURN(CR_SERVER_GONE_ERROR);
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (conn->bg_search)
    spider_bg_conn_break(conn, NULL);
#endif
  if (!conn->mta_conn_mutex_lock_already)
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = need_mon;
  }
  if (conn->quick_target)
  {
    bool tmp_mta_conn_mutex_unlock_later;
    int error_num;
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
  DBUG_PRINT("info", ("spider conn->db_conn %x", conn->db_conn));
  if ((error_num = spider_db_before_query(conn, need_mon)))
    DBUG_RETURN(error_num);
#ifndef DBUG_OFF
  String tmp_query_str(sizeof(char) * (length + 1));
  char *tmp_query = (char *) tmp_query_str.c_ptr_safe();
  memcpy(tmp_query, query, length);
  tmp_query[length] = '\0';
  query = (const char *) tmp_query;
#endif
  DBUG_RETURN(mysql_real_query(conn->db_conn, query, length));
}

int spider_db_errorno(
  SPIDER_CONN *conn
) {
  int error_num;
  DBUG_ENTER("spider_db_errorno");
  DBUG_ASSERT(conn->need_mon);
  if (conn->server_lost)
  {
    *conn->need_mon = ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM;
    my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
      ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
    if (!conn->mta_conn_mutex_unlock_later)
      pthread_mutex_unlock(&conn->mta_conn_mutex);
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
      conn->server_lost = TRUE;
      if (conn->disable_reconnect)
      {
        *conn->need_mon = ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM;
        my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
          ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
      }
      if (!conn->mta_conn_mutex_unlock_later)
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM);
    } else if (
      conn->ignore_dup_key &&
      (error_num == ER_DUP_ENTRY || error_num == ER_DUP_KEY)
    ) {
      conn->error_str = (char*) mysql_error(conn->db_conn);
      conn->error_length = strlen(conn->error_str);
      if (!conn->mta_conn_mutex_unlock_later)
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
    } else if (
      error_num == ER_XAER_NOTA &&
      current_thd &&
      THDVAR(current_thd, force_commit) == 1
    ) {
      push_warning(current_thd, MYSQL_ERROR::WARN_LEVEL_WARN,
        error_num, mysql_error(conn->db_conn));
      if (!conn->mta_conn_mutex_unlock_later)
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(error_num);
    }
    *conn->need_mon = error_num;
    my_message(error_num, mysql_error(conn->db_conn), MYF(0));
    if (!conn->mta_conn_mutex_unlock_later)
      pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(error_num);
  }
  if (!conn->mta_conn_mutex_unlock_later)
    pthread_mutex_unlock(&conn->mta_conn_mutex);
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
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      break;
    default:
      DBUG_RETURN(HA_ERR_UNSUPPORTED);
  }
  DBUG_RETURN(0);
}

int spider_db_set_autocommit(
  SPIDER_CONN *conn,
  bool autocommit,
  int *need_mon
) {
  DBUG_ENTER("spider_db_set_autocommit");
  if (autocommit)
  {
    if (spider_db_query(
      conn,
      SPIDER_SQL_AUTOCOMMIT_ON_STR,
      SPIDER_SQL_AUTOCOMMIT_ON_LEN,
      need_mon)
    )
      DBUG_RETURN(spider_db_errorno(conn));
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  } else {
    if (spider_db_query(
      conn,
      SPIDER_SQL_AUTOCOMMIT_OFF_STR,
      SPIDER_SQL_AUTOCOMMIT_OFF_LEN,
      need_mon)
    )
      DBUG_RETURN(spider_db_errorno(conn));
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

int spider_db_set_sql_log_off(
  SPIDER_CONN *conn,
  bool sql_log_off,
  int *need_mon
) {
  DBUG_ENTER("spider_db_set_sql_log_off");
  if (sql_log_off)
  {
    if (spider_db_query(
      conn,
      SPIDER_SQL_SQL_LOG_ON_STR,
      SPIDER_SQL_SQL_LOG_ON_LEN,
      need_mon)
    )
      DBUG_RETURN(spider_db_errorno(conn));
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  } else {
    if (spider_db_query(
      conn,
      SPIDER_SQL_SQL_LOG_OFF_STR,
      SPIDER_SQL_SQL_LOG_OFF_LEN,
      need_mon)
    )
      DBUG_RETURN(spider_db_errorno(conn));
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

int spider_db_set_names(
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
) {
  bool tmp_mta_conn_mutex_lock_already;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_set_names");
  if (!conn->mta_conn_mutex_lock_already)
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = &spider->need_mons[link_idx];
  }
  if (
    !conn->access_charset ||
    share->access_charset->cset != conn->access_charset->cset
  ) {
    tmp_mta_conn_mutex_lock_already = conn->mta_conn_mutex_lock_already;
    conn->mta_conn_mutex_lock_already = TRUE;
    if (
      spider_db_before_query(conn, &spider->need_mons[link_idx]) ||
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
  if (!conn->mta_conn_mutex_unlock_later)
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_query_with_set_names(
  String *sql,
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
) {
  int error_num;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_query_with_set_names");

  pthread_mutex_lock(&conn->mta_conn_mutex);
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  if ((error_num = spider_db_set_names(spider, conn, link_idx)))
  {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    if (
      share->monitoring_kind[link_idx] &&
      spider->need_mons[link_idx]
    ) {
      error_num = spider_ping_table_mon_from_table(
          spider->trx,
          spider->trx->thd,
          share,
          share->monitoring_sid[link_idx],
          share->table_name,
          share->table_name_length,
          link_idx,
          "",
          0,
          share->monitoring_kind[link_idx],
          share->monitoring_limit[link_idx],
          TRUE
        );
    }
    DBUG_RETURN(error_num);
  }
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
          share->monitoring_sid[link_idx],
          share->table_name,
          share->table_name_length,
          link_idx,
          "",
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
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_query_for_bulk_update(
  String *sql,
  ha_spider *spider,
  SPIDER_CONN *conn,
  int link_idx
) {
  int error_num;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_query_for_bulk_update");

  pthread_mutex_lock(&conn->mta_conn_mutex);
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  if ((error_num = spider_db_set_names(spider, conn, link_idx)))
  {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    if (
      share->monitoring_kind[link_idx] &&
      spider->need_mons[link_idx]
    ) {
      error_num = spider_ping_table_mon_from_table(
          spider->trx,
          spider->trx->thd,
          share,
          share->monitoring_sid[link_idx],
          share->table_name,
          share->table_name_length,
          link_idx,
          "",
          0,
          share->monitoring_kind[link_idx],
          share->monitoring_limit[link_idx],
          TRUE
        );
    }
    DBUG_RETURN(error_num);
  }
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
          share->monitoring_sid[link_idx],
          share->table_name,
          share->table_name_length,
          link_idx,
          "",
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
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    if (
      share->monitoring_kind[link_idx] &&
      spider->need_mons[link_idx]
    ) {
      error_num = spider_ping_table_mon_from_table(
          spider->trx,
          spider->trx->thd,
          share,
          share->monitoring_sid[link_idx],
          share->table_name,
          share->table_name_length,
          link_idx,
          "",
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
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_commit(
  SPIDER_CONN *conn
) {
  int need_mon = 0;
  DBUG_ENTER("spider_db_commit");
  if (spider_db_query(
    conn,
    SPIDER_SQL_COMMIT_STR,
    SPIDER_SQL_COMMIT_LEN,
    &need_mon)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  conn->trx_start = FALSE;
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_rollback(
  SPIDER_CONN *conn
) {
  int error_num, need_mon = 0;
  bool is_error;
  DBUG_ENTER("spider_db_rollback");
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
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(error_num);
    }
  }
  conn->trx_start = FALSE;
  conn->mta_conn_mutex_unlock_later = FALSE;
  pthread_mutex_unlock(&conn->mta_conn_mutex);
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
#ifndef DBUG_OFF
  ((char *) tmp_str->ptr())[tmp_str->length()] = '\0';
#endif

  DBUG_VOID_RETURN;
}

int spider_db_xa_start(
  SPIDER_CONN *conn,
  XID *xid,
  int *need_mon
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
    sql_str.length(),
    need_mon)
  )
    DBUG_RETURN(spider_db_errorno(conn));
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_xa_end(
  SPIDER_CONN *conn,
  XID *xid
) {
  int need_mon = 0;
  char sql_buf[SPIDER_SQL_XA_END_LEN + XIDDATASIZE + sizeof(long) + 9];
  String sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_end");

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
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_xa_prepare(
  SPIDER_CONN *conn,
  XID *xid
) {
  int need_mon = 0;
  char sql_buf[SPIDER_SQL_XA_PREPARE_LEN + XIDDATASIZE + sizeof(long) + 9];
  String sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_prepare");

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
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_xa_commit(
  SPIDER_CONN *conn,
  XID *xid
) {
  int need_mon = 0;
  char sql_buf[SPIDER_SQL_XA_COMMIT_LEN + XIDDATASIZE + sizeof(long) + 9];
  String sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_commit");

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
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_xa_rollback(
  SPIDER_CONN *conn,
  XID *xid
) {
  int need_mon = 0;
  char sql_buf[SPIDER_SQL_XA_ROLLBACK_LEN + XIDDATASIZE + sizeof(long) + 9];
  String sql_str(sql_buf, sizeof(sql_buf), &my_charset_bin);
  DBUG_ENTER("spider_db_xa_rollback");

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
  pthread_mutex_unlock(&conn->mta_conn_mutex);
  DBUG_RETURN(0);
}

int spider_db_lock_tables(
  ha_spider *spider,
  int link_idx
) {
  int error_num;
  ha_spider *tmp_spider;
  int lock_type;
  SPIDER_CONN *conn = spider->conns[link_idx];
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sqls[link_idx];
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
    if (&tmp_spider->share->db_names_str[link_idx])
    {
      if (
        str->append(tmp_spider->share->db_names_str[link_idx].ptr(),
          tmp_spider->share->db_names_str[link_idx].length(),
          tmp_spider->share->access_charset) ||
        str->reserve((SPIDER_SQL_NAME_QUOTE_LEN) * 2 + SPIDER_SQL_DOT_LEN)
      ) {
        my_hash_reset(&conn->lock_table_hash);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    } else {
      if (
        str->append(tmp_spider->share->tgt_dbs[link_idx],
          tmp_spider->share->tgt_dbs_lengths[link_idx],
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
    if (&tmp_spider->share->table_names_str[link_idx])
    {
      if (
        str->append(tmp_spider->share->table_names_str[link_idx].ptr(),
          tmp_spider->share->table_names_str[link_idx].length(),
          tmp_spider->share->access_charset) ||
        str->reserve(SPIDER_SQL_NAME_QUOTE_LEN +
          spider_db_table_lock_len[lock_type])
      ) {
        my_hash_reset(&conn->lock_table_hash);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    } else {
      if (
        str->append(tmp_spider->share->tgt_table_names[link_idx],
          tmp_spider->share->tgt_table_names_lengths[link_idx],
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
  pthread_mutex_lock(&conn->mta_conn_mutex);
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  if ((error_num = spider_db_set_names(spider, conn, link_idx)))
  {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(error_num);
  }
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
  if (spider_db_query(
    conn,
    SPIDER_SQL_UNLOCK_TABLE_STR,
    SPIDER_SQL_UNLOCK_TABLE_LEN,
    &spider->need_mons[link_idx])
  )
    DBUG_RETURN(spider_db_errorno(conn));
  pthread_mutex_unlock(&conn->mta_conn_mutex);
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

int spider_db_create_table_names_str(
  SPIDER_SHARE *share
) {
  int error_num, roop_count,
    table_nm_len, db_nm_len;
  String *str, *first_tbl_nm_str, *first_db_nm_str;
  char *first_tbl_nm, *first_db_nm;
  DBUG_ENTER("spider_db_create_table_names_str");
  if (
    !(share->table_names_str = new String[share->link_count]) ||
    !(share->db_names_str = new String[share->link_count])
  ) {
    share->db_names_str = NULL;
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
  for (roop_count = 0; roop_count < share->link_count; roop_count++)
  {
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
  }
  DBUG_RETURN(0);

error:
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

int spider_db_append_table_name_with_reserve(
  String *str,
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
  String *str,
  SPIDER_SHARE *share,
  int link_idx
) {
  DBUG_ENTER("spider_db_append_table_name");
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
  String *str,
  SPIDER_SHARE *share,
  int link_idx
) {
  int length;
  DBUG_ENTER("spider_db_append_table_name_with_adjusting");
  spider_db_append_table_name(str, share, link_idx);
  length = share->db_nm_max_length - share->db_names_str[link_idx].length() +
    share->table_nm_max_length - share->table_names_str[link_idx].length();
  memset((char *) str->ptr() + str->length(), ' ', length);
  str->length(str->length() + length);
  DBUG_VOID_RETURN;
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
  uint length;
  DBUG_ENTER("spider_db_append_column_value");

  if (new_ptr)
  {
    if (
      field->type() == MYSQL_TYPE_BLOB ||
      field->real_type() == MYSQL_TYPE_VARCHAR ||
      field->type() == MYSQL_TYPE_GEOMETRY
    ) {
      length = uint2korr(new_ptr);
      tmp_str.set_quick((char *) new_ptr + HA_KEY_BLOB_LENGTH, length,
        &my_charset_bin);
      ptr = &tmp_str;
    } else {
      ptr = field->val_str(&tmp_str, new_ptr);
    }
  } else
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

int spider_db_append_column_values(
  const key_range *start_key,
  ha_spider *spider,
  String *str
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
  DBUG_PRINT("info", ("spider key_info->key_parts=%lu", key_info->key_parts));
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
      if (spider_db_append_column_value(share, str, field, ptr))
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
  String *str
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

int spider_db_append_insert_str(
  String *str,
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
  String *str = &result_list->insert_sql;
  DBUG_ENTER("spider_db_append_insert");

  if (spider->write_can_replace && spider->direct_dup_insert)
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
    spider->direct_dup_insert &&
    !spider->write_can_replace &&
    !spider->insert_with_update
  ) {
    if (str->reserve(SPIDER_SQL_SQL_IGNORE_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_SQL_IGNORE_STR, SPIDER_SQL_SQL_IGNORE_LEN);
  }
  DBUG_RETURN(0);
}

int spider_db_append_insert_for_recovery(
  String *insert_sql,
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
  spider_db_append_table_name(insert_sql, share, link_idx);
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
        spider_db_append_column_value(share, insert_sql, *field, NULL) ||
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
  String *str,
  ha_spider *spider,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
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
  if (str->reserve(share->db_nm_max_length +
    SPIDER_SQL_DOT_LEN + share->table_nm_max_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  result_list->table_name_pos = str->length();
  spider_db_append_table_name_with_adjusting(str, share, link_idx);
  DBUG_RETURN(0);
}

int spider_db_append_delete(
  String *str,
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
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
  ha_spider *spider,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->update_sql;
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_truncate");

  if (str->reserve(SPIDER_SQL_TRUNCATE_TABLE_LEN + share->db_nm_max_length +
    SPIDER_SQL_DOT_LEN + share->table_nm_max_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_TRUNCATE_TABLE_STR, SPIDER_SQL_TRUNCATE_TABLE_LEN);
  result_list->table_name_pos = str->length();
  spider_db_append_table_name_with_adjusting(str, share, link_idx);
  DBUG_RETURN(0);
}

int spider_db_append_from_str(
  String *str
) {
  DBUG_ENTER("spider_db_append_from_str");
  if (str->reserve(SPIDER_SQL_FROM_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_from(
  String *str,
  ha_spider *spider,
  int link_idx
) {
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_append_from");
  if (str->reserve(SPIDER_SQL_FROM_LEN + share->db_nm_max_length +
    SPIDER_SQL_DOT_LEN + share->table_nm_max_length +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_FROM_STR, SPIDER_SQL_FROM_LEN);
  result_list->table_name_pos = str->length();
  spider_db_append_table_name_with_adjusting(str, share, link_idx);
  DBUG_RETURN(0);
}

int spider_db_append_from_with_alias(
  String *str,
  char **table_names,
  uint *table_name_lengths,
  char **table_aliases,
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
  String *str
) {
  DBUG_ENTER("spider_db_append_open_paren_str");
  if (str->reserve(SPIDER_SQL_OPEN_PAREN_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_into_str(
  String *str
) {
  DBUG_ENTER("spider_db_append_into_str");
  if (str->reserve(SPIDER_SQL_INTO_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_INTO_STR, SPIDER_SQL_INTO_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_values_str(
  String *str
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
  String *str = &result_list->insert_sql;
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
  spider_db_append_table_name_with_adjusting(str, share, link_idx);
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
  String *str,
  ha_spider *spider,
  TABLE *table
) {
  uint field_name_length;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
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
  String *str,
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
  String *str,
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
  String *str,
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
    DBUG_RETURN(0);
  }
  DBUG_RETURN(spider_db_append_minimum_select(
    &result_list->sql, spider->get_table(), spider));
}

int spider_db_append_table_select_with_alias(
  String *str,
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
  String *str,
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
  String *str,
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
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_append_select_columns_with_alias");
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

int spider_db_append_null_value(
  String *str,
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
  String *str
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  KEY *key_info = result_list->key_info;
  uint key_name_length, key_count;
  key_part_map full_key_part_map = make_prev_keypart_map(key_info->key_parts);
  key_part_map start_key_part_map;
  KEY_PART_INFO *key_part;
  Field *field;
  char tmp_buf[MAX_FIELD_WIDTH];
  String tmp_str(tmp_buf, sizeof(tmp_buf), system_charset_info);
  DBUG_ENTER("spider_db_append_key_column_types");

  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider key_info->key_parts=%lu", key_info->key_parts));
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
    field->sql_type(tmp_str);
    str->append(tmp_str);

    if (str->reserve(SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  }
  str->length(str->length() - SPIDER_SQL_COMMA_LEN);

  DBUG_RETURN(0);
}

int spider_db_append_table_columns(
  String *str,
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
  String *str
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  KEY *key_info = result_list->key_info;
  uint key_name_length, key_count;
  key_part_map full_key_part_map = make_prev_keypart_map(key_info->key_parts);
  key_part_map start_key_part_map;
  char tmp_buf[MAX_FIELD_WIDTH];
  DBUG_ENTER("spider_db_append_key_columns");

  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider key_info->key_parts=%lu", key_info->key_parts));
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
  String *str,
  char **table_aliases,
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
  bool where_pos = (str->length() == spider->result_list.where_pos);
  DBUG_ENTER("spider_db_append_key_join_columns_for_bka");

  start_key_part_map = start_key->keypart_map & full_key_part_map;
  DBUG_PRINT("info", ("spider key_info->key_parts=%lu", key_info->key_parts));
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
  const uchar *ptr, *another_ptr;
  const key_range *use_key, *another_key;
  KEY_PART_INFO *key_part;
  Field *field;
  bool use_both = TRUE, key_eq, set_order = FALSE;
  DBUG_ENTER("spider_db_append_key_where");

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
    another_key = end_key;
    tgt_key_part_map = start_key_part_map;
  } else {
    use_key = end_key;
    another_key = start_key;
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
    key_count = 0;
    tgt_key_part_map;
    length += store_length,
    tgt_key_part_map >>= 1,
    start_key_part_map >>= 1,
    end_key_part_map >>= 1,
    key_part++,
    key_count++,
    store_length = key_part->store_length
  ) {
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
      if ((error_num = spider_db_append_null(share, str, key_part,
        start_key, &ptr)))
      {
        if (error_num > 0)
          DBUG_RETURN(error_num);
      } else if (key_eq)
      {
        DBUG_PRINT("info", ("spider key_eq"));
        if (str->reserve(store_length + key_name_length +
          SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_AND_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(share, str, field->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
        if (spider_db_append_column_value(share, str, field, ptr))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
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
            break;
          case HA_READ_AFTER_KEY:
            if (str->reserve(store_length + key_name_length +
              SPIDER_SQL_GT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_GT_STR, SPIDER_SQL_GT_LEN);
            if (spider_db_append_column_value(share, str, field, ptr))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            if (use_both)
              start_key_part_map = 0;
            if (!set_order)
            {
              result_list->key_order = key_count;
              set_order = TRUE;
            }
            break;
          case HA_READ_BEFORE_KEY:
            if (str->reserve(store_length + key_name_length +
              SPIDER_SQL_LT_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_LT_STR, SPIDER_SQL_LT_LEN);
            if (spider_db_append_column_value(share, str, field, ptr))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            result_list->desc_flg = TRUE;
            if (use_both)
              start_key_part_map = 0;
            if (!set_order)
            {
              result_list->key_order = key_count;
              set_order = TRUE;
            }
            break;
          case HA_READ_PREFIX_LAST_OR_PREV:
            if (str->reserve(store_length + key_name_length +
              SPIDER_SQL_LTEQUAL_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_LTEQUAL_STR, SPIDER_SQL_LTEQUAL_LEN);
            if (spider_db_append_column_value(share, str, field, ptr))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            result_list->desc_flg = TRUE;
            if (!set_order)
            {
              result_list->key_order = key_count;
              set_order = TRUE;
            }
            break;
          default:
            if (str->reserve(store_length + key_name_length +
              SPIDER_SQL_GTEQUAL_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_GTEQUAL_STR, SPIDER_SQL_GTEQUAL_LEN);
            if (spider_db_append_column_value(share, str, field, ptr))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            if (!set_order)
            {
              result_list->key_order = key_count;
              set_order = TRUE;
            }
            break;
        }
      }
      if (str->reserve(SPIDER_SQL_AND_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_AND_STR,
        SPIDER_SQL_AND_LEN);
    }
    if (
      (key_eq && use_key == end_key) ||
      (!key_eq && end_key_part_map)
    ) {
      ptr = end_key->key + length;
      if ((error_num = spider_db_append_null(share, str, key_part,
        end_key, &ptr)))
      {
        if (error_num > 0)
          DBUG_RETURN(error_num);
      } else if (key_eq)
      {
        DBUG_PRINT("info", ("spider key_eq"));
        if (str->reserve(store_length + key_name_length +
          SPIDER_SQL_EQUAL_LEN + SPIDER_SQL_AND_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        spider_db_append_column_name(share, str, field->field_index);
        str->q_append(SPIDER_SQL_EQUAL_STR, SPIDER_SQL_EQUAL_LEN);
        if (spider_db_append_column_value(share, str, field, ptr))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
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
            if (use_both)
              end_key_part_map = 0;
            if (!set_order)
            {
              result_list->key_order = key_count;
              set_order = TRUE;
            }
            break;
          default:
            if (str->reserve(store_length + key_name_length +
              SPIDER_SQL_LTEQUAL_LEN))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            spider_db_append_column_name(share, str, field->field_index);
            str->q_append(SPIDER_SQL_LTEQUAL_STR, SPIDER_SQL_LTEQUAL_LEN);
            if (spider_db_append_column_value(share, str, field, ptr))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            if (!set_order)
            {
              result_list->key_order = key_count;
              set_order = TRUE;
            }
            break;
        }
      }
      if (str->reserve(SPIDER_SQL_AND_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      str->q_append(SPIDER_SQL_AND_STR,
        SPIDER_SQL_AND_LEN);
    }
    if (use_both && (!start_key_part_map || !end_key_part_map))
      break;
  }
  str->length(str->length() - SPIDER_SQL_AND_LEN);
  if (!set_order)
    result_list->key_order = key_count;

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

int spider_db_append_key_order_str(
  String *str,
  KEY *key_info,
  int start_pos,
  bool desc_flg
) {
  int length, error_num;
  KEY_PART_INFO *key_part;
  Field *field;
  DBUG_ENTER("spider_db_append_key_order_str");

  if (key_info->key_parts > start_pos)
  {
    if (str->reserve(SPIDER_SQL_ORDER_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_ORDER_STR, SPIDER_SQL_ORDER_LEN);
    if (desc_flg == TRUE)
    {
      for (
        key_part = key_info->key_part + start_pos,
        length = 0;
        length + start_pos < key_info->key_parts;
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
        length + start_pos < key_info->key_parts;
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

int spider_db_append_select_lock_str(
  String *str,
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
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_append_lock");
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
  String *str;
  DBUG_ENTER("spider_db_append_show_table_status");
  if (!(share->show_table_status = new String[2 * share->link_count]))
    goto error;

  for (roop_count = 0; roop_count < share->link_count; roop_count++)
  {
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

int spider_db_append_show_index(
  SPIDER_SHARE *share
) {
  int roop_count;
  String *str;
  DBUG_ENTER("spider_db_append_show_index");
  if (!(share->show_index = new String[2 * share->link_count]))
    goto error;

  for (roop_count = 0; roop_count < share->link_count; roop_count++)
  {
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
  String *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_disable_keys");

  if (str->reserve(SPIDER_SQL_SQL_ALTER_TABLE_LEN +
    share->db_names_str[link_idx].length() +
    SPIDER_SQL_DOT_LEN + share->table_names_str[link_idx].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_SQL_DISABLE_KEYS_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ALTER_TABLE_STR,
    SPIDER_SQL_SQL_ALTER_TABLE_LEN);
  spider_db_append_table_name(str, share, link_idx);
  str->q_append(SPIDER_SQL_SQL_DISABLE_KEYS_STR,
    SPIDER_SQL_SQL_DISABLE_KEYS_LEN);
  DBUG_RETURN(0);
}

int spider_db_append_enable_keys(
  ha_spider *spider,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_enable_keys");

  if (str->reserve(SPIDER_SQL_SQL_ALTER_TABLE_LEN +
    share->db_names_str[link_idx].length() +
    SPIDER_SQL_DOT_LEN + share->table_names_str[link_idx].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4 + SPIDER_SQL_SQL_ENABLE_KEYS_LEN))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ALTER_TABLE_STR,
    SPIDER_SQL_SQL_ALTER_TABLE_LEN);
  spider_db_append_table_name(str, share, link_idx);
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
  String *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  DBUG_ENTER("spider_db_append_check_table");

  if (str->reserve(SPIDER_SQL_SQL_CHECK_TABLE_LEN +
    share->db_names_str[link_idx].length() +
    SPIDER_SQL_DOT_LEN + share->table_names_str[link_idx].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_CHECK_TABLE_STR,
    SPIDER_SQL_SQL_CHECK_TABLE_LEN);
  spider_db_append_table_name(str, share, link_idx);
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
  String *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  int local_length = (THDVAR(spider->trx->thd, internal_optimize_local) == -1 ?
    share->internal_optimize_local :
    THDVAR(spider->trx->thd, internal_optimize_local)) *
    SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_db_append_repair_table");

  if (str->reserve(SPIDER_SQL_SQL_REPAIR_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length + share->db_names_str[link_idx].length() +
    SPIDER_SQL_DOT_LEN + share->table_names_str[link_idx].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_REPAIR_STR, SPIDER_SQL_SQL_REPAIR_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  spider_db_append_table_name(str, share, link_idx);
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
  String *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  int local_length = (THDVAR(spider->trx->thd, internal_optimize_local) == -1 ?
    share->internal_optimize_local :
    THDVAR(spider->trx->thd, internal_optimize_local)) *
    SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_db_append_analyze_table");

  if (str->reserve(SPIDER_SQL_SQL_ANALYZE_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length + share->db_names_str[link_idx].length() +
    SPIDER_SQL_DOT_LEN + share->table_names_str[link_idx].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_ANALYZE_STR, SPIDER_SQL_SQL_ANALYZE_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  spider_db_append_table_name(str, share, link_idx);
  DBUG_RETURN(0);
}

int spider_db_append_optimize_table(
  ha_spider *spider,
  int link_idx
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sqls[link_idx];
  SPIDER_SHARE *share = spider->share;
  int local_length = (THDVAR(spider->trx->thd, internal_optimize_local) == -1 ?
    share->internal_optimize_local :
    THDVAR(spider->trx->thd, internal_optimize_local)) *
    SPIDER_SQL_SQL_LOCAL_LEN;
  DBUG_ENTER("spider_db_append_optimize_table");

  if (str->reserve(SPIDER_SQL_SQL_OPTIMIZE_LEN + SPIDER_SQL_SQL_TABLE_LEN +
    local_length + share->db_names_str[link_idx].length() +
    SPIDER_SQL_DOT_LEN + share->table_names_str[link_idx].length() +
    (SPIDER_SQL_NAME_QUOTE_LEN) * 4))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_SQL_OPTIMIZE_STR, SPIDER_SQL_SQL_OPTIMIZE_LEN);
  if (local_length)
    str->q_append(SPIDER_SQL_SQL_LOCAL_STR, SPIDER_SQL_SQL_LOCAL_LEN);
  str->q_append(SPIDER_SQL_SQL_TABLE_STR, SPIDER_SQL_SQL_TABLE_LEN);
  spider_db_append_table_name(str, share, link_idx);
  DBUG_RETURN(0);
}

int spider_db_append_flush_tables(
  ha_spider *spider,
  bool lock
) {
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

void spider_db_create_tmp_bka_table_name(
  ha_spider *spider,
  char *tmp_table_name,
  int *tmp_table_name_length,
  int link_idx
) {
  SPIDER_SHARE *share = spider->share;
  uint adjust_length =
    share->db_nm_max_length - share->db_names_str[link_idx].length() +
    share->table_nm_max_length - share->table_names_str[link_idx].length(),
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
    "%s%s%lx%s", SPIDER_SQL_DOT_STR, SPIDER_SQL_TMP_BKA_STR, spider,
    SPIDER_SQL_UNDERSCORE_STR));
  *tmp_table_name_length += length;
  tmp_table_name += length;
  memcpy(tmp_table_name, share->table_names_str[link_idx].c_ptr(),
    share->table_names_str[link_idx].length());
  DBUG_VOID_RETURN;
}

int spider_db_append_create_tmp_bka_table(
  const key_range *start_key,
  ha_spider *spider,
  String *str,
  char *tmp_table_name,
  int tmp_table_name_length,
  int *db_name_pos,
  CHARSET_INFO *table_charset
) {
  int error_num;
  SPIDER_SHARE *share = spider->share;
  THD *thd = spider->trx->thd;
  char *bka_engine = THDVAR(thd, bka_engine) ?
    THDVAR(thd, bka_engine) : share->bka_engine;
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
  String *str,
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
  String *str,
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
    row = current->first_position[result_list->current_row_num].row;
    lengths = current->first_position[result_list->current_row_num].lengths;
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
    row = current->first_position[result_list->current_row_num].row;
    lengths = current->first_position[result_list->current_row_num].lengths;
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

  if (result_list->low_mem_read)
  {
    if (result)
    {
      do {
        spider_db_free_one_result(result_list, result);
        DBUG_PRINT("info",("spider result=%x", result));
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
      for (roop_count = 0; roop_count < result->record_num; roop_count++)
      {
        if (
          position[roop_count].row &&
          !position[roop_count].use_position
        ) {
          my_free(position[roop_count].row, MYF(0));
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
  SPIDER_RESULT *result = (SPIDER_RESULT*) result_list->first;
  SPIDER_RESULT *prev;
  SPIDER_SHARE *share = spider->share;
  SPIDER_TRX *trx = spider->trx;
  SPIDER_POSITION *position;
  int roop_count;
  DBUG_ENTER("spider_db_free_result");
  spider_bg_all_conn_break(spider);

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
        for (roop_count = 0; roop_count < result->record_num; roop_count++)
        {
          if (position[roop_count].row)
            my_free(position[roop_count].row, MYF(0));
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
    if (!final)
    {
      int init_sql_alloc_size =
        THDVAR(trx->thd, init_sql_alloc_size) < 0 ?
        share->init_sql_alloc_size :
        THDVAR(trx->thd, init_sql_alloc_size);
      if (result_list->sql.alloced_length() > alloc_size * 2)
      {
        result_list->sql.free();
        if (result_list->sql.real_alloc(init_sql_alloc_size))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        for (roop_count = 0; roop_count < share->link_count; roop_count++)
        {
          if (result_list->sqls[roop_count].alloced_length() >
            alloc_size * 2)
          {
            result_list->sqls[roop_count].free();
            if (result_list->sqls[roop_count].real_alloc(
              init_sql_alloc_size))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          }
        }
      }
      if (result_list->insert_sql.alloced_length() > alloc_size * 2)
      {
        result_list->insert_sql.free();
        if (result_list->insert_sql.real_alloc(init_sql_alloc_size))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        if (!share->same_db_table_name)
        {
          for (roop_count = 0; roop_count < share->link_count; roop_count++)
          {
            if (result_list->insert_sqls[roop_count].alloced_length() >
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
      if (result_list->update_sql.alloced_length() > alloc_size * 2)
      {
        result_list->update_sql.free();
        if (result_list->update_sql.real_alloc(init_sql_alloc_size))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        if (!share->same_db_table_name)
        {
          for (roop_count = 0; roop_count < share->link_count; roop_count++)
          {
            if (result_list->update_sqls[roop_count].alloced_length() >
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
      for (roop_count = 0; roop_count < share->link_count; roop_count++)
        result_list->update_sqls[roop_count].length(0);

      if (result_list->tmp_sql.alloced_length() > alloc_size * 2)
      {
        result_list->tmp_sql.free();
        if (result_list->tmp_sql.real_alloc(init_sql_alloc_size))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        for (roop_count = 0; roop_count < share->link_count; roop_count++)
        {
          if (result_list->tmp_sqls[roop_count].alloced_length() >
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
        for (roop_count = 0; roop_count < result->record_num; roop_count++)
        {
          if (position[roop_count].row)
            my_free(position[roop_count].row, MYF(0));
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
  SPIDER_CONN *conn = spider->conns[link_idx];
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
      ) {
        if (!conn->mta_conn_mutex_unlock_later)
          pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
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
        ) {
          if (!conn->mta_conn_mutex_unlock_later)
            pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
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
        ) {
          if (!conn->mta_conn_mutex_unlock_later)
            pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
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
          pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(HA_ERR_END_OF_FILE);
      }
    } else {
      if (!conn->mta_conn_mutex_unlock_later)
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      current->record_num =
        (longlong) mysql_num_rows(current->result);
      result_list->record_num += current->record_num;
      if (
        result_list->internal_limit <= result_list->record_num ||
        result_list->split_read > current->record_num
      ) {
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
        pthread_mutex_unlock(&conn->mta_conn_mutex);
    } else {
      if (!(current->result =
        conn->db_conn->methods->use_result(db_conn)))
        DBUG_RETURN(spider_db_errorno(conn));
      conn->quick_target = spider;
      spider->quick_targets[link_idx] = spider;
      if (!conn->mta_conn_mutex_unlock_later)
        pthread_mutex_unlock(&conn->mta_conn_mutex);
    }
    SPIDER_DB_ROW row;
    if (!(row = mysql_fetch_row(current->result)))
    {
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
    if (!(position = (SPIDER_POSITION *)
      my_malloc(sizeof(SPIDER_POSITION) * (page_size + 1),
      MYF(MY_WME | MY_ZEROFILL)))
    )
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    current->first_position = position;
    do {
      row_size = field_count;
      lengths = current->result->lengths;
      for (roop_count2 = 0; roop_count2 < field_count; roop_count2++)
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
      for (roop_count2 = 0; roop_count2 < field_count; roop_count2++)
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
      roop_count++;
    } while (
      page_size > roop_count &&
      (row = mysql_fetch_row(current->result))
    );
    current->record_num = roop_count;
    result_list->record_num += roop_count;
    if (
      result_list->internal_limit <= result_list->record_num ||
      page_size > roop_count
    ) {
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

  if (db_conn->last_used_con->server_status & SERVER_MORE_RESULTS_EXISTS)
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
  if ((result =
    conn->db_conn->methods->use_result(conn->db_conn)))
    mysql_free_result(result);
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
  if (!spider->select_column_mode)
  {
    if (result_list->keyread)
      error_num = spider_db_fetch_key(spider, buf, table,
        result_list->key_info, result_list);
    else
      error_num = spider_db_fetch_table(spider, buf, table,
        result_list);
  } else
    error_num = spider_db_fetch_minimum_columns(spider, buf, table,
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
  int link_idx,
  TABLE *table
) {
  int error_num, tmp_pos;
  String *sql;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn = spider->conns[link_idx];
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_db_seek_next");
  if (result_list->current_row_num >= result_list->current->record_num)
  {
    if (result_list->low_mem_read)
      spider_db_free_one_result(result_list,
        (SPIDER_RESULT*) result_list->current);

    int roop_start, roop_end, roop_count, lock_mode, link_ok;
    lock_mode = spider_conn_lock_mode(spider);
    if (lock_mode)
    {
      /* "for update" or "lock in share mode" */
      link_ok = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_OK);
      roop_start = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
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
          roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
      ) {
        if ((error_num = spider_bg_conn_search(spider, roop_count, FALSE,
          (roop_count != link_ok))))
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
            (
              !result_list->use_union &&
              (error_num = spider_db_append_select_lock(spider))
            )
          )
            DBUG_RETURN(error_num);

          for (roop_count = roop_start; roop_count < roop_end;
            roop_count = spider_conn_link_idx_next(share->link_statuses,
              roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
          ) {
            conn = spider->conns[roop_count];
            if (spider->share->same_db_table_name)
              sql = &result_list->sql;
            else {
              sql = &result_list->sqls[roop_count];
              if (sql->copy(result_list->sql))
                DBUG_RETURN(HA_ERR_OUT_OF_MEM);
              tmp_pos = sql->length();
              sql->length(result_list->table_name_pos);
              spider_db_append_table_name_with_adjusting(sql, spider->share,
                roop_count);
              sql->length(tmp_pos);
            }
            pthread_mutex_lock(&conn->mta_conn_mutex);
            conn->need_mon = &spider->need_mons[roop_count];
            conn->mta_conn_mutex_lock_already = TRUE;
            conn->mta_conn_mutex_unlock_later = TRUE;
            if ((error_num = spider_db_set_names(spider, conn, roop_count)))
            {
              conn->mta_conn_mutex_lock_already = FALSE;
              conn->mta_conn_mutex_unlock_later = FALSE;
              pthread_mutex_unlock(&conn->mta_conn_mutex);
              if (
                share->monitoring_kind[roop_count] &&
                spider->need_mons[roop_count]
              ) {
                error_num = spider_ping_table_mon_from_table(
                    spider->trx,
                    spider->trx->thd,
                    share,
                    share->monitoring_sid[roop_count],
                    share->table_name,
                    share->table_name_length,
                    roop_count,
                    "",
                    0,
                    share->monitoring_kind[roop_count],
                    share->monitoring_limit[roop_count],
                    TRUE
                  );
              }
              DBUG_RETURN(error_num);
            }
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
                    share->monitoring_sid[roop_count],
                    share->table_name,
                    share->table_name_length,
                    roop_count,
                    "",
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
                      share->monitoring_sid[roop_count],
                      share->table_name,
                      share->table_name_length,
                      roop_count,
                      "",
                      0,
                      share->monitoring_kind[roop_count],
                      share->monitoring_limit[roop_count],
                      TRUE
                    );
                }
                DBUG_RETURN(error_num);
              }
            } else {
              spider_db_discard_result(conn);
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
  String *sql;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
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
        -1, share->link_count, SPIDER_LINK_STATUS_OK);
      roop_start = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_end = spider->share->link_count;
    } else {
      link_ok = link_idx;
      roop_start = link_idx;
      roop_end = link_idx + 1;
    }
    for (roop_count = roop_start; roop_count < roop_end;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      conn = spider->conns[roop_count];
      if (spider->share->same_db_table_name)
        sql = &result_list->sql;
      else {
        sql = &result_list->sqls[roop_count];
        if (sql->copy(result_list->sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_pos = sql->length();
        sql->length(result_list->table_name_pos);
        spider_db_append_table_name_with_adjusting(sql, spider->share,
          roop_count);
        sql->length(tmp_pos);
      }
      pthread_mutex_lock(&conn->mta_conn_mutex);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
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
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
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
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      } else {
        spider_db_discard_result(conn);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      }
    }
    result_list->current_row_num = result_list->current->record_num - 1;
    if (result_list->quick_mode == 0)
      result_list->current->result->data_cursor =
        result_list->current->first_row + result_list->current_row_num;
    DBUG_RETURN(spider_db_fetch(buf, spider, table));
  } else {
    if ((error_num = spider_db_free_result(spider, FALSE)))
      DBUG_RETURN(error_num);
    result_list->sql.length(result_list->order_pos);
    result_list->desc_flg = !(result_list->desc_flg);
    result_list->limit_num =
      result_list->internal_limit >= result_list->split_read ?
      result_list->split_read : result_list->internal_limit;
    if (
      (error_num = spider_db_append_key_order(spider)) ||
      (error_num = spider_db_append_limit(
        &result_list->sql, result_list->internal_offset,
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
        -1, share->link_count, SPIDER_LINK_STATUS_OK);
      roop_start = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_end = spider->share->link_count;
    } else {
      link_ok = link_idx;
      roop_start = link_idx;
      roop_end = link_idx + 1;
    }
    for (roop_count = roop_start; roop_count < roop_end;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      conn = spider->conns[roop_count];
      if (spider->share->same_db_table_name)
        sql = &result_list->sql;
      else {
        sql = &result_list->sqls[roop_count];
        if (sql->copy(result_list->sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_pos = sql->length();
        sql->length(result_list->table_name_pos);
        spider_db_append_table_name_with_adjusting(sql, spider->share,
          roop_count);
        sql->length(tmp_pos);
      }
      pthread_mutex_lock(&conn->mta_conn_mutex);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
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
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
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
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      } else {
        spider_db_discard_result(conn);
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      }
    }
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
  pos->mrr_with_cnt = spider->mrr_with_cnt;
  pos->position_bitmap = spider->position_bitmap;
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
  ulong *lengths = pos->lengths;
  Field **field;
  SPIDER_DB_ROW row = pos->row;
  my_ptrdiff_t ptr_diff = PTR_BYTE_DIFF(buf, table->record[0]);
  DBUG_ENTER("spider_db_seek_tmp_table");
  /* for mrr */
  if (spider->mrr_with_cnt)
  {
    DBUG_PRINT("info", ("spider mrr_with_cnt"));
    row++;
    lengths++;
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
  /* for mrr */
  if (spider->mrr_with_cnt)
  {
    DBUG_PRINT("info", ("spider mrr_with_cnt"));
    row++;
    lengths++;
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
/*
  DBUG_ASSERT(spider->position_bitmap_init);
*/
  /* for mrr */
  if (pos->mrr_with_cnt)
  {
    DBUG_PRINT("info", ("spider mrr_with_cnt"));
    row++;
    lengths++;
  }

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
  my_bool not_used_my_bool;
  int not_used_int;
  long not_used_long;
  DBUG_ENTER("spider_db_show_table_status");
  DBUG_PRINT("info",("spider sts_mode=%d", sts_mode));
  if (sts_mode == 1)
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = &spider->need_mons[link_idx];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    if (
      (error_num = spider_db_set_names(spider, conn, link_idx)) ||
      (
        spider_db_query(
          conn,
          share->show_table_status[0 + (2 * link_idx)].ptr(),
          share->show_table_status[0 + (2 * link_idx)].length(),
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
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        if ((error_num = spider_db_set_names(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        if (spider_db_query(
          conn,
          share->show_table_status[0 + (2 * link_idx)].ptr(),
          share->show_table_status[0 + (2 * link_idx)].length(),
          &spider->need_mons[link_idx])
        ) {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          DBUG_RETURN(spider_db_errorno(conn));
        }
      } else {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
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
          spider->share->db_names_str[link_idx].ptr(),
          spider->share->table_names_str[link_idx].ptr());
        DBUG_RETURN(ER_SPIDER_REMOTE_TABLE_NOT_FOUND_NUM);
      }
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
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
      share->auto_increment_value = 1;
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
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = &spider->need_mons[link_idx];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
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
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        if ((error_num = spider_db_set_names(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
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
      share->auto_increment_value = 1;
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
  int link_idx,
  TABLE *table,
  int crd_mode
) {
  int error_num;
  SPIDER_CONN *conn = spider->conns[link_idx];
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
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = &spider->need_mons[link_idx];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
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
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        if ((error_num = spider_db_set_names(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
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
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
      /* no record is ok */
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    pthread_mutex_unlock(&conn->mta_conn_mutex);
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
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = &spider->need_mons[link_idx];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
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
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
        if ((error_num = spider_db_set_names(spider, conn, link_idx)))
        {
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          DBUG_RETURN(error_num);
        }
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
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
      /* no record is ok */
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
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
  ha_spider *spider,
  int link_idx
) {
  int error_num;
  SPIDER_CONN *conn = spider->conns[link_idx];
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->sqls[link_idx];
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
  conn->need_mon = &spider->need_mons[link_idx];
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  if (
    (error_num = spider_db_set_names(spider, conn, link_idx)) ||
    (
      spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        &spider->need_mons[link_idx]) &&
      (my_errno = spider_db_errorno(conn))
    )
  ) {
    if (
      error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM &&
      !conn->disable_reconnect
    ) {
      /* retry */
      if ((my_errno = spider_db_ping(spider, conn, link_idx)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(my_errno);
      }
      if ((error_num = spider_db_set_names(spider, conn, link_idx)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
      if (spider_db_query(
        conn,
        str->ptr(),
        str->length(),
        &spider->need_mons[link_idx])
      ) {
        my_errno = spider_db_errorno(conn);
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(HA_POS_ERROR);
      }
    } else {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
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
      my_errno = error_num;
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(HA_POS_ERROR);
    } else {
      my_errno = ER_QUERY_ON_FOREIGN_DATA_SOURCE;
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      DBUG_RETURN(HA_POS_ERROR);
    }
  }
  conn->mta_conn_mutex_lock_already = FALSE;
  conn->mta_conn_mutex_unlock_later = FALSE;
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
  }
  mysql_free_result(res);
  DBUG_RETURN(rows);
}

int spider_db_bulk_insert_init(
  ha_spider *spider,
  const TABLE *table
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  String *str = &spider->result_list.insert_sql;
  DBUG_ENTER("spider_db_bulk_insert_init");
  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_count < share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
  )
    spider->conns[roop_count]->ignore_dup_key = spider->ignore_dup_key;
  str->length(0);
  spider->result_list.insert_pos = 0;
  if (
    (error_num = spider_db_append_insert(spider)) ||
    (error_num = spider_db_append_into(spider, table, 0))
  )
    DBUG_RETURN(error_num);
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
  String *str = &result_list->insert_sql, *sql;
  Field **field;
  THD *thd = spider->trx->thd;
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
      if (
        bitmap_is_set(table->write_set, (*field)->field_index) ||
        bitmap_is_set(table->read_set, (*field)->field_index)
      ) {
        add_value = TRUE;
        if (
          (*field)->is_null() ||
          (
            table->next_number_field == *field &&
            !table->auto_increment_field_not_null &&
            !spider->force_auto_increment
          )
        ) {
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
    int roop_count2;
    SPIDER_CONN *conn;
    for (
      roop_count2 = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count2 < share->link_count;
      roop_count2 = spider_conn_link_idx_next(share->link_statuses,
        roop_count2, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      conn = spider->conns[roop_count2];
      if (share->same_db_table_name)
        sql = str;
      else {
        sql = &result_list->insert_sqls[roop_count2];
        if (sql->copy(result_list->insert_sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_pos = sql->length();
        sql->length(result_list->insert_table_name_pos);
        spider_db_append_table_name_with_adjusting(sql, share, roop_count2);
        sql->length(tmp_pos);
      }
      pthread_mutex_lock(&conn->mta_conn_mutex);
      conn->need_mon = &spider->need_mons[roop_count2];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count2)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count2] &&
          spider->need_mons[roop_count2]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[roop_count2],
              share->table_name,
              share->table_name_length,
              roop_count2,
              "",
              0,
              share->monitoring_kind[roop_count2],
              share->monitoring_limit[roop_count2],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
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
              share->monitoring_sid[roop_count2],
              share->table_name,
              share->table_name_length,
              roop_count2,
              "",
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
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      if (first_insert_link_idx == -1)
        first_insert_link_idx = roop_count2;
    }

    conn = spider->conns[first_insert_link_idx];
    pthread_mutex_lock(&conn->mta_conn_mutex);
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
      table->next_number_field->set_notnull();
      if (
        (error_num = spider_db_update_auto_increment(spider,
          first_insert_link_idx)) ||
        (error_num = table->next_number_field->store(
          conn->db_conn->last_used_con->insert_id, TRUE))
      ) {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
  DBUG_RETURN(0);
}

int spider_db_update_auto_increment(
  ha_spider *spider,
  int link_idx
) {
  THD *thd = spider->trx->thd;
  ulonglong last_insert_id;
  SPIDER_SHARE *share = spider->share;
  TABLE *table = spider->get_table();
  SPIDER_DB_CONN *last_used_con =
    spider->conns[link_idx]->db_conn->last_used_con;
  DBUG_ENTER("spider_db_update_auto_increment");
  last_insert_id = last_used_con->insert_id;
  share->auto_increment_value =
    last_insert_id + last_used_con->affected_rows;
  thd->record_first_successful_insert_id_in_cur_stmt(last_insert_id);
  if (
    table->s->next_number_keypart == 0 &&
    mysql_bin_log.is_open() &&
    !thd->current_stmt_binlog_row_based
  ) {
    int auto_increment_mode = THDVAR(thd, auto_increment_mode) == -1 ?
      share->auto_increment_mode : THDVAR(thd, auto_increment_mode);
    if (auto_increment_mode == 2)
    {
      if (
        table->file != spider &&
        thd->auto_inc_intervals_in_cur_stmt_for_binlog.nb_elements() > 0
      ) {
        DBUG_PRINT("info",("spider table partitioning"));
        Discrete_interval *current =
          thd->auto_inc_intervals_in_cur_stmt_for_binlog.get_current();
        current->replace(last_insert_id, last_used_con->affected_rows, 1);
      } else {
        DBUG_PRINT("info",("spider table"));
        thd->auto_inc_intervals_in_cur_stmt_for_binlog.append(
          last_insert_id, last_used_con->affected_rows, 1);
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
  String *str = &result_list->update_sql, *sql;
  TABLE **tmp_table = result_list->upd_tmp_tbls;
  DBUG_ENTER("spider_db_bulk_update_size_limit");

  if (result_list->bulk_update_mode == 1)
  {
    /* execute bulk updating */
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
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
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
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
#ifdef MARIADB_BASE_VERSION
      VOID(tmp_table[roop_count]->file->ha_end_bulk_insert(TRUE));
#else
      VOID(tmp_table[roop_count]->file->ha_end_bulk_insert());
#endif
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
  String tmp_str(buf, MAX_FIELD_WIDTH, result_list->update_sql.charset()),
    tmp_str2(buf2, MAX_FIELD_WIDTH, &my_charset_bin), *sql;
  TABLE **tmp_table = result_list->upd_tmp_tbls;
  bool is_error = thd->main_da.is_error();
  DBUG_ENTER("spider_db_bulk_update_end");

  if (result_list->upd_tmp_tbl)
  {
    if (
#ifdef MARIADB_BASE_VERSION
      (error_num2 =
        result_list->upd_tmp_tbl->file->ha_end_bulk_insert(is_error))
#else
      (error_num2 = result_list->upd_tmp_tbl->file->ha_end_bulk_insert())
#endif
    ) {
      error_num = error_num2;
    }

    for (roop_count = 0; roop_count < share->link_count; roop_count++)
    {
      if (
        tmp_table[roop_count] &&
#ifdef MARIADB_BASE_VERSION
        (error_num2 =
          tmp_table[roop_count]->file->ha_end_bulk_insert(is_error))
#else
        (error_num2 = tmp_table[roop_count]->file->ha_end_bulk_insert())
#endif
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

      for (roop_count = 0; roop_count < share->link_count; roop_count++)
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
        result_list->upd_tmp_tbl->field[0]->val_str(&tmp_str);
        for (
          roop_count = spider_conn_link_idx_next(share->link_statuses,
            -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
          roop_count < share->link_count;
          roop_count = spider_conn_link_idx_next(share->link_statuses,
            roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
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
            tmp_table[roop_count]->field[0]->val_str(&tmp_str2);
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
          -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
        roop_count < share->link_count;
        roop_count = spider_conn_link_idx_next(share->link_statuses,
          roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
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
  String *str = &result_list->update_sql, *sql;
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
  if (str->length() >= bulk_update_size)
    size_over = TRUE;

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_count < share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
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
        spider->pk_update &&
        share->link_statuses[roop_count] == SPIDER_LINK_STATUS_RECOVERY
      ) {
        if ((error_num = spider_db_append_insert_for_recovery(sql, spider,
          table, roop_count)))
          DBUG_RETURN(error_num);
        if (sql->reserve(SPIDER_SQL_SEMICOLON_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        sql->q_append(SPIDER_SQL_SEMICOLON_STR, SPIDER_SQL_SEMICOLON_LEN);
      }
      if (
        (error_num = spider_db_append_update(sql, spider, 0)) ||
        (error_num = spider_db_append_update_set(sql, spider, table)) ||
        (error_num = spider_db_append_update_where(
          sql, spider, table, ptr_diff))
      )
        DBUG_RETURN(error_num);
      if (sql->length() >= bulk_update_size)
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
  String *str = &result_list->update_sql, *sql, *insert_sql;
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
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_count < share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
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
      spider_db_append_table_name_with_adjusting(sql, share,
        roop_count);
      sql->length(tmp_pos);
    }
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = &spider->need_mons[roop_count];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    if ((error_num = spider_db_set_names(spider, conn, roop_count)))
    {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      if (
        share->monitoring_kind[roop_count] &&
        spider->need_mons[roop_count]
      ) {
        error_num = spider_ping_table_mon_from_table(
            spider->trx,
            spider->trx->thd,
            share,
            share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            roop_count,
            "",
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
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
            share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            roop_count,
            "",
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }

    if (
      !conn->db_conn->last_used_con->affected_rows &&
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
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        DBUG_RETURN(error_num);
      }
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
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
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
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    result_list->update_sqls[roop_count].length(0);
  }
  str->length(0);
  DBUG_RETURN(0);
}

int spider_db_bulk_delete(
  ha_spider *spider,
  TABLE *table,
  my_ptrdiff_t ptr_diff
) {
  int error_num, roop_count;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->update_sql, *sql;
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
  if (str->length() >= bulk_update_size)
    size_over = TRUE;

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_count < share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
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
      if (sql->length() >= bulk_update_size)
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
  String *str = &result_list->update_sql, *sql;
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
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_count < share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
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
      spider_db_append_table_name_with_adjusting(sql, share,
        roop_count);
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

int spider_db_delete_all_rows(
  ha_spider *spider
) {
  int error_num, roop_count, tmp_pos;
  SPIDER_SHARE *share = spider->share;
  SPIDER_CONN *conn;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  String *str = &result_list->update_sql, *sql;
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
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_count < share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
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
      spider_db_append_table_name_with_adjusting(sql, share,
        roop_count);
      sql->length(tmp_pos);
    }
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = &spider->need_mons[roop_count];
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
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
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          if (
            share->monitoring_kind[roop_count] &&
            spider->need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                spider->trx,
                spider->trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
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
          pthread_mutex_unlock(&conn->mta_conn_mutex);
          if (
            share->monitoring_kind[roop_count] &&
            spider->need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                spider->trx,
                spider->trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
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
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
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
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
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
  String *str;
  DBUG_ENTER("spider_db_disable_keys");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_disable_keys(spider, roop_count)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
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
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
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
  String *str;
  DBUG_ENTER("spider_db_enable_keys");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_enable_keys(spider, roop_count)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
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
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
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
  String *str;
  DBUG_ENTER("spider_db_check_table");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_check_table(spider, roop_count,
        check_opt)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
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
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
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
  String *str;
  DBUG_ENTER("spider_db_repair_table");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_repair_table(spider, roop_count,
        check_opt)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
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
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
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
  String *str;
  DBUG_ENTER("spider_db_analyze_table");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_analyze_table(spider, roop_count)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
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
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
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
  String *str;
  DBUG_ENTER("spider_db_optimize_table");
  if (
    THDVAR(spider->trx->thd, internal_optimize) == 1 ||
    (THDVAR(spider->trx->thd, internal_optimize) == -1 &&
      spider->share->internal_optimize == 1)
  ) {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      str = &result_list->sqls[roop_count];
      str->length(0);
      if ((error_num = spider_db_append_optimize_table(spider, roop_count)))
        DBUG_RETURN(error_num);

      conn = spider->conns[roop_count];
      pthread_mutex_lock(&conn->mta_conn_mutex);
      conn->need_mon = &spider->need_mons[roop_count];
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(spider, conn, roop_count)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
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
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
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
  String *str = &result_list->sql;
  DBUG_ENTER("spider_db_flush_tables");
  str->length(0);
  if ((error_num = spider_db_append_flush_tables(spider, lock)))
    DBUG_RETURN(error_num);

  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_count < share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
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
            share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            roop_count,
            "",
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
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
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_count < share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
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
            share->monitoring_sid[roop_count],
            share->table_name,
            share->table_name_length,
            roop_count,
            "",
            0,
            share->monitoring_kind[roop_count],
            share->monitoring_limit[roop_count],
            TRUE
          );
      }
      DBUG_RETURN(error_num);
    }
    pthread_mutex_unlock(&conn->mta_conn_mutex);
  }
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
    case Item::STRING_ITEM:
      DBUG_RETURN(spider_db_open_item_string(item, spider, str));
    case Item::INT_ITEM:
    case Item::REAL_ITEM:
    case Item::DECIMAL_ITEM:
      DBUG_RETURN(spider_db_open_item_int(item, spider, str));
    case Item::SUBSELECT_ITEM:
    case Item::TRIGGER_FIELD_ITEM:
    case Item::CACHE_ITEM:
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
      if (
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
      if (
        error_num == ER_SPIDER_COND_SKIP_NUM &&
        item_cond->functype() == Item_func::COND_AND_FUNC
      ) {
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
  uint roop_count, item_count = item_func->argument_count();
  char *func_name = SPIDER_SQL_NULL_CHAR_STR,
    *separete_str = SPIDER_SQL_NULL_CHAR_STR,
    *last_str = SPIDER_SQL_NULL_CHAR_STR;
  int func_name_length = SPIDER_SQL_NULL_CHAR_LEN,
    separete_str_length = SPIDER_SQL_NULL_CHAR_LEN,
    last_str_length = SPIDER_SQL_NULL_CHAR_LEN;
  int use_pushdown_udf;
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
      } else if (func_name_length == 4 &&
        !strncasecmp("rand", func_name, func_name_length) &&
        !item_func->arg_count
      ) {
        str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
        DBUG_RETURN(spider_db_open_item_int(item_func, spider, str));
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
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          DBUG_RETURN(spider_db_open_item_string(item_func, spider, str));
        }
      } else if (func_name_length == 8 &&
        (
          !strncasecmp("utc_date", func_name, func_name_length) ||
          !strncasecmp("utc_time", func_name, func_name_length)
        )
      ) {
        str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
        DBUG_RETURN(spider_db_open_item_string(item_func, spider, str));
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
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          if (str->reserve(SPIDER_SQL_CAST_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          last_str = SPIDER_SQL_AS_DATE_STR;
          last_str_length = SPIDER_SQL_AS_DATE_LEN;
          break;
        } else if (!strncasecmp("cast_as_time", func_name, func_name_length))
        {
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          if (str->reserve(SPIDER_SQL_CAST_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          last_str = SPIDER_SQL_AS_TIME_STR;
          last_str_length = SPIDER_SQL_AS_TIME_LEN;
          break;
        }
      } else if (func_name_length == 13 &&
        !strncasecmp("utc_timestamp", func_name, func_name_length)
      ) {
        str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
        DBUG_RETURN(spider_db_open_item_string(item_func, spider, str));
      } else if (func_name_length == 14)
      {
        if (!strncasecmp("cast_as_binary", func_name, func_name_length))
        {
          char tmp_buf[MAX_FIELD_WIDTH], *tmp_ptr, *tmp_ptr2;
          String tmp_str(tmp_buf, MAX_FIELD_WIDTH, str->charset());
          tmp_str.length(0);
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          if (str->reserve(SPIDER_SQL_CAST_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          item_func->print(&tmp_str, QT_IS);
          if (tmp_str.reserve(1))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          tmp_ptr = tmp_str.c_ptr_quick();
          DBUG_PRINT("info",("spider tmp_ptr = %s", tmp_ptr));
          while (tmp_ptr2 = strstr(tmp_ptr, SPIDER_SQL_AS_BINARY_STR))
            tmp_ptr = tmp_ptr2 + 1;
          last_str = tmp_ptr - 1;
          last_str_length = strlen(last_str) - SPIDER_SQL_CLOSE_PAREN_LEN;
          break;
        } else if (!strncasecmp("cast_as_signed", func_name, func_name_length))
        {
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          if (str->reserve(SPIDER_SQL_CAST_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          last_str = SPIDER_SQL_AS_SIGNED_STR;
          last_str_length = SPIDER_SQL_AS_SIGNED_LEN;
          break;
        }
      } else if (func_name_length == 16)
      {
        if (!strncasecmp("cast_as_unsigned", func_name, func_name_length))
        {
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          if (str->reserve(SPIDER_SQL_CAST_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          last_str = SPIDER_SQL_AS_UNSIGNED_STR;
          last_str_length = SPIDER_SQL_AS_UNSIGNED_LEN;
          break;
        } else if (!strncasecmp("decimal_typecast", func_name, func_name_length))
        {
          char tmp_buf[MAX_FIELD_WIDTH], *tmp_ptr, *tmp_ptr2;
          String tmp_str(tmp_buf, MAX_FIELD_WIDTH, str->charset());
          tmp_str.length(0);
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          if (str->reserve(SPIDER_SQL_CAST_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          item_func->print(&tmp_str, QT_IS);
          if (tmp_str.reserve(1))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          tmp_ptr = tmp_str.c_ptr_quick();
          DBUG_PRINT("info",("spider tmp_ptr = %s", tmp_ptr));
          while (tmp_ptr2 = strstr(tmp_ptr, SPIDER_SQL_AS_DECIMAL_STR))
            tmp_ptr = tmp_ptr2 + 1;
          last_str = tmp_ptr - 1;
          last_str_length = strlen(last_str) - SPIDER_SQL_CLOSE_PAREN_LEN;
          break;
        } else if (!strncasecmp("cast_as_datetime", func_name, func_name_length))
        {
          str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
          if (str->reserve(SPIDER_SQL_CAST_LEN))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
          last_str = SPIDER_SQL_AS_DATETIME_STR;
          last_str_length = SPIDER_SQL_AS_DATETIME_LEN;
          break;
        }
      }
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
    case Item_func::NOW_FUNC:
      str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
      DBUG_RETURN(spider_db_open_item_string(item_func, spider, str));
    case Item_func::CHAR_TYPECAST_FUNC:
      {
        char tmp_buf[MAX_FIELD_WIDTH], *tmp_ptr, *tmp_ptr2;
        String tmp_str(tmp_buf, MAX_FIELD_WIDTH, str->charset());
        tmp_str.length(0);
        str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
        if (str->reserve(SPIDER_SQL_CAST_LEN))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        str->q_append(SPIDER_SQL_CAST_STR, SPIDER_SQL_CAST_LEN);
        item_func->print(&tmp_str, QT_IS);
        if (tmp_str.reserve(1))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_ptr = tmp_str.c_ptr_quick();
        DBUG_PRINT("info",("spider tmp_ptr = %s", tmp_ptr));
        while (tmp_ptr2 = strstr(tmp_ptr, SPIDER_SQL_AS_CHAR_STR))
          tmp_ptr = tmp_ptr2 + 1;
        last_str = tmp_ptr - 1;
        last_str_length = strlen(last_str) - SPIDER_SQL_CLOSE_PAREN_LEN;
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
      use_pushdown_udf = THDVAR(spider->trx->thd, use_pushdown_udf) == -1 ?
        spider->share->use_pushdown_udf :
        THDVAR(spider->trx->thd, use_pushdown_udf);
      if (!use_pushdown_udf)
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
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
    case Item_func::TRIG_COND_FUNC:
      DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    case Item_func::GUSERVAR_FUNC:
      str->length(str->length() - SPIDER_SQL_OPEN_PAREN_LEN);
      if (item_func->result_type() == STRING_RESULT)
        DBUG_RETURN(spider_db_open_item_string(item_func, spider, str));
      else
        DBUG_RETURN(spider_db_open_item_int(item_func, spider, str));
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
  if (item_count)
  {
    item_count--;
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
  }
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
#ifdef HANDLER_HAS_TOP_TABLE_FIELDS
    if (spider->set_top_table_fields)
    {
      Field *field = spider->top_table_field[item_ident->cached_field_index];
      if (str->reserve(
        share->column_name_str[field->field_index].length() +
        (SPIDER_SQL_NAME_QUOTE_LEN) * 2))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      spider_db_append_column_name(share, str, field->field_index);
    } else {
#endif
      if (str->reserve(
        share->column_name_str[item_ident->cached_field_index].length() +
        (SPIDER_SQL_NAME_QUOTE_LEN) * 2))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      spider_db_append_column_name(share, str,
        item_ident->cached_field_index);
#ifdef HANDLER_HAS_TOP_TABLE_FIELDS
    }
#endif
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
  Field *field = item_field->field;
  SPIDER_SHARE *share;
  DBUG_ENTER("spider_db_open_item_field");
  if (field)
  {
#ifdef HANDLER_HAS_TOP_TABLE_FIELDS
    if (spider->set_top_table_fields)
    {
      if (field->table != spider->top_table)
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
      if (!(field = spider->top_table_field[field->field_index]))
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
    } else {
#endif
      if (field->table != spider->get_table())
        DBUG_RETURN(ER_SPIDER_COND_SKIP_NUM);
#ifdef HANDLER_HAS_TOP_TABLE_FIELDS
    }
#endif
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

int spider_db_open_item_string(
  Item *item,
  ha_spider *spider,
  String *str
) {
  char tmp_buf[MAX_FIELD_WIDTH];
  String tmp_str(tmp_buf, MAX_FIELD_WIDTH, str->charset()), *tmp_str2;
  DBUG_ENTER("spider_db_open_item_string");
  if (
    !(tmp_str2 = item->val_str(&tmp_str)) ||
    str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN * 2 + tmp_str2->length())
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  if (
    str->append(*tmp_str2) ||
    str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN)
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  DBUG_RETURN(0);
}

int spider_db_open_item_int(
  Item *item,
  ha_spider *spider,
  String *str
) {
  char tmp_buf[MAX_FIELD_WIDTH];
  String tmp_str(tmp_buf, MAX_FIELD_WIDTH, str->charset());
  DBUG_ENTER("spider_db_open_item_int");
  if (str->append(*item->val_str(&tmp_str)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
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

int spider_db_udf_fetch_table(
  SPIDER_TRX *trx,
  TABLE *table,
  SPIDER_DB_RESULT *result,
  uint set_on,
  uint set_off
) {
  int error_num;
  SPIDER_DB_ROW row;
  ulong *lengths;
  Field **field;
  uint roop_count;
  DBUG_ENTER("spider_db_udf_fetch_table");
  if (!(row = mysql_fetch_row(result)))
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  lengths = mysql_fetch_lengths(result);

#ifndef DBUG_OFF
  my_bitmap_map *tmp_map =
    dbug_tmp_use_all_columns(table, table->write_set);
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
  uint net_timeout;
  THD* thd = current_thd;
  longlong connect_retry_interval;
  my_bool connect_mutex = spider_connect_mutex;
  DBUG_ENTER("spider_db_udf_direct_sql_connect");

  if (thd)
  {
    net_timeout = THDVAR(thd, net_timeout) == -1 ?
      direct_sql->net_timeout : THDVAR(thd, net_timeout);
    connect_retry_interval = THDVAR(thd, connect_retry_interval);
    connect_retry_count = THDVAR(thd, connect_retry_count);
  } else {
    net_timeout = direct_sql->net_timeout;
    connect_retry_interval = 0;
    connect_retry_count = 0;
  }
  DBUG_PRINT("info",("spider net_timeout=%u", net_timeout));

  if ((error_num = spider_reset_conn_setted_parameter(conn)))
    DBUG_RETURN(error_num);

  while (TRUE)
  {
    if (!(conn->db_conn = mysql_init(NULL)))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);

    mysql_options(conn->db_conn, MYSQL_OPT_READ_TIMEOUT,
      &net_timeout);
    mysql_options(conn->db_conn, MYSQL_OPT_CONNECT_TIMEOUT,
      &net_timeout);

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
      DBUG_PRINT("info",("spider tgt_default_file=%s", conn->tgt_default_file));
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
      my_sleep(connect_retry_interval);
    } else {
      if (connect_mutex)
        pthread_mutex_unlock(&spider_open_conn_mutex);
      break;
    }
  }
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
  if ((error_num = simple_command(conn->db_conn, COM_PING, 0, 0, 0)))
  {
    spider_db_disconnect(conn);
    if ((error_num = spider_db_udf_direct_sql_connect(direct_sql, conn)))
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

int spider_db_udf_direct_sql(
  SPIDER_DIRECT_SQL *direct_sql
) {
  int error_num = 0, status, roop_count = 0, need_mon = 0;
  uint udf_table_mutex_index, field_num, set_on, set_off;
  long long roop_count2;
  bool end_of_file;
  SPIDER_TRX *trx = direct_sql->trx;
  THD *thd = trx->thd, *c_thd = current_thd;
  SPIDER_CONN *conn = direct_sql->conn;
  SPIDER_DB_RESULT *result;
  TABLE *table;
  int bulk_insert_rows = THDVAR(thd, udf_ds_bulk_insert_rows) <= 0 ?
    direct_sql->bulk_insert_rows : THDVAR(thd, udf_ds_bulk_insert_rows);
  int table_loop_mode = THDVAR(thd, udf_ds_table_loop_mode) == -1 ?
    direct_sql->table_loop_mode : THDVAR(thd, udf_ds_table_loop_mode);
  double ping_interval_at_trx_start =
    THDVAR(thd, ping_interval_at_trx_start);
  time_t tmp_time = (time_t) time((time_t*) 0);
  bool need_trx_end, insert_start = FALSE;
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

  if (
    !(error_num = spider_db_udf_direct_sql_set_names(direct_sql, trx, conn)) &&
    !(error_num = spider_db_udf_direct_sql_select_db(direct_sql, conn))
  ) {
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
        if ((result = conn->db_conn->methods->use_result(conn->db_conn)))
        {
          end_of_file = FALSE;
          if (roop_count >= 0)
          {
            while (!error_num && !end_of_file)
            {
              udf_table_mutex_index = spider_udf_calc_hash(
                direct_sql->db_names[roop_count],
                spider_udf_table_lock_mutex_count);
              udf_table_mutex_index += spider_udf_calc_hash(
                direct_sql->table_names[roop_count],
                spider_udf_table_lock_mutex_count);
              udf_table_mutex_index %= spider_udf_table_lock_mutex_count;
              pthread_mutex_lock(
                &trx->udf_table_mutexes[udf_table_mutex_index]);
              table = direct_sql->tables[roop_count];
              table->in_use = c_thd;
              memset((uchar *) table->null_flags, ~(uchar) 0,
                sizeof(uchar) * table->s->null_bytes);
              insert_start = TRUE;

              field_num = mysql_num_fields(result);
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
                  trx, table, result, set_on, set_off)))
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

#ifdef MARIADB_BASE_VERSION
              if (error_num)
                VOID(table->file->ha_end_bulk_insert(TRUE));
              else
                error_num = table->file->ha_end_bulk_insert(FALSE);
#else
              if (error_num)
                VOID(table->file->ha_end_bulk_insert());
              else
                error_num = table->file->ha_end_bulk_insert();
#endif
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
          mysql_free_result(result);
        } else {
          if ((error_num = spider_db_errorno(conn)))
          {
            if (error_num == ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM)
              my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
                ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
            break;
          }
        }
        if ((status = spider_db_next_result(conn)) > 0)
        {
          error_num = status;
          break;
        }
        if (roop_count >= 0)
          roop_count++;
      } while (status == 0);
      conn->mta_conn_mutex_unlock_later = FALSE;
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
  if (!conn->mta_conn_mutex_lock_already)
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = &need_mon;
  }
  if (
   !db_conn->db ||
   strcmp(direct_sql->tgt_default_db_name, db_conn->db)
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
    conn->mta_conn_mutex_lock_already = tmp_mta_conn_mutex_lock_already;
  }
  if (!conn->mta_conn_mutex_unlock_later)
    pthread_mutex_unlock(&conn->mta_conn_mutex);
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
  if (!conn->mta_conn_mutex_lock_already)
  {
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = &need_mon;
  }
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
    pthread_mutex_unlock(&conn->mta_conn_mutex);
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
  ha_spider spider;
  DBUG_ENTER("spider_db_udf_ping_table");
  if (!pthread_mutex_trylock(&table_mon_list->monitor_mutex))
  {
    spider.share = share;
    spider.trx = trx;
    spider.need_mons = &need_mon;
    pthread_mutex_lock(&conn->mta_conn_mutex);
    conn->need_mon = &need_mon;
    conn->mta_conn_mutex_lock_already = TRUE;
    conn->mta_conn_mutex_unlock_later = TRUE;
    if ((error_num = spider_db_ping(&spider, conn, 0)))
    {
      conn->mta_conn_mutex_lock_already = FALSE;
      conn->mta_conn_mutex_unlock_later = FALSE;
      pthread_mutex_unlock(&conn->mta_conn_mutex);
      table_mon_list->last_mon_result = error_num;
      pthread_mutex_unlock(&table_mon_list->monitor_mutex);
      my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
        share->server_names[0]);
      DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
    }
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    if (!ping_only)
    {
      int init_sql_alloc_size =
        THDVAR(trx->thd, init_sql_alloc_size) < 0 ?
        share->init_sql_alloc_size :
        THDVAR(trx->thd, init_sql_alloc_size);
#ifdef _MSC_VER
      String sql_str(init_sql_alloc_size);
      sql_str.set_charset(system_charset_info);
      String where_str(init_sql_alloc_size);
      where_str.set_charset(system_charset_info);
#else
      char sql_buf[init_sql_alloc_size], where_buf[init_sql_alloc_size];
      String sql_str(sql_buf, sizeof(sql_buf),
        system_charset_info);
      String where_str(where_buf, sizeof(where_buf),
        system_charset_info);
#endif
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
      conn->need_mon = &need_mon;
      conn->mta_conn_mutex_lock_already = TRUE;
      conn->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(&spider, conn, 0)))
      {
        conn->mta_conn_mutex_lock_already = FALSE;
        conn->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conn->mta_conn_mutex);
        table_mon_list->last_mon_result = error_num;
        pthread_mutex_unlock(&table_mon_list->monitor_mutex);
        DBUG_PRINT("info",("spider error_num=%d", error_num));
        DBUG_RETURN(error_num);
      }
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
  String *str,
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
  String child_table_name_str(child_table_name, child_table_name_length + 1,
    str->charset());
  String where_clause_str(where_clause, where_clause_length + 1,
    str->charset());
  DBUG_ENTER("spider_db_udf_ping_table_append_mon_next");
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
  append_escaped(str, &child_table_name_str);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->qs_append(link_id);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->qs_append(flags);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->q_append(limit_str, limit_str_length);
  str->q_append(SPIDER_SQL_COMMA_STR, SPIDER_SQL_COMMA_LEN);
  str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
  append_escaped(str, &where_clause_str);
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
  String *str,
  SPIDER_SHARE *share,
  SPIDER_TRX *trx,
  String *where_str,
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
    append_escaped(str, where_str);
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
  SPIDER_DB_RESULT *res;
  SPIDER_DB_ROW row;
  SPIDER_SHARE *share = table_mon->share;
  int init_sql_alloc_size =
    THDVAR(thd, init_sql_alloc_size) < 0 ?
    share->init_sql_alloc_size :
    THDVAR(thd, init_sql_alloc_size);
#ifdef _MSC_VER
  String sql_str(init_sql_alloc_size);
  sql_str.set_charset(thd->variables.character_set_client);
#else
  char sql_buf[init_sql_alloc_size];
  String sql_str(sql_buf, sizeof(sql_buf),
    thd->variables.character_set_client);
#endif
  ha_spider spider;
  SPIDER_TRX trx;
  DBUG_ENTER("spider_db_udf_ping_table_mon_next");
  sql_str.length(0);
  trx.thd = thd;
  spider.share = share;
  spider.trx = &trx;
  spider.need_mons = &need_mon;

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
  conn->need_mon = &need_mon;
  conn->mta_conn_mutex_lock_already = TRUE;
  conn->mta_conn_mutex_unlock_later = TRUE;
  if ((error_num = spider_db_ping(&spider, conn, 0)))
  {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
      share->server_names[0]);
    DBUG_RETURN(ER_CONNECT_TO_FOREIGN_DATA_SOURCE);
  }
  if ((error_num = spider_db_set_names(&spider, conn, 0)))
  {
    conn->mta_conn_mutex_lock_already = FALSE;
    conn->mta_conn_mutex_unlock_later = FALSE;
    pthread_mutex_unlock(&conn->mta_conn_mutex);
    DBUG_RETURN(error_num);
  }
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
  String *str,
  String *source_str,
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
  String *str,
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
    String tmp_str(*row, *length + 1, str->charset());
    tmp_str.length(*length);
    if (str->reserve(SPIDER_SQL_VALUE_QUOTE_LEN * 2 + *length * 2 + 2 +
      SPIDER_SQL_COMMA_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    str->q_append(SPIDER_SQL_VALUE_QUOTE_STR, SPIDER_SQL_VALUE_QUOTE_LEN);
    append_escaped(str, &tmp_str);
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
  String *str,
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
  int error_num, roop_count, roop_count2;
  bool end_of_file = FALSE;
  SPIDER_DB_ROW row;
  ulong *lengths, *last_lengths, *last_row_pos = NULL;
  ha_spider *tmp_spider;
  SPIDER_CONN *tmp_conn;
  int all_link_cnt =
    copy_tables->link_idx_count[0] + copy_tables->link_idx_count[1];
  SPIDER_COPY_TABLE_CONN *src_tbl_conn = copy_tables->table_conn[0];
  SPIDER_COPY_TABLE_CONN *dst_tbl_conn;
  String *select_sql = &src_tbl_conn->select_sql;
  String *insert_sql;
  KEY *key_info = &table->key_info[table->s->primary_key];
  KEY_PART_INFO *key_part = key_info->key_part;
  int bulk_insert_interval;
  DBUG_ENTER("spider_db_udf_copy_tables");
  if (!(last_row_pos = (ulong *)
    my_multi_malloc(MYF(MY_WME),
      &last_row_pos, sizeof(ulong) * table->s->fields,
      &last_lengths, sizeof(ulong) * table->s->fields,
      NullS))
  ) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    goto error;
  }
  while (!end_of_file)
  {
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
        if (my_hash_insert(&tmp_conn->lock_table_hash,
          (uchar*) tmp_spider))
        {
          my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
          error_num = ER_OUT_OF_RESOURCES;
          goto error_lock_table_hash;
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
          (error_num = spider_db_set_names(tmp_spider, tmp_conn, 0)) ||
          (error_num = spider_db_lock_tables(tmp_spider, 0))
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
                pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
                goto error_db_query;
              }
              select_sql->length(select_sql->length() - SPIDER_SQL_AND_LEN);
              if (select_sql->reserve(SPIDER_SQL_CLOSE_PAREN_LEN +
                SPIDER_SQL_OR_LEN + SPIDER_SQL_OPEN_PAREN_LEN))
              {
                my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
                mysql_free_result(result);
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
            bulk_insert_rows = spider_udf_ct_bulk_insert_rows > 0 ?
              spider_udf_ct_bulk_insert_rows : copy_tables->bulk_insert_rows;
            if (
              spider_db_append_key_order_str(select_sql, key_info, 0, FALSE) ||
              spider_db_append_limit(select_sql, 0, bulk_insert_rows) ||
              (
                copy_tables->use_transaction &&
                spider_db_append_select_lock_str(select_sql,
                  SPIDER_LOCK_MODE_SHARED)
              )
            ) {
              my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
              mysql_free_result(result);
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
          pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
          goto error_db_query;
        }
        mysql_free_result(result);
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
          } else
            pthread_mutex_unlock(&tmp_conn->mta_conn_mutex);
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
      bulk_insert_interval = spider_udf_ct_bulk_insert_interval >= 0 ?
        spider_udf_ct_bulk_insert_interval : copy_tables->bulk_insert_interval;
      my_sleep(bulk_insert_interval);
    }
  }
  my_free(last_row_pos, MYF(0));
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
    my_free(last_row_pos, MYF(0));
  DBUG_RETURN(error_num);
}
