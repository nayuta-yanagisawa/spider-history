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
#include "spd_err.h"
#include "spd_param.h"
#include "spd_db_include.h"
#include "spd_include.h"
#include "spd_sys_table.h"
#include "ha_spider.h"
#include "spd_trx.h"
#include "spd_db_conn.h"
#include "spd_table.h"
#include "spd_conn.h"

extern handlerton *spider_hton_ptr;
volatile ulonglong spider_thread_id = 1;

// for spider_alter_tables
uchar *spider_alter_tbl_get_key(
  SPIDER_ALTER_TABLE *alter_table,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
) {
  DBUG_ENTER("spider_alter_tbl_get_key");
  *length = alter_table->table_name_length;
  DBUG_PRINT("info",("spider table_name_length=%d", *length));
  DBUG_PRINT("info",("spider table_name=%s", alter_table->table_name));
  DBUG_RETURN((uchar*) alter_table->table_name);
}

int spider_free_trx_conn(
  SPIDER_TRX *trx,
  bool trx_free
) {
  int roop_count = 0;
  SPIDER_CONN *conn;
  DBUG_ENTER("spider_free_trx_conn");
  if(
    THDVAR(trx->thd, conn_recycle_mode) != 2 ||
    trx_free
  ) {
    while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_conn_hash,
      roop_count)))
    {
      spider_free_conn_from_trx(trx, conn, FALSE, trx_free, &roop_count);
    }
    trx->trx_conn_adjustment++;
  }
  DBUG_RETURN(0);
}

int spider_free_trx_another_conn(
  SPIDER_TRX *trx,
  bool lock
) {
  int error_num, tmp_error_num;
  int roop_count = 0;
  SPIDER_CONN *conn;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_free_trx_another_conn");
  error_num = 0;
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_another_conn_hash,
    roop_count)))
  {
    tmp_spider.conn = conn;
    if (lock && (tmp_error_num = spider_db_unlock_tables(&tmp_spider)))
      error_num = tmp_error_num;
    spider_free_conn_from_trx(trx, conn, TRUE, TRUE, &roop_count);
  }
  DBUG_RETURN(error_num);
}

int spider_trx_another_lock_tables(
  SPIDER_TRX *trx
) {
  int error_num;
  int roopCount = 0;
  SPIDER_CONN *conn;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_trx_another_lock_tables");
  memset(&tmp_spider, 0, sizeof(ha_spider));
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_another_conn_hash,
    roopCount)))
  {
    tmp_spider.conn = conn;
    if ((error_num = spider_db_lock_tables(&tmp_spider)))
      DBUG_RETURN(error_num);
    roopCount++;
  }
  DBUG_RETURN(0);
}

int spider_trx_another_flush_tables(
  SPIDER_TRX *trx
) {
  int error_num;
  int roopCount = 0;
  SPIDER_CONN *conn;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_trx_another_flush_tables");
  memset(&tmp_spider, 0, sizeof(ha_spider));
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_another_conn_hash,
    roopCount)))
  {
    tmp_spider.conn = conn;
    if ((error_num = spider_db_flush_tables(&tmp_spider, FALSE)))
      DBUG_RETURN(error_num);
    roopCount++;
  }
  DBUG_RETURN(0);
}

int spider_trx_all_flush_tables(
  SPIDER_TRX *trx
) {
  int error_num;
  int roopCount = 0;
  SPIDER_CONN *conn;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_trx_all_flush_tables");
  memset(&tmp_spider, 0, sizeof(ha_spider));
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_conn_hash,
    roopCount)))
  {
    tmp_spider.conn = conn;
    if ((error_num = spider_db_flush_tables(&tmp_spider, TRUE)))
      DBUG_RETURN(error_num);
    roopCount++;
  }
  DBUG_RETURN(0);
}

int spider_trx_all_unlock_tables(
  SPIDER_TRX *trx
) {
  int error_num;
  int roopCount = 0;
  SPIDER_CONN *conn;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_trx_all_unlock_tables");
  memset(&tmp_spider, 0, sizeof(ha_spider));
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_conn_hash,
    roopCount)))
  {
    tmp_spider.conn = conn;
    if ((error_num = spider_db_unlock_tables(&tmp_spider)))
      DBUG_RETURN(error_num);
    roopCount++;
  }
  DBUG_RETURN(0);
}

int spider_trx_all_start_trx(
  SPIDER_TRX *trx
) {
  int error_num;
  int roopCount = 0;
  SPIDER_CONN *conn;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_trx_all_start_trx");
  memset(&tmp_spider, 0, sizeof(ha_spider));
  tmp_spider.trx = trx;
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_conn_hash,
    roopCount)))
  {
    tmp_spider.conn = conn;
    if (
      (THDVAR(trx->thd, sync_trx_isolation) &&
        (error_num = spider_check_and_set_trx_isolation(conn))) ||
      (error_num = spider_internal_start_trx(&tmp_spider))
    )
      DBUG_RETURN(error_num);
    roopCount++;
  }
  DBUG_RETURN(0);
}

int spider_trx_all_flush_logs(
  SPIDER_TRX *trx
) {
  int error_num;
  int roopCount = 0;
  SPIDER_CONN *conn;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_trx_all_flush_logs");
  memset(&tmp_spider, 0, sizeof(ha_spider));
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_conn_hash,
    roopCount)))
  {
    tmp_spider.conn = conn;
    if ((error_num = spider_db_flush_logs(&tmp_spider)))
      DBUG_RETURN(error_num);
    roopCount++;
  }
  DBUG_RETURN(0);
}

void spider_free_trx_alter_table_alloc(
  SPIDER_TRX *trx,
  SPIDER_ALTER_TABLE *alter_table
) {
  DBUG_ENTER("spider_free_trx_alter_table_alloc");
  hash_delete(&trx->trx_alter_table_hash, (uchar*) alter_table);
  if (alter_table->tmp_char)
    my_free(alter_table->tmp_char, MYF(0));
  my_free(alter_table, MYF(0));
  DBUG_VOID_RETURN;
}

int spider_free_trx_alter_table(
  SPIDER_TRX *trx
) {
  SPIDER_ALTER_TABLE *alter_table;
  DBUG_ENTER("spider_free_trx_alter_table");
  while ((alter_table =
    (SPIDER_ALTER_TABLE*) hash_element(&trx->trx_alter_table_hash, 0)))
  {
    spider_free_trx_alter_table_alloc(trx, alter_table);
  }
  DBUG_RETURN(0);
}

int spider_create_trx_alter_table(
  SPIDER_TRX *trx,
  SPIDER_SHARE *share
) {
  int error_num;
  SPIDER_ALTER_TABLE *alter_table, *share_alter;
  char *tmp_name;
  char *tmp_server_name;
  char *tmp_tgt_table_name;
  char *tmp_tgt_db;
  char *tmp_tgt_host;
  char *tmp_tgt_username;
  char *tmp_tgt_password;
  char *tmp_tgt_socket;
  char *tmp_tgt_wrapper;
  DBUG_ENTER("spider_create_trx_alter_table");
  share_alter = &share->alter_table;
  if (!(alter_table = (SPIDER_ALTER_TABLE *)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &alter_table, sizeof(*alter_table),
      &tmp_name, share->table_name_length + 1,
      &tmp_server_name,
        (share_alter->tmp_server_name ?
          share_alter->tmp_server_name_length + 1 : 0),
      &tmp_tgt_table_name,
        (share_alter->tmp_tgt_table_name ?
          share_alter->tmp_tgt_table_name_length + 1 : 0),
      &tmp_tgt_db,
        (share_alter->tmp_tgt_db ?
          share_alter->tmp_tgt_db_length + 1 : 0),
      &tmp_tgt_host,
        (share_alter->tmp_tgt_host ?
          share_alter->tmp_tgt_host_length + 1 : 0),
      &tmp_tgt_username,
        (share_alter->tmp_tgt_username ?
          share_alter->tmp_tgt_username_length + 1 : 0),
      &tmp_tgt_password,
        (share_alter->tmp_tgt_password ?
          share_alter->tmp_tgt_password_length + 1 : 0),
      &tmp_tgt_socket,
        (share_alter->tmp_tgt_socket ?
          share_alter->tmp_tgt_socket_length + 1 : 0),
      &tmp_tgt_wrapper,
        (share_alter->tmp_tgt_wrapper ?
          share_alter->tmp_tgt_wrapper_length + 1 : 0),
      NullS))
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_alloc_alter_table;
  }
  alter_table->table_name = tmp_name;
  memcpy(alter_table->table_name, share->table_name, share->table_name_length);
  alter_table->table_name_length = share->table_name_length;
  alter_table->tmp_priority = share->priority;
  if (share_alter->tmp_server_name)
  {
    alter_table->tmp_server_name = tmp_server_name;
    memcpy(alter_table->tmp_server_name,
      share_alter->tmp_server_name, share_alter->tmp_server_name_length);
  }
  if (share_alter->tmp_tgt_table_name)
  {
    alter_table->tmp_tgt_table_name = tmp_tgt_table_name;
    memcpy(alter_table->tmp_tgt_table_name,
      share_alter->tmp_tgt_table_name, share_alter->tmp_tgt_table_name_length);
  }
  if (share_alter->tmp_tgt_db)
  {
    alter_table->tmp_tgt_db = tmp_tgt_db;
    memcpy(alter_table->tmp_tgt_db,
      share_alter->tmp_tgt_db, share_alter->tmp_tgt_db_length);
  }
  if (share_alter->tmp_tgt_host)
  {
    alter_table->tmp_tgt_host = tmp_tgt_host;
    memcpy(alter_table->tmp_tgt_host,
      share_alter->tmp_tgt_host, share_alter->tmp_tgt_host_length);
  }
  if (share_alter->tmp_tgt_username)
  {
    alter_table->tmp_tgt_username = tmp_tgt_username;
    memcpy(alter_table->tmp_tgt_username,
      share_alter->tmp_tgt_username, share_alter->tmp_tgt_username_length);
  }
  if (share_alter->tmp_tgt_password)
  {
    alter_table->tmp_tgt_password = tmp_tgt_password;
    memcpy(alter_table->tmp_tgt_password,
      share_alter->tmp_tgt_password, share_alter->tmp_tgt_password_length);
  }
  if (share_alter->tmp_tgt_socket)
  {
    alter_table->tmp_tgt_socket = tmp_tgt_socket;
    memcpy(alter_table->tmp_tgt_socket,
      share_alter->tmp_tgt_socket, share_alter->tmp_tgt_socket_length);
  }
  if (share_alter->tmp_tgt_wrapper)
  {
    alter_table->tmp_tgt_wrapper = tmp_tgt_wrapper;
    memcpy(alter_table->tmp_tgt_wrapper,
      share_alter->tmp_tgt_wrapper, share_alter->tmp_tgt_wrapper_length);
  }
  alter_table->tmp_tgt_port = share_alter->tmp_tgt_port;
  alter_table->tmp_server_name_length = share_alter->tmp_server_name_length;
  alter_table->tmp_tgt_table_name_length =
    share_alter->tmp_tgt_table_name_length;
  alter_table->tmp_tgt_db_length = share_alter->tmp_tgt_db_length;
  alter_table->tmp_tgt_host_length = share_alter->tmp_tgt_host_length;
  alter_table->tmp_tgt_username_length = share_alter->tmp_tgt_username_length;
  alter_table->tmp_tgt_password_length = share_alter->tmp_tgt_password_length;
  alter_table->tmp_tgt_socket_length = share_alter->tmp_tgt_socket_length;
  alter_table->tmp_tgt_wrapper_length = share_alter->tmp_tgt_wrapper_length;
  if (my_hash_insert(&trx->trx_alter_table_hash, (uchar*) alter_table))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error;
  }
  DBUG_RETURN(0);

error:
  my_free(alter_table, MYF(0));
error_alloc_alter_table:
  DBUG_RETURN(error_num);
}

bool spider_cmp_trx_alter_table(
  SPIDER_ALTER_TABLE *cmp1,
  SPIDER_ALTER_TABLE *cmp2
) {
  DBUG_ENTER("spider_cmp_trx_alter_table");
  if (
    cmp1->tmp_priority != cmp2->tmp_priority ||
    (
      cmp1->tmp_server_name != cmp2->tmp_server_name &&
      (
        !cmp1->tmp_server_name ||
        !cmp2->tmp_server_name ||
        strcmp(cmp1->tmp_server_name, cmp2->tmp_server_name)
      )
    ) ||
    (
      cmp1->tmp_tgt_table_name != cmp2->tmp_tgt_table_name &&
      (
        !cmp1->tmp_tgt_table_name ||
        !cmp2->tmp_tgt_table_name ||
        strcmp(cmp1->tmp_tgt_table_name, cmp2->tmp_tgt_table_name)
      )
    ) ||
    (
      cmp1->tmp_tgt_db != cmp2->tmp_tgt_db &&
      (
        !cmp1->tmp_tgt_db ||
        !cmp2->tmp_tgt_db ||
        strcmp(cmp1->tmp_tgt_db, cmp2->tmp_tgt_db)
      )
    ) ||
    (
      cmp1->tmp_tgt_host != cmp2->tmp_tgt_host &&
      (
        !cmp1->tmp_tgt_host ||
        !cmp2->tmp_tgt_host ||
        strcmp(cmp1->tmp_tgt_host, cmp2->tmp_tgt_host)
      )
    ) ||
    (
      cmp1->tmp_tgt_username != cmp2->tmp_tgt_username &&
      (
        !cmp1->tmp_tgt_username ||
        !cmp2->tmp_tgt_username ||
        strcmp(cmp1->tmp_tgt_username, cmp2->tmp_tgt_username)
      )
    ) ||
    (
      cmp1->tmp_tgt_password != cmp2->tmp_tgt_password &&
      (
        !cmp1->tmp_tgt_password ||
        !cmp2->tmp_tgt_password ||
        strcmp(cmp1->tmp_tgt_password, cmp2->tmp_tgt_password)
      )
    ) ||
    (
      cmp1->tmp_tgt_socket != cmp2->tmp_tgt_socket &&
      (
        !cmp1->tmp_tgt_socket ||
        !cmp2->tmp_tgt_socket ||
        strcmp(cmp1->tmp_tgt_socket, cmp2->tmp_tgt_socket)
      )
    ) ||
    (
      cmp1->tmp_tgt_wrapper != cmp2->tmp_tgt_wrapper &&
      (
        !cmp1->tmp_tgt_wrapper ||
        !cmp2->tmp_tgt_wrapper ||
        strcmp(cmp1->tmp_tgt_wrapper, cmp2->tmp_tgt_wrapper)
      )
    ) ||
    cmp1->tmp_tgt_port != cmp2->tmp_tgt_port
  )
    DBUG_RETURN(TRUE);
  DBUG_RETURN(FALSE);
}

int spider_free_trx_alloc(
  SPIDER_TRX *trx
) {
  DBUG_ENTER("spider_free_trx_alloc");
  spider_free_trx_conn(trx, TRUE);
  spider_free_trx_alter_table(trx);
  hash_free(&trx->trx_conn_hash);
  hash_free(&trx->trx_another_conn_hash);
  hash_free(&trx->trx_alter_table_hash);
  DBUG_RETURN(0);
}

SPIDER_TRX *spider_get_trx(
  const THD *thd,
  int *error_num
) {
  SPIDER_TRX *trx;
  DBUG_ENTER("spider_get_trx");

  if (!(trx = (SPIDER_TRX*) thd_get_ha_data(thd, spider_hton_ptr)))
  {
    DBUG_PRINT("info",("spider create new trx"));
    if (!(trx = (SPIDER_TRX *)
          my_malloc(sizeof(*trx), MYF(MY_WME | MY_ZEROFILL)))
    )
      goto error_alloc_trx;

    if (
      hash_init(&trx->trx_conn_hash, system_charset_info, 32, 0, 0,
                   (hash_get_key) spider_conn_get_key, 0, 0)
    )
      goto error_init_hash;

    if (
      hash_init(&trx->trx_another_conn_hash, system_charset_info, 32, 0, 0,
                   (hash_get_key) spider_conn_get_key, 0, 0)
    )
      goto error_init_another_hash;

    if (
      hash_init(&trx->trx_alter_table_hash, system_charset_info, 32, 0, 0,
                   (hash_get_key) spider_alter_tbl_get_key, 0, 0)
    )
      goto error_init_alter_hash;

    trx->thd = (THD*) thd;
    trx->spider_thread_id = spider_thread_id++;
    trx->trx_conn_adjustment = 1;

    thd_set_ha_data(thd, spider_hton_ptr, trx);
  }

  DBUG_PRINT("info",("spider trx=%x", trx));
  DBUG_RETURN(trx);

error_init_alter_hash:
  hash_free(&trx->trx_another_conn_hash);
error_init_another_hash:
  hash_free(&trx->trx_conn_hash);
error_init_hash:
  my_free(trx, MYF(0));

error_alloc_trx:
  *error_num = HA_ERR_OUT_OF_MEM;
  DBUG_RETURN(NULL);
}

int spider_free_trx(
  SPIDER_TRX *trx
) {
  DBUG_ENTER("spider_free_trx");
  thd_set_ha_data(trx->thd, spider_hton_ptr, NULL);
  spider_free_trx_alloc(trx);
  my_free(trx, MYF(0));
  DBUG_RETURN(0);
}

int spider_check_and_set_trx_isolation(
  SPIDER_CONN *conn
) {
  int error_num;
  int trx_isolation;
  DBUG_ENTER("spider_check_and_set_trx_isolation");

  trx_isolation = thd_tx_isolation(conn->thd);
  DBUG_PRINT("info",("spider local trx_isolation=%d", trx_isolation));
  DBUG_PRINT("info",("spider conn->trx_isolation=%d", conn->trx_isolation));
  if (conn->trx_isolation != trx_isolation)
  {
    if ((error_num = spider_db_set_trx_isolation(conn, trx_isolation)))
      DBUG_RETURN(error_num);
    conn->trx_isolation = trx_isolation;
  }
  DBUG_RETURN(0);
}

int spider_check_and_set_autocommit(
  THD *thd,
  SPIDER_CONN *conn
) {
  int error_num;
  bool autocommit;
  DBUG_ENTER("spider_check_and_set_autocommit");

  autocommit = !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT);
  if (autocommit && conn->autocommit != 1)
  {
    if ((error_num = spider_db_set_autocommit(conn, TRUE)))
      DBUG_RETURN(error_num);
    conn->autocommit = 1;
  } else if (!autocommit && conn->autocommit != 0)
  {
    if ((error_num = spider_db_set_autocommit(conn, FALSE)))
      DBUG_RETURN(error_num);
    conn->autocommit = 0;
  }
  DBUG_RETURN(0);
}

int spider_check_and_set_sql_log_off(
  THD *thd,
  SPIDER_CONN *conn
) {
  int error_num;
  bool internal_sql_log_off;
  DBUG_ENTER("spider_check_and_set_sql_log_off");

  internal_sql_log_off = THDVAR(thd, internal_sql_log_off);
  if (internal_sql_log_off && conn->sql_log_off != 1)
  {
    if ((error_num = spider_db_set_sql_log_off(conn, TRUE)))
      DBUG_RETURN(error_num);
    conn->sql_log_off = 1;
  } else if (!internal_sql_log_off && conn->sql_log_off != 0)
  {
    if ((error_num = spider_db_set_sql_log_off(conn, FALSE)))
      DBUG_RETURN(error_num);
    conn->sql_log_off = 0;
  }
  DBUG_RETURN(0);
}

int spider_xa_lock(
  XID_STATE *xid_state
) {
  int error_num;
  DBUG_ENTER("spider_xa_lock");
  pthread_mutex_lock(&LOCK_xid_cache);
  if (hash_search(&xid_cache,
    xid_state->xid.key(), xid_state->xid.key_length()))
  {
    error_num = ER_SPIDER_XA_LOCKED_NUM;
    goto error;
  }
  if (my_hash_insert(&xid_cache, (uchar*)xid_state))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error;
  }
  pthread_mutex_unlock(&LOCK_xid_cache);
  DBUG_RETURN(0);

error:
  pthread_mutex_unlock(&LOCK_xid_cache);
  DBUG_RETURN(error_num);
}

int spider_xa_unlock(
  XID_STATE *xid_state
) {
  DBUG_ENTER("spider_xa_unlock");
  pthread_mutex_lock(&LOCK_xid_cache);
  hash_delete(&xid_cache, (uchar *)xid_state);
  pthread_mutex_unlock(&LOCK_xid_cache);
  DBUG_RETURN(0);
}

int spider_start_internal_consistent_snapshot(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn
) {
  int error_num;
  DBUG_ENTER("spider_start_internal_consistent_snapshot");
  if (trx->trx_consistent_snapshot)
    DBUG_RETURN(spider_db_consistent_snapshot(conn));
  DBUG_RETURN(0);
}

int spider_internal_start_trx(
  ha_spider *spider
) {
  int error_num;
  SPIDER_TRX *trx = spider->trx;
  SPIDER_CONN *conn = spider->conn;
  bool sync_autocommit = THDVAR(trx->thd, sync_autocommit);
  bool xa_lock = FALSE;
  DBUG_ENTER("spider_internal_start_trx");

  if ((error_num = spider_db_ping(spider)))
    goto error;
  if (!trx->trx_start)
  {
    if (!trx->trx_consistent_snapshot)
    {
      trx->use_consistent_snapshot = THDVAR(trx->thd, use_consistent_snapshot);
      trx->internal_xa = THDVAR(trx->thd, internal_xa);
      trx->internal_xa_snapshot = THDVAR(trx->thd, internal_xa_snapshot);
    }
  }
  if (
    (error_num = spider_check_and_set_sql_log_off(trx->thd, conn)) ||
    (sync_autocommit &&
      (error_num = spider_check_and_set_autocommit(trx->thd, conn)))
  )
    goto error;
  if (trx->trx_consistent_snapshot)
  {
    if (trx->internal_xa && trx->internal_xa_snapshot < 2)
    {
      error_num = ER_SPIDER_CANT_USE_BOTH_INNER_XA_AND_SNAPSHOT_NUM;
      my_message(error_num, ER_SPIDER_CANT_USE_BOTH_INNER_XA_AND_SNAPSHOT_STR,
        MYF(0));
      goto error;
    } else if (!trx->internal_xa || trx->internal_xa_snapshot == 2)
    {
      if ((error_num = spider_start_internal_consistent_snapshot(trx, conn)))
        goto error;
    }
  }
  if (!trx->trx_start)
  {
    if (
      trx->thd->transaction.xid_state.xa_state == XA_ACTIVE &&
      spider_support_xa
    ) {
      trx->trx_xa = TRUE;
      thd_get_xid(trx->thd, (MYSQL_XID*) &trx->xid);
    }

    if (!trx->trx_xa && trx->internal_xa)
    {
      if (!trx->trx_consistent_snapshot || trx->internal_xa_snapshot == 3)
      {
        trx->trx_xa = TRUE;
        trx->xid.formatID = 1;
        trx->xid.gtrid_length
          = my_sprintf(trx->xid.data,
          (trx->xid.data, "%lx", thd_get_thread_id(trx->thd)));
        trx->xid.bqual_length
          = my_sprintf(trx->xid.data + trx->xid.gtrid_length,
          (trx->xid.data + trx->xid.gtrid_length, "%x",
          trx->thd->server_id));

        trx->internal_xid_state.xa_state = XA_ACTIVE;
        trx->internal_xid_state.xid.set(&trx->xid);
        trx->internal_xid_state.in_thd = 1;
        while ((error_num = spider_xa_lock(&trx->internal_xid_state)))
        {
          if (error_num != ER_SPIDER_XA_LOCKED_NUM)
            goto error;
          else if (trx->xid.formatID == 0)
          {
            my_message(error_num, ER_SPIDER_XA_LOCKED_STR, MYF(0));
            goto error;
          }
          /* retry */
          trx->xid.formatID++;
          trx->internal_xid_state.xid.set(&trx->xid);
        }
        xa_lock = TRUE;
      }
    }

    if (!trx->trx_consistent_snapshot)
    {
      trans_register_ha(trx->thd, FALSE, spider_hton_ptr);
      if (thd_test_options(trx->thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
        trans_register_ha(trx->thd, TRUE, spider_hton_ptr);
    }
    trx->trx_start = TRUE;
  }

  DBUG_PRINT("info",("spider sync_autocommit = %d", sync_autocommit));
  DBUG_PRINT("info",("spider conn->semi_trx_chk = %d", conn->semi_trx_chk));
  DBUG_PRINT("info",("spider conn->table_lock = %d", conn->table_lock));
  DBUG_PRINT("info",("spider conn->autocommit = %d", conn->autocommit));
  DBUG_PRINT("info",("spider semi_trx = %d", THDVAR(trx->thd, semi_trx)));
  conn->semi_trx = FALSE;
  if (conn->table_lock == 3)
  {
    DBUG_PRINT("info",("spider conn->table_lock == 3"));
    conn->disable_xa = TRUE;
  } else if (trx->trx_xa)
  {
    DBUG_PRINT("info",("spider trx->trx_xa"));
    if (
      sync_autocommit &&
      conn->semi_trx_chk &&
      !conn->table_lock &&
      conn->autocommit == 1 &&
      THDVAR(trx->thd, semi_trx)
    ) {
      DBUG_PRINT("info",("spider semi_trx is set"));
      conn->semi_trx = TRUE;
    }
    if ((error_num = spider_db_xa_start(conn, &trx->xid)))
      goto error_start_trx;
    conn->disable_xa = FALSE;
  } else if (
    !trx->trx_consistent_snapshot &&
    !thd_test_options(trx->thd, OPTION_BEGIN) &&
    sync_autocommit &&
    conn->semi_trx_chk &&
    !conn->table_lock &&
    conn->autocommit == 1 &&
    THDVAR(trx->thd, semi_trx)
  ) {
    DBUG_PRINT("info",("spider semi_trx is set"));
    if ((error_num = spider_db_start_transaction(conn)))
      goto error_start_trx;
    conn->semi_trx = TRUE;
  } else if (
    !trx->trx_consistent_snapshot &&
    thd_test_options(trx->thd, OPTION_BEGIN)
  ) {
    DBUG_PRINT("info",("spider start transaction"));
    if ((error_num = spider_db_start_transaction(conn)))
      goto error_start_trx;
  }

  conn->join_trx = 1;
  if (trx->join_trx_top)
    spider_tree_insert(trx->join_trx_top, conn);
  else {
    conn->p_small = NULL;
    conn->p_big = NULL;
    conn->c_small = NULL;
    conn->c_big = NULL;
    trx->join_trx_top = conn;
  }
  DBUG_RETURN(0);

error_start_trx:
error:
  if (xa_lock)
    spider_xa_unlock(&trx->internal_xid_state);
  DBUG_RETURN(error_num);
}

int spider_internal_xa_commit(
  THD* thd,
  SPIDER_TRX *trx,
  XID* xid,
  TABLE *table_xa,
  TABLE *table_xa_member
) {
  int error_num;
  char xa_key[MAX_KEY_LENGTH];
  char xa_member_key[MAX_KEY_LENGTH];
  SPIDER_CONN *conn;
  uint force_commit = THDVAR(thd, force_commit);
  MEM_ROOT mem_root;
  Open_tables_state open_tables_backup;
  bool table_xa_opened = FALSE;
  bool table_xa_member_opened = FALSE;
  DBUG_ENTER("spider_internal_xa_commit");

  /*
    select
      status
    from
      mysql.spider_xa
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if (
    !(table_xa = spider_open_sys_table(
      thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
      TRUE, &open_tables_backup, TRUE, &error_num))
  )
    goto error_open_table;
  table_xa_opened = TRUE;
  spider_store_xa_pk(table_xa, &trx->xid);
  if (
    (error_num = spider_check_sys_table(table_xa, xa_key))
  ) {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table_xa->file->print_error(error_num, MYF(0));
      goto error;
    }
    my_message(ER_SPIDER_XA_NOT_EXISTS_NUM, ER_SPIDER_XA_NOT_EXISTS_STR,
      MYF(0));
    error_num = ER_SPIDER_XA_NOT_EXISTS_NUM;
    goto error;
  }
  init_alloc_root(&mem_root, 4096, 0);
  if (
    force_commit != 2 &&
    (error_num = spider_check_sys_xa_status(
      table_xa,
      SPIDER_SYS_XA_PREPARED_STR,
      SPIDER_SYS_XA_COMMIT_STR,
      NULL,
      ER_SPIDER_XA_NOT_PREPARED_NUM,
      &mem_root))
  ) {
    free_root(&mem_root, MYF(0));
    if (error_num == ER_SPIDER_XA_NOT_PREPARED_NUM)
      my_message(error_num, ER_SPIDER_XA_NOT_PREPARED_STR, MYF(0));
    goto error;
  }
  free_root(&mem_root, MYF(0));

  /*
    update
      mysql.spider_xa
    set
      status = 'COMMIT'
    where
      format_id = trx->xid.format_id and
      gtrid_length = trx->xid.gtrid_length and
      data = trx->xid.data
  */
  if (
    (error_num = spider_update_xa(
      table_xa, &trx->xid, SPIDER_SYS_XA_COMMIT_STR))
  )
    goto error;
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  table_xa_opened = FALSE;

  if ((conn = spider_tree_first(trx->join_trx_top)))
  {
    do {
      if (conn->join_trx)
      {
        if ((error_num = spider_db_xa_commit(conn, &trx->xid)))
        {
          if (force_commit == 0 ||
            (force_commit == 1 && error_num != ER_XAER_NOTA))
            DBUG_RETURN(error_num);
        }
        if ((error_num = spider_end_trx(trx, conn)))
          DBUG_RETURN(error_num);
        conn->join_trx = 0;
      }
    } while ((conn = spider_tree_next(conn)));
    trx->join_trx_top = NULL;
  }

  /*
    delete from
      mysql.spider_xa_member
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if (
    !(table_xa_member = spider_open_sys_table(
      thd, SPIDER_SYS_XA_MEMBER_TABLE_NAME_STR,
      SPIDER_SYS_XA_MEMBER_TABLE_NAME_LEN, TRUE, &open_tables_backup, TRUE,
      &error_num))
  )
    goto error_open_table;
  table_xa_member_opened = TRUE;
  if ((error_num = spider_delete_xa_member(table_xa_member, &trx->xid)))
    goto error;
  spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
  table_xa_member_opened = FALSE;

  /*
    delete from
      mysql.spider_xa
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if (
    !(table_xa = spider_open_sys_table(
      thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
      TRUE, &open_tables_backup, TRUE, &error_num))
  )
    goto error_open_table;
  table_xa_opened = TRUE;
  if ((error_num = spider_delete_xa(table_xa, &trx->xid)))
    goto error;
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  table_xa_opened = FALSE;
  spider_xa_unlock(&trx->internal_xid_state);
  trx->internal_xid_state.xa_state = XA_NOTR;
  DBUG_RETURN(0);

error:
  if (table_xa_opened)
    spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  if (table_xa_member_opened)
    spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
error_open_table:
  DBUG_RETURN(error_num);
}

int spider_internal_xa_rollback(
  THD* thd,
  SPIDER_TRX *trx
) {
  int error_num;
  TABLE *table_xa, *table_xa_member;
  char xa_key[MAX_KEY_LENGTH];
  char xa_member_key[MAX_KEY_LENGTH];
  SPIDER_CONN *conn;
  uint force_commit = THDVAR(thd, force_commit);
  MEM_ROOT mem_root;
  Open_tables_state open_tables_backup;
  bool server_lost = FALSE;
  bool prepared = (thd->transaction.xid_state.xa_state == XA_PREPARED);
  bool table_xa_opened = FALSE;
  bool table_xa_member_opened = FALSE;
  DBUG_ENTER("spider_internal_xa_rollback");

  if (prepared)
  {
    /*
      select
        status
      from
        mysql.spider_xa
      where
        format_id = xid->format_id and
        gtrid_length = xid->gtrid_length and
        data = xid->data
    */
    if (
      !(table_xa = spider_open_sys_table(
        thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
        TRUE, &open_tables_backup, TRUE, &error_num))
    )
      goto error_open_table;
    table_xa_opened = TRUE;
    spider_store_xa_pk(table_xa, &trx->xid);
    if (
      (error_num = spider_check_sys_table(table_xa, xa_key))
    ) {
      if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
      {
        table_xa->file->print_error(error_num, MYF(0));
        goto error;
      }
      my_message(ER_SPIDER_XA_NOT_EXISTS_NUM, ER_SPIDER_XA_NOT_EXISTS_STR,
        MYF(0));
      error_num = ER_SPIDER_XA_NOT_EXISTS_NUM;
      goto error;
    }
    init_alloc_root(&mem_root, 4096, 0);
    if (
      force_commit != 2 &&
      (error_num = spider_check_sys_xa_status(
        table_xa,
        SPIDER_SYS_XA_PREPARED_STR,
        SPIDER_SYS_XA_ROLLBACK_STR,
        NULL,
        ER_SPIDER_XA_NOT_PREPARED_NUM,
        &mem_root))
    ) {
      free_root(&mem_root, MYF(0));
      if (error_num == ER_SPIDER_XA_NOT_PREPARED_NUM)
        my_message(error_num, ER_SPIDER_XA_NOT_PREPARED_STR, MYF(0));
      goto error;
    }
    free_root(&mem_root, MYF(0));

    /*
      update
        mysql.spider_xa
      set
        status = 'COMMIT'
      where
        format_id = trx->xid.format_id and
        gtrid_length = trx->xid.gtrid_length and
        data = trx->xid.data
    */
    if (
      (error_num = spider_update_xa(
        table_xa, &trx->xid, SPIDER_SYS_XA_ROLLBACK_STR))
    )
      goto error;
    spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
    table_xa_opened = FALSE;
  }

  if ((conn = spider_tree_first(trx->join_trx_top)))
  {
    do {
      if (conn->join_trx)
      {
        if (conn->disable_xa)
        {
          if (conn->table_lock != 3 && !prepared)
          {
            if (
              !conn->server_lost &&
              (error_num = spider_db_rollback(conn))
            )
              DBUG_RETURN(error_num);
          }
        } else {
          if (!conn->server_lost)
          {
            if (
              !prepared &&
              (error_num = spider_db_xa_end(conn, &trx->xid))
            ) {
              if (force_commit == 0 ||
                (force_commit == 1 && error_num != ER_XAER_NOTA))
                DBUG_RETURN(error_num);
            }
            if ((error_num = spider_db_xa_rollback(conn, &trx->xid)))
            {
              if (force_commit == 0 ||
                (force_commit == 1 && error_num != ER_XAER_NOTA))
                DBUG_RETURN(error_num);
            }
          }
        }
        if ((error_num = spider_end_trx(trx, conn)))
          DBUG_RETURN(error_num);
        conn->join_trx = 0;
        if (conn->server_lost)
          server_lost = TRUE;
      }
    } while ((conn = spider_tree_next(conn)));
    trx->join_trx_top = NULL;
  }

  if (
    prepared &&
    !server_lost
  ) {
    /*
      delete from
        mysql.spider_xa_member
      where
        format_id = xid->format_id and
        gtrid_length = xid->gtrid_length and
        data = xid->data
    */
    if (
      !(table_xa_member = spider_open_sys_table(
        thd, SPIDER_SYS_XA_MEMBER_TABLE_NAME_STR,
        SPIDER_SYS_XA_MEMBER_TABLE_NAME_LEN, TRUE, &open_tables_backup, TRUE,
        &error_num))
    )
      goto error_open_table;
    table_xa_member_opened = TRUE;
    if ((error_num = spider_delete_xa_member(table_xa_member, &trx->xid)))
      goto error;
    spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
    table_xa_member_opened = FALSE;

    /*
      delete from
        mysql.spider_xa
      where
        format_id = xid->format_id and
        gtrid_length = xid->gtrid_length and
        data = xid->data
    */
    if (
      !(table_xa = spider_open_sys_table(
        thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
        TRUE, &open_tables_backup, TRUE, &error_num))
    )
      goto error_open_table;
    table_xa_opened = TRUE;
    if ((error_num = spider_delete_xa(table_xa, &trx->xid)))
      goto error;
    spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
    table_xa_opened = FALSE;
  }
  spider_xa_unlock(&trx->internal_xid_state);
  trx->internal_xid_state.xa_state = XA_NOTR;
  DBUG_RETURN(0);

error:
  if (table_xa_opened)
    spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  if (table_xa_member_opened)
    spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
error_open_table:
  DBUG_RETURN(error_num);
}

int spider_internal_xa_prepare(
  THD* thd,
  SPIDER_TRX *trx,
  TABLE *table_xa,
  TABLE *table_xa_member,
  bool internal_xa
) {
  char table_xa_key[MAX_KEY_LENGTH];
  int table_xa_key_length;
  int error_num;
  SPIDER_CONN *conn;
  uint force_commit = THDVAR(thd, force_commit);
  Open_tables_state open_tables_backup;
  bool table_xa_opened = FALSE;
  bool table_xa_member_opened = FALSE;
  DBUG_ENTER("spider_internal_xa_prepare");
  /*
    insert into mysql.spider_xa
      (format_id, gtrid_length, bqual_length, data, status) values
      (trx->xid.format_id, trx->xid.gtrid_length, trx->xid.bqual_length,
      trx->xid.data, 'NOT YET')
  */
  if (
    !(table_xa = spider_open_sys_table(
      thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
      TRUE, &open_tables_backup, TRUE, &error_num))
  )
    goto error_open_table;
  table_xa_opened = TRUE;
  if (
    (error_num = spider_insert_xa(
      table_xa, &trx->xid, SPIDER_SYS_XA_NOT_YET_STR))
  )
    goto error;
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  table_xa_opened = FALSE;

  if (
    !(table_xa_member = spider_open_sys_table(
      thd, SPIDER_SYS_XA_MEMBER_TABLE_NAME_STR,
      SPIDER_SYS_XA_MEMBER_TABLE_NAME_LEN, TRUE, &open_tables_backup, TRUE,
      &error_num))
  )
    goto error_open_table;
  table_xa_member_opened = TRUE;
  if ((conn = spider_tree_first(trx->join_trx_top)))
  {
    do {
      if (conn->disable_xa)
      {
        if (conn->table_lock != 3)
        {
          if ((error_num = spider_db_rollback(conn)))
            goto error;
        }
        if ((error_num = spider_end_trx(trx, conn)))
          DBUG_RETURN(error_num);
        conn->join_trx = 0;
      } else {
        /*
          insert into mysql.spider_xa_member
            (format_id, gtrid_length, bqual_length, data,
            scheme, host, port, socket, username, password) values
            (trx->xid.format_id, trx->xid.gtrid_length,
            trx->xid.bqual_length, trx->xid.data,
            conn->tgt_wrapper,
            conn->tgt_host,
            conn->tgt_port,
            conn->tgt_socket,
            conn->tgt_username,
            conn->tgt_password)
        */
        if (
          (error_num = spider_insert_xa_member(
            table_xa_member, &trx->xid, conn))
        )
          goto error;

        if ((error_num = spider_db_xa_end(conn, &trx->xid)))
        {
          if (force_commit == 0 ||
            (force_commit == 1 && error_num != ER_XAER_NOTA))
            goto error;
        }
        if ((error_num = spider_db_xa_prepare(conn, &trx->xid)))
        {
          if (force_commit == 0 ||
            (force_commit == 1 && error_num != ER_XAER_NOTA))
            goto error;
        }
/*
        if (!internal_xa)
        {
          if ((error_num = spider_end_trx(trx, conn)))
            DBUG_RETURN(error_num);
          conn->join_trx = 0;
        }
*/
      }
    } while ((conn = spider_tree_next(conn)));
/*
    if (!internal_xa)
      trx->join_trx_top = NULL;
*/
  }
  spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
  table_xa_member_opened = FALSE;

  /*
    update
      mysql.spider_xa
    set
      status = 'PREPARED'
    where
      format_id = trx->xid.format_id and
      gtrid_length = trx->xid.gtrid_length and
      data = trx->xid.data
  */
  if (
    !(table_xa = spider_open_sys_table(
      thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
      TRUE, &open_tables_backup, TRUE, &error_num))
  )
    goto error_open_table;
  table_xa_opened = TRUE;
  if (
    (error_num = spider_update_xa(
      table_xa, &trx->xid, SPIDER_SYS_XA_PREPARED_STR))
  )
    goto error;
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  table_xa_opened = FALSE;
  if (internal_xa)
    trx->internal_xid_state.xa_state = XA_PREPARED;
  DBUG_RETURN(0);

error:
  if (table_xa_opened)
    spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  if (table_xa_member_opened)
    spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
error_open_table:
  DBUG_RETURN(error_num);
}

int spider_internal_xa_recover(
  THD* thd,
  XID* xid_list,
  uint len
) {
  TABLE *table_xa;
  int cnt = 0;
  char xa_key[MAX_KEY_LENGTH];
  MEM_ROOT mem_root;
  Open_tables_state open_tables_backup;
  DBUG_ENTER("spider_internal_xa_recover");
  /*
    select
      format_id,
      gtrid_length,
      bqual_length,
      data
    from
      mysql.spider_xa
    where
      status = 'PREPARED'
  */
  if (
    !(table_xa = spider_open_sys_table(
      thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
      FALSE, &open_tables_backup, TRUE, &my_errno))
  )
    goto error_open_table;
  spider_store_xa_status(table_xa, SPIDER_SYS_XA_PREPARED_STR);
  if (
    (my_errno = spider_get_sys_table_by_idx(table_xa, xa_key, 1,
    SPIDER_SYS_XA_IDX1_COL_CNT))
  ) {
    spider_sys_index_end(table_xa);
    if (my_errno != HA_ERR_KEY_NOT_FOUND && my_errno != HA_ERR_END_OF_FILE)
    {
      table_xa->file->print_error(my_errno, MYF(0));
      goto error;
    }
    goto error;
  }

  init_alloc_root(&mem_root, 4096, 0);
  do {
    spider_get_sys_xid(table_xa, &xid_list[cnt], &mem_root);
    cnt++;
    my_errno = spider_sys_index_next_same(table_xa, xa_key);
  } while (my_errno == 0 && cnt < len);
  free_root(&mem_root, MYF(0));
  spider_sys_index_end(table_xa);
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  DBUG_RETURN(cnt);

error:
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
error_open_table:
  DBUG_RETURN(0);
}

int spider_initinal_xa_recover(
  XID* xid_list,
  uint len
) {
  int error_num;
  static THD *thd = NULL;
  static TABLE *table_xa = NULL;
  static READ_RECORD *read_record = NULL;
  static Open_tables_state *open_tables_backup = NULL;
  int cnt = 0;
  MEM_ROOT mem_root;
  DBUG_ENTER("spider_initinal_xa_recover");
  if (!open_tables_backup)
  {
    if (!(open_tables_backup = new Open_tables_state))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_create_state;
    }
  }
  if (!read_record)
  {
    if (!(read_record = new READ_RECORD))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_create_read_record;
    }
  }

  if (!thd)
  {
    if (!(thd = spider_create_tmp_thd()))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_create_thd;
    }
  }

  /*
    select
      format_id,
      gtrid_length,
      bqual_length,
      data
    from
      mysql.spider_xa
  */
  if (!table_xa)
  {
    if (
      !(table_xa = spider_open_sys_table(
        thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
        FALSE, open_tables_backup, TRUE, &error_num))
    )
      goto error_open_table;
    init_read_record(read_record, thd, table_xa, NULL, TRUE, FALSE, FALSE);
  }
  init_alloc_root(&mem_root, 4096, 0);
  while ((!(read_record->read_record(read_record))) && cnt < len)
  {
    spider_get_sys_xid(table_xa, &xid_list[cnt], &mem_root);
    cnt++;
  }
  free_root(&mem_root, MYF(0));

  if (cnt < len)
  {
    end_read_record(read_record);
    spider_close_sys_table(thd, table_xa, open_tables_backup, TRUE);
    table_xa = NULL;
    spider_free_tmp_thd(thd);
    thd = NULL;
    delete read_record;
    read_record = NULL;
    delete open_tables_backup;
    open_tables_backup = NULL;
  }
  DBUG_RETURN(cnt);

/*
error:
  end_read_record(&read_record_info);
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  table_xa = NULL;
*/
error_open_table:
  spider_free_tmp_thd(thd);
  thd = NULL;
error_create_thd:
  delete read_record;
  read_record = NULL;
error_create_read_record:
  delete open_tables_backup;
  open_tables_backup = NULL;
error_create_state:
  DBUG_RETURN(0);
}

int spider_internal_xa_commit_by_xid(
  THD* thd,
  SPIDER_TRX *trx,
  XID* xid
) {
  TABLE *table_xa, *table_xa_member;
  int error_num;
  char xa_key[MAX_KEY_LENGTH];
  char xa_member_key[MAX_KEY_LENGTH];
  SPIDER_SHARE tmp_share;
  SPIDER_CONN *conn;
  uint force_commit = THDVAR(thd, force_commit);
  MEM_ROOT mem_root;
  Open_tables_state open_tables_backup;
  bool table_xa_opened = FALSE;
  bool table_xa_member_opened = FALSE;
  DBUG_ENTER("spider_internal_xa_commit_by_xid");
  /*
    select
      status
    from
      mysql.spider_xa
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if (
    !(table_xa = spider_open_sys_table(
      thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
      TRUE, &open_tables_backup, TRUE, &error_num))
  )
    goto error_open_table;
  table_xa_opened = TRUE;
  spider_store_xa_pk(table_xa, xid);
  if (
    (error_num = spider_check_sys_table(table_xa, xa_key))
  ) {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table_xa->file->print_error(error_num, MYF(0));
      goto error;
    }
    my_message(ER_SPIDER_XA_NOT_EXISTS_NUM, ER_SPIDER_XA_NOT_EXISTS_STR,
      MYF(0));
    error_num = ER_SPIDER_XA_NOT_EXISTS_NUM;
    goto error;
  }
  init_alloc_root(&mem_root, 4096, 0);
  if (
    force_commit != 2 &&
    (error_num = spider_check_sys_xa_status(
      table_xa,
      SPIDER_SYS_XA_PREPARED_STR,
      SPIDER_SYS_XA_COMMIT_STR,
      NULL,
      ER_SPIDER_XA_NOT_PREPARED_NUM,
      &mem_root))
  ) {
    free_root(&mem_root, MYF(0));
    if (error_num == ER_SPIDER_XA_NOT_PREPARED_NUM)
      my_message(error_num, ER_SPIDER_XA_NOT_PREPARED_STR, MYF(0));
    goto error;
  }

  /*
    update
      mysql.spider_xa
    set
      status = 'COMMIT'
    where
      format_id = trx->xid.format_id and
      gtrid_length = trx->xid.gtrid_length and
      data = trx->xid.data
  */
  if (
    (error_num = spider_update_xa(
      table_xa, xid, SPIDER_SYS_XA_COMMIT_STR))
  ) {
    free_root(&mem_root, MYF(0));
    goto error;
  }
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  table_xa_opened = FALSE;

  /*
    select
      scheme tmp_share.tgt_wrapper,
      host tmp_share.tgt_host,
      port tmp_share.tgt_port,
      socket tmp_share.tgt_socket,
      username tmp_share.tgt_username,
      password tmp_share.tgt_password
    from
      mysql.spider_xa_member
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if (
    !(table_xa_member = spider_open_sys_table(
      thd, SPIDER_SYS_XA_MEMBER_TABLE_NAME_STR,
      SPIDER_SYS_XA_MEMBER_TABLE_NAME_LEN, TRUE, &open_tables_backup, TRUE,
      &error_num))
  ) {
    free_root(&mem_root, MYF(0));
    goto error_open_table;
  }
  table_xa_member_opened = TRUE;
  spider_store_xa_pk(table_xa_member, xid);
  if (
    (error_num = spider_get_sys_table_by_idx(table_xa_member, xa_member_key, 0,
    SPIDER_SYS_XA_PK_COL_CNT))
  ) {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      free_root(&mem_root, MYF(0));
      table_xa_member->file->print_error(error_num, MYF(0));
      goto error;
    } else {
      free_root(&mem_root, MYF(0));
      spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
      table_xa_member_opened = FALSE;
      goto xa_delete;
    }
  }

  memset(&tmp_share, 0, sizeof(SPIDER_SHARE));
  tmp_share.csname = (char*) table_xa_member->s->table_charset->csname;
  tmp_share.csname_length = strlen(tmp_share.csname);
  do {
    spider_get_sys_server_info(table_xa_member, &tmp_share, &mem_root);
    if ((error_num = spider_create_conn_key(&tmp_share)))
    {
      spider_sys_index_end(table_xa_member);
      free_root(&mem_root, MYF(0));
      goto error;
    }

    if (
      (!(conn = spider_get_conn(
      &tmp_share, trx, NULL, FALSE, FALSE, &error_num)) ||
        (error_num = spider_db_xa_commit(conn, xid))) &&
      (force_commit == 0 ||
        (force_commit == 1 && error_num != ER_XAER_NOTA))
    ) {
      spider_sys_index_end(table_xa_member);
      spider_free_tmp_share_alloc(&tmp_share);
      free_root(&mem_root, MYF(0));
      goto error;
    }
    spider_free_tmp_share_alloc(&tmp_share);
    error_num = spider_sys_index_next_same(table_xa_member, xa_member_key);
  } while (error_num == 0);
  if ((error_num = spider_sys_index_end(table_xa_member)))
  {
    free_root(&mem_root, MYF(0));
    goto error;
  }
  free_root(&mem_root, MYF(0));
  spider_free_trx_conn(trx, FALSE);

  /*
    delete from
      mysql.spider_xa_member
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if ((error_num = spider_delete_xa_member(table_xa_member, xid)))
    goto error;
  spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
  table_xa_member_opened = FALSE;

xa_delete:
  /*
    delete from
      mysql.spider_xa
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if (
    !(table_xa = spider_open_sys_table(
      thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
      TRUE, &open_tables_backup, TRUE, &error_num))
  )
    goto error_open_table;
  table_xa_opened = TRUE;
  if ((error_num = spider_delete_xa(table_xa, xid)))
    goto error;
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  table_xa_opened = FALSE;
  DBUG_RETURN(0);

error:
  if (table_xa_opened)
    spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  if (table_xa_member_opened)
    spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
error_open_table:
  DBUG_RETURN(error_num);
}

int spider_internal_xa_rollback_by_xid(
  THD* thd,
  SPIDER_TRX *trx,
  XID* xid
) {
  TABLE *table_xa, *table_xa_member;
  int error_num;
  char xa_key[MAX_KEY_LENGTH];
  char xa_member_key[MAX_KEY_LENGTH];
  SPIDER_SHARE tmp_share;
  SPIDER_CONN *conn;
  uint force_commit = THDVAR(thd, force_commit);
  MEM_ROOT mem_root;
  Open_tables_state open_tables_backup;
  bool table_xa_opened = FALSE;
  bool table_xa_member_opened = FALSE;
  DBUG_ENTER("spider_internal_xa_rollback_by_xid");
  /*
    select
      status
    from
      mysql.spider_xa
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if (
    !(table_xa = spider_open_sys_table(
      thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
      TRUE, &open_tables_backup, TRUE, &error_num))
  )
    goto error_open_table;
  table_xa_opened = TRUE;
  spider_store_xa_pk(table_xa, xid);
  if (
    (error_num = spider_check_sys_table(table_xa, xa_key))
  ) {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table_xa->file->print_error(error_num, MYF(0));
      goto error;
    }
    error_num = ER_SPIDER_XA_NOT_EXISTS_NUM;
    goto error;
  }
  init_alloc_root(&mem_root, 4096, 0);
  if (
    force_commit != 2 &&
    (error_num = spider_check_sys_xa_status(
      table_xa,
      SPIDER_SYS_XA_NOT_YET_STR,
      SPIDER_SYS_XA_PREPARED_STR,
      SPIDER_SYS_XA_ROLLBACK_STR,
      ER_SPIDER_XA_PREPARED_NUM,
      &mem_root))
  ) {
    free_root(&mem_root, MYF(0));
    if (error_num == ER_SPIDER_XA_PREPARED_NUM)
      my_message(error_num, ER_SPIDER_XA_PREPARED_STR, MYF(0));
    goto error;
  }

  /*
    update
      mysql.spider_xa
    set
      status = 'ROLLBACK'
    where
      format_id = trx->xid.format_id and
      gtrid_length = trx->xid.gtrid_length and
      data = trx->xid.data
  */
  if (
    (error_num = spider_update_xa(
      table_xa, xid, SPIDER_SYS_XA_ROLLBACK_STR))
  ) {
    free_root(&mem_root, MYF(0));
    goto error;
  }
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  table_xa_opened = FALSE;

  /*
    select
      scheme tmp_share.tgt_wrapper,
      host tmp_share.tgt_host,
      port tmp_share.tgt_port,
      socket tmp_share.tgt_socket,
      username tmp_share.tgt_username,
      password tmp_share.tgt_password
    from
      mysql.spider_xa_member
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if (
    !(table_xa_member = spider_open_sys_table(
      thd, SPIDER_SYS_XA_MEMBER_TABLE_NAME_STR,
      SPIDER_SYS_XA_MEMBER_TABLE_NAME_LEN, TRUE, &open_tables_backup, TRUE,
      &error_num))
  ) {
    free_root(&mem_root, MYF(0));
    goto error_open_table;
  }
  table_xa_member_opened = TRUE;
  spider_store_xa_pk(table_xa_member, xid);
  if (
    (error_num = spider_get_sys_table_by_idx(table_xa_member, xa_member_key, 0,
    SPIDER_SYS_XA_PK_COL_CNT))
  ) {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      free_root(&mem_root, MYF(0));
      table_xa_member->file->print_error(error_num, MYF(0));
      goto error;
    } else {
      free_root(&mem_root, MYF(0));
      spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
      table_xa_member_opened = FALSE;
      goto xa_delete;
    }
  }

  memset(&tmp_share, 0, sizeof(SPIDER_SHARE));
  tmp_share.csname = (char*) table_xa_member->s->table_charset->csname;
  tmp_share.csname_length = strlen(tmp_share.csname);
  do {
    spider_get_sys_server_info(table_xa_member, &tmp_share, &mem_root);
    if ((error_num = spider_create_conn_key(&tmp_share)))
    {
      spider_sys_index_end(table_xa_member);
      free_root(&mem_root, MYF(0));
      goto error;
    }

    if (
      (!(conn = spider_get_conn(
      &tmp_share, trx, NULL, FALSE, FALSE, &error_num)) ||
        (error_num = spider_db_xa_rollback(conn, xid))) &&
      (force_commit == 0 ||
        (force_commit == 1 && error_num != ER_XAER_NOTA))
    ) {
      spider_sys_index_end(table_xa_member);
      spider_free_tmp_share_alloc(&tmp_share);
      free_root(&mem_root, MYF(0));
      goto error;
    }
    spider_free_tmp_share_alloc(&tmp_share);
    error_num = spider_sys_index_next_same(table_xa_member, xa_member_key);
  } while (error_num == 0);
  if ((error_num = spider_sys_index_end(table_xa_member)))
  {
    free_root(&mem_root, MYF(0));
    goto error;
  }
  free_root(&mem_root, MYF(0));
  spider_free_trx_conn(trx, FALSE);

  /*
    delete from
      mysql.spider_xa_member
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if ((error_num = spider_delete_xa_member(table_xa_member, xid)))
    goto error;
  spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
  table_xa_member_opened = FALSE;

xa_delete:
  /*
    delete from
      mysql.spider_xa
    where
      format_id = xid->format_id and
      gtrid_length = xid->gtrid_length and
      data = xid->data
  */
  if (
    !(table_xa = spider_open_sys_table(
      thd, SPIDER_SYS_XA_TABLE_NAME_STR, SPIDER_SYS_XA_TABLE_NAME_LEN,
      TRUE, &open_tables_backup, TRUE, &error_num))
  )
    goto error_open_table;
  table_xa_opened = TRUE;
  if ((error_num = spider_delete_xa(table_xa, xid)))
    goto error;
  spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  table_xa_opened = FALSE;
  DBUG_RETURN(0);

error:
  if (table_xa_opened)
    spider_close_sys_table(thd, table_xa, &open_tables_backup, TRUE);
  if (table_xa_member_opened)
    spider_close_sys_table(thd, table_xa_member, &open_tables_backup, TRUE);
error_open_table:
  DBUG_RETURN(error_num);
}

int spider_start_consistent_snapshot(
  handlerton *hton,
  THD* thd
) {
  int error_num;
  SPIDER_TRX *trx;
  DBUG_ENTER("spider_start_consistent_snapshot");

  if (!(trx = spider_get_trx(thd, &error_num)))
    DBUG_RETURN(error_num);
  if (THDVAR(trx->thd, use_consistent_snapshot))
  {
    if (THDVAR(trx->thd, internal_xa) &&
      THDVAR(trx->thd, internal_xa_snapshot) == 1)
    {
      error_num = ER_SPIDER_CANT_USE_BOTH_INNER_XA_AND_SNAPSHOT_NUM;
      my_message(error_num, ER_SPIDER_CANT_USE_BOTH_INNER_XA_AND_SNAPSHOT_STR,
        MYF(0));
      goto error;
    } else {
      trx->trx_consistent_snapshot = TRUE;
      trx->use_consistent_snapshot = TRUE;
      trx->internal_xa_snapshot = THDVAR(trx->thd, internal_xa_snapshot);
      trans_register_ha(trx->thd, FALSE, spider_hton_ptr);
      trans_register_ha(trx->thd, TRUE, spider_hton_ptr);
      if (THDVAR(trx->thd, use_all_conns_snapshot))
      {
        trx->internal_xa = FALSE;
        if (
          (error_num = spider_open_all_tables(trx, TRUE)) ||
          (THDVAR(trx->thd, use_snapshot_with_flush_tables) == 1 &&
            (error_num = spider_trx_all_flush_tables(trx))) ||
          (THDVAR(trx->thd, use_snapshot_with_flush_tables) == 2 &&
            ((error_num = spider_trx_another_lock_tables(trx)) ||
              (error_num = spider_trx_another_flush_tables(trx)))) ||
          (error_num = spider_trx_all_start_trx(trx)) ||
          (THDVAR(trx->thd, use_snapshot_with_flush_tables) == 1 &&
            ((THDVAR(trx->thd, use_flash_logs) &&
              (error_num = spider_trx_all_flush_logs(trx))) ||
            (error_num = spider_trx_all_unlock_tables(trx)))) ||
          (THDVAR(trx->thd, use_snapshot_with_flush_tables) == 2 &&
            ((THDVAR(trx->thd, use_flash_logs) &&
              (error_num = spider_trx_all_flush_logs(trx))) ||
            (error_num = spider_free_trx_another_conn(trx, TRUE))))
        )
          goto error;
      } else
        trx->internal_xa = THDVAR(trx->thd, internal_xa);
    }
  }

  DBUG_RETURN(0);

error:
  DBUG_RETURN(error_num);
}

int spider_commit(
  handlerton *hton,
  THD *thd,
  bool all
) {
  SPIDER_TRX *trx;
  TABLE *table_xa;
  TABLE *table_xa_member;
  int error_num;
  SPIDER_CONN *conn;
  DBUG_ENTER("spider_commit");

  if (all || (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
  {
    if (!(trx = (SPIDER_TRX*) thd_get_ha_data(thd, spider_hton_ptr)))
      DBUG_RETURN(0); /* transaction is not started */

    if (trx->trx_start)
    {
      if (trx->trx_xa)
      {
        if (
          (trx->internal_xa &&
            (error_num = spider_internal_xa_prepare(
              thd, trx, table_xa, table_xa_member, TRUE))) ||
          (error_num = spider_internal_xa_commit(
            thd, trx, &trx->xid, table_xa, table_xa_member))
        ) {
          DBUG_RETURN(error_num);
        }
        trx->trx_xa = FALSE;
      } else {
        if ((conn = spider_tree_first(trx->join_trx_top)))
        {
          do {
            if (
              (conn->autocommit != 1 || conn->trx_start) &&
              (error_num = spider_db_commit(conn))
            )
              DBUG_RETURN(error_num);
            if ((error_num = spider_end_trx(trx, conn)))
              DBUG_RETURN(error_num);
            conn->join_trx = 0;
          } while ((conn = spider_tree_next(conn)));
          trx->join_trx_top = NULL;
        }
      }
      trx->trx_start = FALSE;
    }
    spider_free_trx_conn(trx, FALSE);
    trx->trx_consistent_snapshot = FALSE;
  }
  DBUG_RETURN(0);
}

int spider_rollback(
  handlerton *hton,
  THD *thd,
  bool all
) {
  SPIDER_TRX *trx;
  int error_num;
  int roop_count;
  SPIDER_CONN *conn;
  DBUG_ENTER("spider_rollback");

  if (all || (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
  {
    if (!(trx = (SPIDER_TRX*) thd_get_ha_data(thd, spider_hton_ptr)))
      DBUG_RETURN(0); /* transaction is not started */

    if (trx->trx_start)
    {
      if (trx->trx_xa)
      {
        if (
          (error_num = spider_internal_xa_rollback(thd, trx))
        )
          DBUG_RETURN(error_num);
        trx->trx_xa = FALSE;
      } else {
        if ((conn = spider_tree_first(trx->join_trx_top)))
        {
          do {
            if (
              !conn->server_lost &&
              (conn->autocommit != 1 || conn->trx_start) &&
              (error_num = spider_db_rollback(conn))
            )
              DBUG_RETURN(error_num);
            if ((error_num = spider_end_trx(trx, conn)))
              DBUG_RETURN(error_num);
            conn->join_trx = 0;
          } while ((conn = spider_tree_next(conn)));
          trx->join_trx_top = NULL;
        }
      }
      trx->trx_start = FALSE;
    }
    spider_free_trx_conn(trx, FALSE);
    trx->trx_consistent_snapshot = FALSE;
  }

  DBUG_RETURN(0);
}

int spider_xa_prepare(
  handlerton *hton,
  THD* thd,
  bool all
) {
  int error_num;
  SPIDER_TRX *trx;
  TABLE *table_xa;
  TABLE *table_xa_member;
  DBUG_ENTER("spider_xa_prepare");

  if (all || (!thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
  {
    if (!(trx = (SPIDER_TRX*) thd_get_ha_data(thd, spider_hton_ptr)))
      DBUG_RETURN(0); /* transaction is not started */

    if (trx->trx_start)
    {
      if ((error_num = spider_internal_xa_prepare(
        thd, trx, table_xa, table_xa_member, FALSE)))
        goto error;
    }
  }

  DBUG_RETURN(0);

error:
  DBUG_RETURN(error_num);
}

int spider_xa_recover(
  handlerton *hton,
  XID* xid_list,
  uint len
) {
  THD* thd = current_thd;
  DBUG_ENTER("spider_xa_recover");
	if (len == 0 || xid_list == NULL)
    DBUG_RETURN(0);

  if (thd)
    DBUG_RETURN(spider_internal_xa_recover(thd, xid_list, len));
  else
    DBUG_RETURN(spider_initinal_xa_recover(xid_list, len));
}

int spider_xa_commit_by_xid(
  handlerton *hton,
  XID* xid
) {
  SPIDER_TRX *trx;
  int error_num;
  THD* thd = current_thd;
  DBUG_ENTER("spider_xa_commit_by_xid");

  if (!(trx = spider_get_trx(thd, &error_num)))
    goto error_get_trx;

  if ((error_num = spider_internal_xa_commit_by_xid(thd, trx, xid)))
    goto error;

  DBUG_RETURN(0);

error:
error_get_trx:
  DBUG_RETURN(error_num);
}

int spider_xa_rollback_by_xid(
  handlerton *hton,
  XID* xid
) {
  SPIDER_TRX *trx;
  int error_num;
  THD* thd = current_thd;
  DBUG_ENTER("spider_xa_rollback_by_xid");

  if (!(trx = spider_get_trx(thd, &error_num)))
    goto error_get_trx;

  if ((error_num = spider_internal_xa_rollback_by_xid(thd, trx, xid)))
    goto error;

  DBUG_RETURN(0);

error:
error_get_trx:
  DBUG_RETURN(error_num);
}

int spider_end_trx(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn
) {
  int error_num, tmp_error_num;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_end_trx");
  if (conn->table_lock == 3)
  {
    conn->table_lock = 0;
    tmp_spider.conn = conn;
    if (
      !conn->server_lost &&
      (error_num = spider_db_unlock_tables(&tmp_spider))
    )
      DBUG_RETURN(error_num);
  }
  if (
    conn->semi_trx_isolation >= 0 &&
    conn->trx_isolation != conn->semi_trx_isolation
  ) {
    if (
      !conn->server_lost &&
      (error_num = spider_db_set_trx_isolation(
        conn, THDVAR(trx->thd, semi_trx_isolation)))
    )
      DBUG_RETURN(error_num);
  }
  conn->semi_trx_isolation = -2;
  conn->semi_trx_isolation_chk = FALSE;
  conn->semi_trx_chk = FALSE;
  DBUG_RETURN(0);
}

int spider_check_trx_and_get_conn(
  THD *thd,
  ha_spider *spider
) {
  int error_num;
  SPIDER_TRX *trx;
  SPIDER_CONN *conn = spider->conn;
  DBUG_ENTER("spider_check_trx_and_get_conn");
  if (!(trx = spider_get_trx(thd, &error_num)))
  {
    DBUG_PRINT("info",("spider get trx error"));
    DBUG_RETURN(error_num);
  }
  if (trx->spider_thread_id != spider->spider_thread_id ||
    trx->trx_conn_adjustment != spider->trx_conn_adjustment)
  {
    DBUG_PRINT("info",(trx != spider->trx ? "spider change thd" :
      "spider next trx"));
    spider->trx = trx;
    spider->spider_thread_id = trx->spider_thread_id;
    spider->trx_conn_adjustment = trx->trx_conn_adjustment;
    if (
      !(spider->conn = conn =
        spider_get_conn(spider->share, trx, spider, FALSE, TRUE, &error_num))
    ) {
      DBUG_PRINT("info",("spider get conn error"));
      DBUG_RETURN(error_num);
    }
  } else if (!conn)
  {
    DBUG_PRINT("info",("spider get conn"));
    if (
      !(spider->conn = conn =
        spider_get_conn(spider->share, trx, spider, FALSE, TRUE, &error_num))
    ) {
      DBUG_PRINT("info",("spider get conn error"));
      DBUG_RETURN(error_num);
    }
  }
  DBUG_RETURN(0);
}

THD *spider_create_tmp_thd()
{
  THD *thd;
  DBUG_ENTER("spider_create_tmp_thd");
  if (!(thd = new THD))
    DBUG_RETURN(NULL);
  thd->killed = THD::NOT_KILLED;
  thd->locked_tables = FALSE;
  thd->proc_info = "";
  thd->thread_id = thd->variables.pseudo_thread_id = 0;
  thd->thread_stack = (char*) &thd;
  if (thd->store_globals())
    DBUG_RETURN(NULL);
  lex_start(thd);
  DBUG_RETURN(thd);
}

void spider_free_tmp_thd(
  THD *thd
) {
  DBUG_ENTER("spider_free_tmp_thd");
  thd->cleanup();
  delete thd;
  DBUG_VOID_RETURN;
}
