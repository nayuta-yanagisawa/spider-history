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

HASH spider_xa_locks;
pthread_mutex_t spider_xa_lock_mutex;

// for spider_xa_locks
uchar *spider_xa_lock_get_key(
  SPIDER_XA_LOCK *xa_lock,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
) {
  DBUG_ENTER("spider_xa_lock_get_key");
  *length = xa_lock->xa_lock_key_length;
  DBUG_RETURN((uchar*) xa_lock->xa_lock_key);
}

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
  int roopCount = 0;
  SPIDER_CONN *conn;
  DBUG_ENTER("spider_free_trx_conn");
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_conn_hash, roopCount)))
  {
    spider_free_conn_from_trx(trx, conn, FALSE, trx_free, &roopCount);
  }
  DBUG_RETURN(0);
}

int spider_free_trx_another_conn(
  SPIDER_TRX *trx,
  bool lock
) {
  int error_num, tmp_error_num;
  int roopCount = 0;
  SPIDER_CONN *conn;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_free_trx_another_conn");
  error_num = 0;
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_another_conn_hash,
    roopCount)))
  {
    tmp_spider.conn = conn;
    if (lock && (tmp_error_num = spider_db_unlock_tables(&tmp_spider)))
      error_num = tmp_error_num;
    spider_free_conn_from_trx(trx, conn, TRUE, TRUE, &roopCount);
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

int spider_trx_all_flush_tables(
  SPIDER_TRX *trx
) {
  int error_num;
  int roopCount = 0;
  SPIDER_CONN *conn;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_trx_flush_tables");
  memset(&tmp_spider, 0, sizeof(ha_spider));
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_conn_hash,
    roopCount)))
  {
    tmp_spider.conn = conn;
    if ((error_num = spider_db_flush_tables(&tmp_spider, FALSE)))
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
  DBUG_ENTER("spider_trx_start_trx");
  memset(&tmp_spider, 0, sizeof(ha_spider));
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_conn_hash,
    roopCount)))
  {
    tmp_spider.conn = conn;
    tmp_spider.db_conn = conn->db_conn;
    if ((error_num = spider_internal_start_trx(&tmp_spider)))
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
  DBUG_ENTER("spider_trx_flush_logs");
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
  SPIDER_ALTER_TABLE *alter_table;
  char *tmp_name;
  DBUG_ENTER("spider_create_trx_alter_table");
  if (!(alter_table = (SPIDER_ALTER_TABLE *)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &alter_table, sizeof(*alter_table),
      &tmp_name, share->table_name_length + 1,
      NullS))
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_alloc_alter_table;
  }
  alter_table->table_name = tmp_name;
  memcpy(alter_table->table_name, share->table_name, share->table_name_length);
  alter_table->table_name_length = share->table_name_length;
  alter_table->tmp_priority = share->priority;
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

  autocommit = !thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
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

SPIDER_XA_LOCK *spider_xa_lock(
  XID *xid,
  int *error_num
) {
  SPIDER_XA_LOCK *xa_lock, *tmp;
  String *xid_str;
  DBUG_ENTER("spider_xa_lock");

  if (!(xa_lock = (SPIDER_XA_LOCK *)
    my_malloc(sizeof(*xa_lock), MYF(MY_WME | MY_ZEROFILL)))
  ) {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_alloc_xa_lock;
  }
  if(!(xid_str = new String[1]))
  {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_alloc_string;
  }
  xid_str->set(xa_lock->xa_lock_key,
    sizeof(xa_lock->xa_lock_key), system_charset_info);
  xid_str->length(0);
  spider_db_append_xid_str(xid_str, xid);
  xa_lock->xa_lock_key_length = xid_str->length();

  pthread_mutex_lock(&spider_xa_lock_mutex);
  if (!(tmp = (SPIDER_XA_LOCK*) hash_search(&spider_xa_locks,
                                           (uchar*) xa_lock->xa_lock_key,
                                           xa_lock->xa_lock_key_length)))
  {
    if (my_hash_insert(&spider_xa_locks, (uchar*) xa_lock))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
  } else {
    *error_num = ER_SPIDER_XA_LOCKED_NUM;
    my_message(*error_num, ER_SPIDER_XA_LOCKED_STR, MYF(0));
    goto error;
  }
  pthread_mutex_unlock(&spider_xa_lock_mutex);
  delete xid_str;

  DBUG_RETURN(xa_lock);

error:
  pthread_mutex_unlock(&spider_xa_lock_mutex);
  delete xid_str;

error_alloc_string:
  my_free(xa_lock, MYF(0));

error_alloc_xa_lock:
  DBUG_RETURN(NULL);
}

int spider_xa_unlock(
  SPIDER_XA_LOCK *xa_lock
) {
  DBUG_ENTER("spider_xa_unlock");
  pthread_mutex_lock(&spider_xa_lock_mutex);
  hash_delete(&spider_xa_locks, (uchar*) xa_lock);
  pthread_mutex_unlock(&spider_xa_lock_mutex);
  my_free(xa_lock, MYF(0));
  DBUG_RETURN(0);
}

int spider_start_internal_consistent_snapshot(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn
) {
  int error_num;
  DBUG_ENTER("spider_start_internal_consistent_snapshot");
  if (trx->trx_consistent_snapshot)
  {
    if (THDVAR(trx->thd, use_snapshot_with_flush_tables) == 1)
    {
      ha_spider tmp_spider;
      memset(&tmp_spider, 0, sizeof(ha_spider));
      tmp_spider.conn = conn;
      if ((error_num = spider_db_flush_tables(&tmp_spider, TRUE)))
        DBUG_RETURN(error_num);
    }
    DBUG_RETURN(spider_db_consistent_snapshot(conn));
  }
  DBUG_RETURN(0);
}

int spider_internal_start_trx(
  ha_spider *spider
) {
  int error_num;
  SPIDER_TRX *trx = spider->trx;
  SPIDER_CONN *conn = spider->conn;
  bool sync_autocommit = THDVAR(trx->thd, sync_autocommit);
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
    if (
      (error_num = spider_check_and_set_sql_log_off(trx->thd, conn)) ||
      (sync_autocommit &&
        (error_num = spider_check_and_set_autocommit(trx->thd, conn)))
    )
      goto error;
  }
  if (trx->use_consistent_snapshot)
  {
    if (trx->internal_xa && trx->internal_xa_snapshot < 2)
    {
      error_num = ER_SPIDER_CANT_USE_BOTH_INNER_XA_AND_SNAPSHOT_NUM;
      my_message(error_num, ER_SPIDER_CANT_USE_BOTH_INNER_XA_AND_SNAPSHOT_STR,
        MYF(0));
      goto error;
    } else if (trx->internal_xa && trx->internal_xa_snapshot == 2)
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
      if (!trx->use_consistent_snapshot || trx->internal_xa_snapshot == 3)
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
      }
    }

    trans_register_ha(trx->thd, FALSE, spider_hton_ptr);
    if (thd_test_options(trx->thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
      trans_register_ha(trx->thd, TRUE, spider_hton_ptr);
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
    if (conn->autocommit != 1 || conn->semi_trx)
    {
      if ((error_num = spider_db_xa_start(conn, &trx->xid)))
        goto error;
      conn->disable_xa = FALSE;
    } else {
      conn->disable_xa = TRUE;
    }
  } else if (
    sync_autocommit &&
    conn->semi_trx_chk &&
    !conn->table_lock &&
    conn->autocommit == 1 &&
    THDVAR(trx->thd, semi_trx)
  ) {
    DBUG_PRINT("info",("spider semi_trx is set"));
    if ((error_num = spider_db_start_transaction(conn)))
      goto error;
    conn->semi_trx = TRUE;
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

error:
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
      TRUE, &open_tables_backup, &error_num))
  )
    goto error_open_table;
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
  spider_close_sys_table(thd, &open_tables_backup);

  conn = spider_tree_first(trx->join_trx_top);
  do {
    if (conn->join_trx)
    {
      if ((error_num = spider_db_xa_commit(conn, &trx->xid)))
      {
        if (force_commit == 0 ||
          (force_commit == 1 && error_num != ER_XAER_RMFAIL))
          DBUG_RETURN(error_num);
      }
      if ((error_num = spider_end_trx(trx, conn)))
        DBUG_RETURN(error_num);
      conn->join_trx = 0;
    }
  } while ((conn = spider_tree_next(conn)));
  trx->join_trx_top = NULL;

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
      SPIDER_SYS_XA_MEMBER_TABLE_NAME_LEN, TRUE, &open_tables_backup,
      &error_num))
  )
    goto error_open_table;
  if ((error_num = spider_delete_xa_member(table_xa_member, &trx->xid)))
    goto error;
  spider_close_sys_table(thd, &open_tables_backup);

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
      TRUE, &open_tables_backup, &error_num))
  )
    goto error_open_table;
  if ((error_num = spider_delete_xa(table_xa, &trx->xid)))
    goto error;
  spider_close_sys_table(thd, &open_tables_backup);
  DBUG_RETURN(0);

error:
  spider_close_sys_table(thd, &open_tables_backup);
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
        TRUE, &open_tables_backup, &error_num))
    )
      goto error_open_table;
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
    spider_close_sys_table(thd, &open_tables_backup);
  }

  conn = spider_tree_first(trx->join_trx_top);
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
              (force_commit == 1 && error_num != ER_XAER_RMFAIL))
              DBUG_RETURN(error_num);
          }
          if ((error_num = spider_db_xa_rollback(conn, &trx->xid)))
          {
            if (force_commit == 0 ||
              (force_commit == 1 && error_num != ER_XAER_RMFAIL))
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
        SPIDER_SYS_XA_MEMBER_TABLE_NAME_LEN, TRUE, &open_tables_backup,
        &error_num))
    )
      goto error_open_table;
    if ((error_num = spider_delete_xa_member(table_xa_member, &trx->xid)))
      goto error;
    spider_close_sys_table(thd, &open_tables_backup);

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
        TRUE, &open_tables_backup, &error_num))
    )
      goto error_open_table;
    if ((error_num = spider_delete_xa(table_xa, &trx->xid)))
      goto error;
    spider_close_sys_table(thd, &open_tables_backup);
  }
  DBUG_RETURN(0);

error:
  spider_close_sys_table(thd, &open_tables_backup);
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
  ha_spider *spider;
  uint force_commit = THDVAR(thd, force_commit);
  Open_tables_state open_tables_backup;
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
      TRUE, &open_tables_backup, &error_num))
  )
    goto error_open_table;
  if (
    (error_num = spider_insert_xa(
      table_xa, &trx->xid, SPIDER_SYS_XA_NOT_YET_STR))
  )
    goto error;
  spider_close_sys_table(thd, &open_tables_backup);

  if (
    !(table_xa_member = spider_open_sys_table(
      thd, SPIDER_SYS_XA_MEMBER_TABLE_NAME_STR,
      SPIDER_SYS_XA_MEMBER_TABLE_NAME_LEN, TRUE, &open_tables_backup,
      &error_num))
  )
    goto error_open_table;
  conn = spider_tree_first(trx->join_trx_top);
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
      spider = (ha_spider*) hash_element(&conn->user_ha_hash, 0);
      /*
        insert into mysql.spider_xa_member
          (format_id, gtrid_length, bqual_length, data,
          scheme, host, port, socket, username, password) values
          (trx->xid.format_id, trx->xid.gtrid_length,
          trx->xid.bqual_length, trx->xid.data,
          spider->share->tgt_wrapper,
          spider->share->tgt_host,
          spider->share->tgt_port,
          spider->share->tgt_socket,
          spider->share->tgt_username,
          spider->share->tgt_password)
      */
      if (
        (error_num = spider_insert_xa_member(
          table_xa_member, &trx->xid, spider->share))
      )
        goto error;

      if ((error_num = spider_db_xa_end(conn, &trx->xid)))
      {
        if (force_commit == 0 ||
          (force_commit == 1 && error_num != ER_XAER_RMFAIL))
          goto error;
      }
      if ((error_num = spider_db_xa_prepare(conn, &trx->xid)))
      {
        if (force_commit == 0 ||
          (force_commit == 1 && error_num != ER_XAER_RMFAIL))
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
  spider_close_sys_table(thd, &open_tables_backup);

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
      TRUE, &open_tables_backup, &error_num))
  )
    goto error_open_table;
  if (
    (error_num = spider_update_xa(
      table_xa, &trx->xid, SPIDER_SYS_XA_PREPARED_STR))
  )
    goto error;
  spider_close_sys_table(thd, &open_tables_backup);
  DBUG_RETURN(0);

error:
  spider_close_sys_table(thd, &open_tables_backup);
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
      FALSE, &open_tables_backup, &my_errno))
  )
    goto error_open_table;
  spider_store_xa_status(table_xa, SPIDER_SYS_XA_PREPARED_STR);
  if (
    (my_errno = spider_get_sys_table_by_idx(table_xa, xa_key, 1))
  ) {
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
  spider_close_sys_table(thd, &open_tables_backup);
  DBUG_RETURN(cnt);

error:
  spider_close_sys_table(thd, &open_tables_backup);
error_open_table:
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
      TRUE, &open_tables_backup, &error_num))
  )
    goto error_open_table;
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
      table_xa, &trx->xid, SPIDER_SYS_XA_COMMIT_STR))
  ) {
    free_root(&mem_root, MYF(0));
    goto error;
  }
  spider_close_sys_table(thd, &open_tables_backup);

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
      SPIDER_SYS_XA_MEMBER_TABLE_NAME_LEN, TRUE, &open_tables_backup,
      &error_num))
  ) {
    free_root(&mem_root, MYF(0));
    goto error_open_table;
  }
  spider_store_xa_pk(table_xa, xid);
  if (
    (error_num = spider_check_sys_table(table_xa_member, xa_member_key))
  ) {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      free_root(&mem_root, MYF(0));
      table_xa_member->file->print_error(error_num, MYF(0));
      goto error;
    } else {
      free_root(&mem_root, MYF(0));
      spider_close_sys_table(thd, &open_tables_backup);
      goto xa_delete;
    }
  }

  memset(&tmp_share, 0, sizeof(SPIDER_SHARE));
  tmp_share.csname = (char*) table_xa_member->s->table_charset->csname;
  tmp_share.csname_length = strlen(tmp_share.csname);
  do {
    spider_get_sys_server_info(table_xa_member, &tmp_share, &mem_root);
    if (error_num = spider_create_conn_key(&tmp_share))
    {
      free_root(&mem_root, MYF(0));
      goto error;
    }

    if (
      (!(conn = spider_get_conn(
      &tmp_share, trx, NULL, FALSE, FALSE, &error_num)) ||
        (error_num = spider_db_xa_commit(conn, xid))) &&
      (force_commit == 0 ||
        (force_commit == 1 && error_num != ER_XAER_RMFAIL))
    ) {
      spider_free_tmp_share_alloc(&tmp_share);
      free_root(&mem_root, MYF(0));
      goto error;
    }
    spider_free_tmp_share_alloc(&tmp_share);
    error_num = spider_sys_index_next_same(table_xa_member, xa_member_key);
  } while (error_num == 0);
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
  spider_close_sys_table(thd, &open_tables_backup);

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
      TRUE, &open_tables_backup, &error_num))
  )
    goto error_open_table;
  if ((error_num = spider_delete_xa(table_xa, xid)))
    goto error;
  spider_close_sys_table(thd, &open_tables_backup);
  DBUG_RETURN(0);

error:
  spider_close_sys_table(thd, &open_tables_backup);
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
      TRUE, &open_tables_backup, &error_num))
  )
    goto error_open_table;
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
      table_xa, &trx->xid, SPIDER_SYS_XA_ROLLBACK_STR))
  ) {
    free_root(&mem_root, MYF(0));
    goto error;
  }
  spider_close_sys_table(thd, &open_tables_backup);

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
      SPIDER_SYS_XA_MEMBER_TABLE_NAME_LEN, TRUE, &open_tables_backup,
      &error_num))
  ) {
    free_root(&mem_root, MYF(0));
    goto error_open_table;
  }
  spider_store_xa_pk(table_xa, xid);
  if (
    (error_num = spider_check_sys_table(table_xa_member, xa_member_key))
  ) {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      free_root(&mem_root, MYF(0));
      table_xa_member->file->print_error(error_num, MYF(0));
      goto error;
    } else {
      free_root(&mem_root, MYF(0));
      spider_close_sys_table(thd, &open_tables_backup);
      goto xa_delete;
    }
  }

  memset(&tmp_share, 0, sizeof(SPIDER_SHARE));
  tmp_share.csname = (char*) table_xa_member->s->table_charset->csname;
  tmp_share.csname_length = strlen(tmp_share.csname);
  do {
    spider_get_sys_server_info(table_xa_member, &tmp_share, &mem_root);
    if (error_num = spider_create_conn_key(&tmp_share))
    {
      free_root(&mem_root, MYF(0));
      goto error;
    }

    if (
      (!(conn = spider_get_conn(
      &tmp_share, trx, NULL, FALSE, FALSE, &error_num)) ||
        (error_num = spider_db_xa_rollback(conn, xid))) &&
      (force_commit == 0 ||
        (force_commit == 1 && error_num != ER_XAER_RMFAIL))
    ) {
      spider_free_tmp_share_alloc(&tmp_share);
      free_root(&mem_root, MYF(0));
      goto error;
    }
    spider_free_tmp_share_alloc(&tmp_share);
    error_num = spider_sys_index_next_same(table_xa_member, xa_member_key);
  } while (error_num == 0);
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
  spider_close_sys_table(thd, &open_tables_backup);

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
      TRUE, &open_tables_backup, &error_num))
  )
    goto error_open_table;
  if ((error_num = spider_delete_xa(table_xa, xid)))
    goto error;
  spider_close_sys_table(thd, &open_tables_backup);
  DBUG_RETURN(0);

error:
  spider_close_sys_table(thd, &open_tables_backup);
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
      if (THDVAR(trx->thd, use_all_conns_snapshot))
      {
        trx->internal_xa = FALSE;
        if (
          (error_num = spider_open_all_tables(trx, TRUE)) ||
          (THDVAR(trx->thd, use_snapshot_with_flush_tables) == 2 &&
            ((error_num = spider_trx_another_lock_tables(trx)) ||
              (error_num = spider_trx_all_flush_tables(trx)))) ||
          (error_num = spider_trx_all_start_trx(trx))
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
  SPIDER_XA_LOCK *xa_lock;
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
        if (!(xa_lock = spider_xa_lock(&trx->xid, &error_num)))
          DBUG_RETURN(error_num);
        if (
          (trx->internal_xa &&
            (error_num = spider_internal_xa_prepare(
              thd, trx, table_xa, table_xa_member, TRUE))) ||
          (error_num = spider_internal_xa_commit(
            thd, trx, &trx->xid, table_xa, table_xa_member))
        ) {
          spider_xa_unlock(xa_lock);
          DBUG_RETURN(error_num);
        }
        spider_xa_unlock(xa_lock);
        trx->trx_xa = FALSE;
      } else {
        conn = spider_tree_first(trx->join_trx_top);
        do {
          if (
            (conn->autocommit != 1 || conn->semi_trx) &&
            (error_num = spider_db_commit(conn))
          )
            DBUG_RETURN(error_num);
          if ((error_num = spider_end_trx(trx, conn)))
            DBUG_RETURN(error_num);
          conn->join_trx = 0;
        } while ((conn = spider_tree_next(conn)));
        trx->join_trx_top = NULL;
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
        conn = spider_tree_first(trx->join_trx_top);
        do {
          if (
            !conn->server_lost &&
            (conn->autocommit != 1 || conn->semi_trx) &&
            (error_num = spider_db_rollback(conn))
          )
            DBUG_RETURN(error_num);
          if ((error_num = spider_end_trx(trx, conn)))
            DBUG_RETURN(error_num);
          conn->join_trx = 0;
        } while ((conn = spider_tree_next(conn)));
        trx->join_trx_top = NULL;
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
  SPIDER_XA_LOCK *xa_lock;
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
      if (!(xa_lock = spider_xa_lock(&trx->xid, &error_num)))
        goto error_xa_lock;

      if ((error_num = spider_internal_xa_prepare(
        thd, trx, table_xa, table_xa_member, FALSE)))
        goto error;
      spider_xa_unlock(xa_lock);
/*
      trx->trx_start = FALSE;
      trx->trx_xa = FALSE;
*/
    }
/*
    spider_free_trx_conn(trx, FALSE);
    trx->trx_consistent_snapshot = FALSE;
*/
  }

  DBUG_RETURN(0);

error:
  spider_xa_unlock(xa_lock);

error_xa_lock:
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
  DBUG_RETURN(0);
}

int spider_xa_commit_by_xid(
  handlerton *hton,
  XID* xid
) {
  SPIDER_TRX *trx;
  SPIDER_XA_LOCK *xa_lock;
  int error_num;
  THD* thd = current_thd;
  DBUG_ENTER("spider_xa_commit_by_xid");

  if (!(xa_lock = spider_xa_lock(xid, &error_num)))
    goto error_xa_lock;

  if (!(trx = spider_get_trx(thd, &error_num)))
    goto error_get_trx;

  if ((error_num = spider_internal_xa_commit_by_xid(thd, trx, xid)))
    goto error;

  spider_xa_unlock(xa_lock);

  DBUG_RETURN(0);

error:
error_get_trx:
  spider_xa_unlock(xa_lock);

error_xa_lock:
  DBUG_RETURN(error_num);
}

int spider_xa_rollback_by_xid(
  handlerton *hton,
  XID* xid
) {
  SPIDER_TRX *trx;
  SPIDER_XA_LOCK *xa_lock;
  int error_num;
  THD* thd = current_thd;
  DBUG_ENTER("spider_xa_rollback_by_xid");

  if (!(xa_lock = spider_xa_lock(xid, &error_num)))
    goto error_xa_lock;

  if (!(trx = spider_get_trx(thd, &error_num)))
    goto error_get_trx;

  if ((error_num = spider_internal_xa_rollback_by_xid(thd, trx, xid)))
    goto error;
  spider_xa_unlock(xa_lock);

  DBUG_RETURN(0);

error:
error_get_trx:
  spider_xa_unlock(xa_lock);

error_xa_lock:
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
  SPIDER_TRX *trx = spider->trx;
  SPIDER_CONN *conn = spider->conn;
  DBUG_ENTER("spider_check_trx_and_get_conn");
  if (thd != trx->thd)
  {
    DBUG_PRINT("info",("spider change thd"));
    if (conn)
    {
      hash_delete(&conn->user_ha_hash, (uchar*) spider);
      spider->conn = conn = NULL;
    }
    if (
      !(spider->trx = trx = spider_get_trx(thd, &error_num)) ||
      !(spider->conn = conn =
        spider_get_conn(spider->share, trx, spider, FALSE, TRUE, &error_num))
    ) {
      DBUG_PRINT("info",("spider get trx or conn error"));
      DBUG_RETURN(error_num);
    }
    spider->db_conn = conn->db_conn;
  } else if (!conn)
  {
    if (
      !(spider->conn = conn =
        spider_get_conn(spider->share, trx, spider, FALSE, TRUE, &error_num))
    ) {
      DBUG_PRINT("info",("spider get conn error"));
      DBUG_RETURN(error_num);
    }
    spider->db_conn = conn->db_conn;
  }
  DBUG_RETURN(0);
}