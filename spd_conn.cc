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
#include "spd_param.h"
#include "spd_db_include.h"
#include "spd_include.h"
#include "ha_spider.h"
#include "spd_db_conn.h"
#include "spd_conn.h"
#include "spd_table.h"

HASH spider_open_connections;
pthread_mutex_t spider_conn_mutex;

/* for spider_open_connections and trx_conn_hash */
uchar *spider_conn_get_key(
  SPIDER_CONN *conn,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
) {
  DBUG_ENTER("spider_conn_get_key");
  *length = conn->conn_key_length;
  DBUG_RETURN((uchar*) conn->conn_key);
}

int spider_free_conn_alloc(
  SPIDER_CONN *conn
) {
  ha_spider *spider;
  DBUG_ENTER("spider_free_conn_alloc");
  spider_db_disconnect(conn);
  pthread_mutex_lock(&conn->user_ha_mutex);
  while ((spider = (ha_spider*) hash_element(&conn->user_ha_hash, 0)))
  {
    hash_delete(&conn->user_ha_hash, (uchar*) spider);
    spider->conn = NULL;
    spider->db_conn = NULL;
  }
  pthread_mutex_unlock(&conn->user_ha_mutex);
  hash_free(&conn->user_ha_hash);
  hash_free(&conn->lock_table_hash);
  VOID(pthread_mutex_destroy(&conn->user_ha_mutex));
  DBUG_RETURN(0);
}

void spider_free_conn_from_trx(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn,
  bool another,
  bool trx_free,
  int *roop_count
) {
  ha_spider *spider;
  DBUG_ENTER("spider_free_conn_from_trx");
  if(
    THDVAR(trx->thd, conn_recycle_mode) != 2 ||
    trx_free
  ) {
    conn->thd = NULL;
    if (another)
      hash_delete(&trx->trx_another_conn_hash, (uchar*) conn);
    else
      hash_delete(&trx->trx_conn_hash, (uchar*) conn);
    pthread_mutex_lock(&conn->user_ha_mutex);
    while ((spider = (ha_spider*) hash_element(&conn->user_ha_hash, 0)))
    {
      hash_delete(&conn->user_ha_hash, (uchar*) spider);
      spider->conn = NULL;
      spider->db_conn = NULL;
      if (another)
      {
        spider_free_tmp_share_alloc(spider->share);
        my_free(spider->share, MYF(0));
        delete spider;
      }
    }
    pthread_mutex_unlock(&conn->user_ha_mutex);

    if(
      THDVAR(trx->thd, conn_recycle_mode) == 1 &&
      !trx_free
    ) {
      /* conn_recycle_mode == 1 */
      pthread_mutex_lock(&spider_conn_mutex);
      if (my_hash_insert(&spider_open_connections, (uchar*) conn))
      {
        pthread_mutex_unlock(&spider_conn_mutex);
        spider_free_conn(conn);
      } else
        pthread_mutex_unlock(&spider_conn_mutex);
    } else {
      /* conn_recycle_mode == 0 */
      spider_free_conn(conn);
    }
  } else if (roop_count)
    (*roop_count)++;
  DBUG_VOID_RETURN;
}

SPIDER_CONN *spider_create_conn(
  const SPIDER_SHARE *share,
  int *error_num
) {
  SPIDER_CONN *conn;
  char *tmp_name, *tmp_host, *tmp_username, *tmp_password, *tmp_socket;
  char *tmp_wrapper;
  DBUG_ENTER("spider_create_conn");

  if (!(conn = (SPIDER_CONN *)
       my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                        &conn, sizeof(*conn),
                        &tmp_name, share->conn_key_length + 1,
                        &tmp_host, share->tgt_host_length + 1,
                        &tmp_username, share->tgt_username_length + 1,
                        &tmp_password, share->tgt_password_length + 1,
                        &tmp_socket, share->tgt_socket_length + 1,
                        &tmp_wrapper, share->tgt_wrapper_length + 1,
                        NullS))
  ) {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_alloc_conn;
  }

  conn->conn_key_length = share->conn_key_length;
  conn->conn_key = tmp_name;
  memcpy(conn->conn_key, share->conn_key, share->conn_key_length);
  conn->tgt_host_length = share->tgt_host_length;
  conn->tgt_host = tmp_host;
  memcpy(conn->tgt_host, share->tgt_host, share->tgt_host_length);
  conn->tgt_username_length = share->tgt_username_length;
  conn->tgt_username = tmp_username;
  memcpy(conn->tgt_username, share->tgt_username, share->tgt_username_length);
  conn->tgt_password_length = share->tgt_password_length;
  conn->tgt_password = tmp_password;
  memcpy(conn->tgt_password, share->tgt_password, share->tgt_password_length);
  conn->tgt_socket_length = share->tgt_socket_length;
  conn->tgt_socket = tmp_socket;
  memcpy(conn->tgt_socket, share->tgt_socket, share->tgt_socket_length);
  conn->tgt_wrapper_length = share->tgt_wrapper_length;
  conn->tgt_wrapper = tmp_wrapper;
  memcpy(conn->tgt_wrapper, share->tgt_wrapper, share->tgt_wrapper_length);
  conn->tgt_port = share->tgt_port;
  conn->db_conn = NULL;
  conn->join_trx = 0;
  conn->trx_isolation = -1;
  conn->thd = NULL;
  conn->table_lock = 0;
  conn->autocommit = -1;
  conn->sql_log_off = -1;
  conn->semi_trx_isolation = -2;
  conn->semi_trx_isolation_chk = FALSE;
  conn->semi_trx_chk = FALSE;

  if (pthread_mutex_init(&conn->user_ha_mutex, MY_MUTEX_INIT_FAST))
  {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_init_user_ha_mutex;
  }
  if (
    hash_init(&conn->user_ha_hash, system_charset_info, 32, 0, 0,
              (hash_get_key) spider_ha_get_key, 0, 0)
  ) {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_init_hash;
  }
  if (
    hash_init(&conn->lock_table_hash, system_charset_info, 32, 0, 0,
              (hash_get_key) spider_ha_get_key, 0, 0)
  ) {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_init_lock_table_hash;
  }

  if ((*error_num = spider_db_connect(share, conn)))
    goto error;

  DBUG_RETURN(conn);

error:
  hash_free(&conn->lock_table_hash);
error_init_lock_table_hash:
  hash_free(&conn->user_ha_hash);
error_init_hash:
  VOID(pthread_mutex_destroy(&conn->user_ha_mutex));
error_init_user_ha_mutex:
  my_free(conn, MYF(0));
error_alloc_conn:
  DBUG_RETURN(NULL);
}

SPIDER_CONN *spider_get_conn(
  const SPIDER_SHARE *share,
  SPIDER_TRX *trx,
  ha_spider *spider,
  bool another,
  bool thd_chg,
  int *error_num
) {
  SPIDER_CONN *conn;
  DBUG_ENTER("spider_get_conn");

  if (
    (another &&
      !(conn = (SPIDER_CONN*) hash_search(&trx->trx_another_conn_hash,
        (uchar*) share->conn_key, share->conn_key_length))) ||
    (!another &&
      !(conn = (SPIDER_CONN*) hash_search(&trx->trx_conn_hash,
        (uchar*) share->conn_key, share->conn_key_length)))
  ) {
    if (
      (THDVAR(trx->thd, conn_recycle_mode) & 1) ||
      THDVAR(trx->thd, conn_recycle_strict)
    ) {
      pthread_mutex_lock(&spider_conn_mutex);
      if (!(conn = (SPIDER_CONN*) hash_search(&spider_open_connections,
                                             (uchar*) share->conn_key,
                                             share->conn_key_length)))
      {
        pthread_mutex_unlock(&spider_conn_mutex);
        DBUG_PRINT("info",("spider create new conn"));
        if(!(conn = spider_create_conn(share, error_num)))
          goto error;
      } else {
        hash_delete(&spider_open_connections, (uchar*) conn);
        pthread_mutex_unlock(&spider_conn_mutex);
        DBUG_PRINT("info",("spider get global conn"));
      }
    } else {
      DBUG_PRINT("info",("spider create new conn"));
      /* conn_recycle_strict = 0 and conn_recycle_mode = 0 or 2 */
      if(!(conn = spider_create_conn(share, error_num)))
        goto error;
    }
    conn->thd = trx->thd;
    conn->priority = share->priority;
    if (spider)
    {
      spider->conn = conn;
      spider->db_conn = conn->db_conn;

      pthread_mutex_lock(&conn->user_ha_mutex);
      if (my_hash_insert(&conn->user_ha_hash, (uchar*) spider))
      {
        pthread_mutex_unlock(&conn->user_ha_mutex);
        *error_num = HA_ERR_OUT_OF_MEM;
        goto error;
      }
      pthread_mutex_unlock(&conn->user_ha_mutex);
    }

    if (another)
    {
      if (my_hash_insert(&trx->trx_another_conn_hash, (uchar*) conn))
      {
        spider_free_conn(conn);
        *error_num = HA_ERR_OUT_OF_MEM;
        goto error;
      }
    } else {
      if (my_hash_insert(&trx->trx_conn_hash, (uchar*) conn))
      {
        spider_free_conn(conn);
        *error_num = HA_ERR_OUT_OF_MEM;
        goto error;
      }
    }
  } else if (spider)
  {
    pthread_mutex_lock(&conn->user_ha_mutex);
    if (my_hash_insert(&conn->user_ha_hash, (uchar*) spider))
    {
      pthread_mutex_unlock(&conn->user_ha_mutex);
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    pthread_mutex_unlock(&conn->user_ha_mutex);
  }

  DBUG_PRINT("info",("spider conn=%x", conn));
  DBUG_RETURN(conn);

error:
  DBUG_RETURN(NULL);
}

int spider_free_conn(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_free_conn");
  spider_free_conn_alloc(conn);
  my_free(conn, MYF(0));
  DBUG_RETURN(0);
}

void spider_conn_user_ha_delete(
  SPIDER_CONN *conn,
  ha_spider *spider
) {
  DBUG_ENTER("spider_conn_user_ha_delete");
  pthread_mutex_lock(&conn->user_ha_mutex);
  hash_delete(&conn->user_ha_hash, (uchar*) spider);
  pthread_mutex_unlock(&conn->user_ha_mutex);
  DBUG_VOID_RETURN;
}

void spider_tree_insert(
  SPIDER_CONN *top,
  SPIDER_CONN *conn
) {
  SPIDER_CONN *current = top;
  longlong priority = conn->priority;
  DBUG_ENTER("spider_tree_insert");
  while (TRUE)
  {
    if (priority < current->priority)
    {
      if (current->c_small == NULL)
      {
        conn->p_small = NULL;
        conn->p_big = current;
        conn->c_small = NULL;
        conn->c_big = NULL;
        current->c_small = conn;
        break;
      } else
        current = current->c_small;
    } else {
      if (current->c_big == NULL)
      {
        conn->p_small = current;
        conn->p_big = NULL;
        conn->c_small = NULL;
        conn->c_big = NULL;
        current->c_big = conn;
        break;
      } else
        current = current->c_big;
    }
  }
  DBUG_VOID_RETURN;
}

SPIDER_CONN *spider_tree_first(
  SPIDER_CONN *top
) {
  SPIDER_CONN *current = top;
  DBUG_ENTER("spider_tree_first");
  while (current)
  {
    if (current->c_small == NULL)
      break;
    else
      current = current->c_small;
  }
  DBUG_RETURN(current);
}

SPIDER_CONN *spider_tree_last(
  SPIDER_CONN *top
) {
  SPIDER_CONN *current = top;
  DBUG_ENTER("spider_tree_last");
  while (TRUE)
  {
    if (current->c_big == NULL)
      break;
    else
      current = current->c_big;
  }
  DBUG_RETURN(current);
}

SPIDER_CONN *spider_tree_next(
  SPIDER_CONN *current
) {
  DBUG_ENTER("spider_tree_next");
  if (current->c_big)
    DBUG_RETURN(spider_tree_first(current->c_big));
  while (TRUE)
  {
    if (current->p_big)
      DBUG_RETURN(current->p_big);
    if (!current->p_small)
      DBUG_RETURN(NULL);
    current = current->p_small;
  }
}

SPIDER_CONN *spider_tree_delete(
  SPIDER_CONN *conn,
  SPIDER_CONN *top
) {
  DBUG_ENTER("spider_tree_delete");
  if (conn->p_small)
  {
    if (conn->c_small)
    {
      conn->c_small->p_big = NULL;
      conn->c_small->p_small = conn->p_small;
      conn->p_small->c_big = conn->c_small;
      if (conn->c_big)
      {
        SPIDER_CONN *last = spider_tree_last(conn->c_small);
        conn->c_big->p_small = last;
        last->c_big = conn->c_big;
      }
    } else if (conn->c_big)
    {
      conn->c_big->p_small = conn->p_small;
      conn->p_small->c_big = conn->c_big;
    } else
      conn->p_small->c_big = NULL;
  } else if (conn->p_big)
  {
    if (conn->c_small)
    {
      conn->c_small->p_big = conn->p_big;
      conn->p_big->c_small = conn->c_small;
      if (conn->c_big)
      {
        SPIDER_CONN *last = spider_tree_last(conn->c_small);
        conn->c_big->p_small = last;
        last->c_big = conn->c_big;
      }
    } else if (conn->c_big)
    {
      conn->c_big->p_big = conn->p_big;
      conn->c_big->p_small = NULL;
      conn->p_big->c_small = conn->c_big;
    } else
      conn->p_big->c_small = NULL;
  } else {
    if (conn->c_small)
    {
      conn->c_small->p_big = NULL;
      conn->c_small->p_small = NULL;
      if (conn->c_big)
      {
        SPIDER_CONN *last = spider_tree_last(conn->c_small);
        conn->c_big->p_small = last;
        last->c_big = conn->c_big;
      }
      DBUG_RETURN(conn->c_small);
    } else if (conn->c_big)
    {
      conn->c_big->p_small = NULL;
      DBUG_RETURN(conn->c_big);
    }
    DBUG_RETURN(NULL);
  }
  DBUG_RETURN(top);
}
