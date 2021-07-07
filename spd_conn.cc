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
#include "spd_param.h"
#include "spd_db_include.h"
#include "spd_include.h"
#include "ha_spider.h"
#include "spd_db_conn.h"
#include "spd_conn.h"
#include "spd_table.h"
#include "spd_direct_sql.h"
#include "spd_ping_table.h"

#ifndef WITHOUT_SPIDER_BG_SEARCH
extern pthread_attr_t spider_pt_attr;

extern pthread_mutex_t spider_global_trx_mutex;
extern SPIDER_TRX *spider_global_trx;
#endif

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

int spider_reset_conn_setted_parameter(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_reset_conn_setted_parameter");
  conn->autocommit = spider_remote_autocommit;
  conn->sql_log_off = spider_remote_sql_log_off;
  conn->trx_isolation = spider_remote_trx_isolation;
  if (spider_remote_access_charset)
  {
    if (!(conn->access_charset =
      get_charset_by_csname(spider_remote_access_charset, MY_CS_PRIMARY,
        MYF(MY_WME))))
      DBUG_RETURN(ER_UNKNOWN_CHARACTER_SET);
  } else
    conn->access_charset = NULL;
  DBUG_RETURN(0);
}

int spider_free_conn_alloc(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_free_conn_alloc");
#ifndef WITHOUT_SPIDER_BG_SEARCH
  spider_free_conn_thread(conn);
#endif
  spider_db_disconnect(conn);
  hash_free(&conn->lock_table_hash);
  VOID(pthread_mutex_destroy(&conn->mta_conn_mutex));
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
    trx_free ||
    THDVAR(trx->thd, conn_recycle_mode) != 2
  ) {
    conn->thd = NULL;
    if (another)
    {
      ha_spider *next_spider;
      hash_delete(&trx->trx_another_conn_hash, (uchar*) conn);
      spider = (ha_spider*) conn->another_ha_first;
      while (spider)
      {
        next_spider = spider->next;
        spider_free_tmp_share_alloc(spider->share);
        my_free(spider->share, MYF(0));
        delete spider;
        spider = next_spider;
      }
      conn->another_ha_first = NULL;
      conn->another_ha_last = NULL;
    } else
      hash_delete(&trx->trx_conn_hash, (uchar*) conn);

    if(
      !trx_free &&
      THDVAR(trx->thd, conn_recycle_mode) == 1
    ) {
      /* conn_recycle_mode == 1 */
      *conn->conn_key = '0';
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
  ha_spider *spider,
  int link_idx,
  int *error_num
) {
  int need_mon = 0;
  SPIDER_CONN *conn;
  char *tmp_name, *tmp_host, *tmp_username, *tmp_password, *tmp_socket;
  char *tmp_wrapper, *tmp_ssl_ca, *tmp_ssl_capath, *tmp_ssl_cert;
  char *tmp_ssl_cipher, *tmp_ssl_key, *tmp_default_file, *tmp_default_group;
  DBUG_ENTER("spider_create_conn");

  if (!(conn = (SPIDER_CONN *)
       my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                        &conn, sizeof(*conn),
                        &tmp_name, share->conn_keys_lengths[link_idx] + 1,
                        &tmp_host, share->tgt_hosts_lengths[link_idx] + 1,
                        &tmp_username,
                          share->tgt_usernames_lengths[link_idx] + 1,
                        &tmp_password,
                          share->tgt_passwords_lengths[link_idx] + 1,
                        &tmp_socket, share->tgt_sockets_lengths[link_idx] + 1,
                        &tmp_wrapper,
                          share->tgt_wrappers_lengths[link_idx] + 1,
                        &tmp_ssl_ca, share->tgt_ssl_cas_lengths[link_idx] + 1,
                        &tmp_ssl_capath,
                          share->tgt_ssl_capaths_lengths[link_idx] + 1,
                        &tmp_ssl_cert,
                          share->tgt_ssl_certs_lengths[link_idx] + 1,
                        &tmp_ssl_cipher,
                          share->tgt_ssl_ciphers_lengths[link_idx] + 1,
                        &tmp_ssl_key,
                          share->tgt_ssl_keys_lengths[link_idx] + 1,
                        &tmp_default_file,
                          share->tgt_default_files_lengths[link_idx] + 1,
                        &tmp_default_group,
                          share->tgt_default_groups_lengths[link_idx] + 1,
                        NullS))
  ) {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_alloc_conn;
  }

  conn->conn_key_length = share->conn_keys_lengths[link_idx];
  conn->conn_key = tmp_name;
  memcpy(conn->conn_key, share->conn_keys[link_idx],
    share->conn_keys_lengths[link_idx]);
  conn->tgt_host_length = share->tgt_hosts_lengths[link_idx];
  conn->tgt_host = tmp_host;
  memcpy(conn->tgt_host, share->tgt_hosts[link_idx],
    share->tgt_hosts_lengths[link_idx]);
  conn->tgt_username_length = share->tgt_usernames_lengths[link_idx];
  conn->tgt_username = tmp_username;
  memcpy(conn->tgt_username, share->tgt_usernames[link_idx],
    share->tgt_usernames_lengths[link_idx]);
  conn->tgt_password_length = share->tgt_passwords_lengths[link_idx];
  conn->tgt_password = tmp_password;
  memcpy(conn->tgt_password, share->tgt_passwords[link_idx],
    share->tgt_passwords_lengths[link_idx]);
  conn->tgt_socket_length = share->tgt_sockets_lengths[link_idx];
  conn->tgt_socket = tmp_socket;
  memcpy(conn->tgt_socket, share->tgt_sockets[link_idx],
    share->tgt_sockets_lengths[link_idx]);
  conn->tgt_wrapper_length = share->tgt_wrappers_lengths[link_idx];
  conn->tgt_wrapper = tmp_wrapper;
  memcpy(conn->tgt_wrapper, share->tgt_wrappers[link_idx],
    share->tgt_wrappers_lengths[link_idx]);
  conn->tgt_ssl_ca_length = share->tgt_ssl_cas_lengths[link_idx];
  if (conn->tgt_ssl_ca_length)
  {
    conn->tgt_ssl_ca = tmp_ssl_ca;
    memcpy(conn->tgt_ssl_ca, share->tgt_ssl_cas[link_idx],
      share->tgt_ssl_cas_lengths[link_idx]);
  } else
    conn->tgt_ssl_ca = NULL;
  conn->tgt_ssl_capath_length = share->tgt_ssl_capaths_lengths[link_idx];
  if (conn->tgt_ssl_capath_length)
  {
    conn->tgt_ssl_capath = tmp_ssl_capath;
    memcpy(conn->tgt_ssl_capath, share->tgt_ssl_capaths[link_idx],
      share->tgt_ssl_capaths_lengths[link_idx]);
  } else
    conn->tgt_ssl_capath = NULL;
  conn->tgt_ssl_cert_length = share->tgt_ssl_certs_lengths[link_idx];
  if (conn->tgt_ssl_cert_length)
  {
    conn->tgt_ssl_cert = tmp_ssl_cert;
    memcpy(conn->tgt_ssl_cert, share->tgt_ssl_certs[link_idx],
      share->tgt_ssl_certs_lengths[link_idx]);
  } else
    conn->tgt_ssl_cert = NULL;
  conn->tgt_ssl_cipher_length = share->tgt_ssl_ciphers_lengths[link_idx];
  if (conn->tgt_ssl_cipher_length)
  {
    conn->tgt_ssl_cipher = tmp_ssl_cipher;
    memcpy(conn->tgt_ssl_cipher, share->tgt_ssl_ciphers[link_idx],
      share->tgt_ssl_ciphers_lengths[link_idx]);
  } else
    conn->tgt_ssl_cipher = NULL;
  conn->tgt_ssl_key_length = share->tgt_ssl_keys_lengths[link_idx];
  if (conn->tgt_ssl_key_length)
  {
    conn->tgt_ssl_key = tmp_ssl_key;
    memcpy(conn->tgt_ssl_key, share->tgt_ssl_keys[link_idx],
      share->tgt_ssl_keys_lengths[link_idx]);
  } else
    conn->tgt_ssl_key = NULL;
  conn->tgt_default_file_length = share->tgt_default_files_lengths[link_idx];
  if (conn->tgt_default_file_length)
  {
    conn->tgt_default_file = tmp_default_file;
    memcpy(conn->tgt_default_file, share->tgt_default_files[link_idx],
      share->tgt_default_files_lengths[link_idx]);
  } else
    conn->tgt_default_file = NULL;
  conn->tgt_default_group_length = share->tgt_default_groups_lengths[link_idx];
  if (conn->tgt_default_group_length)
  {
    conn->tgt_default_group = tmp_default_group;
    memcpy(conn->tgt_default_group, share->tgt_default_groups[link_idx],
      share->tgt_default_groups_lengths[link_idx]);
  } else
    conn->tgt_default_group = NULL;
  conn->tgt_port = share->tgt_ports[link_idx];
  conn->tgt_ssl_vsc = share->tgt_ssl_vscs[link_idx];
  conn->db_conn = NULL;
  conn->join_trx = 0;
  conn->thd = NULL;
  conn->table_lock = 0;
  conn->semi_trx_isolation = -2;
  conn->semi_trx_isolation_chk = FALSE;
  conn->semi_trx_chk = FALSE;
  conn->link_idx = link_idx;
  if (spider)
    conn->need_mon = &spider->need_mons[link_idx];
  else
    conn->need_mon = &need_mon;

  if (pthread_mutex_init(&conn->mta_conn_mutex, MY_MUTEX_INIT_FAST))
  {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_mta_conn_mutex_init;
  }

  if (
    hash_init(&conn->lock_table_hash, system_charset_info, 32, 0, 0,
              (hash_get_key) spider_ha_get_key, 0, 0)
  ) {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_init_lock_table_hash;
  }

  if ((*error_num = spider_db_connect(share, conn, link_idx)))
    goto error;
  conn->ping_time = (time_t) time((time_t*) 0);

  DBUG_RETURN(conn);

error:
  hash_free(&conn->lock_table_hash);
error_init_lock_table_hash:
  VOID(pthread_mutex_destroy(&conn->mta_conn_mutex));
error_mta_conn_mutex_init:
  my_free(conn, MYF(0));
error_alloc_conn:
  DBUG_RETURN(NULL);
}

SPIDER_CONN *spider_get_conn(
  const SPIDER_SHARE *share,
  int link_idx,
  char *conn_key,
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
        (uchar*) conn_key, share->conn_keys_lengths[link_idx]))) ||
    (!another &&
      !(conn = (SPIDER_CONN*) hash_search(&trx->trx_conn_hash,
        (uchar*) conn_key, share->conn_keys_lengths[link_idx])))
  ) {
    if (
      !trx->thd ||
      (THDVAR(trx->thd, conn_recycle_mode) & 1) ||
      THDVAR(trx->thd, conn_recycle_strict)
    ) {
      pthread_mutex_lock(&spider_conn_mutex);
      if (!(conn = (SPIDER_CONN*) hash_search(&spider_open_connections,
        (uchar*) share->conn_keys[link_idx],
        share->conn_keys_lengths[link_idx])))
      {
        pthread_mutex_unlock(&spider_conn_mutex);
        DBUG_PRINT("info",("spider create new conn"));
        if (!(conn = spider_create_conn(share, spider, link_idx, error_num)))
          goto error;
        *conn->conn_key = *conn_key;
        if (spider)
          spider->conns[link_idx] = conn;
      } else {
        hash_delete(&spider_open_connections, (uchar*) conn);
        pthread_mutex_unlock(&spider_conn_mutex);
        DBUG_PRINT("info",("spider get global conn"));
        if (spider)
          spider->conns[link_idx] = conn;
      }
    } else {
      DBUG_PRINT("info",("spider create new conn"));
      /* conn_recycle_strict = 0 and conn_recycle_mode = 0 or 2 */
      if (!(conn = spider_create_conn(share, spider, link_idx, error_num)))
        goto error;
      *conn->conn_key = *conn_key;
      if (spider)
        spider->conns[link_idx] = conn;
    }
    conn->thd = trx->thd;
    conn->priority = share->priority;

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
    spider->conns[link_idx] = conn;
  conn->link_idx = link_idx;

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

#ifndef WITHOUT_SPIDER_BG_SEARCH
int spider_set_conn_bg_param(
  ha_spider *spider
) {
  int error_num, roop_count, bgs_mode;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  THD *thd = spider->trx->thd;
  DBUG_ENTER("spider_set_conn_bg_param");
  bgs_mode =
    THDVAR(thd, bgs_mode) < 0 ?
    share->bgs_mode :
    THDVAR(thd, bgs_mode);
  if (bgs_mode == 0)
    result_list->bgs_phase = 0;
  else if (
    bgs_mode <= 2 &&
    (result_list->lock_type == F_WRLCK || spider->lock_mode == 2)
  )
    result_list->bgs_phase = 0;
  else if (bgs_mode <= 1 && spider->lock_mode == 1)
    result_list->bgs_phase = 0;
  else {
    result_list->bgs_phase = 1;

    result_list->bgs_first_read =
      THDVAR(thd, bgs_first_read) < 0 ?
      share->bgs_first_read :
      THDVAR(thd, bgs_first_read);
    result_list->bgs_second_read =
      THDVAR(thd, bgs_second_read) < 0 ?
      share->bgs_second_read :
      THDVAR(thd, bgs_second_read);
    result_list->bgs_split_read = spider_split_read_param(spider);

    result_list->split_read =
      result_list->bgs_first_read > 0 ?
      result_list->bgs_first_read :
      result_list->bgs_split_read;
  }

  if (result_list->bgs_phase > 0)
  {
    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, spider->lock_mode ?
        SPIDER_LINK_STATUS_RECOVERY : SPIDER_LINK_STATUS_OK);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, spider->lock_mode ?
        SPIDER_LINK_STATUS_RECOVERY : SPIDER_LINK_STATUS_OK)
    ) {
      if ((error_num = spider_create_conn_thread(spider->conns[roop_count])))
        DBUG_RETURN(error_num);
    }
  }
  DBUG_RETURN(0);
}

int spider_create_conn_thread(
  SPIDER_CONN *conn
) {
  int error_num;
  DBUG_ENTER("spider_create_conn_thread");
  if (!conn->bg_init)
  {
    if (pthread_mutex_init(&conn->bg_conn_sync_mutex, MY_MUTEX_INIT_FAST))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_sync_mutex_init;
    }
    if (pthread_mutex_init(&conn->bg_conn_mutex, MY_MUTEX_INIT_FAST))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_mutex_init;
    }
    if (pthread_cond_init(&conn->bg_conn_sync_cond, NULL))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_sync_cond_init;
    }
    if (pthread_cond_init(&conn->bg_conn_cond, NULL))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_cond_init;
    }
    pthread_mutex_lock(&conn->bg_conn_mutex);
    if (pthread_create(&conn->bg_thread, &spider_pt_attr,
      spider_bg_conn_action, (void *) conn)
    ) {
      pthread_mutex_unlock(&conn->bg_conn_mutex);
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_thread_create;
    }
    pthread_cond_wait(&conn->bg_conn_cond, &conn->bg_conn_mutex);
    pthread_mutex_unlock(&conn->bg_conn_mutex);
    if (!conn->bg_init)
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_thread_create;
    }
  }
  DBUG_RETURN(0);

error_thread_create:
  VOID(pthread_cond_destroy(&conn->bg_conn_cond));
error_cond_init:
  VOID(pthread_cond_destroy(&conn->bg_conn_sync_cond));
error_sync_cond_init:
  VOID(pthread_mutex_destroy(&conn->bg_conn_mutex));
error_mutex_init:
  VOID(pthread_mutex_destroy(&conn->bg_conn_sync_mutex));
error_sync_mutex_init:
  DBUG_RETURN(error_num);
}

void spider_free_conn_thread(
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_free_conn_thread");
  if (conn->bg_init)
  {
    spider_bg_conn_break(conn, NULL);
    pthread_mutex_lock(&conn->bg_conn_mutex);
    conn->bg_kill = TRUE;
    pthread_cond_signal(&conn->bg_conn_cond);
    pthread_cond_wait(&conn->bg_conn_cond, &conn->bg_conn_mutex);
    pthread_mutex_unlock(&conn->bg_conn_mutex);
    VOID(pthread_cond_destroy(&conn->bg_conn_cond));
    VOID(pthread_cond_destroy(&conn->bg_conn_sync_cond));
    VOID(pthread_mutex_destroy(&conn->bg_conn_mutex));
    VOID(pthread_mutex_destroy(&conn->bg_conn_sync_mutex));
    conn->bg_kill = FALSE;
    conn->bg_init = FALSE;
  }
  DBUG_VOID_RETURN;
}

void spider_bg_conn_break(
  SPIDER_CONN *conn,
  ha_spider *spider
) {
  DBUG_ENTER("spider_bg_conn_break");
  if (
    conn->bg_init &&
    conn->bg_thd != current_thd &&
    (
      !spider ||
      (
        spider->result_list.bgs_working &&
        conn->bg_target == spider
      )
    )
  ) {
    conn->bg_break = TRUE;
    pthread_mutex_lock(&conn->bg_conn_mutex);
    pthread_mutex_unlock(&conn->bg_conn_mutex);
    conn->bg_break = FALSE;
  }
  DBUG_VOID_RETURN;
}

void spider_bg_all_conn_break(
  ha_spider *spider
) {
  int roop_count;
  SPIDER_CONN *conn;
  SPIDER_SHARE *share = spider->share;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_bg_all_conn_break");
  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_count < share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
  ) {
    conn = spider->conns[roop_count];
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (conn && result_list->bgs_working)
      spider_bg_conn_break(conn, spider);
#endif
    if (spider->quick_targets[roop_count])
    {
      DBUG_ASSERT(spider->quick_targets[roop_count] == conn->quick_target);
      conn->quick_target = NULL;
      spider->quick_targets[roop_count] = NULL;
    }
  }
  DBUG_VOID_RETURN;
}

int spider_bg_conn_search(
  ha_spider *spider,
  int link_idx,
  bool first,
  bool discard_result
) {
  int error_num;
  SPIDER_CONN *conn = spider->conns[link_idx];
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_bg_conn_search");
  if (first)
  {
    DBUG_PRINT("info",("spider bg first search"));
    pthread_mutex_lock(&conn->bg_conn_mutex);
    result_list->bgs_working = TRUE;
    conn->bg_search = TRUE;
    conn->bg_caller_wait = TRUE;
    conn->bg_target = spider;
    conn->link_idx = link_idx;
    conn->bg_discard_result = discard_result;
    pthread_cond_signal(&conn->bg_conn_cond);
    pthread_cond_wait(&conn->bg_conn_cond, &conn->bg_conn_mutex);
    conn->bg_caller_wait = FALSE;
    if (result_list->bgs_error)
    {
      pthread_mutex_unlock(&conn->bg_conn_mutex);
      DBUG_RETURN(result_list->bgs_error);
    }
    if (!result_list->finish_flg)
    {
      DBUG_PRINT("info",("spider bg second search"));
      result_list->bgs_phase = 2;
      if (
        result_list->quick_mode == 0 ||
        !result_list->bgs_current->result
      ) {
        result_list->split_read =
          result_list->bgs_second_read > 0 ?
          result_list->bgs_second_read :
          result_list->bgs_split_read;
        result_list->sql.length(result_list->limit_pos);
        result_list->limit_num =
          result_list->internal_limit - result_list->record_num >=
          result_list->split_read ?
          result_list->split_read :
          result_list->internal_limit - result_list->record_num;
        if ((error_num = spider_db_append_limit(
          &result_list->sql,
          result_list->internal_offset + result_list->record_num,
          result_list->limit_num)) ||
          (error_num = spider_db_append_select_lock(spider))
        )
          DBUG_RETURN(error_num);
      }
      result_list->bgs_working = TRUE;
      conn->bg_search = TRUE;
      conn->bg_caller_sync_wait = TRUE;
      pthread_mutex_lock(&conn->bg_conn_sync_mutex);
      pthread_cond_signal(&conn->bg_conn_cond);
      pthread_mutex_unlock(&conn->bg_conn_mutex);
      pthread_cond_wait(&conn->bg_conn_sync_cond, &conn->bg_conn_sync_mutex);
      pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
      conn->bg_caller_sync_wait = FALSE;
    } else {
      DBUG_PRINT("info",("spider bg current->finish_flg=%s",
        result_list->current ?
        (result_list->current->finish_flg ? "TRUE" : "FALSE") : "NULL"));
      pthread_mutex_unlock(&conn->bg_conn_mutex);
    }
  } else {
    DBUG_PRINT("info",("spider bg search"));
    if (result_list->current->finish_flg)
    {
      DBUG_PRINT("info",("spider bg end of file"));
      result_list->table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(HA_ERR_END_OF_FILE);
    }
    if (result_list->bgs_working)
    {
      /* wait */
      DBUG_PRINT("info",("spider bg working wait"));
      pthread_mutex_lock(&conn->bg_conn_mutex);
      pthread_mutex_unlock(&conn->bg_conn_mutex);
    }
    if (result_list->bgs_error)
    {
      DBUG_PRINT("info",("spider bg error"));
      if (result_list->bgs_error == HA_ERR_END_OF_FILE)
      {
        result_list->current = result_list->current->next;
        result_list->current_row_num = 0;
        result_list->table->status = STATUS_NOT_FOUND;
      }
      DBUG_RETURN(result_list->bgs_error);
    }
    result_list->current = result_list->current->next;
    result_list->current_row_num = 0;
    if (result_list->current == result_list->bgs_current)
    {
      DBUG_PRINT("info",("spider bg next search"));
      if (!result_list->current->finish_flg)
      {
        pthread_mutex_lock(&conn->bg_conn_mutex);
        result_list->bgs_phase = 3;
        if (
          result_list->quick_mode == 0 ||
          !result_list->bgs_current->result
        ) {
          result_list->split_read = result_list->bgs_split_read;
          result_list->sql.length(result_list->limit_pos);
          result_list->limit_num =
            result_list->internal_limit - result_list->record_num >=
            result_list->split_read ?
            result_list->split_read :
            result_list->internal_limit - result_list->record_num;
          if ((error_num = spider_db_append_limit(
            &result_list->sql,
            result_list->internal_offset + result_list->record_num,
            result_list->limit_num)) ||
            (error_num = spider_db_append_select_lock(spider))
          )
            DBUG_RETURN(error_num);
        }
        conn->bg_target = spider;
        conn->link_idx = link_idx;
        conn->bg_discard_result = discard_result;
        result_list->bgs_working = TRUE;
        conn->bg_search = TRUE;
        conn->bg_caller_sync_wait = TRUE;
        pthread_mutex_lock(&conn->bg_conn_sync_mutex);
        pthread_cond_signal(&conn->bg_conn_cond);
        pthread_mutex_unlock(&conn->bg_conn_mutex);
        pthread_cond_wait(&conn->bg_conn_sync_cond, &conn->bg_conn_sync_mutex);
        pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
        conn->bg_caller_sync_wait = FALSE;
      }
    }
  }
  DBUG_RETURN(0);
}

void *spider_bg_conn_action(
  void *arg
) {
  int error_num;
  SPIDER_CONN *conn = (SPIDER_CONN*) arg;
  ha_spider *spider;
  SPIDER_RESULT_LIST *result_list;
  THD *thd;
  my_thread_init();
  DBUG_ENTER("spider_bg_conn_action");
  /* init start */
  if (!(thd = new THD()))
  {
    pthread_mutex_lock(&conn->bg_conn_mutex);
    pthread_cond_signal(&conn->bg_conn_cond);
    pthread_mutex_unlock(&conn->bg_conn_mutex);
    my_thread_end();
    DBUG_RETURN(NULL);
  }
  thd->thread_stack = (char*) &thd;
  thd->store_globals();
  /* lex_start(thd); */
  conn->bg_thd = thd;
  pthread_mutex_lock(&conn->bg_conn_mutex);
  pthread_cond_signal(&conn->bg_conn_cond);
  conn->bg_init = TRUE;
  /* init end */

  while (TRUE)
  {
    pthread_cond_wait(&conn->bg_conn_cond, &conn->bg_conn_mutex);
    DBUG_PRINT("info",("spider bg roop start"));
    if (conn->bg_caller_sync_wait)
    {
      pthread_mutex_lock(&conn->bg_conn_sync_mutex);
      pthread_cond_signal(&conn->bg_conn_sync_cond);
      pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
    }
    if (conn->bg_kill)
    {
      DBUG_PRINT("info",("spider bg kill start"));
      /* lex_end(thd->lex); */
      delete thd;
      pthread_cond_signal(&conn->bg_conn_cond);
      pthread_mutex_unlock(&conn->bg_conn_mutex);
      my_pthread_setspecific_ptr(THR_THD, NULL);
      my_thread_end();
      DBUG_RETURN(NULL);
    }
    if (conn->bg_search)
    {
      int tmp_pos;
      String *sql;
      DBUG_PRINT("info",("spider bg search start"));
      spider = (ha_spider*) conn->bg_target;
      result_list = &spider->result_list;
      result_list->bgs_error = 0;
      if (
        result_list->quick_mode == 0 ||
        result_list->bgs_phase == 1 ||
        !result_list->bgs_current->result
      ) {
        if (spider->share->same_db_table_name)
          sql = &result_list->sql;
        else {
          sql = &result_list->sqls[conn->link_idx];
          if (sql->copy(result_list->sql))
            result_list->bgs_error = HA_ERR_OUT_OF_MEM;
          else {
            tmp_pos = sql->length();
            sql->length(result_list->table_name_pos);
            spider_db_append_table_name_with_adjusting(sql, spider->share,
              conn->link_idx);
            sql->length(tmp_pos);
          }
        }
        if (!result_list->bgs_error)
        {
          pthread_mutex_lock(&conn->mta_conn_mutex);
          conn->need_mon = &spider->need_mons[conn->link_idx];
          conn->mta_conn_mutex_lock_already = TRUE;
          conn->mta_conn_mutex_unlock_later = TRUE;
          if (!(result_list->bgs_error =
            spider_db_set_names(spider, conn, conn->link_idx)))
          {
            if (spider_db_query(
              conn,
              sql->ptr(),
              sql->length(),
              &spider->need_mons[conn->link_idx])
            )
              result_list->bgs_error = spider_db_errorno(conn);
            else {
              if (!conn->bg_discard_result)
              {
                result_list->bgs_error =
                  spider_db_store_result(spider, conn->link_idx,
                    result_list->table);
              } else {
                result_list->bgs_error = 0;
                spider_db_discard_result(conn);
              }
            }
          }
          conn->mta_conn_mutex_lock_already = FALSE;
          conn->mta_conn_mutex_unlock_later = FALSE;
          pthread_mutex_unlock(&conn->mta_conn_mutex);
        }
      } else {
        conn->mta_conn_mutex_unlock_later = TRUE;
        result_list->bgs_error =
          spider_db_store_result(spider, conn->link_idx, result_list->table);
        conn->mta_conn_mutex_unlock_later = FALSE;
      }
      conn->bg_search = FALSE;
      result_list->bgs_working = FALSE;
      if (conn->bg_caller_wait)
        pthread_cond_signal(&conn->bg_conn_cond);
      continue;
    }
    if (conn->bg_direct_sql)
    {
      DBUG_PRINT("info",("spider bg direct sql start"));
      SPIDER_DIRECT_SQL *direct_sql = (SPIDER_DIRECT_SQL *) conn->bg_target;
      SPIDER_TRX *trx = direct_sql->trx;
      if (
        (error_num = spider_db_udf_direct_sql(direct_sql))
      ) {
        if (thd->main_da.is_error())
        {
          SPIDER_BG_DIRECT_SQL *bg_direct_sql =
            (SPIDER_BG_DIRECT_SQL *) direct_sql->parent;
          pthread_mutex_lock(direct_sql->bg_mutex);
          bg_direct_sql->bg_error = thd->main_da.sql_errno();
          strmov((char *) bg_direct_sql->bg_error_msg,
            thd->main_da.message());
          pthread_mutex_unlock(direct_sql->bg_mutex);
          thd->clear_error();
        }
      }
      if (direct_sql->modified_non_trans_table)
      {
        SPIDER_BG_DIRECT_SQL *bg_direct_sql =
          (SPIDER_BG_DIRECT_SQL *) direct_sql->parent;
        pthread_mutex_lock(direct_sql->bg_mutex);
        bg_direct_sql->modified_non_trans_table = TRUE;
        pthread_mutex_unlock(direct_sql->bg_mutex);
      }
      spider_udf_free_direct_sql_alloc(direct_sql, TRUE);
      conn->bg_direct_sql = FALSE;
      continue;
    }
    if (conn->bg_break)
    {
      DBUG_PRINT("info",("spider bg break start"));
      spider = (ha_spider*) conn->bg_target;
      result_list = &spider->result_list;
      result_list->bgs_working = FALSE;
      continue;
    }
  }
}

int spider_create_sts_thread(
  SPIDER_SHARE *share
) {
  int error_num;
  DBUG_ENTER("spider_create_sts_thread");
  if (!share->bg_sts_init)
  {
    if (pthread_cond_init(&share->bg_sts_cond, NULL))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_cond_init;
    }
    if (pthread_create(&share->bg_sts_thread, &spider_pt_attr,
      spider_bg_sts_action, (void *) share)
    ) {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_thread_create;
    }
    share->bg_sts_init = TRUE;
  }
  DBUG_RETURN(0);

error_thread_create:
  VOID(pthread_cond_destroy(&share->bg_sts_cond));
error_cond_init:
  DBUG_RETURN(error_num);
}

void spider_free_sts_thread(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_free_sts_thread");
  if (share->bg_sts_init)
  {
    pthread_mutex_lock(&share->sts_mutex);
    share->bg_sts_kill = TRUE;
    pthread_cond_signal(&share->bg_sts_cond);
    pthread_cond_wait(&share->bg_sts_cond, &share->sts_mutex);
    pthread_mutex_unlock(&share->sts_mutex);
    VOID(pthread_cond_destroy(&share->bg_sts_cond));
    share->bg_sts_thd_wait = FALSE;
    share->bg_sts_kill = FALSE;
    share->bg_sts_init = FALSE;
  }
  DBUG_VOID_RETURN;
}

void *spider_bg_sts_action(
  void *arg
) {
  SPIDER_SHARE *share = (SPIDER_SHARE*) arg;
  int error_num = 0, need_mons[share->link_count];
  ha_spider spider;
  SPIDER_CONN *conns[share->link_count];
  THD *thd;
  my_thread_init();
  DBUG_ENTER("spider_bg_sts_action");
  /* init start */
  pthread_mutex_lock(&share->sts_mutex);
  if (!(thd = new THD()))
  {
    share->bg_sts_thd_wait = FALSE;
    share->bg_sts_kill = FALSE;
    share->bg_sts_init = FALSE;
    pthread_mutex_unlock(&share->sts_mutex);
    my_thread_end();
    DBUG_RETURN(NULL);
  }
  thd->thread_stack = (char*) &thd;
  thd->store_globals();
  share->bg_sts_thd = thd;
  spider.trx = spider_global_trx;
  spider.share = share;
  spider.conns = conns;
  spider.need_mons = need_mons;
  memset(&conns, 0, sizeof(SPIDER_CONN *) * share->link_count);
  memset(&need_mons, 0, sizeof(int) * share->link_count);
  spider.search_link_idx = spider_conn_first_link_idx(thd,
    share->link_statuses, share->link_count, SPIDER_LINK_STATUS_OK);
  /* init end */

  while (TRUE)
  {
    DBUG_PRINT("info",("spider bg sts roop start"));
    if (share->bg_sts_kill)
    {
      DBUG_PRINT("info",("spider bg sts kill start"));
      delete thd;
      pthread_cond_signal(&share->bg_sts_cond);
      pthread_mutex_unlock(&share->sts_mutex);
      my_pthread_setspecific_ptr(THR_THD, NULL);
      my_thread_end();
      DBUG_RETURN(NULL);
    }
    if (spider.search_link_idx == -1)
    {
      spider.search_link_idx = spider_conn_next_link_idx(
        thd, share->link_statuses, spider.search_link_idx,
        share->link_count, SPIDER_LINK_STATUS_OK);
    }
    if (spider.search_link_idx >= 0)
    {
      if (difftime(share->bg_sts_try_time, share->sts_get_time) >=
        share->bg_sts_interval)
      {
        if (!conns[spider.search_link_idx])
        {
          pthread_mutex_lock(&spider_global_trx_mutex);
          spider_get_conn(share, 0, share->conn_keys[spider.search_link_idx],
            spider_global_trx, &spider, FALSE, FALSE, &error_num);
          pthread_mutex_unlock(&spider_global_trx_mutex);
          if (
            error_num &&
            share->monitoring_kind[spider.search_link_idx] &&
            need_mons[spider.search_link_idx]
          ) {
            lex_start(thd);
            error_num = spider_ping_table_mon_from_table(
                spider_global_trx,
                thd,
                share,
                share->monitoring_sid[spider.search_link_idx],
                share->table_name,
                share->table_name_length,
                spider.search_link_idx,
                "",
                0,
                share->monitoring_kind[spider.search_link_idx],
                share->monitoring_limit[spider.search_link_idx],
                TRUE
              );
            lex_end(thd->lex);
          }
        }
        if (conns[spider.search_link_idx])
        {
#ifdef WITH_PARTITION_STORAGE_ENGINE
          if (spider_get_sts(share, spider.search_link_idx,
            share->bg_sts_try_time, &spider,
            share->bg_sts_interval, share->bg_sts_mode,
            share->bg_sts_sync,
            2))
#else
          if (spider_get_sts(share, spider.search_link_idx,
            share->bg_sts_try_time, &spider,
            share->bg_sts_interval, share->bg_sts_mode,
            2))
#endif
          {
            if (
              share->monitoring_kind[spider.search_link_idx] &&
              need_mons[spider.search_link_idx]
            ) {
              lex_start(thd);
              error_num = spider_ping_table_mon_from_table(
                  spider_global_trx,
                  thd,
                  share,
                  share->monitoring_sid[spider.search_link_idx],
                  share->table_name,
                  share->table_name_length,
                  spider.search_link_idx,
                  "",
                  0,
                  share->monitoring_kind[spider.search_link_idx],
                  share->monitoring_limit[spider.search_link_idx],
                  TRUE
                );
              lex_end(thd->lex);
            }
            spider.search_link_idx = -1;
          }
        }
      }
    }
    memset(need_mons, 0, sizeof(int) * share->link_count);
    share->bg_sts_thd_wait = TRUE;
    pthread_cond_wait(&share->bg_sts_cond, &share->sts_mutex);
  }
}

int spider_create_crd_thread(
  SPIDER_SHARE *share
) {
  int error_num;
  DBUG_ENTER("spider_create_crd_thread");
  if (!share->bg_crd_init)
  {
    if (pthread_cond_init(&share->bg_crd_cond, NULL))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_cond_init;
    }
    if (pthread_create(&share->bg_crd_thread, &spider_pt_attr,
      spider_bg_crd_action, (void *) share)
    ) {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_thread_create;
    }
    share->bg_crd_init = TRUE;
  }
  DBUG_RETURN(0);

error_thread_create:
  VOID(pthread_cond_destroy(&share->bg_crd_cond));
error_cond_init:
  DBUG_RETURN(error_num);
}

void spider_free_crd_thread(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_free_crd_thread");
  if (share->bg_crd_init)
  {
    pthread_mutex_lock(&share->crd_mutex);
    share->bg_crd_kill = TRUE;
    pthread_cond_signal(&share->bg_crd_cond);
    pthread_cond_wait(&share->bg_crd_cond, &share->crd_mutex);
    pthread_mutex_unlock(&share->crd_mutex);
    VOID(pthread_cond_destroy(&share->bg_crd_cond));
    share->bg_crd_thd_wait = FALSE;
    share->bg_crd_kill = FALSE;
    share->bg_crd_init = FALSE;
  }
  DBUG_VOID_RETURN;
}

void *spider_bg_crd_action(
  void *arg
) {
  SPIDER_SHARE *share = (SPIDER_SHARE*) arg;
  int error_num = 0, need_mons[share->link_count];
  ha_spider spider;
  TABLE table;
  SPIDER_CONN *conns[share->link_count];
  THD *thd;
  my_thread_init();
  DBUG_ENTER("spider_bg_crd_action");
  /* init start */
  pthread_mutex_lock(&share->crd_mutex);
  if (!(thd = new THD()))
  {
    share->bg_crd_thd_wait = FALSE;
    share->bg_crd_kill = FALSE;
    share->bg_crd_init = FALSE;
    pthread_mutex_unlock(&share->crd_mutex);
    my_thread_end();
    DBUG_RETURN(NULL);
  }
  thd->thread_stack = (char*) &thd;
  thd->store_globals();
  share->bg_crd_thd = thd;
  table.s = share->table_share;
  table.field = share->table_share->field;
  table.key_info = share->table_share->key_info;
  spider.trx = spider_global_trx;
  spider.change_table_ptr(&table, share->table_share);
  spider.share = share;
  spider.conns = conns;
  spider.need_mons = need_mons;
  memset(&conns, 0, sizeof(SPIDER_CONN *) * share->link_count);
  memset(&need_mons, 0, sizeof(int) * share->link_count);
  spider.search_link_idx = spider_conn_first_link_idx(thd,
    share->link_statuses, share->link_count, SPIDER_LINK_STATUS_OK);
  /* init end */

  while (TRUE)
  {
    DBUG_PRINT("info",("spider bg crd roop start"));
    if (share->bg_crd_kill)
    {
      DBUG_PRINT("info",("spider bg crd kill start"));
      delete thd;
      pthread_cond_signal(&share->bg_crd_cond);
      pthread_mutex_unlock(&share->crd_mutex);
      my_pthread_setspecific_ptr(THR_THD, NULL);
      my_thread_end();
      DBUG_RETURN(NULL);
    }
    if (spider.search_link_idx == -1)
    {
      spider.search_link_idx = spider_conn_next_link_idx(
        thd, share->link_statuses, spider.search_link_idx,
        share->link_count, SPIDER_LINK_STATUS_OK);
    }
    if (spider.search_link_idx >= 0)
    {
      if (difftime(share->bg_crd_try_time, share->crd_get_time) >=
        share->bg_crd_interval)
      {
        if (!conns[spider.search_link_idx])
        {
          pthread_mutex_lock(&spider_global_trx_mutex);
          spider_get_conn(share, spider.search_link_idx,
            share->conn_keys[spider.search_link_idx],
            spider_global_trx, &spider, FALSE, FALSE, &error_num);
          pthread_mutex_unlock(&spider_global_trx_mutex);
          if (
            error_num &&
            share->monitoring_kind[spider.search_link_idx] &&
            need_mons[spider.search_link_idx]
          ) {
            lex_start(thd);
            error_num = spider_ping_table_mon_from_table(
                spider_global_trx,
                thd,
                share,
                share->monitoring_sid[spider.search_link_idx],
                share->table_name,
                share->table_name_length,
                spider.search_link_idx,
                "",
                0,
                share->monitoring_kind[spider.search_link_idx],
                share->monitoring_limit[spider.search_link_idx],
                TRUE
              );
            lex_end(thd->lex);
          }
        }
        if (conns[spider.search_link_idx])
        {
#ifdef WITH_PARTITION_STORAGE_ENGINE
          if (spider_get_crd(share, spider.search_link_idx,
            share->bg_crd_try_time, &spider, &table,
            share->bg_crd_interval, share->bg_crd_mode,
            share->bg_crd_sync,
            2))
#else
          if (spider_get_crd(share, spider.search_link_idx,
            share->bg_crd_try_time, &spider, &table,
            share->bg_crd_interval, share->bg_crd_mode,
            2))
#endif
          {
            if (
              share->monitoring_kind[spider.search_link_idx] &&
              need_mons[spider.search_link_idx]
            ) {
              lex_start(thd);
              error_num = spider_ping_table_mon_from_table(
                  spider_global_trx,
                  thd,
                  share,
                  share->monitoring_sid[spider.search_link_idx],
                  share->table_name,
                  share->table_name_length,
                  spider.search_link_idx,
                  "",
                  0,
                  share->monitoring_kind[spider.search_link_idx],
                  share->monitoring_limit[spider.search_link_idx],
                  TRUE
                );
              lex_end(thd->lex);
            }
            spider.search_link_idx = -1;
          }
        }
      }
    }
    memset(need_mons, 0, sizeof(int) * share->link_count);
    share->bg_crd_thd_wait = TRUE;
    pthread_cond_wait(&share->bg_crd_cond, &share->crd_mutex);
  }
}

int spider_create_mon_threads(
  SPIDER_TRX *trx,
  SPIDER_SHARE *share
) {
  bool create_bg_mons = FALSE;
  int error_num, roop_count, roop_count2;
  SPIDER_LINK_PACK link_pack;
  SPIDER_TABLE_MON_LIST *table_mon_list;
  DBUG_ENTER("spider_create_mon_threads");
  if (!share->bg_mon_init)
  {
    for (roop_count = 0; roop_count < share->link_count; roop_count++)
    {
      if (share->monitoring_bg_kind[roop_count])
      {
        create_bg_mons = TRUE;
        break;
      }
    }
    if (create_bg_mons)
    {
      char link_idx_str[SPIDER_SQL_INT_LEN];
      int link_idx_str_length;
      char buf[share->table_name_length + SPIDER_SQL_INT_LEN + 1];
      String conv_name_str(buf, share->table_name_length +
        SPIDER_SQL_INT_LEN + 1, system_charset_info);
      conv_name_str.length(0);
      conv_name_str.q_append(share->table_name, share->table_name_length);
      for (roop_count = 0; roop_count < share->link_count; roop_count++)
      {
        if (share->monitoring_bg_kind[roop_count])
        {
          conv_name_str.length(share->table_name_length);
          link_idx_str_length = my_sprintf(link_idx_str, (link_idx_str,
            "%010ld", roop_count));
          conv_name_str.q_append(link_idx_str, link_idx_str_length);
          if (!(table_mon_list = spider_get_ping_table_mon_list(trx, trx->thd,
            &conv_name_str, share->table_name_length, roop_count,
            share->monitoring_sid[roop_count], FALSE, &error_num)))
            goto error_get_ping_table_mon_list;
        }
      }
      if (!(share->bg_mon_thds = (THD **)
        my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
          &share->bg_mon_thds, sizeof(THD *) * share->link_count,
          &share->bg_mon_threads, sizeof(pthread_t) * share->link_count,
          &share->bg_mon_mutexes, sizeof(pthread_mutex_t) * share->link_count,
          &share->bg_mon_conds, sizeof(pthread_cond_t) * share->link_count,
          NullS))
      ) {
        error_num = HA_ERR_OUT_OF_MEM;
        goto error_alloc_base;
      }
      for (roop_count = 0; roop_count < share->link_count; roop_count++)
      {
        if (
          share->monitoring_bg_kind[roop_count] &&
          pthread_mutex_init(&share->bg_mon_mutexes[roop_count],
            MY_MUTEX_INIT_FAST)
        ) {
          error_num = HA_ERR_OUT_OF_MEM;
          goto error_mutex_init;
        }
      }
      for (roop_count = 0; roop_count < share->link_count; roop_count++)
      {
        if (
          share->monitoring_bg_kind[roop_count] &&
          pthread_cond_init(&share->bg_mon_conds[roop_count], NULL)
        ) {
          error_num = HA_ERR_OUT_OF_MEM;
          goto error_cond_init;
        }
      }
      link_pack.share = share;
      for (roop_count = 0; roop_count < share->link_count; roop_count++)
      {
        if (share->monitoring_bg_kind[roop_count])
        {
          link_pack.link_idx = roop_count;
          pthread_mutex_lock(&share->bg_mon_mutexes[roop_count]);
          if (pthread_create(&share->bg_mon_threads[roop_count],
            &spider_pt_attr, spider_bg_mon_action, (void *) &link_pack)
          ) {
            error_num = HA_ERR_OUT_OF_MEM;
            goto error_thread_create;
          }
          pthread_cond_wait(&share->bg_mon_conds[roop_count],
            &share->bg_mon_mutexes[roop_count]);
          pthread_mutex_unlock(&share->bg_mon_mutexes[roop_count]);
        }
      }
      share->bg_mon_init = TRUE;
    }
  }
  DBUG_RETURN(0);

error_thread_create:
  roop_count2 = roop_count;
  for (roop_count--; roop_count >= 0; roop_count--)
  {
    if (share->monitoring_bg_kind[roop_count])
      pthread_mutex_lock(&share->bg_mon_mutexes[roop_count]);
  }
  share->bg_mon_kill = TRUE;
  for (roop_count = roop_count2 - 1; roop_count >= 0; roop_count--)
  {
    if (share->monitoring_bg_kind[roop_count])
    {
      pthread_cond_wait(&share->bg_mon_conds[roop_count],
        &share->bg_mon_mutexes[roop_count]);
      pthread_mutex_unlock(&share->bg_mon_mutexes[roop_count]);
    }
  }
  share->bg_mon_kill = FALSE;
  roop_count = share->link_count;
error_cond_init:
  for (roop_count--; roop_count >= 0; roop_count--)
  {
    if (share->monitoring_bg_kind[roop_count])
      VOID(pthread_cond_destroy(&share->bg_mon_conds[roop_count]));
  }
  roop_count = share->link_count;
error_mutex_init:
  for (roop_count--; roop_count >= 0; roop_count--)
  {
    if (share->monitoring_bg_kind[roop_count])
      VOID(pthread_mutex_destroy(&share->bg_mon_mutexes[roop_count]));
  }
  my_free(share->bg_mon_thds, MYF(0));
error_alloc_base:
error_get_ping_table_mon_list:
  DBUG_RETURN(error_num);
}

void spider_free_mon_threads(
  SPIDER_SHARE *share
) {
  int roop_count;
  DBUG_ENTER("spider_free_mon_threads");
  if (share->bg_mon_init)
  {
    for (roop_count = 0; roop_count < share->link_count; roop_count++)
    {
      if (share->monitoring_bg_kind[roop_count])
        pthread_mutex_lock(&share->bg_mon_mutexes[roop_count]);
    }
    share->bg_mon_kill = TRUE;
    for (roop_count = 0; roop_count < share->link_count; roop_count++)
    {
      if (share->monitoring_bg_kind[roop_count])
      {
        pthread_cond_wait(&share->bg_mon_conds[roop_count],
          &share->bg_mon_mutexes[roop_count]);
        pthread_mutex_unlock(&share->bg_mon_mutexes[roop_count]);
        VOID(pthread_cond_destroy(&share->bg_mon_conds[roop_count]));
        VOID(pthread_mutex_destroy(&share->bg_mon_mutexes[roop_count]));
      }
    }
    my_free(share->bg_mon_thds, MYF(0));
    share->bg_mon_kill = FALSE;
    share->bg_mon_init = FALSE;
  }
  DBUG_VOID_RETURN;
}

void *spider_bg_mon_action(
  void *arg
) {
  SPIDER_LINK_PACK *link_pack = (SPIDER_LINK_PACK*) arg;
  SPIDER_SHARE *share = link_pack->share;
  int error_num, link_idx = link_pack->link_idx;
  THD *thd;
  my_thread_init();
  DBUG_ENTER("spider_bg_mon_action");
  /* init start */
  pthread_mutex_lock(&share->bg_mon_mutexes[link_idx]);
  if (!(thd = new THD()))
  {
    share->bg_mon_kill = FALSE;
    share->bg_mon_init = FALSE;
    pthread_cond_signal(&share->bg_mon_conds[link_idx]);
    pthread_mutex_unlock(&share->bg_mon_mutexes[link_idx]);
    my_thread_end();
    DBUG_RETURN(NULL);
  }
  thd->thread_stack = (char*) &thd;
  thd->store_globals();
  share->bg_mon_thds[link_idx] = thd;
  pthread_cond_signal(&share->bg_mon_conds[link_idx]);
  pthread_mutex_unlock(&share->bg_mon_mutexes[link_idx]);
  /* init end */

  while (TRUE)
  {
    DBUG_PRINT("info",("spider bg mon sleep %lld",
      share->monitoring_bg_interval[link_idx]));
    my_sleep((ulong) share->monitoring_bg_interval[link_idx]);
    DBUG_PRINT("info",("spider bg mon roop start"));
    if (share->bg_mon_kill)
    {
      DBUG_PRINT("info",("spider bg mon kill start"));
      pthread_mutex_lock(&share->bg_mon_mutexes[link_idx]);
      pthread_cond_signal(&share->bg_mon_conds[link_idx]);
      pthread_mutex_unlock(&share->bg_mon_mutexes[link_idx]);
      delete thd;
      my_pthread_setspecific_ptr(THR_THD, NULL);
      my_thread_end();
      DBUG_RETURN(NULL);
    }
    if (share->monitoring_bg_kind[link_idx])
    {
      lex_start(thd);
      error_num = spider_ping_table_mon_from_table(
        spider_global_trx,
        thd,
        share,
        share->monitoring_sid[link_idx],
        share->table_name,
        share->table_name_length,
        link_idx,
        "",
        0,
        share->monitoring_bg_kind[link_idx],
        share->monitoring_limit[link_idx],
        TRUE
      );
      lex_end(thd->lex);
    }
  }
}
#endif

int spider_conn_first_link_idx(
  THD *thd,
  long *link_statuses,
  int link_count,
  int link_status
) {
  int roop_count, active_links = 0, link_idxs[link_count];
  DBUG_ENTER("spider_conn_first_link_idx");
  for (roop_count = 0; roop_count < link_count; roop_count++)
  {
    if (link_statuses[roop_count] <= link_status)
    {
      link_idxs[active_links] = roop_count;
      active_links++;
    }
  }

  if (active_links == 0)
  {
    DBUG_PRINT("info",("spider all links are failed"));
    DBUG_RETURN(-1);
  }

  DBUG_PRINT("info",("spider first link_idx=%d",
    link_idxs[(thd->server_id + thd_get_thread_id(thd)) % active_links]));
  DBUG_RETURN(link_idxs[(thd->server_id + thd_get_thread_id(thd)) %
    active_links]);
}

int spider_conn_next_link_idx(
  THD *thd,
  long *link_statuses,
  int link_idx,
  int link_count,
  int link_status
) {
  int tmp_link_idx;
  DBUG_ENTER("spider_conn_next_link_idx");
  tmp_link_idx = spider_conn_first_link_idx(thd, link_statuses, link_count,
    link_status);
  if (
    tmp_link_idx >= 0 &&
    tmp_link_idx == link_idx
  ) {
    do {
      tmp_link_idx++;
      if (tmp_link_idx >= link_count)
        tmp_link_idx = 0;
      if (tmp_link_idx == link_idx)
        break;
    } while (link_statuses[tmp_link_idx] > link_status);
    DBUG_PRINT("info",("spider next link_idx=%d", tmp_link_idx));
    DBUG_RETURN(tmp_link_idx);
  }
  DBUG_PRINT("info",("spider next link_idx=%d", tmp_link_idx));
  DBUG_RETURN(tmp_link_idx);
}

int spider_conn_link_idx_next(
  long *link_statuses,
  int link_idx,
  int link_count,
  int link_status
) {
  DBUG_ENTER("spider_conn_link_idx_next");
  do {
    link_idx++;
    if (link_idx >= link_count)
      break;
  } while (link_statuses[link_idx] > link_status);
  DBUG_PRINT("info",("spider link_idx=%d", link_idx));
  DBUG_RETURN(link_idx);
}

int spider_conn_lock_mode(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  DBUG_ENTER("spider_conn_lock_mode");
  if (result_list->lock_type == F_WRLCK || spider->lock_mode == 2)
    DBUG_RETURN(SPIDER_LOCK_MODE_EXCLUSIVE);
  else if (spider->lock_mode == 1)
    DBUG_RETURN(SPIDER_LOCK_MODE_SHARED);
  DBUG_RETURN(SPIDER_LOCK_MODE_NO_LOCK);
}

bool spider_conn_check_recovery_link(
  SPIDER_SHARE *share
) {
  int roop_count;
  DBUG_ENTER("spider_check_recovery_link");
  for (roop_count = 0; roop_count < share->link_count; roop_count++)
  {
    if (share->link_statuses[roop_count] == SPIDER_LINK_STATUS_RECOVERY)
      DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}
