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
      conn->ping_query_id = 0;
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

  if ((*error_num = spider_db_connect(share, conn)))
    goto error;

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
      !trx->thd ||
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
        if (spider)
          spider->conn = conn;
      } else {
        hash_delete(&spider_open_connections, (uchar*) conn);
        pthread_mutex_unlock(&spider_conn_mutex);
        DBUG_PRINT("info",("spider get global conn"));
        if (spider)
        {
          spider->conn = conn;
          if (
            trx->thd &&
            !conn->trx_start &&
            (*error_num = spider_db_ping(spider))
          )
            goto error;
        }
      }
    } else {
      DBUG_PRINT("info",("spider create new conn"));
      /* conn_recycle_strict = 0 and conn_recycle_mode = 0 or 2 */
      if(!(conn = spider_create_conn(share, error_num)))
        goto error;
      if (spider)
        spider->conn = conn;
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
  {
    spider->conn = conn;
    if (
      trx->thd &&
      !conn->trx_start &&
      (*error_num = spider_db_ping(spider))
    )
      goto error;
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
void spider_set_conn_bg_param(
  ha_spider *spider
) {
  int bgs_mode;
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
    result_list->bgs_split_read =
      THDVAR(thd, split_read) < 0 ?
      share->split_read :
      THDVAR(thd, split_read);

    result_list->split_read =
      result_list->bgs_first_read > 0 ?
      result_list->bgs_first_read :
      result_list->bgs_split_read;
  }
  DBUG_VOID_RETURN;
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

int spider_bg_conn_search(
  ha_spider *spider,
  bool first
) {
  int error_num;
  SPIDER_CONN *conn = spider->conn;
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
    } else
      pthread_mutex_unlock(&conn->bg_conn_mutex);
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
      pthread_cond_signal(&conn->bg_conn_cond);
      pthread_mutex_unlock(&conn->bg_conn_mutex);
      /* lex_end(thd->lex); */
      delete thd;
      my_pthread_setspecific_ptr(THR_THD, NULL);
      my_thread_end();
      DBUG_RETURN(NULL);
    }
    if (conn->bg_search)
    {
      DBUG_PRINT("info",("spider bg search start"));
      spider = (ha_spider*) conn->bg_target;
      result_list = &spider->result_list;
      if (
        result_list->quick_mode == 0 ||
        result_list->bgs_phase == 1 ||
        !result_list->bgs_current->result
      ) {
        if (spider_db_query(
          conn,
          result_list->sql.ptr(),
          result_list->sql.length())
        )
          result_list->bgs_error = spider_db_errorno(conn);
        else {
          result_list->bgs_error =
            spider_db_store_result(spider, result_list->table);
        }
      } else {
        result_list->bgs_error =
          spider_db_store_result(spider, result_list->table);
      }
      conn->bg_search = FALSE;
      result_list->bgs_working = FALSE;
      if (conn->bg_caller_wait)
        pthread_cond_signal(&conn->bg_conn_cond);
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
  int error_num;
  ha_spider spider;
  SPIDER_CONN *conn = NULL;
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
  /* init end */

  while (TRUE)
  {
    DBUG_PRINT("info",("spider bg sts roop start"));
    if (share->bg_sts_kill)
    {
      DBUG_PRINT("info",("spider bg sts kill start"));
      pthread_cond_signal(&share->bg_sts_cond);
      pthread_mutex_unlock(&share->sts_mutex);
      delete thd;
      my_pthread_setspecific_ptr(THR_THD, NULL);
      my_thread_end();
      DBUG_RETURN(NULL);
    }
    if (difftime(share->bg_sts_try_time, share->sts_get_time) >=
      share->bg_sts_interval)
    {
      if (!conn)
      {
        pthread_mutex_lock(&spider_global_trx_mutex);
        spider.conn = conn = spider_get_conn(share, spider_global_trx,
          &spider, FALSE, FALSE, &error_num);
        pthread_mutex_unlock(&spider_global_trx_mutex);
      }
      if (conn)
      {
        pthread_mutex_lock(&conn->mta_conn_mutex);
#ifdef WITH_PARTITION_STORAGE_ENGINE
        VOID(spider_get_sts(share, share->bg_sts_try_time, &spider,
          share->bg_sts_interval, share->bg_sts_mode,
          share->bg_sts_sync,
          2));
#else
        VOID(spider_get_sts(share, share->bg_sts_try_time, &spider,
          share->bg_sts_interval, share->bg_sts_mode,
          2));
#endif
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      }
    }
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
  int error_num;
  ha_spider spider;
  TABLE table;
  SPIDER_CONN *conn = NULL;
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
  spider.trx = spider_global_trx;
  spider.change_table_ptr(&table, share->table_share);
  spider.share = share;
  /* init end */

  while (TRUE)
  {
    DBUG_PRINT("info",("spider bg crd roop start"));
    if (share->bg_crd_kill)
    {
      DBUG_PRINT("info",("spider bg crd kill start"));
      pthread_cond_signal(&share->bg_crd_cond);
      pthread_mutex_unlock(&share->crd_mutex);
      delete thd;
      my_pthread_setspecific_ptr(THR_THD, NULL);
      my_thread_end();
      DBUG_RETURN(NULL);
    }
    if (difftime(share->bg_crd_try_time, share->crd_get_time) >=
      share->bg_crd_interval)
    {
      if (!conn)
      {
        pthread_mutex_lock(&spider_global_trx_mutex);
        spider.conn = conn = spider_get_conn(share, spider_global_trx,
          &spider, FALSE, FALSE, &error_num);
        pthread_mutex_unlock(&spider_global_trx_mutex);
      }
      if (conn)
      {
        pthread_mutex_lock(&conn->mta_conn_mutex);
#ifdef WITH_PARTITION_STORAGE_ENGINE
        VOID(spider_get_crd(share, share->bg_crd_try_time, &spider, &table,
          share->bg_crd_interval, share->bg_crd_mode,
          share->bg_crd_sync,
          2));
#else
        VOID(spider_get_crd(share, share->bg_crd_try_time, &spider, &table,
          share->bg_crd_interval, share->bg_crd_mode,
          2));
#endif
        pthread_mutex_unlock(&conn->mta_conn_mutex);
      }
    }
    share->bg_crd_thd_wait = TRUE;
    pthread_cond_wait(&share->bg_crd_cond, &share->crd_mutex);
  }
}
#endif
