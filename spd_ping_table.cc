/* Copyright (C) 2009-2011 Kentoku Shiba

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
#endif
#include "spd_err.h"
#include "spd_param.h"
#include "spd_db_include.h"
#include "spd_include.h"
#include "ha_spider.h"
#include "spd_db_conn.h"
#include "spd_trx.h"
#include "spd_conn.h"
#include "spd_sys_table.h"
#include "spd_table.h"
#include "spd_ping_table.h"
#include "spd_direct_sql.h"
#include "spd_udf.h"

#ifdef HAVE_PSI_INTERFACE
extern PSI_mutex_key spd_key_mutex_mon_list_caller;
extern PSI_mutex_key spd_key_mutex_mon_list_receptor;
extern PSI_mutex_key spd_key_mutex_mon_list_monitor;
extern PSI_mutex_key spd_key_mutex_mon_list_update_status;
#endif

#ifndef WITHOUT_SPIDER_BG_SEARCH
extern pthread_mutex_t spider_global_trx_mutex;
extern SPIDER_TRX *spider_global_trx;
#endif

HASH *spider_udf_table_mon_list_hash;
pthread_mutex_t *spider_udf_table_mon_mutexes;
pthread_cond_t *spider_udf_table_mon_conds;

SPIDER_TABLE_MON_LIST *spider_get_ping_table_mon_list(
  SPIDER_TRX *trx,
  THD *thd,
  String *str,
  uint conv_name_length,
  int link_idx,
  uint32 server_id,
  bool need_lock,
  int *error_num
) {
  uint mutex_hash;
  SPIDER_TABLE_MON_LIST *table_mon_list;
  DBUG_ENTER("spider_get_ping_table_mon_list");
  mutex_hash = spider_udf_calc_hash(str->c_ptr(),
    spider_udf_table_mon_mutex_count);
  DBUG_PRINT("info",("spider hash key=%s", str->c_ptr()));
  DBUG_PRINT("info",("spider hash key length=%ld", str->length()));
  pthread_mutex_lock(&spider_udf_table_mon_mutexes[mutex_hash]);
  if (!(table_mon_list = (SPIDER_TABLE_MON_LIST *) my_hash_search(
    &spider_udf_table_mon_list_hash[mutex_hash],
    (uchar*) str->c_ptr(), str->length())))
  {
    DBUG_ASSERT(trx != spider_global_trx);
    if (!(table_mon_list = spider_get_ping_table_tgt(thd, str->c_ptr(),
      conv_name_length, link_idx, server_id, str, need_lock, error_num)))
    {
      pthread_mutex_unlock(&spider_udf_table_mon_mutexes[mutex_hash]);
      goto error;
    }
    table_mon_list->mutex_hash = mutex_hash;
    if (my_hash_insert(&spider_udf_table_mon_list_hash[mutex_hash],
      (uchar*) table_mon_list))
    {
      spider_ping_table_free_mon_list(table_mon_list);
      *error_num = HA_ERR_OUT_OF_MEM;
      my_error(HA_ERR_OUT_OF_MEM, MYF(0));
      pthread_mutex_unlock(&spider_udf_table_mon_mutexes[mutex_hash]);
      goto error;
    }
  }
  table_mon_list->use_count++;
  DBUG_PRINT("info",("spider table_mon_list->use_count=%d", table_mon_list->use_count));
  pthread_mutex_unlock(&spider_udf_table_mon_mutexes[mutex_hash]);
  DBUG_RETURN(table_mon_list);

error:
  DBUG_RETURN(NULL);
}

void spider_free_ping_table_mon_list(
  SPIDER_TABLE_MON_LIST *table_mon_list
) {
  DBUG_ENTER("spider_free_ping_table_mon_list");
  pthread_mutex_lock(&spider_udf_table_mon_mutexes[
    table_mon_list->mutex_hash]);
  table_mon_list->use_count--;
  DBUG_PRINT("info",("spider table_mon_list->use_count=%d", table_mon_list->use_count));
  if (!table_mon_list->use_count)
    pthread_cond_broadcast(&spider_udf_table_mon_conds[
      table_mon_list->mutex_hash]);
  pthread_mutex_unlock(&spider_udf_table_mon_mutexes[
    table_mon_list->mutex_hash]);
  DBUG_VOID_RETURN;
}

void spider_release_ping_table_mon_list(
  const char *conv_name,
  uint conv_name_length,
  int link_idx
) {
  uint mutex_hash;
  SPIDER_TABLE_MON_LIST *table_mon_list;
  char link_idx_str[SPIDER_SQL_INT_LEN];
  int link_idx_str_length;
  DBUG_ENTER("spider_release_ping_table_mon_list");
  DBUG_PRINT("info", ("spider conv_name=%s", conv_name));
  DBUG_PRINT("info", ("spider conv_name_length=%u", conv_name_length));
  DBUG_PRINT("info", ("spider link_idx=%d", link_idx));
  link_idx_str_length = my_sprintf(link_idx_str, (link_idx_str, "%010ld",
    link_idx));
#ifdef _MSC_VER
  String conv_name_str(conv_name_length + link_idx_str_length + 1);
  conv_name_str.set_charset(system_charset_info);
#else
  char buf[conv_name_length + link_idx_str_length + 1];
  String conv_name_str(buf, conv_name_length + link_idx_str_length + 1,
    system_charset_info);
#endif
  conv_name_str.length(0);
  conv_name_str.q_append(conv_name, conv_name_length);
  conv_name_str.q_append(link_idx_str, link_idx_str_length);

  mutex_hash = spider_udf_calc_hash(conv_name_str.c_ptr_safe(),
    spider_udf_table_mon_mutex_count);
  pthread_mutex_lock(&spider_udf_table_mon_mutexes[mutex_hash]);
  if ((table_mon_list = (SPIDER_TABLE_MON_LIST *) my_hash_search(
    &spider_udf_table_mon_list_hash[mutex_hash],
    (uchar*) conv_name_str.c_ptr(), conv_name_str.length())))
  {
    while (TRUE)
    {
      if (table_mon_list->use_count)
        pthread_cond_wait(&spider_udf_table_mon_conds[mutex_hash],
          &spider_udf_table_mon_mutexes[mutex_hash]);
      else {
        my_hash_delete(&spider_udf_table_mon_list_hash[mutex_hash],
          (uchar*) table_mon_list);
        spider_ping_table_free_mon_list(table_mon_list);
        break;
      }
    }
  }
  pthread_mutex_unlock(&spider_udf_table_mon_mutexes[mutex_hash]);
  DBUG_VOID_RETURN;
}

int spider_get_ping_table_mon(
  THD *thd,
  SPIDER_TABLE_MON_LIST *table_mon_list,
  char *name,
  uint name_length,
  int link_idx,
  uint32 server_id,
  MEM_ROOT *mem_root,
  bool need_lock
) {
  int error_num;
  TABLE *table_link_mon = NULL;
#if MYSQL_VERSION_ID < 50500
  Open_tables_state open_tables_backup;
#else
  Open_tables_backup open_tables_backup;
#endif
  char table_key[MAX_KEY_LENGTH];
  SPIDER_TABLE_MON *table_mon, *table_mon_prev = NULL;
  SPIDER_SHARE *tmp_share;
  char **tmp_connect_info, *tmp_ptr;
  uint *tmp_connect_info_length;
  long *tmp_long;
  longlong *tmp_longlong;
  int list_size = 0;
  DBUG_ENTER("spider_get_ping_table_mon");

  if (
    !(table_link_mon = spider_open_sys_table(
      thd, SPIDER_SYS_LINK_MON_TABLE_NAME_STR,
      SPIDER_SYS_LINK_MON_TABLE_NAME_LEN, FALSE, &open_tables_backup,
      need_lock, &error_num))
  ) {
    my_error(error_num, MYF(0));
    goto error;
  }
  spider_store_tables_name(table_link_mon, name, name_length);
  spider_store_tables_link_idx(table_link_mon, link_idx);
  if ((error_num = spider_get_sys_table_by_idx(table_link_mon, table_key,
    table_link_mon->s->primary_key, 3)))
  {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table_link_mon->file->print_error(error_num, MYF(0));
      goto error;
    }
  } else
    goto create_table_mon;
  if (link_idx > 0)
  {
    spider_store_tables_link_idx(table_link_mon, 0);
    if ((error_num = spider_get_sys_table_by_idx(table_link_mon, table_key,
      table_link_mon->s->primary_key, 3)))
    {
      if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
      {
        table_link_mon->file->print_error(error_num, MYF(0));
        goto error;
      }
    } else
      goto create_table_mon;
  }
  if ((tmp_ptr = strstr(name, "#P#")))
  {
    *tmp_ptr = '\0';
    spider_store_tables_name(table_link_mon, name, strlen(name));
    spider_store_tables_link_idx(table_link_mon, link_idx);
    *tmp_ptr = '#';
    if ((error_num = spider_get_sys_table_by_idx(table_link_mon, table_key,
      table_link_mon->s->primary_key, 3)))
    {
      if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
      {
        table_link_mon->file->print_error(error_num, MYF(0));
        goto error;
      }
    } else
      goto create_table_mon;

    if (link_idx > 0)
    {
      spider_store_tables_link_idx(table_link_mon, 0);
      if ((error_num = spider_get_sys_table_by_idx(table_link_mon, table_key,
        table_link_mon->s->primary_key, 3)))
      {
        if (
          error_num != HA_ERR_KEY_NOT_FOUND &&
          error_num != HA_ERR_END_OF_FILE
        ) {
          table_link_mon->file->print_error(error_num, MYF(0));
          goto error;
        }
      } else
        goto create_table_mon;
    }
  }
  table_link_mon->file->print_error(error_num, MYF(0));
  goto error;

create_table_mon:
  do {
    if (!(table_mon = (SPIDER_TABLE_MON *)
      my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
        &table_mon, sizeof(SPIDER_TABLE_MON),
        &tmp_share, sizeof(SPIDER_SHARE),
        &tmp_connect_info, sizeof(char *) * 15,
        &tmp_connect_info_length, sizeof(uint) * 15,
        &tmp_long, sizeof(long) * 5,
        &tmp_longlong, sizeof(longlong) * 3,
        NullS))
    ) {
      spider_sys_index_end(table_link_mon);
      error_num = HA_ERR_OUT_OF_MEM;
      my_error(HA_ERR_OUT_OF_MEM, MYF(0));
      goto error;
    }
    spider_set_tmp_share_pointer(tmp_share, tmp_connect_info,
      tmp_connect_info_length, tmp_long, tmp_longlong);
    tmp_share->link_statuses[0] = -1;
    table_mon->share = tmp_share;
    if (table_mon_prev)
      table_mon_prev->next = table_mon;
    else
      table_mon_list->first = table_mon;
    table_mon_prev = table_mon;
    if (
      (error_num = spider_get_sys_link_mon_server_id(
        table_link_mon, &table_mon->server_id, mem_root)) ||
      (error_num = spider_get_sys_link_mon_connect_info(
        table_link_mon, tmp_share, 0, mem_root))
    ) {
      table_link_mon->file->print_error(error_num, MYF(0));
      spider_sys_index_end(table_link_mon);
      goto error;
    }
    if (
      (error_num = spider_set_connect_info_default(
        tmp_share,
#ifdef WITH_PARTITION_STORAGE_ENGINE
        NULL,
        NULL,
#endif
        NULL
      )) ||
      (error_num = spider_set_connect_info_default_dbtable(
        tmp_share, name, name_length
      )) ||
      (error_num = spider_create_conn_keys(tmp_share))
    ) {
      spider_sys_index_end(table_link_mon);
      goto error;
    }
    if (table_mon->server_id == server_id)
      table_mon_list->current = table_mon;
    list_size++;
    error_num = spider_sys_index_next_same(table_link_mon, table_key);
  } while (error_num == 0);
  spider_sys_index_end(table_link_mon);
  spider_close_sys_table(thd, table_link_mon,
    &open_tables_backup, need_lock);
  table_link_mon = NULL;
  table_mon_list->list_size = list_size;

  if (!table_mon_list->current)
  {
    error_num = ER_SPIDER_UDF_PING_TABLE_NO_SERVER_ID_NUM;
    my_printf_error(ER_SPIDER_UDF_PING_TABLE_NO_SERVER_ID_NUM,
      ER_SPIDER_UDF_PING_TABLE_NO_SERVER_ID_STR, MYF(0));
    goto error;
  }

  DBUG_RETURN(0);

error:
  if (table_link_mon)
    spider_close_sys_table(thd, table_link_mon,
      &open_tables_backup, need_lock);
  table_mon = table_mon_list->first;
  table_mon_list->first = NULL;
  table_mon_list->current = NULL;
  while (table_mon)
  {
    spider_free_tmp_share_alloc(table_mon->share);
    table_mon_prev = table_mon->next;
    my_free(table_mon, MYF(0));
    table_mon = table_mon_prev;
  }
  DBUG_RETURN(error_num);
}

SPIDER_TABLE_MON_LIST *spider_get_ping_table_tgt(
  THD *thd,
  char *name,
  uint name_length,
  int link_idx,
  uint32 server_id,
  String *str,
  bool need_lock,
  int *error_num
) {
  TABLE *table_tables = NULL;
#if MYSQL_VERSION_ID < 50500
  Open_tables_state open_tables_backup;
#else
  Open_tables_backup open_tables_backup;
#endif
  char table_key[MAX_KEY_LENGTH];

  SPIDER_TABLE_MON_LIST *table_mon_list = NULL;
  SPIDER_SHARE *tmp_share;
  char **tmp_connect_info;
  uint *tmp_connect_info_length;
  long *tmp_long;
  longlong *tmp_longlong;
  char *key_str;
  MEM_ROOT mem_root;
  DBUG_ENTER("spider_get_ping_table_tgt");

  init_alloc_root(&mem_root, 4096, 0);
  if (!(table_mon_list = (SPIDER_TABLE_MON_LIST *)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &table_mon_list, sizeof(SPIDER_TABLE_MON_LIST),
      &tmp_share, sizeof(SPIDER_SHARE),
      &tmp_connect_info, sizeof(char *) * 15,
      &tmp_connect_info_length, sizeof(uint) * 15,
      &tmp_long, sizeof(long) * 5,
      &tmp_longlong, sizeof(longlong) * 3,
      &key_str, str->length() + 1,
      NullS))
  ) {
    my_error(HA_ERR_OUT_OF_MEM, MYF(0));
    goto error;
  }
  spider_set_tmp_share_pointer(tmp_share, tmp_connect_info,
    tmp_connect_info_length, tmp_long, tmp_longlong);
  table_mon_list->share = tmp_share;
  table_mon_list->key = key_str;
  table_mon_list->key_length = str->length();
  memcpy(key_str, str->ptr(), table_mon_list->key_length);
  tmp_share->access_charset = thd->variables.character_set_client;

  if (
    !(table_tables = spider_open_sys_table(
      thd, SPIDER_SYS_TABLES_TABLE_NAME_STR,
      SPIDER_SYS_TABLES_TABLE_NAME_LEN, FALSE, &open_tables_backup, need_lock,
      error_num))
  ) {
    my_error(*error_num, MYF(0));
    goto error;
  }
  spider_store_tables_name(table_tables, name, name_length);
  spider_store_tables_link_idx(table_tables, link_idx);
  if (
    (*error_num = spider_check_sys_table(table_tables, table_key)) ||
    (*error_num = spider_get_sys_tables_connect_info(
      table_tables, tmp_share, 0, &mem_root)) ||
    (*error_num = spider_get_sys_tables_link_status(
      table_tables, tmp_share, 0, &mem_root))
  ) {
    table_tables->file->print_error(*error_num, MYF(0));
    goto error;
  }
  spider_close_sys_table(thd, table_tables,
    &open_tables_backup, need_lock);
  table_tables = NULL;

  if (
    (*error_num = spider_set_connect_info_default(
      tmp_share,
#ifdef WITH_PARTITION_STORAGE_ENGINE
      NULL,
      NULL,
#endif
      NULL
    )) ||
    (*error_num = spider_set_connect_info_default_dbtable(
      tmp_share, name, name_length
    )) ||
    (*error_num = spider_create_conn_keys(tmp_share)) ||
    (*error_num = spider_db_create_table_names_str(tmp_share)) ||
    (*error_num = spider_get_ping_table_mon(
      thd, table_mon_list, name, name_length, link_idx, server_id, &mem_root,
      need_lock))
  )
    goto error;

  if (tmp_share->link_statuses[0] == SPIDER_LINK_STATUS_NG)
    table_mon_list->mon_status = SPIDER_LINK_MON_NG;

#if MYSQL_VERSION_ID < 50500
  if (pthread_mutex_init(&table_mon_list->caller_mutex, MY_MUTEX_INIT_FAST))
#else
  if (mysql_mutex_init(spd_key_mutex_mon_list_caller,
    &table_mon_list->caller_mutex, MY_MUTEX_INIT_FAST))
#endif
  {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_caller_mutex_init;
  }
#if MYSQL_VERSION_ID < 50500
  if (pthread_mutex_init(&table_mon_list->receptor_mutex, MY_MUTEX_INIT_FAST))
#else
  if (mysql_mutex_init(spd_key_mutex_mon_list_receptor,
    &table_mon_list->receptor_mutex, MY_MUTEX_INIT_FAST))
#endif
  {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_receptor_mutex_init;
  }
#if MYSQL_VERSION_ID < 50500
  if (pthread_mutex_init(&table_mon_list->monitor_mutex, MY_MUTEX_INIT_FAST))
#else
  if (mysql_mutex_init(spd_key_mutex_mon_list_monitor,
    &table_mon_list->monitor_mutex, MY_MUTEX_INIT_FAST))
#endif
  {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_monitor_mutex_init;
  }
#if MYSQL_VERSION_ID < 50500
  if (pthread_mutex_init(&table_mon_list->update_status_mutex,
    MY_MUTEX_INIT_FAST))
#else
  if (mysql_mutex_init(spd_key_mutex_mon_list_update_status,
    &table_mon_list->update_status_mutex, MY_MUTEX_INIT_FAST))
#endif
  {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_update_status_mutex_init;
  }

  free_root(&mem_root, MYF(0));
  DBUG_RETURN(table_mon_list);

error_update_status_mutex_init:
  pthread_mutex_destroy(&table_mon_list->monitor_mutex);
error_monitor_mutex_init:
  pthread_mutex_destroy(&table_mon_list->receptor_mutex);
error_receptor_mutex_init:
  pthread_mutex_destroy(&table_mon_list->caller_mutex);
error_caller_mutex_init:
error:
  if (table_tables)
    spider_close_sys_table(thd, table_tables,
      &open_tables_backup, need_lock);
  free_root(&mem_root, MYF(0));
  if (table_mon_list)
  {
    spider_free_tmp_share_alloc(table_mon_list->share);
    my_free(table_mon_list, MYF(0));
  }
  DBUG_RETURN(NULL);
}

SPIDER_CONN *spider_get_ping_table_tgt_conn(
  SPIDER_TRX *trx,
  SPIDER_SHARE *share,
  int *error_num
) {
  SPIDER_CONN *conn;
  DBUG_ENTER("spider_get_ping_table_tgt_conn");
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (trx == spider_global_trx)
    pthread_mutex_lock(&spider_global_trx_mutex);
#endif
  if (
    !(conn = spider_get_conn(
      share, 0, share->conn_keys[0], trx, NULL, FALSE, FALSE,
      SPIDER_CONN_KIND_MYSQL, error_num))
  ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (trx == spider_global_trx)
      pthread_mutex_unlock(&spider_global_trx_mutex);
#endif
    my_error(ER_CONNECT_TO_FOREIGN_DATA_SOURCE, MYF(0),
      share->server_names[0]);
    *error_num = ER_CONNECT_TO_FOREIGN_DATA_SOURCE;
    goto error;
  }
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (trx == spider_global_trx)
    pthread_mutex_unlock(&spider_global_trx_mutex);
#endif
  DBUG_RETURN(conn);

error:
  DBUG_RETURN(NULL);
}

long long spider_ping_table_body(
  UDF_INIT *initid,
  UDF_ARGS *args,
  char *is_null,
  char *error
) {
  int error_num = 0, link_idx, flags, full_mon_count, current_mon_count,
    success_count, fault_count;
  uint32 first_sid;
  longlong limit, tmp_sid = -1;
  SPIDER_MON_TABLE_RESULT *mon_table_result =
    (SPIDER_MON_TABLE_RESULT *) initid->ptr;
  SPIDER_TRX *trx = mon_table_result->trx;
  THD *thd = trx->thd;
  SPIDER_CONN *ping_conn, *mon_conn;
  char *where_clause;
  SPIDER_TABLE_MON_LIST *table_mon_list;
  SPIDER_TABLE_MON *table_mon;

  char buf[MAX_FIELD_WIDTH];
  String conv_name(buf, sizeof(buf), system_charset_info);
  int conv_name_length;
  char link_idx_str[SPIDER_SQL_INT_LEN];
  int link_idx_str_length;
  bool get_lock = FALSE;
  DBUG_ENTER("spider_ping_table_body");
  conv_name.length(0);
  if (
    thd->open_tables != 0 ||
    thd->temporary_tables != 0 ||
    thd->handler_tables_hash.records != 0 ||
    thd->derived_tables != 0 ||
    thd->lock != 0 ||
#if MYSQL_VERSION_ID < 50500
    thd->locked_tables != 0 ||
    thd->prelocked_mode != NON_PRELOCKED ||
#else
    thd->locked_tables_list.locked_tables() ||
    thd->locked_tables_mode != LTM_NONE ||
#endif
    thd->m_reprepare_observer != NULL
  ) {
    my_printf_error(ER_SPIDER_UDF_CANT_USE_IF_OPEN_TABLE_NUM,
      ER_SPIDER_UDF_CANT_USE_IF_OPEN_TABLE_STR, MYF(0));
    goto error;
  }

  if (
    args->lengths[0] > SPIDER_CONNECT_INFO_MAX_LEN
  ) {
    my_printf_error(ER_SPIDER_UDF_PING_TABLE_PARAM_TOO_LONG_NUM,
      ER_SPIDER_UDF_PING_TABLE_PARAM_TOO_LONG_STR, MYF(0));
    goto error;
  }
  if (
    args->lengths[0] == 0
  ) {
    my_printf_error(ER_SPIDER_UDF_PING_TABLE_PARAM_REQIRED_NUM,
      ER_SPIDER_UDF_PING_TABLE_PARAM_REQIRED_STR, MYF(0));
    goto error;
  }

  link_idx = args->args[1] ? *((longlong *) args->args[1]) : 0;
  flags = args->args[2] ? *((longlong *) args->args[2]) : 0;
  limit = args->args[3] ? *((longlong *) args->args[3]) : 0;
  where_clause = args->args[4] ? args->args[4] : (char *) "";

  link_idx_str_length = my_sprintf(link_idx_str, (link_idx_str, "%010ld",
    link_idx));

  if (conv_name.append(args->args[0], args->lengths[0],
    trx->thd->variables.character_set_client))
  {
    my_error(HA_ERR_OUT_OF_MEM, MYF(0));
    goto error;
  }
  conv_name_length = conv_name.length();
  if (conv_name.reserve(link_idx_str_length + 1))
  {
    my_error(HA_ERR_OUT_OF_MEM, MYF(0));
    goto error;
  }
  conv_name.q_append(link_idx_str, link_idx_str_length + 1);
  conv_name.length(conv_name.length() - 1);

  if (!(table_mon_list = spider_get_ping_table_mon_list(trx, trx->thd,
    &conv_name, conv_name_length, link_idx, thd->server_id, TRUE, &error_num)))
    goto error;

  if (table_mon_list->mon_status == SPIDER_LINK_MON_NG)
  {
    mon_table_result->result_status = SPIDER_LINK_MON_NG;
    DBUG_PRINT("info",("spider mon_table_result->result_status=SPIDER_LINK_MON_NG 1"));
    goto end;
  }

  if (args->args[5])
    tmp_sid = *((longlong *) args->args[5]);

  if (tmp_sid >= 0)
  {
    first_sid = tmp_sid;
    full_mon_count = args->args[6] ? *((longlong *) args->args[6]) : 0;
    current_mon_count = args->args[7] ? *((longlong *) args->args[7]) + 1 : 1;
    if (full_mon_count != table_mon_list->list_size)
    {
      my_printf_error(ER_SPIDER_UDF_PING_TABLE_DIFFERENT_MON_NUM,
        ER_SPIDER_UDF_PING_TABLE_DIFFERENT_MON_STR, MYF(0));
      goto error_with_free_table_mon_list;
    }
  } else {
    first_sid = thd->server_id;
    full_mon_count = table_mon_list->list_size;
    current_mon_count = 1;
  }

  success_count = args->args[8] ? *((longlong *) args->args[8]) : 0;
  fault_count = args->args[9] ? *((longlong *) args->args[9]) : 0;
  if (
    table_mon_list->mon_status != SPIDER_LINK_MON_NG &&
    !(ping_conn = spider_get_ping_table_tgt_conn(trx,
      table_mon_list->share, &error_num))
  ) {
    if (error_num == HA_ERR_OUT_OF_MEM)
      goto error_with_free_table_mon_list;
    else
      thd->clear_error();
  }
  if (
    table_mon_list->mon_status == SPIDER_LINK_MON_NG ||
    error_num ||
    spider_db_udf_ping_table(table_mon_list, table_mon_list->share, trx,
      ping_conn, where_clause, args->lengths[4],
      (flags & SPIDER_UDF_PING_TABLE_PING_ONLY),
      (flags & SPIDER_UDF_PING_TABLE_USE_WHERE),
      limit
    )
  ) {
    fault_count++;
    error_num = 0;
    if (fault_count > full_mon_count / 2)
    {
      mon_table_result->result_status = SPIDER_LINK_MON_NG;
      DBUG_PRINT("info",("spider mon_table_result->result_status=SPIDER_LINK_MON_NG 2"));
      if (table_mon_list->mon_status != SPIDER_LINK_MON_NG)
      {
        pthread_mutex_lock(&table_mon_list->update_status_mutex);
        if (table_mon_list->mon_status != SPIDER_LINK_MON_NG)
        {
          table_mon_list->mon_status = SPIDER_LINK_MON_NG;
          table_mon_list->share->link_statuses[0] = SPIDER_LINK_STATUS_NG;
          spider_sys_update_tables_link_status(trx->thd,
            conv_name.c_ptr(), conv_name_length, link_idx,
            SPIDER_LINK_STATUS_NG, TRUE);
        }
        pthread_mutex_unlock(&table_mon_list->update_status_mutex);
      }
      goto end;
    }
  } else {
    success_count++;
    if (success_count > full_mon_count / 2)
    {
      mon_table_result->result_status = SPIDER_LINK_MON_OK;
      DBUG_PRINT("info",("spider mon_table_result->result_status=SPIDER_LINK_MON_OK 1"));
      goto end;
    }
  }

  if (tmp_sid < 0)
  {
    if (!pthread_mutex_trylock(&table_mon_list->receptor_mutex))
      get_lock = TRUE;
  }

  if (
    tmp_sid >= 0 ||
    get_lock
  ) {
    table_mon = table_mon_list->current->next;
    while (TRUE)
    {
      if (!table_mon)
        table_mon = table_mon_list->first;
      if (
        table_mon->server_id == first_sid ||
        current_mon_count > full_mon_count
      ) {
        if (success_count + fault_count > full_mon_count / 2)
        {
          mon_table_result->result_status = SPIDER_LINK_MON_DRAW;
          DBUG_PRINT("info",(
            "spider mon_table_result->result_status=SPIDER_LINK_MON_DRAW 1"));
        } else {
          mon_table_result->result_status = SPIDER_LINK_MON_DRAW_FEW_MON;
          DBUG_PRINT("info",(
            "spider mon_table_result->result_status=SPIDER_LINK_MON_DRAW_FEW_MON 1"));
        }
        table_mon_list->last_receptor_result = mon_table_result->result_status;
        break;
      }
      if ((mon_conn = spider_get_ping_table_tgt_conn(trx,
        table_mon->share, &error_num))
      ) {
        if (!spider_db_udf_ping_table_mon_next(
          thd, table_mon, mon_conn, mon_table_result, args->args[0],
          args->lengths[0], link_idx,
          where_clause, args->lengths[4], first_sid, full_mon_count,
          current_mon_count, success_count, fault_count, flags, limit))
        {
          if (
            mon_table_result->result_status == SPIDER_LINK_MON_NG &&
            table_mon_list->mon_status != SPIDER_LINK_MON_NG
          ) {
            pthread_mutex_lock(&table_mon_list->update_status_mutex);
            if (table_mon_list->mon_status != SPIDER_LINK_MON_NG)
            {
              table_mon_list->mon_status = SPIDER_LINK_MON_NG;
              table_mon_list->share->link_statuses[0] = SPIDER_LINK_STATUS_NG;
              spider_sys_update_tables_link_status(trx->thd,
                conv_name.c_ptr(), conv_name_length, link_idx,
                SPIDER_LINK_STATUS_NG, TRUE);
            }
            pthread_mutex_unlock(&table_mon_list->update_status_mutex);
          }
          table_mon_list->last_receptor_result =
            mon_table_result->result_status;
          break;
        }
      }
      thd->clear_error();
      table_mon = table_mon->next;
      current_mon_count++;
    }
    if (get_lock)
      pthread_mutex_unlock(&table_mon_list->receptor_mutex);
  } else {
    pthread_mutex_lock(&table_mon_list->receptor_mutex);
    mon_table_result->result_status = table_mon_list->last_receptor_result;
    DBUG_PRINT("info",("spider mon_table_result->result_status=%d 1",
      table_mon_list->last_receptor_result));
    pthread_mutex_unlock(&table_mon_list->receptor_mutex);
  }

end:
  spider_free_ping_table_mon_list(table_mon_list);
  DBUG_RETURN(mon_table_result->result_status);

error_with_free_table_mon_list:
  spider_free_ping_table_mon_list(table_mon_list);
error:
  *error = 1;
  DBUG_RETURN(0);
}

my_bool spider_ping_table_init_body(
  UDF_INIT *initid,
  UDF_ARGS *args,
  char *message
) {
  int error_num;
  THD *thd = current_thd;
  SPIDER_TRX *trx;
  SPIDER_MON_TABLE_RESULT *mon_table_result = NULL;
  DBUG_ENTER("spider_ping_table_init_body");
  if (args->arg_count != 10)
  {
    strcpy(message, "spider_ping_table() requires 10 arguments");
    goto error;
  }
  if (
    args->arg_type[0] != STRING_RESULT ||
    args->arg_type[4] != STRING_RESULT
  ) {
    strcpy(message, "spider_ping_table() requires string 1st "
      "and 5th arguments");
    goto error;
  }
  if (
    args->arg_type[1] != INT_RESULT ||
    args->arg_type[2] != INT_RESULT ||
    args->arg_type[3] != INT_RESULT ||
    args->arg_type[5] != INT_RESULT ||
    args->arg_type[6] != INT_RESULT ||
    args->arg_type[7] != INT_RESULT ||
    args->arg_type[8] != INT_RESULT ||
    args->arg_type[9] != INT_RESULT
  ) {
    strcpy(message, "spider_ping_table() requires integer 2nd, 3rd, 4,6,7,8,"
      "9th and 10th argument");
    goto error;
  }

  if (!(trx = spider_get_trx(thd, &error_num)))
  {
    my_error(error_num, MYF(0));
#if MYSQL_VERSION_ID < 50500
    strcpy(message, thd->main_da.message());
#else
    strcpy(message, thd->stmt_da->message());
#endif
    goto error;
  }

  if (!(mon_table_result = (SPIDER_MON_TABLE_RESULT *)
      my_malloc(sizeof(SPIDER_MON_TABLE_RESULT), MYF(MY_WME | MY_ZEROFILL)))
  ) {
    strcpy(message, "spider_ping_table() out of memory");
    goto error;
  }
  mon_table_result->trx = trx;
  initid->ptr = (char *) mon_table_result;
  DBUG_RETURN(FALSE);

error:
  if (mon_table_result)
  {
    my_free(mon_table_result, MYF(0));
  }
  DBUG_RETURN(TRUE);
}

void spider_ping_table_deinit_body(
  UDF_INIT *initid
) {
  SPIDER_MON_TABLE_RESULT *mon_table_result =
    (SPIDER_MON_TABLE_RESULT *) initid->ptr;
  DBUG_ENTER("spider_ping_table_deinit_body");
  if (mon_table_result)
  {
    my_free(mon_table_result, MYF(0));
  }
  DBUG_VOID_RETURN;
}

void spider_ping_table_free_mon_list(
  SPIDER_TABLE_MON_LIST *table_mon_list
) {
  DBUG_ENTER("spider_ping_table_free_mon_list");
  if (table_mon_list)
  {
    spider_ping_table_free_mon(table_mon_list->first);
    spider_free_tmp_share_alloc(table_mon_list->share);
    pthread_mutex_destroy(&table_mon_list->update_status_mutex);
    pthread_mutex_destroy(&table_mon_list->monitor_mutex);
    pthread_mutex_destroy(&table_mon_list->receptor_mutex);
    pthread_mutex_destroy(&table_mon_list->caller_mutex);
    my_free(table_mon_list, MYF(0));
  }
  DBUG_VOID_RETURN;
}

void spider_ping_table_free_mon(
  SPIDER_TABLE_MON *table_mon
) {
  SPIDER_TABLE_MON *table_mon_next;
  DBUG_ENTER("spider_ping_table_free_mon");
  while (table_mon)
  {
    spider_free_tmp_share_alloc(table_mon->share);
    table_mon_next = table_mon->next;
    my_free(table_mon, MYF(0));
    table_mon = table_mon_next;
  }
  DBUG_VOID_RETURN;
}

int spider_ping_table_mon_from_table(
  SPIDER_TRX *trx,
  THD *thd,
  SPIDER_SHARE *share,
  uint32 server_id,
  char *conv_name,
  uint conv_name_length,
  int link_idx,
  char *where_clause,
  uint where_clause_length,
  long monitoring_kind,
  longlong monitoring_limit,
  bool need_lock
) {
  int error_num = 0, current_mon_count, flags;
  uint32 first_sid;
/*
  THD *thd = trx->thd;
*/
  SPIDER_TABLE_MON_LIST *table_mon_list;
  SPIDER_TABLE_MON *table_mon;
  SPIDER_MON_TABLE_RESULT mon_table_result;
  SPIDER_CONN *mon_conn;
  TABLE_SHARE *table_share = share->table_share;
  char link_idx_str[SPIDER_SQL_INT_LEN];
  int link_idx_str_length;
  uint sql_command = thd_sql_command(thd);
  DBUG_ENTER("spider_ping_table_mon_from_table");
  if (table_share->tmp_table != NO_TMP_TABLE)
  {
    my_printf_error(ER_SPIDER_TMP_TABLE_MON_NUM,
      ER_SPIDER_TMP_TABLE_MON_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_TMP_TABLE_MON_NUM);
  }
  if (
    sql_command == SQLCOM_DROP_TABLE ||
    sql_command == SQLCOM_ALTER_TABLE
  ) {
    my_printf_error(ER_SPIDER_MON_AT_ALTER_TABLE_NUM,
      ER_SPIDER_MON_AT_ALTER_TABLE_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_MON_AT_ALTER_TABLE_NUM);
  }

  link_idx_str_length = my_sprintf(link_idx_str, (link_idx_str, "%010ld",
    link_idx));
#ifdef _MSC_VER
  String conv_name_str(conv_name_length + link_idx_str_length + 1);
  conv_name_str.set_charset(system_charset_info);
  *((char *)(conv_name_str.ptr() + conv_name_length + link_idx_str_length)) =
    '\0';
#else
  char buf[conv_name_length + link_idx_str_length + 1];
  buf[conv_name_length + link_idx_str_length] = '\0';
  String conv_name_str(buf, conv_name_length + link_idx_str_length + 1,
    system_charset_info);
#endif
  conv_name_str.length(0);
  conv_name_str.q_append(conv_name, conv_name_length);
  conv_name_str.q_append(link_idx_str, link_idx_str_length + 1);
  conv_name_str.length(conv_name_str.length() - 1);

  if (monitoring_kind == 1)
    flags = SPIDER_UDF_PING_TABLE_PING_ONLY;
  else if (monitoring_kind == 3)
    flags = SPIDER_UDF_PING_TABLE_USE_WHERE;
  else
    flags = 0;

  if (!(table_mon_list = spider_get_ping_table_mon_list(trx, thd,
    &conv_name_str, conv_name_length, link_idx, server_id, need_lock,
    &error_num)))
    goto end;

  if (table_mon_list->mon_status == SPIDER_LINK_MON_NG)
  {
    DBUG_PRINT("info", ("spider share->link_statuses[%d]=SPIDER_LINK_STATUS_NG",
      link_idx));
    share->link_statuses[link_idx] = SPIDER_LINK_STATUS_NG;
    error_num = ER_SPIDER_LINK_MON_NG_NUM;
    my_printf_error(error_num,
      ER_SPIDER_LINK_MON_NG_STR, MYF(0),
      table_mon_list->share->db_names_str[0].ptr(),
      table_mon_list->share->table_names_str[0].ptr());
    goto end_with_free_table_mon_list;
  }

  if (!pthread_mutex_trylock(&table_mon_list->caller_mutex))
  {
    table_mon = table_mon_list->current;
    first_sid = table_mon->server_id;
    current_mon_count = 1;
    while (TRUE)
    {
      if (!table_mon)
        table_mon = table_mon_list->first;
      if (
        current_mon_count > table_mon_list->list_size ||
        (current_mon_count > 1 && table_mon->server_id == first_sid)
      ) {
        table_mon_list->last_caller_result = SPIDER_LINK_MON_DRAW_FEW_MON;
        mon_table_result.result_status = SPIDER_LINK_MON_DRAW_FEW_MON;
        DBUG_PRINT("info",(
          "spider mon_table_result->result_status=SPIDER_LINK_MON_DRAW_FEW_MON 1"));
        error_num = ER_SPIDER_LINK_MON_DRAW_FEW_MON_NUM;
        my_printf_error(error_num,
          ER_SPIDER_LINK_MON_DRAW_FEW_MON_STR, MYF(0),
          table_mon_list->share->db_names_str[0].ptr(),
          table_mon_list->share->table_names_str[0].ptr());
        break;
      }
      thd->clear_error();
      if ((mon_conn = spider_get_ping_table_tgt_conn(trx,
        table_mon->share, &error_num))
      ) {
        if (!spider_db_udf_ping_table_mon_next(
          thd, table_mon, mon_conn, &mon_table_result, conv_name,
          conv_name_length, link_idx,
          where_clause, where_clause_length, -1, table_mon_list->list_size,
          0, 0, 0, flags, monitoring_limit))
        {
          if (
            mon_table_result.result_status == SPIDER_LINK_MON_NG &&
            table_mon_list->mon_status != SPIDER_LINK_MON_NG
          ) {
            pthread_mutex_lock(&table_mon_list->update_status_mutex);
            if (table_mon_list->mon_status != SPIDER_LINK_MON_NG)
            {
              table_mon_list->mon_status = SPIDER_LINK_MON_NG;
              table_mon_list->share->link_statuses[0] = SPIDER_LINK_STATUS_NG;
              DBUG_PRINT("info", (
                "spider share->link_statuses[%d]=SPIDER_LINK_STATUS_NG", link_idx));
              share->link_statuses[link_idx] = SPIDER_LINK_STATUS_NG;
              spider_sys_update_tables_link_status(thd, conv_name,
                conv_name_length, link_idx, SPIDER_LINK_STATUS_NG, need_lock);
            }
            pthread_mutex_unlock(&table_mon_list->update_status_mutex);
          }
          table_mon_list->last_caller_result = mon_table_result.result_status;
          if (mon_table_result.result_status == SPIDER_LINK_MON_OK)
          {
            error_num = ER_SPIDER_LINK_MON_OK_NUM;
            my_printf_error(error_num,
              ER_SPIDER_LINK_MON_OK_STR, MYF(0),
              table_mon_list->share->db_names_str[0].ptr(),
              table_mon_list->share->table_names_str[0].ptr());
            break;
          }
          if (mon_table_result.result_status == SPIDER_LINK_MON_NG)
          {
            error_num = ER_SPIDER_LINK_MON_NG_NUM;
            my_printf_error(error_num,
              ER_SPIDER_LINK_MON_NG_STR, MYF(0),
              table_mon_list->share->db_names_str[0].ptr(),
              table_mon_list->share->table_names_str[0].ptr());
            break;
          }
          if (mon_table_result.result_status ==
            SPIDER_LINK_MON_DRAW_FEW_MON)
          {
            error_num = ER_SPIDER_LINK_MON_DRAW_FEW_MON_NUM;
            my_printf_error(error_num,
              ER_SPIDER_LINK_MON_DRAW_FEW_MON_STR, MYF(0),
              table_mon_list->share->db_names_str[0].ptr(),
              table_mon_list->share->table_names_str[0].ptr());
            break;
          }
          error_num = ER_SPIDER_LINK_MON_DRAW_NUM;
          my_printf_error(error_num,
            ER_SPIDER_LINK_MON_DRAW_STR, MYF(0),
            table_mon_list->share->db_names_str[0].ptr(),
            table_mon_list->share->table_names_str[0].ptr());
          break;
        }
      }
      table_mon = table_mon->next;
      current_mon_count++;
    }
    pthread_mutex_unlock(&table_mon_list->caller_mutex);
  } else {
    pthread_mutex_lock(&table_mon_list->caller_mutex);
    switch (table_mon_list->last_caller_result)
    {
      case SPIDER_LINK_MON_OK:
        error_num = ER_SPIDER_LINK_MON_OK_NUM;
        my_printf_error(error_num,
          ER_SPIDER_LINK_MON_OK_STR, MYF(0),
          table_mon_list->share->db_names_str[0].ptr(),
          table_mon_list->share->table_names_str[0].ptr());
        break;
      case SPIDER_LINK_MON_NG:
        error_num = ER_SPIDER_LINK_MON_NG_NUM;
        my_printf_error(error_num,
          ER_SPIDER_LINK_MON_NG_STR, MYF(0),
          table_mon_list->share->db_names_str[0].ptr(),
          table_mon_list->share->table_names_str[0].ptr());
        break;
      case SPIDER_LINK_MON_DRAW_FEW_MON:
        error_num = ER_SPIDER_LINK_MON_DRAW_FEW_MON_NUM;
        my_printf_error(error_num,
          ER_SPIDER_LINK_MON_DRAW_FEW_MON_STR, MYF(0),
          table_mon_list->share->db_names_str[0].ptr(),
          table_mon_list->share->table_names_str[0].ptr());
        break;
      default:
        error_num = ER_SPIDER_LINK_MON_DRAW_NUM;
        my_printf_error(error_num,
          ER_SPIDER_LINK_MON_DRAW_STR, MYF(0),
          table_mon_list->share->db_names_str[0].ptr(),
          table_mon_list->share->table_names_str[0].ptr());
        break;
    }
    pthread_mutex_unlock(&table_mon_list->caller_mutex);
  }

end_with_free_table_mon_list:
  spider_free_ping_table_mon_list(table_mon_list);
end:
  DBUG_RETURN(error_num);
}
