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

handlerton *spider_hton_ptr;

extern HASH spider_xa_locks;
extern pthread_mutex_t spider_xa_lock_mutex;
extern HASH spider_open_connections;
extern pthread_mutex_t spider_conn_mutex;

HASH spider_open_tables;
pthread_mutex_t spider_tbl_mutex;

// for spider_open_tables
uchar *spider_tbl_get_key(
  SPIDER_SHARE *share,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
) {
  DBUG_ENTER("spider_tbl_get_key");
  *length = share->table_name_length;
  DBUG_RETURN((uchar*) share->table_name);
}

uchar *spider_ha_get_key(
  ha_spider *spider,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
) {
  DBUG_ENTER("spider_ha_get_key");
  *length = spider->share->table_name_length;
  DBUG_RETURN((uchar*) spider->share->table_name);
}

int spider_get_server(
  SPIDER_SHARE *share
) {
  MEM_ROOT mem_root;
  int error_num;
  FOREIGN_SERVER *server, server_buf;
  DBUG_ENTER("spider_get_server");
  init_alloc_root(&mem_root, 65, 0);

  if (!(server
       = get_server_by_name(&mem_root, share->server_name, &server_buf)))
  {
    error_num = ER_FOREIGN_SERVER_DOESNT_EXIST;
    goto error;
  }

  if (!share->tgt_wrapper)
  {
    if ((share->tgt_wrapper = server->scheme))
      share->tgt_wrapper_length = strlen(share->tgt_wrapper);
  } else if (server->scheme)
    my_free(server->scheme, MYF(0));

  if (!share->tgt_host)
  {
    if ((share->tgt_host = server->host))
      share->tgt_host_length = strlen(share->tgt_host);
  } else if (server->host)
    my_free(server->host, MYF(0));

  if (share->tgt_port == -1)
  {
    share->tgt_port = server->port;
  }

  if (!share->tgt_socket)
  {
    if ((share->tgt_socket = server->socket))
      share->tgt_socket_length = strlen(share->tgt_socket);
  } else if (server->socket)
    my_free(server->socket, MYF(0));

  if (!share->tgt_db)
  {
    if ((share->tgt_db = server->db))
      share->tgt_db_length = strlen(share->tgt_db);
  } else if (server->db)
    my_free(server->db, MYF(0));

  if (!share->tgt_username)
  {
    if ((share->tgt_username = server->username))
      share->tgt_username_length = strlen(share->tgt_username);
  } else if (server->username)
    my_free(server->username, MYF(0));

  if (!share->tgt_password)
  {
    if ((share->tgt_password = server->password))
      share->tgt_password_length = strlen(share->tgt_password);
  } else if (server->password)
    my_free(server->password, MYF(0));

  my_free(server, MYF(0));
  free_root(&mem_root, MYF(0));
  DBUG_RETURN(0);

error:
  free_root(&mem_root, MYF(0));
  my_error(error_num, MYF(0), share->server_name);
  DBUG_RETURN(error_num);
}

int spider_free_share_alloc(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_free_share_alloc");
  if (share->server_name)
    my_free(share->server_name, MYF(0));
  if (share->tgt_table_name)
    my_free(share->tgt_table_name, MYF(0));
  if (share->tgt_db)
    my_free(share->tgt_db, MYF(0));
  if (share->tgt_host)
    my_free(share->tgt_host, MYF(0));
  if (share->tgt_username)
    my_free(share->tgt_username, MYF(0));
  if (share->tgt_password)
    my_free(share->tgt_password, MYF(0));
  if (share->tgt_socket)
    my_free(share->tgt_socket, MYF(0));
  if (share->tgt_wrapper)
    my_free(share->tgt_wrapper, MYF(0));
  if (share->conn_key)
    my_free(share->conn_key, MYF(0));
  if (share->table_select)
    delete share->table_select;
  if (share->key_select)
    delete [] share->key_select;
  spider_db_free_show_table_status(share);
  spider_db_free_show_index(share);
  DBUG_RETURN(0);
}

void spider_free_tmp_share_alloc(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_free_tmp_share_alloc");
  if (share->server_name)
    my_free(share->server_name, MYF(0));
  if (share->tgt_table_name)
    my_free(share->tgt_table_name, MYF(0));
  if (share->tgt_db)
    my_free(share->tgt_db, MYF(0));
  if (share->tgt_host)
    my_free(share->tgt_host, MYF(0));
  if (share->tgt_username)
    my_free(share->tgt_username, MYF(0));
  if (share->tgt_password)
    my_free(share->tgt_password, MYF(0));
  if (share->tgt_socket)
    my_free(share->tgt_socket, MYF(0));
  if (share->tgt_wrapper)
    my_free(share->tgt_wrapper, MYF(0));
  if (share->conn_key)
    my_free(share->conn_key, MYF(0));
  DBUG_VOID_RETURN;
}

char *spider_get_string_between_quote(
  char *ptr,
  bool alloc
) {
  char *start_ptr, *end_ptr;
  DBUG_ENTER("spider_get_string_between_quote");

  start_ptr = strchr(ptr, '\'');
  end_ptr = strchr(ptr, '"');
  if (start_ptr && (!end_ptr || start_ptr < end_ptr))
  {
    if (!(end_ptr = strchr(++start_ptr, '\'')))
      DBUG_RETURN(NULL);
  }
  else if (
    !(start_ptr = end_ptr) ||
    !(end_ptr = strchr(++start_ptr, '"'))
  )
    DBUG_RETURN(NULL);

  *end_ptr = '\0';
  if (alloc)
  {
    DBUG_RETURN(
      spider_create_string(
      start_ptr,
      strlen(start_ptr))
    );
  } else {
    DBUG_RETURN(start_ptr);
  }
}

#define SPIDER_PARAM_STR_LEN(name) name ## _length
#define SPIDER_PARAM_STR(title_name, param_name) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    if (!share->param_name) \
    { \
      if ((share->param_name = spider_get_string_between_quote( \
        start_ptr, TRUE))) \
        share->SPIDER_PARAM_STR_LEN(param_name) = strlen(share->param_name); \
      else \
        goto error; \
      DBUG_PRINT("info",("spider "title_name"=%s", share->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_INT_WITH_MAX(title_name, param_name, min_val, max_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    if (share->param_name == -1) \
    { \
      if ((tmp_ptr = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        share->param_name = atoi(tmp_ptr); \
        if (share->param_name < min_val) \
          share->param_name = min_val; \
        else if (share->param_name > max_val) \
          share->param_name = max_val; \
      } else \
        goto error; \
      DBUG_PRINT("info",("spider "title_name"=%d", share->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_INT(title_name, param_name, min_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    if (share->param_name == -1) \
    { \
      if ((tmp_ptr = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        share->param_name = atoi(tmp_ptr); \
        if (share->param_name < min_val) \
          share->param_name = min_val; \
      } else \
        goto error; \
      DBUG_PRINT("info",("spider "title_name"=%d", share->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_DOUBLE(title_name, param_name, min_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    if (share->param_name == -1) \
    { \
      if ((tmp_ptr = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        share->param_name = my_atof(tmp_ptr); \
        if (share->param_name < min_val) \
          share->param_name = min_val; \
      } else \
        goto error; \
      DBUG_PRINT("info",("spider "title_name"=%f", share->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_LONGLONG(title_name, param_name, min_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    if (share->param_name == -1) \
    { \
      if ((tmp_ptr = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        share->param_name = my_strtoll10(tmp_ptr, (char**) NULL, &error_num); \
        if (share->param_name < min_val) \
          share->param_name = min_val; \
      } else \
        goto error; \
      DBUG_PRINT("info",("spider "title_name"=%lld", share->param_name)); \
    } \
    break; \
  }

int spider_parse_connect_info(
  SPIDER_SHARE *share,
  TABLE *table,
  uint create_table
) {
  int error_num = 0;
  char *connect_string = NULL;
  char *sprit_ptr[2];
  char *tmp_ptr, *start_ptr;
  int roop_count;
  int title_length;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  partition_element *part_elem;
  partition_element *sub_elem;
#endif
  DBUG_ENTER("spider_parse_connect_info");
  DBUG_PRINT("info",("spider partition_info=%s", table->s->partition_info));
  DBUG_PRINT("info",("spider part_info=%x", table->part_info));
  DBUG_PRINT("info",("spider s->db=%s", table->s->db.str));
  DBUG_PRINT("info",("spider s->table_name=%s", table->s->table_name.str));
  DBUG_PRINT("info",("spider s->path=%s", table->s->path.str));
  DBUG_PRINT("info",
    ("spider s->normalized_path=%s", table->s->normalized_path.str));
#ifdef WITH_PARTITION_STORAGE_ENGINE
  spider_get_partition_info(share->table_name, table, &part_elem, &sub_elem);
#endif
  share->tgt_port = -1;
  share->sts_interval = -1;
  share->sts_mode = -1;
  share->crd_interval = -1;
  share->crd_mode = -1;
  share->crd_type = -1;
  share->crd_weight = -1;
  share->internal_offset = -1;
  share->internal_limit = -1;
  share->split_read = -1;
  share->init_sql_alloc_size = -1;
  share->reset_sql_alloc = -1;
  share->multi_split_read = -1;
  share->max_order = -1;
  share->semi_table_lock = -1;
  share->selupd_lock_mode = -1;
  share->query_cache = -1;
  share->internal_delayed = -1;
  share->bulk_size = -1;
  share->internal_optimize = -1;
  share->internal_optimize_local = -1;
  share->scan_rate = -1;
  share->read_rate = -1;
  share->priority = -1;

#ifdef WITH_PARTITION_STORAGE_ENGINE
  for (roop_count = 4; roop_count > 0; roop_count--)
#else
  for (roop_count = 2; roop_count > 0; roop_count--)
#endif
  {
    if (connect_string)
    {
      my_free(connect_string, MYF(0));
      connect_string = NULL;
    }
    switch (roop_count)
    {
#ifdef WITH_PARTITION_STORAGE_ENGINE
      case 4:
        if (!sub_elem || !sub_elem->part_comment)
          continue;
        DBUG_PRINT("info",("spider create sub comment string"));
        if (
          !(connect_string = spider_create_string(
            sub_elem->part_comment,
            strlen(sub_elem->part_comment)))
        ) {
          error_num = HA_ERR_OUT_OF_MEM;
          goto error_alloc_conn_string;
        }
        DBUG_PRINT("info",("spider sub comment string=%s", connect_string));
        break;
      case 3:
        if (!part_elem || !part_elem->part_comment)
          continue;
        DBUG_PRINT("info",("spider create part comment string"));
        if (
          !(connect_string = spider_create_string(
            part_elem->part_comment,
            strlen(part_elem->part_comment)))
        ) {
          error_num = HA_ERR_OUT_OF_MEM;
          goto error_alloc_conn_string;
        }
        DBUG_PRINT("info",("spider part comment string=%s", connect_string));
        break;
#endif
      case 2:
        if (table->s->comment.length == 0)
          continue;
        DBUG_PRINT("info",("spider create comment string"));
        if (
          !(connect_string = spider_create_string(
            table->s->comment.str,
            table->s->comment.length))
        ) {
          error_num = HA_ERR_OUT_OF_MEM;
          goto error_alloc_conn_string;
        }
        DBUG_PRINT("info",("spider comment string=%s", connect_string));
        break;
      default:
        if (table->s->connect_string.length == 0)
          continue;
        DBUG_PRINT("info",("spider create connect_string string"));
        if (
          !(connect_string = spider_create_string(
            table->s->connect_string.str,
            table->s->connect_string.length))
        ) {
          error_num = HA_ERR_OUT_OF_MEM;
          goto error_alloc_conn_string;
        }
        DBUG_PRINT("info",("spider connect_string=%s", connect_string));
        break;
    }

    sprit_ptr[0] = connect_string;
    while (sprit_ptr[0])
    {
      if ((sprit_ptr[1] = strchr(sprit_ptr[0], ',')))
      {
        *sprit_ptr[1] = '\0';
        sprit_ptr[1]++;
      }
      tmp_ptr = sprit_ptr[0];
      sprit_ptr[0] = sprit_ptr[1];
      while (*tmp_ptr == ' ')
        tmp_ptr++;

      if (*tmp_ptr == '\0')
        continue;

      title_length = 0;
      start_ptr = tmp_ptr;
      while (*start_ptr != ' ' && *start_ptr != '\'' &&
        *start_ptr != '"' && *start_ptr != '\0')
      {
        title_length++;
        start_ptr++;
      }

      switch (title_length)
      {
        case 0:
          continue;
        case 4:
          SPIDER_PARAM_STR("host", tgt_host);
          SPIDER_PARAM_STR("user", tgt_username);
          SPIDER_PARAM_INT_WITH_MAX("port", tgt_port, 0, 65535);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 5:
          SPIDER_PARAM_STR("table", tgt_table_name);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 6:
          SPIDER_PARAM_STR("server", server_name);
          SPIDER_PARAM_STR("socket", tgt_socket);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 7:
          SPIDER_PARAM_STR("wrapper", tgt_wrapper);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 8:
          SPIDER_PARAM_STR("database", tgt_db);
          SPIDER_PARAM_STR("password", tgt_password);
          SPIDER_PARAM_INT_WITH_MAX("sts_mode", sts_mode, 1, 2);
          SPIDER_PARAM_INT_WITH_MAX("crd_mode", crd_mode, 0, 3);
          SPIDER_PARAM_INT_WITH_MAX("crd_type", crd_type, 0, 2);
          SPIDER_PARAM_LONGLONG("priority", priority, 0);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 9:
          SPIDER_PARAM_INT("max_order", max_order, 0);
          SPIDER_PARAM_INT("bulk_size", bulk_size, 0);
          SPIDER_PARAM_INT("scan_rate", scan_rate, 0);
          SPIDER_PARAM_DOUBLE("read_rate", read_rate, 0);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 10:
          SPIDER_PARAM_DOUBLE("crd_weight", crd_weight, 1);
          SPIDER_PARAM_LONGLONG("split_read", split_read, 0);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 11:
          SPIDER_PARAM_INT_WITH_MAX("query_cache", query_cache, 0, 2);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 12:
          SPIDER_PARAM_DOUBLE("sts_interval", sts_interval, 0);
          SPIDER_PARAM_DOUBLE("crd_interval", crd_interval, 0);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 14:
          SPIDER_PARAM_LONGLONG("internal_limit", internal_limit, 0);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 15:
          SPIDER_PARAM_LONGLONG("internal_offset", internal_offset, 0);
          SPIDER_PARAM_INT_WITH_MAX("reset_sql_alloc", reset_sql_alloc, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX("semi_table_lock", semi_table_lock, 0, 1);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 16:
          SPIDER_PARAM_INT_WITH_MAX(
            "multi_split_read", multi_split_read, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX(
            "selupd_lock_mode", selupd_lock_mode, 0, 2);
          SPIDER_PARAM_INT_WITH_MAX(
            "internal_delayed", internal_delayed, 0, 1);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 17:
          SPIDER_PARAM_INT_WITH_MAX(
            "internal_optimize", internal_optimize, 0, 1);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 19:
          SPIDER_PARAM_INT("init_sql_alloc_size", init_sql_alloc_size, 0);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 23:
          SPIDER_PARAM_INT_WITH_MAX(
            "internal_optimize_local", internal_optimize_local, 0, 1);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        default:
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
      }
    }
  }

  if (share->server_name)
  {
    if ((error_num = spider_get_server(share)))
      goto error_get_server;
  }

  if (!share->tgt_wrapper)
  {
    DBUG_PRINT("info",("spider create default tgt_wrapper"));
    share->tgt_wrapper_length = SPIDER_DB_WRAPPER_LEN;
    if (
      !(share->tgt_wrapper = spider_create_string(
        SPIDER_DB_WRAPPER_STR,
        share->tgt_wrapper_length))
    ) {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
  }

  if (!share->tgt_host)
  {
    DBUG_PRINT("info",("spider create default tgt_host"));
    share->tgt_host_length = strlen(my_localhost);
    if (
      !(share->tgt_host = spider_create_string(
        my_localhost,
        share->tgt_host_length))
    ) {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
  }

  if (!share->tgt_db)
  {
    DBUG_PRINT("info",("spider create default tgt_db"));
    share->tgt_db_length = table->s->db.length;
    if (
      !(share->tgt_db = spider_create_string(
        table->s->db.str,
        table->s->db.length))
    ) {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
  }

  if (!share->tgt_table_name)
  {
    DBUG_PRINT("info",("spider create default tgt_table_name"));
    share->tgt_table_name_length = share->table_name_length;
    if (
      !(share->tgt_table_name = spider_create_table_name_string(
        table->s->table_name.str,
#ifdef WITH_PARTITION_STORAGE_ENGINE
        (part_elem ? part_elem->partition_name : NULL),
        (sub_elem ? sub_elem->partition_name : NULL)
#else
        NULL,
        NULL
#endif
      ))
    ) {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
  }

  if (share->tgt_port == -1)
  {
    share->tgt_port = MYSQL_PORT;
  } else if (share->tgt_port < 0)
  {
    share->tgt_port = 0;
  } else if (share->tgt_port > 65535)
  {
    share->tgt_port = 65535;
  }

  if (
    !share->tgt_socket &&
    !strcmp(share->tgt_host, my_localhost)
  ) {
    DBUG_PRINT("info",("spider create default tgt_socket"));
    share->tgt_socket_length = strlen((char *) MYSQL_UNIX_ADDR);
    if (
      !(share->tgt_socket = spider_create_string(
        (char *) MYSQL_UNIX_ADDR,
        share->tgt_socket_length))
    ) {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
  }

  if (share->sts_interval == -1)
    share->sts_interval = 10;
  if (share->sts_mode == -1)
    share->sts_mode = 1;
  if (share->crd_interval == -1)
    share->crd_interval = 51;
  if (share->crd_mode == -1)
    share->crd_mode = 1;
  if (share->crd_type == -1)
    share->crd_type = 2;
  if (share->crd_weight == -1)
    share->crd_weight = 2;
  if (share->internal_offset == -1)
    share->internal_offset = 0;
  if (share->internal_limit == -1)
    share->internal_limit = 9223372036854775807;
  if (share->split_read == -1)
    share->split_read = 9223372036854775807;
  if (share->init_sql_alloc_size == -1)
    share->init_sql_alloc_size = 1024;
  if (share->reset_sql_alloc == -1)
    share->reset_sql_alloc = 1;
  if (share->multi_split_read == -1)
    share->multi_split_read = 0;
  if (share->max_order == -1)
    share->max_order = 32767;
  if (share->semi_table_lock == -1)
    share->semi_table_lock = 0;
  if (share->selupd_lock_mode == -1)
    share->selupd_lock_mode = 1;
  if (share->query_cache == -1)
    share->query_cache = 0;
  if (share->internal_delayed == -1)
    share->internal_delayed = 0;
  if (share->bulk_size == -1)
    share->bulk_size = 16000;
  if (share->internal_optimize == -1)
    share->internal_optimize = 0;
  if (share->internal_optimize_local == -1)
    share->internal_optimize_local = 0;
  if (share->scan_rate == -1)
    share->scan_rate = 0.0001;
  if (share->read_rate == -1)
    share->read_rate = 0.0002;
  if (share->priority == -1)
    share->priority = 1000000;

  if (strcmp(share->tgt_wrapper, SPIDER_DB_WRAPPER_MYSQL))
  {
    error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
    my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
      MYF(0), share->tgt_wrapper);
    goto error;
  }

  if (create_table)
  {
    DBUG_PRINT("info",
      ("spider server_name_length = %ld", share->server_name_length));
    if (share->server_name_length > SPIDER_CONNECT_INFO_MAX_LEN)
    {
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
        MYF(0), share->server_name, "server");
      goto error;
    }

    DBUG_PRINT("info",
      ("spider tgt_table_name_length = %ld", share->tgt_table_name_length));
    if (share->tgt_table_name_length > SPIDER_CONNECT_INFO_MAX_LEN)
    {
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
        MYF(0), share->tgt_table_name, "table");
      goto error;
    }

    DBUG_PRINT("info",
      ("spider tgt_db_length = %ld", share->tgt_db_length));
    if (share->tgt_db_length > SPIDER_CONNECT_INFO_MAX_LEN)
    {
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
        MYF(0), share->tgt_db, "database");
      goto error;
    }

    DBUG_PRINT("info",
      ("spider tgt_host_length = %ld", share->tgt_host_length));
    if (share->tgt_host_length > SPIDER_CONNECT_INFO_MAX_LEN)
    {
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
        MYF(0), share->tgt_host, "host");
      goto error;
    }

    DBUG_PRINT("info",
      ("spider tgt_username_length = %ld", share->tgt_username_length));
    if (share->tgt_username_length > SPIDER_CONNECT_INFO_MAX_LEN)
    {
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
        MYF(0), share->tgt_username, "user");
      goto error;
    }

    DBUG_PRINT("info",
      ("spider tgt_password_length = %ld", share->tgt_password_length));
    if (share->tgt_password_length > SPIDER_CONNECT_INFO_MAX_LEN)
    {
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
        MYF(0), share->tgt_password, "password");
      goto error;
    }

    DBUG_PRINT("info",
      ("spider tgt_socket_length = %ld", share->tgt_socket_length));
    if (share->tgt_socket_length > SPIDER_CONNECT_INFO_MAX_LEN)
    {
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
        MYF(0), share->tgt_socket, "socket");
      goto error;
    }

    DBUG_PRINT("info",
      ("spider tgt_wrapper_length = %ld", share->tgt_wrapper_length));
    if (share->tgt_wrapper_length > SPIDER_CONNECT_INFO_MAX_LEN)
    {
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
        MYF(0), share->tgt_wrapper, "wrapper");
      goto error;
    }
  }

  if (connect_string)
    my_free(connect_string, MYF(0));
  DBUG_RETURN(0);

error:
error_get_server:
  if (connect_string)
    my_free(connect_string, MYF(0));
error_alloc_conn_string:
  DBUG_RETURN(error_num);
}

int spider_create_conn_key(
  SPIDER_SHARE *share
) {
  char *tmp_name, port_str[6];
  DBUG_ENTER("spider_create_conn_key");

  /* tgt_db not use */
  share->conn_key_length
    = share->tgt_wrapper_length + 1
    + share->tgt_host_length + 1
    + 5 + 1
    + share->tgt_socket_length + 1
    + share->tgt_username_length + 1
    + share->csname_length;
  if (!(share->conn_key = (char *)
        my_malloc(share->conn_key_length + 1, MYF(MY_WME | MY_ZEROFILL)))
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  DBUG_PRINT("info",("spider tgt_wrapper=%s", share->tgt_wrapper));
  tmp_name = strmov(share->conn_key, share->tgt_wrapper);
  DBUG_PRINT("info",("spider tgt_host=%s", share->tgt_host));
  tmp_name = strmov(tmp_name + 1, share->tgt_host);
  my_sprintf(port_str, (port_str, "%05ld", share->tgt_port));
  DBUG_PRINT("info",("spider port_str=%s", port_str));
  tmp_name = strmov(tmp_name + 1, port_str);
  if (share->tgt_socket)
  {
    DBUG_PRINT("info",("spider tgt_socket=%s", share->tgt_socket));
    tmp_name = strmov(tmp_name + 1, share->tgt_socket);
  } else
    tmp_name++;
  if (share->tgt_username)
  {
    DBUG_PRINT("info",("spider tgt_username=%s", share->tgt_username));
    tmp_name = strmov(tmp_name + 1, share->tgt_username);
  } else
    tmp_name++;
  DBUG_PRINT("info",("spider csname=%s", share->csname));
  tmp_name = strmov(tmp_name + 1, share->csname);
  DBUG_RETURN(0);
}

SPIDER_SHARE *spider_get_share(
  const char *table_name,
  TABLE *table,
  const THD *thd,
  ha_spider *spider,
  int *error_num
) {
  SPIDER_SHARE *share;
  uint length;
  uint csname_length;
  char *tmp_name;
  char *tmp_csname;
  longlong *tmp_cardinality;
  int roop_count;
  DBUG_ENTER("spider_get_share");

  length = (uint) strlen(table_name);
  pthread_mutex_lock(&spider_tbl_mutex);
  if (!(share = (SPIDER_SHARE*) hash_search(&spider_open_tables,
    (uchar*) table_name, length)))
  {
    DBUG_PRINT("info",("spider create new share"));
    csname_length = (uint) strlen(table->s->table_charset->csname);
    if (!(share = (SPIDER_SHARE *)
      my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
        &share, sizeof(*share),
        &tmp_name, length + 1,
        &tmp_csname, csname_length + 1,
        &tmp_cardinality, sizeof(*tmp_cardinality) * table->s->fields,
        NullS))
    ) {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_alloc_share;
    }

    share->use_count = 0;
    share->table_name_length = length;
    share->table_name = tmp_name;
    strmov(share->table_name, table_name);
    share->csname_length = csname_length;
    share->csname = tmp_csname;
    strmov(share->csname, table->s->table_charset->csname);
    share->cardinality = tmp_cardinality;

    if ((*error_num = spider_parse_connect_info(share, table, 0)))
      goto error_parse_connect_string;

    if ((*error_num = spider_create_conn_key(share)))
      goto error_create_conn_key;

    if (
      !(share->table_select = new String[1]) ||
      (table->s->keys > 0 && !(share->key_select =
        new String[table->s->keys])) ||
      (*error_num = spider_db_append_show_table_status(share)) ||
      (*error_num = spider_db_append_show_index(share))
    ) {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_string;
    }

    if (
      (*error_num = spider_db_append_table_select(
        share->table_select, table, share))
    )
      goto error_init_string;

    for (roop_count = 0; roop_count < table->s->keys; roop_count++)
    {
      if ((*error_num = spider_db_append_key_select(
        &share->key_select[roop_count],
        &table->s->key_info[roop_count], share)))
        goto error_init_string;
    }

    if (pthread_mutex_init(&share->mutex, MY_MUTEX_INIT_FAST))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_mutex;
    }

    if (pthread_mutex_init(&share->sts_mutex, MY_MUTEX_INIT_FAST))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_sts_mutex;
    }

    if (pthread_mutex_init(&share->crd_mutex, MY_MUTEX_INIT_FAST))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_crd_mutex;
    }

    thr_lock_init(&share->lock);

    if (my_hash_insert(&spider_open_tables, (uchar*) share))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_hash_insert;
    }
    share->use_count++;
    pthread_mutex_unlock(&spider_tbl_mutex);

    spider->share = share;
    if (
      !(spider->trx = spider_get_trx(thd, error_num)) ||
      !(spider->conn =
        spider_get_conn(share, spider->trx, spider, FALSE, TRUE, error_num))
    ) {
      share->init_error = TRUE;
      share->init = TRUE;
      spider_free_share(share);
      goto error_but_no_delete;
    }
    spider->db_conn = spider->conn->db_conn;

    share->crd_get_time = share->sts_get_time = (time_t) time((time_t*) 0);
    if (
      (*error_num = spider_db_show_table_status(spider)) ||
      (*error_num = spider_db_show_index(spider, table))
    ) {
      share->init_error = TRUE;
      share->init = TRUE;
      hash_delete(&spider->conn->user_ha_hash, (uchar*) spider);
      if (!spider->conn->user_ha_hash.records && !spider->conn->join_trx)
        spider_free_conn_from_trx(spider->trx, spider->conn,
          FALSE, FALSE, NULL);
      spider_free_share(share);
      goto error_but_no_delete;
    }

    share->init = TRUE;
  } else {
    share->use_count++;
    pthread_mutex_unlock(&spider_tbl_mutex);

    while (!share->init)
    {
      sleep(100);
    }

    spider->share = share;
    if (
      !(spider->trx = spider_get_trx(thd, error_num)) ||
      !(spider->conn =
        spider_get_conn(share, spider->trx, spider, FALSE, TRUE, error_num))
    )
      goto error_but_no_delete;
    spider->db_conn = spider->conn->db_conn;
    if (share->init_error)
    {
      pthread_mutex_lock(&share->sts_mutex);
      if (share->init_error)
      {
        share->crd_get_time = share->sts_get_time = (time_t) time((time_t*) 0);
        if (
          (*error_num = spider_db_show_table_status(spider)) ||
          (*error_num = spider_db_show_index(spider, table))
        ) {
          pthread_mutex_unlock(&share->sts_mutex);
          hash_delete(&spider->conn->user_ha_hash, (uchar*) spider);
          if (!spider->conn->user_ha_hash.records && !spider->conn->join_trx)
            spider_free_conn_from_trx(spider->trx, spider->conn,
              FALSE, FALSE, NULL);
          spider_free_share(share);
          goto error_but_no_delete;
        }
        share->init_error = FALSE;
      }
      pthread_mutex_unlock(&share->sts_mutex);
    }
  }

  DBUG_PRINT("info",("spider share=%x", share));
  DBUG_RETURN(share);

error_hash_insert:
error_get_conn:
  thr_lock_delete(&share->lock);
  VOID(pthread_mutex_destroy(&share->crd_mutex));
error_init_crd_mutex:
  VOID(pthread_mutex_destroy(&share->sts_mutex));
error_init_sts_mutex:
  VOID(pthread_mutex_destroy(&share->mutex));
error_init_mutex:
error_init_string:
error_create_conn_key:
error_parse_connect_string:
  spider_free_share_alloc(share);
  my_free(share, MYF(0));
error_alloc_share:
  pthread_mutex_unlock(&spider_tbl_mutex);
error_but_no_delete:
  DBUG_RETURN(NULL);
}

int spider_free_share(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_free_share");
  pthread_mutex_lock(&spider_tbl_mutex);
  if (!--share->use_count)
  {
    spider_free_share_alloc(share);
    hash_delete(&spider_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    VOID(pthread_mutex_destroy(&share->crd_mutex));
    VOID(pthread_mutex_destroy(&share->sts_mutex));
    VOID(pthread_mutex_destroy(&share->mutex));
    my_free(share, MYF(0));
  }
  pthread_mutex_unlock(&spider_tbl_mutex);
  DBUG_RETURN(0);
}

int spider_open_all_tables(
  SPIDER_TRX *trx,
  bool lock
) {
  TABLE *table_tables;
  TABLE *table;
  int error_num;
  char *db_name;
  char *table_name;
  char tables_key[MAX_KEY_LENGTH];
  TABLE_LIST tables;
  SPIDER_SHARE tmp_share;
  SPIDER_CONN *conn;
  MEM_ROOT mem_root;
  DBUG_ENTER("spider_open_all_tables");
  if (
    !(table_tables = spider_open_sys_table(
      trx->thd, SPIDER_SYS_TABLES_TABLE_NAME_STR, TRUE, &error_num))
  )
    DBUG_RETURN(error_num);
  if (
    (error_num = spider_get_sys_table_by_idx(table_tables, tables_key, 1))
  ) {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table_tables->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    } else
      DBUG_RETURN(0);
  }

  init_alloc_root(&mem_root, 4096, 0);
  memset(&tables, 0, sizeof(TABLE_LIST));
  memset(&tmp_share, 0, sizeof(SPIDER_SHARE));
  tmp_share.csname = (char*) system_charset_info->csname;
  tmp_share.csname_length = strlen(tmp_share.csname);
  do {
    spider_get_sys_tables(table_tables, &db_name, &table_name, &mem_root);
    tables.db = db_name;
    tables.alias = tables.table_name = table_name;
    if (!(table = open_ltable(trx->thd, &tables, TL_READ, 0)))
    {
      error_num = my_errno;
      my_free(db_name, MYF(0));
      my_free(table_name, MYF(0));
      free_root(&mem_root, MYF(0));
      DBUG_RETURN(error_num);
    }
    my_free(db_name, MYF(0));
    my_free(table_name, MYF(0));

    if (lock && THDVAR(trx->thd, use_snapshot_with_flush_tables) == 2)
    {
      tmp_share.conn_key = ((ha_spider*) table->file)->share->conn_key;
      tmp_share.conn_key_length = ((ha_spider*) table->file)->share->conn_key_length;
      if (
        (!(conn = spider_get_conn(
        &tmp_share, trx, NULL, TRUE, FALSE, &error_num)))
      ) {
        free_root(&mem_root, MYF(0));
        DBUG_RETURN(error_num);
      }

      if (lock)
      {
        ((ha_spider*) table->file)->lock_type = TL_READ_NO_INSERT;
        if (my_hash_insert(&conn->lock_table_hash, (uchar*) table->file))
        {
          free_root(&mem_root, MYF(0));
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
      }
    }
    error_num = spider_sys_index_next(table_tables);
  } while (error_num == 0);
  free_root(&mem_root, MYF(0));

  DBUG_RETURN(0);
}

bool spider_flush_logs(
  handlerton *hton
) {
  int error_num;
  THD* thd = current_thd;
  SPIDER_TRX *trx;
  DBUG_ENTER("spider_flush_logs");

  if (!(trx = spider_get_trx(thd, &error_num)))
  {
    my_errno = error_num;
    DBUG_RETURN(TRUE);
  }
  if (THDVAR(trx->thd, use_flash_logs))
  {
    if (
      !trx->trx_another_conn_hash.records &&
      THDVAR(trx->thd, use_all_conns_snapshot)
    ) {
      if ((error_num = spider_open_all_tables(trx, FALSE)))
      {
        my_errno = error_num;
        DBUG_RETURN(TRUE);
      }
    }
    if ((error_num = spider_trx_all_flush_logs(trx)))
    {
      my_errno = error_num;
      DBUG_RETURN(TRUE);
    }
  }

  DBUG_RETURN(FALSE);
}

handler* spider_create_handler(
  handlerton *hton,
  TABLE_SHARE *table, 
  MEM_ROOT *mem_root
) {
  DBUG_ENTER("spider_create_handler");
  DBUG_RETURN(new (mem_root) ha_spider(hton, table));
}

int spider_close_connection(
  handlerton*	hton,
  THD* thd
) {
  SPIDER_TRX *trx;
  DBUG_ENTER("spider_close_connection");

  if (!(trx = (SPIDER_TRX*) thd_get_ha_data(thd, spider_hton_ptr)))
    DBUG_RETURN(0); /* transaction is not started */
  spider_rollback(spider_hton_ptr, thd, TRUE);
  spider_free_trx(trx);

  DBUG_RETURN(0);
}

void spider_drop_database(
  handlerton *hton,
  char* path
) {
  DBUG_ENTER("spider_drop_database");
  DBUG_VOID_RETURN;
}

bool spider_show_status(
  handlerton *hton,
  THD *thd, 
  stat_print_fn *stat_print,
  enum ha_stat_type stat_type
) {
  DBUG_ENTER("spider_show_status");
  switch (stat_type) {
    case HA_ENGINE_STATUS:
    default:
      DBUG_RETURN(FALSE);
	}
}

int spider_db_done(
  void *p
) {
  SPIDER_XA_LOCK *xa_lock;
  SPIDER_CONN *conn;
  DBUG_ENTER("spider_db_done");

  pthread_mutex_lock(&spider_xa_lock_mutex);
  while ((xa_lock = (SPIDER_XA_LOCK*) hash_element(&spider_xa_locks, 0)))
  {
    hash_delete(&spider_xa_locks, (uchar*) xa_lock);
    my_free(xa_lock, MYF(0));
  }
  pthread_mutex_unlock(&spider_xa_lock_mutex);
  pthread_mutex_lock(&spider_conn_mutex);
  while ((conn = (SPIDER_CONN*) hash_element(&spider_open_connections, 0)))
  {
    hash_delete(&spider_open_connections, (uchar*) conn);
    spider_free_conn(conn);
  }
  pthread_mutex_unlock(&spider_conn_mutex);
  hash_free(&spider_xa_locks);
  hash_free(&spider_open_connections);
  hash_free(&spider_open_tables);
  VOID(pthread_mutex_destroy(&spider_xa_lock_mutex));
  VOID(pthread_mutex_destroy(&spider_conn_mutex));
  VOID(pthread_mutex_destroy(&spider_tbl_mutex));

  DBUG_RETURN(0);
}

int spider_panic(
  handlerton *hton,
  ha_panic_function type
) {
  DBUG_ENTER("spider_panic");
  DBUG_RETURN(0);
}

int spider_db_init(
  void *p
) {
  int error_num;
  handlerton *spider_hton = (handlerton *)p;
  DBUG_ENTER("spider_db_init");
  spider_hton_ptr = spider_hton;

  spider_hton->state = SHOW_OPTION_YES;
  spider_hton->flags = HTON_CAN_RECREATE;
  /* spider_hton->db_type = DB_TYPE_SPIDER; */
  /*
  spider_hton->savepoint_offset;
  spider_hton->savepoint_set = spider_savepoint_set;
  spider_hton->savepoint_rollback = spider_savepoint_rollback;
  spider_hton->savepoint_release = spider_savepoint_release;
  spider_hton->create_cursor_read_view = spider_create_cursor_read_view;
  spider_hton->set_cursor_read_view = spider_set_cursor_read_view;
  spider_hton->close_cursor_read_view = spider_close_cursor_read_view;
  */
  spider_hton->panic = spider_panic;
  spider_hton->close_connection = spider_close_connection;
  spider_hton->start_consistent_snapshot = spider_start_consistent_snapshot;
  spider_hton->flush_logs = spider_flush_logs;
  spider_hton->commit = spider_commit;
  spider_hton->rollback = spider_rollback;
  spider_hton->prepare = spider_xa_prepare;
  spider_hton->recover = spider_xa_recover;
  spider_hton->commit_by_xid = spider_xa_commit_by_xid;
  spider_hton->rollback_by_xid = spider_xa_rollback_by_xid;
  spider_hton->create = spider_create_handler;
  spider_hton->drop_database = spider_drop_database;
  spider_hton->show_status = spider_show_status;

  if(pthread_mutex_init(&spider_tbl_mutex, MY_MUTEX_INIT_FAST))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_tbl_mutex_init;
  }
  if(pthread_mutex_init(&spider_conn_mutex, MY_MUTEX_INIT_FAST))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_conn_mutex_init;
  }
  if(pthread_mutex_init(&spider_xa_lock_mutex, MY_MUTEX_INIT_FAST))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_xa_mutex_init;
  }

  if(
    hash_init(&spider_open_tables, system_charset_info, 32, 0, 0,
                   (hash_get_key) spider_tbl_get_key, 0, 0) ||
    hash_init(&spider_open_connections, system_charset_info, 32, 0, 0,
                   (hash_get_key) spider_conn_get_key, 0, 0) ||
    hash_init(&spider_xa_locks, system_charset_info, 32, 0, 0,
                   (hash_get_key) spider_xa_lock_get_key, 0, 0)
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error;
  }

  DBUG_RETURN(0);

error:
  VOID(pthread_mutex_destroy(&spider_xa_lock_mutex));
error_xa_mutex_init:
  VOID(pthread_mutex_destroy(&spider_conn_mutex));
error_conn_mutex_init:
  VOID(pthread_mutex_destroy(&spider_tbl_mutex));
error_tbl_mutex_init:
  DBUG_RETURN(error_num);
}

char *spider_create_string(
  const char *str,
  uint length
) {
  char *res;
  DBUG_ENTER("spider_create_string");
  if (!(res = (char*) my_malloc(length + 1, MYF(MY_WME))))
    DBUG_RETURN(NULL);
  memcpy(res, str, length);
  res[length] = '\0';
  DBUG_RETURN(res);
}

char *spider_create_table_name_string(
  const char *table_name,
  const char *part_name,
  const char *sub_name
) {
  char *res, *tmp;
  uint length = strlen(table_name);
  DBUG_ENTER("spider_create_table_name_string");
  if (part_name)
  {
    length += sizeof("#P#") - 1 + strlen(part_name);
    if (sub_name)
      length += sizeof("#SP#") - 1 + strlen(sub_name);
  }
  if (!(res = (char*) my_malloc(length + 1, MYF(MY_WME))))
    DBUG_RETURN(NULL);
  tmp = strmov(res, table_name);
  if (part_name)
  {
    tmp = strmov(tmp, "#P#");
    tmp = strmov(tmp, part_name);
    if (sub_name)
    {
      tmp = strmov(tmp, "#SP#");
      tmp = strmov(tmp, sub_name);
    }
  }
  DBUG_RETURN(res);
}

#ifdef WITH_PARTITION_STORAGE_ENGINE
void spider_get_partition_info(
  const char *table_name,
  const TABLE *table,
  partition_element **part_elem,
  partition_element **sub_elem
) {
  char tmp_name[FN_LEN];
  partition_info *part_info = table->part_info;
  DBUG_ENTER("spider_get_partition_info");
  *part_elem = NULL;
  *sub_elem = NULL;
  if (!part_info)
    DBUG_VOID_RETURN;

  DBUG_PRINT("info",("spider table_name=%s", table_name));
  List_iterator<partition_element> part_it(part_info->partitions);
  while ((*part_elem = part_it++))
  {
    if ((*part_elem)->subpartitions.elements)
    {
      List_iterator<partition_element> sub_it((*part_elem)->subpartitions);
      while ((*sub_elem = sub_it++))
      {
        create_subpartition_name(tmp_name, table->s->path.str,
          (*part_elem)->partition_name, (*sub_elem)->partition_name,
          NORMAL_PART_NAME);
        DBUG_PRINT("info",("spider tmp_name=%s", tmp_name));
        if (!strcmp(table_name, tmp_name))
          DBUG_VOID_RETURN;
      }
    } else {
      create_partition_name(tmp_name, table->s->path.str,
        (*part_elem)->partition_name, NORMAL_PART_NAME, TRUE);
      DBUG_PRINT("info",("spider tmp_name=%s", tmp_name));
      if (!strcmp(table_name, tmp_name))
        DBUG_VOID_RETURN;
    }
  }
  *part_elem = NULL;
  *sub_elem = NULL;
  DBUG_PRINT("info",("spider no hit"));
  DBUG_VOID_RETURN;
}
#endif
