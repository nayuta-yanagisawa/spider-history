/* Copyright (C) 2009 Kentoku Shiba

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
#include "ha_spider.h"
#include "spd_db_conn.h"
#include "spd_trx.h"
#include "spd_conn.h"
#include "spd_table.h"
#include "spd_direct_sql.h"
#include "spd_udf.h"

extern HASH spider_open_connections;
extern pthread_mutex_t spider_conn_mutex;

uint spider_udf_calc_hash(
  char *key,
  uint mod
) {
  uint sum = 0;
  DBUG_ENTER("spider_udf_calc_hash");
  while (*key != '\0')
  {
    sum += *key;
    key++;
  }
  DBUG_PRINT("info",("spider calc hash = %u", sum % mod));
  DBUG_RETURN(sum % mod);
}

int spider_udf_direct_sql_create_table_list(
  SPIDER_DIRECT_SQL *direct_sql,
  char *table_name_list,
  uint table_name_list_length
) {
  int table_count, roop_count, length;
  char *tmp_ptr, *tmp_ptr2, *tmp_ptr3, *tmp_name_ptr;
  THD *thd = direct_sql->trx->thd;
  DBUG_ENTER("spider_udf_direct_sql_create_table_list");
  tmp_ptr = table_name_list;
  while (*tmp_ptr == ' ')
    tmp_ptr++;
  if (*tmp_ptr)
    table_count = 1;
  else {
    direct_sql->table_count = 0;
    DBUG_RETURN(0);
  }

  while (TRUE)
  {
    if ((tmp_ptr2 = strchr(tmp_ptr, ' ')))
    {
      table_count++;
      tmp_ptr = tmp_ptr2 + 1;
      while (*tmp_ptr == ' ')
        tmp_ptr++;
    } else
      break;
  }
  if (!(direct_sql->db_names = (char**)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &direct_sql->db_names, sizeof(char*) * table_count,
      &direct_sql->table_names, sizeof(char*) * table_count,
      &direct_sql->tables, sizeof(TABLE*) * table_count,
      &tmp_name_ptr, sizeof(char) * (
        table_name_list_length +
        thd->db_length * table_count +
        2 * table_count
      ),
      &direct_sql->iop, sizeof(int) * table_count,
      NullS))
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  tmp_ptr = table_name_list;
  while (*tmp_ptr == ' ')
    tmp_ptr++;
  roop_count = 0;
  while (TRUE)
  {
    if ((tmp_ptr2 = strchr(tmp_ptr, ' ')))
      *tmp_ptr2 = '\0';

    direct_sql->db_names[roop_count] = tmp_name_ptr;

    if ((tmp_ptr3 = strchr(tmp_ptr, '.')))
    {
      /* exist database name */
      *tmp_ptr3 = '\0';
      length = strlen(tmp_ptr);
      memcpy(tmp_name_ptr, tmp_ptr, length + 1);
      tmp_name_ptr += length + 1;
      tmp_ptr = tmp_ptr3 + 1;
    } else {
      memcpy(tmp_name_ptr, thd->db,
        thd->db_length + 1);
      tmp_name_ptr += thd->db_length + 1;
    }

    direct_sql->table_names[roop_count] = tmp_name_ptr;
    length = strlen(tmp_ptr);
    memcpy(tmp_name_ptr, tmp_ptr, length + 1);
    tmp_name_ptr += length + 1;

    DBUG_PRINT("info",("spider db=%s",
      direct_sql->db_names[roop_count]));
    DBUG_PRINT("info",("spider table_name=%s",
      direct_sql->table_names[roop_count]));

    if (!tmp_ptr2)
      break;
    tmp_ptr = tmp_ptr2 + 1;
    while (*tmp_ptr == ' ')
      tmp_ptr++;
    roop_count++;
  }
  direct_sql->table_count = table_count;
  DBUG_RETURN(0);
}

int spider_udf_direct_sql_create_conn_key(
  SPIDER_DIRECT_SQL *direct_sql
) {
  char *tmp_name, port_str[6];
  DBUG_ENTER("spider_udf_direct_sql_create_conn_key");

  /* tgt_db not use */
  direct_sql->conn_key_length
    = 1
    + direct_sql->tgt_wrapper_length + 1
    + direct_sql->tgt_host_length + 1
    + 5 + 1
    + direct_sql->tgt_socket_length + 1
    + direct_sql->tgt_username_length + 1
    + direct_sql->tgt_password_length + 1
    + direct_sql->tgt_ssl_ca_length + 1
    + direct_sql->tgt_ssl_capath_length + 1
    + direct_sql->tgt_ssl_cert_length + 1
    + direct_sql->tgt_ssl_cipher_length + 1
    + direct_sql->tgt_ssl_key_length + 1
    + 1 + 1
    + direct_sql->tgt_default_file_length + 1
    + direct_sql->tgt_default_group_length;
  if (!(direct_sql->conn_key = (char *)
        my_malloc(direct_sql->conn_key_length + 1, MYF(MY_WME | MY_ZEROFILL)))
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  if (direct_sql->connection_channel > 48)
    *direct_sql->conn_key = '0' + 48 - direct_sql->connection_channel;
  else
    *direct_sql->conn_key = '0' + direct_sql->connection_channel;
  DBUG_PRINT("info",("spider tgt_wrapper=%s", direct_sql->tgt_wrapper));
  tmp_name = strmov(direct_sql->conn_key + 1, direct_sql->tgt_wrapper);
  DBUG_PRINT("info",("spider tgt_host=%s", direct_sql->tgt_host));
  tmp_name = strmov(tmp_name + 1, direct_sql->tgt_host);
  my_sprintf(port_str, (port_str, "%05ld", direct_sql->tgt_port));
  DBUG_PRINT("info",("spider port_str=%s", port_str));
  tmp_name = strmov(tmp_name + 1, port_str);
  if (direct_sql->tgt_socket)
  {
    DBUG_PRINT("info",("spider tgt_socket=%s", direct_sql->tgt_socket));
    tmp_name = strmov(tmp_name + 1, direct_sql->tgt_socket);
  } else
    tmp_name++;
  if (direct_sql->tgt_username)
  {
    DBUG_PRINT("info",("spider tgt_username=%s", direct_sql->tgt_username));
    tmp_name = strmov(tmp_name + 1, direct_sql->tgt_username);
  } else
    tmp_name++;
  if (direct_sql->tgt_password)
  {
    DBUG_PRINT("info",("spider tgt_password=%s", direct_sql->tgt_password));
    tmp_name = strmov(tmp_name + 1, direct_sql->tgt_password);
  } else
    tmp_name++;
  if (direct_sql->tgt_ssl_ca)
  {
    DBUG_PRINT("info",("spider tgt_ssl_ca=%s", direct_sql->tgt_ssl_ca));
    tmp_name = strmov(tmp_name + 1, direct_sql->tgt_ssl_ca);
  } else
    tmp_name++;
  if (direct_sql->tgt_ssl_capath)
  {
    DBUG_PRINT("info",("spider tgt_ssl_capath=%s",
      direct_sql->tgt_ssl_capath));
    tmp_name = strmov(tmp_name + 1, direct_sql->tgt_ssl_capath);
  } else
    tmp_name++;
  if (direct_sql->tgt_ssl_cert)
  {
    DBUG_PRINT("info",("spider tgt_ssl_cert=%s", direct_sql->tgt_ssl_cert));
    tmp_name = strmov(tmp_name + 1, direct_sql->tgt_ssl_cert);
  } else
    tmp_name++;
  if (direct_sql->tgt_ssl_cipher)
  {
    DBUG_PRINT("info",("spider tgt_ssl_cipher=%s",
      direct_sql->tgt_ssl_cipher));
    tmp_name = strmov(tmp_name + 1, direct_sql->tgt_ssl_cipher);
  } else
    tmp_name++;
  if (direct_sql->tgt_ssl_key)
  {
    DBUG_PRINT("info",("spider tgt_ssl_key=%s", direct_sql->tgt_ssl_key));
    tmp_name = strmov(tmp_name + 1, direct_sql->tgt_ssl_key);
  } else
    tmp_name++;
  tmp_name++;
  *tmp_name = '0' + direct_sql->tgt_ssl_vsc;
  if (direct_sql->tgt_default_file)
  {
    DBUG_PRINT("info",("spider tgt_default_file=%s",
      direct_sql->tgt_default_file));
    tmp_name = strmov(tmp_name + 1, direct_sql->tgt_default_file);
  } else
    tmp_name++;
  if (direct_sql->tgt_default_group)
  {
    DBUG_PRINT("info",("spider tgt_default_group=%s",
      direct_sql->tgt_default_group));
    tmp_name = strmov(tmp_name + 1, direct_sql->tgt_default_group);
  } else
    tmp_name++;
  DBUG_RETURN(0);
}

SPIDER_CONN *spider_udf_direct_sql_create_conn(
  const SPIDER_DIRECT_SQL *direct_sql,
  int *error_num
) {
  SPIDER_CONN *conn;
  char *tmp_name, *tmp_host, *tmp_username, *tmp_password, *tmp_socket;
  char *tmp_wrapper, *tmp_ssl_ca, *tmp_ssl_capath, *tmp_ssl_cert;
  char *tmp_ssl_cipher, *tmp_ssl_key, *tmp_default_file, *tmp_default_group;
  DBUG_ENTER("spider_udf_direct_sql_create_conn");

  if (!(conn = (SPIDER_CONN *)
       my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
                        &conn, sizeof(*conn),
                        &tmp_name, direct_sql->conn_key_length + 1,
                        &tmp_host, direct_sql->tgt_host_length + 1,
                        &tmp_username, direct_sql->tgt_username_length + 1,
                        &tmp_password, direct_sql->tgt_password_length + 1,
                        &tmp_socket, direct_sql->tgt_socket_length + 1,
                        &tmp_wrapper, direct_sql->tgt_wrapper_length + 1,
                        &tmp_ssl_ca, direct_sql->tgt_ssl_ca_length + 1,
                        &tmp_ssl_capath, direct_sql->tgt_ssl_capath_length + 1,
                        &tmp_ssl_cert, direct_sql->tgt_ssl_cert_length + 1,
                        &tmp_ssl_cipher, direct_sql->tgt_ssl_cipher_length + 1,
                        &tmp_ssl_key, direct_sql->tgt_ssl_key_length + 1,
                        &tmp_default_file,
                          direct_sql->tgt_default_file_length + 1,
                        &tmp_default_group,
                          direct_sql->tgt_default_group_length + 1,
                        NullS))
  ) {
    *error_num = HA_ERR_OUT_OF_MEM;
    goto error_alloc_conn;
  }

  conn->conn_key_length = direct_sql->conn_key_length;
  conn->conn_key = tmp_name;
  memcpy(conn->conn_key, direct_sql->conn_key, direct_sql->conn_key_length);
  conn->tgt_host_length = direct_sql->tgt_host_length;
  conn->tgt_host = tmp_host;
  memcpy(conn->tgt_host, direct_sql->tgt_host, direct_sql->tgt_host_length);
  conn->tgt_username_length = direct_sql->tgt_username_length;
  conn->tgt_username = tmp_username;
  memcpy(conn->tgt_username, direct_sql->tgt_username,
    direct_sql->tgt_username_length);
  conn->tgt_password_length = direct_sql->tgt_password_length;
  conn->tgt_password = tmp_password;
  memcpy(conn->tgt_password, direct_sql->tgt_password,
    direct_sql->tgt_password_length);
  conn->tgt_socket_length = direct_sql->tgt_socket_length;
  conn->tgt_socket = tmp_socket;
  memcpy(conn->tgt_socket, direct_sql->tgt_socket,
    direct_sql->tgt_socket_length);
  conn->tgt_wrapper_length = direct_sql->tgt_wrapper_length;
  conn->tgt_wrapper = tmp_wrapper;
  memcpy(conn->tgt_wrapper, direct_sql->tgt_wrapper,
    direct_sql->tgt_wrapper_length);
  conn->tgt_ssl_ca_length = direct_sql->tgt_ssl_ca_length;
  if (conn->tgt_ssl_ca_length)
  {
    conn->tgt_ssl_ca = tmp_ssl_ca;
    memcpy(conn->tgt_ssl_ca, direct_sql->tgt_ssl_ca,
      direct_sql->tgt_ssl_ca_length);
  } else
    conn->tgt_ssl_ca = NULL;
  conn->tgt_ssl_capath_length = direct_sql->tgt_ssl_capath_length;
  if (conn->tgt_ssl_capath_length)
  {
    conn->tgt_ssl_capath = tmp_ssl_capath;
    memcpy(conn->tgt_ssl_capath, direct_sql->tgt_ssl_capath,
      direct_sql->tgt_ssl_capath_length);
  } else
    conn->tgt_ssl_capath = NULL;
  conn->tgt_ssl_cert_length = direct_sql->tgt_ssl_cert_length;
  if (conn->tgt_ssl_cert_length)
  {
    conn->tgt_ssl_cert = tmp_ssl_cert;
    memcpy(conn->tgt_ssl_cert, direct_sql->tgt_ssl_cert,
      direct_sql->tgt_ssl_cert_length);
  } else
    conn->tgt_ssl_cert = NULL;
  conn->tgt_ssl_cipher_length = direct_sql->tgt_ssl_cipher_length;
  if (conn->tgt_ssl_cipher_length)
  {
    conn->tgt_ssl_cipher = tmp_ssl_cipher;
    memcpy(conn->tgt_ssl_cipher, direct_sql->tgt_ssl_cipher,
      direct_sql->tgt_ssl_cipher_length);
  } else
    conn->tgt_ssl_cipher = NULL;
  conn->tgt_ssl_key_length = direct_sql->tgt_ssl_key_length;
  if (conn->tgt_ssl_key_length)
  {
    conn->tgt_ssl_key = tmp_ssl_key;
    memcpy(conn->tgt_ssl_key, direct_sql->tgt_ssl_key,
      direct_sql->tgt_ssl_key_length);
  } else
    conn->tgt_ssl_key = NULL;
  conn->tgt_default_file_length = direct_sql->tgt_default_file_length;
  if (conn->tgt_default_file_length)
  {
    conn->tgt_default_file = tmp_default_file;
    memcpy(conn->tgt_default_file, direct_sql->tgt_default_file,
      direct_sql->tgt_default_file_length);
  } else
    conn->tgt_default_file = NULL;
  conn->tgt_default_group_length = direct_sql->tgt_default_group_length;
  if (conn->tgt_default_group_length)
  {
    conn->tgt_default_group = tmp_default_group;
    memcpy(conn->tgt_default_group, direct_sql->tgt_default_group,
      direct_sql->tgt_default_group_length);
  } else
    conn->tgt_default_group = NULL;
  conn->tgt_port = direct_sql->tgt_port;
  conn->tgt_ssl_vsc = direct_sql->tgt_ssl_vsc;
  conn->db_conn = NULL;
  conn->join_trx = 0;
  conn->thd = NULL;
  conn->table_lock = 0;
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

  if ((*error_num = spider_db_udf_direct_sql_connect(direct_sql, conn)))
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

SPIDER_CONN *spider_udf_direct_sql_get_conn(
  const SPIDER_DIRECT_SQL *direct_sql,
  SPIDER_TRX *trx,
  int *error_num
) {
  SPIDER_CONN *conn;
  DBUG_ENTER("spider_udf_direct_sql_get_conn");

  if (
    !(conn = (SPIDER_CONN*) hash_search(&trx->trx_conn_hash,
        (uchar*) direct_sql->conn_key, direct_sql->conn_key_length))
  ) {
    if (
      (THDVAR(trx->thd, conn_recycle_mode) & 1) ||
      THDVAR(trx->thd, conn_recycle_strict)
    ) {
      pthread_mutex_lock(&spider_conn_mutex);
      if (!(conn = (SPIDER_CONN*) hash_search(&spider_open_connections,
                                             (uchar*) direct_sql->conn_key,
                                             direct_sql->conn_key_length)))
      {
        pthread_mutex_unlock(&spider_conn_mutex);
        DBUG_PRINT("info",("spider create new conn"));
        if(!(conn = spider_udf_direct_sql_create_conn(direct_sql, error_num)))
          goto error;
      } else {
        hash_delete(&spider_open_connections, (uchar*) conn);
        pthread_mutex_unlock(&spider_conn_mutex);
        DBUG_PRINT("info",("spider get global conn"));
      }
    } else {
      DBUG_PRINT("info",("spider create new conn"));
      /* conn_recycle_strict = 0 and conn_recycle_mode = 0 or 2 */
      if(!(conn = spider_udf_direct_sql_create_conn(direct_sql, error_num)))
        goto error;
    }
    conn->thd = trx->thd;
    conn->priority = direct_sql->priority;

    if (my_hash_insert(&trx->trx_conn_hash, (uchar*) conn))
    {
      spider_free_conn(conn);
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
  }

  DBUG_PRINT("info",("spider conn=%x", conn));
  DBUG_RETURN(conn);

error:
  DBUG_RETURN(NULL);
}

int spider_udf_direct_sql_get_server(
  SPIDER_DIRECT_SQL *direct_sql
) {
  MEM_ROOT mem_root;
  int error_num, length;
  FOREIGN_SERVER *server, server_buf;
  DBUG_ENTER("spider_udf_direct_sql_get_server");
  init_alloc_root(&mem_root, 65, 0);

  if (!(server
       = get_server_by_name(&mem_root, direct_sql->server_name, &server_buf)))
  {
    error_num = ER_FOREIGN_SERVER_DOESNT_EXIST;
    goto error_get_server;
  }

  if (!direct_sql->tgt_wrapper && server->scheme)
  {
    direct_sql->tgt_wrapper_length = strlen(server->scheme);
    if (!(direct_sql->tgt_wrapper =
      spider_create_string(server->scheme, direct_sql->tgt_wrapper_length)))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_wrapper=%s", direct_sql->tgt_wrapper));
  }

  if (!direct_sql->tgt_host && server->host)
  {
    direct_sql->tgt_host_length = strlen(server->host);
    if (!(direct_sql->tgt_host =
      spider_create_string(server->host, direct_sql->tgt_host_length)))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_host=%s", direct_sql->tgt_host));
  }

  if (direct_sql->tgt_port == -1)
  {
    direct_sql->tgt_port = server->port;
    DBUG_PRINT("info",("spider tgt_port=%d", direct_sql->tgt_port));
  }

  if (!direct_sql->tgt_socket && server->socket)
  {
    direct_sql->tgt_socket_length = strlen(server->socket);
    if (!(direct_sql->tgt_socket =
      spider_create_string(server->socket, direct_sql->tgt_socket_length)))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_socket=%s", direct_sql->tgt_socket));
  }

  if (!direct_sql->tgt_default_db_name && server->db &&
    (length = strlen(server->db)))
  {
    direct_sql->tgt_default_db_name_length = length;
    if (!(direct_sql->tgt_default_db_name =
      spider_create_string(server->db, length)))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_default_db_name=%s",
      direct_sql->tgt_default_db_name));
  }

  if (!direct_sql->tgt_username && server->username)
  {
    direct_sql->tgt_username_length = strlen(server->username);
    if (!(direct_sql->tgt_username =
      spider_create_string(server->username, direct_sql->tgt_username_length)))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_username=%s", direct_sql->tgt_username));
  }

  if (!direct_sql->tgt_password && server->password)
  {
    direct_sql->tgt_password_length = strlen(server->password);
    if (!(direct_sql->tgt_password =
      spider_create_string(server->password, direct_sql->tgt_password_length)))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_password=%s", direct_sql->tgt_password));
  }

  free_root(&mem_root, MYF(0));
  DBUG_RETURN(0);

error_get_server:
  my_error(error_num, MYF(0), direct_sql->server_name);
error:
  free_root(&mem_root, MYF(0));
  DBUG_RETURN(error_num);
}

#define SPIDER_PARAM_STR_LEN(name) name ## _length
#define SPIDER_PARAM_STR(title_name, param_name) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (!direct_sql->param_name) \
    { \
      if ((direct_sql->param_name = spider_get_string_between_quote( \
        start_ptr, TRUE))) \
        direct_sql->SPIDER_PARAM_STR_LEN(param_name) = \
          strlen(direct_sql->param_name); \
      else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
      DBUG_PRINT("info",("spider "title_name"=%s", direct_sql->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_HINT_WITH_MAX(title_name, param_name, check_length, max_size, min_val, max_val) \
  if (!strncasecmp(tmp_ptr, title_name, check_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    DBUG_PRINT("info",("spider max_size=%d", max_size)); \
    int hint_num = atoi(tmp_ptr + check_length) - 1; \
    DBUG_PRINT("info",("spider hint_num=%d", hint_num)); \
    DBUG_PRINT("info",("spider direct_sql->param_name=%x", \
      direct_sql->param_name)); \
    if (direct_sql->param_name) \
    { \
      if (hint_num < 0 || hint_num >= max_size) \
      { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } else if (direct_sql->param_name[hint_num] != -1) \
        break; \
      char *hint_str = spider_get_string_between_quote(start_ptr, FALSE); \
      if (hint_str) \
      { \
        direct_sql->param_name[hint_num] = atoi(hint_str); \
        if (direct_sql->param_name[hint_num] < min_val) \
          direct_sql->param_name[hint_num] = min_val; \
        else if (direct_sql->param_name[hint_num] > max_val) \
          direct_sql->param_name[hint_num] = max_val; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
      DBUG_PRINT("info",("spider "title_name"[%d]=%d", hint_num, \
        direct_sql->param_name[hint_num])); \
    } else { \
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
        MYF(0), tmp_ptr); \
      goto error; \
    } \
    break; \
  }
#define SPIDER_PARAM_INT_WITH_MAX(title_name, param_name, min_val, max_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (direct_sql->param_name == -1) \
    { \
      if ((tmp_ptr2 = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        direct_sql->param_name = atoi(tmp_ptr2); \
        if (direct_sql->param_name < min_val) \
          direct_sql->param_name = min_val; \
        else if (direct_sql->param_name > max_val) \
          direct_sql->param_name = max_val; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
      DBUG_PRINT("info",("spider "title_name"=%d", direct_sql->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_INT(title_name, param_name, min_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (direct_sql->param_name == -1) \
    { \
      if ((tmp_ptr2 = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        direct_sql->param_name = atoi(tmp_ptr2); \
        if (direct_sql->param_name < min_val) \
          direct_sql->param_name = min_val; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
      DBUG_PRINT("info",("spider "title_name"=%d", direct_sql->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_LONGLONG(title_name, param_name, min_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (direct_sql->param_name == -1) \
    { \
      if ((tmp_ptr2 = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        direct_sql->param_name = \
          my_strtoll10(tmp_ptr2, (char**) NULL, &error_num); \
        if (direct_sql->param_name < min_val) \
          direct_sql->param_name = min_val; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
      DBUG_PRINT("info",("spider "title_name"=%lld", \
        direct_sql->param_name)); \
    } \
    break; \
  }

int spider_udf_parse_direct_sql_param(
  SPIDER_TRX *trx,
  SPIDER_DIRECT_SQL *direct_sql,
  char *param,
  int param_length
) {
  int error_num = 0, roop_count;
  char *param_string = NULL;
  char *sprit_ptr[2];
  char *tmp_ptr, *tmp_ptr2, *start_ptr;
  int title_length;
  DBUG_ENTER("spider_udf_parse_direct_sql_param");
  direct_sql->tgt_port = -1;
  direct_sql->tgt_ssl_vsc = -1;
  direct_sql->table_loop_mode = -1;
  direct_sql->priority = -1;
  direct_sql->net_timeout = -1;
  direct_sql->bulk_insert_rows = -1;
  direct_sql->connection_channel = -1;
  for (roop_count = 0; roop_count < direct_sql->table_count; roop_count++)
    direct_sql->iop[roop_count] = -1;

  if (param_length == 0)
    goto set_default;
  DBUG_PRINT("info",("spider create param_string string"));
  if (
    !(param_string = spider_create_string(
      param,
      param_length))
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    goto error_alloc_param_string;
  }
  DBUG_PRINT("info",("spider param_string=%s", param_string));

  sprit_ptr[0] = param_string;
  while (sprit_ptr[0])
  {
    if ((sprit_ptr[1] = strchr(sprit_ptr[0], ',')))
    {
      *sprit_ptr[1] = '\0';
      sprit_ptr[1]++;
    }
    tmp_ptr = sprit_ptr[0];
    sprit_ptr[0] = sprit_ptr[1];
    while (*tmp_ptr == ' ' || *tmp_ptr == '\r' ||
      *tmp_ptr == '\n' || *tmp_ptr == '\t')
      tmp_ptr++;

    if (*tmp_ptr == '\0')
      continue;

    title_length = 0;
    start_ptr = tmp_ptr;
    while (*start_ptr != ' ' && *start_ptr != '\'' &&
      *start_ptr != '"' && *start_ptr != '\0' &&
      *start_ptr != '\r' && *start_ptr != '\n' &&
      *start_ptr != '\t')
    {
      title_length++;
      start_ptr++;
    }

    switch (title_length)
    {
      case 0:
        continue;
      case 3:
        SPIDER_PARAM_LONGLONG("bir", bulk_insert_rows, 0);
        SPIDER_PARAM_INT_WITH_MAX("cch", connection_channel, 0, 63);
        SPIDER_PARAM_STR("dff", tgt_default_file);
        SPIDER_PARAM_STR("dfg", tgt_default_group);
        SPIDER_PARAM_INT("nto", net_timeout, 0);
        SPIDER_PARAM_STR("sca", tgt_ssl_ca);
        SPIDER_PARAM_STR("sch", tgt_ssl_cipher);
        SPIDER_PARAM_STR("scp", tgt_ssl_capath);
        SPIDER_PARAM_STR("scr", tgt_ssl_cert);
        SPIDER_PARAM_STR("sky", tgt_ssl_key);
        SPIDER_PARAM_STR("srv", server_name);
        SPIDER_PARAM_INT_WITH_MAX("svc", tgt_ssl_vsc, 0, 1);
        SPIDER_PARAM_INT_WITH_MAX("tlm", table_loop_mode, 0, 2);
        SPIDER_PARAM_LONGLONG("prt", priority, 0);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 4:
        SPIDER_PARAM_STR("host", tgt_host);
        SPIDER_PARAM_STR("user", tgt_username);
        SPIDER_PARAM_INT_WITH_MAX("port", tgt_port, 0, 65535);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 6:
        SPIDER_PARAM_STR("server", server_name);
        SPIDER_PARAM_STR("socket", tgt_socket);
        SPIDER_PARAM_HINT_WITH_MAX("iop", iop, 3, direct_sql->table_count, 0, 2);
        SPIDER_PARAM_STR("ssl_ca", tgt_ssl_ca);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 7:
        SPIDER_PARAM_STR("wrapper", tgt_wrapper);
        SPIDER_PARAM_STR("ssl_key", tgt_ssl_key);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 8:
        SPIDER_PARAM_STR("database", tgt_default_db_name);
        SPIDER_PARAM_STR("password", tgt_password);
        SPIDER_PARAM_LONGLONG("priority", priority, 0);
        SPIDER_PARAM_STR("ssl_cert", tgt_ssl_cert);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 10:
        SPIDER_PARAM_STR("ssl_cipher", tgt_ssl_cipher);
        SPIDER_PARAM_STR("ssl_capath", tgt_ssl_capath);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 11:
        SPIDER_PARAM_INT("net_timeout", net_timeout, 0);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 12:
        SPIDER_PARAM_STR("default_file", tgt_default_file);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 13:
        SPIDER_PARAM_STR("default_group", tgt_default_group);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 15:
        SPIDER_PARAM_INT_WITH_MAX("table_loop_mode", table_loop_mode, 0, 2);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 16:
        SPIDER_PARAM_LONGLONG("bulk_insert_rows", bulk_insert_rows, 1);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 18:
        SPIDER_PARAM_INT_WITH_MAX(
          "connection_channel", connection_channel, 0, 63);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      case 22:
        SPIDER_PARAM_INT_WITH_MAX("ssl_verify_server_cert", tgt_ssl_vsc, 0, 1);
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
      default:
        error_num = ER_SPIDER_INVALID_UDF_PARAM_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_UDF_PARAM_STR,
          MYF(0), tmp_ptr);
        goto error;
    }
  }

set_default:
  if ((error_num = spider_udf_set_direct_sql_param_default(
    trx,
    direct_sql
  )))
    goto error;

  if (param_string)
    my_free(param_string, MYF(0));
  DBUG_RETURN(0);

error:
  if (param_string)
    my_free(param_string, MYF(0));
error_alloc_param_string:
  DBUG_RETURN(error_num);
}

int spider_udf_set_direct_sql_param_default(
  SPIDER_TRX *trx,
  SPIDER_DIRECT_SQL *direct_sql
) {
  int error_num, roop_count;
  DBUG_ENTER("spider_udf_set_direct_sql_param_default");
  if (direct_sql->server_name)
  {
    if ((error_num = spider_udf_direct_sql_get_server(direct_sql)))
      DBUG_RETURN(error_num);
  }

  if (!direct_sql->tgt_default_db_name)
  {
    DBUG_PRINT("info",("spider create default tgt_default_db_name"));
    direct_sql->tgt_default_db_name_length = trx->thd->db_length;
    if (
      !(direct_sql->tgt_default_db_name = spider_create_string(
        trx->thd->db,
        direct_sql->tgt_default_db_name_length))
    ) {
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
  }

  if (!direct_sql->tgt_wrapper)
  {
    DBUG_PRINT("info",("spider create default tgt_wrapper"));
    direct_sql->tgt_wrapper_length = SPIDER_DB_WRAPPER_LEN;
    if (
      !(direct_sql->tgt_wrapper = spider_create_string(
        SPIDER_DB_WRAPPER_STR,
        direct_sql->tgt_wrapper_length))
    ) {
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
  }

  if (!direct_sql->tgt_host)
  {
    DBUG_PRINT("info",("spider create default tgt_host"));
    direct_sql->tgt_host_length = strlen(my_localhost);
    if (
      !(direct_sql->tgt_host = spider_create_string(
        my_localhost,
        direct_sql->tgt_host_length))
    ) {
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
  }

  if (
    !direct_sql->tgt_default_file &&
    direct_sql->tgt_default_group &&
    (my_defaults_file || my_defaults_extra_file)
  ) {
    DBUG_PRINT("info",("spider create default tgt_default_file"));
    if (my_defaults_extra_file)
    {
      direct_sql->tgt_default_file_length = strlen(my_defaults_extra_file);
      if (
        !(direct_sql->tgt_default_file = spider_create_string(
          my_defaults_extra_file,
          direct_sql->tgt_default_file_length))
      ) {
        my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    } else {
      direct_sql->tgt_default_file_length = strlen(my_defaults_file);
      if (
        !(direct_sql->tgt_default_file = spider_create_string(
          my_defaults_file,
          direct_sql->tgt_default_file_length))
      ) {
        my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    }
  }

  if (direct_sql->tgt_port == -1)
    direct_sql->tgt_port = MYSQL_PORT;
  else if (direct_sql->tgt_port < 0)
    direct_sql->tgt_port = 0;
  else if (direct_sql->tgt_port > 65535)
    direct_sql->tgt_port = 65535;

  if (direct_sql->tgt_ssl_vsc == -1)
    direct_sql->tgt_ssl_vsc = 0;

  if (
    !direct_sql->tgt_socket &&
    !strcmp(direct_sql->tgt_host, my_localhost)
  ) {
    DBUG_PRINT("info",("spider create default tgt_socket"));
    direct_sql->tgt_socket_length = strlen((char *) MYSQL_UNIX_ADDR);
    if (
      !(direct_sql->tgt_socket = spider_create_string(
        (char *) MYSQL_UNIX_ADDR,
        direct_sql->tgt_socket_length))
    ) {
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
  }

  if (direct_sql->table_loop_mode == -1)
    direct_sql->table_loop_mode = 0;
  if (direct_sql->priority == -1)
    direct_sql->priority = 1000000;
  if (direct_sql->net_timeout == -1)
    direct_sql->net_timeout = 600;
  if (direct_sql->bulk_insert_rows == -1)
    direct_sql->bulk_insert_rows = 3000;
  if (direct_sql->connection_channel == -1)
    direct_sql->connection_channel = 0;
  for (roop_count = 0; roop_count < direct_sql->table_count; roop_count++)
  {
    if (direct_sql->iop[roop_count] == -1)
      direct_sql->iop[roop_count] = 0;
  }
  DBUG_RETURN(0);
}

void spider_udf_free_direct_sql_alloc(
  SPIDER_DIRECT_SQL *direct_sql,
  my_bool bg
) {
  SPIDER_BG_DIRECT_SQL *bg_direct_sql;
  DBUG_ENTER("spider_udf_free_direct_sql_alloc");
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (bg)
  {
    pthread_mutex_lock(direct_sql->bg_mutex);
    bg_direct_sql = (SPIDER_BG_DIRECT_SQL *) direct_sql->parent;
    if (bg_direct_sql->direct_sql == direct_sql)
      bg_direct_sql->direct_sql = direct_sql->next;
    if (direct_sql->next)
      direct_sql->next->prev = direct_sql->prev;
    if (direct_sql->prev)
      direct_sql->prev->next = direct_sql->next;
    pthread_cond_signal(direct_sql->bg_cond);
    pthread_mutex_unlock(direct_sql->bg_mutex);
  }
#endif
  if (direct_sql->server_name)
    my_free(direct_sql->server_name, MYF(0));
  if (direct_sql->tgt_default_db_name)
    my_free(direct_sql->tgt_default_db_name, MYF(0));
  if (direct_sql->tgt_host)
    my_free(direct_sql->tgt_host, MYF(0));
  if (direct_sql->tgt_username)
    my_free(direct_sql->tgt_username, MYF(0));
  if (direct_sql->tgt_password)
    my_free(direct_sql->tgt_password, MYF(0));
  if (direct_sql->tgt_socket)
    my_free(direct_sql->tgt_socket, MYF(0));
  if (direct_sql->tgt_wrapper)
    my_free(direct_sql->tgt_wrapper, MYF(0));
  if (direct_sql->tgt_ssl_ca)
    my_free(direct_sql->tgt_ssl_ca, MYF(0));
  if (direct_sql->tgt_ssl_capath)
    my_free(direct_sql->tgt_ssl_capath, MYF(0));
  if (direct_sql->tgt_ssl_cert)
    my_free(direct_sql->tgt_ssl_cert, MYF(0));
  if (direct_sql->tgt_ssl_cipher)
    my_free(direct_sql->tgt_ssl_cipher, MYF(0));
  if (direct_sql->tgt_ssl_key)
    my_free(direct_sql->tgt_ssl_key, MYF(0));
  if (direct_sql->tgt_default_file)
    my_free(direct_sql->tgt_default_file, MYF(0));
  if (direct_sql->tgt_default_group)
    my_free(direct_sql->tgt_default_group, MYF(0));
  if (direct_sql->conn_key)
    my_free(direct_sql->conn_key, MYF(0));
  if (direct_sql->db_names)
    my_free(direct_sql->db_names, MYF(0));
  my_free(direct_sql, MYF(0));
  DBUG_VOID_RETURN;
}

long long spider_direct_sql_body(
  UDF_INIT *initid,
  UDF_ARGS *args,
  char *is_null,
  char *error,
  my_bool bg
) {
  int error_num, roop_count;
  SPIDER_DIRECT_SQL *direct_sql = NULL, *tmp_direct_sql;
  THD *thd = current_thd;
  SPIDER_TRX *trx;
  SPIDER_CONN *conn;
  char *sql;
  TABLE_LIST table_list;
  SPIDER_BG_DIRECT_SQL *bg_direct_sql;
  DBUG_ENTER("spider_direct_sql_body");
  if (!(direct_sql = (SPIDER_DIRECT_SQL *)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &direct_sql, sizeof(SPIDER_DIRECT_SQL),
      &sql, sizeof(char) * args->lengths[0],
      NullS))
  ) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    goto error;
  }
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (bg)
  {
    bg_direct_sql = (SPIDER_BG_DIRECT_SQL *) initid->ptr;
    pthread_mutex_lock(&bg_direct_sql->bg_mutex);
    tmp_direct_sql = (SPIDER_DIRECT_SQL *) bg_direct_sql->direct_sql;
    bg_direct_sql->direct_sql = direct_sql;
    if (tmp_direct_sql)
    {
      tmp_direct_sql->prev = direct_sql;
      direct_sql->next = tmp_direct_sql;
    }
    pthread_mutex_unlock(&bg_direct_sql->bg_mutex);
    direct_sql->bg_mutex = &bg_direct_sql->bg_mutex;
    direct_sql->bg_cond = &bg_direct_sql->bg_cond;
    direct_sql->parent = bg_direct_sql;
    bg_direct_sql->called_cnt++;
  }
#endif
  if (!(trx = spider_get_trx(thd, &error_num)))
  {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    goto error;
  }
  direct_sql->trx = trx;

  if (args->args[1])
  {
    if (spider_udf_direct_sql_create_table_list(
      direct_sql,
      args->args[1],
      args->lengths[1]
    )) {
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      goto error;
    }
  } else {
    if (spider_udf_direct_sql_create_table_list(
      direct_sql,
      "",
      0
    )) {
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      goto error;
    }
  }
  if (args->args[2])
  {
    if (spider_udf_parse_direct_sql_param(
      trx,
      direct_sql,
      args->args[2],
      args->lengths[2]
    )) {
      goto error;
    }
  } else {
    if (spider_udf_parse_direct_sql_param(
      trx,
      direct_sql,
      "",
      0
    )) {
      goto error;
    }
  }
  for (roop_count = 0; roop_count < direct_sql->table_count; roop_count++)
  {
    table_list.db = direct_sql->db_names[roop_count];
    table_list.table_name = direct_sql->table_names[roop_count];
    if (!(direct_sql->tables[roop_count] =
      find_temporary_table(thd, &table_list)))
    {
      my_printf_error(ER_SPIDER_UDF_TMP_TABLE_NOT_FOUND_NUM,
        ER_SPIDER_UDF_TMP_TABLE_NOT_FOUND_STR,
        MYF(0), table_list.db, table_list.table_name);
      goto error;
    }
  }
  if (spider_udf_direct_sql_create_conn_key(direct_sql))
  {
    goto error;
  }
  if (!(conn = spider_udf_direct_sql_get_conn(direct_sql, trx, &error_num)))
  {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    goto error;
  }
  direct_sql->conn = conn;
  if (spider_db_udf_check_and_set_set_names(trx))
  {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    goto error;
  }
  if (args->args[0])
  {
    direct_sql->sql_length = args->lengths[0];
    memcpy(sql, args->args[0], direct_sql->sql_length);
  } else
    direct_sql->sql_length = 0;
  direct_sql->sql = sql;

#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (bg)
  {
    if (spider_udf_bg_direct_sql(direct_sql))
    {
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      goto error;
    }
  } else {
#endif
    if (conn->bg_init)
      pthread_mutex_lock(&conn->bg_conn_mutex);
    if (spider_db_udf_direct_sql(direct_sql))
    {
      if (conn->bg_init)
        pthread_mutex_unlock(&conn->bg_conn_mutex);
      if (direct_sql->modified_non_trans_table)
        thd->transaction.stmt.modified_non_trans_table = TRUE;
      goto error;
    }
    if (conn->bg_init)
      pthread_mutex_unlock(&conn->bg_conn_mutex);
    if (direct_sql->modified_non_trans_table)
      thd->transaction.stmt.modified_non_trans_table = TRUE;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  }
  if (!bg)
    spider_udf_free_direct_sql_alloc(direct_sql, FALSE);
#endif
  DBUG_RETURN(1);

error:
  if (direct_sql)
    spider_udf_free_direct_sql_alloc(direct_sql, bg);
  *error = 1;
  DBUG_RETURN(0);
}

my_bool spider_direct_sql_init_body(
  UDF_INIT *initid,
  UDF_ARGS *args,
  char *message,
  my_bool bg
) {
  SPIDER_BG_DIRECT_SQL *bg_direct_sql;
  DBUG_ENTER("spider_direct_sql_init_body");
  if (args->arg_count != 3)
  {
    strcpy(message, "spider_(bg)_direct_sql() requires 3 arguments");
    goto error;
  }
  if (
    args->arg_type[0] != STRING_RESULT ||
    args->arg_type[1] != STRING_RESULT ||
    args->arg_type[2] != STRING_RESULT
  ) {
    strcpy(message, "spider_(bg)_direct_sql() requires string arguments");
    goto error;
  }
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (bg)
  {
    if (!(bg_direct_sql = (SPIDER_BG_DIRECT_SQL *)
        my_malloc(sizeof(SPIDER_BG_DIRECT_SQL), MYF(MY_WME | MY_ZEROFILL)))
    ) {
      strcpy(message, "spider_bg_direct_sql() out of memory");
      goto error;
    }
    if (pthread_mutex_init(&bg_direct_sql->bg_mutex, MY_MUTEX_INIT_FAST))
    {
      strcpy(message, "spider_bg_direct_sql() out of memory");
      goto error_mutex_init;
    }
    if (pthread_cond_init(&bg_direct_sql->bg_cond, NULL))
    {
      strcpy(message, "spider_bg_direct_sql() out of memory");
      goto error_cond_init;
    }
    initid->ptr = (char *) bg_direct_sql;
  }
#endif
  DBUG_RETURN(FALSE);

#ifndef WITHOUT_SPIDER_BG_SEARCH
error_cond_init:
  VOID(pthread_mutex_destroy(&bg_direct_sql->bg_mutex));
error_mutex_init:
  my_free(bg_direct_sql, MYF(0));
#endif
error:
  DBUG_RETURN(TRUE);
}

void spider_direct_sql_deinit_body(
  UDF_INIT *initid
) {
  SPIDER_BG_DIRECT_SQL *bg_direct_sql = (SPIDER_BG_DIRECT_SQL *) initid->ptr;
  DBUG_ENTER("spider_direct_sql_deinit_body");
  if (bg_direct_sql)
  {
    VOID(pthread_cond_destroy(&bg_direct_sql->bg_cond));
    VOID(pthread_mutex_destroy(&bg_direct_sql->bg_mutex));
    my_free(bg_direct_sql, MYF(0));
  }
  DBUG_VOID_RETURN;
}

#ifndef WITHOUT_SPIDER_BG_SEARCH
void spider_direct_sql_bg_start(
  UDF_INIT *initid
) {
  SPIDER_BG_DIRECT_SQL *bg_direct_sql = (SPIDER_BG_DIRECT_SQL *) initid->ptr;
  DBUG_ENTER("spider_direct_sql_bg_start");
  bg_direct_sql->called_cnt = 0;
  bg_direct_sql->bg_error = 0;
  DBUG_VOID_RETURN;
}

long long spider_direct_sql_bg_end(
  UDF_INIT *initid
) {
  THD *thd = current_thd;
  SPIDER_BG_DIRECT_SQL *bg_direct_sql = (SPIDER_BG_DIRECT_SQL *) initid->ptr;
  DBUG_ENTER("spider_direct_sql_bg_end");
  pthread_mutex_lock(&bg_direct_sql->bg_mutex);
  while (bg_direct_sql->direct_sql)
    pthread_cond_wait(&bg_direct_sql->bg_cond, &bg_direct_sql->bg_mutex);
  pthread_mutex_unlock(&bg_direct_sql->bg_mutex);
  if (bg_direct_sql->modified_non_trans_table)
    thd->transaction.stmt.modified_non_trans_table = TRUE;
  if (bg_direct_sql->bg_error)
  {
    my_message(bg_direct_sql->bg_error, bg_direct_sql->bg_error_msg, MYF(0));
    DBUG_RETURN(0);
  }
  DBUG_RETURN(bg_direct_sql->called_cnt);
}

int spider_udf_bg_direct_sql(
  SPIDER_DIRECT_SQL *direct_sql
) {
  int error_num;
  SPIDER_CONN *conn = direct_sql->conn;
  DBUG_ENTER("spider_udf_bg_direct_sql");
  if ((error_num = spider_create_conn_thread(conn)))
    DBUG_RETURN(error_num);
  pthread_mutex_lock(&conn->bg_conn_mutex);
  conn->bg_target = direct_sql;
  conn->bg_direct_sql = TRUE;
  conn->bg_caller_sync_wait = TRUE;
  pthread_mutex_lock(&conn->bg_conn_sync_mutex);
  pthread_cond_signal(&conn->bg_conn_cond);
  pthread_mutex_unlock(&conn->bg_conn_mutex);
  pthread_cond_wait(&conn->bg_conn_sync_cond, &conn->bg_conn_sync_mutex);
  pthread_mutex_unlock(&conn->bg_conn_sync_mutex);
  conn->bg_caller_sync_wait = FALSE;
  DBUG_RETURN(0);
}
#endif
