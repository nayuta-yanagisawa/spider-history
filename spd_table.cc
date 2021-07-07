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
#include "spd_ping_table.h"

handlerton *spider_hton_ptr;

extern HASH spider_xa_locks;
extern pthread_mutex_t spider_xa_lock_mutex;
extern HASH spider_open_connections;
extern pthread_mutex_t spider_conn_mutex;
extern HASH *spider_udf_table_mon_list_hash;
extern pthread_mutex_t *spider_udf_table_mon_mutexes;
extern pthread_cond_t *spider_udf_table_mon_conds;
extern pthread_mutex_t spider_open_conn_mutex;

HASH spider_open_tables;
pthread_mutex_t spider_tbl_mutex;
HASH spider_init_error_tables;
pthread_mutex_t spider_init_error_tbl_mutex;

#ifdef WITH_PARTITION_STORAGE_ENGINE
HASH spider_open_pt_share;
pthread_mutex_t spider_pt_share_mutex;
#endif

#ifndef WITHOUT_SPIDER_BG_SEARCH
pthread_attr_t spider_pt_attr;

pthread_mutex_t spider_global_trx_mutex;
SPIDER_TRX *spider_global_trx;
#endif

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

#ifdef WITH_PARTITION_STORAGE_ENGINE
uchar *spider_pt_share_get_key(
  SPIDER_PARTITION_SHARE *share,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
) {
  DBUG_ENTER("spider_pt_share_get_key");
  *length = share->table_name_length;
  DBUG_RETURN((uchar*) share->table_name);
}
#endif

uchar *spider_ha_get_key(
  ha_spider *spider,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
) {
  DBUG_ENTER("spider_ha_get_key");
  *length = spider->share->table_name_length;
  DBUG_RETURN((uchar*) spider->share->table_name);
}

uchar *spider_udf_tbl_mon_list_key(
  SPIDER_TABLE_MON_LIST *table_mon_list,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
) {
  DBUG_ENTER("spider_udf_tbl_mon_list_key");
  DBUG_PRINT("info",("spider hash key=%s", table_mon_list->key));
  DBUG_PRINT("info",("spider hash key length=%ld", table_mon_list->key_length));
  *length = table_mon_list->key_length;
  DBUG_RETURN((uchar*) table_mon_list->key);
}

int spider_get_server(
  SPIDER_SHARE *share,
  int link_idx
) {
  MEM_ROOT mem_root;
  int error_num, length;
  FOREIGN_SERVER *server, server_buf;
  DBUG_ENTER("spider_get_server");
  init_alloc_root(&mem_root, 128, 0);

  if (!(server
       = get_server_by_name(&mem_root, share->server_names[link_idx],
         &server_buf)))
  {
    error_num = ER_FOREIGN_SERVER_DOESNT_EXIST;
    goto error;
  }

  if (!share->tgt_wrappers[link_idx] && server->scheme)
  {
    share->tgt_wrappers_lengths[link_idx] = strlen(server->scheme);
    if (!(share->tgt_wrappers[link_idx] =
      spider_create_string(server->scheme,
      share->tgt_wrappers_lengths[link_idx])))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_wrappers=%s",
      share->tgt_wrappers[link_idx]));
  }

  if (!share->tgt_hosts[link_idx] && server->host)
  {
    share->tgt_hosts_lengths[link_idx] = strlen(server->host);
    if (!(share->tgt_hosts[link_idx] =
      spider_create_string(server->host, share->tgt_hosts_lengths[link_idx])))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_hosts=%s", share->tgt_hosts[link_idx]));
  }

  if (share->tgt_ports[link_idx] == -1)
  {
    share->tgt_ports[link_idx] = server->port;
    DBUG_PRINT("info",("spider tgt_ports=%d", share->tgt_ports[link_idx]));
  }

  if (!share->tgt_sockets[link_idx] && server->socket)
  {
    share->tgt_sockets_lengths[link_idx] = strlen(server->socket);
    if (!(share->tgt_sockets[link_idx] =
      spider_create_string(server->socket,
      share->tgt_sockets_lengths[link_idx])))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_sockets=%s", share->tgt_sockets[link_idx]));
  }

  if (!share->tgt_dbs[link_idx] && server->db && (length = strlen(server->db)))
  {
    share->tgt_dbs_lengths[link_idx] = length;
    if (!(share->tgt_dbs[link_idx] =
      spider_create_string(server->db, length)))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_dbs=%s", share->tgt_dbs[link_idx]));
  }

  if (!share->tgt_usernames[link_idx] && server->username)
  {
    share->tgt_usernames_lengths[link_idx] = strlen(server->username);
    if (!(share->tgt_usernames[link_idx] =
      spider_create_string(server->username,
      share->tgt_usernames_lengths[link_idx])))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_usernames=%s",
      share->tgt_usernames[link_idx]));
  }

  if (!share->tgt_passwords[link_idx] && server->password)
  {
    share->tgt_passwords_lengths[link_idx] = strlen(server->password);
    if (!(share->tgt_passwords[link_idx] =
      spider_create_string(server->password,
      share->tgt_passwords_lengths[link_idx])))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error;
    }
    DBUG_PRINT("info",("spider tgt_passwords=%s",
      share->tgt_passwords[link_idx]));
  }

  free_root(&mem_root, MYF(0));
  DBUG_RETURN(0);

error:
  free_root(&mem_root, MYF(0));
  my_error(error_num, MYF(0), share->server_names[link_idx]);
  DBUG_RETURN(error_num);
}

int spider_free_share_alloc(
  SPIDER_SHARE *share
) {
  int roop_count;
  DBUG_ENTER("spider_free_share_alloc");
  if (share->server_names)
  {
    for (roop_count = 0; roop_count < share->server_names_length; roop_count++)
    {
      if (share->server_names[roop_count])
        my_free(share->server_names[roop_count], MYF(0));
    }
    my_free(share->server_names, MYF(0));
  }
  if (share->tgt_table_names)
  {
    for (roop_count = 0; roop_count < share->tgt_table_names_length;
      roop_count++)
    {
      if (share->tgt_table_names[roop_count])
        my_free(share->tgt_table_names[roop_count], MYF(0));
    }
    my_free(share->tgt_table_names, MYF(0));
  }
  if (share->tgt_dbs)
  {
    for (roop_count = 0; roop_count < share->tgt_dbs_length; roop_count++)
    {
      if (share->tgt_dbs[roop_count])
        my_free(share->tgt_dbs[roop_count], MYF(0));
    }
    my_free(share->tgt_dbs, MYF(0));
  }
  if (share->tgt_hosts)
  {
    for (roop_count = 0; roop_count < share->tgt_hosts_length; roop_count++)
    {
      if (share->tgt_hosts[roop_count])
        my_free(share->tgt_hosts[roop_count], MYF(0));
    }
    my_free(share->tgt_hosts, MYF(0));
  }
  if (share->tgt_usernames)
  {
    for (roop_count = 0; roop_count < share->tgt_usernames_length;
      roop_count++)
    {
      if (share->tgt_usernames[roop_count])
        my_free(share->tgt_usernames[roop_count], MYF(0));
    }
    my_free(share->tgt_usernames, MYF(0));
  }
  if (share->tgt_passwords)
  {
    for (roop_count = 0; roop_count < share->tgt_passwords_length;
      roop_count++)
    {
      if (share->tgt_passwords[roop_count])
        my_free(share->tgt_passwords[roop_count], MYF(0));
    }
    my_free(share->tgt_passwords, MYF(0));
  }
  if (share->tgt_sockets)
  {
    for (roop_count = 0; roop_count < share->tgt_sockets_length; roop_count++)
    {
      if (share->tgt_sockets[roop_count])
        my_free(share->tgt_sockets[roop_count], MYF(0));
    }
    my_free(share->tgt_sockets, MYF(0));
  }
  if (share->tgt_wrappers)
  {
    for (roop_count = 0; roop_count < share->tgt_wrappers_length; roop_count++)
    {
      if (share->tgt_wrappers[roop_count])
        my_free(share->tgt_wrappers[roop_count], MYF(0));
    }
    my_free(share->tgt_wrappers, MYF(0));
  }
  if (share->tgt_ssl_cas)
  {
    for (roop_count = 0; roop_count < share->tgt_ssl_cas_length; roop_count++)
    {
      if (share->tgt_ssl_cas[roop_count])
        my_free(share->tgt_ssl_cas[roop_count], MYF(0));
    }
    my_free(share->tgt_ssl_cas, MYF(0));
  }
  if (share->tgt_ssl_capaths)
  {
    for (roop_count = 0; roop_count < share->tgt_ssl_capaths_length;
      roop_count++)
    {
      if (share->tgt_ssl_capaths[roop_count])
        my_free(share->tgt_ssl_capaths[roop_count], MYF(0));
    }
    my_free(share->tgt_ssl_capaths, MYF(0));
  }
  if (share->tgt_ssl_certs)
  {
    for (roop_count = 0; roop_count < share->tgt_ssl_certs_length;
      roop_count++)
    {
      if (share->tgt_ssl_certs[roop_count])
        my_free(share->tgt_ssl_certs[roop_count], MYF(0));
    }
    my_free(share->tgt_ssl_certs, MYF(0));
  }
  if (share->tgt_ssl_ciphers)
  {
    for (roop_count = 0; roop_count < share->tgt_ssl_ciphers_length;
      roop_count++)
    {
      if (share->tgt_ssl_ciphers[roop_count])
        my_free(share->tgt_ssl_ciphers[roop_count], MYF(0));
    }
    my_free(share->tgt_ssl_ciphers, MYF(0));
  }
  if (share->tgt_ssl_keys)
  {
    for (roop_count = 0; roop_count < share->tgt_ssl_keys_length; roop_count++)
    {
      if (share->tgt_ssl_keys[roop_count])
        my_free(share->tgt_ssl_keys[roop_count], MYF(0));
    }
    my_free(share->tgt_ssl_keys, MYF(0));
  }
  if (share->tgt_default_files)
  {
    for (roop_count = 0; roop_count < share->tgt_default_files_length;
      roop_count++)
    {
      if (share->tgt_default_files[roop_count])
        my_free(share->tgt_default_files[roop_count], MYF(0));
    }
    my_free(share->tgt_default_files, MYF(0));
  }
  if (share->tgt_default_groups)
  {
    for (roop_count = 0; roop_count < share->tgt_default_groups_length;
      roop_count++)
    {
      if (share->tgt_default_groups[roop_count])
        my_free(share->tgt_default_groups[roop_count], MYF(0));
    }
    my_free(share->tgt_default_groups, MYF(0));
  }
  if (share->conn_keys)
    my_free(share->conn_keys, MYF(0));
  if (share->tgt_ports)
    my_free(share->tgt_ports, MYF(0));
  if (share->tgt_ssl_vscs)
    my_free(share->tgt_ssl_vscs, MYF(0));
  if (share->link_statuses)
    my_free(share->link_statuses, MYF(0));
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (share->monitoring_bg_kind)
    my_free(share->monitoring_bg_kind, MYF(0));
#endif
  if (share->monitoring_kind)
    my_free(share->monitoring_kind, MYF(0));
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (share->monitoring_bg_interval)
    my_free(share->monitoring_bg_interval, MYF(0));
#endif
  if (share->monitoring_limit)
    my_free(share->monitoring_limit, MYF(0));
  if (share->monitoring_sid)
    my_free(share->monitoring_sid, MYF(0));
  if (share->alter_table.tmp_server_names)
    my_free(share->alter_table.tmp_server_names, MYF(0));
  if (share->table_select)
    delete [] share->table_select;
  if (share->key_select)
    delete [] share->key_select;
  if (share->key_hint)
    delete [] share->key_hint;
  spider_db_free_show_table_status(share);
  spider_db_free_show_index(share);
  spider_db_free_set_names(share);
  spider_db_free_column_name_str(share);
  spider_db_free_table_names_str(share);
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (share->partition_share)
    spider_free_pt_share(share->partition_share);
#endif
  DBUG_RETURN(0);
}

void spider_free_tmp_share_alloc(
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_free_tmp_share_alloc");
  if (share->server_names && share->server_names[0])
  {
    my_free(share->server_names[0], MYF(0));
    share->server_names[0] = NULL;
  }
  if (share->tgt_table_names && share->tgt_table_names[0])
  {
    my_free(share->tgt_table_names[0], MYF(0));
    share->tgt_table_names[0] = NULL;
  }
  if (share->tgt_dbs && share->tgt_dbs[0])
  {
    my_free(share->tgt_dbs[0], MYF(0));
    share->tgt_dbs[0] = NULL;
  }
  if (share->tgt_hosts && share->tgt_hosts[0])
  {
    my_free(share->tgt_hosts[0], MYF(0));
    share->tgt_hosts[0] = NULL;
  }
  if (share->tgt_usernames && share->tgt_usernames[0])
  {
    my_free(share->tgt_usernames[0], MYF(0));
    share->tgt_usernames[0] = NULL;
  }
  if (share->tgt_passwords && share->tgt_passwords[0])
  {
    my_free(share->tgt_passwords[0], MYF(0));
    share->tgt_passwords[0] = NULL;
  }
  if (share->tgt_sockets && share->tgt_sockets[0])
  {
    my_free(share->tgt_sockets[0], MYF(0));
    share->tgt_sockets[0] = NULL;
  }
  if (share->tgt_wrappers && share->tgt_wrappers[0])
  {
    my_free(share->tgt_wrappers[0], MYF(0));
    share->tgt_wrappers[0] = NULL;
  }
  if (share->tgt_ssl_cas && share->tgt_ssl_cas[0])
  {
    my_free(share->tgt_ssl_cas[0], MYF(0));
    share->tgt_ssl_cas[0] = NULL;
  }
  if (share->tgt_ssl_capaths && share->tgt_ssl_capaths[0])
  {
    my_free(share->tgt_ssl_capaths[0], MYF(0));
    share->tgt_ssl_capaths[0] = NULL;
  }
  if (share->tgt_ssl_certs && share->tgt_ssl_certs[0])
  {
    my_free(share->tgt_ssl_certs[0], MYF(0));
    share->tgt_ssl_certs[0] = NULL;
  }
  if (share->tgt_ssl_ciphers && share->tgt_ssl_ciphers[0])
  {
    my_free(share->tgt_ssl_ciphers[0], MYF(0));
    share->tgt_ssl_ciphers[0] = NULL;
  }
  if (share->tgt_ssl_keys && share->tgt_ssl_keys[0])
  {
    my_free(share->tgt_ssl_keys[0], MYF(0));
    share->tgt_ssl_keys[0] = NULL;
  }
  if (share->tgt_default_files && share->tgt_default_files[0])
  {
    my_free(share->tgt_default_files[0], MYF(0));
    share->tgt_default_files[0] = NULL;
  }
  if (share->tgt_default_groups && share->tgt_default_groups[0])
  {
    my_free(share->tgt_default_groups[0], MYF(0));
    share->tgt_default_groups[0] = NULL;
  }
  if (share->conn_keys)
  {
    my_free(share->conn_keys, MYF(0));
    share->conn_keys = NULL;
  }
  if (share->key_hint)
  {
    delete [] share->key_hint;
    share->key_hint = NULL;
  }
  spider_db_free_table_names_str(share);
  DBUG_VOID_RETURN;
}

char *spider_get_string_between_quote(
  char *ptr,
  bool alloc
) {
  char *start_ptr, *end_ptr, *tmp_ptr, *esc_ptr;
  bool find_flg = FALSE, esc_flg = FALSE;
  DBUG_ENTER("spider_get_string_between_quote");

  start_ptr = strchr(ptr, '\'');
  end_ptr = strchr(ptr, '"');
  if (start_ptr && (!end_ptr || start_ptr < end_ptr))
  {
    tmp_ptr = ++start_ptr;
    while (!find_flg)
    {
      if (!(end_ptr = strchr(tmp_ptr, '\'')))
        DBUG_RETURN(NULL);
      esc_ptr = tmp_ptr;
      while (!find_flg)
      {
        esc_ptr = strchr(esc_ptr, '\\');
        if (!esc_ptr || esc_ptr > end_ptr)
          find_flg = TRUE;
        else if (esc_ptr == end_ptr - 1)
        {
          esc_flg = TRUE;
          tmp_ptr = end_ptr + 1;
          break;
        } else {
          esc_flg = TRUE;
          esc_ptr += 2;
        }
      }
    }
  } else if (end_ptr)
  {
    start_ptr = end_ptr;
    tmp_ptr = ++start_ptr;
    while (!find_flg)
    {
      if (!(end_ptr = strchr(tmp_ptr, '"')))
        DBUG_RETURN(NULL);
      esc_ptr = tmp_ptr;
      while (!find_flg)
      {
        esc_ptr = strchr(esc_ptr, '\\');
        if (!esc_ptr || esc_ptr > end_ptr)
          find_flg = TRUE;
        else if (esc_ptr == end_ptr - 1)
        {
          esc_flg = TRUE;
          tmp_ptr = end_ptr + 1;
          break;
        } else {
          esc_flg = TRUE;
          esc_ptr += 2;
        }
      }
    }
  } else
    DBUG_RETURN(NULL);

  *end_ptr = '\0';
  if (esc_flg)
  {
    esc_ptr = start_ptr;
    while (TRUE)
    {
      esc_ptr = strchr(esc_ptr, '\\');
      if (!esc_ptr)
        break;
      switch(*(esc_ptr + 1))
      {
        case 'b':
          *esc_ptr = '\b';
          break;
        case 'n':
          *esc_ptr = '\n';
          break;
        case 'r':
          *esc_ptr = '\r';
          break;
        case 't':
          *esc_ptr = '\t';
          break;
        default:
          *esc_ptr = *(esc_ptr + 1);
          break;
      }
      esc_ptr++;
      strcpy(esc_ptr, esc_ptr + 1);
    }
  }
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

int spider_create_string_list(
  char ***string_list,
  uint **string_length_list,
  uint *list_length,
  char *str,
  uint length
) {
  int roop_count;
  char *tmp_ptr, *tmp_ptr2, *tmp_ptr3, *esc_ptr;
  bool find_flg = FALSE;
  DBUG_ENTER("spider_create_string_list");

  *list_length = 0;
  if (!str)
  {
    *string_list = NULL;
    DBUG_RETURN(0);
  }

  tmp_ptr = str;
  while (*tmp_ptr == ' ')
    tmp_ptr++;
  if (*tmp_ptr)
    *list_length = 1;
  else {
    *string_list = NULL;
    DBUG_RETURN(0);
  }

  while (TRUE)
  {
    if ((tmp_ptr2 = strchr(tmp_ptr, ' ')))
    {
      esc_ptr = tmp_ptr;
      while (!find_flg)
      {
        esc_ptr = strchr(esc_ptr, '\\');
        if (!esc_ptr || esc_ptr > tmp_ptr2)
          find_flg = TRUE;
        else if (esc_ptr == tmp_ptr2 - 1)
        {
          tmp_ptr = tmp_ptr2 + 1;
          break;
        } else
          esc_ptr += 2;
      }
      if (find_flg)
      {
        (*list_length)++;
        tmp_ptr = tmp_ptr2 + 1;
        while (*tmp_ptr == ' ')
          tmp_ptr++;
      }
    } else
      break;
  }

  if (!(*string_list = (char**)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      string_list, sizeof(char*) * (*list_length),
      string_length_list, sizeof(int) * (*list_length),
      NullS))
  ) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  tmp_ptr = str;
  while (*tmp_ptr == ' ')
  {
    *tmp_ptr = '\0';
    tmp_ptr++;
  }
  tmp_ptr3 = tmp_ptr;

  for (roop_count = 0; roop_count < *list_length - 1; roop_count++)
  {
    while (TRUE)
    {
      tmp_ptr2 = strchr(tmp_ptr, ' ');

      esc_ptr = tmp_ptr;
      while (!find_flg)
      {
        esc_ptr = strchr(esc_ptr, '\\');
        if (!esc_ptr || esc_ptr > tmp_ptr2)
          find_flg = TRUE;
        else if (esc_ptr == tmp_ptr2 - 1)
        {
          tmp_ptr = tmp_ptr2 + 1;
          break;
        } else
          esc_ptr += 2;
      }
      if (find_flg)
        break;
    }
    tmp_ptr = tmp_ptr2;

    while (*tmp_ptr == ' ')
    {
      *tmp_ptr = '\0';
      tmp_ptr++;
    }

    (*string_length_list)[roop_count] = strlen(tmp_ptr3);
    if (!((*string_list)[roop_count] = spider_create_string(
      tmp_ptr3, (*string_length_list)[roop_count]))
    ) {
      my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    }
    DBUG_PRINT("info",("spider string_list[%d]=%s", roop_count,
      (*string_list)[roop_count]));
    tmp_ptr3 = tmp_ptr;
  }
  (*string_length_list)[roop_count] = strlen(tmp_ptr3);
  if (!((*string_list)[roop_count] = spider_create_string(
    tmp_ptr3, (*string_length_list)[roop_count]))
  ) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }
  DBUG_PRINT("info",("spider string_list[%d]=%s", roop_count,
    (*string_list)[roop_count]));

  DBUG_RETURN(0);
}

int spider_create_long_list(
  long **long_list,
  uint *list_length,
  char *str,
  uint length,
  long min_val,
  long max_val
) {
  int roop_count;
  char *tmp_ptr;
  DBUG_ENTER("spider_create_long_list");

  *list_length = 0;
  if (!str)
  {
    *long_list = NULL;
    DBUG_RETURN(0);
  }

  tmp_ptr = str;
  while (*tmp_ptr == ' ')
    tmp_ptr++;
  if (*tmp_ptr)
    *list_length = 1;
  else {
    *long_list = NULL;
    DBUG_RETURN(0);
  }

  while (TRUE)
  {
    if ((tmp_ptr = strchr(tmp_ptr, ' ')))
    {
      (*list_length)++;
      tmp_ptr = tmp_ptr + 1;
      while (*tmp_ptr == ' ')
        tmp_ptr++;
    } else
      break;
  }

  if (!(*long_list = (long*)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      long_list, sizeof(long) * (*list_length),
      NullS))
  ) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  tmp_ptr = str;
  for (roop_count = 0; roop_count < *list_length; roop_count++)
  {
    if (roop_count != 0)
      tmp_ptr = strchr(tmp_ptr, ' ');

    while (*tmp_ptr == ' ')
    {
      *tmp_ptr = '\0';
      tmp_ptr++;
    }
    (*long_list)[roop_count] = atol(tmp_ptr);
    if ((*long_list)[roop_count] < min_val)
      (*long_list)[roop_count] = min_val;
    else if ((*long_list)[roop_count] > max_val)
      (*long_list)[roop_count] = max_val;
  }

#ifndef DBUG_OFF
  for (roop_count = 0; roop_count < *list_length; roop_count++)
  {
    DBUG_PRINT("info",("spider long_list[%d]=%ld", roop_count,
      (*long_list)[roop_count]));
  }
#endif

  DBUG_RETURN(0);
}

int spider_create_longlong_list(
  longlong **longlong_list,
  uint *list_length,
  char *str,
  uint length,
  longlong min_val,
  longlong max_val
) {
  int error_num, roop_count;
  char *tmp_ptr;
  DBUG_ENTER("spider_create_longlong_list");

  *list_length = 0;
  if (!str)
  {
    *longlong_list = NULL;
    DBUG_RETURN(0);
  }

  tmp_ptr = str;
  while (*tmp_ptr == ' ')
    tmp_ptr++;
  if (*tmp_ptr)
    *list_length = 1;
  else {
    *longlong_list = NULL;
    DBUG_RETURN(0);
  }

  while (TRUE)
  {
    if ((tmp_ptr = strchr(tmp_ptr, ' ')))
    {
      (*list_length)++;
      tmp_ptr = tmp_ptr + 1;
      while (*tmp_ptr == ' ')
        tmp_ptr++;
    } else
      break;
  }

  if (!(*longlong_list = (longlong *)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      longlong_list, sizeof(longlong) * (*list_length),
      NullS))
  ) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  tmp_ptr = str;
  for (roop_count = 0; roop_count < *list_length; roop_count++)
  {
    if (roop_count != 0)
      tmp_ptr = strchr(tmp_ptr, ' ');

    while (*tmp_ptr == ' ')
    {
      *tmp_ptr = '\0';
      tmp_ptr++;
    }
    (*longlong_list)[roop_count] = my_strtoll10(tmp_ptr, (char**) NULL,
      &error_num);
    if ((*longlong_list)[roop_count] < min_val)
      (*longlong_list)[roop_count] = min_val;
    else if ((*longlong_list)[roop_count] > max_val)
      (*longlong_list)[roop_count] = max_val;
  }

#ifndef DBUG_OFF
  for (roop_count = 0; roop_count < *list_length; roop_count++)
  {
    DBUG_PRINT("info",("spider longlong_list[%d]=%lld", roop_count,
      (*longlong_list)[roop_count]));
  }
#endif

  DBUG_RETURN(0);
}

int spider_increase_string_list(
  char ***string_list,
  uint **string_length_list,
  uint *list_length,
  uint *list_charlen,
  uint link_count
) {
  int roop_count;
  char **tmp_str_list, *tmp_str;
  uint *tmp_length_list, tmp_length;
  DBUG_ENTER("spider_increase_string_list");
  if (*list_length == link_count)
    DBUG_RETURN(0);
  if (*list_length > 1)
  {
    my_printf_error(ER_SPIDER_DIFFERENT_LINK_COUNT_NUM,
      ER_SPIDER_DIFFERENT_LINK_COUNT_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_DIFFERENT_LINK_COUNT_NUM);
  }

  if (*string_list)
  {
    tmp_str = (*string_list)[0];
    tmp_length = (*string_length_list)[0];
  } else {
    tmp_str = NULL;
    tmp_length = 0;
  }

  if (!(tmp_str_list = (char**)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &tmp_str_list, sizeof(char*) * link_count,
      &tmp_length_list, sizeof(uint) * link_count,
      NullS))
  ) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  for (roop_count = 0; roop_count < link_count; roop_count++)
  {
    tmp_length_list[roop_count] = tmp_length;
    if (tmp_str)
    {
      if (!(tmp_str_list[roop_count] = spider_create_string(
        tmp_str, tmp_length))
      )
        goto error;
      DBUG_PRINT("info",("spider string_list[%d]=%s", roop_count,
        tmp_str_list[roop_count]));
    } else
      tmp_str_list[roop_count] = NULL;
  }
  if (*string_list)
  {
    if ((*string_list)[0])
      my_free((*string_list)[0], MYF(0));
    my_free(*string_list, MYF(0));
  }
  *list_charlen = (tmp_length + 1) * link_count - 1;
  *list_length = link_count;
  *string_list = tmp_str_list;
  *string_length_list = tmp_length_list;

  DBUG_RETURN(0);

error:
  for (roop_count = 0; roop_count < link_count; roop_count++)
  {
    if (tmp_str_list[roop_count])
      my_free(tmp_str_list[roop_count], MYF(0));
  }
  if (tmp_str_list)
    my_free(tmp_str_list, MYF(0));
  my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
  DBUG_RETURN(HA_ERR_OUT_OF_MEM);
}

int spider_increase_long_list(
  long **long_list,
  uint *list_length,
  uint link_count
) {
  int roop_count;
  long *tmp_long_list, tmp_long;
  DBUG_ENTER("spider_increase_long_list");
  if (*list_length == link_count)
    DBUG_RETURN(0);
  if (*list_length > 1)
  {
    my_printf_error(ER_SPIDER_DIFFERENT_LINK_COUNT_NUM,
      ER_SPIDER_DIFFERENT_LINK_COUNT_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_DIFFERENT_LINK_COUNT_NUM);
  }

  if (*long_list)
    tmp_long = (*long_list)[0];
  else
    tmp_long = -1;

  if (!(tmp_long_list = (long*)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &tmp_long_list, sizeof(long) * link_count,
      NullS))
  ) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  for (roop_count = 0; roop_count < link_count; roop_count++)
  {
    tmp_long_list[roop_count] = tmp_long;
    DBUG_PRINT("info",("spider long_list[%d]=%ld", roop_count,
      tmp_long));
  }
  if (*long_list)
    my_free(*long_list, MYF(0));
  *list_length = link_count;
  *long_list = tmp_long_list;

  DBUG_RETURN(0);
}

int spider_increase_longlong_list(
  longlong **longlong_list,
  uint *list_length,
  uint link_count
) {
  int roop_count;
  longlong *tmp_longlong_list, tmp_longlong;
  DBUG_ENTER("spider_increase_longlong_list");
  if (*list_length == link_count)
    DBUG_RETURN(0);
  if (*list_length > 1)
  {
    my_printf_error(ER_SPIDER_DIFFERENT_LINK_COUNT_NUM,
      ER_SPIDER_DIFFERENT_LINK_COUNT_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_DIFFERENT_LINK_COUNT_NUM);
  }

  if (*longlong_list)
    tmp_longlong = (*longlong_list)[0];
  else
    tmp_longlong = -1;

  if (!(tmp_longlong_list = (longlong*)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &tmp_longlong_list, sizeof(longlong) * link_count,
      NullS))
  ) {
    my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  }

  for (roop_count = 0; roop_count < link_count; roop_count++)
  {
    tmp_longlong_list[roop_count] = tmp_longlong;
    DBUG_PRINT("info",("spider longlong_list[%d]=%lld", roop_count,
      tmp_longlong));
  }
  if (*longlong_list)
    my_free(*longlong_list, MYF(0));
  *list_length = link_count;
  *longlong_list = tmp_longlong_list;

  DBUG_RETURN(0);
}

#define SPIDER_PARAM_STR_LEN(name) name ## _length
#define SPIDER_PARAM_STR(title_name, param_name) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (!share->param_name) \
    { \
      if ((share->param_name = spider_get_string_between_quote( \
        start_ptr, TRUE))) \
        share->SPIDER_PARAM_STR_LEN(param_name) = strlen(share->param_name); \
      else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
      DBUG_PRINT("info",("spider "title_name"=%s", share->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_STR_LENS(name) name ## _lengths
#define SPIDER_PARAM_STR_CHARLEN(name) name ## _charlen
#define SPIDER_PARAM_STR_LIST(title_name, param_name) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (!share->param_name) \
    { \
      if ((tmp_ptr2 = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        share->SPIDER_PARAM_STR_CHARLEN(param_name) = strlen(tmp_ptr2); \
        if ((error_num = spider_create_string_list( \
          &share->param_name, \
          &share->SPIDER_PARAM_STR_LENS(param_name), \
          &share->SPIDER_PARAM_STR_LEN(param_name), \
          tmp_ptr2, \
          share->SPIDER_PARAM_STR_CHARLEN(param_name)))) \
          goto error; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
    } \
    break; \
  }
#define SPIDER_PARAM_HINT(title_name, param_name, check_length, max_size, append_method) \
  if (!strncasecmp(tmp_ptr, title_name, check_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    DBUG_PRINT("info",("spider max_size=%d", max_size)); \
    int hint_num = atoi(tmp_ptr + check_length); \
    DBUG_PRINT("info",("spider hint_num=%d", hint_num)); \
    DBUG_PRINT("info",("spider share->param_name=%x", share->param_name)); \
    if (share->param_name) \
    { \
      if (hint_num < 0 || hint_num >= max_size) \
      { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } else if (share->param_name[hint_num].length() > 0) \
        break; \
      char *hint_str = spider_get_string_between_quote(start_ptr, FALSE); \
      if ((error_num = \
        append_method(&share->param_name[hint_num], hint_str))) \
        goto error; \
      DBUG_PRINT("info",("spider "title_name"[%d]=%s", hint_num, \
        share->param_name[hint_num].ptr())); \
    } else { \
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
        MYF(0), tmp_ptr); \
      goto error; \
    } \
    break; \
  }
#define SPIDER_PARAM_LONG_LEN(name) name ## _length
#define SPIDER_PARAM_LONG_LIST_WITH_MAX(title_name, param_name, \
  min_val, max_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (!share->param_name) \
    { \
      if ((tmp_ptr2 = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        if ((error_num = spider_create_long_list( \
          &share->param_name, \
          &share->SPIDER_PARAM_LONG_LEN(param_name), \
          tmp_ptr2, \
          strlen(tmp_ptr2), \
          min_val, max_val))) \
          goto error; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
    } \
    break; \
  }
#define SPIDER_PARAM_LONGLONG_LEN(name) name ## _length
#define SPIDER_PARAM_LONGLONG_LIST_WITH_MAX(title_name, param_name, \
  min_val, max_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (!share->param_name) \
    { \
      if ((tmp_ptr2 = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        if ((error_num = spider_create_longlong_list( \
          &share->param_name, \
          &share->SPIDER_PARAM_LONGLONG_LEN(param_name), \
          tmp_ptr2, \
          strlen(tmp_ptr2), \
          min_val, max_val))) \
          goto error; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
    } \
    break; \
  }
#define SPIDER_PARAM_INT_WITH_MAX(title_name, param_name, min_val, max_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (share->param_name == -1) \
    { \
      if ((tmp_ptr2 = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        share->param_name = atoi(tmp_ptr2); \
        if (share->param_name < min_val) \
          share->param_name = min_val; \
        else if (share->param_name > max_val) \
          share->param_name = max_val; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
      DBUG_PRINT("info",("spider "title_name"=%d", share->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_INT(title_name, param_name, min_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (share->param_name == -1) \
    { \
      if ((tmp_ptr2 = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        share->param_name = atoi(tmp_ptr2); \
        if (share->param_name < min_val) \
          share->param_name = min_val; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
      DBUG_PRINT("info",("spider "title_name"=%d", share->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_DOUBLE(title_name, param_name, min_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (share->param_name == -1) \
    { \
      if ((tmp_ptr2 = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        share->param_name = my_atof(tmp_ptr2); \
        if (share->param_name < min_val) \
          share->param_name = min_val; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
      DBUG_PRINT("info",("spider "title_name"=%f", share->param_name)); \
    } \
    break; \
  }
#define SPIDER_PARAM_LONGLONG(title_name, param_name, min_val) \
  if (!strncasecmp(tmp_ptr, title_name, title_length)) \
  { \
    DBUG_PRINT("info",("spider "title_name" start")); \
    if (share->param_name == -1) \
    { \
      if ((tmp_ptr2 = spider_get_string_between_quote( \
        start_ptr, FALSE))) \
      { \
        share->param_name = my_strtoll10(tmp_ptr2, (char**) NULL, &error_num); \
        if (share->param_name < min_val) \
          share->param_name = min_val; \
      } else { \
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM; \
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR, \
          MYF(0), tmp_ptr); \
        goto error; \
      } \
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
  char *tmp_ptr, *tmp_ptr2, *start_ptr;
  int roop_count;
  int title_length;
  SPIDER_ALTER_TABLE *share_alter;
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
  spider_get_partition_info(share->table_name, share->table_name_length, table,
    &part_elem, &sub_elem);
#endif
#ifndef WITHOUT_SPIDER_BG_SEARCH
  share->sts_bg_mode = -1;
#endif
  share->sts_interval = -1;
  share->sts_mode = -1;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  share->sts_sync = -1;
#endif
#ifndef WITHOUT_SPIDER_BG_SEARCH
  share->crd_bg_mode = -1;
#endif
  share->crd_interval = -1;
  share->crd_mode = -1;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  share->crd_sync = -1;
#endif
  share->crd_type = -1;
  share->crd_weight = -1;
  share->internal_offset = -1;
  share->internal_limit = -1;
  share->split_read = -1;
  share->semi_split_read = -1;
  share->init_sql_alloc_size = -1;
  share->reset_sql_alloc = -1;
  share->multi_split_read = -1;
  share->max_order = -1;
  share->semi_table_lock = -1;
  share->semi_table_lock_conn = -1;
  share->selupd_lock_mode = -1;
  share->query_cache = -1;
  share->internal_delayed = -1;
  share->bulk_size = -1;
  share->bulk_update_mode = -1;
  share->bulk_update_size = -1;
  share->internal_optimize = -1;
  share->internal_optimize_local = -1;
  share->scan_rate = -1;
  share->read_rate = -1;
  share->priority = -1;
  share->net_timeout = -1;
  share->quick_mode = -1;
  share->quick_page_size = -1;
  share->low_mem_read = -1;
  share->table_count_mode = -1;
  share->select_column_mode = -1;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  share->bgs_mode = -1;
  share->bgs_first_read = -1;
  share->bgs_second_read = -1;
#endif
  share->auto_increment_mode = -1;
  share->use_table_charset = -1;
  share->use_pushdown_udf = -1;
  share->direct_dup_insert = -1;

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
          SPIDER_PARAM_INT_WITH_MAX("aim", auto_increment_mode, 0, 2);
#ifndef WITHOUT_SPIDER_BG_SEARCH
          SPIDER_PARAM_LONGLONG("bfr", bgs_first_read, 0);
          SPIDER_PARAM_INT("bmd", bgs_mode, 0);
          SPIDER_PARAM_LONGLONG("bsr", bgs_second_read, 0);
#endif
          SPIDER_PARAM_INT("bsz", bulk_size, 0);
          SPIDER_PARAM_INT_WITH_MAX("bum", bulk_update_mode, 0, 2);
          SPIDER_PARAM_INT("bus", bulk_update_size, 0);
#ifndef WITHOUT_SPIDER_BG_SEARCH
          SPIDER_PARAM_INT_WITH_MAX("cbm", crd_bg_mode, 0, 1);
#endif
          SPIDER_PARAM_DOUBLE("civ", crd_interval, 0);
          SPIDER_PARAM_INT_WITH_MAX("cmd", crd_mode, 0, 3);
#ifdef WITH_PARTITION_STORAGE_ENGINE
          SPIDER_PARAM_INT_WITH_MAX("csy", crd_sync, 0, 2);
#endif
          SPIDER_PARAM_INT_WITH_MAX("ctp", crd_type, 0, 2);
          SPIDER_PARAM_DOUBLE("cwg", crd_weight, 1);
          SPIDER_PARAM_INT_WITH_MAX("ddi", direct_dup_insert, 0, 1);
          SPIDER_PARAM_STR_LIST("dff", tgt_default_files);
          SPIDER_PARAM_STR_LIST("dfg", tgt_default_groups);
          SPIDER_PARAM_INT("isa", init_sql_alloc_size, 0);
          SPIDER_PARAM_INT_WITH_MAX("idl", internal_delayed, 0, 1);
          SPIDER_PARAM_LONGLONG("ilm", internal_limit, 0);
          SPIDER_PARAM_LONGLONG("ios", internal_offset, 0);
          SPIDER_PARAM_INT_WITH_MAX("iom", internal_optimize, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX("iol", internal_optimize_local, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX("lmr", low_mem_read, 0, 1);
          SPIDER_PARAM_LONG_LIST_WITH_MAX("lst", link_statuses, 0, 3);
#ifndef WITHOUT_SPIDER_BG_SEARCH
          SPIDER_PARAM_LONGLONG_LIST_WITH_MAX(
            "mbi", monitoring_bg_interval, 0, 4294967295);
          SPIDER_PARAM_LONG_LIST_WITH_MAX("mbk", monitoring_bg_kind, 0, 3);
#endif
          SPIDER_PARAM_LONG_LIST_WITH_MAX("mkd", monitoring_kind, 0, 3);
          SPIDER_PARAM_LONGLONG_LIST_WITH_MAX(
            "mlt", monitoring_limit, 0, 9223372036854775807LL);
          SPIDER_PARAM_INT("mod", max_order, 0);
          SPIDER_PARAM_LONGLONG_LIST_WITH_MAX(
            "msi", monitoring_sid, 0, 4294967295);
          SPIDER_PARAM_INT_WITH_MAX("msr", multi_split_read, 0, 1);
          SPIDER_PARAM_INT("nto", net_timeout, 0);
          SPIDER_PARAM_LONGLONG("prt", priority, 0);
          SPIDER_PARAM_INT_WITH_MAX("qch", query_cache, 0, 2);
          SPIDER_PARAM_INT_WITH_MAX("qmd", quick_mode, 0, 2);
          SPIDER_PARAM_LONGLONG("qps", quick_page_size, 0);
          SPIDER_PARAM_DOUBLE("rrt", read_rate, 0);
          SPIDER_PARAM_INT_WITH_MAX("rsa", reset_sql_alloc, 0, 1);
#ifndef WITHOUT_SPIDER_BG_SEARCH
          SPIDER_PARAM_INT_WITH_MAX("sbm", sts_bg_mode, 0, 1);
#endif
          SPIDER_PARAM_STR_LIST("sca", tgt_ssl_cas);
          SPIDER_PARAM_STR_LIST("sch", tgt_ssl_ciphers);
          SPIDER_PARAM_INT_WITH_MAX("scm", select_column_mode, 0, 1);
          SPIDER_PARAM_STR_LIST("scp", tgt_ssl_capaths);
          SPIDER_PARAM_STR_LIST("scr", tgt_ssl_certs);
          SPIDER_PARAM_STR_LIST("sky", tgt_ssl_keys);
          SPIDER_PARAM_LONG_LIST_WITH_MAX("svc", tgt_ssl_vscs, 0, 1);
          SPIDER_PARAM_INT("srt", scan_rate, 0);
          SPIDER_PARAM_INT_WITH_MAX("slm", selupd_lock_mode, 0, 2);
          SPIDER_PARAM_DOUBLE("ssr", semi_split_read, 0);
          SPIDER_PARAM_INT_WITH_MAX("stc", semi_table_lock_conn, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX("stl", semi_table_lock, 0, 1);
          SPIDER_PARAM_STR_LIST("srv", server_names);
          SPIDER_PARAM_LONGLONG("srd", split_read, 0);
          SPIDER_PARAM_DOUBLE("siv", sts_interval, 0);
          SPIDER_PARAM_INT_WITH_MAX("smd", sts_mode, 1, 2);
#ifdef WITH_PARTITION_STORAGE_ENGINE
          SPIDER_PARAM_INT_WITH_MAX("ssy", sts_sync, 0, 2);
#endif
          SPIDER_PARAM_STR_LIST("tbl", tgt_table_names);
          SPIDER_PARAM_INT_WITH_MAX("tcm", table_count_mode, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX("upu", use_pushdown_udf, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX("utc", use_table_charset, 0, 1);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 4:
          SPIDER_PARAM_STR_LIST("host", tgt_hosts);
          SPIDER_PARAM_STR_LIST("user", tgt_usernames);
          SPIDER_PARAM_LONG_LIST_WITH_MAX("port", tgt_ports, 0, 65535);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 5:
          SPIDER_PARAM_STR_LIST("table", tgt_table_names);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 6:
          SPIDER_PARAM_STR_LIST("server", server_names);
          SPIDER_PARAM_STR_LIST("socket", tgt_sockets);
          SPIDER_PARAM_HINT(
            "idx", key_hint, 3, table->s->keys, spider_db_append_key_hint);
          SPIDER_PARAM_STR_LIST("ssl_ca", tgt_ssl_cas);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 7:
          SPIDER_PARAM_STR_LIST("wrapper", tgt_wrappers);
          SPIDER_PARAM_STR_LIST("ssl_key", tgt_ssl_keys);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 8:
          SPIDER_PARAM_STR_LIST("database", tgt_dbs);
          SPIDER_PARAM_STR_LIST("password", tgt_passwords);
          SPIDER_PARAM_INT_WITH_MAX("sts_mode", sts_mode, 1, 2);
#ifdef WITH_PARTITION_STORAGE_ENGINE
          SPIDER_PARAM_INT_WITH_MAX("sts_sync", sts_sync, 0, 2);
#endif
          SPIDER_PARAM_INT_WITH_MAX("crd_mode", crd_mode, 0, 3);
#ifdef WITH_PARTITION_STORAGE_ENGINE
          SPIDER_PARAM_INT_WITH_MAX("crd_sync", crd_sync, 0, 2);
#endif
          SPIDER_PARAM_INT_WITH_MAX("crd_type", crd_type, 0, 2);
          SPIDER_PARAM_LONGLONG("priority", priority, 0);
#ifndef WITHOUT_SPIDER_BG_SEARCH
          SPIDER_PARAM_INT("bgs_mode", bgs_mode, 0);
#endif
          SPIDER_PARAM_STR_LIST("ssl_cert", tgt_ssl_certs);
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
          SPIDER_PARAM_INT_WITH_MAX("quick_mode", quick_mode, 0, 2);
          SPIDER_PARAM_STR_LIST("ssl_cipher", tgt_ssl_ciphers);
          SPIDER_PARAM_STR_LIST("ssl_capath", tgt_ssl_capaths);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 11:
          SPIDER_PARAM_INT_WITH_MAX("query_cache", query_cache, 0, 2);
          SPIDER_PARAM_INT("net_timeout", net_timeout, 0);
#ifndef WITHOUT_SPIDER_BG_SEARCH
          SPIDER_PARAM_INT_WITH_MAX("crd_bg_mode", crd_bg_mode, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX("sts_bg_mode", sts_bg_mode, 0, 1);
#endif
          SPIDER_PARAM_LONG_LIST_WITH_MAX("link_status", link_statuses, 0, 3);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 12:
          SPIDER_PARAM_DOUBLE("sts_interval", sts_interval, 0);
          SPIDER_PARAM_DOUBLE("crd_interval", crd_interval, 0);
          SPIDER_PARAM_INT_WITH_MAX("low_mem_read", low_mem_read, 0, 1);
          SPIDER_PARAM_STR_LIST("default_file", tgt_default_files);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 13:
          SPIDER_PARAM_STR_LIST("default_group", tgt_default_groups);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 14:
          SPIDER_PARAM_LONGLONG("internal_limit", internal_limit, 0);
#ifndef WITHOUT_SPIDER_BG_SEARCH
          SPIDER_PARAM_LONGLONG("bgs_first_read", bgs_first_read, 0);
#endif
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 15:
          SPIDER_PARAM_LONGLONG("internal_offset", internal_offset, 0);
          SPIDER_PARAM_INT_WITH_MAX("reset_sql_alloc", reset_sql_alloc, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX("semi_table_lock", semi_table_lock, 0, 1);
          SPIDER_PARAM_LONGLONG("quick_page_size", quick_page_size, 0);
#ifndef WITHOUT_SPIDER_BG_SEARCH
          SPIDER_PARAM_LONGLONG("bgs_second_read", bgs_second_read, 0);
#endif
          SPIDER_PARAM_LONG_LIST_WITH_MAX("monitoring_kind", monitoring_kind, 0, 3);
          SPIDER_PARAM_DOUBLE("semi_split_read", semi_split_read, 0);
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
          SPIDER_PARAM_INT_WITH_MAX(
            "table_count_mode", table_count_mode, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX(
            "use_pushdown_udf", use_pushdown_udf, 0, 1);
          SPIDER_PARAM_LONGLONG_LIST_WITH_MAX(
            "monitoring_limit", monitoring_limit, 0, 9223372036854775807LL);
          SPIDER_PARAM_INT_WITH_MAX(
            "bulk_update_mode", bulk_update_mode, 0, 2);
          SPIDER_PARAM_INT("bulk_update_size", bulk_update_size, 0);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 17:
          SPIDER_PARAM_INT_WITH_MAX(
            "internal_optimize", internal_optimize, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX(
            "use_table_charset", use_table_charset, 0, 1);
          SPIDER_PARAM_INT_WITH_MAX(
            "direct_dup_insert", direct_dup_insert, 0, 1);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 18:
          SPIDER_PARAM_INT_WITH_MAX(
            "select_column_mode", select_column_mode, 0, 1);
#ifndef WITHOUT_SPIDER_BG_SEARCH
          SPIDER_PARAM_LONG_LIST_WITH_MAX(
            "monitoring_bg_kind", monitoring_bg_kind, 0, 3);
#endif
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 19:
          SPIDER_PARAM_INT("init_sql_alloc_size", init_sql_alloc_size, 0);
          SPIDER_PARAM_INT_WITH_MAX(
            "auto_increment_mode", auto_increment_mode, 0, 2);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 20:
          SPIDER_PARAM_LONGLONG_LIST_WITH_MAX(
            "monitoring_server_id", monitoring_sid, 0, 4294967295);
          error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
          my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
            MYF(0), tmp_ptr);
          goto error;
        case 22:
          SPIDER_PARAM_LONG_LIST_WITH_MAX(
            "ssl_verify_server_cert", tgt_ssl_vscs, 0, 1);
#ifndef WITHOUT_SPIDER_BG_SEARCH
          SPIDER_PARAM_LONGLONG_LIST_WITH_MAX(
            "monitoring_bg_interval", monitoring_bg_interval, 0, 4294967295);
#endif
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
        case 26:
          SPIDER_PARAM_INT_WITH_MAX(
            "semi_table_lock_connection", semi_table_lock_conn, 0, 1);
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

  /* check link_count */
  share->link_count = share->server_names_length;
  if (share->link_count < share->tgt_table_names_length)
    share->link_count = share->tgt_table_names_length;
  if (share->link_count < share->tgt_dbs_length)
    share->link_count = share->tgt_dbs_length;
  if (share->link_count < share->tgt_hosts_length)
    share->link_count = share->tgt_hosts_length;
  if (share->link_count < share->tgt_usernames_length)
    share->link_count = share->tgt_usernames_length;
  if (share->link_count < share->tgt_passwords_length)
    share->link_count = share->tgt_passwords_length;
  if (share->link_count < share->tgt_sockets_length)
    share->link_count = share->tgt_sockets_length;
  if (share->link_count < share->tgt_wrappers_length)
    share->link_count = share->tgt_wrappers_length;
  if (share->link_count < share->tgt_ssl_cas_length)
    share->link_count = share->tgt_ssl_cas_length;
  if (share->link_count < share->tgt_ssl_capaths_length)
    share->link_count = share->tgt_ssl_capaths_length;
  if (share->link_count < share->tgt_ssl_certs_length)
    share->link_count = share->tgt_ssl_certs_length;
  if (share->link_count < share->tgt_ssl_ciphers_length)
    share->link_count = share->tgt_ssl_ciphers_length;
  if (share->link_count < share->tgt_ssl_keys_length)
    share->link_count = share->tgt_ssl_keys_length;
  if (share->link_count < share->tgt_default_files_length)
    share->link_count = share->tgt_default_files_length;
  if (share->link_count < share->tgt_default_groups_length)
    share->link_count = share->tgt_default_groups_length;
  if (share->link_count < share->tgt_ports_length)
    share->link_count = share->tgt_ports_length;
  if (share->link_count < share->tgt_ssl_vscs_length)
    share->link_count = share->tgt_ssl_vscs_length;
  if (share->link_count < share->link_statuses_length)
    share->link_count = share->link_statuses_length;
  if (share->link_count < share->monitoring_kind_length)
    share->link_count = share->monitoring_kind_length;
  if (share->link_count < share->monitoring_limit_length)
    share->link_count = share->monitoring_limit_length;
  if (share->link_count < share->monitoring_sid_length)
    share->link_count = share->monitoring_sid_length;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (share->link_count < share->monitoring_bg_kind_length)
    share->link_count = share->monitoring_bg_kind_length;
  if (share->link_count < share->monitoring_bg_interval_length)
    share->link_count = share->monitoring_bg_interval_length;
#endif
  if ((error_num = spider_increase_string_list(
    &share->server_names,
    &share->server_names_lengths,
    &share->server_names_length,
    &share->server_names_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_table_names,
    &share->tgt_table_names_lengths,
    &share->tgt_table_names_length,
    &share->tgt_table_names_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_dbs,
    &share->tgt_dbs_lengths,
    &share->tgt_dbs_length,
    &share->tgt_dbs_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_hosts,
    &share->tgt_hosts_lengths,
    &share->tgt_hosts_length,
    &share->tgt_hosts_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_usernames,
    &share->tgt_usernames_lengths,
    &share->tgt_usernames_length,
    &share->tgt_usernames_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_passwords,
    &share->tgt_passwords_lengths,
    &share->tgt_passwords_length,
    &share->tgt_passwords_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_sockets,
    &share->tgt_sockets_lengths,
    &share->tgt_sockets_length,
    &share->tgt_sockets_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_wrappers,
    &share->tgt_wrappers_lengths,
    &share->tgt_wrappers_length,
    &share->tgt_wrappers_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_ssl_cas,
    &share->tgt_ssl_cas_lengths,
    &share->tgt_ssl_cas_length,
    &share->tgt_ssl_cas_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_ssl_capaths,
    &share->tgt_ssl_capaths_lengths,
    &share->tgt_ssl_capaths_length,
    &share->tgt_ssl_capaths_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_ssl_certs,
    &share->tgt_ssl_certs_lengths,
    &share->tgt_ssl_certs_length,
    &share->tgt_ssl_certs_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_ssl_ciphers,
    &share->tgt_ssl_ciphers_lengths,
    &share->tgt_ssl_ciphers_length,
    &share->tgt_ssl_ciphers_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_ssl_keys,
    &share->tgt_ssl_keys_lengths,
    &share->tgt_ssl_keys_length,
    &share->tgt_ssl_keys_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_default_files,
    &share->tgt_default_files_lengths,
    &share->tgt_default_files_length,
    &share->tgt_default_files_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_string_list(
    &share->tgt_default_groups,
    &share->tgt_default_groups_lengths,
    &share->tgt_default_groups_length,
    &share->tgt_default_groups_charlen,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_long_list(
    &share->tgt_ports,
    &share->tgt_ports_length,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_long_list(
    &share->tgt_ssl_vscs,
    &share->tgt_ssl_vscs_length,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_long_list(
    &share->link_statuses,
    &share->link_statuses_length,
    share->link_count)))
    goto error;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if ((error_num = spider_increase_long_list(
    &share->monitoring_bg_kind,
    &share->monitoring_bg_kind_length,
    share->link_count)))
    goto error;
#endif
  if ((error_num = spider_increase_long_list(
    &share->monitoring_kind,
    &share->monitoring_kind_length,
    share->link_count)))
    goto error;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if ((error_num = spider_increase_longlong_list(
    &share->monitoring_bg_interval,
    &share->monitoring_bg_interval_length,
    share->link_count)))
    goto error;
#endif
  if ((error_num = spider_increase_longlong_list(
    &share->monitoring_limit,
    &share->monitoring_limit_length,
    share->link_count)))
    goto error;
  if ((error_num = spider_increase_longlong_list(
    &share->monitoring_sid,
    &share->monitoring_sid_length,
    share->link_count)))
    goto error;

  /* copy for tables start */
  share_alter = &share->alter_table;
  share_alter->link_count = share->link_count;
  if (!(share_alter->tmp_server_names = (char **)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &share_alter->tmp_server_names,
      sizeof(char *) * 15 * share->link_count,
      &share_alter->tmp_server_names_lengths,
      sizeof(uint *) * 15 * share->link_count,
      &share_alter->tmp_tgt_ports,
      sizeof(long) * share->link_count,
      &share_alter->tmp_tgt_ssl_vscs,
      sizeof(long) * share->link_count,
      &share_alter->tmp_link_statuses,
      sizeof(long) * share->link_count,
      NullS))
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error;
  }


  memcpy(share_alter->tmp_server_names, share->server_names,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_table_names =
    share_alter->tmp_server_names + share->link_count;
  memcpy(share_alter->tmp_tgt_table_names, share->tgt_table_names,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_dbs =
    share_alter->tmp_tgt_table_names + share->link_count;
  memcpy(share_alter->tmp_tgt_dbs, share->tgt_dbs,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_hosts =
    share_alter->tmp_tgt_dbs + share->link_count;
  memcpy(share_alter->tmp_tgt_hosts, share->tgt_hosts,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_usernames =
    share_alter->tmp_tgt_hosts + share->link_count;
  memcpy(share_alter->tmp_tgt_usernames, share->tgt_usernames,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_passwords =
    share_alter->tmp_tgt_usernames + share->link_count;
  memcpy(share_alter->tmp_tgt_passwords, share->tgt_passwords,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_sockets =
    share_alter->tmp_tgt_passwords + share->link_count;
  memcpy(share_alter->tmp_tgt_sockets, share->tgt_sockets,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_wrappers =
    share_alter->tmp_tgt_sockets + share->link_count;
  memcpy(share_alter->tmp_tgt_wrappers, share->tgt_wrappers,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_ssl_cas =
    share_alter->tmp_tgt_wrappers + share->link_count;
  memcpy(share_alter->tmp_tgt_ssl_cas, share->tgt_ssl_cas,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_ssl_capaths =
    share_alter->tmp_tgt_ssl_cas + share->link_count;
  memcpy(share_alter->tmp_tgt_ssl_capaths, share->tgt_ssl_capaths,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_ssl_certs =
    share_alter->tmp_tgt_ssl_capaths + share->link_count;
  memcpy(share_alter->tmp_tgt_ssl_certs, share->tgt_ssl_certs,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_ssl_ciphers =
    share_alter->tmp_tgt_ssl_certs + share->link_count;
  memcpy(share_alter->tmp_tgt_ssl_ciphers, share->tgt_ssl_ciphers,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_ssl_keys =
    share_alter->tmp_tgt_ssl_ciphers + share->link_count;
  memcpy(share_alter->tmp_tgt_ssl_keys, share->tgt_ssl_keys,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_default_files =
    share_alter->tmp_tgt_ssl_keys + share->link_count;
  memcpy(share_alter->tmp_tgt_default_files, share->tgt_default_files,
    sizeof(char *) * share->link_count);
  share_alter->tmp_tgt_default_groups =
    share_alter->tmp_tgt_default_files + share->link_count;
  memcpy(share_alter->tmp_tgt_default_groups, share->tgt_default_groups,
    sizeof(char *) * share->link_count);

  memcpy(share_alter->tmp_tgt_ports, share->tgt_ports,
    sizeof(long) * share->link_count);
  memcpy(share_alter->tmp_tgt_ssl_vscs, share->tgt_ssl_vscs,
    sizeof(long) * share->link_count);
  memcpy(share_alter->tmp_link_statuses, share->link_statuses,
    sizeof(long) * share->link_count);

  memcpy(share_alter->tmp_server_names_lengths,
    share->server_names_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_table_names_lengths =
    share_alter->tmp_server_names_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_table_names_lengths,
    share->tgt_table_names_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_dbs_lengths =
    share_alter->tmp_tgt_table_names_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_dbs_lengths, share->tgt_dbs_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_hosts_lengths =
    share_alter->tmp_tgt_dbs_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_hosts_lengths, share->tgt_hosts_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_usernames_lengths =
    share_alter->tmp_tgt_hosts_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_usernames_lengths,
    share->tgt_usernames_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_passwords_lengths =
    share_alter->tmp_tgt_usernames_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_passwords_lengths,
    share->tgt_passwords_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_sockets_lengths =
    share_alter->tmp_tgt_passwords_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_sockets_lengths, share->tgt_sockets_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_wrappers_lengths =
    share_alter->tmp_tgt_sockets_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_wrappers_lengths,
    share->tgt_wrappers_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_ssl_cas_lengths =
    share_alter->tmp_tgt_wrappers_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_ssl_cas_lengths,
    share->tgt_ssl_cas_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_ssl_capaths_lengths =
    share_alter->tmp_tgt_ssl_cas_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_ssl_capaths_lengths,
    share->tgt_ssl_capaths_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_ssl_certs_lengths =
    share_alter->tmp_tgt_ssl_capaths_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_ssl_certs_lengths,
    share->tgt_ssl_certs_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_ssl_ciphers_lengths =
    share_alter->tmp_tgt_ssl_certs_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_ssl_ciphers_lengths,
    share->tgt_ssl_ciphers_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_ssl_keys_lengths =
    share_alter->tmp_tgt_ssl_ciphers_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_ssl_keys_lengths,
    share->tgt_ssl_keys_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_default_files_lengths =
    share_alter->tmp_tgt_ssl_keys_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_default_files_lengths,
    share->tgt_default_files_lengths,
    sizeof(uint) * share->link_count);
  share_alter->tmp_tgt_default_groups_lengths =
    share_alter->tmp_tgt_default_files_lengths + share->link_count;
  memcpy(share_alter->tmp_tgt_default_groups_lengths,
    share->tgt_default_groups_lengths,
    sizeof(uint) * share->link_count);

  share_alter->tmp_server_names_charlen = share->server_names_charlen;
  share_alter->tmp_tgt_table_names_charlen = share->tgt_table_names_charlen;
  share_alter->tmp_tgt_dbs_charlen = share->tgt_dbs_charlen;
  share_alter->tmp_tgt_hosts_charlen = share->tgt_hosts_charlen;
  share_alter->tmp_tgt_usernames_charlen = share->tgt_usernames_charlen;
  share_alter->tmp_tgt_passwords_charlen = share->tgt_passwords_charlen;
  share_alter->tmp_tgt_sockets_charlen = share->tgt_sockets_charlen;
  share_alter->tmp_tgt_wrappers_charlen = share->tgt_wrappers_charlen;
  share_alter->tmp_tgt_ssl_cas_charlen = share->tgt_ssl_cas_charlen;
  share_alter->tmp_tgt_ssl_capaths_charlen = share->tgt_ssl_capaths_charlen;
  share_alter->tmp_tgt_ssl_certs_charlen = share->tgt_ssl_certs_charlen;
  share_alter->tmp_tgt_ssl_ciphers_charlen = share->tgt_ssl_ciphers_charlen;
  share_alter->tmp_tgt_ssl_keys_charlen = share->tgt_ssl_keys_charlen;
  share_alter->tmp_tgt_default_files_charlen =
    share->tgt_default_files_charlen;
  share_alter->tmp_tgt_default_groups_charlen =
    share->tgt_default_groups_charlen;

  share_alter->tmp_server_names_length = share->server_names_length;
  share_alter->tmp_tgt_table_names_length = share->tgt_table_names_length;
  share_alter->tmp_tgt_dbs_length = share->tgt_dbs_length;
  share_alter->tmp_tgt_hosts_length = share->tgt_hosts_length;
  share_alter->tmp_tgt_usernames_length = share->tgt_usernames_length;
  share_alter->tmp_tgt_passwords_length = share->tgt_passwords_length;
  share_alter->tmp_tgt_sockets_length = share->tgt_sockets_length;
  share_alter->tmp_tgt_wrappers_length = share->tgt_wrappers_length;
  share_alter->tmp_tgt_ssl_cas_length = share->tgt_ssl_cas_length;
  share_alter->tmp_tgt_ssl_capaths_length = share->tgt_ssl_capaths_length;
  share_alter->tmp_tgt_ssl_certs_length = share->tgt_ssl_certs_length;
  share_alter->tmp_tgt_ssl_ciphers_length = share->tgt_ssl_ciphers_length;
  share_alter->tmp_tgt_ssl_keys_length = share->tgt_ssl_keys_length;
  share_alter->tmp_tgt_default_files_length = share->tgt_default_files_length;
  share_alter->tmp_tgt_default_groups_length =
    share->tgt_default_groups_length;
  share_alter->tmp_tgt_ports_length = share->tgt_ports_length;
  share_alter->tmp_tgt_ssl_vscs_length = share->tgt_ssl_vscs_length;
  share_alter->tmp_link_statuses_length = share->link_statuses_length;
  /* copy for tables end */

  if ((error_num = spider_set_connect_info_default(
    share,
#ifdef WITH_PARTITION_STORAGE_ENGINE
    part_elem,
    sub_elem,
#endif
    table
  )))
    goto error;

  for (roop_count = 0; roop_count < share->link_count; roop_count++)
  {
    if (strcmp(share->tgt_wrappers[roop_count], SPIDER_DB_WRAPPER_MYSQL))
    {
      DBUG_PRINT("info",("spider err tgt_wrappers[%d]=%s", roop_count,
        share->tgt_wrappers[roop_count]));
      error_num = ER_SPIDER_INVALID_CONNECT_INFO_NUM;
      my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_STR,
        MYF(0), share->tgt_wrappers[roop_count]);
      goto error;
    }

    if (create_table)
    {
      DBUG_PRINT("info",
        ("spider server_names_lengths[%d] = %ld",
         share->server_names_lengths[roop_count]));
      if (share->server_names_lengths[roop_count] > SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->server_names[roop_count], "server");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_table_names_lengths[%d] = %ld", roop_count,
        share->tgt_table_names_lengths[roop_count]));
      if (share->tgt_table_names_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_table_names[roop_count], "table");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_dbs_lengths[%d] = %ld", roop_count,
        share->tgt_dbs_lengths[roop_count]));
      if (share->tgt_dbs_lengths[roop_count] > SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_dbs[roop_count], "database");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_hosts_lengths[%d] = %ld", roop_count,
        share->tgt_hosts_lengths[roop_count]));
      if (share->tgt_hosts_lengths[roop_count] > SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_hosts[roop_count], "host");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_usernames_lengths[%d] = %ld", roop_count,
        share->tgt_usernames_lengths[roop_count]));
      if (share->tgt_usernames_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_usernames[roop_count], "user");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_passwords_lengths[%d] = %ld", roop_count,
        share->tgt_passwords_lengths[roop_count]));
      if (share->tgt_passwords_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_passwords[roop_count], "password");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_sockets_lengths[%d] = %ld", roop_count,
        share->tgt_sockets_lengths[roop_count]));
      if (share->tgt_sockets_lengths[roop_count] > SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_sockets[roop_count], "socket");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_wrappers_lengths[%d] = %ld", roop_count,
        share->tgt_wrappers_lengths[roop_count]));
      if (share->tgt_wrappers_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_wrappers[roop_count], "wrapper");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_ssl_cas_lengths[%d] = %ld", roop_count,
        share->tgt_ssl_cas_lengths[roop_count]));
      if (share->tgt_ssl_cas_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_ssl_cas[roop_count], "ssl_ca");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_ssl_capaths_lengths[%d] = %ld", roop_count,
        share->tgt_ssl_capaths_lengths[roop_count]));
      if (share->tgt_ssl_capaths_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_ssl_capaths[roop_count], "ssl_capath");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_ssl_certs_lengths[%d] = %ld", roop_count,
        share->tgt_ssl_certs_lengths[roop_count]));
      if (share->tgt_ssl_certs_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_ssl_certs[roop_count], "ssl_cert");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_ssl_ciphers_lengths[%d] = %ld", roop_count,
        share->tgt_ssl_ciphers_lengths[roop_count]));
      if (share->tgt_ssl_ciphers_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_ssl_ciphers[roop_count], "ssl_cipher");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_ssl_keys_lengths[%d] = %ld", roop_count,
        share->tgt_ssl_keys_lengths[roop_count]));
      if (share->tgt_ssl_keys_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_ssl_keys[roop_count], "ssl_key");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_default_files_lengths[%d] = %ld", roop_count,
        share->tgt_default_files_lengths[roop_count]));
      if (share->tgt_default_files_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_default_files[roop_count], "default_file");
        goto error;
      }

      DBUG_PRINT("info",
        ("spider tgt_default_groups_lengths[%d] = %ld", roop_count,
        share->tgt_default_groups_lengths[roop_count]));
      if (share->tgt_default_groups_lengths[roop_count] >
        SPIDER_CONNECT_INFO_MAX_LEN)
      {
        error_num = ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM;
        my_printf_error(error_num, ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR,
          MYF(0), share->tgt_default_groups[roop_count], "default_group");
        goto error;
      }
    }
  }

  if (connect_string)
    my_free(connect_string, MYF(0));
  DBUG_RETURN(0);

error:
  if (connect_string)
    my_free(connect_string, MYF(0));
error_alloc_conn_string:
  DBUG_RETURN(error_num);
}

int spider_set_connect_info_default(
  SPIDER_SHARE *share,
#ifdef WITH_PARTITION_STORAGE_ENGINE
  partition_element *part_elem,
  partition_element *sub_elem,
#endif
  TABLE *table
) {
  int error_num, roop_count;
  DBUG_ENTER("spider_set_connect_info_default");
  for (roop_count = 0; roop_count < share->link_count; roop_count++)
  {
    if (share->server_names[roop_count])
    {
      if ((error_num = spider_get_server(share, roop_count)))
        DBUG_RETURN(error_num);
    }

    if (!share->tgt_wrappers[roop_count])
    {
      DBUG_PRINT("info",("spider create default tgt_wrappers"));
      share->tgt_wrappers_lengths[roop_count] = SPIDER_DB_WRAPPER_LEN;
      if (
        !(share->tgt_wrappers[roop_count] = spider_create_string(
          SPIDER_DB_WRAPPER_STR,
          share->tgt_wrappers_lengths[roop_count]))
      ) {
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    }

    if (!share->tgt_hosts[roop_count])
    {
      DBUG_PRINT("info",("spider create default tgt_hosts"));
      share->tgt_hosts_lengths[roop_count] = strlen(my_localhost);
      if (
        !(share->tgt_hosts[roop_count] = spider_create_string(
          my_localhost,
          share->tgt_hosts_lengths[roop_count]))
      ) {
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    }

    if (!share->tgt_dbs[roop_count] && table)
    {
      DBUG_PRINT("info",("spider create default tgt_dbs"));
      share->tgt_dbs_lengths[roop_count] = table->s->db.length;
      if (
        !(share->tgt_dbs[roop_count] = spider_create_string(
          table->s->db.str,
          table->s->db.length))
      ) {
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    }

    if (!share->tgt_table_names[roop_count] && table)
    {
      DBUG_PRINT("info",("spider create default tgt_table_names"));
      share->tgt_table_names_lengths[roop_count] = share->table_name_length;
      if (
        !(share->tgt_table_names[roop_count] = spider_create_table_name_string(
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
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    }

    if (
      !share->tgt_default_files[roop_count] &&
      share->tgt_default_groups[roop_count] &&
      (my_defaults_file || my_defaults_extra_file)
    ) {
      DBUG_PRINT("info",("spider create default tgt_default_files"));
      if (my_defaults_extra_file)
      {
        share->tgt_default_files_lengths[roop_count] =
          strlen(my_defaults_extra_file);
        if (
          !(share->tgt_default_files[roop_count] = spider_create_string(
            my_defaults_extra_file,
            share->tgt_default_files_lengths[roop_count]))
        ) {
          my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
      } else {
        share->tgt_default_files_lengths[roop_count] =
          strlen(my_defaults_file);
        if (
          !(share->tgt_default_files[roop_count] = spider_create_string(
            my_defaults_file,
            share->tgt_default_files_lengths[roop_count]))
        ) {
          my_error(ER_OUT_OF_RESOURCES, MYF(0), HA_ERR_OUT_OF_MEM);
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        }
      }
    }

    if (share->tgt_ports[roop_count] == -1)
    {
      share->tgt_ports[roop_count] = MYSQL_PORT;
    } else if (share->tgt_ports[roop_count] < 0)
    {
      share->tgt_ports[roop_count] = 0;
    } else if (share->tgt_ports[roop_count] > 65535)
    {
      share->tgt_ports[roop_count] = 65535;
    }

    if (share->tgt_ssl_vscs[roop_count] == -1)
      share->tgt_ssl_vscs[roop_count] = 0;

    if (
      !share->tgt_sockets[roop_count] &&
      !strcmp(share->tgt_hosts[roop_count], my_localhost)
    ) {
      DBUG_PRINT("info",("spider create default tgt_sockets"));
      share->tgt_sockets_lengths[roop_count] =
        strlen((char *) MYSQL_UNIX_ADDR);
      if (
        !(share->tgt_sockets[roop_count] = spider_create_string(
          (char *) MYSQL_UNIX_ADDR,
          share->tgt_sockets_lengths[roop_count]))
      ) {
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    }

    if (share->link_statuses[roop_count] == -1)
      share->link_statuses[roop_count] = SPIDER_LINK_STATUS_NO_CHANGE;

#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (share->monitoring_bg_kind[roop_count] == -1)
      share->monitoring_bg_kind[roop_count] = 0;
#endif
    if (share->monitoring_kind[roop_count] == -1)
      share->monitoring_kind[roop_count] = 0;
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (share->monitoring_bg_interval[roop_count] == -1)
      share->monitoring_bg_interval[roop_count] = 10000000;
#endif
    if (share->monitoring_limit[roop_count] == -1)
      share->monitoring_limit[roop_count] = 1;
    if (share->monitoring_sid[roop_count] == -1)
      share->monitoring_sid[roop_count] = current_thd->server_id;
  }

#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (share->sts_bg_mode == -1)
    share->sts_bg_mode = 1;
#endif
  if (share->sts_interval == -1)
    share->sts_interval = 10;
  if (share->sts_mode == -1)
    share->sts_mode = 1;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (share->sts_sync == -1)
    share->sts_sync = 0;
#endif
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (share->crd_bg_mode == -1)
    share->crd_bg_mode = 1;
#endif
  if (share->crd_interval == -1)
    share->crd_interval = 51;
  if (share->crd_mode == -1)
    share->crd_mode = 1;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (share->crd_sync == -1)
    share->crd_sync = 0;
#endif
  if (share->crd_type == -1)
    share->crd_type = 2;
  if (share->crd_weight == -1)
    share->crd_weight = 2;
  if (share->internal_offset == -1)
    share->internal_offset = 0;
  if (share->internal_limit == -1)
    share->internal_limit = 9223372036854775807LL;
  if (share->split_read == -1)
    share->split_read = 9223372036854775807LL;
  if (share->semi_split_read == -1)
    share->semi_split_read = 0;
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
  if (share->semi_table_lock_conn == -1)
    share->semi_table_lock_conn = 1;
  if (share->selupd_lock_mode == -1)
    share->selupd_lock_mode = 1;
  if (share->query_cache == -1)
    share->query_cache = 0;
  if (share->internal_delayed == -1)
    share->internal_delayed = 0;
  if (share->bulk_size == -1)
    share->bulk_size = 16000;
  if (share->bulk_update_mode == -1)
    share->bulk_update_mode = 0;
  if (share->bulk_update_size == -1)
    share->bulk_update_size = 16000;
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
  if (share->net_timeout == -1)
    share->net_timeout = 600;
  if (share->quick_mode == -1)
    share->quick_mode = 0;
  if (share->quick_page_size == -1)
    share->quick_page_size = 100;
  if (share->low_mem_read == -1)
    share->low_mem_read = 1;
  if (share->table_count_mode == -1)
    share->table_count_mode = 0;
  if (share->select_column_mode == -1)
    share->select_column_mode = 1;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (share->bgs_mode == -1)
    share->bgs_mode = 0;
  if (share->bgs_first_read == -1)
    share->bgs_first_read = 2;
  if (share->bgs_second_read == -1)
    share->bgs_second_read = 100;
#endif
  if (share->auto_increment_mode == -1)
    share->auto_increment_mode = 0;
  if (share->use_table_charset == -1)
    share->use_table_charset = 1;
  if (share->use_pushdown_udf == -1)
    share->use_pushdown_udf = 1;
  if (share->direct_dup_insert == -1)
    share->direct_dup_insert = 0;
  DBUG_RETURN(0);
}

int spider_create_conn_keys(
  SPIDER_SHARE *share
) {
  int roop_count;
  char *tmp_name, port_str[6];
  uint conn_keys_lengths[share->link_count];
  DBUG_ENTER("spider_create_conn_keys");

  share->conn_keys_charlen = 0;
  for (roop_count = 0; roop_count < share->link_count; roop_count++)
  {
    /* tgt_db not use */
    conn_keys_lengths[roop_count]
      = 1
      + share->tgt_wrappers_lengths[roop_count] + 1
      + share->tgt_hosts_lengths[roop_count] + 1
      + 5 + 1
      + share->tgt_sockets_lengths[roop_count] + 1
      + share->tgt_usernames_lengths[roop_count] + 1
      + share->tgt_passwords_lengths[roop_count] + 1
      + share->tgt_ssl_cas_lengths[roop_count] + 1
      + share->tgt_ssl_capaths_lengths[roop_count] + 1
      + share->tgt_ssl_certs_lengths[roop_count] + 1
      + share->tgt_ssl_ciphers_lengths[roop_count] + 1
      + share->tgt_ssl_keys_lengths[roop_count] + 1
      + 1 + 1
      + share->tgt_default_files_lengths[roop_count] + 1
      + share->tgt_default_groups_lengths[roop_count];
    share->conn_keys_charlen += conn_keys_lengths[roop_count] + 1;
  }
  if (!(share->conn_keys = (char **)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &share->conn_keys, sizeof(char *) * share->link_count,
      &share->conn_keys_lengths, sizeof(uint) * share->link_count,
      &tmp_name, sizeof(char) * share->conn_keys_charlen,
      NullS))
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  share->conn_keys_length = share->link_count;
  memcpy(share->conn_keys_lengths, conn_keys_lengths,
    sizeof(uint) * share->link_count);

  for (roop_count = 0; roop_count < share->link_count; roop_count++)
  {
    share->conn_keys[roop_count] = tmp_name;
    *tmp_name = '0';
    DBUG_PRINT("info",("spider tgt_wrappers[%d]=%s", roop_count,
      share->tgt_wrappers[roop_count]));
    tmp_name = strmov(tmp_name + 1, share->tgt_wrappers[roop_count]);
    DBUG_PRINT("info",("spider tgt_hosts[%d]=%s", roop_count,
      share->tgt_hosts[roop_count]));
    tmp_name = strmov(tmp_name + 1, share->tgt_hosts[roop_count]);
    my_sprintf(port_str, (port_str, "%05ld", share->tgt_ports[roop_count]));
    DBUG_PRINT("info",("spider port_str=%s", port_str));
    tmp_name = strmov(tmp_name + 1, port_str);
    if (share->tgt_sockets[roop_count])
    {
      DBUG_PRINT("info",("spider tgt_sockets[%d]=%s", roop_count,
        share->tgt_sockets[roop_count]));
      tmp_name = strmov(tmp_name + 1, share->tgt_sockets[roop_count]);
    } else
      tmp_name++;
    if (share->tgt_usernames[roop_count])
    {
      DBUG_PRINT("info",("spider tgt_usernames[%d]=%s", roop_count,
        share->tgt_usernames[roop_count]));
      tmp_name = strmov(tmp_name + 1, share->tgt_usernames[roop_count]);
    } else
      tmp_name++;
    if (share->tgt_passwords[roop_count])
    {
      DBUG_PRINT("info",("spider tgt_passwords[%d]=%s", roop_count,
        share->tgt_passwords[roop_count]));
      tmp_name = strmov(tmp_name + 1, share->tgt_passwords[roop_count]);
    } else
      tmp_name++;
    if (share->tgt_ssl_cas[roop_count])
    {
      DBUG_PRINT("info",("spider tgt_ssl_cas[%d]=%s", roop_count,
        share->tgt_ssl_cas[roop_count]));
      tmp_name = strmov(tmp_name + 1, share->tgt_ssl_cas[roop_count]);
    } else
      tmp_name++;
    if (share->tgt_ssl_capaths[roop_count])
    {
      DBUG_PRINT("info",("spider tgt_ssl_capaths[%d]=%s", roop_count,
        share->tgt_ssl_capaths[roop_count]));
      tmp_name = strmov(tmp_name + 1, share->tgt_ssl_capaths[roop_count]);
    } else
      tmp_name++;
    if (share->tgt_ssl_certs[roop_count])
    {
      DBUG_PRINT("info",("spider tgt_ssl_certs[%d]=%s", roop_count,
        share->tgt_ssl_certs[roop_count]));
      tmp_name = strmov(tmp_name + 1, share->tgt_ssl_certs[roop_count]);
    } else
      tmp_name++;
    if (share->tgt_ssl_ciphers[roop_count])
    {
      DBUG_PRINT("info",("spider tgt_ssl_ciphers[%d]=%s", roop_count,
        share->tgt_ssl_ciphers[roop_count]));
      tmp_name = strmov(tmp_name + 1, share->tgt_ssl_ciphers[roop_count]);
    } else
      tmp_name++;
    if (share->tgt_ssl_keys[roop_count])
    {
      DBUG_PRINT("info",("spider tgt_ssl_keys[%d]=%s", roop_count,
        share->tgt_ssl_keys[roop_count]));
      tmp_name = strmov(tmp_name + 1, share->tgt_ssl_keys[roop_count]);
    } else
      tmp_name++;
    tmp_name++;
    *tmp_name = '0' + share->tgt_ssl_vscs[roop_count];
    if (share->tgt_default_files[roop_count])
    {
      DBUG_PRINT("info",("spider tgt_default_files[%d]=%s", roop_count,
        share->tgt_default_files[roop_count]));
      tmp_name = strmov(tmp_name + 1, share->tgt_default_files[roop_count]);
    } else
      tmp_name++;
    if (share->tgt_default_groups[roop_count])
    {
      DBUG_PRINT("info",("spider tgt_default_groups[%d]=%s", roop_count,
        share->tgt_default_groups[roop_count]));
      tmp_name = strmov(tmp_name + 1, share->tgt_default_groups[roop_count]);
    } else
      tmp_name++;
    tmp_name++;
  }
  DBUG_RETURN(0);
}

SPIDER_SHARE *spider_get_share(
  const char *table_name,
  TABLE *table,
  THD *thd,
  ha_spider *spider,
  int *error_num
) {
  SPIDER_SHARE *share;
  TABLE_SHARE *table_share = table->s;
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  uint length;
  int use_table_charset;
  char *tmp_name;
  longlong *tmp_cardinality;
  uchar *tmp_cardinality_upd;
  int *tmp_key_select_pos;
  int bitmap_size;
  int roop_count;
  double sts_interval;
  int sts_mode;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  int sts_sync;
#endif
  double crd_interval;
  int crd_mode;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  int crd_sync;
#endif
  char first_byte;
  int semi_table_lock_conn;
  int search_link_idx;
  uint sql_command = thd_sql_command(thd);
  DBUG_ENTER("spider_get_share");

  length = (uint) strlen(table_name);
  pthread_mutex_lock(&spider_tbl_mutex);
  if (!(share = (SPIDER_SHARE*) hash_search(&spider_open_tables,
    (uchar*) table_name, length)))
  {
    bitmap_size = (table_share->fields + 7) / 8;
    DBUG_PRINT("info",("spider create new share"));
    if (!(share = (SPIDER_SHARE *)
      my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
        &share, sizeof(*share),
        &tmp_name, length + 1,
        &tmp_cardinality, sizeof(*tmp_cardinality) * table_share->fields,
        &tmp_cardinality_upd, sizeof(*tmp_cardinality_upd) * bitmap_size,
        &tmp_key_select_pos, sizeof(int) * table_share->keys,
        NullS))
    ) {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_alloc_share;
    }

    share->use_count = 0;
    share->table_name_length = length;
    share->table_name = tmp_name;
    strmov(share->table_name, table_name);
    share->cardinality = tmp_cardinality;
    share->cardinality_upd = tmp_cardinality_upd;
    share->key_select_pos = tmp_key_select_pos;
    share->bitmap_size = bitmap_size;
    share->table_share = table_share;

    if (table_share->keys > 0 &&
      !(share->key_hint = new String[table_share->keys])
    ) {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_hint_string;
    }
    DBUG_PRINT("info",("spider share->key_hint=%x", share->key_hint));

    if ((*error_num = spider_parse_connect_info(share, table, 0)))
      goto error_parse_connect_string;

    if (
      table_share->tmp_table == NO_TMP_TABLE &&
      sql_command != SQLCOM_DROP_TABLE &&
      sql_command != SQLCOM_ALTER_TABLE
    ) {
      MEM_ROOT mem_root;
      TABLE *table_tables = NULL;
      Open_tables_state open_tables_backup;
      init_alloc_root(&mem_root, 4096, 0);
      if (
        !(table_tables = spider_open_sys_table(
          thd, SPIDER_SYS_TABLES_TABLE_NAME_STR,
          SPIDER_SYS_TABLES_TABLE_NAME_LEN, FALSE, &open_tables_backup, FALSE,
          error_num))
      )
        goto error_open_sys_table;
      *error_num = spider_get_link_statuses(table_tables, share, &mem_root);
      spider_close_sys_table(thd, table_tables,
        &open_tables_backup, FALSE);
      table_tables = NULL;
      free_root(&mem_root, MYF(0));
      if (*error_num)
        goto error_get_link_statuses;
    }

    use_table_charset = spider_use_table_charset == -1 ?
      share->use_table_charset : spider_use_table_charset;
    if (use_table_charset)
      share->access_charset = table_share->table_charset;
    else
      share->access_charset = system_charset_info;

    share->have_recovery_link = spider_conn_check_recovery_link(share);

    if ((*error_num = spider_create_conn_keys(share)))
      goto error_create_conn_keys;

    if (
      !(share->table_select = new String[1]) ||
      (table_share->keys > 0 &&
        !(share->key_select = new String[table_share->keys])
      ) ||
      (*error_num = spider_db_create_table_names_str(share)) ||
      (*error_num = spider_db_create_column_name_str(share, table_share)) ||
      (*error_num = spider_db_convert_key_hint_str(share, table_share)) ||
      (*error_num = spider_db_append_show_table_status(share)) ||
      (*error_num = spider_db_append_show_index(share)) ||
      (*error_num = spider_db_append_set_names(share))
    ) {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_string;
    }

    spider->share = share;
    if (
      (*error_num = spider_db_append_table_select(
        share->table_select, table, spider))
    )
      goto error_init_string;
    share->table_select_pos = spider->result_list.table_name_pos;

    for (roop_count = 0; roop_count < table_share->keys; roop_count++)
    {
      if ((*error_num = spider_db_append_key_select(
        &share->key_select[roop_count],
        &table_share->key_info[roop_count], spider)))
        goto error_init_string;
      share->key_select_pos[roop_count] = spider->result_list.table_name_pos;
    }

    if (share->table_count_mode)
      share->additional_table_flags |= HA_STATS_RECORDS_IS_EXACT;

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

    if (pthread_mutex_init(&share->auto_increment_mutex, MY_MUTEX_INIT_FAST))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_auto_increment_mutex;
    }

    thr_lock_init(&share->lock);

#ifdef WITH_PARTITION_STORAGE_ENGINE
    if (!(share->partition_share =
      spider_get_pt_share(table, share, error_num)))
      goto error_get_pt_share;
#endif

    if (my_hash_insert(&spider_open_tables, (uchar*) share))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_hash_insert;
    }
    share->use_count++;
    pthread_mutex_unlock(&spider_tbl_mutex);

    semi_table_lock_conn = THDVAR(thd, semi_table_lock_connection) == -1 ?
      share->semi_table_lock_conn : THDVAR(thd, semi_table_lock_connection);
    if (semi_table_lock_conn)
      first_byte = '0' +
        (share->semi_table_lock & THDVAR(thd, semi_table_lock));
    else
      first_byte = '0';

    if (!(spider->trx = spider_get_trx(thd, error_num)))
    {
      share->init_error = TRUE;
      share->init_error_time = (time_t) time((time_t*) 0);
      share->init = TRUE;
      spider_free_share(share);
      goto error_but_no_delete;
    }

#ifndef WITHOUT_SPIDER_BG_SEARCH
    if ((*error_num = spider_create_mon_threads(spider->trx, share)))
    {
      share->init_error = TRUE;
      share->init_error_time = (time_t) time((time_t*) 0);
      share->init = TRUE;
      spider_free_share(share);
      goto error_but_no_delete;
    }
#endif

    if (!(spider->conn_keys = (char **)
      my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
        &spider->conn_keys, sizeof(char *) * share->link_count,
        &tmp_name, sizeof(char) * share->conn_keys_charlen,
        &spider->conns, sizeof(SPIDER_CONN *) * share->link_count,
        &spider->need_mons, sizeof(int) * share->link_count,
        &spider->quick_targets, sizeof(void *) * share->link_count,
        &result_list->upd_tmp_tbls, sizeof(TABLE *) * share->link_count,
        &result_list->upd_tmp_tbl_prms,
          sizeof(TMP_TABLE_PARAM) * share->link_count,
        NullS))
    ) {
      share->init_error = TRUE;
      share->init_error_time = (time_t) time((time_t*) 0);
      share->init = TRUE;
      spider_free_share(share);
      goto error_but_no_delete;
    }
    result_list->upd_tmp_tbl = NULL;
    result_list->upd_tmp_tbl_prm.init();
    result_list->upd_tmp_tbl_prm.field_count = 1;
    memcpy(tmp_name, share->conn_keys[0], share->conn_keys_charlen);

    for (roop_count = 0; roop_count < share->link_count; roop_count++)
    {
      spider->conn_keys[roop_count] = tmp_name;
      *tmp_name = first_byte;
      tmp_name += share->conn_keys_lengths[roop_count] + 1;
      result_list->upd_tmp_tbl_prms[roop_count].init();
      result_list->upd_tmp_tbl_prms[roop_count].field_count = 1;
    }

    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      if (
        !(spider->conns[roop_count] =
          spider_get_conn(share, roop_count, spider->conn_keys[roop_count],
            spider->trx, spider, FALSE, TRUE, error_num))
      ) {
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          *error_num = spider_ping_table_mon_from_table(
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
              FALSE
            );
        }
        share->init_error = TRUE;
        share->init_error_time = (time_t) time((time_t*) 0);
        share->init = TRUE;
        spider_free_share(share);
        goto error_but_no_delete;
      }
    }
    search_link_idx = spider_conn_first_link_idx(thd,
      share->link_statuses, share->link_count, SPIDER_LINK_STATUS_OK);
    if (search_link_idx == -1)
    {
      char db[table_share->db.length + 1],
        table_name[table_share->table_name.length + 1];
      memcpy(db, table_share->db.str, table_share->db.length);
      db[table_share->db.length] = '\0';
      memcpy(table_name, table_share->table_name.str,
        table_share->table_name.length);
      table_name[table_share->table_name.length] = '\0';
      my_printf_error(ER_SPIDER_ALL_LINKS_FAILED_NUM,
        ER_SPIDER_ALL_LINKS_FAILED_STR, MYF(0), db, table_name);
      *error_num = ER_SPIDER_ALL_LINKS_FAILED_NUM;
      share->init_error = TRUE;
      share->init_error_time = (time_t) time((time_t*) 0);
      share->init = TRUE;
      spider_free_share(share);
      goto error_but_no_delete;
    }
    spider->search_link_idx = search_link_idx;

    if (!THDVAR(thd, same_server_link))
    {
      SPIDER_INIT_ERROR_TABLE *spider_init_error_table;
      sts_interval = THDVAR(thd, sts_interval) == -1 ?
        share->sts_interval : THDVAR(thd, sts_interval);
      sts_mode = THDVAR(thd, sts_mode) <= 0 ?
        share->sts_mode : THDVAR(thd, sts_mode);
#ifdef WITH_PARTITION_STORAGE_ENGINE
      sts_sync = THDVAR(thd, sts_sync) == -1 ?
        share->sts_sync : THDVAR(thd, sts_sync);
#endif
      crd_interval = THDVAR(thd, crd_interval) == -1 ?
        share->crd_interval : THDVAR(thd, crd_interval);
      crd_mode = THDVAR(thd, crd_mode) <= 0 ?
        share->crd_mode : THDVAR(thd, crd_mode);
      if (crd_mode == 3)
        crd_mode = 1;
#ifdef WITH_PARTITION_STORAGE_ENGINE
      crd_sync = THDVAR(thd, crd_sync) == -1 ?
        share->crd_sync : THDVAR(thd, crd_sync);
#endif
      time_t tmp_time = (time_t) time((time_t*) 0);
      if ((spider_init_error_table =
        spider_get_init_error_table(share, FALSE)))
      {
        DBUG_PRINT("info",("spider diff1=%f",
          difftime(tmp_time, spider_init_error_table->init_error_time)));
        if (difftime(tmp_time,
          spider_init_error_table->init_error_time) <
          spider_table_init_error_interval)
        {
          *error_num = spider_init_error_table->init_error;
          if (spider_init_error_table->init_error_with_message)
            my_message(spider_init_error_table->init_error,
              spider_init_error_table->init_error_msg, MYF(0));
          share->init_error = TRUE;
          share->init = TRUE;
          spider_free_share(share);
          goto error_but_no_delete;
        }
      }

      if (
        (*error_num = spider_get_sts(share, spider->search_link_idx, tmp_time,
          spider, sts_interval, sts_mode,
#ifdef WITH_PARTITION_STORAGE_ENGINE
          sts_sync,
#endif
          1)) ||
        (*error_num = spider_get_crd(share, spider->search_link_idx, tmp_time,
          spider, table, crd_interval, crd_mode,
#ifdef WITH_PARTITION_STORAGE_ENGINE
          crd_sync,
#endif
          1))
      ) {
        if (
          share->monitoring_kind[spider->search_link_idx] &&
          spider->need_mons[spider->search_link_idx]
        ) {
          *error_num = spider_ping_table_mon_from_table(
              spider->trx,
              spider->trx->thd,
              share,
              share->monitoring_sid[spider->search_link_idx],
              share->table_name,
              share->table_name_length,
              spider->search_link_idx,
              "",
              0,
              share->monitoring_kind[spider->search_link_idx],
              share->monitoring_limit[spider->search_link_idx],
              FALSE
            );
        }
        if (
          spider_init_error_table ||
          (spider_init_error_table =
            spider_get_init_error_table(share, TRUE))
        ) {
          spider_init_error_table->init_error = *error_num;
          if ((spider_init_error_table->init_error_with_message =
            thd->main_da.is_error()))
            strmov(spider_init_error_table->init_error_msg,
              thd->main_da.message());
          spider_init_error_table->init_error_time =
            (time_t) time((time_t*) 0);
        }
        share->init_error = TRUE;
        share->init = TRUE;
        spider_free_share(share);
        goto error_but_no_delete;
      }
    }

    share->init = TRUE;
  } else {
    share->use_count++;
    pthread_mutex_unlock(&spider_tbl_mutex);

    while (!share->init)
    {
      my_sleep(10);
    }

    semi_table_lock_conn = THDVAR(thd, semi_table_lock_connection) == -1 ?
      share->semi_table_lock_conn : THDVAR(thd, semi_table_lock_connection);
    if (semi_table_lock_conn)
      first_byte = '0' +
        (share->semi_table_lock & THDVAR(thd, semi_table_lock));
    else
      first_byte = '0';

    spider->share = share;
    if (!(spider->trx = spider_get_trx(thd, error_num)))
    {
      spider_free_share(share);
      goto error_but_no_delete;
    }

#ifndef WITHOUT_SPIDER_BG_SEARCH
    if ((*error_num = spider_create_mon_threads(spider->trx, share)))
    {
      spider_free_share(share);
      goto error_but_no_delete;
    }
#endif

    if (!(spider->conn_keys = (char **)
      my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
        &spider->conn_keys, sizeof(char *) * share->link_count,
        &tmp_name, sizeof(char) * share->conn_keys_charlen,
        &spider->conns, sizeof(SPIDER_CONN *) * share->link_count,
        &spider->need_mons, sizeof(int) * share->link_count,
        &spider->quick_targets, sizeof(void *) * share->link_count,
        &result_list->upd_tmp_tbls, sizeof(TABLE *) * share->link_count,
        &result_list->upd_tmp_tbl_prms,
          sizeof(TMP_TABLE_PARAM) * share->link_count,
        NullS))
    ) {
      spider_free_share(share);
      goto error_but_no_delete;
    }
    result_list->upd_tmp_tbl = NULL;
    result_list->upd_tmp_tbl_prm.init();
    result_list->upd_tmp_tbl_prm.field_count = 1;
    memcpy(tmp_name, share->conn_keys[0], share->conn_keys_charlen);

    for (roop_count = 0; roop_count < share->link_count; roop_count++)
    {
      spider->conn_keys[roop_count] = tmp_name;
      *tmp_name = first_byte;
      tmp_name += share->conn_keys_lengths[roop_count] + 1;
      result_list->upd_tmp_tbl_prms[roop_count].init();
      result_list->upd_tmp_tbl_prms[roop_count].field_count = 1;
    }

    for (
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_count < share->link_count;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
      if (
        !(spider->conns[roop_count] =
          spider_get_conn(share, roop_count, spider->conn_keys[roop_count],
            spider->trx, spider, FALSE, TRUE, error_num))
      ) {
        if (
          share->monitoring_kind[roop_count] &&
          spider->need_mons[roop_count]
        ) {
          *error_num = spider_ping_table_mon_from_table(
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
              FALSE
            );
        }
        spider_free_share(share);
        goto error_but_no_delete;
      }
    }
    search_link_idx = spider_conn_first_link_idx(thd,
      share->link_statuses, share->link_count, SPIDER_LINK_STATUS_OK);
    if (search_link_idx == -1)
    {
      char db[table_share->db.length + 1],
        table_name[table_share->table_name.length + 1];
      memcpy(db, table_share->db.str, table_share->db.length);
      db[table_share->db.length] = '\0';
      memcpy(table_name, table_share->table_name.str,
        table_share->table_name.length);
      table_name[table_share->table_name.length] = '\0';
      my_printf_error(ER_SPIDER_ALL_LINKS_FAILED_NUM,
        ER_SPIDER_ALL_LINKS_FAILED_STR, MYF(0), db, table_name);
      *error_num = ER_SPIDER_ALL_LINKS_FAILED_NUM;
      spider_free_share(share);
      goto error_but_no_delete;
    }
    spider->search_link_idx = search_link_idx;

    if (share->init_error)
    {
      pthread_mutex_lock(&share->sts_mutex);
      pthread_mutex_lock(&share->crd_mutex);
      if (share->init_error)
      {
        if (!THDVAR(thd, same_server_link))
        {
          SPIDER_INIT_ERROR_TABLE *spider_init_error_table;
          sts_interval = THDVAR(thd, sts_interval) == -1 ?
            share->sts_interval : THDVAR(thd, sts_interval);
          sts_mode = THDVAR(thd, sts_mode) <= 0 ?
            share->sts_mode : THDVAR(thd, sts_mode);
#ifdef WITH_PARTITION_STORAGE_ENGINE
          sts_sync = THDVAR(thd, sts_sync) == -1 ?
            share->sts_sync : THDVAR(thd, sts_sync);
#endif
          crd_interval = THDVAR(thd, crd_interval) == -1 ?
            share->crd_interval : THDVAR(thd, crd_interval);
          crd_mode = THDVAR(thd, crd_mode) <= 0 ?
            share->crd_mode : THDVAR(thd, crd_mode);
          if (crd_mode == 3)
            crd_mode = 1;
#ifdef WITH_PARTITION_STORAGE_ENGINE
          crd_sync = THDVAR(thd, crd_sync) == -1 ?
            share->crd_sync : THDVAR(thd, crd_sync);
#endif
          time_t tmp_time = (time_t) time((time_t*) 0);
          if ((spider_init_error_table =
            spider_get_init_error_table(share, FALSE)))
          {
            DBUG_PRINT("info",("spider diff2=%f",
              difftime(tmp_time, spider_init_error_table->init_error_time)));
            if (difftime(tmp_time,
              spider_init_error_table->init_error_time) <
              spider_table_init_error_interval)
            {
              *error_num = spider_init_error_table->init_error;
              if (spider_init_error_table->init_error_with_message)
                my_message(spider_init_error_table->init_error,
                  spider_init_error_table->init_error_msg, MYF(0));
              pthread_mutex_unlock(&share->crd_mutex);
              pthread_mutex_unlock(&share->sts_mutex);
              spider_free_share(share);
              goto error_but_no_delete;
            }
          }

          if (
            (*error_num = spider_get_sts(share, spider->search_link_idx,
              tmp_time, spider, sts_interval, sts_mode,
#ifdef WITH_PARTITION_STORAGE_ENGINE
              sts_sync,
#endif
              1)) ||
            (*error_num = spider_get_crd(share, spider->search_link_idx,
              tmp_time, spider, table, crd_interval, crd_mode,
#ifdef WITH_PARTITION_STORAGE_ENGINE
              crd_sync,
#endif
              1))
          ) {
            if (
              share->monitoring_kind[spider->search_link_idx] &&
              spider->need_mons[spider->search_link_idx]
            ) {
              *error_num = spider_ping_table_mon_from_table(
                  spider->trx,
                  spider->trx->thd,
                  share,
                  share->monitoring_sid[spider->search_link_idx],
                  share->table_name,
                  share->table_name_length,
                  spider->search_link_idx,
                  "",
                  0,
                  share->monitoring_kind[spider->search_link_idx],
                  share->monitoring_limit[spider->search_link_idx],
                  FALSE
                );
            }
            if (
              spider_init_error_table ||
              (spider_init_error_table =
                spider_get_init_error_table(share, TRUE))
            ) {
              spider_init_error_table->init_error = *error_num;
              if ((spider_init_error_table->init_error_with_message =
                thd->main_da.is_error()))
                strmov(spider_init_error_table->init_error_msg,
                  thd->main_da.message());
              spider_init_error_table->init_error_time =
                (time_t) time((time_t*) 0);
            }
            pthread_mutex_unlock(&share->crd_mutex);
            pthread_mutex_unlock(&share->sts_mutex);
            spider_free_share(share);
            goto error_but_no_delete;
          }
        }
        share->init_error = FALSE;
      }
      pthread_mutex_unlock(&share->crd_mutex);
      pthread_mutex_unlock(&share->sts_mutex);
    }
  }

  DBUG_PRINT("info",("spider share=%x", share));
  DBUG_RETURN(share);

error_hash_insert:
#ifdef WITH_PARTITION_STORAGE_ENGINE
error_get_pt_share:
#endif
  thr_lock_delete(&share->lock);
  VOID(pthread_mutex_destroy(&share->auto_increment_mutex));
error_init_auto_increment_mutex:
  VOID(pthread_mutex_destroy(&share->crd_mutex));
error_init_crd_mutex:
  VOID(pthread_mutex_destroy(&share->sts_mutex));
error_init_sts_mutex:
  VOID(pthread_mutex_destroy(&share->mutex));
error_init_mutex:
error_init_string:
error_create_conn_keys:
error_get_link_statuses:
error_open_sys_table:
error_parse_connect_string:
error_init_hint_string:
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
#ifndef WITHOUT_SPIDER_BG_SEARCH
    spider_free_sts_thread(share);
    spider_free_crd_thread(share);
    spider_free_mon_threads(share);
#endif
    spider_free_share_alloc(share);
    hash_delete(&spider_open_tables, (uchar*) share);
    thr_lock_delete(&share->lock);
    VOID(pthread_mutex_destroy(&share->auto_increment_mutex));
    VOID(pthread_mutex_destroy(&share->crd_mutex));
    VOID(pthread_mutex_destroy(&share->sts_mutex));
    VOID(pthread_mutex_destroy(&share->mutex));
    my_free(share, MYF(0));
  }
  pthread_mutex_unlock(&spider_tbl_mutex);
  DBUG_RETURN(0);
}

#ifdef WITH_PARTITION_STORAGE_ENGINE
SPIDER_PARTITION_SHARE *spider_get_pt_share(
  TABLE *table,
  SPIDER_SHARE *share,
  int *error_num
) {
  SPIDER_PARTITION_SHARE *partition_share;
  char *tmp_name;
  longlong *tmp_cardinality;
  DBUG_ENTER("spider_get_pt_share");

  pthread_mutex_lock(&spider_pt_share_mutex);
  if (!(partition_share = (SPIDER_PARTITION_SHARE*) hash_search(
    &spider_open_pt_share,
    (uchar*) table->s->path.str, table->s->path.length)))
  {
    DBUG_PRINT("info",("spider create new pt share"));
    if (!(partition_share = (SPIDER_PARTITION_SHARE *)
      my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
        &partition_share, sizeof(*partition_share),
        &tmp_name, table->s->path.length + 1,
        &tmp_cardinality, sizeof(*tmp_cardinality) * table->s->fields,
        NullS))
    ) {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_alloc_share;
    }

    partition_share->use_count = 0;
    partition_share->table_name_length = table->s->path.length;
    partition_share->table_name = tmp_name;
    memcpy(partition_share->table_name, table->s->path.str,
      partition_share->table_name_length);
    partition_share->cardinality = tmp_cardinality;

    partition_share->crd_get_time = partition_share->sts_get_time =
      share->crd_get_time;

    if (pthread_mutex_init(&partition_share->sts_mutex, MY_MUTEX_INIT_FAST))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_sts_mutex;
    }

    if (pthread_mutex_init(&partition_share->crd_mutex, MY_MUTEX_INIT_FAST))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_crd_mutex;
    }

    if (my_hash_insert(&spider_open_pt_share, (uchar*) partition_share))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_hash_insert;
    }
  }
  partition_share->use_count++;
  pthread_mutex_unlock(&spider_pt_share_mutex);

  DBUG_PRINT("info",("spider partition_share=%x", partition_share));
  DBUG_RETURN(partition_share);

error_hash_insert:
  VOID(pthread_mutex_destroy(&partition_share->crd_mutex));
error_init_crd_mutex:
  VOID(pthread_mutex_destroy(&partition_share->sts_mutex));
error_init_sts_mutex:
  my_free(partition_share, MYF(0));
error_alloc_share:
  pthread_mutex_unlock(&spider_pt_share_mutex);
  DBUG_RETURN(NULL);
}

int spider_free_pt_share(
  SPIDER_PARTITION_SHARE *partition_share
) {
  DBUG_ENTER("spider_free_pt_share");
  pthread_mutex_lock(&spider_pt_share_mutex);
  if (!--partition_share->use_count)
  {
    hash_delete(&spider_open_pt_share, (uchar*) partition_share);
    VOID(pthread_mutex_destroy(&partition_share->crd_mutex));
    VOID(pthread_mutex_destroy(&partition_share->sts_mutex));
    my_free(partition_share, MYF(0));
  }
  pthread_mutex_unlock(&spider_pt_share_mutex);
  DBUG_RETURN(0);
}

void spider_copy_sts_to_pt_share(
  SPIDER_PARTITION_SHARE *partition_share,
  SPIDER_SHARE *share
) {
  DBUG_ENTER("spider_copy_sts_to_pt_share");
  memcpy(&partition_share->data_file_length, &share->data_file_length,
    sizeof(ulonglong) * 4 + sizeof(ha_rows) +
    sizeof(ulong) + sizeof(time_t) * 3);
  DBUG_VOID_RETURN;
}

void spider_copy_sts_to_share(
  SPIDER_SHARE *share,
  SPIDER_PARTITION_SHARE *partition_share
) {
  DBUG_ENTER("spider_copy_sts_to_share");
  memcpy(&share->data_file_length, &partition_share->data_file_length,
    sizeof(ulonglong) * 4 + sizeof(ha_rows) +
    sizeof(ulong) + sizeof(time_t) * 3);
  DBUG_VOID_RETURN;
}

void spider_copy_crd_to_pt_share(
  SPIDER_PARTITION_SHARE *partition_share,
  SPIDER_SHARE *share,
  int fields
) {
  DBUG_ENTER("spider_copy_crd_to_pt_share");
  memcpy(partition_share->cardinality, share->cardinality,
    sizeof(longlong) * fields);
  DBUG_VOID_RETURN;
}

void spider_copy_crd_to_share(
  SPIDER_SHARE *share,
  SPIDER_PARTITION_SHARE *partition_share,
  int fields
) {
  DBUG_ENTER("spider_copy_crd_to_share");
  memcpy(share->cardinality, partition_share->cardinality,
    sizeof(longlong) * fields);
  DBUG_VOID_RETURN;
}
#endif

int spider_open_all_tables(
  SPIDER_TRX *trx,
  bool lock
) {
  TABLE *table_tables;
  int error_num, roop_count, *need_mon;
  char tables_key[MAX_KEY_LENGTH];
  SPIDER_SHARE tmp_share;
  char *tmp_connect_info[15];
  uint tmp_connect_info_length[15];
  long tmp_long[5];
  longlong tmp_longlong[3];
  SPIDER_CONN *conn, **conns;
  ha_spider *spider;
  SPIDER_SHARE *share;
  char **connect_info;
  uint *connect_info_length;
  long *long_info;
  longlong *longlong_info;
  MEM_ROOT mem_root;
  Open_tables_state open_tables_backup;
  DBUG_ENTER("spider_open_all_tables");
  if (
    !(table_tables = spider_open_sys_table(
      trx->thd, SPIDER_SYS_TABLES_TABLE_NAME_STR,
      SPIDER_SYS_TABLES_TABLE_NAME_LEN, TRUE, &open_tables_backup, TRUE,
      &error_num))
  )
    DBUG_RETURN(error_num);
  if (
    (error_num = spider_sys_index_first(table_tables, 1))
  ) {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table_tables->file->print_error(error_num, MYF(0));
      spider_close_sys_table(trx->thd, table_tables,
        &open_tables_backup, TRUE);
      DBUG_RETURN(error_num);
    } else {
      spider_close_sys_table(trx->thd, table_tables,
        &open_tables_backup, TRUE);
      DBUG_RETURN(0);
    }
  }

  init_alloc_root(&mem_root, 4096, 0);
  memset(&tmp_share, 0, sizeof(SPIDER_SHARE));
  spider_set_tmp_share_pointer(&tmp_share, (char **) &tmp_connect_info,
    tmp_connect_info_length, tmp_long, tmp_longlong);
  tmp_share.link_statuses[0] = -1;

  do {
    if (
      (error_num = spider_get_sys_tables_connect_info(
        table_tables, &tmp_share, 0, &mem_root)) ||
      (error_num = spider_set_connect_info_default(
        &tmp_share,
#ifdef WITH_PARTITION_STORAGE_ENGINE
        NULL,
        NULL,
#endif
        NULL
      )) ||
      (error_num = spider_create_conn_keys(&tmp_share))
    ) {
      spider_sys_index_end(table_tables);
      spider_close_sys_table(trx->thd, table_tables,
        &open_tables_backup, TRUE);
      spider_free_tmp_share_alloc(&tmp_share);
      free_root(&mem_root, MYF(0));
      DBUG_RETURN(error_num);
    }

    /* create conn */
    if (
      !(conn = spider_get_conn(
        &tmp_share, 0, tmp_share.conn_keys[0], trx, NULL, FALSE, FALSE,
        &error_num))
    ) {
      spider_sys_index_end(table_tables);
      spider_close_sys_table(trx->thd, table_tables,
        &open_tables_backup, TRUE);
      spider_free_tmp_share_alloc(&tmp_share);
      free_root(&mem_root, MYF(0));
      DBUG_RETURN(error_num);
    }

    if (lock && THDVAR(trx->thd, use_snapshot_with_flush_tables) == 2)
    {
      if (!(spider = new ha_spider()))
      {
        spider_sys_index_end(table_tables);
        spider_close_sys_table(trx->thd, table_tables,
          &open_tables_backup, TRUE);
        spider_free_tmp_share_alloc(&tmp_share);
        free_root(&mem_root, MYF(0));
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
      spider->lock_type = TL_READ_NO_INSERT;

      if (!(share = (SPIDER_SHARE *)
        my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
          &share, sizeof(*share),
          &connect_info, sizeof(char *) * 15,
          &connect_info_length, sizeof(uint) * 15,
          &long_info, sizeof(long) * 5,
          &longlong_info, sizeof(longlong) * 3,
          &conns, sizeof(SPIDER_CONN *),
          &need_mon, sizeof(int),
          NullS))
      ) {
        delete spider;
        spider_sys_index_end(table_tables);
        spider_close_sys_table(trx->thd, table_tables,
          &open_tables_backup, TRUE);
        spider_free_tmp_share_alloc(&tmp_share);
        free_root(&mem_root, MYF(0));
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
      memcpy(share, &tmp_share, sizeof(*share));
      spider_set_tmp_share_pointer(share, connect_info,
        connect_info_length, long_info, longlong_info);
      memcpy(connect_info, &tmp_connect_info, sizeof(char *) * 15);
      memcpy(connect_info_length, &tmp_connect_info_length, sizeof(uint) * 15);
      memcpy(long_info, &tmp_long, sizeof(long) * 5);
      memcpy(longlong_info, &tmp_longlong, sizeof(longlong) * 3);
      spider->share = share;
      spider->trx = trx;
      spider->conns = conns;
      spider->need_mons = need_mon;

      /* create another conn */
      if (
        (!(conn = spider_get_conn(
        &tmp_share, 0, tmp_share.conn_keys[0], trx, spider, TRUE, FALSE,
        &error_num)))
      ) {
        my_free(share, MYF(0));
        delete spider;
        spider_sys_index_end(table_tables);
        spider_close_sys_table(trx->thd, table_tables,
          &open_tables_backup, TRUE);
        spider_free_tmp_share_alloc(&tmp_share);
        free_root(&mem_root, MYF(0));
        DBUG_RETURN(error_num);
      }

      spider->next = NULL;
      if (conn->another_ha_last)
      {
        ((ha_spider*) conn->another_ha_last)->next = spider;
      } else {
        conn->another_ha_first = (void*) spider;
      }
      conn->another_ha_last = (void*) spider;

      if (my_hash_insert(&conn->lock_table_hash, (uchar*) spider))
      {
        my_free(share, MYF(0));
        delete spider;
        spider_sys_index_end(table_tables);
        spider_close_sys_table(trx->thd, table_tables,
          &open_tables_backup, TRUE);
        spider_free_tmp_share_alloc(&tmp_share);
        free_root(&mem_root, MYF(0));
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      }
    } else
      spider_free_tmp_share_alloc(&tmp_share);
    error_num = spider_sys_index_next(table_tables);
  } while (error_num == 0);
  free_root(&mem_root, MYF(0));

  spider_sys_index_end(table_tables);
  spider_close_sys_table(trx->thd, table_tables,
    &open_tables_backup, TRUE);
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
  if (
    THDVAR(trx->thd, use_flash_logs) &&
    (
      !trx->trx_consistent_snapshot ||
      !THDVAR(trx->thd, use_all_conns_snapshot) ||
      !THDVAR(trx->thd, use_snapshot_with_flush_tables)
    )
  ) {
    if (
      (error_num = spider_open_all_tables(trx, FALSE)) ||
      (error_num = spider_trx_all_flush_logs(trx))
    ) {
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
  handlerton* hton,
  THD* thd
) {
  int roop_count = 0, need_mon = 0;
  SPIDER_CONN *conn;
  SPIDER_TRX *trx;
  ha_spider tmp_spider;
  DBUG_ENTER("spider_close_connection");
  if (!(trx = (SPIDER_TRX*) thd_get_ha_data(thd, spider_hton_ptr)))
    DBUG_RETURN(0); /* transaction is not started */

  memset(&tmp_spider, 0, sizeof(ha_spider));
  tmp_spider.conns = &conn;
  tmp_spider.need_mons = &need_mon;
  tmp_spider.trx = trx;
  while ((conn = (SPIDER_CONN*) hash_element(&trx->trx_conn_hash,
    roop_count)))
  {
    DBUG_PRINT("info",("spider conn->table_lock=%d", conn->table_lock));
    if (conn->table_lock > 0)
    {
      if (!conn->trx_start)
        conn->disable_reconnect = FALSE;
      if (conn->table_lock != 2)
      {
        spider_db_unlock_tables(&tmp_spider, 0);
      }
      conn->table_lock = 0;
    }
    roop_count++;
  }

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
  int roop_count;
  SPIDER_CONN *conn;
  SPIDER_INIT_ERROR_TABLE *spider_init_error_table;
  SPIDER_TABLE_MON_LIST *table_mon_list;
  DBUG_ENTER("spider_db_done");

#ifndef WITHOUT_SPIDER_BG_SEARCH
  VOID(spider_free_trx(spider_global_trx));
#endif

  for (roop_count = spider_udf_table_mon_mutex_count - 1; roop_count >= 0;
    roop_count--)
  {
    while ((table_mon_list = (SPIDER_TABLE_MON_LIST *) hash_element(
      &spider_udf_table_mon_list_hash[roop_count], 0)))
    {
      hash_delete(&spider_udf_table_mon_list_hash[roop_count],
        (uchar*) table_mon_list);
      spider_ping_table_free_mon_list(table_mon_list);
    }
    hash_free(&spider_udf_table_mon_list_hash[roop_count]);
  }
  for (roop_count = spider_udf_table_mon_mutex_count - 1; roop_count >= 0;
    roop_count--)
    VOID(pthread_cond_destroy(&spider_udf_table_mon_conds[roop_count]));
  for (roop_count = spider_udf_table_mon_mutex_count - 1; roop_count >= 0;
    roop_count--)
    VOID(pthread_mutex_destroy(&spider_udf_table_mon_mutexes[roop_count]));
  my_free(spider_udf_table_mon_mutexes, MYF(0));

  pthread_mutex_lock(&spider_conn_mutex);
  while ((conn = (SPIDER_CONN*) hash_element(&spider_open_connections, 0)))
  {
    hash_delete(&spider_open_connections, (uchar*) conn);
    spider_free_conn(conn);
  }
  pthread_mutex_unlock(&spider_conn_mutex);
  hash_free(&spider_open_connections);
#ifdef WITH_PARTITION_STORAGE_ENGINE
  hash_free(&spider_open_pt_share);
#endif
  pthread_mutex_lock(&spider_init_error_tbl_mutex);
  while ((spider_init_error_table = (SPIDER_INIT_ERROR_TABLE*)
    hash_element(&spider_init_error_tables, 0)))
  {
    hash_delete(&spider_init_error_tables, (uchar*) spider_init_error_table);
    my_free(spider_init_error_table, MYF(0));
  }
  pthread_mutex_unlock(&spider_init_error_tbl_mutex);
  hash_free(&spider_init_error_tables);
  hash_free(&spider_open_tables);
  VOID(pthread_mutex_destroy(&spider_open_conn_mutex));
#ifndef WITHOUT_SPIDER_BG_SEARCH
  VOID(pthread_mutex_destroy(&spider_global_trx_mutex));
#endif
  VOID(pthread_mutex_destroy(&spider_conn_mutex));
#ifdef WITH_PARTITION_STORAGE_ENGINE
  VOID(pthread_mutex_destroy(&spider_pt_share_mutex));
#endif
  VOID(pthread_mutex_destroy(&spider_init_error_tbl_mutex));
  VOID(pthread_mutex_destroy(&spider_tbl_mutex));
#ifndef WITHOUT_SPIDER_BG_SEARCH
  VOID(pthread_attr_destroy(&spider_pt_attr));
#endif

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
  int error_num, roop_count;
  handlerton *spider_hton = (handlerton *)p;
  DBUG_ENTER("spider_db_init");
  spider_hton_ptr = spider_hton;

  spider_hton->state = SHOW_OPTION_YES;
  spider_hton->flags = HTON_NO_FLAGS;
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
  if (spider_support_xa)
  {
    spider_hton->prepare = spider_xa_prepare;
    spider_hton->recover = spider_xa_recover;
    spider_hton->commit_by_xid = spider_xa_commit_by_xid;
    spider_hton->rollback_by_xid = spider_xa_rollback_by_xid;
  }
  spider_hton->create = spider_create_handler;
  spider_hton->drop_database = spider_drop_database;
  spider_hton->show_status = spider_show_status;

#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (pthread_attr_init(&spider_pt_attr))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_pt_attr_init;
  }
  if (pthread_attr_setdetachstate(&spider_pt_attr, PTHREAD_CREATE_DETACHED))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_pt_attr_setstate;
  }
#endif

  if (pthread_mutex_init(&spider_tbl_mutex, MY_MUTEX_INIT_FAST))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_tbl_mutex_init;
  }
  if (pthread_mutex_init(&spider_init_error_tbl_mutex, MY_MUTEX_INIT_FAST))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_init_error_tbl_mutex_init;
  }
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (pthread_mutex_init(&spider_pt_share_mutex, MY_MUTEX_INIT_FAST))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_pt_share_mutex_init;
  }
#endif
  if (pthread_mutex_init(&spider_conn_mutex, MY_MUTEX_INIT_FAST))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_conn_mutex_init;
  }
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (pthread_mutex_init(&spider_global_trx_mutex, MY_MUTEX_INIT_FAST))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_global_trx_mutex_init;
  }
#endif
  if (pthread_mutex_init(&spider_open_conn_mutex, MY_MUTEX_INIT_FAST))
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_open_conn_mutex_init;
  }

  if(
    hash_init(&spider_open_tables, system_charset_info, 32, 0, 0,
                   (hash_get_key) spider_tbl_get_key, 0, 0)
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_open_tables_hash_init;
  }
  if(
    hash_init(&spider_init_error_tables, system_charset_info, 32, 0, 0,
                   (hash_get_key) spider_tbl_get_key, 0, 0)
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_init_error_tables_hash_init;
  }
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if(
    hash_init(&spider_open_pt_share, system_charset_info, 32, 0, 0,
                   (hash_get_key) spider_pt_share_get_key, 0, 0)
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_open_pt_share_hash_init;
  }
#endif
  if(
    hash_init(&spider_open_connections, system_charset_info, 32, 0, 0,
                   (hash_get_key) spider_conn_get_key, 0, 0)
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_open_connections_hash_init;
  }

  if (!(spider_udf_table_mon_mutexes = (pthread_mutex_t *)
    my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
      &spider_udf_table_mon_mutexes, sizeof(pthread_mutex_t) *
        spider_udf_table_mon_mutex_count,
      &spider_udf_table_mon_conds, sizeof(pthread_cond_t) *
        spider_udf_table_mon_mutex_count,
      &spider_udf_table_mon_list_hash, sizeof(HASH) *
        spider_udf_table_mon_mutex_count,
      NullS))
  )
    goto error_alloc_mon_mutxes;

  for (roop_count = 0; roop_count < spider_udf_table_mon_mutex_count;
    roop_count++)
  {
    if (pthread_mutex_init(&spider_udf_table_mon_mutexes[roop_count],
      MY_MUTEX_INIT_FAST))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_udf_table_mon_mutex;
    }
  }
  for (roop_count = 0; roop_count < spider_udf_table_mon_mutex_count;
    roop_count++)
  {
    if (pthread_cond_init(&spider_udf_table_mon_conds[roop_count], NULL))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_udf_table_mon_cond;
    }
  }
  for (roop_count = 0; roop_count < spider_udf_table_mon_mutex_count;
    roop_count++)
  {
    if (hash_init(&spider_udf_table_mon_list_hash[roop_count],
      system_charset_info, 32, 0, 0,
      (hash_get_key) spider_udf_tbl_mon_list_key, 0, 0))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_udf_table_mon_list_hash;
    }
  }

#ifndef WITHOUT_SPIDER_BG_SEARCH
  if (!(spider_global_trx = spider_get_trx(NULL, &error_num)))
    goto error;
#endif

  DBUG_RETURN(0);

#ifndef WITHOUT_SPIDER_BG_SEARCH
error:
  roop_count = spider_udf_table_mon_mutex_count - 1;
#endif
error_init_udf_table_mon_list_hash:
  for (; roop_count >= 0; roop_count--)
    hash_free(&spider_udf_table_mon_list_hash[roop_count]);
  roop_count = spider_udf_table_mon_mutex_count - 1;
error_init_udf_table_mon_cond:
  for (; roop_count >= 0; roop_count--)
    VOID(pthread_cond_destroy(&spider_udf_table_mon_conds[roop_count]));
  roop_count = spider_udf_table_mon_mutex_count - 1;
error_init_udf_table_mon_mutex:
  for (; roop_count >= 0; roop_count--)
    VOID(pthread_mutex_destroy(&spider_udf_table_mon_mutexes[roop_count]));
  my_free(spider_udf_table_mon_mutexes, MYF(0));
error_alloc_mon_mutxes:
  hash_free(&spider_open_connections);
error_open_connections_hash_init:
#ifdef WITH_PARTITION_STORAGE_ENGINE
  hash_free(&spider_open_pt_share);
error_open_pt_share_hash_init:
#endif
  hash_free(&spider_init_error_tables);
error_init_error_tables_hash_init:
  hash_free(&spider_open_tables);
error_open_tables_hash_init:
  VOID(pthread_mutex_destroy(&spider_open_conn_mutex));
error_open_conn_mutex_init:
#ifndef WITHOUT_SPIDER_BG_SEARCH
  VOID(pthread_mutex_destroy(&spider_global_trx_mutex));
error_global_trx_mutex_init:
#endif
  VOID(pthread_mutex_destroy(&spider_conn_mutex));
error_conn_mutex_init:
#ifdef WITH_PARTITION_STORAGE_ENGINE
  VOID(pthread_mutex_destroy(&spider_pt_share_mutex));
error_pt_share_mutex_init:
#endif
  VOID(pthread_mutex_destroy(&spider_init_error_tbl_mutex));
error_init_error_tbl_mutex_init:
  VOID(pthread_mutex_destroy(&spider_tbl_mutex));
error_tbl_mutex_init:
#ifndef WITHOUT_SPIDER_BG_SEARCH
error_pt_attr_setstate:
  VOID(pthread_attr_destroy(&spider_pt_attr));
error_pt_attr_init:
#endif
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
  uint table_name_length,
  const TABLE *table,
  partition_element **part_elem,
  partition_element **sub_elem
) {
  char tmp_name[FN_LEN];
  partition_info *part_info = table->part_info;
  partition_element *tmp_part_elem = NULL, *tmp_sub_elem = NULL;
  bool tmp_flg = FALSE, tmp_find_flg = FALSE;
  DBUG_ENTER("spider_get_partition_info");
  *part_elem = NULL;
  *sub_elem = NULL;
  if (!part_info)
    DBUG_VOID_RETURN;

  if (!memcmp(table_name + table_name_length - 5, "#TMP#", 5))
    tmp_flg = TRUE;

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
        if (!memcmp(table_name, tmp_name, table_name_length + 1))
          DBUG_VOID_RETURN;
        if (
          tmp_flg &&
          *(tmp_name + table_name_length - 5) == '\0' &&
          !memcmp(table_name, tmp_name, table_name_length - 5)
        ) {
          tmp_part_elem = *part_elem;
          tmp_sub_elem = *sub_elem;
          tmp_flg = FALSE;
          tmp_find_flg = TRUE;
        }
      }
    } else {
      create_partition_name(tmp_name, table->s->path.str,
        (*part_elem)->partition_name, NORMAL_PART_NAME, TRUE);
      DBUG_PRINT("info",("spider tmp_name=%s", tmp_name));
      if (!memcmp(table_name, tmp_name, table_name_length + 1))
        DBUG_VOID_RETURN;
      if (
        tmp_flg &&
        *(tmp_name + table_name_length - 5) == '\0' &&
        !memcmp(table_name, tmp_name, table_name_length - 5)
      ) {
        tmp_part_elem = *part_elem;
        tmp_flg = FALSE;
        tmp_find_flg = TRUE;
      }
    }
  }
  if (tmp_find_flg)
  {
    *part_elem = tmp_part_elem;
    *sub_elem = tmp_sub_elem;
    DBUG_PRINT("info",("spider tmp find"));
    DBUG_VOID_RETURN;
  }
  *part_elem = NULL;
  *sub_elem = NULL;
  DBUG_PRINT("info",("spider no hit"));
  DBUG_VOID_RETURN;
}
#endif

int spider_get_sts(
  SPIDER_SHARE *share,
  int link_idx,
  time_t tmp_time,
  ha_spider *spider,
  double sts_interval,
  int sts_mode,
#ifdef WITH_PARTITION_STORAGE_ENGINE
  int sts_sync,
#endif
  int sts_sync_level
) {
  int get_type;
  int error_num = 0;
  DBUG_ENTER("spider_get_sts");

#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (
    sts_sync == 0
  ) {
#endif
    /* get */
    get_type = 1;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  } else if (
    !share->partition_share->sts_init
  ) {
    pthread_mutex_lock(&share->partition_share->sts_mutex);
    if (!share->partition_share->sts_init)
    {
      /* get after mutex_lock */
      get_type = 2;
    } else {
      pthread_mutex_unlock(&share->partition_share->sts_mutex);
      /* copy */
      get_type = 0;
    }
  } else if (
    difftime(share->sts_get_time, share->partition_share->sts_get_time) <
      sts_interval
  ) {
    /* copy */
    get_type = 0;
  } else if (
    !pthread_mutex_trylock(&share->partition_share->sts_mutex)
  ) {
    /* get after mutex_trylock */
    get_type = 3;
  } else {
    /* copy */
    get_type = 0;
  }
  if (get_type == 0)
    spider_copy_sts_to_share(share, share->partition_share);
  else
#endif
    error_num = spider_db_show_table_status(spider, link_idx, sts_mode);
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (get_type >= 2)
    pthread_mutex_unlock(&share->partition_share->sts_mutex);
#endif
  if (error_num)
    DBUG_RETURN(error_num);
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (sts_sync >= sts_sync_level && get_type > 0)
  {
    spider_copy_sts_to_pt_share(share->partition_share, share);
    share->partition_share->sts_get_time = tmp_time;
    share->partition_share->sts_init = TRUE;
  }
#endif
  share->sts_get_time = tmp_time;
  share->sts_init = TRUE;
  DBUG_RETURN(0);
}

int spider_get_crd(
  SPIDER_SHARE *share,
  int link_idx,
  time_t tmp_time,
  ha_spider *spider,
  TABLE *table,
  double crd_interval,
  int crd_mode,
#ifdef WITH_PARTITION_STORAGE_ENGINE
  int crd_sync,
#endif
  int crd_sync_level
) {
  int get_type;
  int error_num = 0;
  DBUG_ENTER("spider_get_crd");

#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (
    crd_sync == 0
  ) {
#endif
    /* get */
    get_type = 1;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  } else if (
    !share->partition_share->crd_init
  ) {
    pthread_mutex_lock(&share->partition_share->crd_mutex);
    if (!share->partition_share->crd_init)
    {
      /* get after mutex_lock */
      get_type = 2;
    } else {
      pthread_mutex_unlock(&share->partition_share->crd_mutex);
      /* copy */
      get_type = 0;
    }
  } else if (
    difftime(share->crd_get_time, share->partition_share->crd_get_time) <
      crd_interval
  ) {
    /* copy */
    get_type = 0;
  } else if (
    !pthread_mutex_trylock(&share->partition_share->crd_mutex)
  ) {
    /* get after mutex_trylock */
    get_type = 3;
  } else {
    /* copy */
    get_type = 0;
  }
  if (get_type == 0)
    spider_copy_crd_to_share(share, share->partition_share,
      table->s->fields);
  else
#endif
    error_num = spider_db_show_index(spider, link_idx, table, crd_mode);
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (get_type >= 2)
    pthread_mutex_unlock(&share->partition_share->crd_mutex);
#endif
  if (error_num)
    DBUG_RETURN(error_num);
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (crd_sync >= crd_sync_level && get_type > 0)
  {
    spider_copy_crd_to_pt_share(share->partition_share, share,
      table->s->fields);
    share->partition_share->crd_get_time = tmp_time;
    share->partition_share->crd_init = TRUE;
  }
#endif
  share->crd_get_time = tmp_time;
  share->crd_init = TRUE;
  DBUG_RETURN(0);
}

void spider_set_result_list_param(
  ha_spider *spider
) {
  SPIDER_RESULT_LIST *result_list = &spider->result_list;
  SPIDER_SHARE *share = spider->share;
  THD *thd = spider->trx->thd;
  DBUG_ENTER("spider_set_result_list_param");
  result_list->internal_offset =
    THDVAR(thd, internal_offset) < 0 ?
    share->internal_offset :
    THDVAR(thd, internal_offset);
  result_list->internal_limit =
    THDVAR(thd, internal_limit) < 0 ?
    share->internal_limit :
    THDVAR(thd, internal_limit);
  result_list->split_read = spider_split_read_param(spider);
  result_list->multi_split_read =
    THDVAR(thd, multi_split_read) < 0 ?
    share->multi_split_read :
    THDVAR(thd, multi_split_read);
  result_list->max_order =
    THDVAR(thd, max_order) < 0 ?
    share->max_order :
    THDVAR(thd, max_order);
  result_list->quick_mode =
    THDVAR(thd, quick_mode) < 0 ?
    share->quick_mode :
    THDVAR(thd, quick_mode);
  result_list->quick_page_size =
    THDVAR(thd, quick_page_size) < 0 ?
    share->quick_page_size :
    THDVAR(thd, quick_page_size);
  result_list->low_mem_read =
    THDVAR(thd, low_mem_read) < 0 ?
    share->low_mem_read :
    THDVAR(thd, low_mem_read);
  DBUG_VOID_RETURN;
}

SPIDER_INIT_ERROR_TABLE *spider_get_init_error_table(
  SPIDER_SHARE *share,
  bool create
) {
  SPIDER_INIT_ERROR_TABLE *spider_init_error_table;
  char *tmp_name;
  DBUG_ENTER("spider_get_init_error_table");
  pthread_mutex_lock(&spider_init_error_tbl_mutex);
  if (!(spider_init_error_table = (SPIDER_INIT_ERROR_TABLE *) hash_search(
    &spider_init_error_tables,
    (uchar*) share->table_name, share->table_name_length)))
  {
    if (!create)
    {
      pthread_mutex_unlock(&spider_init_error_tbl_mutex);
      DBUG_RETURN(NULL);
    }
    if (!(spider_init_error_table = (SPIDER_INIT_ERROR_TABLE *)
      my_multi_malloc(MYF(MY_WME | MY_ZEROFILL),
        &spider_init_error_table, sizeof(*spider_init_error_table),
        &tmp_name, share->table_name_length + 1,
        NullS))
    ) {
      pthread_mutex_unlock(&spider_init_error_tbl_mutex);
      DBUG_RETURN(NULL);
    }
    memcpy(tmp_name, share->table_name, share->table_name_length);
    spider_init_error_table->table_name = tmp_name;
    spider_init_error_table->table_name_length = share->table_name_length;
    if (my_hash_insert(&spider_init_error_tables,
      (uchar*) spider_init_error_table))
    {
      my_free(spider_init_error_table, MYF(0));
      pthread_mutex_unlock(&spider_init_error_tbl_mutex);
      DBUG_RETURN(NULL);
    }
  }
  pthread_mutex_unlock(&spider_init_error_tbl_mutex);
  DBUG_RETURN(spider_init_error_table);
}

bool spider_check_pk_update(
  TABLE *table
) {
  int roop_count;
  TABLE_SHARE *table_share = table->s;
  KEY *key_info;
  KEY_PART_INFO *key_part;
  DBUG_ENTER("spider_check_pk_update");
  if (table_share->primary_key == MAX_KEY)
    DBUG_RETURN(FALSE);

  key_info = &table_share->key_info[table_share->primary_key];
  key_part = key_info->key_part;
  for (roop_count = 0; roop_count < key_info->key_parts; roop_count++)
  {
    if (bitmap_is_set(table->write_set,
      key_part[roop_count].field->field_index))
      DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}

void spider_set_tmp_share_pointer(
  SPIDER_SHARE *tmp_share,
  char **tmp_connect_info,
  uint *tmp_connect_info_length,
  long *tmp_long,
  longlong *tmp_longlong
) {
  DBUG_ENTER("spider_set_tmp_share_pointer");
  tmp_share->link_count = 1;
  tmp_share->server_names = &tmp_connect_info[0];
  tmp_share->tgt_table_names = &tmp_connect_info[1];
  tmp_share->tgt_dbs = &tmp_connect_info[2];
  tmp_share->tgt_hosts = &tmp_connect_info[3];
  tmp_share->tgt_usernames = &tmp_connect_info[4];
  tmp_share->tgt_passwords = &tmp_connect_info[5];
  tmp_share->tgt_sockets = &tmp_connect_info[6];
  tmp_share->tgt_wrappers = &tmp_connect_info[7];
  tmp_share->tgt_ssl_cas = &tmp_connect_info[8];
  tmp_share->tgt_ssl_capaths = &tmp_connect_info[9];
  tmp_share->tgt_ssl_certs = &tmp_connect_info[10];
  tmp_share->tgt_ssl_ciphers = &tmp_connect_info[11];
  tmp_share->tgt_ssl_keys = &tmp_connect_info[12];
  tmp_share->tgt_default_files = &tmp_connect_info[13];
  tmp_share->tgt_default_groups = &tmp_connect_info[14];
  tmp_share->tgt_ports = &tmp_long[0];
  tmp_share->tgt_ssl_vscs = &tmp_long[1];
  tmp_share->link_statuses = &tmp_long[2];
  tmp_share->monitoring_kind = &tmp_long[3];
#ifndef WITHOUT_SPIDER_BG_SEARCH
  tmp_share->monitoring_bg_kind = &tmp_long[4];
#endif
  tmp_share->monitoring_limit = &tmp_longlong[0];
  tmp_share->monitoring_sid = &tmp_longlong[1];
#ifndef WITHOUT_SPIDER_BG_SEARCH
  tmp_share->monitoring_bg_interval = &tmp_longlong[2];
#endif
  tmp_share->server_names_lengths = &tmp_connect_info_length[0];
  tmp_share->tgt_table_names_lengths = &tmp_connect_info_length[1];
  tmp_share->tgt_dbs_lengths = &tmp_connect_info_length[2];
  tmp_share->tgt_hosts_lengths = &tmp_connect_info_length[3];
  tmp_share->tgt_usernames_lengths = &tmp_connect_info_length[4];
  tmp_share->tgt_passwords_lengths = &tmp_connect_info_length[5];
  tmp_share->tgt_sockets_lengths = &tmp_connect_info_length[6];
  tmp_share->tgt_wrappers_lengths = &tmp_connect_info_length[7];
  tmp_share->tgt_ssl_cas_lengths = &tmp_connect_info_length[8];
  tmp_share->tgt_ssl_capaths_lengths = &tmp_connect_info_length[9];
  tmp_share->tgt_ssl_certs_lengths = &tmp_connect_info_length[10];
  tmp_share->tgt_ssl_ciphers_lengths = &tmp_connect_info_length[11];
  tmp_share->tgt_ssl_keys_lengths = &tmp_connect_info_length[12];
  tmp_share->tgt_default_files_lengths = &tmp_connect_info_length[13];
  tmp_share->tgt_default_groups_lengths = &tmp_connect_info_length[14];
  tmp_share->server_names_length = 1;
  tmp_share->tgt_table_names_length = 1;
  tmp_share->tgt_dbs_length = 1;
  tmp_share->tgt_hosts_length = 1;
  tmp_share->tgt_usernames_length = 1;
  tmp_share->tgt_passwords_length = 1;
  tmp_share->tgt_sockets_length = 1;
  tmp_share->tgt_wrappers_length = 1;
  tmp_share->tgt_ssl_cas_length = 1;
  tmp_share->tgt_ssl_capaths_length = 1;
  tmp_share->tgt_ssl_certs_length = 1;
  tmp_share->tgt_ssl_ciphers_length = 1;
  tmp_share->tgt_ssl_keys_length = 1;
  tmp_share->tgt_default_files_length = 1;
  tmp_share->tgt_default_groups_length = 1;
  tmp_share->tgt_ports_length = 1;
  tmp_share->tgt_ssl_vscs_length = 1;
  tmp_share->link_statuses_length = 1;
  tmp_share->monitoring_kind_length = 1;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  tmp_share->monitoring_bg_kind_length = 1;
#endif
  tmp_share->monitoring_limit_length = 1;
  tmp_share->monitoring_sid_length = 1;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  tmp_share->monitoring_bg_interval_length = 1;
#endif
  tmp_share->net_timeout = -1;

#ifndef WITHOUT_SPIDER_BG_SEARCH
  tmp_share->monitoring_bg_kind[0] = -1;
#endif
  tmp_share->monitoring_kind[0] = -1;
#ifndef WITHOUT_SPIDER_BG_SEARCH
  tmp_share->monitoring_bg_interval[0] = -1;
#endif
  tmp_share->monitoring_limit[0] = -1;
  tmp_share->monitoring_sid[0] = -1;
  DBUG_VOID_RETURN;
}

longlong spider_split_read_param(
  ha_spider *spider
) {
  SPIDER_SHARE *share = spider->share;
  THD *thd = spider->trx->thd;
  TABLE *table = spider->get_table();
  TABLE_LIST *table_list = table->pos_in_table_list;
  st_select_lex *select_lex = NULL;
  longlong select_limit = 0;
  longlong offset_limit = 0;
  double semi_split_read;
  longlong split_read;
  DBUG_ENTER("spider_split_read_param");
  if (table_list)
  {
    while (table_list->parent_l)
      table_list = table_list->parent_l;
    select_lex = table_list->select_lex;
    if (select_lex)
    {
      select_limit = select_lex->select_limit ?
        select_lex->select_limit->val_int() : 0;
      offset_limit = select_lex->offset_limit ?
        select_lex->offset_limit->val_int() : 0;
    }
  }
  semi_split_read =
    THDVAR(thd, semi_split_read) < 0 ?
    share->semi_split_read :
    THDVAR(thd, semi_split_read);
  if (semi_split_read > 0 && select_lex && select_lex->explicit_limit)
  {
    semi_split_read = semi_split_read * (select_limit + offset_limit);
    DBUG_PRINT("info",("spider semi_split_read=%f", semi_split_read));
    if (semi_split_read > 9223372036854775807LL)
      DBUG_RETURN(9223372036854775807LL);
    else {
      split_read = (longlong) semi_split_read;
      if (split_read < 0)
        DBUG_RETURN(9223372036854775807LL);
      else if (split_read == 0)
        DBUG_RETURN(1);
      else
        DBUG_RETURN(split_read);
    }
  } else
    DBUG_RETURN(
      THDVAR(thd, split_read) < 0 ?
      share->split_read :
      THDVAR(thd, split_read)
    );
}
