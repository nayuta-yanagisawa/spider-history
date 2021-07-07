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

TABLE *spider_open_sys_table(
  THD *thd,
  char *table_name,
  int table_name_length,
  bool write,
  Open_tables_state *open_tables_backup,
  bool need_lock,
  int *error_num
) {
  int res;
  TABLE *table;
  TABLE_SHARE *table_share;
  TABLE_LIST tables;
  char table_key[MAX_DBKEY_LENGTH];
  uint table_key_length;
  DBUG_ENTER("spider_open_sys_table");

  memset(&tables, 0, sizeof(TABLE_LIST));
  tables.db = (char*)"mysql";
  tables.db_length = sizeof("mysql") - 1;
  tables.alias = tables.table_name = table_name;
  tables.table_name_length = table_name_length;
  tables.lock_type = (write ? TL_WRITE : TL_READ);

  if (need_lock)
  {
    if (!(table = open_performance_schema_table(thd, &tables,
      open_tables_backup)))
    {
      *error_num = my_errno;
      DBUG_RETURN(NULL);
    }
  } else {
    thd->reset_n_backup_open_tables_state(open_tables_backup);

    if (!(table = (TABLE*) my_malloc(sizeof(*table), MYF(MY_WME))))
    {
      *error_num = HA_ERR_OUT_OF_MEM;
      goto error_malloc;
    }

    table_key_length =
      create_table_def_key(thd, table_key, &tables, FALSE);

    if (!(table_share = get_table_share(thd,
      &tables, table_key, table_key_length, 0, error_num)))
      goto error;
    if (open_table_from_share(thd, table_share, tables.alias,
      (uint) (HA_OPEN_KEYFILE | HA_OPEN_RNDFILE | HA_GET_INDEX),
      READ_KEYINFO | COMPUTE_TYPES | EXTRA_RECORD,
      (uint) HA_OPEN_IGNORE_IF_LOCKED | HA_OPEN_FROM_SQL_LAYER,
      table, FALSE)
    ) {
      release_table_share(table_share, RELEASE_NORMAL);
      *error_num = my_errno;
      goto error;
    }
  }
  DBUG_RETURN(table);

error:
  my_free(table, MYF(0));
error_malloc:
  thd->restore_backup_open_tables_state(open_tables_backup);
  DBUG_RETURN(NULL);
}

void spider_close_sys_table(
  THD *thd,
  TABLE *table,
  Open_tables_state *open_tables_backup,
  bool need_lock
) {
  DBUG_ENTER("spider_close_sys_table");
  if (need_lock)
    close_performance_schema_table(thd, open_tables_backup);
  else {
    table->file->ha_reset();
    closefrm(table, TRUE);
    my_free(table, MYF(0));
    thd->restore_backup_open_tables_state(open_tables_backup);
  }
  DBUG_VOID_RETURN;
}

int spider_sys_index_init(
  TABLE *table,
  uint idx,
  bool sorted
) {
  DBUG_ENTER("spider_sys_index_init");
  DBUG_RETURN(table->file->ha_index_init(idx, sorted));
}

int spider_sys_index_end(
  TABLE *table
) {
  DBUG_ENTER("spider_sys_index_end");
  DBUG_RETURN(table->file->ha_index_end());
}

int spider_check_sys_table(
  TABLE *table,
  char *table_key
) {
  DBUG_ENTER("spider_check_sys_table");

  key_copy(
    (uchar *) table_key,
    table->record[0],
    table->key_info,
    table->key_info->key_length);

  DBUG_RETURN(table->file->index_read_idx_map(
    table->record[0], 0, (uchar *) table_key,
    HA_WHOLE_KEY, HA_READ_KEY_EXACT));
}

int spider_get_sys_table_by_idx(
  TABLE *table,
  char *table_key,
  const int idx,
  const int col_count
) {
  int error_num;
  DBUG_ENTER("spider_get_sys_table_by_idx");
  if ((error_num = spider_sys_index_init(table, idx, FALSE)))
    DBUG_RETURN(error_num);

  key_copy(
    (uchar *) table_key,
    table->record[0],
    table->key_info,
    table->key_info->key_length);

  if ((error_num = table->file->index_read_idx_map(
    table->record[0], idx, (uchar *) table_key,
    make_prev_keypart_map(col_count), HA_READ_KEY_EXACT)))
  {
    spider_sys_index_end(table);
    DBUG_RETURN(error_num);
  }
  DBUG_RETURN(0);
}

int spider_sys_index_next_same(
  TABLE *table,
  char *table_key
) {
  DBUG_ENTER("spider_sys_index_next_same");
  DBUG_RETURN(table->file->index_next_same(
    table->record[0],
    (const uchar*) table_key,
    table->key_info->key_length));
}

int spider_sys_index_first(
  TABLE *table,
  const int idx
) {
  int error_num;
  DBUG_ENTER("spider_sys_index_first");
  if ((error_num = spider_sys_index_init(table, idx, FALSE)))
    DBUG_RETURN(error_num);

  if ((error_num = table->file->index_first(table->record[0])))
  {
    spider_sys_index_end(table);
    DBUG_RETURN(error_num);
  }
  DBUG_RETURN(0);
}

int spider_sys_index_next(
  TABLE *table
) {
  DBUG_ENTER("spider_sys_index_next");
  DBUG_RETURN(table->file->index_next(table->record[0]));
}

void spider_store_xa_pk(
  TABLE *table,
  XID *xid
) {
  DBUG_ENTER("spider_store_xa_pk");
  table->field[0]->store(xid->formatID);
  table->field[1]->store(xid->gtrid_length);
  table->field[3]->store(
    xid->data,
    (uint) xid->gtrid_length + xid->bqual_length,
    system_charset_info);
  DBUG_VOID_RETURN;
}

void spider_store_xa_bqual_length(
  TABLE *table,
  XID *xid
) {
  DBUG_ENTER("spider_store_xa_bqual_length");
  table->field[2]->store(xid->bqual_length);
  DBUG_VOID_RETURN;
}

void spider_store_xa_status(
  TABLE *table,
  const char *status
) {
  DBUG_ENTER("spider_store_xa_status");
  table->field[4]->store(
    status,
    (uint) strlen(status),
    system_charset_info);
  DBUG_VOID_RETURN;
}

void spider_store_xa_member_pk(
  TABLE *table,
  XID *xid,
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_store_xa_member_pk");
  table->field[0]->store(xid->formatID);
  table->field[1]->store(xid->gtrid_length);
  table->field[3]->store(
    xid->data,
    (uint) xid->gtrid_length + xid->bqual_length,
    system_charset_info);
  table->field[5]->store(
    conn->tgt_host,
    (uint) conn->tgt_host_length,
    system_charset_info);
  table->field[6]->store(
    conn->tgt_port);
  table->field[7]->store(
    conn->tgt_socket,
    (uint) conn->tgt_socket_length,
    system_charset_info);
  DBUG_VOID_RETURN;
}

void spider_store_xa_member_info(
  TABLE *table,
  XID *xid,
  SPIDER_CONN *conn
) {
  DBUG_ENTER("spider_store_xa_member_info");
  table->field[2]->store(xid->bqual_length);
  table->field[4]->store(
    conn->tgt_wrapper,
    (uint) conn->tgt_wrapper_length,
    system_charset_info);
  table->field[8]->store(
    conn->tgt_username,
    (uint) conn->tgt_username_length,
    system_charset_info);
  table->field[9]->store(
    conn->tgt_password,
    (uint) conn->tgt_password_length,
    system_charset_info);
  DBUG_VOID_RETURN;
}

void spider_store_tables_name(
  TABLE *table,
  const char *name,
  const uint name_length
) {
  const char *ptr_db, *ptr_table;
  my_ptrdiff_t ptr_diff_db, ptr_diff_table;
  DBUG_ENTER("spider_store_tables_name");
  if (name[0] == '.' && name[1] == '/')
  {
    ptr_db = strchr(name, '/');
    ptr_db++;
    ptr_diff_db = PTR_BYTE_DIFF(ptr_db, name);
    DBUG_PRINT("info",("spider ptr_diff_db = %ld", ptr_diff_db));
    ptr_table = strchr(ptr_db, '/');
    ptr_table++;
    ptr_diff_table = PTR_BYTE_DIFF(ptr_table, ptr_db);
    DBUG_PRINT("info",("spider ptr_diff_table = %ld", ptr_diff_table));
  } else {
    DBUG_PRINT("info",("spider temporary table"));
    ptr_db = "";
    ptr_diff_db = 1;
    ptr_table = "";
    ptr_diff_table = 1;
  }
  table->field[0]->store(
    ptr_db,
    ptr_diff_table - 1,
    system_charset_info);
  DBUG_PRINT("info",("spider field[0]->null_bit = %d",
    table->field[0]->null_bit));
  table->field[1]->store(
    ptr_table,
    name_length - ptr_diff_db - ptr_diff_table,
    system_charset_info);
  DBUG_PRINT("info",("spider field[1]->null_bit = %d",
    table->field[1]->null_bit));
  DBUG_VOID_RETURN;
}

void spider_store_tables_priority(
  TABLE *table,
  longlong priority
) {
  DBUG_ENTER("spider_store_tables_priority");
  DBUG_PRINT("info",("spider priority = %lld", priority));
  table->field[2]->store(priority, FALSE);
  DBUG_VOID_RETURN;
}

void spider_store_tables_connect_info(
  TABLE *table,
  SPIDER_ALTER_TABLE *alter_table
) {
  DBUG_ENTER("spider_store_tables_connect_info");
  if (alter_table->tmp_server_name)
  {
    table->field[3]->set_notnull();
    table->field[3]->store(
      alter_table->tmp_server_name,
      (uint) alter_table->tmp_server_name_length,
      system_charset_info);
  } else {
    table->field[3]->set_null();
    table->field[3]->reset();
  }
  if (alter_table->tmp_tgt_wrapper)
  {
    table->field[4]->set_notnull();
    table->field[4]->store(
      alter_table->tmp_tgt_wrapper,
      (uint) alter_table->tmp_tgt_wrapper_length,
      system_charset_info);
  } else {
    table->field[4]->set_null();
    table->field[4]->reset();
  }
  if (alter_table->tmp_tgt_host)
  {
    table->field[5]->set_notnull();
    table->field[5]->store(
      alter_table->tmp_tgt_host,
      (uint) alter_table->tmp_tgt_host_length,
      system_charset_info);
  } else {
    table->field[5]->set_null();
    table->field[5]->reset();
  }
  if (alter_table->tmp_tgt_port >= 0)
  {
    table->field[6]->set_notnull();
    table->field[6]->store(
      alter_table->tmp_tgt_port);
  } else {
    table->field[6]->set_null();
    table->field[6]->reset();
  }
  if (alter_table->tmp_tgt_socket)
  {
    table->field[7]->set_notnull();
    table->field[7]->store(
      alter_table->tmp_tgt_socket,
      (uint) alter_table->tmp_tgt_socket_length,
      system_charset_info);
  } else {
    table->field[7]->set_null();
    table->field[7]->reset();
  }
  if (alter_table->tmp_tgt_username)
  {
    table->field[8]->set_notnull();
    table->field[8]->store(
      alter_table->tmp_tgt_username,
      (uint) alter_table->tmp_tgt_username_length,
      system_charset_info);
  } else {
    table->field[8]->set_null();
    table->field[8]->reset();
  }
  if (alter_table->tmp_tgt_password)
  {
    table->field[9]->set_notnull();
    table->field[9]->store(
      alter_table->tmp_tgt_password,
      (uint) alter_table->tmp_tgt_password_length,
      system_charset_info);
  } else {
    table->field[9]->set_null();
    table->field[9]->reset();
  }
  if (alter_table->tmp_tgt_db)
  {
    table->field[10]->set_notnull();
    table->field[10]->store(
      alter_table->tmp_tgt_db,
      (uint) alter_table->tmp_tgt_db_length,
      system_charset_info);
  } else {
    table->field[10]->set_null();
    table->field[10]->reset();
  }
  if (alter_table->tmp_tgt_table_name)
  {
    table->field[11]->set_notnull();
    table->field[11]->store(
      alter_table->tmp_tgt_table_name,
      (uint) alter_table->tmp_tgt_table_name_length,
      system_charset_info);
  } else {
    table->field[11]->set_null();
    table->field[11]->reset();
  }
  DBUG_VOID_RETURN;
}

int spider_insert_xa(
  TABLE *table,
  XID *xid,
  const char *status
) {
  int error_num;
  char table_key[MAX_KEY_LENGTH];
  DBUG_ENTER("spider_insert_xa");
  table->use_all_columns();
  empty_record(table);
  spider_store_xa_pk(table, xid);

  if ((error_num = spider_check_sys_table(table, table_key)))
  {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
    table->use_all_columns();
    spider_store_xa_bqual_length(table, xid);
    spider_store_xa_status(table, status);
    if ((error_num = table->file->ha_write_row(table->record[0])))
    {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
  } else {
    my_message(ER_SPIDER_XA_EXISTS_NUM, ER_SPIDER_XA_EXISTS_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_XA_EXISTS_NUM);
  }

  DBUG_RETURN(0);
}

int spider_insert_xa_member(
  TABLE *table,
  XID *xid,
  SPIDER_CONN *conn
) {
  int error_num;
  char table_key[MAX_KEY_LENGTH];
  DBUG_ENTER("spider_insert_xa_member");
  table->use_all_columns();
  empty_record(table);
  spider_store_xa_member_pk(table, xid, conn);

  if ((error_num = spider_check_sys_table(table, table_key)))
  {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
    table->use_all_columns();
    spider_store_xa_member_info(table, xid, conn);
    if ((error_num = table->file->ha_write_row(table->record[0])))
    {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
  } else {
    my_message(ER_SPIDER_XA_MEMBER_EXISTS_NUM, ER_SPIDER_XA_MEMBER_EXISTS_STR,
      MYF(0));
    DBUG_RETURN(ER_SPIDER_XA_MEMBER_EXISTS_NUM);
  }

  DBUG_RETURN(0);
}

int spider_insert_tables(
  TABLE *table,
  SPIDER_SHARE *share
) {
  int error_num;
  DBUG_ENTER("spider_insert_tables");
  table->use_all_columns();
  empty_record(table);

  spider_store_tables_name(table, share->table_name, share->table_name_length);
  spider_store_tables_priority(table, share->priority);
  spider_store_tables_connect_info(table, &share->alter_table);
  if ((error_num = table->file->ha_write_row(table->record[0])))
  {
    table->file->print_error(error_num, MYF(0));
    DBUG_RETURN(error_num);
  }

  DBUG_RETURN(0);
}

int spider_update_xa(
  TABLE *table,
  XID *xid,
  const char *status
) {
  int error_num;
  char table_key[MAX_KEY_LENGTH];
  DBUG_ENTER("spider_update_xa");
  table->use_all_columns();
  spider_store_xa_pk(table, xid);

  if ((error_num = spider_check_sys_table(table, table_key)))
  {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
    my_message(ER_SPIDER_XA_NOT_EXISTS_NUM, ER_SPIDER_XA_NOT_EXISTS_STR,
      MYF(0));
    DBUG_RETURN(ER_SPIDER_XA_NOT_EXISTS_NUM);
  } else {
    store_record(table, record[1]);
    table->use_all_columns();
    spider_store_xa_status(table, status);
    if (
      (error_num = table->file->ha_update_row(
        table->record[1], table->record[0])) &&
      error_num != HA_ERR_RECORD_IS_THE_SAME
    ) {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
  }

  DBUG_RETURN(0);
}

int spider_update_tables_name(
  TABLE *table,
  const char *from,
  const char *to
) {
  int error_num;
  char table_key[MAX_KEY_LENGTH];
  DBUG_ENTER("spider_update_tables_name");
  table->use_all_columns();
  spider_store_tables_name(table, from, strlen(from));
  if ((error_num = spider_check_sys_table(table, table_key)))
  {
    table->file->print_error(error_num, MYF(0));
    DBUG_RETURN(error_num);
  } else {
    store_record(table, record[1]);
    table->use_all_columns();
    spider_store_tables_name(table, to, strlen(to));
    if (
      (error_num = table->file->ha_update_row(
        table->record[1], table->record[0])) &&
      error_num != HA_ERR_RECORD_IS_THE_SAME
    ) {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
  }

  DBUG_RETURN(0);
}

int spider_update_tables_priority(
  TABLE *table,
  SPIDER_ALTER_TABLE *alter_table,
  const char *name
) {
  int error_num;
  char table_key[MAX_KEY_LENGTH];
  DBUG_ENTER("spider_update_tables_name");
  table->use_all_columns();
  spider_store_tables_name(table, alter_table->table_name,
    alter_table->table_name_length);
  if ((error_num = spider_check_sys_table(table, table_key)))
  {
    table->file->print_error(error_num, MYF(0));
    DBUG_RETURN(error_num);
  } else {
    store_record(table, record[1]);
    table->use_all_columns();
    spider_store_tables_name(table, name, strlen(name));
    spider_store_tables_priority(table, alter_table->tmp_priority);
    spider_store_tables_connect_info(table, alter_table);
    if (
      (error_num = table->file->ha_update_row(
        table->record[1], table->record[0])) &&
      error_num != HA_ERR_RECORD_IS_THE_SAME
    ) {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
  }

  DBUG_RETURN(0);
}

int spider_delete_xa(
  TABLE *table,
  XID *xid
) {
  int error_num;
  char table_key[MAX_KEY_LENGTH];
  DBUG_ENTER("spider_delete_xa");
  table->use_all_columns();
  spider_store_xa_pk(table, xid);

  if ((error_num = spider_check_sys_table(table, table_key)))
  {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
    my_message(ER_SPIDER_XA_NOT_EXISTS_NUM, ER_SPIDER_XA_NOT_EXISTS_STR,
      MYF(0));
    DBUG_RETURN(ER_SPIDER_XA_NOT_EXISTS_NUM);
  } else {
    if ((error_num = table->file->ha_delete_row(table->record[0])))
    {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
  }

  DBUG_RETURN(0);
}

int spider_delete_xa_member(
  TABLE *table,
  XID *xid
) {
  int error_num;
  char table_key[MAX_KEY_LENGTH];
  DBUG_ENTER("spider_delete_xa_member");
  table->use_all_columns();
  spider_store_xa_pk(table, xid);

  if ((error_num = spider_get_sys_table_by_idx(table, table_key, 0,
    SPIDER_SYS_XA_PK_COL_CNT)))
  {
    if (error_num != HA_ERR_KEY_NOT_FOUND && error_num != HA_ERR_END_OF_FILE)
    {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
    DBUG_RETURN(0);
  } else {
    do {
      if ((error_num = table->file->ha_delete_row(table->record[0])))
      {
        spider_sys_index_end(table);
        table->file->print_error(error_num, MYF(0));
        DBUG_RETURN(error_num);
      }
      error_num = spider_sys_index_next_same(table, table_key);
    } while (error_num == 0);
  }
  if ((error_num = spider_sys_index_end(table)))
  {
    table->file->print_error(error_num, MYF(0));
    DBUG_RETURN(error_num);
  }

  DBUG_RETURN(0);
}

int spider_delete_tables(
  TABLE *table,
  const char *name
) {
  int error_num;
  char table_key[MAX_KEY_LENGTH];
  DBUG_ENTER("spider_delete_tables");
  table->use_all_columns();
  spider_store_tables_name(table, name, strlen(name));

  if ((error_num = spider_check_sys_table(table, table_key)))
    DBUG_RETURN(0);
  else {
    if ((error_num = table->file->ha_delete_row(table->record[0])))
    {
      table->file->print_error(error_num, MYF(0));
      DBUG_RETURN(error_num);
    }
  }

  DBUG_RETURN(0);
}

int spider_get_sys_xid(
  TABLE *table,
  XID *xid,
  MEM_ROOT *mem_root
) {
  char *ptr;
  DBUG_ENTER("spider_get_sys_xid");
  ptr = get_field(mem_root, table->field[0]);
  if (ptr)
  {
    xid->formatID = atoi(ptr);
  } else
    xid->formatID = 0;
  ptr = get_field(mem_root, table->field[1]);
  if (ptr)
  {
    xid->gtrid_length = atoi(ptr);
  } else
    xid->gtrid_length = 0;
  ptr = get_field(mem_root, table->field[2]);
  if (ptr)
  {
    xid->bqual_length = atoi(ptr);
  } else
    xid->bqual_length = 0;
  ptr = get_field(mem_root, table->field[3]);
  if (ptr)
  {
    strmov(xid->data, ptr);
  }
  DBUG_RETURN(0);
}

int spider_get_sys_server_info(
  TABLE *table,
  SPIDER_SHARE *share,
  MEM_ROOT *mem_root
) {
  char *ptr;
  DBUG_ENTER("spider_get_sys_server_info");
  if ((ptr = get_field(mem_root, table->field[4])))
  {
    share->tgt_wrapper_length = strlen(ptr);
    share->tgt_wrapper = spider_create_string(ptr, share->tgt_wrapper_length);
  } else {
    share->tgt_wrapper_length = 0;
    share->tgt_wrapper = NULL;
  }
  if ((ptr = get_field(mem_root, table->field[5])))
  {
    share->tgt_host_length = strlen(ptr);
    share->tgt_host = spider_create_string(ptr, share->tgt_host_length);
  } else {
    share->tgt_host_length = 0;
    share->tgt_host = NULL;
  }
  if ((ptr = get_field(mem_root, table->field[6])))
  {
    share->tgt_port = atol(ptr);
  } else
    share->tgt_port = MYSQL_PORT;
  if ((ptr = get_field(mem_root, table->field[7])))
  {
    share->tgt_socket_length = strlen(ptr);
    share->tgt_socket = spider_create_string(ptr, share->tgt_socket_length);
  } else {
    share->tgt_socket_length = 0;
    share->tgt_socket = NULL;
  }
  if ((ptr = get_field(mem_root, table->field[8])))
  {
    share->tgt_username_length = strlen(ptr);
    share->tgt_username =
      spider_create_string(ptr, share->tgt_username_length);
  } else {
    share->tgt_username_length = 0;
    share->tgt_username = NULL;
  }
  if ((ptr = get_field(mem_root, table->field[9])))
  {
    share->tgt_password_length = strlen(ptr);
    share->tgt_password =
      spider_create_string(ptr, share->tgt_password_length);
  } else {
    share->tgt_password_length = 0;
    share->tgt_password = NULL;
  }
  DBUG_RETURN(0);
}

int spider_check_sys_xa_status(
  TABLE *table,
  const char *status1,
  const char *status2,
  const char *status3,
  const int check_error_num,
  MEM_ROOT *mem_root
) {
  char *ptr;
  int error_num;
  DBUG_ENTER("spider_check_sys_xa_status");
  ptr = get_field(mem_root, table->field[4]);
  if (ptr)
  {
    if (
      strcmp(ptr, status1) &&
      (status2 == NULL || strcmp(ptr, status2)) &&
      (status3 == NULL || strcmp(ptr, status3))
    )
      error_num = check_error_num;
    else
      error_num = 0;
  } else
    error_num = check_error_num;
  DBUG_RETURN(error_num);
}

int spider_get_sys_tables(
  TABLE *table,
  char **db_name,
  char **table_name,
  MEM_ROOT *mem_root
) {
  char *ptr;
  DBUG_ENTER("spider_get_sys_tables");
  if ((ptr = get_field(mem_root, table->field[0])))
  {
    *db_name = spider_create_string(ptr, strlen(ptr));
  } else {
    *db_name = NULL;
  }
  if ((ptr = get_field(mem_root, table->field[1])))
  {
    *table_name = spider_create_string(ptr, strlen(ptr));
  } else {
    *table_name = NULL;
  }
  DBUG_RETURN(0);
}

int spider_get_sys_tables_connect_info(
  TABLE *table,
  SPIDER_SHARE *share,
  MEM_ROOT *mem_root
) {
  char *ptr;
  int error_num = 0;
  DBUG_ENTER("spider_get_sys_tables_connect_info");
  if ((ptr = get_field(mem_root, table->field[2])))
  {
    share->priority = my_strtoll10(ptr, (char**) NULL, &error_num);
  } else
    share->priority = 1000000;
  if (
    !table->field[3]->is_null() &&
    (ptr = get_field(mem_root, table->field[3]))
  ) {
    share->server_name_length = strlen(ptr);
    share->server_name = spider_create_string(ptr, share->server_name_length);
  } else {
    share->server_name_length = 0;
    share->server_name = NULL;
  }
  if (
    !table->field[4]->is_null() &&
    (ptr = get_field(mem_root, table->field[4]))
  ) {
    share->tgt_wrapper_length = strlen(ptr);
    share->tgt_wrapper = spider_create_string(ptr, share->tgt_wrapper_length);
  } else {
    share->tgt_wrapper_length = 0;
    share->tgt_wrapper = NULL;
  }
  if (
    !table->field[5]->is_null() &&
    (ptr = get_field(mem_root, table->field[5]))
  ) {
    share->tgt_host_length = strlen(ptr);
    share->tgt_host = spider_create_string(ptr, share->tgt_host_length);
  } else {
    share->tgt_host_length = 0;
    share->tgt_host = NULL;
  }
  if (
    !table->field[6]->is_null() &&
    (ptr = get_field(mem_root, table->field[6]))
  ) {
    share->tgt_port = atol(ptr);
  } else {
    share->tgt_port = -1;
  }
  if (
    !table->field[7]->is_null() &&
    (ptr = get_field(mem_root, table->field[7]))
  ) {
    share->tgt_socket_length = strlen(ptr);
    share->tgt_socket = spider_create_string(ptr, share->tgt_socket_length);
  } else {
    share->tgt_socket_length = 0;
    share->tgt_socket = NULL;
  }
  if (
    !table->field[8]->is_null() &&
    (ptr = get_field(mem_root, table->field[8]))
  ) {
    share->tgt_username_length = strlen(ptr);
    share->tgt_username =
      spider_create_string(ptr, share->tgt_username_length);
  } else {
    share->tgt_username_length = 0;
    share->tgt_username = NULL;
  }
  if (
    !table->field[9]->is_null() &&
    (ptr = get_field(mem_root, table->field[9]))
  ) {
    share->tgt_password_length = strlen(ptr);
    share->tgt_password =
      spider_create_string(ptr, share->tgt_password_length);
  } else {
    share->tgt_password_length = 0;
    share->tgt_password = NULL;
  }
  if (
    !table->field[10]->is_null() &&
    (ptr = get_field(mem_root, table->field[10]))
  ) {
    share->tgt_db_length = strlen(ptr);
    share->tgt_db =
      spider_create_string(ptr, share->tgt_db_length);
  } else {
    share->tgt_db_length = 0;
    share->tgt_db = NULL;
  }
  if (
    !table->field[11]->is_null() &&
    (ptr = get_field(mem_root, table->field[11]))
  ) {
    share->tgt_table_name_length = strlen(ptr);
    share->tgt_table_name =
      spider_create_string(ptr, share->tgt_table_name_length);
  } else {
    share->tgt_table_name_length = 0;
    share->tgt_table_name = NULL;
  }
  DBUG_RETURN(error_num);
}

int spider_sys_replace(
  TABLE *table,
  bool *modified_non_trans_table
) {
  int error_num, key_num;
  bool last_uniq_key;
  char table_key[MAX_KEY_LENGTH];
  DBUG_ENTER("spider_sys_replace");

  while ((error_num = table->file->ha_write_row(table->record[0])))
  {
    if (
      table->file->is_fatal_error(error_num, HA_CHECK_DUP) ||
      (key_num = table->file->get_dup_key(error_num)) < 0
    )
      goto error;

    if (table->file->ha_table_flags() & HA_DUPLICATE_POS)
    {
      error_num = table->file->rnd_pos(table->record[1], table->file->dup_ref);
      if (error_num)
      {
        if (error_num == HA_ERR_RECORD_DELETED)
          error_num = HA_ERR_KEY_NOT_FOUND;
        goto error;
      }
    } else {
      if ((error_num = table->file->extra(HA_EXTRA_FLUSH_CACHE)))
        goto error;

      key_copy((uchar*)table_key, table->record[0],
        table->key_info + key_num, 0);
      error_num = table->file->index_read_idx_map(table->record[1], key_num,
        (const uchar*)table_key, HA_WHOLE_KEY, HA_READ_KEY_EXACT);
      if (error_num)
      {
        if (error_num == HA_ERR_RECORD_DELETED)
          error_num = HA_ERR_KEY_NOT_FOUND;
        goto error;
      }
    }

    last_uniq_key = TRUE;
    while (++key_num < table->s->keys)
      if (table->key_info[key_num].flags & HA_NOSAME)
        last_uniq_key = FALSE;

    if (
      last_uniq_key &&
      !table->file->referenced_by_foreign_key()
    ) {
      error_num = table->file->ha_update_row(table->record[1],
        table->record[0]);
      if (error_num && error_num != HA_ERR_RECORD_IS_THE_SAME)
        goto error;
      DBUG_RETURN(0);
    } else {
      if ((error_num = table->file->ha_delete_row(table->record[1])))
        goto error;
      *modified_non_trans_table = TRUE;
    }
  }

  DBUG_RETURN(0);

error:
  DBUG_RETURN(error_num);
}
