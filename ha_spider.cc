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

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation
#endif

#define MYSQL_SERVER 1
#include "mysql_priv.h"
#include <mysql/plugin.h>
#include "spd_param.h"
#include "spd_err.h"
#include "spd_db_include.h"
#include "spd_include.h"
#include "ha_spider.h"
#include "spd_table.h"
#include "spd_sys_table.h"
#include "spd_trx.h"
#include "spd_conn.h"
#include "spd_db_conn.h"
#include "spd_ping_table.h"

#define SPIDER_CAN_BG_SEARCH (LL(1) << 37)
#define SPIDER_CAN_BG_INSERT (LL(1) << 38)
#define SPIDER_CAN_BG_UPDATE (LL(1) << 39)

extern handlerton *spider_hton_ptr;

ha_spider::ha_spider(
) : handler(spider_hton_ptr, NULL)
{
  DBUG_ENTER("ha_spider::ha_spider");
  DBUG_PRINT("info",("spider this=%x", this));
  share = NULL;
  trx = NULL;
  conns = NULL;
  need_mons = NULL;
  condition = NULL;
  blob_buff = NULL;
  conn_keys = NULL;
  spider_thread_id = 0;
  searched_bitmap = NULL;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  partition_handler_share = NULL;
  pt_handler_share_creator = NULL;
#endif
  result_list.sqls = NULL;
  result_list.insert_sqls = NULL;
  result_list.update_sqls = NULL;
  result_list.bgs_working = FALSE;
  DBUG_VOID_RETURN;
}

ha_spider::ha_spider(
  handlerton *hton,
  TABLE_SHARE *table_arg
) : handler(hton, table_arg)
{
  DBUG_ENTER("ha_spider::ha_spider");
  DBUG_PRINT("info",("spider this=%x", this));
  share = NULL;
  trx = NULL;
  conns = NULL;
  need_mons = NULL;
  condition = NULL;
  blob_buff = NULL;
  conn_keys = NULL;
  spider_thread_id = 0;
  searched_bitmap = NULL;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  partition_handler_share = NULL;
  pt_handler_share_creator = NULL;
#endif
  result_list.sqls = NULL;
  result_list.insert_sqls = NULL;
  result_list.update_sqls = NULL;
  result_list.bgs_working = FALSE;
  ref_length = sizeof(my_off_t);
  DBUG_VOID_RETURN;
}

static const char *ha_spider_exts[] = {
  NullS
};

const char **ha_spider::bas_ext() const
{
  return ha_spider_exts;
}

int ha_spider::open(
  const char* name,
  int mode,
  uint test_if_locked
) {
  THD *thd = ha_thd();
  int error_num, roop_count;
  int init_sql_alloc_size;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  SPIDER_PARTITION_SHARE *partition_share;
  uchar *idx_read_bitmap, *idx_write_bitmap,
    *rnd_read_bitmap, *rnd_write_bitmap;
  uint part_num;
  bool create_pt_handler_share = FALSE, pt_handler_mutex = FALSE;
#endif
  DBUG_ENTER("ha_spider::open");
  DBUG_PRINT("info",("spider this=%x", this));

  dup_key_idx = (uint) -1;
  if (!spider_get_share(name, table, thd, this, &error_num))
    goto error_get_share;
  thr_lock_data_init(&share->lock,&lock,NULL);

#ifdef WITH_PARTITION_STORAGE_ENGINE
  partition_share = share->partition_share;
  table->file->get_no_parts("", &part_num);
  if (partition_share)
  {
    pt_handler_mutex = TRUE;
    pthread_mutex_lock(&partition_share->pt_handler_mutex);
    if (
      !partition_share->partition_handler_share ||
      partition_share->partition_handler_share->table != table
    )
      create_pt_handler_share = TRUE;
  }

  if (create_pt_handler_share)
  {
    if (!(searched_bitmap = (uchar *)
      my_multi_malloc(MYF(MY_WME),
        &searched_bitmap, sizeof(uchar) * no_bytes_in_map(table->read_set),
        &partition_handler_share, sizeof(SPIDER_PARTITION_HANDLER_SHARE),
        &idx_read_bitmap, sizeof(uchar) * no_bytes_in_map(table->read_set),
        &idx_write_bitmap, sizeof(uchar) * no_bytes_in_map(table->read_set),
        &rnd_read_bitmap, sizeof(uchar) * no_bytes_in_map(table->read_set),
        &rnd_write_bitmap, sizeof(uchar) * no_bytes_in_map(table->read_set),
        NullS))
    ) {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_searched_bitmap_alloc;
    }
    DBUG_PRINT("info",("spider create partition_handler_share"));
    partition_handler_share->use_count = 1;
    if (partition_handler_share->use_count < part_num)
      partition_share->partition_handler_share = partition_handler_share;
    pthread_mutex_unlock(&partition_share->pt_handler_mutex);
    pt_handler_mutex = FALSE;
    DBUG_PRINT("info",("spider table=%x", table));
    partition_handler_share->table = table;
    partition_handler_share->searched_bitmap = NULL;
    partition_handler_share->idx_read_bitmap = idx_read_bitmap;
    partition_handler_share->idx_write_bitmap = idx_write_bitmap;
    partition_handler_share->rnd_read_bitmap = rnd_read_bitmap;
    partition_handler_share->rnd_write_bitmap = rnd_write_bitmap;
    partition_handler_share->between_flg = FALSE;
    partition_handler_share->idx_bitmap_is_set = FALSE;
    partition_handler_share->rnd_bitmap_is_set = FALSE;
    partition_handler_share->creator = this;
    pt_handler_share_creator = this;
  } else {
#endif
    if (!(searched_bitmap = (uchar *)
      my_multi_malloc(MYF(MY_WME),
        &searched_bitmap, sizeof(uchar) * no_bytes_in_map(table->read_set),
        NullS))
    ) {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_searched_bitmap_alloc;
    }
#ifdef WITH_PARTITION_STORAGE_ENGINE
    if (partition_share)
    {
      DBUG_PRINT("info",("spider copy partition_handler_share"));
      partition_handler_share = (SPIDER_PARTITION_HANDLER_SHARE *)
        partition_share->partition_handler_share;
      partition_handler_share->use_count++;
      if (partition_handler_share->use_count == part_num)
        partition_share->partition_handler_share = NULL;
      pthread_mutex_unlock(&partition_share->pt_handler_mutex);
      pt_handler_mutex = FALSE;
    }
  }
#endif

  init_sql_alloc_size =
    THDVAR(thd, init_sql_alloc_size) < 0 ?
    share->init_sql_alloc_size :
    THDVAR(thd, init_sql_alloc_size);

  result_list.table = table;
  result_list.first = NULL;
  result_list.last = NULL;
  result_list.current = NULL;
  result_list.record_num = 0;
  if (
    (result_list.sql.real_alloc(init_sql_alloc_size)) ||
    (result_list.insert_sql.real_alloc(init_sql_alloc_size)) ||
    (result_list.update_sql.real_alloc(init_sql_alloc_size)) ||
    !(result_list.sqls = new String[share->link_count]) ||
    !(result_list.insert_sqls = new String[share->link_count]) ||
    !(result_list.update_sqls = new String[share->link_count])
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error_init_result_list;
  }
  if (!share->same_db_table_name)
  {
    for (roop_count = 0; roop_count < share->link_count; roop_count++)
    {
      if (
        result_list.sqls[roop_count].real_alloc(init_sql_alloc_size) ||
        result_list.insert_sqls[roop_count].real_alloc(init_sql_alloc_size) ||
        result_list.update_sqls[roop_count].real_alloc(init_sql_alloc_size)
      ) {
        error_num = HA_ERR_OUT_OF_MEM;
        goto error_init_result_list;
      }
      result_list.sqls[roop_count].set_charset(share->access_charset);
      result_list.insert_sqls[roop_count].set_charset(share->access_charset);
      result_list.update_sqls[roop_count].set_charset(share->access_charset);
    }
  }
  result_list.sql.set_charset(share->access_charset);
  result_list.insert_sql.set_charset(share->access_charset);
  result_list.update_sql.set_charset(share->access_charset);

  DBUG_PRINT("info",("spider blob_fields=%d", table_share->blob_fields));
  if (table_share->blob_fields)
  {
    if (!(blob_buff = new String[table_share->fields]))
    {
      error_num = HA_ERR_OUT_OF_MEM;
      goto error_init_blob_buff;
    }
    for (roop_count = 0; roop_count < table_share->fields; roop_count++)
      blob_buff[roop_count].set_charset(table->field[roop_count]->charset());
  }

  if (reset())
  {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error;
  }

  DBUG_RETURN(0);

error:
  delete [] blob_buff;
  blob_buff = NULL;
error_init_blob_buff:
error_init_result_list:
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (
    partition_handler_share &&
    pt_handler_share_creator == this
  ) {
    partition_share = share->partition_share;
    if (!pt_handler_mutex)
      pthread_mutex_lock(&partition_share->pt_handler_mutex);
    if (partition_share->partition_handler_share == partition_handler_share)
      partition_share->partition_handler_share = NULL;
    pthread_mutex_unlock(&partition_share->pt_handler_mutex);
    pt_handler_mutex = FALSE;
  }
  partition_handler_share = NULL;
  pt_handler_share_creator = NULL;
#endif
  if (searched_bitmap)
  {
    my_free(searched_bitmap, MYF(0));
    searched_bitmap = NULL;
  }
error_searched_bitmap_alloc:
  if (pt_handler_mutex)
    pthread_mutex_unlock(&partition_share->pt_handler_mutex);
  spider_free_share(share);
  share = NULL;
error_get_share:
  if (conn_keys)
  {
    my_free(conn_keys, MYF(0));
    conn_keys = NULL;
  }
  DBUG_RETURN(error_num);
}

int ha_spider::close()
{
  int error_num = 0, roop_count;
  THD *thd = ha_thd();
#ifdef WITH_PARTITION_STORAGE_ENGINE
  SPIDER_PARTITION_SHARE *partition_share;
#endif
  DBUG_ENTER("ha_spider::close");
  DBUG_PRINT("info",("spider this=%x", this));
  if (!thd || !thd_get_ha_data(thd, spider_hton_ptr))
  {
    for (roop_count = 0; roop_count < share->link_count; roop_count++)
      conns[roop_count] = NULL;
  }

  spider_db_free_result(this, TRUE);
  if (conn_keys)
  {
    my_free(conn_keys, MYF(0));
    conn_keys = NULL;
  }
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (
    partition_handler_share &&
    pt_handler_share_creator == this
  ) {
    partition_share = share->partition_share;
    pthread_mutex_lock(&partition_share->pt_handler_mutex);
    if (partition_share->partition_handler_share == partition_handler_share)
      partition_share->partition_handler_share = NULL;
    pthread_mutex_unlock(&partition_share->pt_handler_mutex);
  }
  partition_handler_share = NULL;
  pt_handler_share_creator = NULL;
#endif
  if (searched_bitmap)
  {
    my_free(searched_bitmap, MYF(0));
    searched_bitmap = NULL;
  }
  if (blob_buff)
  {
    delete [] blob_buff;
    blob_buff = NULL;
  }
  if (result_list.sqls)
  {
    delete [] result_list.sqls;
    result_list.sqls = NULL;
  }
  if (result_list.insert_sqls)
  {
    delete [] result_list.insert_sqls;
    result_list.insert_sqls = NULL;
  }
  if (result_list.update_sqls)
  {
    delete [] result_list.update_sqls;
    result_list.update_sqls = NULL;
  }

  spider_free_share(share);
  share = NULL;
  trx = NULL;
  conns = NULL;

  DBUG_RETURN(error_num);
}

THR_LOCK_DATA **ha_spider::store_lock(
  THD *thd,
  THR_LOCK_DATA **to,
  enum thr_lock_type lock_type
) {
  int error_num, roop_count;
  ha_spider *spider;
  DBUG_ENTER("ha_spider::store_lock");
  DBUG_PRINT("info",("spider this=%x", this));
  if ((error_num = spider_check_trx_and_get_conn(thd, this)))
  {
    store_error_num = error_num;
    DBUG_RETURN(to);
  }
  sql_command = thd_sql_command(thd);
  DBUG_PRINT("info",("spider sql_command=%u", sql_command));
  DBUG_PRINT("info",("spider lock_type=%d", lock_type));
  DBUG_PRINT("info",("spider thd->query_id=%ld", thd->query_id));
  if (sql_command == SQLCOM_ALTER_TABLE)
  {
    if (trx->query_id != thd->query_id)
    {
      spider_free_trx_alter_table(trx);
      trx->query_id = thd->query_id;
      trx->tmp_flg = FALSE;
    }
    if (!(SPIDER_ALTER_TABLE*) hash_search(&trx->trx_alter_table_hash,
      (uchar*) share->table_name, share->table_name_length))
    {
      if (spider_create_trx_alter_table(trx, share, FALSE))
      {
        store_error_num = HA_ERR_OUT_OF_MEM;
        DBUG_RETURN(to);
      }
    }
  }

  this->lock_type = lock_type;
  selupd_lock_mode = THDVAR(thd, selupd_lock_mode) == -1 ?
    share->selupd_lock_mode : THDVAR(thd, selupd_lock_mode);
  SPIDER_SET_CONNS_PARAM(semi_trx_chk, FALSE, conns, share->link_statuses,
    share->link_count, SPIDER_LINK_STATUS_RECOVERY);
  update_request = FALSE;
  switch (sql_command)
  {
    case SQLCOM_SELECT:
      if (lock_type == TL_READ_WITH_SHARED_LOCKS)
        lock_mode = 1;
      else if (lock_type <= TL_READ_NO_INSERT)
      {
        lock_mode = 0;
        SPIDER_SET_CONNS_PARAM(semi_trx_isolation_chk, TRUE, conns,
          share->link_statuses, share->link_count,
          SPIDER_LINK_STATUS_RECOVERY);
      } else
        lock_mode = -1;
      SPIDER_SET_CONNS_PARAM(semi_trx_chk, TRUE, conns, share->link_statuses,
        share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      break;
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
      update_request = TRUE;
    case SQLCOM_CREATE_TABLE:
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_DELETE:
    case SQLCOM_LOAD:
    case SQLCOM_REPLACE:
    case SQLCOM_REPLACE_SELECT:
    case SQLCOM_DELETE_MULTI:
      if (lock_type >= TL_READ && lock_type <= TL_READ_NO_INSERT)
      {
        lock_mode = selupd_lock_mode;
        SPIDER_SET_CONNS_PARAM(semi_trx_isolation_chk, TRUE, conns,
          share->link_statuses, share->link_count,
          SPIDER_LINK_STATUS_RECOVERY);
      } else
        lock_mode = -1;
      SPIDER_SET_CONNS_PARAM(semi_trx_chk, TRUE, conns, share->link_statuses,
        share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      break;
    default:
        lock_mode = -1;
  }
  switch (lock_type)
  {
    case TL_READ_HIGH_PRIORITY:
      high_priority = TRUE;
      break;
    case TL_WRITE_DELAYED:
      insert_delayed = TRUE;
      break;
    case TL_WRITE_LOW_PRIORITY:
      low_priority = TRUE;
  }

  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
  {
    if (sql_command == SQLCOM_LOCK_TABLES ||
      (THDVAR(thd, lock_exchange) == 1 && share->semi_table_lock))
    {
      if (
        (
          this->lock_type == TL_READ ||
          this->lock_type == TL_READ_NO_INSERT ||
          this->lock_type == TL_WRITE_LOW_PRIORITY ||
          this->lock_type == TL_WRITE
        ) &&
        THDVAR(thd, local_lock_table) == 0
      ) {
        for (
          roop_count = spider_conn_link_idx_next(share->link_statuses,
            -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
          roop_count < share->link_count;
          roop_count = spider_conn_link_idx_next(share->link_statuses,
            roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
        ) {
          if (!(spider = (ha_spider*) hash_search(
            &conns[roop_count]->lock_table_hash,
            (uchar*) share->table_name, share->table_name_length)))
          {
            if (my_hash_insert(&conns[roop_count]->lock_table_hash,
              (uchar*) this))
            {
              store_error_num = HA_ERR_OUT_OF_MEM;
              DBUG_RETURN(to);
            }
            conns[roop_count]->table_lock = 2;
          } else {
            if (spider->lock_type < this->lock_type)
            {
              hash_delete(&conns[roop_count]->lock_table_hash,
                (uchar*) spider);
              if (my_hash_insert(&conns[roop_count]->lock_table_hash,
                (uchar*) this))
              {
                store_error_num = HA_ERR_OUT_OF_MEM;
                DBUG_RETURN(to);
              }
            }
          }
        }
      }
    } else {
      if (
        this->lock_type == TL_READ ||
        this->lock_type == TL_READ_NO_INSERT ||
        this->lock_type == TL_WRITE_LOW_PRIORITY ||
        this->lock_type == TL_WRITE
      ) {
        for (
          roop_count = spider_conn_link_idx_next(share->link_statuses,
            -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
          roop_count < share->link_count;
          roop_count = spider_conn_link_idx_next(share->link_statuses,
            roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
        ) {
          if (
            conns[roop_count]->table_lock != 1 &&
            THDVAR(thd, semi_table_lock) == 1 &&
            share->semi_table_lock &&
            THDVAR(thd, local_lock_table) == 0
          ) {
            if (!(spider = (ha_spider*) hash_search(
              &conns[roop_count]->lock_table_hash,
              (uchar*) share->table_name, share->table_name_length)))
            {
              if (my_hash_insert(&conns[roop_count]->lock_table_hash,
                (uchar*) this))
              {
                store_error_num = HA_ERR_OUT_OF_MEM;
                DBUG_RETURN(to);
              }
              conns[roop_count]->table_lock = 3;
            } else {
              if (spider->lock_type < this->lock_type)
              {
                hash_delete(&conns[roop_count]->lock_table_hash,
                  (uchar*) spider);
                if (my_hash_insert(&conns[roop_count]->lock_table_hash,
                  (uchar*) this))
                {
                  store_error_num = HA_ERR_OUT_OF_MEM;
                  DBUG_RETURN(to);
                }
              }
            }
          }
        }
      }
      if (
        ((lock_type >= TL_READ && lock_type <= TL_READ_NO_INSERT) ||
        (lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE)) &&
        !thd->in_lock_tables
      )
        lock_type = TL_WRITE_ALLOW_WRITE;
    }
    lock.type = lock_type;
  }
  *to ++= &lock;
  DBUG_RETURN(to);
}

int ha_spider::external_lock(
  THD *thd,
  int lock_type
) {
  int error_num, roop_count;
  bool sync_trx_isolation = THDVAR(thd, sync_trx_isolation);
  DBUG_ENTER("ha_spider::external_lock");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider lock_type=%x", lock_type));
  DBUG_PRINT("info",("spider thd->options=%x", (int) thd->options));

  sql_command = thd_sql_command(thd);
  if (sql_command == SQLCOM_BEGIN)
    sql_command = SQLCOM_UNLOCK_TABLES;
  if (
    sql_command == SQLCOM_UNLOCK_TABLES &&
    (error_num = spider_check_trx_and_get_conn(thd, this))
  ) {
    DBUG_RETURN(error_num);
  }

  DBUG_PRINT("info",("spider sql_command=%d", sql_command));
  DBUG_ASSERT(trx == spider_get_trx(thd, &error_num));
  if (store_error_num)
    DBUG_RETURN(store_error_num);
  if (
    lock_type == F_UNLCK &&
    sql_command != SQLCOM_UNLOCK_TABLES
  )
    DBUG_RETURN(0);
  if (
    trx->locked_connections &&
    /* SQLCOM_RENAME_TABLE and SQLCOM_DROP_DB don't come here */
    (
      sql_command == SQLCOM_DROP_TABLE ||
      sql_command == SQLCOM_ALTER_TABLE
    )
  ) {
    my_message(ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM,
      ER_SPIDER_ALTER_BEFORE_UNLOCK_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM);
  }
  if (!conns[search_link_idx])
  {
    my_message(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM,
      ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM);
  }
  for (
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_count < share->link_count;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
  ) {
    if (sql_command == SQLCOM_TRUNCATE)
      DBUG_RETURN(0);
    else if (sql_command != SQLCOM_UNLOCK_TABLES)
    {
      if (
        (!conns[roop_count]->join_trx &&
          (error_num = spider_internal_start_trx(this, conns[roop_count],
            roop_count)))
      ) {
        if (
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      result_list.lock_type = lock_type;
      if (
        conns[roop_count]->semi_trx_isolation == -2 &&
        conns[roop_count]->semi_trx_isolation_chk == TRUE &&
        sync_trx_isolation &&
        THDVAR(trx->thd, semi_trx_isolation) >= 0
      ) {
        if (
          conns[roop_count]->trx_isolation !=
            THDVAR(trx->thd, semi_trx_isolation) &&
          (error_num = spider_db_set_trx_isolation(
            conns[roop_count], THDVAR(trx->thd, semi_trx_isolation),
            &need_mons[roop_count]))
        ) {
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        conns[roop_count]->semi_trx_isolation =
          THDVAR(trx->thd, semi_trx_isolation);
        conns[roop_count]->trx_isolation =
          thd_tx_isolation(conns[roop_count]->thd);
      } else {
        if (sync_trx_isolation)
        {
          if ((error_num = spider_check_and_set_trx_isolation(
            conns[roop_count], &need_mons[roop_count])))
          {
            if (
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
            DBUG_RETURN(error_num);
          }
        }
        conns[roop_count]->semi_trx_isolation = -1;
      }
    }
    if (conns[roop_count]->table_lock >= 2)
    {
      if (
        conns[roop_count]->lock_table_hash.records &&
        (error_num = spider_db_lock_tables(this, roop_count))
      ) {
        if (
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        conns[roop_count]->table_lock = 0;
        DBUG_RETURN(error_num);
      }
      if (conns[roop_count]->table_lock == 2)
        conns[roop_count]->table_lock = 1;
    } else if (sql_command == SQLCOM_UNLOCK_TABLES ||
      THDVAR(thd, internal_unlock) == 1)
    {
      if (conns[roop_count]->table_lock == 1)
      {
        conns[roop_count]->table_lock = 0;
        if (!conns[roop_count]->trx_start)
          conns[roop_count]->disable_reconnect = FALSE;
        if ((error_num = spider_db_unlock_tables(this, roop_count)))
        {
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      }
    }
  }
  DBUG_RETURN(0);
}

int ha_spider::reset()
{
  int error_num, roop_count;
  THD *thd = ha_thd();
  SPIDER_TRX *tmp_trx, *trx_bak;
  SPIDER_CONDITION *tmp_cond;
  char first_byte, first_byte_bak;
  DBUG_ENTER("ha_spider::reset");
  DBUG_PRINT("info",("spider this=%x", this));
  store_error_num = 0;
#ifdef WITH_PARTITION_STORAGE_ENGINE
  if (
    partition_handler_share &&
    partition_handler_share->searched_bitmap
  ) {
    partition_handler_share->searched_bitmap = NULL;
    partition_handler_share->between_flg = FALSE;
    partition_handler_share->idx_bitmap_is_set = FALSE;
    partition_handler_share->rnd_bitmap_is_set = FALSE;
  }
#endif
  if (!(tmp_trx = spider_get_trx(thd, &error_num)))
  {
    DBUG_PRINT("info",("spider get trx error"));
    DBUG_RETURN(error_num);
  }
  if (share)
  {
/*
    int semi_table_lock_conn = THDVAR(thd, semi_table_lock_connection) == -1 ?
      share->semi_table_lock_conn : THDVAR(thd, semi_table_lock_connection);
    if (semi_table_lock_conn)
      first_byte = '0' +
        (share->semi_table_lock & THDVAR(thd, semi_table_lock));
    else
      first_byte = '0';
    DBUG_PRINT("info",("spider semi_table_lock_conn = %d",
      semi_table_lock_conn));
    DBUG_PRINT("info",("spider semi_table_lock = %d",
      (share->semi_table_lock & THDVAR(thd, semi_table_lock))));
    DBUG_PRINT("info",("spider first_byte = %d", first_byte));
    if (tmp_trx->spider_thread_id != spider_thread_id ||
      (tmp_trx->trx_conn_adjustment != trx_conn_adjustment &&
        tmp_trx->trx_conn_adjustment - 1 != trx_conn_adjustment) ||
      first_byte != *conn_keys[0]
    ) {
      DBUG_PRINT("info",(first_byte != *conn_keys[0] ?
        "spider change conn type" : tmp_trx != trx ? "spider change thd" :
        "spider next trx"));
      trx = tmp_trx;
      spider_thread_id = tmp_trx->spider_thread_id;
      trx_conn_adjustment = tmp_trx->trx_conn_adjustment;

      first_byte_bak = *conn_keys[0];
      *conn_keys[0] = first_byte;
      for (
        roop_count = spider_conn_link_idx_next(share->link_statuses,
          -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
        roop_count < share->link_count;
        roop_count = spider_conn_link_idx_next(share->link_statuses,
          roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
      ) {
        *conn_keys[roop_count] = first_byte;
        if (
          !(conns[roop_count] =
            spider_get_conn(share, roop_count, conn_keys[roop_count], trx,
              this, FALSE, TRUE, &error_num))
        ) {
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_PRINT("info",("spider get conn error"));
          *conn_keys[0] = first_byte_bak;
          conns[0] = NULL;
          DBUG_RETURN(error_num);
        }
      }
    }
*/
    memset(need_mons, 0, sizeof(int) * share->link_count);
    TABLE **tmp_table = result_list.upd_tmp_tbls;
    for (roop_count = share->link_count - 1; roop_count >= 0; roop_count--)
    {
      if (tmp_table[roop_count])
      {
        spider_rm_sys_tmp_table(thd, tmp_table[roop_count],
          &result_list.upd_tmp_tbl_prms[roop_count]);
        tmp_table[roop_count] = NULL;
      }
    }
    if (result_list.upd_tmp_tbl)
    {
      spider_rm_sys_tmp_table(thd, result_list.upd_tmp_tbl,
        &result_list.upd_tmp_tbl_prm);
      result_list.upd_tmp_tbl = NULL;
    }
    result_list.bulk_update_mode = 0;
    result_list.bulk_update_size = 0;
    result_list.bulk_update_start = SPD_BU_NOT_START;
  }
  quick_mode = FALSE;
  keyread = FALSE;
  ignore_dup_key = FALSE;
  write_can_replace = FALSE;
  insert_with_update = FALSE;
  low_priority = FALSE;
  high_priority = FALSE;
  insert_delayed = FALSE;
  bulk_insert = FALSE;
  while (condition)
  {
    tmp_cond = condition->next;
    my_free(condition, MYF(0));
    condition = tmp_cond;
  }
  if (share)
  {
    trx_bak = trx;
    trx = tmp_trx;
    if ((error_num = spider_db_free_result(this, FALSE)))
      DBUG_RETURN(error_num);
    trx = trx_bak;
  }
  DBUG_RETURN(0);
}

int ha_spider::extra(
  enum ha_extra_function operation
) {
  DBUG_ENTER("ha_spider::extra");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider operation=%d", (int) operation));
  switch (operation)
  {
    case HA_EXTRA_QUICK:
      quick_mode = TRUE;
      break;
    case HA_EXTRA_KEYREAD:
      keyread = TRUE;
      break;
    case HA_EXTRA_NO_KEYREAD:
      keyread = FALSE;
      break;
    case HA_EXTRA_IGNORE_DUP_KEY:
      ignore_dup_key = TRUE;
      break;
    case HA_EXTRA_NO_IGNORE_DUP_KEY:
      ignore_dup_key = FALSE;
      break;
    case HA_EXTRA_WRITE_CAN_REPLACE:
      write_can_replace = TRUE;
      break;
    case HA_EXTRA_WRITE_CANNOT_REPLACE:
      write_can_replace = FALSE;
      break;
    case HA_EXTRA_INSERT_WITH_UPDATE:
      insert_with_update = TRUE;
      break;
  }
  DBUG_RETURN(0);
}

int ha_spider::index_init(
  uint idx,
  bool sorted
) {
  int error_num, roop_count, lock_mode;
  DBUG_ENTER("ha_spider::index_init");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider idx=%u", idx));
  lock_mode = spider_conn_lock_mode(this);
  active_index = idx;
  result_list.sorted = sorted;
  spider_set_result_list_param(this);

  if (result_list.lock_type == F_WRLCK)
  {
    check_and_start_bulk_update(SPD_BU_START_BY_INDEX_OR_RND_INIT);

    if (
      update_request &&
      share->have_recovery_link &&
      (pk_update = spider_check_pk_update(table))
    ) {
      bitmap_set_all(table->read_set);
    }
  }

  result_list.sql.length(0);

  set_select_column_mode();
  DBUG_RETURN(0);
}

int ha_spider::index_end()
{
  DBUG_ENTER("ha_spider::index_end");
  DBUG_PRINT("info",("spider this=%x", this));
  active_index = MAX_KEY;
  DBUG_RETURN(check_and_end_bulk_update(SPD_BU_START_BY_INDEX_OR_RND_INIT));
}

int ha_spider::index_read_map(
  uchar *buf,
  const uchar *key,
  key_part_map keypart_map,
  enum ha_rkey_function find_flag
) {
  int error_num, roop_count, tmp_pos, lock_mode;
  key_range start_key;
  String *sql;
  DBUG_ENTER("ha_spider::index_read_map");
  DBUG_PRINT("info",("spider this=%x", this));
  lock_mode = spider_conn_lock_mode(this);
  spider_set_result_list_param(this);
  start_key.key = key;
  start_key.keypart_map = keypart_map;
  start_key.flag = find_flag;
  result_list.sql.length(0);
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if ((error_num = spider_set_conn_bg_param(this)))
    DBUG_RETURN(error_num);
#endif
#ifdef WITH_PARTITION_STORAGE_ENGINE
  check_select_column(FALSE);
#endif
  result_list.finish_flg = FALSE;
  result_list.record_num = 0;
  spider_db_free_one_result_for_start_next(this);
  if (keyread)
    result_list.keyread = TRUE;
  else
    result_list.keyread = FALSE;
  if (
    (error_num = spider_db_append_select(this)) ||
    (error_num = spider_db_append_select_columns(this))
  )
    DBUG_RETURN(error_num);
  if (
    share->key_hint &&
    (error_num = spider_db_append_hint_after_table(
      &result_list.sql, &share->key_hint[active_index]))
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  result_list.where_pos = result_list.sql.length();
  result_list.desc_flg = FALSE;
  result_list.sorted = TRUE;
  result_list.key_info = &table->key_info[active_index];
  result_list.limit_num =
    result_list.internal_limit >= result_list.split_read ?
    result_list.split_read : result_list.internal_limit;
  if (
    (error_num = spider_db_append_key_where(
      &start_key, NULL, this)) ||
    (error_num = spider_db_append_key_order(this)) ||
    (error_num = spider_db_append_limit(
      &result_list.sql, result_list.internal_offset,
      result_list.limit_num)) ||
    (error_num = spider_db_append_select_lock(this))
  )
    DBUG_RETURN(error_num);

  int roop_start, roop_end, tmp_lock_mode, link_ok;
  tmp_lock_mode = spider_conn_lock_mode(this);
  if (tmp_lock_mode)
  {
    /* "for update" or "lock in share mode" */
    link_ok = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_OK);
    roop_start = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_end = share->link_count;
  } else {
    link_ok = search_link_idx;
    roop_start = search_link_idx;
    roop_end = search_link_idx + 1;
  }
  for (roop_count = roop_start; roop_count < roop_end;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
  ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (result_list.bgs_phase > 0)
    {
      if ((error_num = spider_bg_conn_search(this, roop_count, TRUE,
        (roop_count != link_ok))))
      {
        if (
          error_num != HA_ERR_END_OF_FILE &&
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
    } else {
#endif
      if (share->same_db_table_name)
        sql = &result_list.sql;
      else {
        sql = &result_list.sqls[roop_count];
        if (sql->copy(result_list.sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_pos = sql->length();
        sql->length(result_list.table_name_pos);
        spider_db_append_table_name_with_adjusting(sql, share, roop_count);
        sql->length(tmp_pos);
      }
      pthread_mutex_lock(&conns[roop_count]->mta_conn_mutex);
      conns[roop_count]->need_mon = &need_mons[roop_count];
      conns[roop_count]->mta_conn_mutex_lock_already = TRUE;
      conns[roop_count]->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(this, conns[roop_count],
        roop_count)))
      {
        conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
        conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      if (spider_db_query(
        conns[roop_count],
        sql->ptr(),
        sql->length(),
        &need_mons[roop_count])
      ) {
        conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
        conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conns[roop_count]);
        if (
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(spider_db_errorno(conns[roop_count]));
      }
      conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
      conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
      if (roop_count == link_ok)
      {
        if ((error_num = spider_db_store_result(this, roop_count, table)))
        {
          if (
            error_num != HA_ERR_END_OF_FILE &&
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      } else {
        spider_db_discard_result(conns[roop_count]);
        pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
      }
#ifndef WITHOUT_SPIDER_BG_SEARCH
    }
#endif
  }
  DBUG_RETURN(spider_db_fetch(buf, this, table));
}

int ha_spider::index_read_last_map(
  uchar *buf,
  const uchar *key,
  key_part_map keypart_map
) {
  int error_num, tmp_pos;
  key_range start_key;
  String *sql;
  DBUG_ENTER("ha_spider::index_read_last_map");
  DBUG_PRINT("info",("spider this=%x", this));
  if (
    result_list.current &&
    (error_num = spider_db_free_result(this, FALSE))
  )
    DBUG_RETURN(error_num);

  start_key.key = key;
  start_key.keypart_map = keypart_map;
  start_key.flag = HA_READ_KEY_EXACT;
  result_list.sql.length(0);
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if ((error_num = spider_set_conn_bg_param(this)))
    DBUG_RETURN(error_num);
#endif
#ifdef WITH_PARTITION_STORAGE_ENGINE
  check_select_column(FALSE);
#endif
  result_list.finish_flg = FALSE;
  result_list.record_num = 0;
  spider_db_free_one_result_for_start_next(this);
  if (keyread)
    result_list.keyread = TRUE;
  else
    result_list.keyread = FALSE;
  if (
    (error_num = spider_db_append_select(this)) ||
    (error_num = spider_db_append_select_columns(this))
  )
    DBUG_RETURN(error_num);
  if (
    share->key_hint &&
    (error_num = spider_db_append_hint_after_table(
      &result_list.sql, &share->key_hint[active_index]))
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  result_list.where_pos = result_list.sql.length();
  result_list.desc_flg = TRUE;
  result_list.sorted = TRUE;
  result_list.key_info = &table->key_info[active_index];
  result_list.limit_num =
    result_list.internal_limit >= result_list.split_read ?
    result_list.split_read : result_list.internal_limit;
  if (
    (error_num = spider_db_append_key_where(
      &start_key, NULL, this)) ||
    (error_num = spider_db_append_key_order(this)) ||
    (error_num = spider_db_append_limit(
      &result_list.sql, result_list.internal_offset,
      result_list.limit_num)) ||
    (error_num = spider_db_append_select_lock(this))
  )
    DBUG_RETURN(error_num);

  int roop_start, roop_end, roop_count, tmp_lock_mode, link_ok;
  tmp_lock_mode = spider_conn_lock_mode(this);
  if (tmp_lock_mode)
  {
    /* "for update" or "lock in share mode" */
    link_ok = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_OK);
    roop_start = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_end = share->link_count;
  } else {
    link_ok = search_link_idx;
    roop_start = search_link_idx;
    roop_end = search_link_idx + 1;
  }
  for (roop_count = roop_start; roop_count < roop_end;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
  ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (result_list.bgs_phase > 0)
    {
      if ((error_num = spider_bg_conn_search(this, roop_count, TRUE,
        (roop_count != link_ok))))
      {
        if (
          error_num != HA_ERR_END_OF_FILE &&
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
    } else {
#endif
      if (share->same_db_table_name)
        sql = &result_list.sql;
      else {
        sql = &result_list.sqls[roop_count];
        if (sql->copy(result_list.sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_pos = sql->length();
        sql->length(result_list.table_name_pos);
        spider_db_append_table_name_with_adjusting(sql, share, roop_count);
        sql->length(tmp_pos);
      }
      pthread_mutex_lock(&conns[roop_count]->mta_conn_mutex);
      conns[roop_count]->need_mon = &need_mons[roop_count];
      conns[roop_count]->mta_conn_mutex_lock_already = TRUE;
      conns[roop_count]->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(this, conns[roop_count],
        roop_count)))
      {
        conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
        conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      if (spider_db_query(
        conns[roop_count],
        sql->ptr(),
        sql->length(),
        &need_mons[roop_count])
      ) {
        conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
        conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conns[roop_count]);
        if (
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
      conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
      if (roop_count == link_ok)
      {
        if ((error_num = spider_db_store_result(this, roop_count, table)))
        {
          if (
            error_num != HA_ERR_END_OF_FILE &&
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      } else {
        spider_db_discard_result(conns[roop_count]);
        pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
      }
#ifndef WITHOUT_SPIDER_BG_SEARCH
    }
#endif
  }
  DBUG_RETURN(spider_db_fetch(buf, this, table));
}

int ha_spider::index_next(
  uchar *buf
) {
  DBUG_ENTER("ha_spider::index_next");
  DBUG_PRINT("info",("spider this=%x", this));
  if (
    result_list.sorted &&
    result_list.desc_flg
  )
    DBUG_RETURN(spider_db_seek_prev(buf, this, table));
  DBUG_RETURN(spider_db_seek_next(buf, this, search_link_idx, table));
}

int ha_spider::index_prev(
  uchar *buf
) {
  DBUG_ENTER("ha_spider::index_prev");
  DBUG_PRINT("info",("spider this=%x", this));
  if (
    result_list.sorted &&
    result_list.desc_flg
  )
    DBUG_RETURN(spider_db_seek_next(buf, this, search_link_idx, table));
  DBUG_RETURN(spider_db_seek_prev(buf, this, table));
}

int ha_spider::index_first(
  uchar *buf
) {
  int error_num, tmp_pos;
  String *sql;
  DBUG_ENTER("ha_spider::index_first");
  DBUG_PRINT("info",("spider this=%x", this));
  if (!result_list.sql.length())
  {
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if ((error_num = spider_set_conn_bg_param(this)))
      DBUG_RETURN(error_num);
#endif
#ifdef WITH_PARTITION_STORAGE_ENGINE
    check_select_column(FALSE);
#endif
    result_list.finish_flg = FALSE;
    result_list.record_num = 0;
    spider_db_free_one_result_for_start_next(this);
    if (keyread)
      result_list.keyread = TRUE;
    else
      result_list.keyread = FALSE;
    if (
      (error_num = spider_db_append_select(this)) ||
      (error_num = spider_db_append_select_columns(this))
    )
      DBUG_RETURN(error_num);
    if (
      share->key_hint &&
      (error_num = spider_db_append_hint_after_table(
        &result_list.sql, &share->key_hint[active_index]))
    )
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    result_list.where_pos = result_list.sql.length();
    result_list.desc_flg = FALSE;
    result_list.sorted = TRUE;
    result_list.key_info = &table->key_info[active_index];
    result_list.key_order = 0;
    result_list.limit_num =
      result_list.internal_limit >= result_list.split_read ?
      result_list.split_read : result_list.internal_limit;
    if (
      (error_num = spider_db_append_key_where(
        NULL, NULL, this)) ||
      (error_num = spider_db_append_key_order(this)) ||
      (error_num = spider_db_append_limit(
        &result_list.sql, result_list.internal_offset,
        result_list.limit_num)) ||
      (error_num = spider_db_append_select_lock(this))
    )
      DBUG_RETURN(error_num);

    int roop_start, roop_end, roop_count, tmp_lock_mode, link_ok;
    tmp_lock_mode = spider_conn_lock_mode(this);
    if (tmp_lock_mode)
    {
      /* "for update" or "lock in share mode" */
      link_ok = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_OK);
      roop_start = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_end = share->link_count;
    } else {
      link_ok = search_link_idx;
      roop_start = search_link_idx;
      roop_end = search_link_idx + 1;
    }
    for (roop_count = roop_start; roop_count < roop_end;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
      if (result_list.bgs_phase > 0)
      {
        if ((error_num = spider_bg_conn_search(this, roop_count, TRUE,
          (roop_count != link_ok))))
        {
          if (
            error_num != HA_ERR_END_OF_FILE &&
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      } else {
#endif
        if (share->same_db_table_name)
          sql = &result_list.sql;
        else {
          sql = &result_list.sqls[roop_count];
          if (sql->copy(result_list.sql))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          tmp_pos = sql->length();
          sql->length(result_list.table_name_pos);
          spider_db_append_table_name_with_adjusting(sql, share, roop_count);
          sql->length(tmp_pos);
        }
        pthread_mutex_lock(&conns[roop_count]->mta_conn_mutex);
        conns[roop_count]->need_mon = &need_mons[roop_count];
        conns[roop_count]->mta_conn_mutex_lock_already = TRUE;
        conns[roop_count]->mta_conn_mutex_unlock_later = TRUE;
        if ((error_num = spider_db_set_names(this, conns[roop_count],
          roop_count)))
        {
          conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
          conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
          pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        if (spider_db_query(
          conns[roop_count],
          sql->ptr(),
          sql->length(),
          &need_mons[roop_count])
        ) {
          conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
          conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
          error_num = spider_db_errorno(conns[roop_count]);
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
        conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
        if (roop_count == link_ok)
        {
          if ((error_num = spider_db_store_result(this, roop_count, table)))
          {
            if (
              error_num != HA_ERR_END_OF_FILE &&
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
            DBUG_RETURN(error_num);
          }
        } else {
          spider_db_discard_result(conns[roop_count]);
          pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
        }
#ifndef WITHOUT_SPIDER_BG_SEARCH
      }
#endif
    }
  }

  if (
    result_list.sorted &&
    result_list.desc_flg
  )
    DBUG_RETURN(spider_db_seek_last(buf, this, search_link_idx, table));
  DBUG_RETURN(spider_db_seek_first(buf, this, table));
}

int ha_spider::index_last(
  uchar *buf
) {
  int error_num, tmp_pos;
  String *sql;
  DBUG_ENTER("ha_spider::index_last");
  DBUG_PRINT("info",("spider this=%x", this));
  if (!result_list.sql.length())
  {
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if ((error_num = spider_set_conn_bg_param(this)))
      DBUG_RETURN(error_num);
#endif
#ifdef WITH_PARTITION_STORAGE_ENGINE
    check_select_column(FALSE);
#endif
    result_list.finish_flg = FALSE;
    result_list.record_num = 0;
    spider_db_free_one_result_for_start_next(this);
    if (keyread)
      result_list.keyread = TRUE;
    else
      result_list.keyread = FALSE;
    if (
      (error_num = spider_db_append_select(this)) ||
      (error_num = spider_db_append_select_columns(this))
    )
      DBUG_RETURN(error_num);
    if (
      share->key_hint &&
      (error_num = spider_db_append_hint_after_table(
        &result_list.sql, &share->key_hint[active_index]))
    )
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    result_list.where_pos = result_list.sql.length();
    result_list.desc_flg = TRUE;
    result_list.sorted = TRUE;
    result_list.key_info = &table->key_info[active_index];
    result_list.key_order = 0;
    result_list.limit_num =
      result_list.internal_limit >= result_list.split_read ?
      result_list.split_read : result_list.internal_limit;
    if (
      (error_num = spider_db_append_key_where(
        NULL, NULL, this)) ||
      (error_num = spider_db_append_key_order(this)) ||
      (error_num = spider_db_append_limit(
        &result_list.sql, result_list.internal_offset,
        result_list.limit_num)) ||
      (error_num = spider_db_append_select_lock(this))
    )
      DBUG_RETURN(error_num);

    int roop_start, roop_end, roop_count, tmp_lock_mode, link_ok;
    tmp_lock_mode = spider_conn_lock_mode(this);
    if (tmp_lock_mode)
    {
      /* "for update" or "lock in share mode" */
      link_ok = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_OK);
      roop_start = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_end = share->link_count;
    } else {
      link_ok = search_link_idx;
      roop_start = search_link_idx;
      roop_end = search_link_idx + 1;
    }
    for (roop_count = roop_start; roop_count < roop_end;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
      if (result_list.bgs_phase > 0)
      {
        if ((error_num = spider_bg_conn_search(this, roop_count, TRUE,
          (roop_count != link_ok))))
        {
          if (
            error_num != HA_ERR_END_OF_FILE &&
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      } else {
#endif
        if (share->same_db_table_name)
          sql = &result_list.sql;
        else {
          sql = &result_list.sqls[roop_count];
          if (sql->copy(result_list.sql))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          tmp_pos = sql->length();
          sql->length(result_list.table_name_pos);
          spider_db_append_table_name_with_adjusting(sql, share, roop_count);
          sql->length(tmp_pos);
        }
        pthread_mutex_lock(&conns[roop_count]->mta_conn_mutex);
        conns[roop_count]->need_mon = &need_mons[roop_count];
        conns[roop_count]->mta_conn_mutex_lock_already = TRUE;
        conns[roop_count]->mta_conn_mutex_unlock_later = TRUE;
        if ((error_num = spider_db_set_names(this, conns[roop_count],
          roop_count)))
        {
          conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
          conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
          pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        if (spider_db_query(
          conns[roop_count],
          sql->ptr(),
          sql->length(),
          &need_mons[roop_count])
        ) {
          conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
          conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
          error_num = spider_db_errorno(conns[roop_count]);
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
        conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
        if (roop_count == link_ok)
        {
          if ((error_num = spider_db_store_result(this, roop_count, table)))
          {
            if (
              error_num != HA_ERR_END_OF_FILE &&
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
            DBUG_RETURN(error_num);
          }
        } else {
          spider_db_discard_result(conns[roop_count]);
          pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
        }
#ifndef WITHOUT_SPIDER_BG_SEARCH
      }
#endif
    }
  }

  if (
    result_list.sorted &&
    result_list.desc_flg
  )
    DBUG_RETURN(spider_db_seek_first(buf, this, table));
  DBUG_RETURN(spider_db_seek_last(buf, this, search_link_idx, table));
}

int ha_spider::index_next_same(
  uchar *buf,
  const uchar *key,
  uint keylen
) {
  DBUG_ENTER("ha_spider::index_next_same");
  DBUG_PRINT("info",("spider this=%x", this));
  if (
    result_list.sorted &&
    result_list.desc_flg
  )
    DBUG_RETURN(spider_db_seek_prev(buf, this, table));
  DBUG_RETURN(spider_db_seek_next(buf, this, search_link_idx, table));
}

int ha_spider::read_range_first(
  const key_range *start_key,
  const key_range *end_key,
  bool eq_range,
  bool sorted
) {
  int error_num, tmp_pos;
  String *sql;
  DBUG_ENTER("ha_spider::read_range_first");
  DBUG_PRINT("info",("spider this=%x", this));
  result_list.sql.length(0);
#ifndef WITHOUT_SPIDER_BG_SEARCH
  if ((error_num = spider_set_conn_bg_param(this)))
    DBUG_RETURN(error_num);
#endif
#ifdef WITH_PARTITION_STORAGE_ENGINE
  check_select_column(FALSE);
#endif
  result_list.finish_flg = FALSE;
  result_list.record_num = 0;
  spider_db_free_one_result_for_start_next(this);
  if (keyread)
    result_list.keyread = TRUE;
  else
    result_list.keyread = FALSE;
  if (
    (error_num = spider_db_append_select(this)) ||
    (error_num = spider_db_append_select_columns(this))
  )
    DBUG_RETURN(error_num);
  if (
    share->key_hint &&
    (error_num = spider_db_append_hint_after_table(
      &result_list.sql, &share->key_hint[active_index]))
  )
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  result_list.where_pos = result_list.sql.length();
  result_list.desc_flg = FALSE;
  result_list.sorted = sorted;
  result_list.key_info = &table->key_info[active_index];
  result_list.limit_num =
    result_list.internal_limit >= result_list.split_read ?
    result_list.split_read : result_list.internal_limit;
  if (
    (error_num = spider_db_append_key_where(
      start_key, eq_range ? NULL : end_key, this)) ||
    (error_num = spider_db_append_key_order(this)) ||
    (error_num = spider_db_append_limit(
      &result_list.sql, result_list.internal_offset,
      result_list.limit_num)) ||
    (error_num = spider_db_append_select_lock(this))
  )
    DBUG_RETURN(error_num);

  int roop_start, roop_end, roop_count, tmp_lock_mode, link_ok;
  tmp_lock_mode = spider_conn_lock_mode(this);
  if (tmp_lock_mode)
  {
    /* "for update" or "lock in share mode" */
    link_ok = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_OK);
    roop_start = spider_conn_link_idx_next(share->link_statuses,
      -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
    roop_end = share->link_count;
  } else {
    link_ok = search_link_idx;
    roop_start = search_link_idx;
    roop_end = search_link_idx + 1;
  }
  for (roop_count = roop_start; roop_count < roop_end;
    roop_count = spider_conn_link_idx_next(share->link_statuses,
      roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
  ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if (result_list.bgs_phase > 0)
    {
      if ((error_num = spider_bg_conn_search(this, roop_count, TRUE,
        (roop_count != link_ok))))
      {
        if (
          error_num != HA_ERR_END_OF_FILE &&
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
    } else {
#endif
      if (share->same_db_table_name)
        sql = &result_list.sql;
      else {
        sql = &result_list.sqls[roop_count];
        if (sql->copy(result_list.sql))
          DBUG_RETURN(HA_ERR_OUT_OF_MEM);
        tmp_pos = sql->length();
        sql->length(result_list.table_name_pos);
        spider_db_append_table_name_with_adjusting(sql, share, roop_count);
        sql->length(tmp_pos);
      }
      pthread_mutex_lock(&conns[roop_count]->mta_conn_mutex);
      conns[roop_count]->need_mon = &need_mons[roop_count];
      conns[roop_count]->mta_conn_mutex_lock_already = TRUE;
      conns[roop_count]->mta_conn_mutex_unlock_later = TRUE;
      if ((error_num = spider_db_set_names(this, conns[roop_count],
        roop_count)))
      {
        conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
        conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
        pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
        if (
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      if (spider_db_query(
        conns[roop_count],
        sql->ptr(),
        sql->length(),
        &need_mons[roop_count])
      ) {
        conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
        conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
        error_num = spider_db_errorno(conns[roop_count]);
        if (
          share->monitoring_kind[roop_count] &&
          need_mons[roop_count]
        ) {
          error_num = spider_ping_table_mon_from_table(
              trx,
              trx->thd,
              share,
              share->monitoring_sid[roop_count],
              share->table_name,
              share->table_name_length,
              roop_count,
              "",
              0,
              share->monitoring_kind[roop_count],
              share->monitoring_limit[roop_count],
              TRUE
            );
        }
        DBUG_RETURN(error_num);
      }
      conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
      conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
      if (roop_count == link_ok)
      {
        if ((error_num = spider_db_store_result(this, roop_count, table)))
        {
          if (
            error_num != HA_ERR_END_OF_FILE &&
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      } else {
        spider_db_discard_result(conns[roop_count]);
        pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
      }
#ifndef WITHOUT_SPIDER_BG_SEARCH
    }
#endif
  }
  DBUG_RETURN(spider_db_fetch(table->record[0], this, table));
}

int ha_spider::read_range_next()
{
  DBUG_ENTER("ha_spider::read_range_next");
  DBUG_PRINT("info",("spider this=%x", this));
  if (
    result_list.sorted &&
    result_list.desc_flg
  )
    DBUG_RETURN(spider_db_seek_prev(table->record[0], this, table));
  DBUG_RETURN(spider_db_seek_next(table->record[0], this, search_link_idx,
    table));
}

int ha_spider::read_multi_range_first(
  KEY_MULTI_RANGE **found_range_p,
  KEY_MULTI_RANGE *ranges,
  uint range_count,
  bool sorted,
  HANDLER_BUFFER *buffer
) {
  int error_num, tmp_pos;
  String *sql;
  DBUG_ENTER("ha_spider::read_multi_range_first");
  DBUG_PRINT("info",("spider this=%x", this));
  multi_range_sorted = sorted;
  multi_range_buffer = buffer;

#ifndef WITHOUT_SPIDER_BG_SEARCH
  if ((error_num = spider_set_conn_bg_param(this)))
    DBUG_RETURN(error_num);
#endif
#ifdef WITH_PARTITION_STORAGE_ENGINE
  check_select_column(FALSE);
#endif
  result_list.finish_flg = FALSE;
  result_list.record_num = 0;
  spider_db_free_one_result_for_start_next(this);
  result_list.sql.length(0);
  result_list.desc_flg = FALSE;
  result_list.sorted = sorted;
  result_list.key_info = &table->key_info[active_index];
  if (result_list.multi_split_read)
  {
    if (keyread)
      result_list.keyread = TRUE;
    else
      result_list.keyread = FALSE;
    if (
      (error_num = spider_db_append_select(this)) ||
      (error_num = spider_db_append_select_columns(this))
    )
      DBUG_RETURN(error_num);
    if (
      share->key_hint &&
      (error_num = spider_db_append_hint_after_table(
        &result_list.sql, &share->key_hint[active_index]))
    )
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    result_list.where_pos = result_list.sql.length();
    for (
      multi_range_curr = ranges,
      multi_range_end = ranges + range_count;
      multi_range_curr < multi_range_end;
      multi_range_curr++
    ) {
      result_list.limit_num =
        result_list.internal_limit - result_list.record_num >=
        result_list.split_read ?
        result_list.split_read :
        result_list.internal_limit - result_list.record_num;
      if (
        (error_num = spider_db_append_key_where(
          &multi_range_curr->start_key,
          test(multi_range_curr->range_flag & EQ_RANGE) ?
          NULL : &multi_range_curr->end_key, this)) ||
        (error_num = spider_db_append_key_order(this)) ||
        (error_num = spider_db_append_limit(
          &result_list.sql,
          result_list.internal_offset + result_list.record_num,
          result_list.limit_num)) ||
        (error_num = spider_db_append_select_lock(this))
      )
        DBUG_RETURN(error_num);

      int roop_start, roop_end, roop_count, tmp_lock_mode, link_ok;
      tmp_lock_mode = spider_conn_lock_mode(this);
      if (tmp_lock_mode)
      {
        /* "for update" or "lock in share mode" */
        link_ok = spider_conn_link_idx_next(share->link_statuses,
          -1, share->link_count, SPIDER_LINK_STATUS_OK);
        roop_start = spider_conn_link_idx_next(share->link_statuses,
          -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
        roop_end = share->link_count;
      } else {
        link_ok = search_link_idx;
        roop_start = search_link_idx;
        roop_end = search_link_idx + 1;
      }
      for (roop_count = roop_start; roop_count < roop_end;
        roop_count = spider_conn_link_idx_next(share->link_statuses,
          roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
      ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
        if (result_list.bgs_phase > 0)
        {
          error_num = spider_bg_conn_search(this, roop_count, TRUE,
            (roop_count != link_ok));
          if (
            error_num &&
            error_num != HA_ERR_END_OF_FILE &&
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
        } else {
#endif
          if (share->same_db_table_name)
            sql = &result_list.sql;
          else {
            sql = &result_list.sqls[roop_count];
            if (sql->copy(result_list.sql))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            tmp_pos = sql->length();
            sql->length(result_list.table_name_pos);
            spider_db_append_table_name_with_adjusting(sql, share, roop_count);
            sql->length(tmp_pos);
          }
          pthread_mutex_lock(&conns[roop_count]->mta_conn_mutex);
          conns[roop_count]->need_mon = &need_mons[roop_count];
          conns[roop_count]->mta_conn_mutex_lock_already = TRUE;
          conns[roop_count]->mta_conn_mutex_unlock_later = TRUE;
          if ((error_num = spider_db_set_names(this, conns[roop_count],
            roop_count)))
          {
            conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
            conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
            pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
            if (
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
            DBUG_RETURN(error_num);
          }
          if (spider_db_query(
            conns[roop_count],
            sql->ptr(),
            sql->length(),
            &need_mons[roop_count])
          ) {
            conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
            conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
            error_num = spider_db_errorno(conns[roop_count]);
            if (
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
            DBUG_RETURN(error_num);
          }
          conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
          conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
          if (roop_count == link_ok)
          {
            error_num = spider_db_store_result(this, roop_count, table);
            if (
              error_num &&
              error_num != HA_ERR_END_OF_FILE &&
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
          } else {
            spider_db_discard_result(conns[roop_count]);
            pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
          }
#ifndef WITHOUT_SPIDER_BG_SEARCH
        }
#endif
      }
      if (error_num)
      {
        if (error_num == HA_ERR_END_OF_FILE)
        {
          result_list.finish_flg = FALSE;
          result_list.current->finish_flg = FALSE;
          result_list.record_num = 0;
          if (result_list.current == result_list.first)
            result_list.current = NULL;
          else
            result_list.current = result_list.current->prev;
        } else
          DBUG_RETURN(error_num);
      } else {
        *found_range_p = multi_range_curr;
        break;
      }
      result_list.sql.length(result_list.where_pos);
    }
    if (error_num)
    {
      if (error_num == HA_ERR_END_OF_FILE)
      {
        result_list.finish_flg = TRUE;
        if (result_list.current)
          result_list.current->finish_flg = TRUE;
        table->status = STATUS_NOT_FOUND;
      }
      DBUG_RETURN(error_num);
    }
  } else {
    bool tmp_high_priority = high_priority;
    if (result_list.sql.reserve(SPIDER_SQL_OPEN_PAREN_LEN))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);
    result_list.sql.q_append(
      SPIDER_SQL_OPEN_PAREN_STR, SPIDER_SQL_OPEN_PAREN_LEN);
    for (
      multi_range_curr = ranges,
      multi_range_end = ranges + range_count;
      multi_range_curr < multi_range_end;
      multi_range_curr++
    ) {
      if (keyread)
        result_list.keyread = TRUE;
      else
        result_list.keyread = FALSE;
      if (
        (error_num = spider_db_append_select(this)) ||
        (error_num = spider_db_append_select_columns(this))
      )
        DBUG_RETURN(error_num);
      high_priority = FALSE;
      if (
        share->key_hint &&
        (error_num = spider_db_append_hint_after_table(
          &result_list.sql, &share->key_hint[active_index]))
      )
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      result_list.where_pos = result_list.sql.length();
      if (
        (error_num = spider_db_append_key_where(
          &multi_range_curr->start_key,
          test(multi_range_curr->range_flag & EQ_RANGE) ?
          NULL : &multi_range_curr->end_key, this)) ||
        (error_num = spider_db_append_key_order(this)) ||
        (error_num = spider_db_append_select_lock(this))
      )
        DBUG_RETURN(error_num);
      if (result_list.sql.reserve(SPIDER_SQL_UNION_ALL_LEN))
        DBUG_RETURN(HA_ERR_OUT_OF_MEM);
      result_list.sql.q_append(
        SPIDER_SQL_UNION_ALL_STR, SPIDER_SQL_UNION_ALL_LEN);
    }
    high_priority = tmp_high_priority;
    result_list.sql.length(result_list.sql.length() -
      SPIDER_SQL_UNION_ALL_LEN + SPIDER_SQL_CLOSE_PAREN_LEN);
    result_list.limit_num =
      result_list.internal_limit >= result_list.split_read ?
      result_list.split_read : result_list.internal_limit;
    if (
      (error_num = spider_db_append_limit(
        &result_list.sql, result_list.internal_offset,
        result_list.limit_num))
    )
      DBUG_RETURN(error_num);

    int roop_start, roop_end, roop_count, tmp_lock_mode, link_ok;
    tmp_lock_mode = spider_conn_lock_mode(this);
    if (tmp_lock_mode)
    {
      /* "for update" or "lock in share mode" */
      link_ok = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_OK);
      roop_start = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_end = share->link_count;
    } else {
      link_ok = search_link_idx;
      roop_start = search_link_idx;
      roop_end = search_link_idx + 1;
    }
    for (roop_count = roop_start; roop_count < roop_end;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
      if (result_list.bgs_phase > 0)
      {
        if ((error_num = spider_bg_conn_search(this, roop_count, TRUE,
          (roop_count != link_ok))))
        {
          if (
            error_num != HA_ERR_END_OF_FILE &&
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      } else {
#endif
        if (share->same_db_table_name)
          sql = &result_list.sql;
        else {
          sql = &result_list.sqls[roop_count];
          if (sql->copy(result_list.sql))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          tmp_pos = sql->length();
          sql->length(result_list.table_name_pos);
          spider_db_append_table_name_with_adjusting(sql, share, roop_count);
          sql->length(tmp_pos);
        }
        pthread_mutex_lock(&conns[roop_count]->mta_conn_mutex);
        conns[roop_count]->need_mon = &need_mons[roop_count];
        conns[roop_count]->mta_conn_mutex_lock_already = TRUE;
        conns[roop_count]->mta_conn_mutex_unlock_later = TRUE;
        if ((error_num = spider_db_set_names(this, conns[roop_count],
          roop_count)))
        {
          conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
          conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
          pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        if (spider_db_query(
          conns[roop_count],
          sql->ptr(),
          sql->length(),
          &need_mons[roop_count])
        ) {
          conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
          conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
          error_num = spider_db_errorno(conns[roop_count]);
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
        conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
        if (roop_count == link_ok)
        {
          if ((error_num = spider_db_store_result(this, roop_count, table)))
          {
            if (
              error_num != HA_ERR_END_OF_FILE &&
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
            DBUG_RETURN(error_num);
          }
        } else {
          spider_db_discard_result(conns[roop_count]);
          pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
        }
#ifndef WITHOUT_SPIDER_BG_SEARCH
      }
#endif
    }
    *found_range_p = ranges;
  }
  DBUG_RETURN(spider_db_fetch(table->record[0], this, table));
}

int ha_spider::read_multi_range_next(
  KEY_MULTI_RANGE **found_range_p
) {
  int error_num, tmp_pos;
  String *sql;
  DBUG_ENTER("ha_spider::read_multi_range_next");
  DBUG_PRINT("info",("spider this=%x", this));
  if (result_list.multi_split_read)
  {
    if (!(error_num = spider_db_seek_next(table->record[0], this,
      search_link_idx, table)))
      DBUG_RETURN(0);
    multi_range_curr++;
    if (
      error_num != HA_ERR_END_OF_FILE ||
      multi_range_curr == multi_range_end
    )
      DBUG_RETURN(error_num);
#ifndef WITHOUT_SPIDER_BG_SEARCH
    if ((error_num = spider_set_conn_bg_param(this)))
      DBUG_RETURN(error_num);
#endif
    result_list.finish_flg = FALSE;
    result_list.current->finish_flg = FALSE;
    result_list.record_num = 0;
    spider_db_free_one_result_for_start_next(this);
    for (
      ;
      multi_range_curr < multi_range_end;
      multi_range_curr++
    ) {
      result_list.sql.length(result_list.where_pos);
      result_list.limit_num =
        result_list.internal_limit - result_list.record_num >=
        result_list.split_read ?
        result_list.split_read :
        result_list.internal_limit - result_list.record_num;
      if (
        (error_num = spider_db_append_key_where(
          &multi_range_curr->start_key,
          test(multi_range_curr->range_flag & EQ_RANGE) ?
          NULL : &multi_range_curr->end_key, this)) ||
        (error_num = spider_db_append_key_order(this)) ||
        (error_num = spider_db_append_limit(
          &result_list.sql,
          result_list.internal_offset + result_list.record_num,
          result_list.limit_num)) ||
        (error_num = spider_db_append_select_lock(this))
      )
        DBUG_RETURN(error_num);

      int roop_start, roop_end, roop_count, tmp_lock_mode, link_ok;
      tmp_lock_mode = spider_conn_lock_mode(this);
      if (tmp_lock_mode)
      {
        /* "for update" or "lock in share mode" */
        link_ok = spider_conn_link_idx_next(share->link_statuses,
          -1, share->link_count, SPIDER_LINK_STATUS_OK);
        roop_start = spider_conn_link_idx_next(share->link_statuses,
          -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
        roop_end = share->link_count;
      } else {
        link_ok = search_link_idx;
        roop_start = search_link_idx;
        roop_end = search_link_idx + 1;
      }
      for (roop_count = roop_start; roop_count < roop_end;
        roop_count = spider_conn_link_idx_next(share->link_statuses,
          roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
      ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
        if (result_list.bgs_phase > 0)
        {
          error_num = spider_bg_conn_search(this, roop_count, TRUE,
            (roop_count != link_ok));
          if (
            error_num &&
            error_num != HA_ERR_END_OF_FILE &&
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
        } else {
#endif
          if (share->same_db_table_name)
            sql = &result_list.sql;
          else {
            sql = &result_list.sqls[roop_count];
            if (sql->copy(result_list.sql))
              DBUG_RETURN(HA_ERR_OUT_OF_MEM);
            tmp_pos = sql->length();
            sql->length(result_list.table_name_pos);
            spider_db_append_table_name_with_adjusting(sql, share, roop_count);
            sql->length(tmp_pos);
          }
          pthread_mutex_lock(&conns[roop_count]->mta_conn_mutex);
          conns[roop_count]->need_mon = &need_mons[roop_count];
          conns[roop_count]->mta_conn_mutex_lock_already = TRUE;
          conns[roop_count]->mta_conn_mutex_unlock_later = TRUE;
          if ((error_num = spider_db_set_names(this, conns[roop_count],
            roop_count)))
          {
            conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
            conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
            pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
            if (
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
            DBUG_RETURN(error_num);
          }
          if (spider_db_query(
            conns[roop_count],
            sql->ptr(),
            sql->length(),
            &need_mons[roop_count])
          ) {
            conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
            conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
            error_num = spider_db_errorno(conns[roop_count]);
            if (
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
            DBUG_RETURN(error_num);
          }
          conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
          conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
          if (roop_count == link_ok)
          {
            error_num = spider_db_store_result(this, roop_count, table);
            if (
              error_num &&
              error_num != HA_ERR_END_OF_FILE &&
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
          } else {
            spider_db_discard_result(conns[roop_count]);
            pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
          }
#ifndef WITHOUT_SPIDER_BG_SEARCH
        }
#endif
      }
      if (error_num)
      {
        if (error_num == HA_ERR_END_OF_FILE)
        {
          result_list.finish_flg = FALSE;
          result_list.current->finish_flg = FALSE;
          result_list.record_num = 0;
          result_list.current = result_list.current->prev;
        } else
          DBUG_RETURN(error_num);
      } else {
        *found_range_p = multi_range_curr;
        break;
      }
    }
    if (error_num)
    {
      if (error_num == HA_ERR_END_OF_FILE)
      {
        result_list.finish_flg = TRUE;
        if (result_list.current)
          result_list.current->finish_flg = TRUE;
        table->status = STATUS_NOT_FOUND;
      }
      DBUG_RETURN(error_num);
    }
  } else {
    DBUG_RETURN(spider_db_seek_next(table->record[0], this, search_link_idx,
      table));
  }
  DBUG_RETURN(spider_db_fetch(table->record[0], this, table));
}

int ha_spider::rnd_init(
  bool scan
) {
  int error_num, roop_count, lock_mode;
  DBUG_ENTER("ha_spider::rnd_init");
  DBUG_PRINT("info",("spider this=%x", this));
  if (result_list.lock_type == F_WRLCK)
    check_and_start_bulk_update(SPD_BU_START_BY_INDEX_OR_RND_INIT);

  lock_mode = spider_conn_lock_mode(this);
  rnd_scan_and_first = scan;
  if (
    scan &&
    sql_command != SQLCOM_ALTER_TABLE
  ) {
    if (
      result_list.current &&
      result_list.low_mem_read
    ) {
      int roop_start, roop_end, roop_count, tmp_lock_mode;
      tmp_lock_mode = spider_conn_lock_mode(this);
      if (tmp_lock_mode)
      {
        /* "for update" or "lock in share mode" */
        roop_start = spider_conn_link_idx_next(share->link_statuses,
          -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
        roop_end = share->link_count;
      } else {
        roop_start = search_link_idx;
        roop_end = search_link_idx + 1;
      }
      for (roop_count = roop_start; roop_count < roop_end;
        roop_count = spider_conn_link_idx_next(share->link_statuses,
          roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
      ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
        if (conns[roop_count] && result_list.bgs_working)
          spider_bg_conn_break(conns[roop_count], this);
#endif
        if (quick_targets[roop_count])
        {
          DBUG_ASSERT(quick_targets[roop_count] ==
            conns[roop_count]->quick_target);
          conns[roop_count]->quick_target = NULL;
          quick_targets[roop_count] = NULL;
        }
      }
      result_list.record_num = 0;
      result_list.finish_flg = FALSE;
      result_list.quick_phase = 0;
#ifndef WITHOUT_SPIDER_BG_SEARCH
      result_list.bgs_phase = 0;
#endif
    }

    if (
      result_list.current &&
      !result_list.low_mem_read
    ) {
      result_list.current = result_list.first;
      spider_db_set_pos_to_first_row(&result_list);
      rnd_scan_and_first = FALSE;
    } else {
      spider_set_result_list_param(this);

      if (
        update_request &&
        share->have_recovery_link &&
        result_list.lock_type == F_WRLCK &&
        (pk_update = spider_check_pk_update(table))
      ) {
        bitmap_set_all(table->read_set);
      }

      result_list.sql.length(0);
#ifndef WITHOUT_SPIDER_BG_SEARCH
      if ((error_num = spider_set_conn_bg_param(this)))
        DBUG_RETURN(error_num);
#endif
      set_select_column_mode();
      result_list.keyread = FALSE;
    }
  }
  DBUG_RETURN(0);
}

int ha_spider::rnd_end()
{
  DBUG_ENTER("ha_spider::rnd_end");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(check_and_end_bulk_update(SPD_BU_START_BY_INDEX_OR_RND_INIT));
}

int ha_spider::rnd_next(
  uchar *buf
) {
  int error_num, tmp_pos;
  String *sql;
  DBUG_ENTER("ha_spider::rnd_next");
  DBUG_PRINT("info",("spider this=%x", this));
  /* do not copy table data at alter table */
  if (sql_command == SQLCOM_ALTER_TABLE)
    DBUG_RETURN(HA_ERR_END_OF_FILE);

  if (rnd_scan_and_first)
  {
#ifdef WITH_PARTITION_STORAGE_ENGINE
    check_select_column(TRUE);
#endif
    result_list.finish_flg = FALSE;
    result_list.record_num = 0;
    spider_db_free_one_result_for_start_next(this);
    if (
      (error_num = spider_db_append_select(this)) ||
      (error_num = spider_db_append_select_columns(this))
    )
      DBUG_RETURN(error_num);
    result_list.where_pos = result_list.sql.length();

    /* append condition pushdown */
    if (spider_db_append_condition(this, &result_list.sql))
      DBUG_RETURN(HA_ERR_OUT_OF_MEM);

    result_list.order_pos = result_list.sql.length();
    result_list.limit_pos = result_list.order_pos;
    result_list.desc_flg = FALSE;
    result_list.sorted = FALSE;
    result_list.key_info = NULL;
    result_list.limit_num =
      result_list.internal_limit >= result_list.split_read ?
      result_list.split_read : result_list.internal_limit;
    if (
      (error_num = spider_db_append_limit(
        &result_list.sql, result_list.internal_offset,
        result_list.limit_num)) ||
      (error_num = spider_db_append_select_lock(this))
    )
      DBUG_RETURN(error_num);

    int roop_start, roop_end, roop_count, tmp_lock_mode, link_ok;
    tmp_lock_mode = spider_conn_lock_mode(this);
    if (tmp_lock_mode)
    {
      /* "for update" or "lock in share mode" */
      link_ok = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_OK);
      roop_start = spider_conn_link_idx_next(share->link_statuses,
        -1, share->link_count, SPIDER_LINK_STATUS_RECOVERY);
      roop_end = share->link_count;
    } else {
      link_ok = search_link_idx;
      roop_start = search_link_idx;
      roop_end = search_link_idx + 1;
    }
    for (roop_count = roop_start; roop_count < roop_end;
      roop_count = spider_conn_link_idx_next(share->link_statuses,
        roop_count, share->link_count, SPIDER_LINK_STATUS_RECOVERY)
    ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
      if (result_list.bgs_phase > 0)
      {
        if ((error_num = spider_bg_conn_search(this, roop_count, TRUE,
          (roop_count != link_ok))))
        {
          if (
            error_num != HA_ERR_END_OF_FILE &&
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
      } else {
#endif
        if (share->same_db_table_name)
          sql = &result_list.sql;
        else {
          sql = &result_list.sqls[roop_count];
          if (sql->copy(result_list.sql))
            DBUG_RETURN(HA_ERR_OUT_OF_MEM);
          tmp_pos = sql->length();
          sql->length(result_list.table_name_pos);
          spider_db_append_table_name_with_adjusting(sql, share, roop_count);
          sql->length(tmp_pos);
        }
        pthread_mutex_lock(&conns[roop_count]->mta_conn_mutex);
        conns[roop_count]->need_mon = &need_mons[roop_count];
        conns[roop_count]->mta_conn_mutex_lock_already = TRUE;
        conns[roop_count]->mta_conn_mutex_unlock_later = TRUE;
        if ((error_num = spider_db_set_names(this, conns[roop_count],
          roop_count)))
        {
          conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
          conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
          pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        if (spider_db_query(
          conns[roop_count],
          sql->ptr(),
          sql->length(),
          &need_mons[roop_count])
        ) {
          conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
          conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
          error_num = spider_db_errorno(conns[roop_count]);
          if (
            share->monitoring_kind[roop_count] &&
            need_mons[roop_count]
          ) {
            error_num = spider_ping_table_mon_from_table(
                trx,
                trx->thd,
                share,
                share->monitoring_sid[roop_count],
                share->table_name,
                share->table_name_length,
                roop_count,
                "",
                0,
                share->monitoring_kind[roop_count],
                share->monitoring_limit[roop_count],
                TRUE
              );
          }
          DBUG_RETURN(error_num);
        }
        conns[roop_count]->mta_conn_mutex_lock_already = FALSE;
        conns[roop_count]->mta_conn_mutex_unlock_later = FALSE;
        if (roop_count == link_ok)
        {
          if ((error_num = spider_db_store_result(this, roop_count, table)))
          {
            if (
              error_num != HA_ERR_END_OF_FILE &&
              share->monitoring_kind[roop_count] &&
              need_mons[roop_count]
            ) {
              error_num = spider_ping_table_mon_from_table(
                  trx,
                  trx->thd,
                  share,
                  share->monitoring_sid[roop_count],
                  share->table_name,
                  share->table_name_length,
                  roop_count,
                  "",
                  0,
                  share->monitoring_kind[roop_count],
                  share->monitoring_limit[roop_count],
                  TRUE
                );
            }
            DBUG_RETURN(error_num);
          }
        } else {
          spider_db_discard_result(conns[roop_count]);
          pthread_mutex_unlock(&conns[roop_count]->mta_conn_mutex);
        }
#ifndef WITHOUT_SPIDER_BG_SEARCH
      }
#endif
    }
    rnd_scan_and_first = FALSE;
  }
  DBUG_RETURN(spider_db_seek_next(buf, this, search_link_idx, table));
}

void ha_spider::position(
  const uchar *record
) {
  DBUG_ENTER("ha_spider::position");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider first_row=%x", result_list.current->first_row));
  DBUG_PRINT("info",
    ("spider current_row_num=%ld", result_list.current_row_num));
  my_store_ptr(ref, ref_length, (my_off_t) spider_db_create_position(this));
  DBUG_VOID_RETURN;
}

int ha_spider::rnd_pos(
  uchar *buf,
  uchar *pos
) {
  SPIDER_POSITION *tmp_pos;
  DBUG_ENTER("ha_spider::rnd_pos");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider pos=%x", pos));
  DBUG_PRINT("info",("spider buf=%x", buf));
  DBUG_PRINT("info",("spider ref=%x", my_get_ptr(pos, ref_length)));
  if (!(tmp_pos = (SPIDER_POSITION*) my_get_ptr(pos, ref_length)))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);
  DBUG_RETURN(spider_db_seek_tmp(buf, tmp_pos, this, table));
}

int ha_spider::info(
  uint flag
) {
  int error_num;
  THD *thd = ha_thd();
  double sts_interval = THDVAR(thd, sts_interval) == -1 ?
    share->sts_interval : THDVAR(thd, sts_interval);
  int sts_mode = THDVAR(thd, sts_mode) <= 0 ?
    share->sts_mode : THDVAR(thd, sts_mode);
#ifdef WITH_PARTITION_STORAGE_ENGINE
  int sts_sync = THDVAR(thd, sts_sync) == -1 ?
    share->sts_sync : THDVAR(thd, sts_sync);
#endif
#ifndef WITHOUT_SPIDER_BG_SEARCH
  int sts_bg_mode = THDVAR(thd, sts_bg_mode) == -1 ?
    share->sts_bg_mode : THDVAR(thd, sts_bg_mode);
#endif
  SPIDER_INIT_ERROR_TABLE *spider_init_error_table = NULL;
  DBUG_ENTER("ha_spider::info");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider flag=%x", flag));
  if (flag &
    (HA_STATUS_TIME | HA_STATUS_CONST | HA_STATUS_VARIABLE | HA_STATUS_AUTO))
  {
    time_t tmp_time = (time_t) time((time_t*) 0);
    DBUG_PRINT("info",
      ("spider difftime=%f", difftime(tmp_time, share->sts_get_time)));
    DBUG_PRINT("info",
      ("spider sts_interval=%f", sts_interval));
    if (!share->sts_init)
    {
      pthread_mutex_lock(&share->sts_mutex);
      if ((spider_init_error_table =
        spider_get_init_error_table(share, FALSE)))
      {
        DBUG_PRINT("info",("spider diff=%f",
          difftime(tmp_time, spider_init_error_table->init_error_time)));
        if (difftime(tmp_time,
          spider_init_error_table->init_error_time) <
          spider_table_init_error_interval)
        {
          pthread_mutex_unlock(&share->sts_mutex);
          if (spider_init_error_table->init_error_with_message)
            my_message(spider_init_error_table->init_error,
              spider_init_error_table->init_error_msg, MYF(0));
          DBUG_RETURN(spider_init_error_table->init_error);
        }
      }
      pthread_mutex_unlock(&share->sts_mutex);
      sts_interval = 0;
    }
    if (difftime(tmp_time, share->sts_get_time) >= sts_interval)
    {
      if (
        sts_interval == 0 ||
        !pthread_mutex_trylock(&share->sts_mutex)
      ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
        if (sts_interval == 0 || sts_bg_mode == 0)
        {
#endif
          if (sts_interval == 0)
            pthread_mutex_lock(&share->sts_mutex);
          if (difftime(tmp_time, share->sts_get_time) >= sts_interval)
          {
            if (
              (error_num = spider_check_trx_and_get_conn(ha_thd(), this)) ||
              (error_num = spider_get_sts(share, search_link_idx, tmp_time,
                this, sts_interval, sts_mode,
#ifdef WITH_PARTITION_STORAGE_ENGINE
                sts_sync,
#endif
                share->sts_init ? 2 : 1))
            ) {
              pthread_mutex_unlock(&share->sts_mutex);
              if (
                share->monitoring_kind[search_link_idx] &&
                need_mons[search_link_idx]
              ) {
                error_num = spider_ping_table_mon_from_table(
                    trx,
                    trx->thd,
                    share,
                    share->monitoring_sid[search_link_idx],
                    share->table_name,
                    share->table_name_length,
                    search_link_idx,
                    "",
                    0,
                    share->monitoring_kind[search_link_idx],
                    share->monitoring_limit[search_link_idx],
                    TRUE
                  );
              }
              if (!share->sts_init)
              {
                if (
                  spider_init_error_table ||
                  (spider_init_error_table =
                    spider_get_init_error_table(share, TRUE))
                ) {
                  spider_init_error_table->init_error = error_num;
                  if ((spider_init_error_table->init_error_with_message =
                    thd->main_da.is_error()))
                    strmov(spider_init_error_table->init_error_msg,
                      thd->main_da.message());
                  spider_init_error_table->init_error_time =
                    (time_t) time((time_t*) 0);
                }
                share->init_error = TRUE;
                share->init = TRUE;
              }
              DBUG_RETURN(error_num);
            }
          }
#ifndef WITHOUT_SPIDER_BG_SEARCH
        } else {
          /* background */
          if (!share->bg_sts_init || share->bg_sts_thd_wait)
          {
            share->bg_sts_thd_wait = FALSE;
            share->bg_sts_try_time = tmp_time;
            share->bg_sts_interval = sts_interval;
            share->bg_sts_mode = sts_mode;
#ifdef WITH_PARTITION_STORAGE_ENGINE
            share->bg_sts_sync = sts_sync;
#endif
            if (!share->bg_sts_init)
            {
              if ((error_num = spider_create_sts_thread(share)))
              {
                pthread_mutex_unlock(&share->sts_mutex);
                DBUG_RETURN(error_num);
              }
            } else
              pthread_cond_signal(&share->bg_sts_cond);
          }
        }
#endif
        pthread_mutex_unlock(&share->sts_mutex);
      }
    }

    if (flag & HA_STATUS_TIME)
      stats.update_time = share->update_time;
    if (flag & (HA_STATUS_CONST | HA_STATUS_VARIABLE))
    {
      stats.max_data_file_length = share->max_data_file_length;
      stats.create_time = share->create_time;
      stats.block_size = THDVAR(thd, block_size);
    }
    if (flag & HA_STATUS_VARIABLE)
    {
      stats.data_file_length = share->data_file_length;
      stats.index_file_length = share->index_file_length;
      stats.records = share->records;
      stats.mean_rec_length = share->mean_rec_length;
      stats.check_time = share->check_time;
      if (stats.records <= 1 && (flag & HA_STATUS_NO_LOCK))
        stats.records = 2;
    }
    if (flag & HA_STATUS_AUTO)
      stats.auto_increment_value = share->auto_increment_value;
  }
  if (flag & HA_STATUS_ERRKEY)
    errkey = dup_key_idx;
  DBUG_RETURN(0);
}

ha_rows ha_spider::records_in_range(
  uint inx,
  key_range *start_key,
  key_range *end_key
) {
  int error_num;
  THD *thd = ha_thd();
  double crd_interval = THDVAR(thd, crd_interval) == -1 ?
    share->crd_interval : THDVAR(thd, crd_interval);
  int crd_mode = THDVAR(thd, crd_mode) <= 0 ?
    share->crd_mode : THDVAR(thd, crd_mode);
  int crd_type = THDVAR(thd, crd_type) == -1 ?
    share->crd_type : THDVAR(thd, crd_type);
#ifdef WITH_PARTITION_STORAGE_ENGINE
  int crd_sync = THDVAR(thd, crd_sync) == -1 ?
    share->crd_sync : THDVAR(thd, crd_sync);
#endif
#ifndef WITHOUT_SPIDER_BG_SEARCH
  int crd_bg_mode = THDVAR(thd, crd_bg_mode) == -1 ?
    share->crd_bg_mode : THDVAR(thd, crd_bg_mode);
#endif
  SPIDER_INIT_ERROR_TABLE *spider_init_error_table = NULL;
  DBUG_ENTER("ha_spider::records_in_range");
  DBUG_PRINT("info",("spider this=%x", this));
  time_t tmp_time = (time_t) time((time_t*) 0);
  if (!share->crd_init)
  {
    pthread_mutex_lock(&share->crd_mutex);
    if ((spider_init_error_table =
      spider_get_init_error_table(share, FALSE)))
    {
      DBUG_PRINT("info",("spider diff=%f",
        difftime(tmp_time, spider_init_error_table->init_error_time)));
      if (difftime(tmp_time,
        spider_init_error_table->init_error_time) <
        spider_table_init_error_interval)
      {
        pthread_mutex_unlock(&share->crd_mutex);
        my_errno = spider_init_error_table->init_error;
        if (spider_init_error_table->init_error_with_message)
          my_message(spider_init_error_table->init_error,
            spider_init_error_table->init_error_msg, MYF(0));
        DBUG_RETURN(HA_POS_ERROR);
      }
    }
    pthread_mutex_unlock(&share->crd_mutex);
    if (crd_mode == 3)
      crd_mode = 1;
    crd_interval = 0;
  }
  if (crd_mode == 1 || crd_mode == 2)
  {
    DBUG_PRINT("info",
      ("spider difftime=%f", difftime(tmp_time, share->crd_get_time)));
    DBUG_PRINT("info",
      ("spider crd_interval=%f", crd_interval));
    if (difftime(tmp_time, share->crd_get_time) >= crd_interval)
    {
      if (
        crd_interval == 0 ||
        !pthread_mutex_trylock(&share->crd_mutex)
      ) {
#ifndef WITHOUT_SPIDER_BG_SEARCH
        if (crd_interval == 0 || crd_bg_mode == 0)
        {
#endif
          if (crd_interval == 0)
            pthread_mutex_lock(&share->crd_mutex);
          if (difftime(tmp_time, share->crd_get_time) >= crd_interval)
          {
            if ((error_num = spider_get_crd(share, search_link_idx, tmp_time,
              this, table, crd_interval, crd_mode,
#ifdef WITH_PARTITION_STORAGE_ENGINE
              crd_sync,
#endif
              share->crd_init ? 2 : 1)))
            {
              pthread_mutex_unlock(&share->crd_mutex);
              if (
                share->monitoring_kind[search_link_idx] &&
                need_mons[search_link_idx]
              ) {
                error_num = spider_ping_table_mon_from_table(
                    trx,
                    trx->thd,
                    share,
                    share->monitoring_sid[search_link_idx],
                    share->table_name,
                    share->table_name_length,
                    search_link_idx,
                    "",
                    0,
                    share->monitoring_kind[search_link_idx],
                    share->monitoring_limit[search_link_idx],
                    TRUE
                  );
              }
              my_errno = error_num;
              if (!share->crd_init)
              {
                if (
                  spider_init_error_table ||
                  (spider_init_error_table =
                    spider_get_init_error_table(share, TRUE))
                ) {
                  spider_init_error_table->init_error = error_num;
                  if ((spider_init_error_table->init_error_with_message =
                    thd->main_da.is_error()))
                    strmov(spider_init_error_table->init_error_msg,
                      thd->main_da.message());
                  spider_init_error_table->init_error_time =
                    (time_t) time((time_t*) 0);
                }
                share->init_error = TRUE;
                share->init = TRUE;
              }
              DBUG_RETURN(HA_POS_ERROR);
            }
          }
#ifndef WITHOUT_SPIDER_BG_SEARCH
        } else {
          /* background */
          if (!share->bg_crd_init || share->bg_crd_thd_wait)
          {
            share->bg_crd_thd_wait = FALSE;
            share->bg_crd_try_time = tmp_time;
            share->bg_crd_interval = crd_interval;
            share->bg_crd_mode = crd_mode;
#ifdef WITH_PARTITION_STORAGE_ENGINE
            share->bg_crd_sync = crd_sync;
#endif
            if (!share->bg_crd_init)
            {
              if ((error_num = spider_create_crd_thread(share)))
              {
                pthread_mutex_unlock(&share->crd_mutex);
                DBUG_RETURN(error_num);
              }
            } else
              pthread_cond_signal(&share->bg_crd_cond);
          }
        }
#endif
        pthread_mutex_unlock(&share->crd_mutex);
      }
    }

    KEY *key_info = &table->key_info[inx];
    key_part_map full_key_part_map =
      make_prev_keypart_map(key_info->key_parts);
    key_part_map start_key_part_map;
    key_part_map end_key_part_map;
    key_part_map tgt_key_part_map;
    KEY_PART_INFO *key_part;
    Field *field;
    double rows = (double) share->records;
    double weight, rate;
    if (start_key)
      start_key_part_map = start_key->keypart_map & full_key_part_map;
    else
      start_key_part_map = 0;
    if (end_key)
      end_key_part_map = end_key->keypart_map & full_key_part_map;
    else
      end_key_part_map = 0;

    if (!start_key_part_map && !end_key_part_map)
      DBUG_RETURN(HA_POS_ERROR);
    else if (start_key_part_map >= end_key_part_map)
    {
      tgt_key_part_map = start_key_part_map;
    } else {
      tgt_key_part_map = end_key_part_map;
    }

    if (crd_type == 0)
      weight = share->crd_weight;
    else
      weight = 1;

    for (
      key_part = key_info->key_part;
      tgt_key_part_map > 1;
      tgt_key_part_map >>= 1,
      key_part++
    ) {
      field = key_part->field;
      if ((rate =
        ((double) share->cardinality[field->field_index]) / weight) >= 1
      ) {
        if ((rows = rows / rate) < 2)
          DBUG_RETURN((ha_rows) 2);
      }
      if (crd_type == 1)
        weight += share->crd_weight;
      else if (crd_type == 2)
        weight *= share->crd_weight;
    }
    field = key_part->field;
    if (
      start_key_part_map >= end_key_part_map &&
      start_key->flag == HA_READ_KEY_EXACT
    ) {
      if ((rate =
        ((double) share->cardinality[field->field_index]) / weight) >= 1)
        rows = rows / rate;
    } else if (start_key_part_map == end_key_part_map)
    {
      if ((rate =
        ((double) share->cardinality[field->field_index]) / weight / 4) >= 1)
        rows = rows / rate;
    } else {
      if ((rate =
        ((double) share->cardinality[field->field_index]) / weight / 16) >= 1)
        rows = rows / rate;
    }
    if (rows < 2)
      DBUG_RETURN((ha_rows) 2);
    DBUG_RETURN((ha_rows) rows);
  } else if (crd_mode == 3)
  {
    result_list.key_info = &table->key_info[inx];
    DBUG_RETURN(spider_db_explain_select(start_key, end_key, this,
      search_link_idx));
  }
  DBUG_RETURN((ha_rows) share->crd_weight);
}

const char *ha_spider::table_type() const
{
  DBUG_ENTER("ha_spider::table_type");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN("SPIDER"); 
}

ulonglong ha_spider::table_flags() const
{
  DBUG_ENTER("ha_spider::table_flags");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(
    HA_REC_NOT_IN_SEQ |
    HA_CAN_GEOMETRY |
    HA_NULL_IN_KEY |
    HA_CAN_INDEX_BLOBS |
    HA_AUTO_PART_KEY |
    /* HA_CAN_RTREEKEYS | */
    HA_PRIMARY_KEY_REQUIRED_FOR_DELETE |
    HA_NO_PREFIX_CHAR_KEYS |
    /* HA_CAN_FULLTEXT | */
    HA_CAN_SQL_HANDLER |
    HA_FILE_BASED |
    HA_CAN_INSERT_DELAYED |
    HA_CAN_BIT_FIELD |
    HA_NO_COPY_ON_ALTER |
    HA_BINLOG_ROW_CAPABLE |
    HA_BINLOG_STMT_CAPABLE |
    SPIDER_CAN_BG_SEARCH |
    SPIDER_CAN_BG_INSERT |
    SPIDER_CAN_BG_UPDATE |
    (share ? share->additional_table_flags : 0)
  );
}

const char *ha_spider::index_type(
  uint key_number
) {
  KEY *key_info = &table->s->key_info[key_number];
  DBUG_ENTER("ha_spider::index_type");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider flags=%ld", key_info->flags));
  DBUG_PRINT("info",("spider algorithm=%ld", key_info->algorithm));
  DBUG_RETURN(
    (key_info->flags & HA_FULLTEXT) ? "FULLTEXT" :
    (key_info->flags & HA_SPATIAL) ? "SPATIAL" :
    (key_info->algorithm == HA_KEY_ALG_HASH) ? "HASH" :
    (key_info->algorithm == HA_KEY_ALG_RTREE) ? "RTREE" :
    "BTREE"
  );
}

ulong ha_spider::index_flags(
  uint idx,
  uint part,
  bool all_parts
) const {
  DBUG_ENTER("ha_spider::index_flags");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(
    (table_share->key_info[idx].algorithm == HA_KEY_ALG_FULLTEXT) ?
      0 :
    (table_share->key_info[idx].algorithm == HA_KEY_ALG_HASH) ?
      HA_ONLY_WHOLE_INDEX | HA_KEY_SCAN_NOT_ROR :
    HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE |
    HA_KEYREAD_ONLY
  );
}

uint ha_spider::max_supported_record_length() const
{
  DBUG_ENTER("ha_spider::max_supported_record_length");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(HA_MAX_REC_LENGTH);
}

uint ha_spider::max_supported_keys() const
{
  DBUG_ENTER("ha_spider::max_supported_keys");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(MAX_KEY);
}

uint ha_spider::max_supported_key_parts() const
{
  DBUG_ENTER("ha_spider::max_supported_key_parts");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(MAX_REF_PARTS);
}

uint ha_spider::max_supported_key_length() const
{
  DBUG_ENTER("ha_spider::max_supported_key_length");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(SPIDER_MAX_KEY_LENGTH);
}

uint ha_spider::max_supported_key_part_length() const
{
  DBUG_ENTER("ha_spider::max_supported_key_part_length");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(SPIDER_MAX_KEY_LENGTH);
}

uint8 ha_spider::table_cache_type()
{
  DBUG_ENTER("ha_spider::table_cache_type");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(HA_CACHE_TBL_NOCACHE);
}

void ha_spider::get_auto_increment(
  ulonglong offset,
  ulonglong increment,
  ulonglong nb_desired_values,
  ulonglong *first_value,
  ulonglong *nb_reserved_values
) {
  THD *thd = ha_thd();
  int auto_increment_mode = THDVAR(thd, auto_increment_mode) == -1 ?
    share->auto_increment_mode : THDVAR(thd, auto_increment_mode);
  DBUG_ENTER("ha_spider::get_auto_increment");
  DBUG_PRINT("info",("spider this=%x", this));
  if (auto_increment_mode == 0)
  {
    /* strict mode */
    int error_num;
    VOID(extra(HA_EXTRA_KEYREAD));
    if (index_init(table_share->next_number_index, TRUE))
      goto error_index_init;
    result_list.internal_limit = 1;
    if (table_share->next_number_keypart)
    {
      uchar key[MAX_KEY_LENGTH];
      key_copy(key, table->record[0],
        &table->key_info[table_share->next_number_index],
        table_share->next_number_key_offset);
      error_num = index_read_last_map(table->record[1], key,
        make_prev_keypart_map(table_share->next_number_keypart));
    } else
      error_num = index_last(table->record[1]);

    if (error_num)
      *first_value = 1;
    else
      *first_value = ((ulonglong) table->next_number_field->
        val_int_offset(table_share->rec_buff_length) + 1);
    VOID(index_end());
    VOID(extra(HA_EXTRA_NO_KEYREAD));
    DBUG_VOID_RETURN;

error_index_init:
    VOID(extra(HA_EXTRA_NO_KEYREAD));
    *first_value = ~(ulonglong)0;
    DBUG_VOID_RETURN;
  } else {
    pthread_mutex_lock(&share->auto_increment_mutex);
    *first_value = share->auto_increment_lclval;
    share->auto_increment_lclval += nb_desired_values * increment;
    pthread_mutex_unlock(&share->auto_increment_mutex);
  }
}

int ha_spider::reset_auto_increment(
  ulonglong value
) {
  DBUG_ENTER("ha_spider::reset_auto_increment");
  DBUG_PRINT("info",("spider this=%x", this));
  if (table->next_number_field)
  {
    pthread_mutex_lock(&share->auto_increment_mutex);
    share->auto_increment_lclval = value;
    share->auto_increment_init = TRUE;
    pthread_mutex_unlock(&share->auto_increment_mutex);
  }
  DBUG_RETURN(0);
}

void ha_spider::start_bulk_insert(
  ha_rows rows
) {
  DBUG_ENTER("ha_spider::start_bulk_insert");
  DBUG_PRINT("info",("spider this=%x", this));
  bulk_insert = TRUE;
  bulk_size = -1;
  DBUG_VOID_RETURN;
}

int ha_spider::end_bulk_insert()
{
  DBUG_ENTER("ha_spider::end_bulk_insert");
  DBUG_RETURN(end_bulk_insert(FALSE));
}

int ha_spider::end_bulk_insert(
  bool abort
) {
  DBUG_ENTER("ha_spider::end_bulk_insert");
  DBUG_PRINT("info",("spider this=%x", this));
  bulk_insert = FALSE;
  if (bulk_size == -1 || abort)
    DBUG_RETURN(0);
  DBUG_RETURN(spider_db_bulk_insert(this, table, TRUE));
}

int ha_spider::write_row(
  uchar *buf
) {
  int error_num;
  THD *thd = ha_thd();
  int auto_increment_mode = THDVAR(thd, auto_increment_mode) == -1 ?
    share->auto_increment_mode : THDVAR(thd, auto_increment_mode);
  bool auto_increment_flag =
    table->next_number_field && buf == table->record[0];
  DBUG_ENTER("ha_spider::write_row");
  DBUG_PRINT("info",("spider this=%x", this));
  ha_statistic_increment(&SSV::ha_write_count);
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_INSERT)
    table->timestamp_field->set_time();
  if (auto_increment_flag)
  {
    if (auto_increment_mode == 2)
    {
#ifndef DBUG_OFF
      my_bitmap_map *tmp_map =
        dbug_tmp_use_all_columns(table, table->write_set);
#endif
      table->next_number_field->store((longlong) 0, TRUE);
      table->auto_increment_field_not_null = FALSE;
#ifndef DBUG_OFF
      dbug_tmp_restore_column_map(table->write_set, tmp_map);
#endif
    } else {
      if (!share->auto_increment_init)
      {
        pthread_mutex_lock(&share->auto_increment_mutex);
        if (!share->auto_increment_init)
        {
          VOID(info(HA_STATUS_AUTO));
          share->auto_increment_lclval = stats.auto_increment_value;
          share->auto_increment_init = TRUE;
        }
        pthread_mutex_unlock(&share->auto_increment_mutex);
      }
      if ((error_num = update_auto_increment()))
        DBUG_RETURN(error_num);
    }
  }
  if (!bulk_insert || bulk_size < 0)
  {
    direct_dup_insert =
      THDVAR(trx->thd, direct_dup_insert) < 0 ?
      share->direct_dup_insert :
      THDVAR(trx->thd, direct_dup_insert);
    DBUG_PRINT("info",("spider direct_dup_insert=%d", direct_dup_insert));
    if ((error_num = spider_db_bulk_insert_init(this, table)))
      DBUG_RETURN(error_num);
    if (bulk_insert)
      bulk_size =
        insert_with_update || (!direct_dup_insert && ignore_dup_key) ?
        0 :
        THDVAR(trx->thd, bulk_size) < 0 ?
        share->bulk_size :
        THDVAR(trx->thd, bulk_size);
    else
      bulk_size = 0;
  }
  DBUG_RETURN(spider_db_bulk_insert(this, table, FALSE));
}

bool ha_spider::start_bulk_update(
) {
  DBUG_ENTER("ha_spider::start_bulk_update");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(check_and_start_bulk_update(SPD_BU_START_BY_BULK_INIT));
}

int ha_spider::exec_bulk_update(
  uint *dup_key_found
) {
  DBUG_ENTER("ha_spider::exec_bulk_update");
  DBUG_PRINT("info",("spider this=%x", this));
  *dup_key_found = 0;
  DBUG_RETURN(spider_db_bulk_update_end(this));
}

void ha_spider::end_bulk_update(
) {
  DBUG_ENTER("ha_spider::end_bulk_update");
  DBUG_PRINT("info",("spider this=%x", this));
  VOID(check_and_end_bulk_update(SPD_BU_START_BY_BULK_INIT));
  DBUG_VOID_RETURN;
}

int ha_spider::bulk_update_row(
  const uchar *old_data,
  uchar *new_data,
  uint *dup_key_found
) {
  DBUG_ENTER("ha_spider::bulk_update_row");
  DBUG_PRINT("info",("spider this=%x", this));
  *dup_key_found = 0;
  DBUG_RETURN(update_row(old_data, new_data));
}

int ha_spider::update_row(
  const uchar *old_data,
  uchar *new_data
) {
  int error_num;
  DBUG_ENTER("ha_spider::update_row");
  DBUG_PRINT("info",("spider this=%x", this));
  ha_statistic_increment(&SSV::ha_update_count);
  if (table->timestamp_field_type & TIMESTAMP_AUTO_SET_ON_UPDATE)
    table->timestamp_field->set_time();
  if ((error_num = spider_db_update(this, table, old_data)))
    DBUG_RETURN(error_num);
  if (table->found_next_number_field &&
    new_data == table->record[0] &&
    !table->s->next_number_keypart
  ) {
    pthread_mutex_lock(&share->auto_increment_mutex);
    if (!share->auto_increment_init)
    {
      VOID(info(HA_STATUS_AUTO));
      share->auto_increment_lclval = stats.auto_increment_value;
      share->auto_increment_init = TRUE;
    }
    int tmp_auto_increment = table->found_next_number_field->val_int();
    if (tmp_auto_increment >= share->auto_increment_lclval)
    {
      share->auto_increment_lclval = tmp_auto_increment + 1;
      share->auto_increment_value = tmp_auto_increment + 1;
    }
    pthread_mutex_unlock(&share->auto_increment_mutex);
  }
  DBUG_RETURN(0);
}

bool ha_spider::start_bulk_delete(
) {
  DBUG_ENTER("ha_spider::start_bulk_delete");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(check_and_start_bulk_update(SPD_BU_START_BY_BULK_INIT));
}

int ha_spider::end_bulk_delete(
) {
  DBUG_ENTER("ha_spider::end_bulk_delete");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(check_and_end_bulk_update(SPD_BU_START_BY_BULK_INIT));
}

int ha_spider::delete_row(
  const uchar *buf
) {
  DBUG_ENTER("ha_spider::delete_row");
  DBUG_PRINT("info",("spider this=%x", this));
  ha_statistic_increment(&SSV::ha_delete_count);
  DBUG_RETURN(spider_db_delete(this, table, buf));
}

int ha_spider::delete_all_rows()
{
  int error_num;
  DBUG_ENTER("ha_spider::delete_all_rows");
  DBUG_PRINT("info",("spider this=%x", this));
  if ((error_num = spider_db_delete_all_rows(this)))
    DBUG_RETURN(error_num);
  if (sql_command == SQLCOM_TRUNCATE && table->found_next_number_field)
  {
    DBUG_PRINT("info",("spider reset auto increment"));
    pthread_mutex_lock(&share->auto_increment_mutex);
    share->auto_increment_lclval = 1;
    share->auto_increment_init = TRUE;
    share->auto_increment_value = 1;
    pthread_mutex_unlock(&share->auto_increment_mutex);
  }
  DBUG_RETURN(0);
}

double ha_spider::scan_time()
{
  DBUG_ENTER("ha_spider::scan_time");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider scan_time = %.6f",
    share->scan_rate * share->records * share->mean_rec_length + 2));
  DBUG_RETURN(share->scan_rate * share->records * share->mean_rec_length + 2);
}

double ha_spider::read_time(
  uint index,
  uint ranges,
  ha_rows rows
) {
  DBUG_ENTER("ha_spider::read_time");
  DBUG_PRINT("info",("spider this=%x", this));
  if (keyread)
  {
    DBUG_PRINT("info",("spider read_time(keyread) = %.6f",
      share->read_rate * table->s->key_info[index].key_length *
      rows / 2 + 2));
    DBUG_RETURN(share->read_rate * table->s->key_info[index].key_length *
      rows / 2 + 2);
  } else {
    DBUG_PRINT("info",("spider read_time = %.6f",
      share->read_rate * share->mean_rec_length * rows + 2));
    DBUG_RETURN(share->read_rate * share->mean_rec_length * rows + 2);
  }
}

const key_map *ha_spider::keys_to_use_for_scanning()
{
  DBUG_ENTER("ha_spider::keys_to_use_for_scanning");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(&key_map_full);
}

ha_rows ha_spider::estimate_rows_upper_bound()
{
  DBUG_ENTER("ha_spider::estimate_rows_upper_bound");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(HA_POS_ERROR);
}

bool ha_spider::get_error_message(
  int error,
  String *buf
) {
  DBUG_ENTER("ha_spider::get_error_message");
  DBUG_PRINT("info",("spider this=%x", this));
  switch (error)
  {
    case ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM:
      if (buf->reserve(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_LEN))
        DBUG_RETURN(TRUE);
      buf->q_append(ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR,
        ER_SPIDER_REMOTE_SERVER_GONE_AWAY_LEN);
      break;
    default:
      if (buf->reserve(ER_SPIDER_UNKNOWN_LEN))
        DBUG_RETURN(TRUE);
      buf->q_append(ER_SPIDER_UNKNOWN_STR, ER_SPIDER_UNKNOWN_LEN);
      break;
  }
  DBUG_RETURN(FALSE);
}

int ha_spider::create(
  const char *name,
  TABLE *form,
  HA_CREATE_INFO *info
) {
  int error_num;
  SPIDER_SHARE tmp_share;
  THD *thd = ha_thd();
  uint sql_command = thd_sql_command(thd);
  SPIDER_TRX *trx;
  TABLE *table_tables = NULL;
  Open_tables_state open_tables_backup;
  SPIDER_CONN *conn;
  bool need_lock = FALSE;
  DBUG_ENTER("ha_spider::create");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider name=%s", name));
  DBUG_PRINT("info",
    ("spider form->s->connect_string=%s", form->s->connect_string.str));
  DBUG_PRINT("info",
    ("spider info->connect_string=%s", info->connect_string.str));
  if (!(trx = spider_get_trx(thd, &error_num)))
    goto error_get_trx;
  if (
    trx->locked_connections &&
    sql_command == SQLCOM_ALTER_TABLE
  ) {
    my_message(ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM,
      ER_SPIDER_ALTER_BEFORE_UNLOCK_STR, MYF(0));
    error_num = ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM;
    goto error_alter_before_unlock;
  }
  memset(&tmp_share, 0, sizeof(SPIDER_SHARE));
  tmp_share.table_name = (char*) name;
  tmp_share.table_name_length = strlen(name);
  if (form->s->keys > 0 &&
    !(tmp_share.key_hint = new String[form->s->keys])
  ) {
    error_num = HA_ERR_OUT_OF_MEM;
    goto error;
  }
  DBUG_PRINT("info",("spider tmp_share.key_hint=%x", tmp_share.key_hint));
  if ((error_num = spider_parse_connect_info(&tmp_share, form, 1)))
    goto error;
  DBUG_PRINT("info",("spider tmp_table=%d", form->s->tmp_table));
  if (
    (sql_command == SQLCOM_CREATE_TABLE &&
      !(info->options & HA_LEX_CREATE_TMP_TABLE))
  ) {
    if (
      !(table_tables = spider_open_sys_table(
        current_thd, SPIDER_SYS_TABLES_TABLE_NAME_STR,
        SPIDER_SYS_TABLES_TABLE_NAME_LEN, TRUE, &open_tables_backup, FALSE,
        &error_num))
    ) {
      goto error;
    }
    if (
      (error_num = spider_insert_tables(table_tables, &tmp_share))
    ) {
      goto error;
    }
    spider_close_sys_table(current_thd, table_tables,
      &open_tables_backup, FALSE);
    table_tables = NULL;
  } else if (
    sql_command == SQLCOM_ALTER_TABLE
  ) {
    SPIDER_ALTER_TABLE *alter_table;
    if (trx->query_id != thd->query_id)
    {
      spider_free_trx_alter_table(trx);
      trx->query_id = thd->query_id;
    }
    if (!(alter_table =
      (SPIDER_ALTER_TABLE*) hash_search(&trx->trx_alter_table_hash,
      (uchar*) tmp_share.table_name, tmp_share.table_name_length)))
    {
      if ((error_num = spider_create_trx_alter_table(trx, &tmp_share, TRUE)))
        goto error;
    }
    trx->tmp_flg = TRUE;

    DBUG_PRINT("info",
      ("spider alter_info.flags=%u", thd->lex->alter_info.flags));
    if (
      (thd->lex->alter_info.flags &
        (
          ALTER_ADD_PARTITION | ALTER_DROP_PARTITION |
          ALTER_COALESCE_PARTITION | ALTER_REORGANIZE_PARTITION |
          ALTER_TABLE_REORG | ALTER_REBUILD_PARTITION
        )
      ) &&
      memcmp(name + strlen(name) - 5, "#TMP#", 5)
    ) {
      need_lock = TRUE;
      if (
        !(table_tables = spider_open_sys_table(
          current_thd, SPIDER_SYS_TABLES_TABLE_NAME_STR,
          SPIDER_SYS_TABLES_TABLE_NAME_LEN, TRUE, &open_tables_backup, TRUE,
          &error_num))
      ) {
        goto error;
      }
      if (
        (error_num = spider_insert_tables(table_tables, &tmp_share))
      ) {
        goto error;
      }
      spider_close_sys_table(current_thd, table_tables,
        &open_tables_backup, TRUE);
      table_tables = NULL;
    }
  }

  spider_free_share_alloc(&tmp_share);
  DBUG_RETURN(0);

error:
  if (table_tables)
    spider_close_sys_table(current_thd, table_tables,
      &open_tables_backup, need_lock);
  spider_free_share_alloc(&tmp_share);
error_alter_before_unlock:
error_get_trx:
  DBUG_RETURN(error_num);
}

void ha_spider::update_create_info(
  HA_CREATE_INFO* create_info
) {
  DBUG_ENTER("ha_spider::update_create_info");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",
    ("spider create_info->connect_string=%s",
    create_info->connect_string.str));
  DBUG_VOID_RETURN;
}

int ha_spider::rename_table(
  const char *from,
  const char *to
) {
  int error_num, roop_count, old_link_count, from_len = strlen(from);
  THD *thd = ha_thd();
  uint sql_command = thd_sql_command(thd);
  SPIDER_TRX *trx;
  TABLE *table_tables = NULL;
  SPIDER_ALTER_TABLE *alter_table_from, *alter_table_to;
  Open_tables_state open_tables_backup;
  bool need_lock = FALSE;
  DBUG_ENTER("ha_spider::rename_table");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider from=%s", from));
  DBUG_PRINT("info",("spider to=%s", to));
  if (!(trx = spider_get_trx(thd, &error_num)))
    goto error;
  if (
    trx->locked_connections &&
    /* SQLCOM_RENAME_TABLE doesn't come here */
    sql_command == SQLCOM_ALTER_TABLE
  ) {
    my_message(ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM,
      ER_SPIDER_ALTER_BEFORE_UNLOCK_STR, MYF(0));
    error_num = ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM;
    goto error;
  }
  if (
    sql_command == SQLCOM_RENAME_TABLE ||
    (sql_command == SQLCOM_ALTER_TABLE && !trx->tmp_flg) ||
    !(alter_table_from =
      (SPIDER_ALTER_TABLE*) hash_search(&trx->trx_alter_table_hash,
      (uchar*) from, from_len))
  ) {
    if (
      !(table_tables = spider_open_sys_table(
        current_thd, SPIDER_SYS_TABLES_TABLE_NAME_STR,
        SPIDER_SYS_TABLES_TABLE_NAME_LEN, TRUE, &open_tables_backup, FALSE,
        &error_num))
    ) {
      goto error;
    }
    if (
      (error_num = spider_update_tables_name(
        table_tables, from, to, &old_link_count))
    ) {
      goto error;
    }
    spider_close_sys_table(current_thd, table_tables,
      &open_tables_backup, FALSE);
    table_tables = NULL;

    /* release table mon list */
    for (roop_count = 0; roop_count < old_link_count; roop_count++)
      spider_release_ping_table_mon_list(from, from_len, roop_count);
  } else if (sql_command == SQLCOM_ALTER_TABLE)
  {
    int to_len = strlen(to);
    DBUG_PRINT("info",("spider alter_table_from=%x", alter_table_from));
    if ((alter_table_to =
      (SPIDER_ALTER_TABLE*) hash_search(&trx->trx_alter_table_hash,
      (uchar*) to, to_len))
    ) {
      DBUG_PRINT("info",("spider copy link_statuses"));
      int link_count = alter_table_from->link_count;
      if (link_count > alter_table_to->link_count)
        link_count = alter_table_to->link_count;
      for (roop_count = 0; roop_count < link_count; roop_count++)
      {
        if (alter_table_from->tmp_link_statuses[roop_count] <=
          SPIDER_LINK_STATUS_NO_CHANGE)
          alter_table_from->tmp_link_statuses[roop_count] =
            alter_table_to->tmp_link_statuses[roop_count];
      }
    }

    DBUG_PRINT("info",
      ("spider alter_info.flags=%u", thd->lex->alter_info.flags));
    if (
      (thd->lex->alter_info.flags &
        (
          ALTER_ADD_PARTITION | ALTER_DROP_PARTITION |
          ALTER_COALESCE_PARTITION | ALTER_REORGANIZE_PARTITION |
          ALTER_TABLE_REORG | ALTER_REBUILD_PARTITION
        )
      )
    )
      need_lock = TRUE;

    if (
      !(table_tables = spider_open_sys_table(
        current_thd, SPIDER_SYS_TABLES_TABLE_NAME_STR,
        SPIDER_SYS_TABLES_TABLE_NAME_LEN, TRUE, &open_tables_backup, need_lock,
        &error_num))
    ) {
      goto error;
    }

    if (alter_table_from->now_create)
    {
      SPIDER_SHARE tmp_share;
      tmp_share.table_name = (char*) to;
      tmp_share.table_name_length = to_len;
      tmp_share.priority = alter_table_from->tmp_priority;
      tmp_share.link_count = alter_table_from->link_count;
      memcpy(&tmp_share.alter_table, alter_table_from,
        sizeof(*alter_table_from));
      if (
        (error_num = spider_insert_tables(table_tables, &tmp_share))
      ) {
        goto error;
      }
    } else {
      if (
        (error_num = spider_update_tables_priority(
          table_tables, alter_table_from, to, &old_link_count))
      ) {
        goto error;
      }
    }
    spider_close_sys_table(current_thd, table_tables,
      &open_tables_backup, need_lock);
    table_tables = NULL;

    if (!alter_table_from->now_create)
    {
      /* release table mon list */
      for (roop_count = 0; roop_count < old_link_count; roop_count++)
        spider_release_ping_table_mon_list(to, to_len, roop_count);
    }
/*
    spider_free_trx_alter_table_alloc(trx, alter_table_from);
*/
  }

  DBUG_RETURN(0);

error:
  if (table_tables)
    spider_close_sys_table(current_thd, table_tables,
      &open_tables_backup, need_lock);
  DBUG_RETURN(error_num);
}

int ha_spider::delete_table(
  const char *name
) {
  int error_num;
  THD *thd = ha_thd();
  SPIDER_TRX *trx;
  TABLE *table_tables = NULL;
  uint sql_command = thd_sql_command(thd);
  SPIDER_ALTER_TABLE *alter_table;
  Open_tables_state open_tables_backup;
  bool need_lock = FALSE;
  DBUG_ENTER("ha_spider::delete_table");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider name=%s", name));
  if (!(trx = spider_get_trx(thd, &error_num)))
    goto error;
  if (
    trx->locked_connections &&
    /* SQLCOM_DROP_DB doesn't come here */
    (
      sql_command == SQLCOM_DROP_TABLE ||
      sql_command == SQLCOM_ALTER_TABLE
    )
  ) {
    my_message(ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM,
      ER_SPIDER_ALTER_BEFORE_UNLOCK_STR, MYF(0));
    error_num = ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM;
    goto error;
  }
  if (sql_command == SQLCOM_DROP_TABLE ||
    sql_command == SQLCOM_DROP_DB ||
    sql_command == SQLCOM_ALTER_TABLE ||
    sql_command == SQLCOM_CREATE_TABLE)
  {
    int roop_count, old_link_count = 0, name_len = strlen(name);
    if (
      sql_command == SQLCOM_ALTER_TABLE &&
      (alter_table =
        (SPIDER_ALTER_TABLE*) hash_search(&trx->trx_alter_table_hash,
        (uchar*) name, name_len)) &&
      alter_table->now_create
    )
      DBUG_RETURN(0);

    DBUG_PRINT("info",
      ("spider alter_info.flags=%u", thd->lex->alter_info.flags));
    if (
      sql_command == SQLCOM_ALTER_TABLE &&
      (thd->lex->alter_info.flags &
        (
          ALTER_ADD_PARTITION | ALTER_DROP_PARTITION |
          ALTER_COALESCE_PARTITION | ALTER_REORGANIZE_PARTITION |
          ALTER_TABLE_REORG | ALTER_REBUILD_PARTITION
        )
      )
    )
      need_lock = TRUE;

    if (
      !(table_tables = spider_open_sys_table(
        current_thd, SPIDER_SYS_TABLES_TABLE_NAME_STR,
        SPIDER_SYS_TABLES_TABLE_NAME_LEN, TRUE, &open_tables_backup, need_lock,
        &error_num))
    ) {
      goto error;
    }
    if (
      (error_num = spider_delete_tables(
        table_tables, name, &old_link_count))
    ) {
      goto error;
    }
    spider_close_sys_table(current_thd, table_tables,
      &open_tables_backup, need_lock);
    table_tables = NULL;

    /* release table mon list */
    for (roop_count = 0; roop_count < old_link_count; roop_count++)
      spider_release_ping_table_mon_list(name, name_len, roop_count);
  }

  DBUG_RETURN(0);

error:
  if (table_tables)
    spider_close_sys_table(current_thd, table_tables,
      &open_tables_backup, need_lock);
  DBUG_RETURN(error_num);
}

bool ha_spider::is_crashed() const
{
  DBUG_ENTER("ha_spider::is_crashed");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(FALSE);
}

bool ha_spider::auto_repair() const
{
  DBUG_ENTER("ha_spider::auto_repair");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(FALSE);
}

int ha_spider::disable_indexes(
  uint mode
) {
  DBUG_ENTER("ha_spider::disable_indexes");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(spider_db_disable_keys(this));
}

int ha_spider::enable_indexes(
  uint mode
) {
  DBUG_ENTER("ha_spider::enable_indexes");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(spider_db_enable_keys(this));
}


int ha_spider::check(
  THD* thd,
  HA_CHECK_OPT* check_opt
) {
  DBUG_ENTER("ha_spider::check");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(spider_db_check_table(this, check_opt));
}

int ha_spider::repair(
  THD* thd,
  HA_CHECK_OPT* check_opt
) {
  DBUG_ENTER("ha_spider::repair");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(spider_db_repair_table(this, check_opt));
}

bool ha_spider::check_and_repair(
  THD *thd
) {
  HA_CHECK_OPT check_opt;
  DBUG_ENTER("ha_spider::check_and_repair");
  DBUG_PRINT("info",("spider this=%x", this));
  check_opt.init();
  check_opt.flags = T_MEDIUM;
  if (spider_db_check_table(this, &check_opt))
  {
    check_opt.flags = T_QUICK;
    if (spider_db_repair_table(this, &check_opt))
      DBUG_RETURN(TRUE);
  }
  DBUG_RETURN(FALSE);
}

int ha_spider::analyze(
  THD* thd,
  HA_CHECK_OPT* check_opt
) {
  DBUG_ENTER("ha_spider::analyze");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(spider_db_analyze_table(this));
}

int ha_spider::optimize(
  THD* thd,
  HA_CHECK_OPT* check_opt
) {
  DBUG_ENTER("ha_spider::optimize");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_RETURN(spider_db_optimize_table(this));
}

bool ha_spider::is_fatal_error(
  int error_num,
  uint flags
) {
  DBUG_ENTER("ha_spider::is_fatal_error");
  DBUG_PRINT("info",("spider error_num=%d", error_num));
  DBUG_PRINT("info",("spider flags=%u", flags));
  if (
    !handler::is_fatal_error(error_num, flags)
  ) {
    DBUG_PRINT("info",("spider FALSE"));
    DBUG_RETURN(FALSE);
  }
  DBUG_PRINT("info",("spider TRUE"));
  DBUG_RETURN(TRUE);
}

const COND *ha_spider::cond_push(
  const COND *cond
) {
  DBUG_ENTER("ha_spider::cond_push");
  if (cond)
  {
    SPIDER_CONDITION *tmp_cond;
    if (!(tmp_cond = (SPIDER_CONDITION *)
      my_malloc(sizeof(*tmp_cond), MYF(MY_WME | MY_ZEROFILL)))
    )
      DBUG_RETURN(cond);
    tmp_cond->cond = (COND *) cond;
    tmp_cond->next = condition;
    condition = tmp_cond;
  }
  DBUG_RETURN(NULL);
}

void ha_spider::cond_pop()
{
  DBUG_ENTER("ha_spider::cond_pop");
  if (condition)
  {
    SPIDER_CONDITION *tmp_cond = condition->next;
    my_free(condition, MYF(0));
    condition = tmp_cond;
  }
  DBUG_VOID_RETURN;
}

st_table *ha_spider::get_table()
{
  DBUG_ENTER("ha_spider::get_table");
  DBUG_RETURN(table);
}

void ha_spider::set_select_column_mode()
{
  int roop_count;
  KEY *key_info;
  KEY_PART_INFO *key_part;
  Field *field;
  THD *thd = trx->thd;
  DBUG_ENTER("ha_spider::set_select_column_mode");
#ifndef DBUG_OFF
  for (roop_count = 0; roop_count < (table_share->fields + 7) / 8;
    roop_count++)
    DBUG_PRINT("info", ("spider bitmap is %x",
      ((uchar *) table->read_set->bitmap)[roop_count]));
#endif
  select_column_mode = THDVAR(thd, select_column_mode) == -1 ?
    share->select_column_mode : THDVAR(thd, select_column_mode);
  if (select_column_mode)
  {
#ifdef WITH_PARTITION_STORAGE_ENGINE
    if (
      partition_handler_share &&
      partition_handler_share->searched_bitmap
    ) {
      memcpy(searched_bitmap, partition_handler_share->searched_bitmap,
        (table_share->fields + 7) / 8);
      partition_handler_share->between_flg = FALSE;
      DBUG_PRINT("info",("spider copy searched_bitmap"));
    } else {
#endif
      for (roop_count = 0; roop_count < (table_share->fields + 7) / 8;
        roop_count++)
      {
        searched_bitmap[roop_count] =
          ((uchar *) table->read_set->bitmap)[roop_count] |
          ((uchar *) table->write_set->bitmap)[roop_count];
        DBUG_PRINT("info",("spider roop_count=%d", roop_count));
        DBUG_PRINT("info",("spider searched_bitmap=%d",
          searched_bitmap[roop_count]));
        DBUG_PRINT("info",("spider read_set=%d",
          ((uchar *) table->read_set->bitmap)[roop_count]));
        DBUG_PRINT("info",("spider write_set=%d",
          ((uchar *) table->write_set->bitmap)[roop_count]));
      }
      if (result_list.lock_type == F_WRLCK && sql_command != SQLCOM_SELECT)
      {
        if (table_share->primary_key == MAX_KEY)
        {
          /* need all columns */
          for (roop_count = 0; roop_count < table_share->fields; roop_count++)
            spider_set_bit(searched_bitmap, roop_count);
        } else {
          /* need primary key columns */
          key_info = &table_share->key_info[table_share->primary_key];
          key_part = key_info->key_part;
          for (roop_count = 0; roop_count < key_info->key_parts; roop_count++)
          {
            field = key_part[roop_count].field;
            spider_set_bit(searched_bitmap, field->field_index);
          }
        }
#ifndef DBUG_OFF
        for (roop_count = 0; roop_count < (table_share->fields + 7) / 8;
          roop_count++)
          DBUG_PRINT("info", ("spider change bitmap is %x",
            searched_bitmap[roop_count]));
#endif
      }
#ifdef WITH_PARTITION_STORAGE_ENGINE
      if (partition_handler_share)
      {
        partition_handler_share->searched_bitmap = searched_bitmap;
        partition_handler_share->between_flg = TRUE;
        DBUG_PRINT("info",("spider set searched_bitmap"));
      }
    }
#endif
  }
  DBUG_VOID_RETURN;
}

#ifdef WITH_PARTITION_STORAGE_ENGINE
void ha_spider::check_select_column(bool rnd)
{
  THD *thd = trx->thd;
  DBUG_ENTER("ha_spider::check_select_column");
  select_column_mode = THDVAR(thd, select_column_mode) == -1 ?
    share->select_column_mode : THDVAR(thd, select_column_mode);
  if (select_column_mode && partition_handler_share)
  {
    if (!rnd)
    {
      if (partition_handler_share->between_flg)
      {
        memcpy(partition_handler_share->idx_read_bitmap,
          table->read_set->bitmap, (table_share->fields + 7) / 8);
        memcpy(partition_handler_share->idx_write_bitmap,
          table->write_set->bitmap, (table_share->fields + 7) / 8);
        partition_handler_share->between_flg = FALSE;
        partition_handler_share->idx_bitmap_is_set = TRUE;
        DBUG_PRINT("info",("spider set idx_bitmap"));
      } else if (partition_handler_share->idx_bitmap_is_set)
      {
        memcpy(table->read_set->bitmap,
          partition_handler_share->idx_read_bitmap,
          (table_share->fields + 7) / 8);
        memcpy(table->write_set->bitmap,
          partition_handler_share->idx_write_bitmap,
          (table_share->fields + 7) / 8);
        DBUG_PRINT("info",("spider copy idx_bitmap"));
      }
    } else {
      if (
        !partition_handler_share->rnd_bitmap_is_set &&
        (
          partition_handler_share->between_flg ||
          partition_handler_share->idx_bitmap_is_set
        )
      ) {
        memcpy(partition_handler_share->rnd_read_bitmap,
          table->read_set->bitmap, (table_share->fields + 7) / 8);
        memcpy(partition_handler_share->rnd_write_bitmap,
          table->write_set->bitmap, (table_share->fields + 7) / 8);
        partition_handler_share->between_flg = FALSE;
        partition_handler_share->rnd_bitmap_is_set = TRUE;
        DBUG_PRINT("info",("spider set rnd_bitmap"));
      } else if (partition_handler_share->rnd_bitmap_is_set)
      {
        memcpy(table->read_set->bitmap,
          partition_handler_share->rnd_read_bitmap,
          (table_share->fields + 7) / 8);
        memcpy(table->write_set->bitmap,
          partition_handler_share->rnd_write_bitmap,
          (table_share->fields + 7) / 8);
        DBUG_PRINT("info",("spider copy rnd_bitmap"));
      }
    }
  }
  DBUG_VOID_RETURN;
}
#endif

bool ha_spider::check_and_start_bulk_update(
  spider_bulk_upd_start bulk_upd_start
) {
  DBUG_ENTER("ha_spider::check_and_start_bulk_update");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider bulk_update_start=%d",
    result_list.bulk_update_start));
  if (result_list.bulk_update_start == SPD_BU_NOT_START)
  {
    THD *thd = ha_thd();
    int bulk_update_mode = THDVAR(thd, bulk_update_mode) == -1 ?
      share->bulk_update_mode : THDVAR(thd, bulk_update_mode);
    longlong split_read = spider_split_read_param(this);
    result_list.bulk_update_size = THDVAR(thd, bulk_update_size) == -1 ?
      share->bulk_update_size : THDVAR(thd, bulk_update_size);
#ifndef WITHOUT_SPIDER_BG_SEARCH
    int bgs_mode = THDVAR(thd, bgs_mode) < 0 ?
      share->bgs_mode : THDVAR(thd, bgs_mode);
#endif
    if (
#ifndef WITHOUT_SPIDER_BG_SEARCH
      bgs_mode ||
#endif
      split_read != 9223372036854775807LL
    )
      result_list.bulk_update_mode = 2;
    else
      result_list.bulk_update_mode = bulk_update_mode;
    result_list.bulk_update_start = bulk_upd_start;
    DBUG_RETURN(FALSE);
  }
  DBUG_RETURN(TRUE);
}

int ha_spider::check_and_end_bulk_update(
  spider_bulk_upd_start bulk_upd_start
) {
  int error_num = 0;
  DBUG_ENTER("ha_spider::check_and_end_bulk_update");
  DBUG_PRINT("info",("spider this=%x", this));
  DBUG_PRINT("info",("spider bulk_update_start=%d",
    result_list.bulk_update_start));
  DBUG_PRINT("info",("spider bulk_update_mode=%d",
    result_list.bulk_update_mode));
  if (result_list.bulk_update_start == bulk_upd_start)
  {
    if (result_list.bulk_update_mode)
      error_num = spider_db_bulk_update_end(this);
    result_list.bulk_update_size = 0;
    result_list.bulk_update_mode = 0;
    result_list.bulk_update_start = SPD_BU_NOT_START;
  }
  DBUG_RETURN(error_num);
}
