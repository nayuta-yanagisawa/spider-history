/* Copyright (C) 2008-2011 Kentoku Shiba

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
#include <my_getopt.h>
#include "spd_err.h"
#include "spd_db_include.h"
#include "spd_include.h"
#include "ha_spider.h"
#include "spd_table.h"
#include "spd_trx.h"

extern volatile ulonglong spider_mon_table_cache_version;
extern volatile ulonglong spider_mon_table_cache_version_req;

#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
static int spider_direct_update(THD *thd, SHOW_VAR *var, char *buff)
{
  int error_num = 0;
  SPIDER_TRX *trx;
  DBUG_ENTER("spider_direct_update");
  var->type = SHOW_LONGLONG;
  if ((trx = spider_get_trx(thd, &error_num)))
    var->value = (char *) &trx->direct_update_count;
  DBUG_RETURN(error_num);
}

static int spider_direct_delete(THD *thd, SHOW_VAR *var, char *buff)
{
  int error_num = 0;
  SPIDER_TRX *trx;
  DBUG_ENTER("spider_direct_delete");
  var->type = SHOW_LONGLONG;
  if ((trx = spider_get_trx(thd, &error_num)))
    var->value = (char *) &trx->direct_delete_count;
  DBUG_RETURN(error_num);
}
#endif

static int spider_direct_order_limit(THD *thd, SHOW_VAR *var, char *buff)
{
  int error_num = 0;
  SPIDER_TRX *trx;
  DBUG_ENTER("spider_direct_order_limit");
  var->type = SHOW_LONGLONG;
  if ((trx = spider_get_trx(thd, &error_num)))
    var->value = (char *) &trx->direct_order_limit_count;
  DBUG_RETURN(error_num);
}

struct st_mysql_show_var spider_status_variables[] =
{
  {"Spider_mon_table_cache_version",
    (char *) &spider_mon_table_cache_version, SHOW_LONGLONG},
  {"Spider_mon_table_cache_version_req",
    (char *) &spider_mon_table_cache_version_req, SHOW_LONGLONG},
#ifdef HANDLER_HAS_DIRECT_UPDATE_ROWS
  {"Spider_direct_update", (char *) &spider_direct_update, SHOW_FUNC},
  {"Spider_direct_delete", (char *) &spider_direct_delete, SHOW_FUNC},
#endif
  {"Spider_direct_order_limit",
    (char *) &spider_direct_order_limit, SHOW_FUNC},
  {NullS, NullS, SHOW_LONG}
};

typedef DECLARE_MYSQL_THDVAR_SIMPLE(thdvar_int_t, int);
#if MYSQL_VERSION_ID < 50500
extern bool throw_bounds_warning(THD *thd, bool fixed, bool unsignd,
  const char *name, long long val);
#else
extern bool throw_bounds_warning(THD *thd, const char *name, bool fixed,
  bool is_unsignd, longlong v);
#endif

my_bool spider_support_xa;
uint spider_table_init_error_interval;
int spider_use_table_charset;
uint spider_udf_table_lock_mutex_count;
uint spider_udf_table_mon_mutex_count;
char *spider_remote_access_charset;
int spider_remote_autocommit;
char *spider_remote_time_zone;
int spider_remote_sql_log_off;
int spider_remote_trx_isolation;
my_bool spider_connect_mutex;
int spider_udf_ct_bulk_insert_interval;
long long spider_udf_ct_bulk_insert_rows;

static MYSQL_SYSVAR_BOOL(
  support_xa,
  spider_support_xa,
  PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
  "XA support",
  NULL,
  NULL,
  TRUE
);

static MYSQL_SYSVAR_BOOL(
  connect_mutex,
  spider_connect_mutex,
  PLUGIN_VAR_OPCMDARG,
  "Use mutex at connecting",
  NULL,
  NULL,
  FALSE
);

/*
  0-: interval
 */
static MYSQL_SYSVAR_UINT(
  table_init_error_interval,
  spider_table_init_error_interval,
  PLUGIN_VAR_RQCMDARG,
  "Return same error code until interval passes if table init is failed",
  NULL,
  NULL,
  1,
  0,
  4294967295U,
  0
);

/*
 -1 :use table parameter
  0 :use utf8
  1 :use table charset
 */
static MYSQL_SYSVAR_INT(
  use_table_charset,
  spider_use_table_charset,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Use table charset for remote access",
  NULL,
  NULL,
  -1,
  -1,
  1,
  0
);

/*
  0: no recycle
  1: recycle in instance
  2: recycle in thread
 */
MYSQL_THDVAR_UINT(
  conn_recycle_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Connection recycle mode", /* comment */
  NULL, /* check */
  NULL, /* update */
  0, /* def */
  0, /* min */
  2, /* max */
  0 /* blk */
);

/*
  0: weak
  1: strict
 */
MYSQL_THDVAR_UINT(
  conn_recycle_strict, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Strict connection recycle", /* comment */
  NULL, /* check */
  NULL, /* update */
  0, /* def */
  0, /* min */
  1, /* max */
  0 /* blk */
);

/*
  FALSE: no sync
  TRUE:  sync
 */
MYSQL_THDVAR_BOOL(
  sync_trx_isolation, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Sync transaction isolation level", /* comment */
  NULL, /* check */
  NULL, /* update */
  TRUE /* def */
);

/*
  FALSE: no use
  TRUE:  use
 */
MYSQL_THDVAR_BOOL(
  use_consistent_snapshot, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Use start transaction with consistent snapshot", /* comment */
  NULL, /* check */
  NULL, /* update */
  FALSE /* def */
);

/*
  FALSE: off
  TRUE:  on
 */
MYSQL_THDVAR_BOOL(
  internal_xa, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Use inner xa transaction", /* comment */
  NULL, /* check */
  NULL, /* update */
  FALSE /* def */
);

/*
  0 :err when use a spider table
  1 :err when start trx
  2 :start trx with snapshot on remote server(not use xa)
  3 :start xa on remote server(not use trx with snapshot)
 */
MYSQL_THDVAR_UINT(
  internal_xa_snapshot, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Action of inner xa and snapshot both using", /* comment */
  NULL, /* check */
  NULL, /* update */
  0, /* def */
  0, /* min */
  3, /* max */
  0 /* blk */
);

/*
  0 :off
  1 :continue prepare, commit, rollback if xid not found return
  2 :continue prepare, commit, rollback if all error return
 */
MYSQL_THDVAR_UINT(
  force_commit, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Force prepare, commit, rollback mode", /* comment */
  NULL, /* check */
  NULL, /* update */
  0, /* def */
  0, /* min */
  2, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0-:offset
 */
MYSQL_THDVAR_LONGLONG(
  internal_offset, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Internal offset", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0-:limit
 */
MYSQL_THDVAR_LONGLONG(
  internal_limit, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Internal limit", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0-:number of rows at a select
 */
MYSQL_THDVAR_LONGLONG(
  split_read, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Number of rows at a select", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
  -1 :use table parameter
   0 :doesn't use "offset" and "limit" for "split_read"
   1-:magnification
 */
MYSQL_THDVAR_INT(
  semi_split_read, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Use offset and limit parameter in SQL for split_read parameter.", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2147483647, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0-:the limit value
 */
MYSQL_THDVAR_LONGLONG(
  semi_split_read_limit, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "The limit value for semi_split_read", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :no alloc
  1-:alloc size
 */
MYSQL_THDVAR_INT(
  init_sql_alloc_size, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Initial sql string alloc size", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2147483647, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :off
  1 :on
 */
MYSQL_THDVAR_INT(
  reset_sql_alloc, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Reset sql string alloc after execute", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :off
  1 :on
 */
MYSQL_THDVAR_INT(
  multi_split_read, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Sprit read mode for multi range", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2147483647, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0-:max order columns
 */
MYSQL_THDVAR_INT(
  max_order, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Max columns for order by", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  32767, /* max */
  0 /* blk */
);

/*
 -1 :off
  0 :read uncommitted
  1 :read committed
  2 :repeatable read
  3 :serializable
 */
MYSQL_THDVAR_INT(
  semi_trx_isolation, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Transaction isolation level during execute a sql", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  3, /* max */
  0 /* blk */
);

static int spider_param_semi_table_lock_check(
  MYSQL_THD thd,
  struct st_mysql_sys_var *var,
  void *save,
  struct st_mysql_value *value
) {
  int error_num;
  SPIDER_TRX *trx;
  my_bool fixed;
  long long tmp;
  struct my_option options;
  DBUG_ENTER("spider_param_semi_table_lock_check");
  if (!(trx = spider_get_trx((THD *) thd, &error_num)))
    DBUG_RETURN(error_num);
  if (trx->locked_connections)
  {
    my_message(ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM,
      ER_SPIDER_ALTER_BEFORE_UNLOCK_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM);
  }
  value->val_int(value, &tmp);
  options.sub_size = 0;
  options.var_type = GET_INT;
  options.def_value = ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->def_val;
  options.min_value = ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->min_val;
  options.max_value = ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->max_val;
  options.block_size =
    (long) ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->blk_sz;
  options.arg_type = REQUIRED_ARG;
  *((int *) save) = (int) getopt_ll_limit_value(tmp, &options, &fixed);
#if MYSQL_VERSION_ID < 50500
  DBUG_RETURN(throw_bounds_warning(thd, fixed, FALSE,
    ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->name, (long long) tmp));
#else
  DBUG_RETURN(throw_bounds_warning(thd,
    ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->name, fixed, FALSE,
    (longlong) tmp));
#endif
}

/*
  0 :off
  1 :on
 */
MYSQL_THDVAR_INT(
  semi_table_lock, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Table lock during execute a sql", /* comment */
  &spider_param_semi_table_lock_check, /* check */
  NULL, /* update */
  1, /* def */
  0, /* min */
  1, /* max */
  0 /* blk */
);

static int spider_param_semi_table_lock_connection_check(
  MYSQL_THD thd,
  struct st_mysql_sys_var *var,
  void *save,
  struct st_mysql_value *value
) {
  int error_num;
  SPIDER_TRX *trx;
  my_bool fixed;
  long long tmp;
  struct my_option options;
  DBUG_ENTER("spider_param_semi_table_lock_connection_check");
  if (!(trx = spider_get_trx((THD *) thd, &error_num)))
    DBUG_RETURN(error_num);
  if (trx->locked_connections)
  {
    my_message(ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM,
      ER_SPIDER_ALTER_BEFORE_UNLOCK_STR, MYF(0));
    DBUG_RETURN(ER_SPIDER_ALTER_BEFORE_UNLOCK_NUM);
  }
  value->val_int(value, &tmp);
  options.sub_size = 0;
  options.var_type = GET_INT;
  options.def_value = ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->def_val;
  options.min_value = ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->min_val;
  options.max_value = ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->max_val;
  options.block_size =
    (long) ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->blk_sz;
  options.arg_type = REQUIRED_ARG;
  *((int *) save) = (int) getopt_ll_limit_value(tmp, &options, &fixed);
#if MYSQL_VERSION_ID < 50500
  DBUG_RETURN(throw_bounds_warning(thd, fixed, FALSE,
    ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->name, (long long) tmp));
#else
  DBUG_RETURN(throw_bounds_warning(thd,
    ((MYSQL_SYSVAR_NAME(thdvar_int_t) *) var)->name, fixed, FALSE,
    (longlong) tmp));
#endif
}

/*
 -1 :off
  0 :use same connection
  1 :use different connection
 */
MYSQL_THDVAR_INT(
  semi_table_lock_connection, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Use different connection if semi_table_lock is enabled", /* comment */
  &spider_param_semi_table_lock_connection_check, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

/*
  0-:block_size
 */
MYSQL_THDVAR_UINT(
  block_size, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Index block size", /* comment */
  NULL, /* check */
  NULL, /* update */
  16384, /* def */
  0, /* min */
  4294967295U, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :off
  1 :lock in share mode
  2 :for update
 */
MYSQL_THDVAR_INT(
  selupd_lock_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Lock for select with update", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2, /* max */
  0 /* blk */
);

/*
  FALSE: no sync
  TRUE:  sync
 */
MYSQL_THDVAR_BOOL(
  sync_autocommit, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Sync autocommit", /* comment */
  NULL, /* check */
  NULL, /* update */
  TRUE /* def */
);

/*
  FALSE: no sync
  TRUE:  sync
 */
MYSQL_THDVAR_BOOL(
  sync_time_zone, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Sync time_zone", /* comment */
  NULL, /* check */
  NULL, /* update */
  FALSE /* def */
);

/*
  FALSE: sql_log_off = 0
  TRUE:  sql_log_off = 1
 */
MYSQL_THDVAR_BOOL(
  internal_sql_log_off, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Sync sql_log_off", /* comment */
  NULL, /* check */
  NULL, /* update */
  TRUE /* def */
);

/*
 -1 :use table parameter
  0-:bulk insert size
 */
MYSQL_THDVAR_INT(
  bulk_size, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Bulk insert size", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2147483647, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 : Send "update" and "delete" statements one by one.
  1 : Send collected multiple "update" and "delete" statements.
      (Collected statements are sent one by one)
  2 : Send collected multiple "update" and "delete" statements.
      (Collected statements are sent together)
 */
MYSQL_THDVAR_INT(
  bulk_update_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "The mode of bulk updating and deleting", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0-:bulk update size
 */
MYSQL_THDVAR_INT(
  bulk_update_size, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Bulk update size", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2147483647, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :off
  1 :on
 */
MYSQL_THDVAR_INT(
  internal_optimize, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Execute optimize to remote server", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :off
  1 :on
 */
MYSQL_THDVAR_INT(
  internal_optimize_local, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Execute optimize to remote server with local", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

/*
  FALSE: off
  TRUE:  on
 */
MYSQL_THDVAR_BOOL(
  use_flash_logs, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Execute flush logs to remote server", /* comment */
  NULL, /* check */
  NULL, /* update */
  FALSE /* def */
);

/*
  0 :off
  1 :flush tables with read lock
  2 :flush tables another connection
 */
MYSQL_THDVAR_INT(
  use_snapshot_with_flush_tables, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Execute optimize to remote server with local", /* comment */
  NULL, /* check */
  NULL, /* update */
  0, /* def */
  0, /* min */
  2, /* max */
  0 /* blk */
);

/*
  FALSE: off
  TRUE:  on
 */
MYSQL_THDVAR_BOOL(
  use_all_conns_snapshot, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "When start trx with snapshot, it send to all connections", /* comment */
  NULL, /* check */
  NULL, /* update */
  FALSE /* def */
);

/*
  FALSE: off
  TRUE:  on
 */
MYSQL_THDVAR_BOOL(
  lock_exchange, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Exchange select lock to lock tables", /* comment */
  NULL, /* check */
  NULL, /* update */
  FALSE /* def */
);

/*
  FALSE: off
  TRUE:  on
 */
MYSQL_THDVAR_BOOL(
  internal_unlock, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Unlock tables for using connections in sql", /* comment */
  NULL, /* check */
  NULL, /* update */
  FALSE /* def */
);

/*
  FALSE: off
  TRUE:  on
 */
MYSQL_THDVAR_BOOL(
  semi_trx, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Take a transaction during execute a sql", /* comment */
  NULL, /* check */
  NULL, /* update */
  TRUE /* def */
);

/*
 -1 :use table parameter
  0-:seconds of timeout
 */
MYSQL_THDVAR_INT(
  net_timeout, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Wait timeout of receiving data from remote server", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2147483647, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :It acquires it collectively.
  1 :Acquisition one by one.If it discontinues once, and it will need
     it later, it retrieves it again when there is interrupt on the way.
  2 :Acquisition one by one.Interrupt is waited for until end of getting
     result when there is interrupt on the way.
 */
MYSQL_THDVAR_INT(
  quick_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "The retrieval result from a remote server is acquired by acquisition one by one", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0-:number of records
 */
MYSQL_THDVAR_LONGLONG(
  quick_page_size, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Number of records in a page when acquisition one by one", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :It doesn't use low memory mode.
  1 :It uses low memory mode.
 */
MYSQL_THDVAR_INT(
  low_mem_read, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Use low memory mode when SQL(SELECT) internally issued to a remote server is executed and get a result list", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :Use index columns if select statement can solve by using index,
     otherwise use all columns.
  1 :Use columns that are judged necessary.
 */
MYSQL_THDVAR_INT(
  select_column_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "The mode of using columns at select clause", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

#ifndef WITHOUT_SPIDER_BG_SEARCH
/*
 -1 :use table parameter
  0 :background search is disabled
  1 :background search is used if search with no lock
  2 :background search is used if search with no lock or shared lock
  3 :background search is used regardless of the lock
 */
MYSQL_THDVAR_INT(
  bgs_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Mode of background search", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  3, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :records is gotten usually
  1-:number of records
 */
MYSQL_THDVAR_LONGLONG(
  bgs_first_read, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Number of first read records when background search is used", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :records is gotten usually
  1-:number of records
 */
MYSQL_THDVAR_LONGLONG(
  bgs_second_read, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Number of second read records when background search is used", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);
#endif

/*
 -1 :use table parameter
  0 :records is gotten usually
  1-:number of records
 */
MYSQL_THDVAR_LONGLONG(
  first_read, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Number of first read records", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :records is gotten usually
  1-:number of records
 */
MYSQL_THDVAR_LONGLONG(
  second_read, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Number of second read records", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :always get the newest information
  1-:interval
 */
MYSQL_THDVAR_INT(
  crd_interval, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Interval of cardinality confirmation.(second)", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2147483647, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :use table parameter
  1 :use show command
  2 :use information schema
  3 :use explain
 */
MYSQL_THDVAR_INT(
  crd_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Mode of cardinality confirmation.", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  3, /* max */
  0 /* blk */
);

#ifdef WITH_PARTITION_STORAGE_ENGINE
/*
 -1 :use table parameter
  0 :No synchronization.
  1 :Cardinality is synchronized when opening a table.
     Then no synchronization.
  2 :Synchronization.
 */
MYSQL_THDVAR_INT(
  crd_sync, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Cardinality synchronization in partitioned table.", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2, /* max */
  0 /* blk */
);
#endif

/*
 -1 :use table parameter
  0 :The crd_weight is used as a fixed value.
  1 :The crd_weight is used as an addition value.
  2 :The crd_weight is used as a multiplication value.
 */
MYSQL_THDVAR_INT(
  crd_type, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Type of cardinality calculation.", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0-:weight
 */
MYSQL_THDVAR_INT(
  crd_weight, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Weight coefficient to calculate effectiveness of index from cardinality of column.", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2147483647, /* max */
  0 /* blk */
);

#ifndef WITHOUT_SPIDER_BG_SEARCH
/*
 -1 :use table parameter
  0 :Background confirmation is disabled
  1 :Background confirmation is enabled
 */
MYSQL_THDVAR_INT(
  crd_bg_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Mode of cardinality confirmation at background.", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);
#endif

/*
 -1 :use table parameter
  0 :always get the newest information
  1-:interval
 */
MYSQL_THDVAR_INT(
  sts_interval, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Interval of table state confirmation.(second)", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2147483647, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :use table parameter
  1 :use show command
  2 :use information schema
 */
MYSQL_THDVAR_INT(
  sts_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Mode of table state confirmation.", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2, /* max */
  0 /* blk */
);

#ifdef WITH_PARTITION_STORAGE_ENGINE
/*
 -1 :use table parameter
  0 :No synchronization.
  1 :Table state is synchronized when opening a table.
     Then no synchronization.
  2 :Synchronization.
 */
MYSQL_THDVAR_INT(
  sts_sync, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Table state synchronization in partitioned table.", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2, /* max */
  0 /* blk */
);
#endif

#ifndef WITHOUT_SPIDER_BG_SEARCH
/*
 -1 :use table parameter
  0 :Background confirmation is disabled
  1 :Background confirmation is enabled
 */
MYSQL_THDVAR_INT(
  sts_bg_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Mode of table state confirmation at background.", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);
#endif

/*
  0 :always ping
  1-:interval
 */
MYSQL_THDVAR_INT(
  ping_interval_at_trx_start, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Ping interval at transaction start", /* comment */
  NULL, /* check */
  NULL, /* update */
  3600, /* def */
  0, /* min */
  2147483647, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :normal mode
  1 :quick mode
  2 :set 0 value
 */
MYSQL_THDVAR_INT(
  auto_increment_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Mode of auto increment.", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  3, /* max */
  0 /* blk */
);

/*
  FALSE: off
  TRUE:  on
 */
MYSQL_THDVAR_BOOL(
  same_server_link, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Permit to link same server's table", /* comment */
  NULL, /* check */
  NULL, /* update */
  FALSE /* def */
);

/*
  FALSE: transmits
  TRUE:  don't transmit
 */
MYSQL_THDVAR_BOOL(
  local_lock_table, /* name */
  PLUGIN_VAR_OPCMDARG, /* opt */
  "Remote server transmission when lock tables is executed at local",
    /* comment */
  NULL, /* check */
  NULL, /* update */
  FALSE /* def */
);

/*
 -1 :use table parameter
  0 :don't transmit
  1 :transmits
 */
MYSQL_THDVAR_INT(
  use_pushdown_udf, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Remote server transmission existence when UDF is used at condition and \"engine_condition_pushdown=1\"", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :duplicate check on local server
  1 :avoid duplicate check on local server
 */
MYSQL_THDVAR_INT(
  direct_dup_insert, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Execute \"REPLACE\" and \"INSERT IGNORE\" on remote server and avoid duplicate check on local server", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

/*
  1-: mutex count
 */
static MYSQL_SYSVAR_UINT(
  udf_table_lock_mutex_count,
  spider_udf_table_lock_mutex_count,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Mutex count of table lock for Spider UDFs",
  NULL,
  NULL,
  20,
  1,
  4294967295U,
  0
);

/*
  1-: mutex count
 */
static MYSQL_SYSVAR_UINT(
  udf_table_mon_mutex_count,
  spider_udf_table_mon_mutex_count,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Mutex count of table mon for Spider UDFs",
  NULL,
  NULL,
  20,
  1,
  4294967295U,
  0
);

/*
  1-:number of rows
 */
MYSQL_THDVAR_LONGLONG(
  udf_ds_bulk_insert_rows, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Number of rows for bulk inserting", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :drop records
  1 :insert last table
  2 :insert first table and loop again
 */
MYSQL_THDVAR_INT(
  udf_ds_table_loop_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Table loop mode if the number of tables in table list are less than the number of result sets", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  2, /* max */
  0 /* blk */
);

/*
 */
#ifdef PLUGIN_VAR_CAN_MEMALLOC
static MYSQL_SYSVAR_STR(
  remote_access_charset,
  spider_remote_access_charset,
  PLUGIN_VAR_MEMALLOC |
  PLUGIN_VAR_RQCMDARG,
  "Set remote access charset at connecting for improvement performance of connection if you know",
  NULL,
  NULL,
  NULL
);
#else
static MYSQL_SYSVAR_STR(
  remote_access_charset,
  spider_remote_access_charset,
  PLUGIN_VAR_RQCMDARG,
  "Set remote access charset at connecting for improvement performance of connection if you know",
  NULL,
  NULL,
  NULL
);
#endif

/*
 -1 :don't set
  0 :autocommit = 0
  1 :autocommit = 1
 */
static MYSQL_SYSVAR_INT(
  remote_autocommit,
  spider_remote_autocommit,
  PLUGIN_VAR_RQCMDARG,
  "Set autocommit mode at connecting for improvement performance of connection if you know",
  NULL,
  NULL,
  -1,
  -1,
  1,
  0
);

/*
 */
#ifdef PLUGIN_VAR_CAN_MEMALLOC
static MYSQL_SYSVAR_STR(
  remote_time_zone,
  spider_remote_time_zone,
  PLUGIN_VAR_MEMALLOC |
  PLUGIN_VAR_RQCMDARG,
  "Set remote time_zone at connecting for improvement performance of connection if you know",
  NULL,
  NULL,
  NULL
);
#else
static MYSQL_SYSVAR_STR(
  remote_time_zone,
  spider_remote_time_zone,
  PLUGIN_VAR_RQCMDARG,
  "Set remote time_zone at connecting for improvement performance of connection if you know",
  NULL,
  NULL,
  NULL
);
#endif

/*
 -1 :don't set
  0 :sql_log_off = 0
  1 :sql_log_off = 1
 */
static MYSQL_SYSVAR_INT(
  remote_sql_log_off,
  spider_remote_sql_log_off,
  PLUGIN_VAR_RQCMDARG,
  "Set sql_log_off mode at connecting for improvement performance of connection if you know",
  NULL,
  NULL,
  -1,
  -1,
  1,
  0
);

/*
 -1 :don't set
  0 :READ UNCOMMITTED
  1 :READ COMMITTED
  2 :REPEATABLE READ
  3 :SERIALIZABLE
 */
static MYSQL_SYSVAR_INT(
  remote_trx_isolation,
  spider_remote_trx_isolation,
  PLUGIN_VAR_RQCMDARG,
  "Set transaction isolation level at connecting for improvement performance of connection if you know",
  NULL,
  NULL,
  -1,
  -1,
  3,
  0
);

/*
  0-:connect retry interval (micro second)
 */
MYSQL_THDVAR_LONGLONG(
  connect_retry_interval, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Connect retry interval", /* comment */
  NULL, /* check */
  NULL, /* update */
  1000, /* def */
  0, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
  0-:connect retry count
 */
MYSQL_THDVAR_INT(
  connect_retry_count, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Connect retry count", /* comment */
  NULL, /* check */
  NULL, /* update */
  1000, /* def */
  0, /* min */
  2147483647, /* max */
  0 /* blk */
);

/*
 */
#ifdef PLUGIN_VAR_CAN_MEMALLOC
MYSQL_THDVAR_STR(
  bka_engine, /* name */
  PLUGIN_VAR_MEMALLOC |
  PLUGIN_VAR_RQCMDARG,
  "Temporary table's engine for BKA", /* comment */
  NULL, /* check */
  NULL, /* update */
  NULL /* def */
);
#else
MYSQL_THDVAR_STR(
  bka_engine, /* name */
  PLUGIN_VAR_RQCMDARG,
  "Temporary table's engine for BKA", /* comment */
  NULL, /* check */
  NULL, /* update */
  NULL /* def */
);
#endif

/*
 -1 :use table parameter
  0 :use union all
  1 :use temporary table
 */
MYSQL_THDVAR_INT(
  bka_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Mode of BKA for Spider", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

/*
 -1         : The UDF parameter is adopted.
  0 or more : Milliseconds.
 */
MYSQL_SYSVAR_INT(
  udf_ct_bulk_insert_interval,
  spider_udf_ct_bulk_insert_interval,
  PLUGIN_VAR_RQCMDARG,
  "The interval time between bulk insert and next bulk insert at coping",
  NULL,
  NULL,
  -1,
  -1,
  2147483647,
  0
);

/*
 -1,0       : The UDF parameter is adopted.
  1 or more : Number of rows.
 */
MYSQL_SYSVAR_LONGLONG(
  udf_ct_bulk_insert_rows,
  spider_udf_ct_bulk_insert_rows,
  PLUGIN_VAR_RQCMDARG,
  "The number of rows inserted with bulk insert of one time at coping",
  NULL,
  NULL,
  -1,
  -1,
  9223372036854775807LL,
  0
);

#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
/*
  0: no recycle
  1: recycle in instance
  2: recycle in thread
 */
MYSQL_THDVAR_UINT(
  hs_r_conn_recycle_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Handlersocket connection recycle mode", /* comment */
  NULL, /* check */
  NULL, /* update */
  2, /* def */
  0, /* min */
  2, /* max */
  0 /* blk */
);

/*
  0: weak
  1: strict
 */
MYSQL_THDVAR_UINT(
  hs_r_conn_recycle_strict, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Strict handlersocket connection recycle", /* comment */
  NULL, /* check */
  NULL, /* update */
  0, /* def */
  0, /* min */
  1, /* max */
  0 /* blk */
);

/*
  0: no recycle
  1: recycle in instance
  2: recycle in thread
 */
MYSQL_THDVAR_UINT(
  hs_w_conn_recycle_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Handlersocket connection recycle mode", /* comment */
  NULL, /* check */
  NULL, /* update */
  2, /* def */
  0, /* min */
  2, /* max */
  0 /* blk */
);

/*
  0: weak
  1: strict
 */
MYSQL_THDVAR_UINT(
  hs_w_conn_recycle_strict, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Strict handlersocket connection recycle", /* comment */
  NULL, /* check */
  NULL, /* update */
  0, /* def */
  0, /* min */
  1, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :not use
  1 :use handlersocket
 */
MYSQL_THDVAR_INT(
  use_hs_read, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Use handlersocket for reading", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :not use
  1 :use handlersocket
 */
MYSQL_THDVAR_INT(
  use_hs_write, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Use handlersocket for writing", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);
#endif

/*
 -1 :use table parameter
  0 :not use
  1 :use handler
 */
MYSQL_THDVAR_INT(
  use_handler, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Use handler for reading", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  3, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :not skip
  1 :skip
 */
MYSQL_THDVAR_INT(
  skip_default_condition, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Skip generating internal default condition", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :not send directly
  1-:send directly
 */
MYSQL_THDVAR_LONGLONG(
  direct_order_limit, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Send 'ORDER BY' and 'LIMIT' to remote server directly", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  9223372036854775807LL, /* max */
  0 /* blk */
);

/*
 -1 :use table parameter
  0 :writable
  1 :read only
 */
MYSQL_THDVAR_INT(
  read_only_mode, /* name */
  PLUGIN_VAR_RQCMDARG, /* opt */
  "Read only", /* comment */
  NULL, /* check */
  NULL, /* update */
  -1, /* def */
  -1, /* min */
  1, /* max */
  0 /* blk */
);

struct st_mysql_storage_engine spider_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

struct st_mysql_sys_var* spider_system_variables[] = {
  MYSQL_SYSVAR(support_xa),
  MYSQL_SYSVAR(table_init_error_interval),
  MYSQL_SYSVAR(use_table_charset),
  MYSQL_SYSVAR(conn_recycle_mode),
  MYSQL_SYSVAR(conn_recycle_strict),
  MYSQL_SYSVAR(sync_trx_isolation),
  MYSQL_SYSVAR(use_consistent_snapshot),
  MYSQL_SYSVAR(internal_xa),
  MYSQL_SYSVAR(internal_xa_snapshot),
  MYSQL_SYSVAR(force_commit),
  MYSQL_SYSVAR(internal_offset),
  MYSQL_SYSVAR(internal_limit),
  MYSQL_SYSVAR(split_read),
  MYSQL_SYSVAR(semi_split_read),
  MYSQL_SYSVAR(semi_split_read_limit),
  MYSQL_SYSVAR(init_sql_alloc_size),
  MYSQL_SYSVAR(reset_sql_alloc),
  MYSQL_SYSVAR(multi_split_read),
  MYSQL_SYSVAR(max_order),
  MYSQL_SYSVAR(semi_trx_isolation),
  MYSQL_SYSVAR(semi_table_lock),
  MYSQL_SYSVAR(semi_table_lock_connection),
  MYSQL_SYSVAR(block_size),
  MYSQL_SYSVAR(selupd_lock_mode),
  MYSQL_SYSVAR(sync_autocommit),
  MYSQL_SYSVAR(sync_time_zone),
  MYSQL_SYSVAR(internal_sql_log_off),
  MYSQL_SYSVAR(bulk_size),
  MYSQL_SYSVAR(bulk_update_mode),
  MYSQL_SYSVAR(bulk_update_size),
  MYSQL_SYSVAR(internal_optimize),
  MYSQL_SYSVAR(internal_optimize_local),
  MYSQL_SYSVAR(use_flash_logs),
  MYSQL_SYSVAR(use_snapshot_with_flush_tables),
  MYSQL_SYSVAR(use_all_conns_snapshot),
  MYSQL_SYSVAR(lock_exchange),
  MYSQL_SYSVAR(internal_unlock),
  MYSQL_SYSVAR(semi_trx),
  MYSQL_SYSVAR(net_timeout),
  MYSQL_SYSVAR(quick_mode),
  MYSQL_SYSVAR(quick_page_size),
  MYSQL_SYSVAR(low_mem_read),
  MYSQL_SYSVAR(select_column_mode),
#ifndef WITHOUT_SPIDER_BG_SEARCH
  MYSQL_SYSVAR(bgs_mode),
  MYSQL_SYSVAR(bgs_first_read),
  MYSQL_SYSVAR(bgs_second_read),
#endif
  MYSQL_SYSVAR(first_read),
  MYSQL_SYSVAR(second_read),
  MYSQL_SYSVAR(crd_interval),
  MYSQL_SYSVAR(crd_mode),
#ifdef WITH_PARTITION_STORAGE_ENGINE
  MYSQL_SYSVAR(crd_sync),
#endif
  MYSQL_SYSVAR(crd_type),
  MYSQL_SYSVAR(crd_weight),
#ifndef WITHOUT_SPIDER_BG_SEARCH
  MYSQL_SYSVAR(crd_bg_mode),
#endif
  MYSQL_SYSVAR(sts_interval),
  MYSQL_SYSVAR(sts_mode),
#ifdef WITH_PARTITION_STORAGE_ENGINE
  MYSQL_SYSVAR(sts_sync),
#endif
#ifndef WITHOUT_SPIDER_BG_SEARCH
  MYSQL_SYSVAR(sts_bg_mode),
#endif
  MYSQL_SYSVAR(ping_interval_at_trx_start),
  MYSQL_SYSVAR(auto_increment_mode),
  MYSQL_SYSVAR(same_server_link),
  MYSQL_SYSVAR(local_lock_table),
  MYSQL_SYSVAR(use_pushdown_udf),
  MYSQL_SYSVAR(direct_dup_insert),
  MYSQL_SYSVAR(udf_table_lock_mutex_count),
  MYSQL_SYSVAR(udf_table_mon_mutex_count),
  MYSQL_SYSVAR(udf_ds_bulk_insert_rows),
  MYSQL_SYSVAR(udf_ds_table_loop_mode),
  MYSQL_SYSVAR(remote_access_charset),
  MYSQL_SYSVAR(remote_autocommit),
  MYSQL_SYSVAR(remote_time_zone),
  MYSQL_SYSVAR(remote_sql_log_off),
  MYSQL_SYSVAR(remote_trx_isolation),
  MYSQL_SYSVAR(connect_retry_interval),
  MYSQL_SYSVAR(connect_retry_count),
  MYSQL_SYSVAR(connect_mutex),
  MYSQL_SYSVAR(bka_engine),
  MYSQL_SYSVAR(bka_mode),
  MYSQL_SYSVAR(udf_ct_bulk_insert_interval),
  MYSQL_SYSVAR(udf_ct_bulk_insert_rows),
#if defined(HS_HAS_SQLCOM) && defined(HAVE_HANDLERSOCKET)
  MYSQL_SYSVAR(hs_r_conn_recycle_mode),
  MYSQL_SYSVAR(hs_r_conn_recycle_strict),
  MYSQL_SYSVAR(hs_w_conn_recycle_mode),
  MYSQL_SYSVAR(hs_w_conn_recycle_strict),
  MYSQL_SYSVAR(use_hs_read),
  MYSQL_SYSVAR(use_hs_write),
#endif
  MYSQL_SYSVAR(use_handler),
  MYSQL_SYSVAR(skip_default_condition),
  MYSQL_SYSVAR(direct_order_limit),
  MYSQL_SYSVAR(read_only_mode),
  NULL
};

mysql_declare_plugin(spider)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &spider_storage_engine,
  "SPIDER",
  "Kentoku Shiba",
  "Spider storage engine",
  PLUGIN_LICENSE_GPL,
  spider_db_init,
  spider_db_done,
  0x021a,
  spider_status_variables,
  spider_system_variables,
  NULL
}
mysql_declare_plugin_end;
