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

#define SPIDER_LOCK_MODE_NO_LOCK             0
#define SPIDER_LOCK_MODE_SHARED              1
#define SPIDER_LOCK_MODE_EXCLUSIVE           2

uchar *spider_conn_get_key(
  SPIDER_CONN *conn,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
);

int spider_reset_conn_setted_parameter(
  SPIDER_CONN *conn
);

int spider_free_conn_alloc(
  SPIDER_CONN *conn
);

void spider_free_conn_from_trx(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn,
  bool another,
  bool trx_free,
  int *roop_count
);

SPIDER_CONN *spider_create_conn(
  const SPIDER_SHARE *share,
  ha_spider *spider,
  int link_id,
  int *error_num
);

SPIDER_CONN *spider_get_conn(
  const SPIDER_SHARE *share,
  int link_idx,
  char *conn_key,
  SPIDER_TRX *trx,
  ha_spider *spider,
  bool another,
  bool thd_chg,
  int *error_num
);

int spider_free_conn(
  SPIDER_CONN *conn
);

void spider_tree_insert(
  SPIDER_CONN *top,
  SPIDER_CONN *conn
);

SPIDER_CONN *spider_tree_first(
  SPIDER_CONN *top
);

SPIDER_CONN *spider_tree_last(
  SPIDER_CONN *top
);

SPIDER_CONN *spider_tree_next(
  SPIDER_CONN *current
);

SPIDER_CONN *spider_tree_delete(
  SPIDER_CONN *conn,
  SPIDER_CONN *top
);

#ifndef WITHOUT_SPIDER_BG_SEARCH
void spider_set_conn_bg_param(
  ha_spider *spider
);

int spider_create_conn_thread(
  SPIDER_CONN *conn
);

void spider_free_conn_thread(
  SPIDER_CONN *conn
);

void spider_bg_conn_break(
  SPIDER_CONN *conn,
  ha_spider *spider
);

int spider_bg_conn_search(
  ha_spider *spider,
  int link_idx,
  bool first,
  bool discard_result
);

void *spider_bg_conn_action(
  void *arg
);

int spider_create_sts_thread(
  SPIDER_SHARE *share
);

void spider_free_sts_thread(
  SPIDER_SHARE *share
);

void *spider_bg_sts_action(
  void *arg
);

int spider_create_crd_thread(
  SPIDER_SHARE *share
);

void spider_free_crd_thread(
  SPIDER_SHARE *share
);

void *spider_bg_crd_action(
  void *arg
);
#endif

int spider_conn_first_link_idx(
  THD *thd,
  long *tmp_link_statuses,
  int link_count,
  int link_status
);

int spider_conn_next_link_idx(
  THD *thd,
  long *tmp_link_statuses,
  int link_idx,
  int link_count,
  int link_status
);

int spider_conn_link_idx_next(
  long *tmp_link_statuses,
  int link_idx,
  int link_count,
  int link_status
);

int spider_conn_lock_mode(
  ha_spider *spider
);

bool spider_conn_check_recovery_link(
  SPIDER_SHARE *share
);
