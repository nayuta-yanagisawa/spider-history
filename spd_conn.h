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

uchar *spider_conn_get_key(
  SPIDER_CONN *conn,
  size_t *length,
  my_bool not_used __attribute__ ((unused))
);

int spider_free_conn_alloc(
  SPIDER_CONN *conn
);

void spider_free_conn_from_trx(
  SPIDER_TRX *trx,
  SPIDER_CONN *conn,
  bool another,
  bool trx_free,
  int *roopCount
);

SPIDER_CONN *spider_create_conn(
  const SPIDER_SHARE *share,
  int *error_num
);

SPIDER_CONN *spider_get_conn(
  const SPIDER_SHARE *share,
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