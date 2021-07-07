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

long long spider_direct_sql_body(
  UDF_INIT *initid,
  UDF_ARGS *args,
  char *is_null,
  char *error,
  my_bool bg
);

my_bool spider_direct_sql_init_body(
  UDF_INIT *initid,
  UDF_ARGS *args,
  char *message,
  my_bool bg
);

void spider_direct_sql_deinit_body(
  UDF_INIT *initid
);

#ifndef WITHOUT_SPIDER_BG_SEARCH
void spider_direct_sql_bg_start(
  UDF_INIT *initid
);

long long spider_direct_sql_bg_end(
  UDF_INIT *initid
);
#endif
