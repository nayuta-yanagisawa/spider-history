# Copyright (C) 2005-2008 MySQL AB & Kentoku Shiba
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA

#called from the top level Makefile

MYSQLDATAdir  = $(localstatedir)
MYSQLSHAREdir = $(pkgdatadir)
MYSQLBASEdir  = $(prefix)
MYSQLLIBdir   = $(pkglibdir)
pkgplugindir  = $(pkglibdir)/plugin
INCLUDES      = -I$(top_srcdir)/include -I$(top_builddir)/include \
                -I$(top_srcdir)/regex \
                -I$(top_srcdir)/sql \
                -I$(srcdir)
WRAPLIBS      =

LDADD         =

DEFS          = @DEFS@

noinst_HEADERS = ha_spider.h spd_conn.h spd_db_conn.h \
                 spd_db_include.h spd_err.h spd_sys_table.h \
                 spd_table.h spd_trx.h spd_include.h spd_param.h

EXTRA_LTLIBRARIES     = ha_spider.la
pkgplugin_LTLIBRARIES = @plugin_spider_shared_target@
ha_spider_la_LDFLAGS  = -module -rpath $(pkgplugindir)
ha_spider_la_CXXFLAGS = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_spider_la_CFLAGS   = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_spider_la_SOURCES  = \
                   spd_param.cc \
                   spd_sys_table.cc \
                   spd_trx.cc \
                   spd_db_conn.cc \
                   spd_conn.cc \
                   spd_table.cc \
                   ha_spider.cc

EXTRA_LIBRARIES      = libspider.a
noinst_LIBRARIES     = @plugin_spider_static_target@
libspider_a_CXXFLAGS = $(AM_CFLAGS)
libspider_a_CFLAGS   = $(AM_CFLAGS)
libspider_a_SOURCES  = \
                   spd_param.cc \
                   spd_sys_table.cc \
                   spd_trx.cc \
                   spd_db_conn.cc \
                   spd_conn.cc \
                   spd_table.cc \
                   ha_spider.cc

EXTRA_DIST = CMakeLists.txt plug.in
# Don't update the files from bitkeeper
%::SCCS/s.%
