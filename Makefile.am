pkgplugindir   = $(pkglibdir)/plugin
INCLUDES       = -I$(MYSQL_INC)$(top_srcdir)/include \
                 -I$(MYSQL_INC)$(top_srcdir)/regex \
                 -I$(MYSQL_INC)$(top_srcdir)/sql

noinst_HEADERS = ha_spider.h spd_conn.h spd_db_conn.h \
                 spd_db_include.h spd_err.h spd_sys_table.h \
                 spd_table.h spd_trx.h spd_include.h spd_param.h \
                 spd_direct_sql.h spd_udf.h spd_ping_table.h \
                 spd_copy_tables.h

lib_LTLIBRARIES       = $(PLUGIN_SPIDER_LTLIBRARIES_TARGET)
EXTRA_LTLIBRARIES     = ha_spider.la
pkgplugin_LTLIBRARIES = @plugin_spider_shared_target@
ha_spider_la_LDFLAGS  = -shared -module -rpath $(pkgplugindir)
ha_spider_la_CXXFLAGS = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_spider_la_CFLAGS   = $(AM_CFLAGS) -DMYSQL_DYNAMIC_PLUGIN
ha_spider_la_SOURCES  = \
                   spd_param.cc \
                   spd_sys_table.cc \
                   spd_trx.cc \
                   spd_db_conn.cc \
                   spd_conn.cc \
                   spd_table.cc \
                   spd_direct_sql.cc \
                   spd_udf.cc \
                   spd_ping_table.cc \
                   spd_copy_tables.cc \
                   ha_spider.cc

lib_LIBRARIES        = $(PLUGIN_SPIDER_LIBRARIES_TARGET)
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
                   spd_direct_sql.cc \
                   spd_udf.cc \
                   spd_ping_table.cc \
                   spd_copy_tables.cc \
                   ha_spider.cc

EXTRA_DIST = plug.in
# Don't update the files from bitkeeper
%::SCCS/s.%
