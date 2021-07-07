# Copyright (C) 2006-2008 MySQL AB & Kentoku Shiba
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

SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX")
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DSAFEMALLOC -DSAFE_MUTEX")

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/include ${CMAKE_SOURCE_DIR}/sql
                    ${CMAKE_SOURCE_DIR}/regex
                    ${CMAKE_SOURCE_DIR}/extra/yassl/include)

SET(SPIDER_SOURCES spd_err.h
                   spd_param.h spd_param.cc
                   spd_db_include.h spd_include.h
                   spd_sys_table.h spd_sys_table.cc
                   spd_trx.h spd_trx.cc
                   spd_db_conn.h spd_db_conn.cc
                   spd_conn.h spd_conn.cc
                   spd_table.h spd_table.cc
                   ha_spider.h ha_spider.cc)

IF(NOT SOURCE_SUBLIBS)
  ADD_LIBRARY(spider ${SPIDER_SOURCES})
  ADD_DEPENDENCIES(spider GenError)
ENDIF(NOT SOURCE_SUBLIBS)
