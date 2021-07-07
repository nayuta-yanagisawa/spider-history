
// vim:sw=2:ai

/*
 * Copyright (C) 2010-2011 DeNA Co.,Ltd.. All rights reserved.
 * Copyright (C) 2011 Kentoku SHIBA
 * See COPYRIGHT.txt for details.
 */

#ifndef DENA_HSTCPCLI_HPP
#define DENA_HSTCPCLI_HPP

#define HANDLERSOCKET_MYSQL_UTIL 1

#include "mysql_version.h"
#if MYSQL_VERSION_ID < 50500
#include "mysql_priv.h"
#include <mysql/plugin.h>
#else
#include "sql_priv.h"
#include "probes_mysql.h"
#endif

#include "config.hpp"
#include "socket.hpp"
#include "string_ref.hpp"
#include "string_buffer.hpp"

namespace dena {

struct hstcpcli_filter {
  string_ref filter_type;
  string_ref op;
  size_t ff_offset;
  string_ref val;
  hstcpcli_filter() : ff_offset(0) { }
};

struct hstcpcli_i;
typedef hstcpcli_i *hstcpcli_ptr;

struct hstcpcli_i {
  virtual ~hstcpcli_i() { }
  virtual void close() = 0;
  virtual int reconnect() = 0;
  virtual bool stable_point() = 0;
  virtual void request_buf_open_index(size_t pst_id, const char *dbn,
    const char *tbl, const char *idx, const char *retflds,
    const char *filflds = 0) = 0;
  virtual void request_buf_exec_generic(size_t pst_id, const string_ref& op,
    const string_ref *kvs, size_t kvslen, uint32 limit, uint32 skip,
    const string_ref& mod_op, const string_ref *mvs, size_t mvslen,
    const hstcpcli_filter *fils = 0, size_t filslen = 0) = 0;
  virtual int request_send() = 0;
  virtual int response_recv(size_t& num_flds_r) = 0;
  virtual const string_ref *get_next_row() = 0;
  virtual void response_buf_remove() = 0;
  virtual int get_error_code() = 0;
  virtual String get_error() = 0;
  static hstcpcli_ptr create(const socket_args& args);
};

};

#endif

