
// vim:sw=2:ai

/*
 * Copyright (C) 2010-2011 DeNA Co.,Ltd.. All rights reserved.
 * Copyright (C) 2011 Kentoku SHIBA
 * See COPYRIGHT.txt for details.
 */

#include "mysql_version.h"
#if MYSQL_VERSION_ID < 50500
#include "mysql_priv.h"
#include <mysql/plugin.h>
#else
#include "sql_priv.h"
#include "probes_mysql.h"
#include "sql_class.h"
#endif

#include "hstcpcli.hpp"
#include "auto_file.hpp"
#include "string_util.hpp"
#include "auto_addrinfo.hpp"
#include "escape.hpp"
#include "util.hpp"

/* TODO */
#if !defined(__linux__) && !defined(__FreeBSD__) && !defined(MSG_NOSIGNAL)
#define MSG_NOSIGNAL 0
#endif

#define DBG(x)

namespace dena {

struct hstcpcli : public hstcpcli_i, private noncopyable {
  hstcpcli(const socket_args& args);
  virtual ~hstcpcli();
  virtual void close();
  virtual int reconnect();
  virtual bool stable_point();
  virtual void request_buf_open_index(size_t pst_id, const char *dbn,
    const char *tbl, const char *idx, const char *retflds, const char *filflds);
  #if 0
  virtual void request_buf_find(size_t pst_id, const string_ref& op,
    const string_ref *kvs, size_t kvslen, uint32 limit, uint32 skip);
  virtual void request_buf_insert(size_t pst_id, const string_ref *fvs,
    size_t fvslen);
  virtual void request_buf_update(size_t pst_id, const string_ref& op,
    const string_ref *kvs, size_t kvslen, uint32 limit, uint32 skip,
    const string_ref *mvs, size_t mvslen);
  virtual void request_buf_delete(size_t pst_id, const string_ref& op,
    const string_ref *kvs, size_t kvslen, uint32 limit, uint32 skip);
  #endif
  virtual void request_buf_exec_generic(size_t pst_id, const string_ref& op,
    const string_ref *kvs, size_t kvslen, uint32 limit, uint32 skip,
    const string_ref& mod_op, const string_ref *mvs, size_t mvslen,
    const hstcpcli_filter *fils, size_t filslen);
  virtual int request_send();
  virtual int response_recv(size_t& num_flds_r);
  virtual const string_ref *get_next_row();
  virtual void response_buf_remove();
  virtual int get_error_code();
  virtual String get_error();
 private:
  int read_more();
  void clear_error();
  int set_error(int code, const String& str);
  int set_error(int code, const char *str);
 private:
  auto_file fd;
  socket_args sargs;
  string_buffer readbuf;
  string_buffer writebuf;
  size_t response_end_offset; /* incl newline */
  size_t cur_row_offset;
  size_t num_flds;
  size_t num_req_bufd; /* buffered but not yet sent */
  size_t num_req_sent; /* sent but not yet received */
  size_t num_req_rcvd; /* received but not yet removed */
  int error_code;
  String error_str;
  DYNAMIC_ARRAY flds;
};

hstcpcli::hstcpcli(const socket_args& args)
  : sargs(args), response_end_offset(0), cur_row_offset(0), num_flds(0),
    num_req_bufd(0), num_req_sent(0), num_req_rcvd(0), error_code(0)
{
  String err;
  my_init_dynamic_array2(&flds, sizeof(string_ref), NULL, 16, 16);
  if (socket_connect(fd, sargs, err) != 0) {
    set_error(-1, err);
  }
}

hstcpcli::~hstcpcli()
{
  delete_dynamic(&flds);
}

void
hstcpcli::close()
{
  fd.close();
  readbuf.clear();
  writebuf.clear();
  response_end_offset = 0;
  cur_row_offset = 0;
  num_flds = 0;
  num_req_bufd = 0;
  num_req_sent = 0;
  num_req_rcvd = 0;
}

int
hstcpcli::reconnect()
{
  clear_error();
  close();
  String err;
  if (socket_connect(fd, sargs, err) != 0) {
    set_error(-1, err);
  }
  return error_code;
}

bool
hstcpcli::stable_point()
{
  /* returns true if cli can send a new request */
  return fd.get() >= 0 && num_req_bufd == 0 && num_req_sent == 0 &&
    num_req_rcvd == 0 && response_end_offset == 0;
}

int
hstcpcli::get_error_code()
{
  return error_code;
}

String
hstcpcli::get_error()
{
  return error_str;
}

int
hstcpcli::read_more()
{
  const size_t block_size = 4096; // FIXME
  char *const wp = readbuf.make_space(block_size);
  const ssize_t rlen = read(fd.get(), wp, block_size);
  if (rlen <= 0) {
    if (rlen < 0) {
      error_str = String("read: failed", &my_charset_bin);
    } else {
      error_str = String("read: eof", &my_charset_bin);
    }
    return rlen;
  }
  readbuf.space_wrote(rlen);
  return rlen;
}

void
hstcpcli::clear_error()
{
  DBG(fprintf(stderr, "CLEAR_ERROR: %d\n", error_code));
  error_code = 0;
  error_str.length(0);
}

int
hstcpcli::set_error(int code, const String& str)
{
  DBG(fprintf(stderr, "SET_ERROR: %d\n", code));
  error_code = code;
  error_str = str;
  return error_code;
}

int
hstcpcli::set_error(int code, const char *str)
{
  uint32 str_len = strlen(str);
  DBG(fprintf(stderr, "SET_ERROR: %d\n", code));
  error_code = code;
  error_str.length(0);
  if (error_str.reserve(str_len + 1))
    return 0;
  error_str.q_append(str, str_len);
  error_str.c_ptr_safe();
  return error_code;
}

void
hstcpcli::request_buf_open_index(size_t pst_id, const char *dbn,
  const char *tbl, const char *idx, const char *retflds, const char *filflds)
{
  if (num_req_sent > 0 || num_req_rcvd > 0) {
    close();
    set_error(-1, "request_buf_open_index: protocol out of sync");
    return;
  }
  const string_ref dbn_ref(dbn, strlen(dbn));
  const string_ref tbl_ref(tbl, strlen(tbl));
  const string_ref idx_ref(idx, strlen(idx));
  const string_ref rfs_ref(retflds, strlen(retflds));
  writebuf.append_literal("P\t");
  append_uint32(writebuf, pst_id); // FIXME size_t ?
  writebuf.append_literal("\t");
  writebuf.append(dbn_ref.begin(), dbn_ref.end());
  writebuf.append_literal("\t");
  writebuf.append(tbl_ref.begin(), tbl_ref.end());
  writebuf.append_literal("\t");
  writebuf.append(idx_ref.begin(), idx_ref.end());
  writebuf.append_literal("\t");
  writebuf.append(rfs_ref.begin(), rfs_ref.end());
  if (filflds != 0) {
    const string_ref fls_ref(filflds, strlen(filflds));
    writebuf.append_literal("\t");
    writebuf.append(fls_ref.begin(), fls_ref.end());
  }
  writebuf.append_literal("\n");
  ++num_req_bufd;
}

namespace {

void
append_delim_value(string_buffer& buf, const char *start, const char *finish)
{
  if (start == 0) {
    /* null */
    const char t[] = "\t\0";
    buf.append(t, t + 2);
  } else {
    /* non-null */
    buf.append_literal("\t");
    escape_string(buf, start, finish);
  }
}

};

void
hstcpcli::request_buf_exec_generic(size_t pst_id, const string_ref& op,
  const string_ref *kvs, size_t kvslen, uint32 limit, uint32 skip,
  const string_ref& mod_op, const string_ref *mvs, size_t mvslen,
  const hstcpcli_filter *fils, size_t filslen)
{
  if (num_req_sent > 0 || num_req_rcvd > 0) {
    close();
    set_error(-1, "request_buf_exec_generic: protocol out of sync");
    return;
  }
  append_uint32(writebuf, pst_id); // FIXME size_t ?
  writebuf.append_literal("\t");
  writebuf.append(op.begin(), op.end());
  writebuf.append_literal("\t");
  append_uint32(writebuf, kvslen); // FIXME size_t ?
  for (size_t i = 0; i < kvslen; ++i) {
    const string_ref& kv = kvs[i];
    append_delim_value(writebuf, kv.begin(), kv.end());
  }
  if (limit != 0 || skip != 0 || mod_op.size() != 0 || filslen != 0) {
    writebuf.append_literal("\t");
    append_uint32(writebuf, limit); // FIXME size_t ?
    if (skip != 0 || mod_op.size() != 0 || filslen != 0) {
      writebuf.append_literal("\t");
      append_uint32(writebuf, skip); // FIXME size_t ?
    }
    for (size_t i = 0; i < filslen; ++i) {
      const hstcpcli_filter& f = fils[i];
      writebuf.append_literal("\t");
      writebuf.append(f.filter_type.begin(), f.filter_type.end());
      writebuf.append_literal("\t");
      writebuf.append(f.op.begin(), f.op.end());
      writebuf.append_literal("\t");
      append_uint32(writebuf, f.ff_offset);
      append_delim_value(writebuf, f.val.begin(), f.val.end());
    }
    if (mod_op.size() != 0) {
      writebuf.append_literal("\t");
      writebuf.append(mod_op.begin(), mod_op.end());
      for (size_t i = 0; i < mvslen; ++i) {
        const string_ref& mv = mvs[i];
        append_delim_value(writebuf, mv.begin(), mv.end());
      }
    }
  }
  writebuf.append_literal("\n");
  ++num_req_bufd;
}

#if 0
void
hstcpcli::request_buf_find(size_t pst_id, const string_ref& op,
  const string_ref *kvs, size_t kvslen, uint32 limit, uint32 skip)
{
  return request_buf_exec_generic(pst_id, op, kvs, kvslen, limit, skip,
    0, 0, 0);
}

void
hstcpcli::request_buf_insert(size_t pst_id, const string_ref *fvs,
  size_t fvslen)
{
  const string_ref insert_op("+", 1);
  return request_buf_exec_generic(pst_id, insert_op, fvs, fvslen,
    0, 0, string_ref(), 0, 0);
}

void
hstcpcli::request_buf_update(size_t pst_id, const string_ref& op,
  const string_ref *kvs, size_t kvslen, uint32 limit, uint32 skip,
  const string_ref *mvs, size_t mvslen)
{
  const string_ref modop_update("U", 1);
  return request_buf_exec_generic(pst_id, op, kvs, kvslen, limit, skip,
    modop_update, mvs, mvslen);
}

void
hstcpcli::request_buf_delete(size_t pst_id, const string_ref& op,
  const string_ref *kvs, size_t kvslen, uint32 limit, uint32 skip)
{
  const string_ref modop_delete("D", 1);
  return request_buf_exec_generic(pst_id, op, kvs, kvslen, limit, skip,
    modop_delete, 0, 0);
}
#endif

int
hstcpcli::request_send()
{
  if (error_code < 0) {
    return error_code;
  }
  clear_error();
  if (fd.get() < 0) {
    close();
    return set_error(-1, "write: closed");
  }
  if (num_req_bufd == 0 || num_req_sent > 0 || num_req_rcvd > 0) {
    close();
    return set_error(-1, "request_send: protocol out of sync");
  }
  const size_t wrlen = writebuf.size();
  const ssize_t r = send(fd.get(), writebuf.begin(), wrlen, MSG_NOSIGNAL);
  if (r <= 0) {
    close();
    return set_error(-1, r < 0 ? "write: failed" : "write: eof");
  }
  writebuf.erase_front(r);
  if (static_cast<size_t>(r) != wrlen) {
    close();
    return set_error(-1, "write: incomplete");
  }
  num_req_sent = num_req_bufd;
  num_req_bufd = 0;
  DBG(fprintf(stderr, "REQSEND 0\n"));
  return 0;
}

int
hstcpcli::response_recv(size_t& num_flds_r)
{
  if (error_code < 0) {
    return error_code;
  }
  clear_error();
  if (num_req_bufd > 0 || num_req_sent == 0 || num_req_rcvd > 0 ||
    response_end_offset != 0) {
    close();
    return set_error(-1, "response_recv: protocol out of sync");
  }
  cur_row_offset = 0;
  num_flds_r = num_flds = 0;
  if (fd.get() < 0) {
    return set_error(-1, "read: closed");
  }
  size_t offset = 0;
  while (true) {
    const char *const lbegin = readbuf.begin() + offset;
    const char *const lend = readbuf.end();
    const char *const nl = memchr_char(lbegin, '\n', lend - lbegin);
    if (nl != 0) {
      offset = (nl + 1) - readbuf.begin();
      break;
    }
    if (read_more() <= 0) {
      close();
      return set_error(-1, "read: eof");
    }
  }
  response_end_offset = offset;
  --num_req_sent;
  ++num_req_rcvd;
  char *start = readbuf.begin();
  char *const finish = start + response_end_offset - 1;
  const size_t resp_code = read_ui32(start, finish);
  skip_one(start, finish);
  num_flds_r = num_flds = read_ui32(start, finish);
  if (resp_code != 0) {
    skip_one(start, finish);
    char *const err_begin = start;
    read_token(start, finish);
    char *const err_end = start;
    String e = String(err_begin, err_end - err_begin, &my_charset_bin);
    if (!e.length()) {
      e = String("unknown_error", &my_charset_bin);
    }
    return set_error(resp_code, e);
  }
  cur_row_offset = start - readbuf.begin();
  DBG(fprintf(stderr, "[%s] ro=%zu eol=%zu\n",
    String(readbuf.begin(), readbuf.begin() + response_end_offset)
      .c_str(),
    cur_row_offset, response_end_offset));
  DBG(fprintf(stderr, "RES 0\n"));
  if (flds.max_element < num_flds)
  {
    if (allocate_dynamic(&flds, num_flds))
      return set_error(-1, "out of memory");
  }
  flds.elements = num_flds;
  return 0;
}

const string_ref *
hstcpcli::get_next_row()
{
  if (num_flds == 0 || flds.elements < num_flds) {
    DBG(fprintf(stderr, "GNR NF 0\n"));
    return 0;
  }
  char *start = readbuf.begin() + cur_row_offset;
  char *const finish = readbuf.begin() + response_end_offset - 1;
  if (start >= finish) { /* start[0] == nl */
    DBG(fprintf(stderr, "GNR FIN 0 %p %p\n", start, finish));
    return 0;
  }
  for (size_t i = 0; i < num_flds; ++i) {
    skip_one(start, finish);
    char *const fld_begin = start;
    read_token(start, finish);
    char *const fld_end = start;
    char *wp = fld_begin;
    if (is_null_expression(fld_begin, fld_end)) {
      /* null */
      ((string_ref *) flds.buffer)[i] = string_ref();
    } else {
      unescape_string(wp, fld_begin, fld_end); /* in-place */
      ((string_ref *) flds.buffer)[i] = string_ref(fld_begin, wp);
    }
  }
  cur_row_offset = start - readbuf.begin();
  return (string_ref *) flds.buffer;
}

void
hstcpcli::response_buf_remove()
{
  if (response_end_offset == 0) {
    close();
    set_error(-1, "response_buf_remove: protocol out of sync");
    return;
  }
  readbuf.erase_front(response_end_offset);
  response_end_offset = 0;
  --num_req_rcvd;
  cur_row_offset = 0;
  num_flds = 0;
}

hstcpcli_ptr
hstcpcli_i::create(const socket_args& args)
{
  return hstcpcli_ptr(new hstcpcli(args));
}

};

