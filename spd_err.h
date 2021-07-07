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

#define ER_SPIDER_INVALID_CONNECT_INFO_NUM 12501
#define ER_SPIDER_INVALID_CONNECT_INFO_STR "The connect info '%-.64s' is invalid"
#define ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_NUM 12502
#define ER_SPIDER_INVALID_CONNECT_INFO_TOO_LONG_STR "The connect info '%-.64s' for %s is too long"
#define ER_SPIDER_CANT_USE_BOTH_INNER_XA_AND_SNAPSHOT_NUM 12601
#define ER_SPIDER_CANT_USE_BOTH_INNER_XA_AND_SNAPSHOT_STR "Can't use both spider_use_consistent_snapshot = 1 and spider_internal_xa = 1"
#define ER_SPIDER_XA_LOCKED_NUM 12602
#define ER_SPIDER_XA_LOCKED_STR "This xid is now locked"
#define ER_SPIDER_XA_NOT_PREPARED_NUM 12603
#define ER_SPIDER_XA_NOT_PREPARED_STR "This xid is not prepared"
#define ER_SPIDER_XA_PREPARED_NUM 12604
#define ER_SPIDER_XA_PREPARED_STR "This xid is prepared"
#define ER_SPIDER_XA_EXISTS_NUM 12605
#define ER_SPIDER_XA_EXISTS_STR "This xid is already exist"
#define ER_SPIDER_XA_MEMBER_EXISTS_NUM 12606
#define ER_SPIDER_XA_MEMBER_EXISTS_STR "This xid member is already exist"
#define ER_SPIDER_XA_NOT_EXISTS_NUM 12607
#define ER_SPIDER_XA_NOT_EXISTS_STR "This xid is not exist"
#define ER_SPIDER_XA_MEMBER_NOT_EXISTS_NUM 12608
#define ER_SPIDER_XA_MEMBER_NOT_EXISTS_STR "This xid member is not exist"
#define ER_SPIDER_RENAME_WITH_OTHER_NUM 12611
#define ER_SPIDER_RENAME_WITH_OTHER_STR "Can't rename with other alter specifications"
#define ER_SPIDER_LOW_MEM_READ_PREV_NUM 12621
#define ER_SPIDER_LOW_MEM_READ_PREV_STR "Can't use this operation at low mem read mode"
#define ER_SPIDER_REMOTE_SERVER_GONE_AWAY_NUM 12701
#define ER_SPIDER_REMOTE_SERVER_GONE_AWAY_STR "Remote MySQL server has gone away"
#define ER_SPIDER_REMOTE_TABLE_NOT_FOUND_NUM 12702
#define ER_SPIDER_REMOTE_TABLE_NOT_FOUND_STR "Remote table '%s.%s' is not found"

#define ER_SPIDER_UNKNOWN_STR "unknown"
#define ER_SPIDER_UNKNOWN_LEN (sizeof(ER_SPIDER_UNKNOWN_STR) - 1)
