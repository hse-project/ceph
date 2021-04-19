// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Micron Technology, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <vector>
#include <string>
#include <string_view>
#include <climits>
#include <cstring>
#include <sys/mman.h>

extern "C" {
  #include <hse/hse.h>
  #include <hse/hse_limits.h>
}

#include "common/errno.h"
#include "common/debug.h"
#include "include/uuid.h"
#include "HseStore.h"
#include "os/kv.h"

/* Because we are using std::string/std::string_view/char in the code along with
 * uint8_t, let's just assure ourselves that the sizes of char and uint8_t are
 * the same.
 */
static_assert(sizeof(char) == sizeof(uint8_t));

#define dout_context cct
#define dout_subsys ceph_subsys_hsestore
#undef dout_prefix
#define dout_prefix *_dout << "hsestore(" << path << ") "

#define ENCODED_KEY_PREFIX_LEN 13 // 1 + 8 + 4
#define ENCODED_KEY_COLL 14
#define CEPH_METADATA_GENERAL_KEY(key) ("G" key)

static constexpr std::string_view CEPH_METADATA_KVS_NAME = "ceph-metadata";
static constexpr std::string_view COLLECTION_KVS_NAME = "collection";
static constexpr std::string_view COLLECTION_OBJECT_KVS_NAME = "collection-object";
static constexpr std::string_view OBJECT_DATA_KVS_NAME = "object-data";
static constexpr std::string_view OBJECT_XATTR_KVS_NAME = "object-xattr";
static constexpr std::string_view OBJECT_OMAP_KVS_NAME = "object-omap";

// Buffer used for Ceph write operations (write(), omap value xattr value).
// If the write operation requires a read (for merge), this buffer is also used for the read.
alignas(1 << 12) thread_local uint8_t write_buf[OMAP_BLOCK_LEN];

// Buffer used for Ceph read data operations.
// It is always zero (never changed). It is used to return zeroes to Ceph without doing a copy.
// This same buffer can be usd in parallel by different read operations across transactions
// and collections.
// It is assumed that Ceph, (that sees this buffer only in the result of a read operations),
// is not changing its content.
alignas(DATA_BLOCK_LEN) uint8_t zbuf[DATA_BLOCK_LEN] = {0};

static void ghobject_t2key(CephContext *cct, const ghobject_t& oid, std::string *key);
static int key2ghobject_t(const std::string_view key, ghobject_t &oid);
static void coll_t2key(CephContext *cct, const coll_t& coll, std::string *key);
static int key2coll_t(const std::string_view key, coll_t &coll);

//
// WaitCondTs functions
//

// Constructor, called when entering function queue_transactions()
WaitCondTs::WaitCondTs(TransactionSerialization *ts)
{
  this->_ts = ts;
  boost::unique_lock<boost::mutex> lock(ts->_ts_mutex);
  if (ts->_ts_th_running) {
    ts->_ts_th_waiting.push_back(this);
    lock.unlock();

    // Wait for the thread processing transaction[s] and for all thread waiting
    // to do so to go before us.
    this->wait();
  } else {
    // No thread is running transaction on this collection, we can go.
    ts->_ts_th_running = true;
    lock.unlock();
  }
}

//
// Destructor, called when leaving the function queue_transactions()
// Wakeup the oldest thread queue_transaction() waiting to run.
WaitCondTs::~WaitCondTs()
{
  ceph_assert(_ts->_ts_th_running);

  boost::unique_lock<boost::mutex> lock(_ts->_ts_mutex);

  if (_ts->_ts_th_waiting.empty()) {
    // No thread is queued waiting to run
    _ts->_ts_th_running = false;
    lock.unlock();
  } else {
    // Wakeup the oldest thread waiting to run.
    auto it = _ts->_ts_th_waiting.begin();
    auto wcts = *it;
    _ts->_ts_th_waiting.pop_front();
    lock.unlock();

    wcts->wakeup();
  }
}


//
// class TxnWaitPersist functions
//
TxnWaitPersist::TxnWaitPersist(Context *ctx, uint64_t t_seq) : _persist_ctx(ctx), _t_seq(t_seq) {}

//
// Collection functions
//
HseStore::Collection::Collection(HseStore *store, coll_t cid)
  : CollectionImpl(store->cct, cid),
    _store(store)
{
  coll_t2key(_store->cct, cid, &_coll_tkey);
}

void HseStore::Collection::flush()
{
  return;
}

#pragma push_macro("dout_prefix")
#pragma push_macro("dout_context")
#undef dout_prefix
#undef dout_context
#define dout_prefix *_dout << "hsestore "
#define dout_context _store->cct

bool HseStore::Collection::flush_commit(Context *ctx)
{
  if (_t_seq_persisted_latest + 1 == _t_seq_next) {
    // All transactions persisted.
    dout(10) << __func__ << " return true" << dendl;
    return true;
  }

  // Queue the sync request to the syncer thread
  _store->_syncer->post_sync(this, ctx, _t_seq_next - 1);

  dout(10) << __func__ << " return false" << dendl;
  return false;
}

void HseStore::Collection::queue_wait_persist(Context *ctx, uint64_t t_seq) {
	TxnWaitPersist *twp = new TxnWaitPersist(ctx, t_seq);

  dout(10) << __func__ << " entering seq " << t_seq << dendl;

  boost::unique_lock<std::mutex> lock(_committed_wait_persist_mtx);
  _committed_wait_persist.push_back(twp);
}

void HseStore::Collection::committed_wait_persist_cb(HseStore::Collection *c)
{

  boost::unique_lock<std::mutex> lock(c->_committed_wait_persist_mtx);

  auto it = c->_committed_wait_persist.begin();
  while (it != c->_committed_wait_persist.end()) {
    TxnWaitPersist *twp = *it;

    // Stop at the first transaction not yet persisted.
    if (twp->_t_seq > c->_t_seq_persisted_latest)
    break;

    c->_committed_wait_persist.pop_front();
    lock.unlock();

#pragma push_macro("dout_context")
#undef dout_context
#define dout_context c->_store->cct
    dout(10) << __func__ << " seq " << twp->_t_seq << dendl;
#pragma pop_macro("dout_context")

    // Invoke the Ceph "commit" callback.
    c->_store->finisher.queue(twp->_persist_ctx);

    delete twp;

    lock.lock();
    it = c->_committed_wait_persist.begin();
  }
}

/*
 * Update the Onode if necessary.
 */
void HseStore::Collection::get_onode(
  Onode& o,
  const ghobject_t& oid,
  bool create)
{
  bool found;

  if (!o.o_gotten) {
    o.o_gotten = true;
    o.o_oid = std::addressof(oid);
    ghobject_t2key(_store->cct, oid, &o.o_ghobject_tkey);
  }

  if (o.o_exists || o.o_dirty)
    return;


  this->_store->ghobject_t2hse_oid(this->cid, oid, found, o.o_hse_oid, o.o_data_len);

  if (found) {
    o.o_exists = true;
  } else {
    if (create)
      o.o_dirty = true;
  }
}
#pragma pop_macro("dout_prefix")
#pragma pop_macro("dout_context")


//
// Syncer functions
//
#pragma push_macro("dout_prefix")
#pragma push_macro("dout_context")
#undef dout_prefix
#undef dout_context
#define dout_prefix *_dout << "hsestore "
#define dout_context store->cct

// Sync the whole kvdb
void Syncer::kvdb_sync(HseStore *store)
{
  hse_err_t rc = 0;
  HseStore::Collection *c;
  bool needsync = false;


  dout(10) << __func__ << " entering" << dendl;

  // Record for each collection, the latest committed before starting the sync.
  std::shared_lock<ceph::shared_mutex> l{store->coll_lock};
  for (auto const& [key, val] : store->coll_map) {
    c = static_cast<HseStore::Collection*>(val.get());
    c->_t_seq_committed_wait_sync = c->_t_seq_next - 1;
    if (c->_t_seq_persisted_latest < c->_t_seq_committed_wait_sync)
      needsync = true;
  }
  l.unlock();

  if (!needsync)
    return;

  rc = hse_kvdb_sync(store->_kvdb);
  if (rc) {
    dout(10) << " failed to sync the kvdb" << dendl;
    return;
  }

  // Record the latest transaction persisted.
  l.lock();
  for (auto const& [key, val] : store->coll_map) {
    c = static_cast<HseStore::Collection*>(val.get());
    c->_t_seq_persisted_latest = c->_t_seq_committed_wait_sync;

    // Invoke the Ceph "commit" callback for all the transactions that had
    // been committed and are now persisted.
    HseStore::Collection::committed_wait_persist_cb(c);
  }
  l.unlock();
}

// flush_commit() requested a sync
void Syncer::do_sync(HseStore::Collection *c, Context *ctx, uint64_t t_seq_committed_at_flush)
{
  if (c->_t_seq_persisted_latest < t_seq_committed_at_flush) {
    //
    // We have to sync
    //
    Syncer::kvdb_sync(c->_store);
  }

#pragma push_macro("dout_context")
#undef dout_context
#define dout_context c->_store->cct
  dout(10) << __func__ << " calling flush commit context, persisted seq " <<
    c->_t_seq_persisted_latest << dendl;
#pragma pop_macro("dout_context")
  // Call the flush_commit() callback.
  c->_store->finisher.queue(ctx);
}

void Syncer::timer_cb(const boost::system::error_code& e, boost::asio::deadline_timer* timer,
    HseStore* store)
{
  if (store->_ready_for_sync)
    Syncer::kvdb_sync(store);

  // Restart the timer.
  timer->expires_from_now(boost::posix_time::milliseconds(SYNCER_PERIOD_MS));
  timer->async_wait(boost::bind(Syncer::timer_cb,
        boost::asio::placeholders::error, timer, store));
}
#pragma pop_macro("dout_prefix")
#pragma pop_macro("dout_context")


#pragma push_macro("dout_prefix")
#pragma push_macro("dout_context")
#undef dout_prefix
#undef dout_context
#define dout_prefix *_dout << "hsestore "
#define dout_context this->_c->_store->cct
//
// OmapIteratorImpl functions
//

HseStore::OmapIteratorImpl::OmapIteratorImpl(CollectionRef c, Onode& o) : _c(c), _o(o),
  _os(nullptr), _valid(false)
{
  hse_err_t rc;
  std::string hse_key;

  dout(10) << __func__ << " entering hse_oid " <<  _o.o_hse_oid << " oid " << *_o.o_oid << dendl;

  _key_encode_u64(o.o_hse_oid, &hse_key);
  rc = this->_c->_store->hse_kvs_cursor_create_wrapper(this->_c->_store->_object_omap_kvs,
      this->_os, hse_key.c_str(), hse_key.length(), &(this->_cursor));
  ceph_assert(!rc);
}
#pragma pop_macro("dout_prefix")
#pragma pop_macro("dout_context")

HseStore::OmapIteratorImpl::~OmapIteratorImpl()
{
  hse_err_t rc;

  rc = hse_kvs_cursor_destroy(_cursor);
  ceph_assert(!rc);
}

/*
 * Read the key and value of one omap entry.
 *
 * The cursor passed in is limited to one Ceph object (aka one hse_oid).
 * When entering this function, the cursor points:
 *  - to the first block of an omap entry. OmapBlk0.already_read is false.
 *  - or to the second block (block nb 1) of an omap entry. blk0.already_read is true
 *    and "blk0" contains block 0 key and data.
 *
 * If "ceph_iterator" is true, it means we are in the context of a Ceph OmapIterator.
 * If val_bl is null, then the caller doesn't need the omap entry value.
 */
hse_err_t HseStore::kv_omap_read_entry(
    struct hse_kvdb_opspec *os,
    struct hse_kvs_cursor *cursor,
    bool ceph_iterator,
    struct OmapBlk0 &blk0, // in/out
    std::string& omap_key, // out
    bufferlist* val_bl, // out
    bool& found)
{
  found = true;
  bool need_copy = true;
  bool done = false;
  std::string hse_key_str;
  const char *hse_key;
  size_t hse_key_len;
  const char *hse_val;
  size_t hse_val_len;
  uint32_t block_nb = 0;;
  hse_err_t rc;
  bool eof;

  dout(10) << __func__ << " entering"  << dendl;
  if (val_bl)
    val_bl->clear();


  //
  // Get the first block (block 0)
  //
  if (blk0.already_read) {
    blk0.already_read = false;

    // The first block has already been read, use the result of that read.

    if (blk0.hse_val_len < OMAP_BLOCK_LEN) {
      // Only one block for the value of this omap entry.
      done = true;

      if (ceph_iterator) {
        // Ceph is going to use the value buffer (provided by Hse) only till it calls
        // again ObjectMap::ObjectMapIterator.next() (that call hse_kvs_cursor_read()).
        // So is safe to pass to Ceph the buffer allocated by hse.
        need_copy = false;
      }
    }
    if (val_bl) {
      if (need_copy) {
        // Copy takes place
        val_bl->append(blk0.hse_val, blk0.hse_val_len);
      } else {
        // No copy.
        bufferptr ptr(buffer::create_static(blk0.hse_val_len, (char *)blk0.hse_val));
        val_bl->append(ptr);
      }
    }

    // Extract the omap key from the hse key.
    hse_key_str.assign(blk0.hse_key);
    block_nb = HseStore::object_omap_hse_key2block_nb(hse_key_str);
    ceph_assert(!block_nb);
    HseStore::object_omap_hse_key2omap_key(hse_key_str, omap_key);

  } else {
    // Read the first block of the omap entry
    rc = hse_kvs_cursor_read_wrapper(cursor, os, reinterpret_cast<const void **>(&hse_key),
      &hse_key_len, reinterpret_cast<const void **>(&hse_val), &hse_val_len, &eof);
    if (rc) {
      dout(10) << __func__ << " failed to cursor read the first block rc " <<
	hse_err_to_errno(rc) << dendl;
      return rc;
    }
    if (eof) {
      done = true;
      found = false;
    } else {
      // Extract the omap key from the hse key.
      hse_key_str.assign(hse_key, hse_key_len);
      block_nb = HseStore::object_omap_hse_key2block_nb(hse_key_str);
      ceph_assert(!block_nb);
      HseStore::object_omap_hse_key2omap_key(hse_key_str, omap_key);

      if (hse_val_len < OMAP_BLOCK_LEN) {
	done = true;
        if (ceph_iterator) {
          // Ceph is going to use the value buffer (provided by Hse) only till it calls
          // again ObjectMap::ObjectMapIterator.next() (that call hse_kvs_cursor_read()).
          // So is safe to pass to Ceph the buffer allocated by hse.
          need_copy = false;
        }
      }
      if (val_bl) {
        if (need_copy) {
          // Copy takes place
          val_bl->append(hse_val, hse_val_len);
        } else {
          // No copy.
          bufferptr ptr(buffer::create_static(hse_val_len, (char *)hse_val));
          val_bl->append(ptr);
        }
      }
    }
  }

  //
  // The block 0 is read, read following blocks of the same omap entry if they exist.
  //

  for (block_nb = 1; !done; block_nb++) {
    std::string hse_key_blk0_str;

    if (block_nb == 1) {
      // Save hse key of block 0 to comapre it the following hse keys.
      hse_key_blk0_str.assign(hse_key_str);
    }

    rc = hse_kvs_cursor_read_wrapper(cursor, os, reinterpret_cast<const void **>(&hse_key),
      &hse_key_len, reinterpret_cast<const void **>(&hse_val), &hse_val_len, &eof);
    if (rc) {
      dout(10) << __func__ << " failed to cursor read the block nb " << block_nb <<
	" rc " << hse_err_to_errno(rc) << dendl;
      return rc;
    }
    if (eof) {
      done = true;
    } else {
      hse_key_str.assign(hse_key, hse_key_len);

      //
      // Compare the first part of the hse key (without the ending block number)
      // to know if we are still on the same omap entry.
      //
      if (hse_key_blk0_str.compare(0, hse_key_blk0_str.length() - sizeof(uint32_t),
	    hse_key_str, 0, hse_key_str.length() - sizeof(uint32_t))) {

	//
	// We ended up on the first block of the next omap entry.
	// We have to stop and save what was read from this first block, it will be used
	// next time this function is called to get the next omap entry.
	//

	ceph_assert(HseStore::object_omap_hse_key2block_nb(hse_key_str) == 0);

	done = true;

	blk0.already_read = true;
        blk0.hse_key.assign(hse_key, hse_key_len);
	blk0.hse_val = hse_val;
	blk0.hse_val_len = hse_val_len;

      } else {
	//
	// We are still on the same omap entry.
	//
	ceph_assert(block_nb == HseStore::object_omap_hse_key2block_nb(hse_key_str));

	if (val_bl) {
	  // Copy takes place
	  val_bl->append(hse_val, hse_val_len);
	}

	if (hse_val_len < OMAP_BLOCK_LEN) {
	  done = true;
	}
      }
    }
  }
  dout(10) << __func__ << " found " << found << " return omap entry "  << omap_key << dendl;
  return 0;
}

/*
 * Create a kvs cursor in kvs _object_omap_kvs that is restricted to a particular object
 * and have it point on block 0 of the first omap entry of the object (skipping the omap header).
 */
hse_err_t HseStore::kv_omap_create_hse_cursor(
    struct hse_kvdb_opspec *os,
    Onode& o,
    struct hse_kvs_cursor **cursor)
{
  hse_err_t rc;
  std::string hse_key;

  dout(10) << __func__ << " entering"  << dendl;

  ceph_assert(o.o_exists);
  _key_encode_u64(o.o_hse_oid, &hse_key);
  rc = hse_kvs_cursor_create_wrapper(_object_omap_kvs, os, hse_key.c_str(), hse_key.length(),
      cursor);
  if (rc)
    return rc;

  // Need to skip the omap header whose hse key is only the hse_oid_t (8 bytes)
  // Add a zero after the hse_oid_t
  hse_key.append(1, 0);

  rc = hse_kvs_cursor_seek_wrapper(*cursor, os, hse_key.c_str(), hse_key.length(),
      nullptr, nullptr);

  if (rc)
    hse_kvs_cursor_destroy(*cursor);

  return rc;
}

#pragma push_macro("dout_prefix")
#pragma push_macro("dout_context")
#undef dout_prefix
#undef dout_context
#define dout_prefix *_dout << "hsestore "
#define dout_context _c->_store->cct

int HseStore::OmapIteratorImpl::seek_to_first()
{
  hse_err_t rc;
  std::string hse_key;
  std::string hse_key_found;
  const char *seek_found;
  size_t seek_found_len;

  dout(10) << __func__ << " entering"  << dendl;

  _valid = false;
  _blk0.already_read = false;

  _key_encode_u64(_o.o_hse_oid, &hse_key);

  // Need to skip the omap header whose hse key is only the hse_oid_t (8 bytes)
  // Add a zero after the hse_oid_t
  hse_key.append(1, 0);

  rc = _c->_store->hse_kvs_cursor_seek_wrapper(_cursor, _os, hse_key.c_str(), hse_key.length(),
      (const void **)&seek_found, &seek_found_len);

  if (!rc)
    goto end;

  // Read the omap entry pointed by the cursor.
  if (seek_found_len) {
    bool found = false;

    hse_key_found.assign(seek_found, seek_found_len);
    ceph_assert(HseStore::object_omap_hse_key2block_nb(hse_key_found) == 0);

    rc = this->_c->_store->kv_omap_read_entry(_os, _cursor, true, _blk0, _omap_key, &_value, found);
    if (rc)
      goto end;

    ceph_assert(found);
    _valid = true;
  }

end:
  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::OmapIteratorImpl::upper_bound(const string &after)
{
  hse_err_t rc;
  std::string hse_key;
  const char *seek_found;
  size_t seek_found_len;
  std::string hse_key_found;

  dout(10) << __func__ << " entering"  << dendl;

  _valid = false;
  _blk0.already_read = false;

  _key_encode_u64(_o.o_hse_oid, &hse_key);
  hse_key.append(after); // append the omap key after the hse_oid
  _key_encode_u32(MAX_BLOCK_NB, &hse_key); // append block number 0xFFFFFFFF


  // The seek is not going to find the block number 0xFFFFFFFF and is going to land on
  // the block 0 of the next omap entry.
  rc = _c->_store->hse_kvs_cursor_seek_wrapper(_cursor, _os, hse_key.c_str(), hse_key.length(),
      (const void **)&seek_found, &seek_found_len);

  if (!rc)
    goto end;

  // Read the omap entry pointed by the cursor.
  if (seek_found_len) {
    bool found = false;

    hse_key_found.assign(seek_found, seek_found_len);
    ceph_assert(HseStore::object_omap_hse_key2block_nb(hse_key_found) == 0);

    rc = this->_c->_store->kv_omap_read_entry(_os, _cursor, true, _blk0, _omap_key, &_value, found);
    if (rc)
      goto end;

    ceph_assert(found);
    _valid = true;
  }

end:
  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::OmapIteratorImpl::lower_bound(const string &to)
{
  hse_err_t rc;
  std::string hse_key;
  const char *seek_found;
  size_t seek_found_len;
  std::string hse_key_found;

  dout(10) << __func__ << " entering"  << dendl;

  _valid = false;
  _blk0.already_read = false;

  _key_encode_u64(_o.o_hse_oid, &hse_key);
  hse_key.append(to); // append the omap key after the hse_oid

  rc = _c->_store->hse_kvs_cursor_seek_wrapper(_cursor, _os, hse_key.c_str(), hse_key.length(),
      (const void **)&seek_found, &seek_found_len);
  if (!rc)
    goto end;

  // Read the omap entry pointed by the cursor.
  if (seek_found_len) {
    bool found = false;

    hse_key_found.assign(seek_found, seek_found_len);
    ceph_assert(HseStore::object_omap_hse_key2block_nb(hse_key_found) == 0);

    rc = this->_c->_store->kv_omap_read_entry(_os, _cursor, true, _blk0, _omap_key, &_value, found);
    if (rc)
      goto end;

    ceph_assert(found);
    _valid = true;
  }

end:
  return rc ? -hse_err_to_errno(rc) : 0;
}

bool HseStore::OmapIteratorImpl::valid()
{
  dout(10) << __func__ << " entering"  << dendl;

  return _valid;
}

int HseStore::OmapIteratorImpl::next()
{
  hse_err_t rc;
  bool found;

  dout(10) << __func__ << " entering"  << dendl;

  _valid = false;

  rc = this->_c->_store->kv_omap_read_entry(_os, _cursor, true, _blk0, _omap_key, &_value, found);
  if (rc)
      goto end;

  if (found)
    _valid = true;
end:
  return rc ? -hse_err_to_errno(rc) : 0;
}

string HseStore::OmapIteratorImpl::key()
{
  dout(10) << __func__ << " entering"  << dendl;

  ceph_assert(_valid);
  return _omap_key;
}

bufferlist HseStore::OmapIteratorImpl::value()
{
  dout(10) << __func__ << " entering"  << dendl;

  ceph_assert(_valid);
  return _value;
}
#pragma pop_macro("dout_prefix")
#pragma pop_macro("dout_context")

//
// HseStore functions
//


HseStore::HseStore(CephContext *cct, const std::string& path)
  : ObjectStore(cct, path),
    _syncer(new Syncer(this)),
    finisher(cct),
    kvdb_name(cct->_conf->hsestore_kvdb)
{
  // The zero page should not be changed.
  mprotect(zbuf, sizeof(zbuf), PROT_READ);
}

HseStore::~HseStore() {}

ObjectStore::CollectionHandle HseStore::open_collection(const coll_t &cid)
{
  std::shared_lock<ceph::shared_mutex> l{coll_lock};

  return coll_map[cid];
}

ObjectStore::CollectionHandle HseStore::create_new_collection(const coll_t& cid)
{
  auto c = ceph::make_ref<HseStore::Collection>(this, cid);

  std::unique_lock<ceph::shared_mutex> l{coll_lock};
  new_coll_map[cid] = c;

  return c;
}

bool HseStore::exists(CollectionHandle &c, const ghobject_t &oid)
{
  bool found;
  hse_oid_t hse_oid;
  uint64_t o_data_len;

  ghobject_t2hse_oid(c->cid, oid, found, hse_oid, o_data_len);

  return found;
}

int HseStore::list_collections(vector<coll_t>& ls)
{
  std::shared_lock<ceph::shared_mutex> l{coll_lock};

  for (auto const& [key, val] : coll_map)
    ls.push_back(key);

  return 0;
}

bool HseStore::collection_exists(const coll_t& c)
{
  std::shared_lock<ceph::shared_mutex> l{coll_lock};
  return coll_map.count(c);
}

HseStore::CollectionRef HseStore::get_collection(coll_t cid)
{
  std::shared_lock<ceph::shared_mutex> l{coll_lock};
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

int HseStore::collection_empty(CollectionHandle &c, bool *empty)
{
  hse_err_t rc = 0;
  hse_err_t rc1;

  std::string coll_tkey;
  coll_t2key(cct, c->cid, &coll_tkey);

  struct hse_kvs_cursor *cursor;
  rc = hse_kvs_cursor_create_wrapper(_collection_object_kvs, nullptr, coll_tkey.c_str(),
    coll_tkey.size(), &cursor);
  if (rc) {
    dout(10) << " failed to create cursor while checking if collection ("
      << c->cid << ") is empty" << dendl;
    goto end;
  }

  rc = hse_kvs_cursor_read(cursor, nullptr, nullptr, nullptr, nullptr, nullptr,
    empty);
  if (rc) {
    dout(10) << " failed to read from cursor while checking if collection ("
      << c->cid << ") is empty" << dendl;
  }

  rc1 = hse_kvs_cursor_destroy(cursor);
  if (rc1) {
    dout(10) << " failed to destroy cursor while checking if collection ("
      << c->cid << ") is empty" << dendl;
    if (!rc)
      rc = rc1;
  }

end:
  return rc ? -hse_err_to_errno(rc) : 0;
}

hse_err_t HseStore::remove_collection(
    struct hse_kvdb_opspec *os,
    coll_t cid, CollectionRef *c)
{
  hse_err_t rc = 0;

  std::unique_lock<ceph::shared_mutex> l{coll_lock};

  *c = coll_map[cid];
  coll_map.erase(cid);

  l.unlock();

  rc = hse_kvs_prefix_delete(_collection_kvs, os, (*c)->_coll_tkey.c_str(), (*c)->_coll_tkey.size(),
    nullptr);
  if (rc) {
    dout(10) << " failed to prefix delete all data pertaining to collection (" <<
      cid << ") in collection kvs" << dendl;
    goto err_out;
  }
  rc = hse_kvs_prefix_delete(_collection_object_kvs, os, (*c)->_coll_tkey.c_str(),
    (*c)->_coll_tkey.size(), nullptr);
  if (rc) {
    dout(10) << " failed to prefix delete all data pertaining to collection (" <<
      cid << ") in collection-object kvs" << dendl;
    goto err_out;
  }

err_out:
  return rc;
}

/*
 * TODO "bits" are ignored and not saved.
 */
hse_err_t HseStore::kv_create_collection(
    struct hse_kvdb_opspec *os,
    const coll_t &cid,
    unsigned bits)
{
  hse_err_t rc;

  std::string coll_tkey;
  coll_t2key(cct, cid, &coll_tkey);
  rc = hse_kvs_put(_collection_kvs, os, coll_tkey.c_str(), coll_tkey.length(), nullptr, 0);
  if (rc) {
    dout(10) << __func__ << " failed to put the new collection " << cid << dendl;
  }
  return rc;
}

hse_err_t HseStore::create_collection(
    struct hse_kvdb_opspec *os,
    coll_t cid,
    unsigned bits,
    CollectionRef *c)
{
  hse_err_t rc = 0;

  {
    std::unique_lock<ceph::shared_mutex> l{coll_lock};
    if (*c) {
      rc = EEXIST;
      goto out;
    }
    auto p = new_coll_map.find(cid);
    ceph_assert(p != new_coll_map.end());
    *c = p->second;
    ceph_assert((*c)->cid == cid);
    // (*c)->cnode.bits = bits;   TODO
    coll_map[cid] = *c;
    new_coll_map.erase(p);
  }
  rc = kv_create_collection(os, cid, bits);
  if (rc)
    dout(10) << __func__ << " failed to create the new collection " << cid << dendl;

out:
  return rc;
}

hse_err_t HseStore::split_collection(
    struct hse_kvdb_opspec *os,
    CollectionRef& c, CollectionRef& d, unsigned bits, int rem)
{
  hse_err_t rc = 0;
  hse_err_t rc1;
  struct hse_kvs_cursor *cursor;
  bool eof = false;

  const coll_t old_cid = c->cid;
  const coll_t new_cid = d->cid;

  std::string oldcid_tkey;
  coll_t2key(cct, old_cid, &oldcid_tkey);
  std::string newcid_tkey;
  coll_t2key(cct, new_cid, &newcid_tkey);

  rc = hse_kvs_cursor_create_wrapper(_collection_object_kvs, os, oldcid_tkey.c_str(),
    oldcid_tkey.size(), &cursor);
  if (rc) {
    dout(10) << " failed to create cursor while splitting collection ("
      << old_cid << "->" << new_cid << ')' << dendl;
    return rc;
  }

  while (!eof) {
    const uint8_t *key;
    size_t key_len;
    rc = hse_kvs_cursor_read_wrapper(cursor, os, reinterpret_cast<const void **>(&key),
      &key_len, nullptr, nullptr, &eof);
    if (rc) {
      dout(10) << " failed to read from cursor while splitting collections ("
        << old_cid << "->" << new_cid << ')' << dendl;
      goto destroy;
    }

    std::string_view ghobject_tkey {
      reinterpret_cast<const char *>(key + ENCODED_KEY_COLL),
      key_len - ENCODED_KEY_COLL - sizeof(hse_oid_t) };
    ghobject_t obj;
    if (const int err = key2ghobject_t(ghobject_tkey, obj)) {
      dout(10) << " failed to convert key (" << ghobject_tkey << ") to ghobject_t: " << err << dendl;
      goto destroy;
    }

    // if not a match don't put/delete; this check was stolen from MemStore
    if (!obj.match(bits, rem))
      continue;

    auto new_key = std::make_unique<uint8_t[]>(key_len);
    memcpy(new_key.get(), newcid_tkey.c_str(), newcid_tkey.size());
    memcpy(new_key.get() + newcid_tkey.size(), key + newcid_tkey.size(),
      key_len - newcid_tkey.size());

    rc = hse_kvs_put(_collection_object_kvs, os, static_cast<void *>(new_key.get()),
      key_len, nullptr, 0);
    if (rc) {
      dout(10) << " failed to read from cursor while merging collections ("
        << old_cid << "->" << new_cid << ')' << dendl;
      goto destroy;
    }

    rc = hse_kvs_delete(_collection_object_kvs, os, key, key_len);
    if (rc) {
      dout(10) << " failed to data from collection while merging collections ("
        << old_cid << "->" << new_cid << ')' << dendl;
    }
  }

destroy:
  rc1 = hse_kvs_cursor_destroy(cursor);
  if (rc1) {
    dout(10) << " failed to destroy cursor while merging collections ("
      << old_cid << "->" << new_cid << ')' << dendl;
    if (!rc)
      rc = rc1;
  }
  return rc;
}

hse_err_t HseStore::merge_collection(
    struct hse_kvdb_opspec *os,
    CollectionRef *c, CollectionRef& d, unsigned bits)
{
  // HSE_TODO: what to do with bits?
  hse_err_t rc = 0;
  hse_err_t rc1;
  struct hse_kvs_cursor *cursor;
  bool eof = false;

  const coll_t old_cid = (*c)->cid;
  const coll_t new_cid = d->cid;

  std::string old_cid_tkey;
  coll_t2key(cct, old_cid, &old_cid_tkey);
  std::string new_cid_tkey;
  coll_t2key(cct, new_cid, &new_cid_tkey);

  rc = hse_kvs_cursor_create_wrapper(_collection_object_kvs, os, old_cid_tkey.c_str(),
    old_cid_tkey.size(), &cursor);
  if (rc) {
    dout(10) << " failed to create cursor while merging collections (" <<
      old_cid << "->" << new_cid << ')' << dendl;
    return rc;
  }

  while (!eof) {
    const uint8_t *key;
    size_t key_len;
    rc = hse_kvs_cursor_read_wrapper(cursor, os, reinterpret_cast<const void **>(&key),
      &key_len, nullptr, nullptr, &eof);
    if (rc) {
      dout(10) << " failed to read from cursor while merging collections ("
        << old_cid << "->" << new_cid << ')' << dendl;
      goto destroy;
    }

    auto new_key = std::make_unique<uint8_t[]>(key_len);
    memcpy(new_key.get(), new_cid_tkey.c_str(), new_cid_tkey.size());
    memcpy(new_key.get() + ENCODED_KEY_COLL, key + ENCODED_KEY_COLL, key_len - ENCODED_KEY_COLL);

    rc = hse_kvs_put(_collection_object_kvs, os, static_cast<void *>(new_key.get()), key_len, nullptr, 0);
    if (rc) {
      dout(10) << " failed to put while merging collections ("
        << old_cid << "->" << new_cid << ')' << dendl;
      goto destroy;
    }
  }

  rc = hse_kvs_prefix_delete(_collection_object_kvs, os, old_cid_tkey.c_str(),
    old_cid_tkey.size(), nullptr);
  if (rc) {
    dout(10) << " failed to prefix delete data from collection while merging collections ("
      << old_cid << "->" << new_cid << ')' << dendl;
  }

destroy:
  rc1 = hse_kvs_cursor_destroy(cursor);
  if (rc1) {
    dout(10) << " failed to destroy cursor while merging collections ("
      << old_cid << "->" << new_cid << ')' << dendl;
    if (!rc)
      rc = rc1;
  }
  return rc;
}

int HseStore::queue_transactions(
  ObjectStore::CollectionHandle& ch,
  std::vector<ceph::os::Transaction>& tls,
  TrackedOpRef op,
  ThreadPool::TPHandle *handle)
{
  Collection *c = static_cast<Collection*>(ch.get());

  //
  // Block if a thread is already processing transactions on this collection.
  // The WaitCondTs destructor will wakeup the next queue_transactions() thread
  // waiting to run on the collection.
  //
  WaitCondTs ts(&(c->_ts));

  for (auto t : tls) {
    Context *contexts;

    start_one_transaction(c, &t);

    //
    // Invoke the two Ceph apply callbacks.
    //
    contexts = t.get_on_applied_sync();
    if (contexts)
      contexts->complete(0);

    contexts = t.get_on_applied();
    if (contexts)
      finisher.queue(contexts);

    //
    // Queue the transaction till persisted.
    // The "commit" Ceph callback will be invoked later when the transaction
    // is persisted by the syncer.
    //
    contexts = t.get_on_commit();
    if (contexts)
      c->queue_wait_persist(contexts, c->_t_seq_next);

    c->_t_seq_next++;
  }

  // The WaitCondTs destructor (variable ts) will wakeup the next queue_transactions()
  // thread waiting to run on the collection.
  return 0;
}

hse_err_t HseStore::kv_retrieve_hse_oid(void)
{
  const std::string hse_oid_key(CEPH_METADATA_GENERAL_KEY("hse_oid"));
  bool found;
  uint64_t val;
  size_t val_len;
  hse_err_t rc;

  rc = hse_kvs_get(_ceph_metadata_kvs, nullptr, hse_oid_key.c_str(), hse_oid_key.length(),
    &found, &val, sizeof(val), &val_len);
  if (rc) {
    dout(10) << __func__ << " failed to get hse_oid" << dendl;
    return rc;
  }
  if (found) {
    const char *p;
    uint64_t host_val;

    ceph_assert(val_len == sizeof(val));
    p = (char *)(&val);
    _key_decode_u64(p, &host_val);
    _next_hse_oid = host_val;
  } else {
    _next_hse_oid = 0;
  }

  dout(10) << __func__ << " exiting, _next_hse_oid " << _next_hse_oid  << dendl;

  return 0;
}

hse_err_t HseStore::kv_save_hse_oid(void)
{
  const std::string hse_oid_key(CEPH_METADATA_GENERAL_KEY("hse_oid"));
  uint64_t val;
  std::string valc;
  hse_err_t rc;

  dout(10) << __func__ << " entering, _next_hse_oid " << _next_hse_oid  << dendl;

  val = _next_hse_oid;
  _key_encode_u64(val, &valc);
  ceph_assert(valc.length() == sizeof(val));
  rc = hse_kvs_put(_ceph_metadata_kvs, nullptr, hse_oid_key.c_str(), hse_oid_key.length(),
    valc.c_str(), valc.length());
  if (rc) {
    dout(10) << __func__ << " failed to persist hse_oid" << dendl;
  }
  return rc;
}

int HseStore::mount()
{
  hse_err_t rc = 0;
  hse_err_t rc1;
  bool fsid_found = false;
  static const char fsid_key[] = CEPH_METADATA_GENERAL_KEY("fsid");
  // 36 is length of UUID, +1 for NUL byte
  uint8_t fsid_buf[37] = { 0 };
  size_t fsid_len;
  std::unique_lock<ceph::shared_mutex> l{coll_lock};

  rc = hse_kvdb_init();
  if (rc) {
    goto end;
  }

  rc = hse_kvdb_open(kvdb_name.data(), nullptr, &_kvdb);
  if (rc) {
    dout(10) << " failed to open the kvdb" << dendl;
    goto end;
  }

  /* HSE_TODO: how to handle error logic here */
  rc = hse_kvdb_kvs_open(_kvdb, CEPH_METADATA_KVS_NAME.data(), nullptr, &_ceph_metadata_kvs);
  if (rc) {
    dout(10) << " failed to open the ceph-metadata kvs" << dendl;
    goto end;
  }

  rc = hse_kvdb_kvs_open(_kvdb, COLLECTION_KVS_NAME.data(), nullptr, &_collection_kvs);
  if (rc) {
    dout(10) << " failed to open the collection kvs" << dendl;
    goto end;
  }

  rc = hse_kvdb_kvs_open(_kvdb, COLLECTION_OBJECT_KVS_NAME.data(), nullptr, &_collection_object_kvs);
  if (rc) {
    dout(10) << " failed to open the collection-object kvs" << dendl;
    goto end;
  }

  rc = hse_kvdb_kvs_open(_kvdb, OBJECT_DATA_KVS_NAME.data(), nullptr, &_object_data_kvs);
  if (rc) {
    dout(10) << " failed to open the object-data kvs" << dendl;
    goto end;
  }

  rc = hse_kvdb_kvs_open(_kvdb, OBJECT_XATTR_KVS_NAME.data(), nullptr, &_object_xattr_kvs);
  if (rc) {
    dout(10) << " failed to open the object-xattr kvs" << dendl;
    goto end;
  }

  rc = hse_kvdb_kvs_open(_kvdb, OBJECT_OMAP_KVS_NAME.data(), nullptr, &_object_omap_kvs);
  if (rc) {
    dout(10) << " failed to open the object-omap kvs" << dendl;
    goto end;
  }

  rc = HseStore::kv_retrieve_hse_oid();
  if (rc) {
    dout(10) << __func__ << "failed to retrieve the hse_oid counter" << dendl;
    goto end;
  }

  struct hse_kvs_cursor *cursor;
  rc = hse_kvs_cursor_create_wrapper(_collection_kvs, nullptr, nullptr, 0, &cursor);
  if (rc) {
    dout(10) << " failed to create a cursor for populating the in-memory collection map" << dendl;
    goto end;
  }

  const uint8_t *key, *val;
  size_t key_len, val_len;
  bool eof;
  while (true) {
    rc = hse_kvs_cursor_read(cursor, nullptr, reinterpret_cast<const void **>(&key),
      &key_len, reinterpret_cast<const void **>(&val), &val_len, &eof);
    if (rc) {
      dout(10) << " failed to read cursor for populating initial coll_map" << dendl;
      goto destroy;
    }

    if (eof)
      break;

    std::string_view coll_tkey { reinterpret_cast<const char *>(key), ENCODED_KEY_COLL };
    coll_t cid;
    if (const int err = key2coll_t(coll_tkey, cid)) {
      dout(10) << " failed to convert key (" << coll_tkey << ") to coll_t: " << err << dendl;
      goto destroy;
    }

    coll_map[cid] = ceph::make_ref<Collection>(this, std::move(cid));
  }

  // -1 removed the NUL byte
  rc = hse_kvs_get(_ceph_metadata_kvs, nullptr, fsid_key, sizeof(fsid_key) - 1,
    &fsid_found, fsid_buf, sizeof(fsid_buf) - 1, &fsid_len);
  if (rc) {
    dout(10) << " failed to get fsid" << dendl;
    goto destroy;
  }
  ceph_assert(sizeof(fsid_buf) - 1 == fsid_len);
  if (!fsid.parse(reinterpret_cast<char *>(fsid_buf))) {
    dout(10) << " failed to parse fsid" << dendl;
    rc = ENOTRECOVERABLE;
    goto destroy;
  }

  finisher.start();

destroy:
  rc1 = hse_kvs_cursor_destroy(cursor);
  if (rc1) {
    dout(10) << " failed to destroy cursor while populating initial coll_map" << dendl;
    if (!rc)
      rc = rc1;
  }

  if (!rc)
    _ready_for_sync = true;


end:
  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::umount()
{
  hse_err_t rc = 0;
  std::unique_lock<ceph::shared_mutex> l{coll_lock};

  rc = HseStore::kv_save_hse_oid();
  if (rc) {
    dout(10) << __func__ << " failed to save the hse_oid counter" << dendl;
    // continue anyway.
  }

  finisher.wait_for_empty();
  finisher.stop();

  /* HSE_TODO: how to handle error logic here */
  rc = hse_kvdb_kvs_close(_ceph_metadata_kvs);
  if (rc) {
    dout(10) << " failed to close the ceph-metadata kvs" << dendl;
    goto err_out;
  }
  _ceph_metadata_kvs = nullptr;

  rc = hse_kvdb_kvs_close(_collection_kvs);
  if (rc) {
    dout(10) << " failed to close the collection kvs" << dendl;
    goto err_out;
  }
  _collection_kvs = nullptr;

  rc = hse_kvdb_kvs_close(_collection_object_kvs);
  if (rc) {
    dout(10) << " failed to close the collection-object kvs" << dendl;
    goto err_out;
  }
  _collection_object_kvs = nullptr;

  rc = hse_kvdb_kvs_close(_object_data_kvs);
  if (rc) {
    dout(10) << " failed to close the object-data kvs" << dendl;
    goto err_out;
  }
  _object_data_kvs = nullptr;

  rc = hse_kvdb_kvs_close(_object_xattr_kvs);
  if (rc) {
    dout(10) << " failed to close the object-xattr kvs" << dendl;
    goto err_out;
  }
  _object_xattr_kvs = nullptr;

  rc = hse_kvdb_kvs_close(_object_omap_kvs);
  if (rc) {
    dout(10) << " failed to close the object-omap kvs" << dendl;
    goto err_out;
  }
  _object_omap_kvs = nullptr;

  rc = hse_kvdb_close(_kvdb);
  if (rc) {
    dout(10) << " failed to close the kvdb" << dendl;
    goto err_out;
  }
  _kvdb = nullptr;

  new_coll_map.clear();
  coll_map.clear();

err_out:
  hse_kvdb_fini();

  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::mkfs()
{
  hse_err_t rc = 0;
  static const char fsid_key[] = CEPH_METADATA_GENERAL_KEY("fsid");
  bool found;
  size_t old_fsid_len;
  // 36 is length of UUID, +1 for NUL byte
  uint8_t old_fsid_buf[37] = { 0 };
  std::string fsid_str;
  struct hse_params *params = NULL;

  rc = hse_kvdb_init();
  if (rc) {
    goto err_out;
  }

  /* HSE_TODO: how to handle error logic here */
  rc = hse_kvdb_make(kvdb_name.data(), nullptr);
  if (rc && hse_err_to_errno(rc) != EEXIST) {
    dout(10) << " failed to make the kvdb" << dendl;
    goto err_out;
  }

  rc = hse_kvdb_open(kvdb_name.data(), nullptr, &_kvdb);
  if (rc && hse_err_to_errno(rc) != EEXIST) {
    dout(10) << " failed to open the kvdb" << dendl;
    goto err_out;
  }

  rc = hse_kvdb_kvs_make(_kvdb, CEPH_METADATA_KVS_NAME.data(), nullptr);
  if (rc && hse_err_to_errno(rc) != EEXIST) {
    dout(10) << " failed to make the ceph-metadata kvs" << dendl;
    goto kvdb_out;
  }

  rc = hse_kvdb_kvs_make(_kvdb, COLLECTION_KVS_NAME.data(), nullptr);
  if (rc && hse_err_to_errno(rc) != EEXIST) {
    dout(10) << " failed to make the collection kvs" << dendl;
    goto kvdb_out;
  }

  hse_params_create(&params);
  hse_params_set(params, "kvs.pfx_len", "14"); // coll_t2key

  rc = hse_kvdb_kvs_make(_kvdb, COLLECTION_OBJECT_KVS_NAME.data(), params);
  if (rc && hse_err_to_errno(rc) != EEXIST) {
    dout(10) << " failed to make the collection-object kvs" << dendl;
    goto kvdb_out;
  }
  hse_params_destroy(params);

  hse_params_create(&params);
  hse_params_set(params, "kvs.pfx_len", "8"); // hse_oid_t

  rc = hse_kvdb_kvs_make(_kvdb, OBJECT_DATA_KVS_NAME.data(), params);
  if (rc && hse_err_to_errno(rc) != EEXIST) {
    dout(10) << " failed to make the object-data kvs" << dendl;
    goto kvdb_out;
  }

  rc = hse_kvdb_kvs_make(_kvdb, OBJECT_XATTR_KVS_NAME.data(), params);
  if (rc && hse_err_to_errno(rc) != EEXIST) {
    dout(10) << " failed to make the object-xattr kvs" << dendl;
    goto kvdb_out;
  }

  rc = hse_kvdb_kvs_make(_kvdb, OBJECT_OMAP_KVS_NAME.data(), params);
  if (rc && hse_err_to_errno(rc) != EEXIST) {
    dout(10) << " failed to make the object-omap kvs" << dendl;
    goto kvdb_out;
  }

  rc = hse_kvdb_kvs_open(_kvdb, CEPH_METADATA_KVS_NAME.data(), nullptr,
    &_ceph_metadata_kvs);
  if (rc) {
    dout(10) << " failed to open ceph-metadata kvs" << dendl;
    goto kvdb_out;
  }

  // -1 removes NUL byte
  rc = hse_kvs_get(_ceph_metadata_kvs, nullptr, fsid_key, sizeof(fsid_key) - 1,
    &found, &old_fsid_buf, sizeof(old_fsid_buf), &old_fsid_len);
  if (rc) {
    dout(10) << " failed to read the old fsid" << dendl;
    goto kvdb_out;
  }

  if (!found) {
    if (fsid.is_zero()) {
      fsid.generate_random();
      dout(1) << " generated fsid " << fsid << dendl;
    } else {
      dout(1) << " using provided fsid " << fsid << dendl;
    }
  }

  fsid_str = fsid.to_string();

  ceph_assert(fsid_str.size() == (sizeof(old_fsid_buf) - 1));

  // -1 because we don't want to compare NUL byte
  if (found && memcmp(fsid_str.c_str(), old_fsid_buf, fsid_str.size())) {
    derr << " on-disk fsid " << old_fsid_buf << " != provided " << fsid << dendl;
    rc = EINVAL;
    goto kvdb_out;
  }

  // -1 removes NUL byte
  rc = hse_kvs_put(_ceph_metadata_kvs, nullptr, fsid_key, sizeof(fsid_key) - 1,
    fsid_str.c_str(), fsid_str.size());
  if (rc) {
    dout(10) << " failed to persist fsid" << dendl;
    goto kvs_out;
  }

kvs_out:
  // HSE_TODO: this will overwrite rc, best way to not do that?
  rc = hse_kvdb_kvs_close(_ceph_metadata_kvs);
  if (rc) {
    dout(10) << " failed to close ceph-metadata kvs" << dendl;
    goto kvdb_out;
  }
kvdb_out:
  rc = hse_kvdb_close(_kvdb);
  if (rc) {
    dout(10) << " failed to close the kvdb" << dendl;
    goto kvdb_out;
  }
err_out:
  hse_kvdb_fini();
  if (params)
    hse_params_destroy(params);

  return rc && hse_err_to_errno(rc) != EEXIST ? -hse_err_to_errno(rc) : 0;
}

/*
 * Compute a key to use in the KVS object_data_kvs
 * The key is composed of the hse_oid_t + data block number.
 */
void HseStore::offset2object_data_key(const hse_oid_t &hse_oid, uint64_t offset,
    std::string *key)
{
  uint32_t data_block_number;

  key->clear();

  _key_encode_u64(hse_oid, key);

  data_block_number = offset >> DATA_BLOCK_SHIFT;
  // Must be always MSB first to sort correctly.
  _key_encode_u32(data_block_number, key);

  ceph_assert(key->length() == OBJECT_DATA_KEY_LEN);
}

// Return the object data block number from the hse key used in kvs _object_data_kvs (hse_oid +
// encoded block number)
uint32_t HseStore::object_data_hse_key2block_nb(
    std::string& object_data_hse_key)
{
  uint32_t block_nb;

  _key_decode_u32(object_data_hse_key.c_str() + sizeof(hse_oid_t), &block_nb);
  return block_nb;
}

// Return the omap value block number from the hse key used in kvs _object_omap_kvs
// (hse_oid + ceph omap entry key + encoded block number)
uint32_t HseStore::object_omap_hse_key2block_nb(
    std::string& object_omap_hse_key)
{
  uint32_t block_nb;

  _key_decode_u32(object_omap_hse_key.c_str() + object_omap_hse_key.length() -
      sizeof(uint32_t), &block_nb);
  return block_nb;
}

// Return the ceph omap key from the hse key used in kvs _object_omap_kvs
// (hse_oid + ceph omap entry key + encoded block number)
void HseStore::object_omap_hse_key2omap_key(
    std::string& object_omap_kse_key,
    std::string& omap_key)
{
  omap_key = object_omap_kse_key.substr(sizeof(hse_oid_t),
      object_omap_kse_key.length() - sizeof(hse_oid_t) - sizeof(uint32_t));
}

// Append buffers (each block sized and zero filled) in the buffer list "bl".
uint32_t HseStore::append_zero_blocks_in_bl(
    int32_t first_block_nb,
    int32_t last_block_nb,
    bufferlist& bl,
    uint32_t first_offset,
    uint32_t last_length)
{
  int32_t block_nb;
  uint32_t block_cnt;

  for (block_nb = first_block_nb, block_cnt = 0;
    block_nb < last_block_nb; block_nb++, block_cnt++) {

    bufferptr ptr(buffer::create_static(DATA_BLOCK_LEN, (char *)zbuf));
    if (block_cnt == 0) {
      // First zero block
      ptr.set_offset(first_offset);
    } else if (block_nb == last_block_nb - 1) {
      // Last zero block
      ptr.set_length(last_length);
    }
    bl.append(ptr);
  }
  return block_cnt;
}

/*
 * Return offset (in bytes) of the first byte of the data block overlapping "offset"
 */
uint32_t HseStore::block_offset(uint64_t offset)
{
  return offset & ~(DATA_BLOCK_LEN -1);
}
/*
 * Return the relative length (from the beginning of the block, in bytes)
 * in the last data block.
 */
uint32_t HseStore::end_block_length(uint64_t offset, size_t length)
{
  return (offset + length) & (DATA_BLOCK_LEN -1);
}

/*
 * It happend that Ceph read oject data passing a length set to 0.
 * In that case Ceph expects all the object data to be returned.
 * An example is when Ceoh OSD daemon reads its superblock.
 *
 * Note that this function calls hse_kvs_cursor_seek() that can't be called in the context
 * of a transaction.
 */
hse_err_t HseStore::kv_read_data(
    struct hse_kvdb_opspec *os,
    Onode& o,
    uint64_t offset,
    size_t length,
    bufferlist& bl)
{
  hse_err_t rc = 0;
  hse_err_t rc1;
  std::string start_block_key;
  int32_t prev_cursor_block_nb;
  std::string cursor_block_key;
  uint64_t start_block_offset;
  uint64_t end_block_offset;
  bool found;
  const void *val;
  size_t val_len;
  const void *seek_found;
  size_t seek_found_len;
  size_t cursor_block_key_len;
  int32_t start_block_nb;
  int32_t end_block_nb;
  uint32_t start_block_rel_offset;
  uint32_t end_block_rel_length;


  dout(10) << __func__ << " " << " " << *o.o_oid << " hse_oid " << o.o_hse_oid
	   << " " << offset << "~" << length << ":" << o.o_data_len << dendl;

  if (length == 0) {
    // Return all the object data from the passed in offset.
    if (o.o_data_len >= offset) {
      length = o.o_data_len - offset;
    }
  }

  if (offset + length > MAX_DATA_LEN) {
    dout(10) << __func__ << " offset + length too big" << dendl;
    return EINVAL;
  }

  // To avoid data copy:
  // - free whatever buffers present in bl (done by bl.clear())
  // - this function will allocate adequate buffers to avoid data copy and these
  //   buffers are hooked up to bl.
  bl.clear();

  start_block_key.reserve(sizeof(hse_oid_t) + sizeof(uint32_t));
  cursor_block_key.reserve(sizeof(hse_oid_t) + sizeof(uint32_t));

  start_block_offset = block_offset(offset);
  start_block_rel_offset = offset - start_block_offset;
  offset2object_data_key(o.o_hse_oid, start_block_offset, &start_block_key);
  start_block_nb = start_block_offset >> DATA_BLOCK_SHIFT;

  end_block_offset = block_offset(offset + length);
  end_block_rel_length = end_block_length(offset, length);
  end_block_nb = end_block_offset >> DATA_BLOCK_SHIFT;

  if (start_block_nb == end_block_nb) {

    //
    // The read in contained in one block, do not use a cursor.
    //

    // Allocate a full data block.
    bufferptr ptr(buffer::create_page_aligned(DATA_BLOCK_LEN));

    rc = hse_kvs_get(_object_data_kvs, os, start_block_key.c_str(), start_block_key.length(),
      &found, ptr.c_str(), DATA_BLOCK_LEN, &val_len);
    if (rc) {
      dout(10) << __func__ << " failed to get object data block " << dendl;
      return rc;
    }


    if (found) {
      ceph_assert(val_len == DATA_BLOCK_LEN);
      ptr.set_offset(start_block_rel_offset);
      ptr.set_length(end_block_rel_length);
      bl.append(ptr);
    } else {
      // Return zeroes. Use our zeroed buffer zbuf.
      // Because create_static is used zbuf will not be freed by ceph
      bufferptr ptr1(buffer::create_static(DATA_BLOCK_LEN, (char *)zbuf));
      ptr1.set_offset(start_block_rel_offset);
      ptr1.set_length(end_block_rel_length);
      bl.append(ptr1);
    }
    return rc;
  }


  //
  // Several blocks need to be read, use a cursor.
  //


  //
  // Create a cursor with the hse oid as filter and that can see the result of the
  // previous write operations from the current Ceph transaction.
  //
  struct hse_kvs_cursor *cursor;

  rc = hse_kvs_cursor_create_wrapper(_object_data_kvs, os, (void *)(&o.o_hse_oid),
    sizeof(hse_oid_t), &cursor);
  if (rc) {
    dout(10) << __func__ << " failed to create a cursor data" << dendl;
    return rc;
  }

  //
  // Go to the first block. It may not exist.
  //
  rc = hse_kvs_cursor_seek_wrapper(cursor, os, start_block_key.c_str(),
      start_block_key.length(), &seek_found, &seek_found_len);
  if (rc) {
    dout(10) << __func__ << " failed to seek to start block" << dendl;
    goto end;
  }

  if (!seek_found_len) {
    //
    // There is no data to be read (at start offset and beyond). Return all zero.
    //
    append_zero_blocks_in_bl(start_block_nb,
      end_block_nb + 1, bl, start_block_rel_offset, end_block_rel_length);

    goto end;
  }

  ceph_assert(seek_found_len == OBJECT_DATA_KEY_LEN);
  prev_cursor_block_nb = start_block_nb - 1; // May be negative.

  while (true) {
    bool eof;
    uint32_t block_rel_offset;
    uint32_t block_rel_length;
    int32_t cursor_block_nb;

    rc = hse_kvs_cursor_read_wrapper(cursor, os,
      reinterpret_cast<const void **>(&cursor_block_key),
      &cursor_block_key_len, reinterpret_cast<const void **>(&val), &val_len, &eof);
    if (rc) {
      dout(10) << __func__ << " cursor read failed" << dendl;
      goto end;
    }
    if (eof) {
      // Adjust cursor_block_nb to add the correct number of zero buffers/pages
      // in the call to append_zero_blocks_in_bl() below.
      // We need zero buffers till end_block_nb included.
      cursor_block_nb = end_block_nb + 1;
    } else {
      ceph_assert(cursor_block_key_len == OBJECT_DATA_KEY_LEN);
      ceph_assert(val_len == DATA_BLOCK_LEN);
      cursor_block_nb = object_data_hse_key2block_nb(cursor_block_key);
      if (cursor_block_nb > end_block_nb + 1) {
	cursor_block_nb  = end_block_nb + 1;
      }
    }


    //
    // Add zeroed buffers for the blocks before the cursor position.
    //
    block_rel_offset = 0;
    if (prev_cursor_block_nb + 1 == start_block_nb) {
      // Adjust offset in the first buffer
      block_rel_offset = start_block_rel_offset;
    }
    block_rel_length = DATA_BLOCK_LEN;
    if (cursor_block_nb == end_block_nb + 1) {
      // Adjust length in the last buffer.
      block_rel_length = end_block_rel_length;
    }
    append_zero_blocks_in_bl(prev_cursor_block_nb + 1,
      cursor_block_nb, bl, block_rel_offset, block_rel_length);



    if ((cursor_block_nb > end_block_nb) || eof) {
      break;
    }

    //
    // Attach the data of the block at cursor position to the buffer list bl
    //

    // We need to do a copy until hse read cursor allows us to provide a buffer.

    // Allocate a full data block.
    bufferptr ptr(buffer::create_page_aligned(DATA_BLOCK_LEN));
    // Copy the data block we got from hse into ptr.
    ceph_assert(val_len == DATA_BLOCK_LEN);
    ptr.append((char *)val, DATA_BLOCK_LEN);

    // Can't do the below because the buffer "val" is reclaimed by hse as soon as cusor read
    // goes to the next block....
    // bufferptr ptr(buffer::create_static(DATA_BLOCK_LEN, (char *)val));

    if (cursor_block_nb == start_block_nb) {
      // This is the first block returned.
      // Adjust it start offset.
      ptr.set_offset(start_block_rel_offset);
    }
    if (cursor_block_nb == end_block_nb) {
      // This is the last block returned.
      // Adjust the length in the buffer.
      ptr.set_length(end_block_rel_length);
    }
    bl.append(ptr);


    prev_cursor_block_nb = cursor_block_nb;
  }

end:
  if (!rc) {
    ceph_assert(bl.length() == length);
  }
  rc1 = hse_kvs_cursor_destroy(cursor);
  if (rc1) {
    dout(10) << __func__ << " failed to destroy cursor" << dendl;
    if (!rc)
      rc = rc1;
  }
  return rc;
}

int HseStore::read(
     CollectionHandle &ch,
     const ghobject_t& oid,
     uint64_t offset,
     size_t len,
     ceph::buffer::list& bl,
     uint32_t op_flags)
{
  hse_err_t rc = 0;
  struct hse_kvdb_opspec os;
  HSE_KVDB_OPSPEC_INIT(&os);
  Onode o;

  Collection *c = static_cast<Collection*>(ch.get());
  c->get_onode(o, oid, true);
  if (!o.o_exists) {
    dout(10) << __func__ << " object doesn't exist " << *o.o_oid
	   << " " << offset << "~" << len << dendl;
    return -ENOENT;
  }

  rc = HseStore::kv_read_data(&os, o, offset, len, bl);

  return -hse_err_to_errno(rc);
}

/*
 * Try to avoid a data copy by passing to HSE (when possible) a pointer into one of the buffer
 * of the input Ceph bufferlist.
 */
hse_err_t HseStore::kv_write_data(
  struct hse_kvdb_opspec *os,
  CollectionRef& c,
  Onode& o,
  uint64_t offset, size_t length,
  bufferlist& bl)
{
  hse_err_t rc = 0;
  std::string object_data_key;
  object_data_key.reserve(sizeof(hse_oid_t) + sizeof(uint32_t));
  uint64_t block_offset;
  uint64_t start_offset = offset;
  uint64_t end_offset = offset + length;
  bufferlist::iterator i(&bl);
  uint32_t to_copy_in_block;

  dout(10) << __func__ << " " << c->cid << " " << *o.o_oid << " hse_oid " << o.o_hse_oid
	   << " " << offset << "~" << length
	   << dendl;

  // Loop on the data blocks.
  for (; length > 0; offset += to_copy_in_block, length -= to_copy_in_block) {
    bool merge = false;
    uint32_t block_wr_offset = 0; // Offset from beginning of block
    size_t val_len;
    uint8_t *wb;
    uint8_t *ceph_ptr;

    to_copy_in_block = DATA_BLOCK_LEN;
    block_offset = HseStore::block_offset(offset);
    offset2object_data_key(o.o_hse_oid, block_offset, &object_data_key);

    if (start_offset > block_offset) {
      // The data start is in the middle of the first block.
      merge = true;
      block_wr_offset = start_offset - block_offset;
      to_copy_in_block -= block_wr_offset;
    }
    if (end_offset < block_offset + DATA_BLOCK_LEN) {
      // The data end is in the middle of the last block.
      merge = true;
      to_copy_in_block -= block_offset + DATA_BLOCK_LEN - end_offset;
    }
    ceph_assert(to_copy_in_block);

    if (merge) {
      bool found;

      // A merge need to be done.

      //
      // Read the block.
      //

      rc = hse_kvs_get(_object_data_kvs, os, object_data_key.c_str(), object_data_key.length(),
	  &found, write_buf, DATA_BLOCK_LEN, &val_len);
      if (rc) {
        dout(10) << __func__ << " failed to get object data block " << c->cid << dendl;
        goto end;
      }

      if (found) {
	ceph_assert(val_len == DATA_BLOCK_LEN);
	//
	// Merge new data with old data read in the write_buf.
	//
	i.copy(to_copy_in_block, (char *)write_buf + block_wr_offset);
      } else {
	uint32_t copied;

	// Block doesn't exist yet, no need to merge, pad with zeroes.
	if (block_wr_offset)
	  memset(write_buf, 0, block_wr_offset);
	i.copy(to_copy_in_block, (char *)write_buf + block_wr_offset);
	copied = block_wr_offset + to_copy_in_block;
	if (copied < DATA_BLOCK_LEN)
	  memset(write_buf + copied, 0, DATA_BLOCK_LEN - copied);
      }
      wb = write_buf;

    } else {
      size_t got;

      //
      // No merge needed
      //

      ceph_assert(to_copy_in_block == DATA_BLOCK_LEN);
      ceph_assert(block_wr_offset == 0);

      //
      // To avoid a copy, try to get a pointer in the buffer list.
      //
      got = i.get_ptr_and_advance(DATA_BLOCK_LEN, (const char**)&ceph_ptr);
      ceph_assert(got <= DATA_BLOCK_LEN);
      if (got == DATA_BLOCK_LEN) {
	// No data copy!
	wb = ceph_ptr;
      } else {
	// Ceph buffer not big enough we need to do a copy
	memcpy(write_buf, ceph_ptr, got);
	i.copy(DATA_BLOCK_LEN - got, (char *)write_buf + got);
	wb = write_buf;
      }
    }


    // Write the block
    rc = hse_kvs_put(_object_data_kvs, os, static_cast<const void *>(object_data_key.c_str()),
      object_data_key.length(), wb, DATA_BLOCK_LEN);
    if (rc) {
      dout(10) << __func__ << " failed to get object data block " << c->cid << dendl;
      goto end;
    }
  }

end:
  if (!rc) {
    // Record the new length of the object data
    if (offset + length >= o.o_data_len) {
      rc = kv_update_obj_data_len(os, c, o, offset + length);
    }
  }
  dout(10) << __func__ << " return " << c->cid << " " << *o.o_oid << " hse_oid " << o.o_hse_oid
	   << " object data len " << o.o_data_len << " rc " << rc << dendl;
  return rc;
}

hse_err_t HseStore::write(
  struct hse_kvdb_opspec *os,
  CollectionRef& c,
  Onode& o,
  uint64_t offset, size_t length,
  bufferlist& bl,
  uint32_t fadvise_flags)
{
  hse_err_t rc;

  ceph_assert(o.o_gotten);

  if (!o.o_exists) {
    rc = kv_create_obj(os, c, o);
    if (rc)
      return rc;
  }
  rc = kv_write_data(os, c, o, offset, length, bl);
  return rc;
}


// Process (till hse_kvdb_txn_commit() returns) one transaction
void HseStore::start_one_transaction(Collection *c, Transaction *t)
{
  struct hse_kvdb_opspec os;
  hse_err_t rc = 0;
  HSE_KVDB_OPSPEC_INIT(&os);
  Transaction::iterator i = t->begin();

  dout(10) << __func__ << " entering " << c->cid << dendl;

  //
  // Start a hse transaction.
  //
  os.kop_txn = hse_kvdb_txn_alloc(_kvdb);
  rc = hse_kvdb_txn_begin(_kvdb, os.kop_txn);
  if (rc) {
    hse_kvdb_txn_free(_kvdb, os.kop_txn);
    dout(10) << __func__ << " failed to begin transaction " << c->cid << dendl;
    return;
  }

  std::vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (auto c : i.colls) {
    cvec[j++] = get_collection(c);
  }

  vector<HseStore::Onode> ovec(i.objects.size());

  for (int pos = 0; i.have_op(); ++pos) {
    const Transaction::Op *op = i.decode_op();

    // no coll or obj
    if (op->op == Transaction::OP_NOP)
      continue;

    // collection operations
    CollectionRef &c = cvec[op->cid];
    switch (op->op) {
    case Transaction::OP_RMCOLL:
      {
        const coll_t &cid = i.get_cid(op->cid);
        rc = remove_collection(&os, cid, &c);
        if (!rc)
          continue;
      }
      break;

    case Transaction::OP_MKCOLL:
      {
        ceph_assert(!c);
        const coll_t &cid = i.get_cid(op->cid);
        rc = create_collection(&os, cid, op->split_bits, &c);
        if (!rc)
          continue;
      }
      break;

    case Transaction::OP_SPLIT_COLLECTION:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_SPLIT_COLLECTION2:
      {
        const uint32_t bits = op->split_bits;
        const uint32_t rem = op->split_rem;
        rc = split_collection(&os, c, cvec[op->dest_cid], bits, rem);
        if (!rc)
          continue;
      }
      break;

    case Transaction::OP_MERGE_COLLECTION:
      {
        const uint32_t bits = op->split_bits;
	rc = merge_collection(&os, &c, cvec[op->dest_cid], bits);
        if (!rc)
          continue;
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
        ceph_abort_msg("not supported");
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      rc = EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RMATTR:
      rc = EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RENAME:
      ceph_abort_msg("not implemented");
      break;
    }

    if (rc != 0) {
	    /*
      derr << " error " << cpp_strerror(r)
	   << " not handled on operation " << op->op
	   << " (op " << pos << ", counting from 0)" << dendl;
      dout(0) << " transaction dump:\n";
      JSONFormatter f(true);
      f.open_object_section("transaction");
      t->dump(&f);
      f.close_section();
      f.flush(*_dout);
      *_dout << dendl;
      */
      ceph_abort_msg("unexpected error");
    }

    // object operations
    Onode &o = ovec[op->oid];
    if (!o.o_gotten) {
      // these operations implicity create the object
      bool create = false;
      if (op->op == Transaction::OP_TOUCH ||
	  op->op == Transaction::OP_CREATE ||
	  op->op == Transaction::OP_WRITE ||
	  op->op == Transaction::OP_ZERO) {
	create = true;
      }
      const ghobject_t &oid = i.get_oid(op->oid);
      c->get_onode(o, oid, create);
      if (!create) {
	if (!o.o_exists) {
	  dout(10) << __func__ << " op " << op->op << " got ENOENT on "
		   << oid << dendl;
	  rc = ENOENT;
	  goto endop;
	}
      }
    }

    switch (op->op) {
    case Transaction::OP_TOUCH:
    case Transaction::OP_CREATE:
      rc = kv_create_obj(&os, c, o);
      break;

    case Transaction::OP_WRITE:
      {
        uint64_t off = op->off;
        uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
	rc = write(&os, c, o, off, len, bl, fadvise_flags);
      }
      break;

#if 0
    case Transaction::OP_ZERO:
      {
        uint64_t off = op->off;
        uint64_t len = op->len;
	r = _zero(txc, c, o, off, len);
      }
      break;

    case Transaction::OP_TRIMCACHE:
      {
        // deprecated, no-op
      }
      break;

    case Transaction::OP_TRUNCATE:
      {
        uint64_t off = op->off;
	r = _truncate(txc, c, o, off);
      }
      break;
#endif

    case Transaction::OP_REMOVE:
      rc = kv_remove_obj(&os, c, o);
      break;

#if 0
    case Transaction::OP_SETATTR:
      {
        string name = i.decode_string();
        bufferlist bl;
        i.decode_bl(bl);
	map<string, bufferptr> to_set;
	to_set[name] = bufferptr(bl.c_str(), bl.length());
	r = _setattrs(txc, c, o, to_set);
      }
      break;

    case Transaction::OP_SETATTRS:
      {
        map<string, bufferptr> aset;
        i.decode_attrset(aset);
	r = _setattrs(txc, c, o, aset);
      }
      break;

    case Transaction::OP_RMATTR:
      {
	string name = i.decode_string();
	r = _rmattr(txc, c, o, name);
      }
      break;

    case Transaction::OP_RMATTRS:
      {
	r = _rmattrs(txc, c, o);
      }
      break;

    case Transaction::OP_CLONE:
      {
        const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef no = c->get_onode(noid, true);
	r = _clone(txc, c, o, no);
      }
      break;

    case Transaction::OP_CLONERANGE:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_CLONERANGE2:
      {
	const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef no = c->get_onode(noid, true);
        uint64_t srcoff = op->off;
        uint64_t len = op->len;
        uint64_t dstoff = op->dest_off;
	r = _clone_range(txc, c, o, no, srcoff, len, dstoff);
      }
      break;

    case Transaction::OP_COLL_ADD:
      ceph_abort_msg("not implemented");
      break;

    case Transaction::OP_COLL_REMOVE:
      ceph_abort_msg("not implemented");
      break;

    case Transaction::OP_COLL_MOVE:
      ceph_abort_msg("deprecated");
      break;

    case Transaction::OP_COLL_MOVE_RENAME:
      {
	ceph_assert(op->cid == op->dest_cid);
	const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef no = c->get_onode(noid, true);
	r = _rename(txc, c, o, no, noid);
	o.reset();
      }
      break;

    case Transaction::OP_TRY_RENAME:
      {
	const ghobject_t& noid = i.get_oid(op->dest_oid);
	OnodeRef no = c->get_onode(noid, true);
	r = _rename(txc, c, o, no, noid);
	if (r == -ENOENT)
	  r = 0;
	o.reset();
      }
      break;

    case Transaction::OP_OMAP_CLEAR:
      {
	r = _omap_clear(txc, c, o);
      }
      break;
#endif
    case Transaction::OP_OMAP_SETKEYS:
      {
	bufferlist aset_bl;
        i.decode_attrset_bl(&aset_bl);
	rc = HseStore::_omap_setkeys(&os, c, o, aset_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
	bufferlist keys_bl;
        i.decode_keyset_bl(&keys_bl);
	rc = HseStore::_omap_rmkeys(&os, c, o, keys_bl);
      }
      break;
#if 0
    case Transaction::OP_OMAP_RMKEYRANGE:
      {
        string first, last;
        first = i.decode_string();
        last = i.decode_string();
	r = _omap_rmkey_range(txc, c, o, first, last);
      }
      break;
    case Transaction::OP_OMAP_SETHEADER:
      {
        bufferlist bl;
        i.decode_bl(bl);
	r = _omap_setheader(txc, c, o, bl);
      }
      break;

    case Transaction::OP_SETALLOCHINT:
      {
        uint64_t expected_object_size = op->expected_object_size;
        uint64_t expected_write_size = op->expected_write_size;
	uint32_t flags = op->alloc_hint_flags;
	r = _setallochint(txc, c, o,
			  expected_object_size,
			  expected_write_size,
			  flags);
      }
      break;
#endif

    default:
      derr << "bad op " << op->op << dendl;
      ceph_abort();
    }

  endop:
    if (rc != 0) {
      bool ok = false;

      if (rc == ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD))
	// ENOENT is usually okay
	ok = true;
      if (rc == ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (rc == ENOENT && (op->op == Transaction::OP_CLONERANGE ||
			     op->op == Transaction::OP_CLONE ||
			     op->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (rc == ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC from key value store, misconfigured cluster";

	if (rc == ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	}

	dout(0) << " error " << cpp_strerror(hse_err_to_errno(rc)) <<
	  " not handled on operation " << op->op
	  << " (op " << pos << ", counting from 0)" << dendl;
	dout(0) << msg << dendl;
	dout(0) << " transaction dump:\n";
	JSONFormatter f(true);
	f.open_object_section("transaction");
	t->dump(&f);
	f.close_section();
	f.flush(*_dout);
	*_dout << dendl;
	ceph_abort_msg("unexpected error");
      }
    }
  }

  //
  // Commit the transaction in hse
  //
  if (rc) {
      hse_kvdb_txn_abort(_kvdb, os.kop_txn);
      dout(10)  << __func__ << " transaction aborted" << dendl;
  } else {
    rc = hse_kvdb_txn_commit(_kvdb, os.kop_txn);
    if (rc) {
      dout(10)  << __func__ << " failed to commit transaction" << dendl;
    }
  }
  hse_kvdb_txn_free(_kvdb, os.kop_txn);
}

// some things we encode in binary (as le32 or le64); print the
// resulting key strings nicely
static string pretty_binary_string(const std::string& in)
{
  char buf[10];
  std::string out;
  out.reserve(in.length() * 3);
  enum { NONE, HEX, STRING } mode = NONE;
  unsigned from = 0, i;
  for (i=0; i < in.length(); ++i) {
    if ((in[i] < 32 || (unsigned char)in[i] > 126) ||
	(mode == HEX && in.length() - i >= 4 &&
	 ((in[i] < 32 || (unsigned char)in[i] > 126) ||
	  (in[i+1] < 32 || (unsigned char)in[i+1] > 126) ||
	  (in[i+2] < 32 || (unsigned char)in[i+2] > 126) ||
	  (in[i+3] < 32 || (unsigned char)in[i+3] > 126)))) {
      if (mode == STRING) {
	out.append(in.substr(from, i - from));
	out.push_back('\'');
      }
      if (mode != HEX) {
	out.append("0x");
	mode = HEX;
      }
      if (in.length() - i >= 4) {
	// print a whole u32 at once
	snprintf(buf, sizeof(buf), "%08x",
		 (uint32_t)(((unsigned char)in[i] << 24) |
			    ((unsigned char)in[i+1] << 16) |
			    ((unsigned char)in[i+2] << 8) |
			    ((unsigned char)in[i+3] << 0)));
	i += 3;
      } else {
	snprintf(buf, sizeof(buf), "%02x", (int)(unsigned char)in[i]);
      }
      out.append(buf);
    } else {
      if (mode != STRING) {
	out.push_back('\'');
	mode = STRING;
	from = i;
      }
    }
  }
  if (mode == STRING) {
    out.append(in.substr(from, i - from));
    out.push_back('\'');
  }
  return out;
}

/*
 * Reverse hexa encoding.
 * Integer value 10 is encoded in hexa as 'a'.
 * This function from 'a' in input returns 10.
 */
static inline unsigned h2i(uint8_t c)
{
  if ((c >= '0') && (c <= '9')) {
    return c - 0x30;
  } else if ((c >= 'a') && (c <= 'f')) {
    return c - 'a' + 10;
  } else if ((c >= 'A') && (c <= 'F')) {
    return c - 'A' + 10;
  } else {
    return 256; // make it always larger than 255
  }
}

static void _key_encode_shard(shard_id_t shard, std::string *key)
{
  key->push_back((char)((uint8_t)shard.id + (uint8_t)0x80));
}

static const char *_key_decode_shard(const char *key, shard_id_t *pshard)
{
  pshard->id = (uint8_t)*key - (uint8_t)0x80;
  return key + 1;
}

/*
 * Used by ghobject_t2key() to encode the strings from ghobject_t into the key
 * that is the ghobject_t2key() output.
 *
 * The key string needs to lexicographically sort the same way that
 * ghobject_t does.  We do this by escaping anything <= to '#' with #
 * plus a 2 digit hex string, and anything >= '~' with ~ plus the two
 * hex digits.
 *
 * We use ! as a terminator for strings; this works because it is < #
 * and will get escaped if it is present in the string.
 *
 * The bug deescribed below present in KStore and BlueStore implementation is fixed.
 *   Bug: due to implicit character type conversion in comparison it may produce
 *   unexpected ordering.
 */
static void append_escaped(const string &in, std::string *out)
{
  uint8_t hexbyte[in.length() * 3 + 1];
  uint8_t* ptr = &hexbyte[0];
  for (string::const_iterator i = in.begin(); i != in.end(); ++i) {
    uint8_t u = *i; // Fix the bug present in BlueStore and KStore.

    if (u <= '#') {
      *ptr++ = '#';
      *ptr++ = "0123456789abcdef"[(u >> 4) & 0x0f];
      *ptr++ = "0123456789abcdef"[u & 0x0f];
    } else if (u >= '~') {
      *ptr++ = '~';
      *ptr++ = "0123456789abcdef"[(u >> 4) & 0x0f];
      *ptr++ = "0123456789abcdef"[u & 0x0f];
    } else {
      *ptr++  = u;
    }
  }
  *ptr++ = '!';
  out->append((char *)hexbyte, ptr - &hexbyte[0]);
}

static int decode_escaped(const char *p, string *out)
{
  uint8_t buff[256];
  uint8_t* ptr = &buff[0];
  uint8_t* max = &buff[252];
  const char *orig_p = p;
  while (*p && *p != '!') {
    if (*p == '#' || *p == '~') {
      unsigned hex = 0;
      p++;
      hex = h2i(*p++) << 4;
      if (hex > 255) {
        return -EINVAL;
      }
      hex |= h2i(*p++);
      if (hex > 255) {
        return -EINVAL;
      }
      *ptr++ = hex;
    } else {
      *ptr++ = *p++;
    }
    if (ptr > max) {
       out->append((char *)buff, ptr-buff);
       ptr = &buff[0];
    }
  }
  if (ptr != buff) {
     out->append((char *)buff, ptr-buff);
  }
  return p - orig_p;
}

#undef dout_prefix
#define dout_prefix *_dout << "hsestore "

/*
 * Convert a ghobject_t into a string that can be used in a hse key.
 * This string may contain non printable characters.
 * Based on Bluestore implementation (get_object_key()).
 *
 * encoded u8: shard + 2^7 (so that it sorts properly)
 * encoded u64: poolid + 2^63 (so that it sorts properly)
 * encoded u32: hash (bit reversed)
 *
 * escaped string: namespace
 *
 * escaped string: key or object name
 * 1 char: '<', '=', or '>'.  if =, then object key == object name, and
 *         we are done.  otherwise, we are followed by the object name.
 * escaped string: object name (unless '=' above)
 *
 * encoded u64: snap
 * encoded u64: generation
 */
static void ghobject_t2key(CephContext *cct, const ghobject_t& oid, std::string *key)
{
  key->clear();

  size_t max_len = ENCODED_KEY_PREFIX_LEN +
                  (oid.hobj.nspace.length() * 3 + 1) +
                  (oid.hobj.get_key().length() * 3 + 1) +
                   1 + // for '<', '=', or '>'
                  (oid.hobj.oid.name.length() * 3 + 1) +
                   8 + 8;
  key->reserve(max_len);

  //
  // shard_id.
  // One byte signed. The value is positive or is -1 (aka NO_SHARD).
  // Encoded u8: shard + 2^7 (so that it sorts properly).
  //
  _key_encode_shard(oid.shard_id, key);

  //
  // pool
  // Encoded u64: poolid + 2^63 (so that it sorts properly)
  //
  _key_encode_u64(oid.hobj.pool + 0x8000000000000000ull, key);

  //
  // hash
  // Encoded u32: hash (bit reversed)
  //
  _key_encode_u32(oid.hobj.get_bitwise_key_u32(), key);

  // Namespace
  append_escaped(oid.hobj.nspace, key);


  if (oid.hobj.get_key().length()) {
    // is a key... could be < = or >.
    append_escaped(oid.hobj.get_key(), key);
    // (ASCII chars < = and > sort in that order, yay)
    int r = oid.hobj.get_key().compare(oid.hobj.oid.name);
    if (r) {
      key->append(r > 0 ? ">" : "<");
      append_escaped(oid.hobj.oid.name, key);
    } else {
      // same as no key
      key->append("=");
    }
  } else {
    // no key
    append_escaped(oid.hobj.oid.name, key);
    key->append("=");
  }

  _key_encode_u64(oid.hobj.snap, key);
  _key_encode_u64(oid.generation, key);

  // sanity check
  if (true) {
    ghobject_t t;
    int r = key2ghobject_t(*key, t);
    if (r || t != oid) {
      derr << "  r " << r << dendl;
      derr << "key " << pretty_binary_string(*key) << dendl;
      derr << "oid " << oid << dendl;
      derr << "  t " << t << dendl;
      ceph_assert(t == oid);
    }
  }
}

void HseStore::collection_object_hse_key(CollectionRef& c, Onode& o, std::string *key)
{
  key->clear();
  key->append(c->_coll_tkey);
  key->append(o.o_ghobject_tkey);
  _key_encode_u64(o.o_hse_oid, key);
}

/*
 * Get the hse_oid from the ghobject_t looking in _collection_object_kvs
 */
hse_err_t HseStore::ghobject_t2hse_oid(const coll_t &cid, const ghobject_t &oid, bool& found,
    HseStore::hse_oid_t& hse_oid, uint64_t& o_data_len)
{
  hse_err_t rc = 0;
  hse_err_t rc1;
  const void *foundkey = nullptr;
  size_t foundkey_len;

  std::string coll_tkey;
  std::string ghobject_tkey;

  coll_t2key(cct, cid, &coll_tkey);
  ghobject_t2key(cct, oid, &ghobject_tkey);

  const size_t filt_min_len = coll_tkey.size() + ghobject_tkey.size();
  const size_t filt_max_len = filt_min_len + sizeof(HseStore::hse_oid_t);

  auto filt_min = std::make_unique<uint8_t[]>(filt_min_len);
  auto filt_max = std::make_unique<uint8_t[]>(filt_max_len);
  memcpy(filt_min.get(), coll_tkey.c_str(), coll_tkey.size());
  memcpy(filt_min.get() + coll_tkey.size(), ghobject_tkey.c_str(), ghobject_tkey.size());
  memcpy(filt_max.get(), filt_min.get(), filt_min_len);
  // guarantees that the last bits of filt_max are UINT64_MAX
  memset(filt_max.get() + filt_min_len, 0xFF, sizeof(HseStore::hse_oid_t));

  struct hse_kvs_cursor *cursor;
  rc = hse_kvs_cursor_create_wrapper(_collection_object_kvs, nullptr, coll_tkey.c_str(),
      coll_tkey.size(), &cursor);
  if (rc) {
    dout(10) << " failed to create cursor when check existence of object ("
      << oid << ") with collection (" << cid << ')' << dendl;
    return rc;
  }

  // if everything is correct, there is only one key in this range
  rc = hse_kvs_cursor_seek_range(cursor, nullptr, filt_min.get(),
    filt_min_len, filt_max.get(), filt_max_len, &foundkey, &foundkey_len);
  if (rc) {
    dout(10) << " failed to check existence of object ("
      << oid << ") in collection (" << cid << ')' << dendl;
    goto err_out;
  }

  if (foundkey_len) {
    const char *p;
    uint64_t val;
    size_t val_len;
    bool found_val;

    ceph_assert(foundkey_len == filt_max_len);
    // The last 8 bytes of the key are the hse_oid.
    p = &(((const char *)foundkey)[foundkey_len - sizeof(hse_oid_t)]);
    p = _key_decode_u64(p, &hse_oid);

    rc = hse_kvs_get(_collection_object_kvs, nullptr, foundkey, foundkey_len,
      &found_val, &val, sizeof(val), &val_len);
    if (rc) {
      dout(10) << " failed to get the object data length" << dendl;
      goto err_out;
    }

    ceph_assert(found_val);
    ceph_assert(val_len == sizeof(uint64_t));
    p = (char *)(&val);
    p = _key_decode_u64(p, &o_data_len);
    found = true;
  }

err_out:
  rc1 = hse_kvs_cursor_destroy(cursor);
  if (rc1) {
    dout(10) << " failed to destroy cursor while checking existence of object ("
      << oid << ") in collection (" << cid << ')' << dendl;
    if (!rc)
      rc = rc1;
  }

  return rc ? rc : rc1;
}

/*
 * Reverse of ghobject_t2key().
 * Based on Bluestore implementation (get_key_object()).
 */
static int key2ghobject_t(const std::string_view key, ghobject_t &oid)
{
  int r;
  uint64_t pool;
  unsigned hash;
  string k;
  const char *p = key.data();

  if (key.length() < ENCODED_KEY_PREFIX_LEN)
    return -1;

  p = _key_decode_shard(p, &oid.shard_id);

  p = _key_decode_u64(p, &pool);
  oid.hobj.pool = pool - 0x8000000000000000ull;

  p = _key_decode_u32(p, &hash);

  oid.hobj.set_bitwise_key_u32(hash);

  if (key.length() == ENCODED_KEY_PREFIX_LEN)
    return -2;

  r = decode_escaped(p, &oid.hobj.nspace);
  if (r < 0)
    return -2;
  p += r + 1;

  r = decode_escaped(p, &k);
  if (r < 0)
    return -3;
  p += r + 1;
  if (*p == '=') {
    // no key
    ++p;
    oid.hobj.oid.name = k;
  } else if (*p == '<' || *p == '>') {
    // key + name
    ++p;
    r = decode_escaped(p, &oid.hobj.oid.name);
    if (r < 0)
      return -5;
    p += r + 1;
    oid.hobj.set_key(k);
  } else {
    // malformed
    return -6;
  }

  p = _key_decode_u64(p, &oid.hobj.snap.val);
  p = _key_decode_u64(p, &oid.generation);

  if (*p) {
    // if we get something other than a null terminator here,
    // something goes wrong.
    return -8;
  }

  return 0;
}

/*
 * Convert a coll_t into a 14 bytes "key"
 * The order on coll_t is defined by coll_t::operator<
 * The first byte of the output, letter P, T or M  maintain that order.
 * The output is always 14 bytes: 1 (type) + 1 (shard) + 4 (seed) + 8 (pool)
 */
static void coll_t2key(CephContext *cct, const coll_t& coll, std::string *key)
{
  spg_t pgid;

  key->clear();

  if (coll.is_pg_prefix(&pgid)) {
    if (coll.is_pg()) {
      key->append("P"); // TYPE_PG
    } else {
      key->append("T"); // TYPE_PG_TEMP
    }
    // 1+4+8 bytes below
    _key_encode_shard(pgid.shard, key);
    _key_encode_u32(pgid.pgid.m_seed, key);
    _key_encode_u64(pgid.pgid.m_pool, key);
  } else {
    // Metadata collection
    // 10 bytes
    key->append("M1234567890123"); // TYPE_META
  }

  ceph_assert_always(key->size() == ENCODED_KEY_COLL);
}

/*
 * Get a coll_t from an encoded key use in hse key to represent coll_t
 * Reverse of coll_t2key()
 */
static int key2coll_t(const std::string_view key, coll_t &coll)
{
  spg_t pgid;
  const char *p = key.data();

  if (key.length() != ENCODED_KEY_COLL)
    return -1;

  if (*p == 'M') {
    coll = coll_t();
  } else if (*p == 'P') {
    // Temporary collections are not persisted.
    p++;
    p = _key_decode_shard(p, &pgid.shard);
    p = _key_decode_u32(p, &pgid.pgid.m_seed);
    p = _key_decode_u64(p, &pgid.pgid.m_pool);

    coll = coll_t(pgid);
  } else {
    return -1;
  }
  return 0;
}

hse_err_t HseStore::kv_update_obj_data_len(
  struct hse_kvdb_opspec *os,
  CollectionRef& c,
  Onode& o,
  uint64_t object_data_len)
{
  hse_err_t rc;
  std::string key;
  std::string sval;

  collection_object_hse_key(c, o, &key);

  _key_encode_u64(object_data_len, &sval); // 8 bytes

  rc = hse_kvs_put(_collection_object_kvs, os, key.c_str(), key.length(),
    sval.c_str(), sval.length());
  if (rc) {
    dout(10) << __func__ << " failed to put in _collection_object_kvs" << dendl;
    return rc;
  }
  o.o_data_len = object_data_len;

  return rc;
}

hse_err_t HseStore::kv_create_obj(
  struct hse_kvdb_opspec *os,
  CollectionRef& c,
  Onode& o)
{
  hse_err_t rc;

  ceph_assert(!o.o_exists);

  o.o_hse_oid = this->_next_hse_oid++;

  dout(10) << __func__ << " " << *o.o_oid << " hse_oid " << o.o_hse_oid << " t_seq " <<
    c->_t_seq_next << dendl;

  rc = HseStore::kv_update_obj_data_len(os, c, o, 0);
  if (rc) {
    dout(10) << __func__ << " failed to create object in _collection_object_kvs" << dendl;
    return rc;
  }
  o.o_exists = true;
  return rc;
}

hse_err_t HseStore::kv_remove_obj(
  struct hse_kvdb_opspec *os,
  CollectionRef& c,
  Onode& o)
{
  hse_err_t rc;
  std::string prefix;

  if (!o.o_exists)
    return 0;

  dout(10) << __func__ << " exists, removing oid " <<
      *o.o_oid << " hse_oid " << o.o_hse_oid << dendl;
    
  _key_encode_u64(o.o_hse_oid, &prefix);

  //
  // Remove object xattr
  //
  rc = hse_kvs_prefix_delete(_object_xattr_kvs, os, prefix.c_str(), prefix.length(),
    nullptr);
  if (rc) {
    dout(10) << __func__ << " failed to prefix delete object from _object_xattr_kvs oid " <<
      *o.o_oid << dendl;
    return rc;
  }
 
  //
  // Remove object omap
  //
  rc = hse_kvs_prefix_delete(_object_omap_kvs, os, prefix.c_str(), prefix.length(),
    nullptr);
  if (rc) {
    dout(10) << __func__ << " failed to prefix delete object from _object_omap_kvs oid " <<
      *o.o_oid << dendl;
    return rc;
  }

  //
  // Remove object data
  //
  rc = hse_kvs_prefix_delete(_object_data_kvs, os, prefix.c_str(), prefix.length(),
    nullptr);
  if (rc) {
    dout(10) << __func__ << " failed to prefix delete object from _object_data_kvs oid " <<
      *o.o_oid << dendl;
    return rc;
  }

  //
  // Remove the association object-collection
  //
  collection_object_hse_key(c, o, &prefix);
  rc = hse_kvs_delete(_collection_object_kvs, os, prefix.c_str(), prefix.length());
  if (rc) {
    dout(10) << __func__ << " failed to delete object from _collection_object_kvs oid " <<
      *o.o_oid << dendl;
    return rc;
  }

  o.o_exists = false;
  return 0;
}

/* Temporary hack, TODO */
int HseStore::statfs(struct store_statfs_t* buf0, osd_alert_list_t* alerts)
{
  buf0->reset();

  // Todo put real values.
  // For now reply always 2 TB
  buf0->total = 2ULL*1024*1024*1204*1024;
  buf0->available = buf0->total;

  return 0;
}

ObjectMap::ObjectMapIterator HseStore::get_omap_iterator(
  CollectionHandle &ch,
  const ghobject_t &oid)
{
  Onode o;
  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;


  Collection *c = static_cast<Collection*>(ch.get());
  c->get_onode(o, oid, false);
  if (!o.o_exists) {
    dout(10) << __func__ << " object doesn't exist " << oid << dendl;
    return ObjectMap::ObjectMapIterator(); // nullptr
  }


  return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(c, o));
}

/*
 * Build a key to use on the kvs _object_omap_kvs
 */
void HseStore::omap_hse_key(
    hse_oid_t hse_oid,
    const std::string& omap_key,
    uint32_t omap_block_nb,
    std::string *omap_block_hse_key)
{
  omap_block_hse_key->clear();
  _key_encode_u64(hse_oid, omap_block_hse_key);
  omap_block_hse_key->append(omap_key);
  _key_encode_u32(omap_block_nb, omap_block_hse_key);
}

/*
 * Remove/delete one entry from an object omap.
 */
hse_err_t HseStore::kv_omap_rm_entry(
    struct hse_kvdb_opspec *os,
    Onode& o,
    const std::string& omap_key)
{
  struct hse_kvs_cursor *cursor;
  std::string omap_block_hse_key;
  hse_err_t rc, rc1;
  uint32_t omap_block_nb = 0;
  size_t omap_block_hse_key_len;
  const void *seek_found;
  size_t seek_found_len;

  dout(10) << __func__ << " " << *o.o_oid << " hse_oid " << o.o_hse_oid
	   << dendl;

  //
  // Create a cursor with (hse oid + omap key) as filter.
  //
  omap_hse_key(o.o_hse_oid, omap_key, omap_block_nb, &omap_block_hse_key);
  omap_block_hse_key_len = omap_block_hse_key.length();
  rc = hse_kvs_cursor_create_wrapper(_object_omap_kvs, os, (void *)omap_block_hse_key.c_str(),
    omap_block_hse_key_len - OMAP_BLOCK_NB_LEN, &cursor);
  if (rc) {
    dout(10) << __func__ << " failed to create a cursor on object omap kvs" << dendl;
    return rc;
  }

  //
  // Iterate on the omap value blocks.
  // No need to read them, seek is enough because we delete them.
  //
  while (true) {
    rc = hse_kvs_cursor_seek_wrapper(cursor, os, omap_block_hse_key.c_str(),
      omap_block_hse_key.length(), &seek_found, &seek_found_len);
    if (rc) {
      dout(10) << __func__ << " failed to seek to an omap data block" << dendl;
      goto end;
    }

    if (!seek_found_len) {
      goto end;
    }

    //
    // Delete the k/v pair
    //
    rc = hse_kvs_delete(_object_omap_kvs, os, seek_found, seek_found_len);
    if (rc) {
      dout(10) << __func__ << " failed to delete an omap data block" << dendl;
      goto end;
    }


    // Key for the next omap data block number
    omap_block_hse_key.resize(omap_block_hse_key_len - OMAP_BLOCK_NB_LEN);
    _key_encode_u32(++omap_block_nb, &omap_block_hse_key);
  }

end:
  rc1 = hse_kvs_cursor_destroy(cursor);
  if (rc1) {
    dout(10) << __func__ << " failed to destroy cursor" << dendl;
  }
  return rc ? rc : rc1;
}

hse_err_t HseStore::_omap_rmkeys(
    struct hse_kvdb_opspec *os,
    CollectionRef& c,
    Onode& o,
    bufferlist& keys_bl)
{
  dout(10) << __func__ << " " << c->cid << " " << *o.o_oid << " hse_oid " << o.o_hse_oid
	   << " exists " << o.o_exists << dendl;

  if (!o.o_exists)
    return ENOENT;

  auto p = keys_bl.cbegin();
  uint32_t num;
  decode(num, p);
  while (num--) {
    hse_err_t rc;

    string key;
    decode(key, p);
    rc = HseStore::kv_omap_rm_entry(os, o, key);
    if (rc) {
      dout(10) << __func__ << " failed to remove one omap k/v " << c->cid << " "
	<< *o.o_oid << " hse_oid " << o.o_hse_oid << dendl;
      return rc;
    }
  }
  return 0;
}

hse_err_t HseStore::kv_omap_put(
  struct hse_kvdb_opspec *os,
  CollectionRef& c,
  Onode& o,
  std::string& omap_key,
  bufferlist& omap_value)
{
  hse_err_t rc;
  uint32_t block_nb;
  size_t left = omap_value.length();
  bufferlist::iterator iter(&omap_value);
  size_t in_buffer;
  std::string omap_block_hse_key;
  size_t omap_block_hse_key_len;

  dout(10) << __func__ << " " << c->cid << " " << *o.o_oid << " hse_oid " << o.o_hse_oid
	   << dendl;

  // The hse key is hse_oid + omap_key + block number
  omap_hse_key(o.o_hse_oid, omap_key, 0, &omap_block_hse_key);
  omap_block_hse_key_len = omap_block_hse_key.length();

  // Loop on the omap blocks
  for (block_nb = 0; left ; block_nb++, left -= in_buffer) {
    uint8_t *ceph_ptr;
    size_t got;
    uint8_t *wb;

    in_buffer = OMAP_BLOCK_LEN;
    if (in_buffer > left)
      in_buffer = left;

    // Copy in an intermediate buffer if needed
    got = iter.get_ptr_and_advance(in_buffer, (const char**)&ceph_ptr);
    ceph_assert(got <= in_buffer);
    if (got == in_buffer) {
      // No data copy!
      wb = ceph_ptr;
    } else {
      // Ceph buffer not big enough, we need to do a copy
      memcpy(write_buf, ceph_ptr, got);
      iter.copy(in_buffer - got, (char *)write_buf + got);
      wb = write_buf;
    }

    // Compute the hse key for the omap value block number
    omap_block_hse_key.resize(omap_block_hse_key_len - OMAP_BLOCK_NB_LEN);
    _key_encode_u32(block_nb, &omap_block_hse_key);
    rc = hse_kvs_put(_object_omap_kvs, os, omap_block_hse_key.c_str(),
	omap_block_hse_key.length(), wb, in_buffer);
    if (rc) {
      dout(10) << __func__ << " failed to put " << c->cid << " " << *o.o_oid << " hse_oid "
	<< o.o_hse_oid << dendl;
      return rc;
    }
  }
  return 0;
}

hse_err_t HseStore::_omap_setkeys(
    struct hse_kvdb_opspec *os,
    CollectionRef& c,
    Onode& o,
    bufferlist& keys_bl)
{
  hse_err_t rc;

  dout(10) << __func__ << " " << c->cid << " " << *o.o_oid << " hse_oid " << o.o_hse_oid
	   << " exists " << o.o_exists << dendl;

  if (!o.o_exists)
    return ENOENT;

  auto p = keys_bl.cbegin();
  uint32_t num;
  decode(num, p);
  while (num--) {
    string key;
    bufferlist value;

    decode(key, p);
    decode(value, p);
    rc = HseStore::kv_omap_put(os, c, o, key, value);
    if (rc) {
      dout(10) << __func__ << " " << c->cid << " " << *o.o_oid << " hse_oid " << o.o_hse_oid
	   << " rc " << rc << dendl;
      return rc;
    }
  }
  return 0;
}

/*
 * If there is no omap header, it is not an error. 0 is returned and "header" is not updated.
 */
hse_err_t HseStore::kv_omap_header_read(
    struct hse_kvdb_opspec *os,
    Onode& o,
    ceph::buffer::list *header,
    bool& found)
{
  std::string hse_key;
  size_t val_len;
  hse_err_t rc;

  dout(10) << __func__ << " " << *o.o_oid << " hse_oid " << o.o_hse_oid
	   << dendl;

  found = false;

  // The hse key for the omap header is simply the hse_oid_t
  _key_encode_u64(o.o_hse_oid, &hse_key);


  // Get the omap header size.
  rc = hse_kvs_get(_object_omap_kvs, os, hse_key.c_str(), hse_key.length(), &found,
      nullptr, 0, &val_len);

  if (rc)
    return rc;

  if (!found) {
    // The object has no omap header. It not an error.
    return 0;
  }

  {
    bufferptr ptr(val_len); // allocate Ceph buffer
    rc = hse_kvs_get(_object_omap_kvs, os, hse_key.c_str(), hse_key.length(), &found,
        ptr.c_str(), ptr.length(), &val_len);
    if (rc)
     return rc;

    ceph_assert(found);
    ceph_assert(ptr.length() == val_len);
    header->clear();
    header->append(ptr);
  }
  return 0;
}


int HseStore::omap_get_header(CollectionHandle &ch, const ghobject_t &oid,
    ceph::buffer::list *header, bool allow_eio)
{
  hse_err_t rc;
  Onode o;
  bool found;

  dout(10) << __func__ << " " << oid << dendl;

  Collection *c = static_cast<Collection*>(ch.get());
  c->get_onode(o, oid, false);
  if (!o.o_exists) {
    dout(10) << __func__ << " object doesn't exist " << oid << dendl;
    return -ENOENT;
  }

  rc = HseStore::kv_omap_header_read(nullptr, o, header, found);

  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::omap_get_keys(
    CollectionHandle &ch,
    const ghobject_t &oid,
    std::set<std::string> *keys)
{
  struct hse_kvs_cursor *cursor;
  struct OmapBlk0 blk0;
  std::string omap_key;
  bool found;
  Onode o;
  hse_err_t rc;

  dout(10) << __func__ << " " << oid << dendl;

  Collection *c = static_cast<Collection*>(ch.get());
  c->get_onode(o, oid, false);
  if (!o.o_exists) {
    dout(10) << __func__ << " object doesn't exist " << oid << dendl;
    return -ENOENT;
  }

  rc = kv_omap_create_hse_cursor(nullptr, o, &cursor);
  if (rc) {
    dout(10) << __func__ << " kv_omap_create_hse_cursor() failed " << oid << dendl;
    return -hse_err_to_errno(rc);
  }

  do {
    rc = kv_omap_read_entry(nullptr, cursor, false, blk0, omap_key, nullptr, found);
    if (rc)
      goto end;
    if (found)
      keys->insert(omap_key);
  } while (!found);

end:
  hse_kvs_cursor_destroy(cursor);

  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::omap_get_values(
    CollectionHandle &ch,
    const ghobject_t &oid,
    const std::set<std::string> &keys,
    std::map<std::string, ceph::buffer::list> *out)
{
  struct hse_kvs_cursor *cursor;
  struct OmapBlk0 blk0;
  bool found;
  Onode o;
  hse_err_t rc;
  const char *seek_found;
  size_t seek_found_len;

  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;

  Collection *c = static_cast<Collection*>(ch.get());
  c->get_onode(o, oid, false);
  if (!o.o_exists) {
    dout(10) << __func__ << " object doesn't exist " << oid << dendl;
    return -ENOENT;
  }

  rc = kv_omap_create_hse_cursor(nullptr, o, &cursor);
  if (rc) {
    dout(10) << __func__ << " kv_omap_create_hse_cursor() failed " << oid << dendl;
    return -hse_err_to_errno(rc);
  }

  //
  // Loop on the input keys
  //

  for(const auto& key : keys) {
    bufferlist val_bl;
    std::string hse_key;
    std::string omap_key;

    blk0.already_read = false;
    hse_key.clear();

    omap_hse_key(o.o_hse_oid, key, 0, &hse_key);

    rc = HseStore::hse_kvs_cursor_seek_wrapper(cursor, nullptr, hse_key.c_str(), hse_key.length(),
      (const void **)&seek_found, &seek_found_len);
    if (!rc)
      goto end;

    if (!seek_found_len)
      continue;

    if (hse_key.compare(seek_found))
      continue;

    rc = kv_omap_read_entry(nullptr, cursor, false, blk0, omap_key, &val_bl, found);
    if (rc)
      goto end;

    ceph_assert(found);
    ceph_assert(key.compare(omap_key) == 0);
    out->insert({key, val_bl});
  }

end:
  hse_kvs_cursor_destroy(cursor);

  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::omap_check_keys(
    CollectionHandle &ch,
    const ghobject_t &oid,
    const std::set<std::string> &keys,
    std::set<std::string> *out)
{
  hse_err_t rc = 0;
  Onode o;
  bool found;

  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;

  Collection *c = static_cast<Collection*>(ch.get());
  c->get_onode(o, oid, false);
  if (!o.o_exists) {
    dout(10) << __func__ << " object doesn't exist " << oid << dendl;
    return -ENOENT;
  }
  //
  // Loop on the input keys
  //

  for(const auto& key : keys) {
    std::string hse_key;
    size_t val_len;

    omap_hse_key(o.o_hse_oid, key, 0, &hse_key);

    rc = hse_kvs_get(_object_omap_kvs, nullptr, hse_key.c_str(), hse_key.length(), &found,
      nullptr, 0, &val_len);
    if (rc)
      goto end;

    if (found)
      out->insert(key);
  }

end:
  return rc ? -hse_err_to_errno(rc) : 0;
}

/*
 */
int HseStore::omap_get(CollectionHandle &ch, const ghobject_t &oid, ceph::buffer::list *header,
    std::map<std::string, ceph::buffer::list> *out)
{
  hse_err_t rc;
  Onode o;
  bool found;
  struct OmapBlk0 blk0;
  struct hse_kvs_cursor *cursor;

  dout(10) << __func__ << " " << ch->cid << " " << oid << dendl;

  Collection *c = static_cast<Collection*>(ch.get());
  c->get_onode(o, oid, false);
  if (!o.o_exists) {
    dout(10) << __func__ << " object doesn't exist " << oid << dendl;
    return -ENOENT;
  }

  rc = HseStore::kv_omap_header_read(nullptr, o, header, found);
  if (rc) {
    dout(10) << __func__ << " kv_omap_create_haeder_read() failed " << oid << dendl;
    return -hse_err_to_errno(rc);
  }

  rc = kv_omap_create_hse_cursor(nullptr, o, &cursor);
  if (rc) {
    dout(10) << __func__ << " kv_omap_create_hse_cursor() failed " << oid << dendl;
    return -hse_err_to_errno(rc);
  }

  do {
    bufferlist val_bl;
    std::string omap_key;

    rc = kv_omap_read_entry(nullptr, cursor, false, blk0, omap_key, &val_bl, found);
    if (rc)
      goto end;
    if (found)
      out->insert({omap_key, val_bl});
  } while (!found);

end:
  hse_kvs_cursor_destroy(cursor);
  return rc ? -hse_err_to_errno(rc) : 0;
}

/*
 * opspec->kop_txn must be NULL when hse_kvs_cursor_create() is called
 */
hse_err_t HseStore::hse_kvs_cursor_create_wrapper(
    struct hse_kvs *        kvs,
    struct hse_kvdb_opspec *opspec,
    const void *            filt,
    size_t                  filt_len,
    struct hse_kvs_cursor **cursor)
{

  hse_err_t rc;
  struct hse_kvdb_txn *kop_txn_sav;

  if (opspec) {
    kop_txn_sav = opspec->kop_txn;
    opspec->kop_txn = NULL;
  }
  rc = hse_kvs_cursor_create(kvs, opspec, filt, filt_len, cursor);
  if (opspec) {
    opspec->kop_txn = kop_txn_sav;
  }
  return rc;
}


/*
 * opspec->kop_txn must be NULL when hse_kvs_cursor_seek() is called (Gaurav)
 */
hse_err_t HseStore::hse_kvs_cursor_seek_wrapper(
    struct hse_kvs_cursor * cursor,
    struct hse_kvdb_opspec *opspec,
    const void *            key,
    size_t                  key_len,
    const void **           found,
    size_t *                found_len)
{

  hse_err_t rc;
  struct hse_kvdb_txn *kop_txn_sav;

  if (opspec) {
    kop_txn_sav = opspec->kop_txn;
    opspec->kop_txn = NULL;
  }
  rc = hse_kvs_cursor_seek(cursor, opspec, key, key_len, found, found_len);
  if (opspec) {
    opspec->kop_txn = kop_txn_sav;
  }
  return rc;
}

/*
 * opspec->kop_txn must be NULL when hse_kvs_cursor_read() is called (Gaurav)
 */
hse_err_t HseStore::hse_kvs_cursor_read_wrapper(
    struct hse_kvs_cursor * cursor,
    struct hse_kvdb_opspec *opspec,
    const void **           key,
    size_t *                key_len,
    const void **           val,
    size_t *                val_len,
    bool *                  eof)
{

  hse_err_t rc;
  struct hse_kvdb_txn *kop_txn_sav;

  if (opspec) {
    kop_txn_sav = opspec->kop_txn;
    opspec->kop_txn = NULL;
  }
  rc = hse_kvs_cursor_read(cursor, opspec, key, key_len, val, val_len, eof);
  if (opspec) {
    opspec->kop_txn = kop_txn_sav;
  }
  return rc;
}
