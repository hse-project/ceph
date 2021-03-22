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
#include <hse/hse.h>
#include <hse/hse_limits.h>
#include <climits>
#include <cstring>

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
#define GENERAL_METADATA_KEY(key) ("G" key)

static constexpr std::string_view CEPH_METADATA_KVS_NAME = "ceph-metadata";
static constexpr std::string_view COLLECTION_KVS_NAME = "collection";
static constexpr std::string_view COLLECTION_OBJECT_KVS_NAME = "collection-object";
static constexpr std::string_view OBJECT_DATA_KVS_NAME = "object-data";
static constexpr std::string_view OBJECT_XATTR_KVS_NAME = "object-xattr";
static constexpr std::string_view OBJECT_OMAP_KVS_NAME = "object-omap";

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
}

void HseStore::Collection::flush()
{
  return;
}

bool HseStore::Collection::flush_commit(Context *ctx)
{
  if ((_t_seq_persisted_latest + 1 == _t_seq_next) || (_t_seq_next == 0)) {
    // All transactions persisted.
    return true;
  }

  // Queue the sync request to the syncer thread
  _syncer->post_sync(this, ctx, _t_seq_next - 1);

  return false;
}

void HseStore::Collection::queue_wait_persist(Context *ctx, uint64_t t_seq) {
	TxnWaitPersist *twp = new TxnWaitPersist(ctx, t_seq);

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

    // Invoke the Ceph "commit" callback.
    c->_store->finisher.queue(twp->_persist_ctx);

    delete twp;

    lock.lock();
    it = c->_committed_wait_persist.begin();
  }
}


//
// Syncer functions
//

// flush_commit() requested a sync
void Syncer::do_sync(HseStore::Collection *c, Context *ctx, uint64_t t_seq_committed_at_flush)
{
  if (c->_t_seq_persisted_latest < t_seq_committed_at_flush) {
    //
    // We have to sync
    //
    HseStore::Collection *c1;

    // Record for each collection, the latest committed before starting the sync.
    boost::shared_lock<ceph::shared_mutex> l{c->_store->coll_lock};
    for (auto const& [key, val] : c->_store->coll_map) {
  		c1 = static_cast<HseStore::Collection*>(val.get());
  		c1->_t_seq_committed_wait_sync = c1->_t_seq_next - 1;
    }
    l.unlock();

    // hse_sync()

    // Record the latest transaction persisted.
    l.lock();
    for (auto const& [key, val] : c->_store->coll_map) {
      c1 = static_cast<HseStore::Collection*>(val.get());
      c1->_t_seq_persisted_latest = c1->_t_seq_committed_wait_sync;

      // Invoke the Ceph "commit" callback for all the transactions that had
      // been committed  and are now persisted.
      HseStore::Collection::committed_wait_persist_cb(c1);
    }
    l.unlock();
  }

  c->_store->finisher.queue(ctx);
}


//
// HseStore functions
//


HseStore::HseStore(CephContext *cct, const std::string& path)
  : ObjectStore(cct, path),
    finisher(cct),
    kvdb_name(cct->_conf->hsestore_kvdb)
{
}

HseStore::~HseStore()
{}

int HseStore::write_meta(const std::string& key, const std::string& value)
{
  hse_err_t rc = 0;
  const size_t encoded_key_size = key.size() + 1;
  auto encoded_key = std::make_unique<uint8_t[]>(key.size() + 1);

  encoded_key[0] = 'G';
  memcpy(encoded_key.get() + 1, key.c_str(), encoded_key_size - 1);
  rc = hse_kvs_put(ceph_metadata_kvs, nullptr, encoded_key.get(), encoded_key_size,
    value.c_str(), value.size());
  if (rc)
    dout(10) << " failed to write metadata (" << key << ", " << value << ')' << dendl;

  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::read_meta(const std::string& key, std::string *value)
{
  hse_err_t rc = 0;
  bool found = false;
  char buf[HSE_KVS_VLEN_MAX];
  const size_t encoded_key_size = key.size() + 1;
  auto encoded_key = std::make_unique<uint8_t[]>(key.size() + 1);

  encoded_key[0] = 'G';
  memcpy(encoded_key.get() + 1, key.c_str(), encoded_key_size - 1);
  rc = hse_kvs_get(ceph_metadata_kvs, nullptr, encoded_key.get(), encoded_key_size,
    &found, buf, sizeof(buf), nullptr);
  if (rc)
    dout(10) << " failed to read metadata (" << key << ')' << dendl;
  if (found)
    *value = std::string(buf);

  return rc ? -hse_err_to_errno(rc) : 0;
}

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
  hse_err_t rc = 0;
  const void *found = nullptr;

  std::string coll_tkey;
  std::string ghobject_tkey;

  coll_t2key(cct, c->cid, &coll_tkey);
  ghobject_t2key(cct, oid, &ghobject_tkey);

  const size_t filt_min_len = coll_tkey.size() + ghobject_tkey.size();
  const size_t filt_max_len = filt_min_len + sizeof(hse_oid_t);

  auto filt_min = std::make_unique<uint8_t[]>(filt_min_len);
  auto filt_max = std::make_unique<uint8_t[]>(filt_max_len);
  memcpy(filt_min.get(), coll_tkey.c_str(), coll_tkey.size());
  memcpy(filt_min.get() + coll_tkey.size(), ghobject_tkey.c_str(), ghobject_tkey.size());
  memcpy(filt_max.get(), filt_min.get(), filt_min_len);
  // guarantees that the last bits of filt_max are UINT64_MAX
  memset(filt_max.get() + filt_min_len, 0xFF, sizeof(hse_oid_t));

  struct hse_kvs_cursor *cursor;
  rc = hse_kvs_cursor_create(collection_object_kvs, nullptr, coll_tkey.c_str(), coll_tkey.size(), &cursor);
  if (rc) {
    dout(10) << " failed to create cursor when check existence of object ("
      << oid << ") with collection (" << c->cid << ')' << dendl;
    goto err_out;
  }

  // if everything is correct, there is only one key in this range
  rc = hse_kvs_cursor_seek_range(cursor, nullptr, filt_min.get(),
    filt_min_len, filt_max.get(), filt_max_len, &found, nullptr);
  if (rc) {
    dout(10) << " failed to check existence of object ("
      << oid << ") in collection (" << c->cid << ')' << dendl;
    goto err_out;
  }

  rc = hse_kvs_cursor_destroy(cursor);
  if (rc) {
    dout(10) << " failed to destroy cursor while checking existence of object ("
      << oid << ") in collection (" << c->cid << ')' << dendl;
    goto err_out;
  }

err_out:
  return found != nullptr;
}

int HseStore::list_collections(vector<coll_t>& ls)
{
  boost::shared_lock<ceph::shared_mutex> l{coll_lock};

  for (auto const& [key, val] : coll_map)
    ls.push_back(key);

  return 0;
}

bool HseStore::collection_exists(const coll_t& c)
{
  boost::shared_lock<ceph::shared_mutex> l{coll_lock};
  return coll_map.count(c);
}

HseStore::CollectionRef HseStore::get_collection(coll_t cid)
{
  boost::shared_lock<ceph::shared_mutex> l{coll_lock};
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}

int HseStore::collection_empty(CollectionHandle &c, bool *empty)
{
  hse_err_t rc = 0;

  std::string coll_tkey;
  coll_t2key(cct, c->cid, &coll_tkey);

  struct hse_kvs_cursor *cursor;
  rc = hse_kvs_cursor_create(collection_object_kvs, nullptr, coll_tkey.c_str(),
    coll_tkey.size(), &cursor);
  if (rc) {
    dout(10) << " failed to create cursor while checking if collection ("
      << c->cid << ") is empty" << dendl;
    goto err_out;
  }

  rc = hse_kvs_cursor_read(cursor, nullptr, nullptr, nullptr, nullptr, nullptr,
    empty);
  if (rc) {
    dout(10) << " failed to read from cursor while checking if collection ("
      << c->cid << ") is empty" << dendl;
    goto destroy_cursor;
  }

destroy_cursor:
  rc = hse_kvs_cursor_destroy(cursor);
  if (rc) {
    dout(10) << " failed to destroy cursor while checking if collection ("
      << c->cid << ") is empty" << dendl;
    goto err_out;
  }

err_out:
  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::remove_collection(coll_t cid, CollectionRef *c)
{
  hse_err_t rc = 0;
  std::string filt;

  std::unique_lock<ceph::shared_mutex> l{coll_lock};

  *c = coll_map[cid];
  coll_map.erase(cid);

  l.unlock();

  struct hse_kvdb_opspec os;
  HSE_KVDB_OPSPEC_INIT(&os);

  struct hse_kvdb_txn *txn = hse_kvdb_txn_alloc(kvdb);
  os.kop_txn = txn;

  rc = hse_kvdb_txn_begin(kvdb, txn);
  if (rc) {
    dout(10) << " failed to begin transaction while removing collection("
      << cid << ')' << dendl;
    goto err_out;
  }

  coll_t2key(cct, cid, &filt);
  rc = hse_kvs_prefix_delete(collection_kvs, &os, filt.c_str(), filt.size(),
    nullptr);
  if (rc) {
    dout(10) << " failed to prefix delete all data pertaining to collection (" <<
      cid << ") in collection kvs" << dendl;
    goto err_out;
  }
  rc = hse_kvs_prefix_delete(collection_object_kvs, &os, filt.c_str(),
    filt.size(), nullptr);
  if (rc) {
    dout(10) << " failed to prefix delete all data pertaining to collection (" <<
      cid << ") in collection-object kvs" << dendl;
    goto err_out;
  }

  rc = hse_kvdb_txn_commit(kvdb, txn);
  if (rc) {
    dout(10) << " failed to commit transaction while removing collection("
      << cid << ')' << dendl;
    goto err_out;
  }

err_out:
  hse_kvdb_txn_free(kvdb, txn);

  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::create_collection(coll_t cid, unsigned bits, CollectionRef *c)
{
  *c = ceph::make_ref<Collection>(this, cid);

  std::unique_lock<ceph::shared_mutex> l{coll_lock};
  new_coll_map[cid] = *c;

  return 0;
}

int HseStore::split_collection(CollectionRef& c, CollectionRef& d, unsigned bits, int rem)
{
  hse_err_t rc = 0;
  struct hse_kvs_cursor *cursor;
  bool eof = false;

  struct hse_kvdb_opspec os;
  HSE_KVDB_OPSPEC_INIT(&os);

  struct hse_kvdb_txn *txn = hse_kvdb_txn_alloc(kvdb);
  os.kop_txn = txn;

  const coll_t old_cid = c->cid;
  const coll_t new_cid = d->cid;

  std::string oldcid_tkey;
  coll_t2key(cct, old_cid, &oldcid_tkey);
  std::string newcid_tkey;
  coll_t2key(cct, new_cid, &newcid_tkey);

  rc = hse_kvs_cursor_create(collection_object_kvs, nullptr, oldcid_tkey.c_str(),
    oldcid_tkey.size(), &cursor);
  if (rc) {
    dout(10) << " failed to create cursor while splitting collection ("
      << old_cid << "->" << new_cid << ')' << dendl;
    goto err_out;
  }

  rc = hse_kvdb_txn_begin(kvdb, txn);
  if (rc) {
    dout(10) << " failed to begin transaction while splitting collection ("
      << old_cid << "->" << new_cid << ')' << dendl;
    goto err_out;
  }

  while (!eof) {
    const uint8_t *key;
    size_t key_len;
    rc = hse_kvs_cursor_read(cursor, nullptr, reinterpret_cast<const void **>(&key),
      &key_len, nullptr, nullptr, &eof);
    if (rc) {
      dout(10) << " failed to read from cursor while splitting collections ("
        << old_cid << "->" << new_cid << ')' << dendl;
      goto err_out;
    }

    std::string_view ghobject_tkey {
      reinterpret_cast<const char *>(key + ENCODED_KEY_COLL),
      key_len - ENCODED_KEY_COLL - sizeof(hse_oid_t) };
    ghobject_t obj;
    if (const int err = key2ghobject_t(ghobject_tkey, obj)) {
      dout(10) << " failed to convert key (" << ghobject_tkey << ") to ghobject_t: " << err << dendl;
      goto err_out;
    }

    // if not a match don't put/delete; this check was stolen from MemStore
    if (!obj.match(bits, rem))
      continue;

    auto new_key = std::make_unique<uint8_t[]>(key_len);
    memcpy(new_key.get(), newcid_tkey.c_str(), newcid_tkey.size());
    memcpy(new_key.get() + newcid_tkey.size(), key + newcid_tkey.size(),
      key_len - newcid_tkey.size());

    rc = hse_kvs_put(collection_object_kvs, &os, static_cast<void *>(new_key.get()),
      key_len, nullptr, 0);
    if (rc) {
      // ignoring rc from abort since we don't want to mask original error
      hse_kvdb_txn_abort(kvdb, txn);
      dout(10) << " failed to read from cursor while merging collections ("
        << old_cid << "->" << new_cid << ')' << dendl;
      goto err_out;
    }

    rc = hse_kvs_delete(collection_object_kvs, &os, key, key_len);
    if (rc) {
      hse_kvdb_txn_abort(kvdb, txn);
      dout(10) << " failed to data from collection while merging collections ("
        << old_cid << "->" << new_cid << ')' << dendl;
      goto err_out;
    }
  }

  rc = hse_kvdb_txn_commit(kvdb, txn);
  if (rc) {
    dout(10) << " failed to commit transaction while merging collections ("
      << old_cid << "->" << new_cid << ')' << dendl;
    goto err_out;
  }

  rc = hse_kvs_cursor_destroy(cursor);
  if (rc) {
    dout(10) << " failed to destroy cursor while merging collections ("
      << old_cid << "->" << new_cid << ')' << dendl;
    goto err_out;
  }

err_out:
  hse_kvdb_txn_free(kvdb, txn);

  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::merge_collection(CollectionRef *c, CollectionRef& d, unsigned bits)
{
  // HSE_TODO: what to do with bits?
  hse_err_t rc = 0;
  struct hse_kvs_cursor *cursor;
  bool eof = false;

  struct hse_kvdb_opspec os;
  HSE_KVDB_OPSPEC_INIT(&os);

  // all puts/deletes will use the same opspec
  struct hse_kvdb_txn *txn = hse_kvdb_txn_alloc(kvdb);
  os.kop_txn = txn;

  const coll_t old_cid = (*c)->cid;
  const coll_t new_cid = d->cid;

  std::string old_cid_tkey;
  coll_t2key(cct, old_cid, &old_cid_tkey);
  std::string new_cid_tkey;
  coll_t2key(cct, new_cid, &new_cid_tkey);

  rc = hse_kvs_cursor_create(collection_object_kvs, nullptr, old_cid_tkey.c_str(),
    old_cid_tkey.size(), &cursor);
  if (rc) {
    dout(10) << " failed to create cursor while merging collections (" <<
      old_cid << "->" << new_cid << ')' << dendl;
    goto err_out;
  }

  rc = hse_kvdb_txn_begin(kvdb, txn);
  if (rc) {
    dout(10) << " failed to begin transaction while merging collections ("
      << old_cid << "->" << new_cid << ')' << dendl;
    goto err_out;
  }

  while (!eof) {
    const uint8_t *key;
    size_t key_len;
    rc = hse_kvs_cursor_read(cursor, nullptr, reinterpret_cast<const void **>(&key),
      &key_len, nullptr, nullptr, &eof);
    if (rc) {
      dout(10) << " failed to read from cursor while merging collections ("
        << old_cid << "->" << new_cid << ')' << dendl;
      goto err_out;
    }

    auto new_key = std::make_unique<uint8_t[]>(key_len);
    memcpy(new_key.get(), new_cid_tkey.c_str(), new_cid_tkey.size());
    memcpy(new_key.get() + ENCODED_KEY_COLL, key + ENCODED_KEY_COLL, key_len - ENCODED_KEY_COLL);

    rc = hse_kvs_put(collection_object_kvs, &os, static_cast<void *>(new_key.get()), key_len, nullptr, 0);
    if (rc) {
      // ignoring rc from abort since we don't want to mask original error
      hse_kvdb_txn_abort(kvdb, txn);
      dout(10) << " failed to put while merging collections ("
        << old_cid << "->" << new_cid << ')' << dendl;
      goto err_out;
    }
  }

  rc = hse_kvs_prefix_delete(collection_object_kvs, &os, old_cid_tkey.c_str(),
    old_cid_tkey.size(), nullptr);
  if (rc) {
    hse_kvdb_txn_abort(kvdb, txn);
    dout(10) << " failed to prefix delete data from collection while merging collections ("
      << old_cid << "->" << new_cid << ')' << dendl;
    goto err_out;
  }

  rc = hse_kvdb_txn_commit(kvdb, txn);
  if (rc) {
    dout(10) << " failed to commit transaction while merging collections ("
      << old_cid << "->" << new_cid << ')' << dendl;
    goto err_out;
  }

  rc = hse_kvs_cursor_destroy(cursor);
  if (rc) {
    dout(10) << " failed to destroy cursor while merging collections ("
      << old_cid << "->" << new_cid << ')' << dendl;
    goto err_out;
  }

err_out:
  hse_kvdb_txn_free(kvdb, txn);

  return rc ? -hse_err_to_errno(rc) : 0;
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
    c->queue_wait_persist(t.get_on_commit(), c->_t_seq_next++);
  }

  // The WaitCondTs destructor (variable ts) will wakeup the next queue_transactions()
  // thread waiting to run on the collection.
  return 0;
}

int HseStore::mount()
{
  hse_err_t rc = 0;
  bool eof = false;
  bool fsid_found = false;
  static const char fsid_key[] = GENERAL_METADATA_KEY("fsid");
  char fsid_buf[fsid.uuid.size()];
  std::unique_lock<ceph::shared_mutex> l{coll_lock};

  rc = hse_kvdb_init();
  if (rc) {
    goto err_out;
  }

  rc = hse_kvdb_open(kvdb_name.data(), nullptr, &kvdb);
  if (rc) {
    dout(10) << " failed to open the kvdb" << dendl;
    goto err_out;
  }

  /* HSE_TODO: how to handle error logic here */
  rc = hse_kvdb_kvs_open(kvdb, CEPH_METADATA_KVS_NAME.data(), nullptr, &ceph_metadata_kvs);
  if (rc) {
    dout(10) << " failed to open the ceph-metadata kvs" << dendl;
    goto err_out;
  }

  rc = hse_kvdb_kvs_open(kvdb, COLLECTION_KVS_NAME.data(), nullptr, &collection_kvs);
  if (rc) {
    dout(10) << " failed to open the collection kvs" << dendl;
    goto err_out;
  }

  rc = hse_kvdb_kvs_open(kvdb, COLLECTION_OBJECT_KVS_NAME.data(), nullptr, &collection_object_kvs);
  if (rc) {
    dout(10) << " failed to open the collection-object kvs" << dendl;
    goto err_out;
  }

  rc = hse_kvdb_kvs_open(kvdb, OBJECT_DATA_KVS_NAME.data(), nullptr, &object_data_kvs);
  if (rc) {
    dout(10) << " failed to open the object-data kvs" << dendl;
    goto err_out;
  }

  rc = hse_kvdb_kvs_open(kvdb, OBJECT_XATTR_KVS_NAME.data(), nullptr, &object_xattr_kvs);
  if (rc) {
    dout(10) << " failed to open the object-xattr kvs" << dendl;
    goto err_out;
  }

  rc = hse_kvdb_kvs_open(kvdb, OBJECT_OMAP_KVS_NAME.data(), nullptr, &object_omap_kvs);
  if (rc) {
    dout(10) << " failed to open the object-omap kvs" << dendl;
    goto err_out;
  }

  struct hse_kvs_cursor *cursor;
  rc = hse_kvs_cursor_create(collection_kvs, nullptr, nullptr, 0, &cursor);
  if (rc) {
    dout(10) << " failed to create a cursor for populating the in-memory collection map" << dendl;
    goto err_out;
  }

  const uint8_t *key;
  size_t key_len;
  while (!eof) {
    rc = hse_kvs_cursor_read(cursor, NULL, reinterpret_cast<const void **>(&key),
      &key_len, nullptr, 0, &eof);
    if (rc) {
      dout(10) << " failed to read cursor for populating initial coll_map" << dendl;
      goto err_out;
    }

    std::string_view coll_tkey { reinterpret_cast<const char *>(key), ENCODED_KEY_COLL };
    coll_t cid;
    if (const int err = key2coll_t(coll_tkey, cid)) {
      dout(10) << " failed to convert key (" << coll_tkey << ") to coll_t: " << err << dendl;
      goto err_out;
    }

    coll_map[cid] = ceph::make_ref<Collection>(this, std::move(cid));
  }

  rc = hse_kvs_cursor_destroy(cursor);
  if (rc) {
    dout(10) << " failed to destroy cursor while populating initial coll_map" << dendl;
    goto err_out;
  }

  // -1 removed the NUL byte
  rc = hse_kvs_get(ceph_metadata_kvs, nullptr, fsid_key, sizeof(fsid_key) - 1,
    &fsid_found, fsid_buf, sizeof(fsid_buf), nullptr);
  if (rc) {
    dout(10) << " failed to get fsid" << dendl;
    goto err_out;
  }
  if (fsid_found && !fsid.parse(fsid_buf)) {
    dout(10) << " failed to parse fsid" << dendl;
    rc = ENOTRECOVERABLE;
    goto err_out;
  }

err_out:
  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::umount()
{
  hse_err_t rc = 0;
  std::unique_lock<ceph::shared_mutex> l{coll_lock};

  /* HSE_TODO: how to handle error logic here */
  rc = hse_kvdb_kvs_close(ceph_metadata_kvs);
  if (rc) {
    dout(10) << " failed to close the ceph-metadata kvs" << dendl;
    goto err_out;
  }
  ceph_metadata_kvs = nullptr;

  rc = hse_kvdb_kvs_close(collection_kvs);
  if (rc) {
    dout(10) << " failed to close the collection kvs" << dendl;
    goto err_out;
  }
  collection_kvs = nullptr;

  rc = hse_kvdb_kvs_close(collection_object_kvs);
  if (rc) {
    dout(10) << " failed to close the collection-object kvs" << dendl;
    goto err_out;
  }
  collection_object_kvs = nullptr;

  rc = hse_kvdb_kvs_close(object_data_kvs);
  if (rc) {
    dout(10) << " failed to close the object-data kvs" << dendl;
    goto err_out;
  }
  object_data_kvs = nullptr;

  rc = hse_kvdb_kvs_close(object_xattr_kvs);
  if (rc) {
    dout(10) << " failed to close the object-xattr kvs" << dendl;
    goto err_out;
  }
  object_xattr_kvs = nullptr;

  rc = hse_kvdb_kvs_close(object_omap_kvs);
  if (rc) {
    dout(10) << " failed to close the object-omap kvs" << dendl;
    goto err_out;
  }
  object_omap_kvs = nullptr;

  rc = hse_kvdb_close(kvdb);
  if (rc) {
    dout(10) << " failed to close the kvdb" << dendl;
    goto err_out;
  }
  kvdb = nullptr;

  new_coll_map.clear();
  coll_map.clear();

err_out:
  hse_kvdb_fini();

  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::mkfs()
{
  hse_err_t rc = 0;

  rc = hse_kvdb_init();
  if (rc) {
    goto err_out;
  }

  /* HSE_TODO: how to handle error logic here */
  rc = hse_kvdb_make(kvdb_name.data(), nullptr);
  if (rc) {
    dout(10) << " failed to make the kvdb" << dendl;
    goto err_out;
  }

  rc = hse_kvdb_open(kvdb_name.data(), nullptr, &kvdb);
  if (rc) {
    dout(10) << " failed to open the kvdb" << dendl;
    goto err_out;
  }

  rc = hse_kvdb_kvs_make(kvdb, CEPH_METADATA_KVS_NAME.data(), nullptr);
  if (rc) {
    dout(10) << " failed to make the ceph-metadata kvs" << dendl;
    goto kvdb_out;
  }

  rc = hse_kvdb_kvs_make(kvdb, COLLECTION_KVS_NAME.data(), nullptr);
  if (rc) {
    dout(10) << " failed to make the collection kvs" << dendl;
    goto kvdb_out;
  }

  rc = hse_kvdb_kvs_make(kvdb, COLLECTION_OBJECT_KVS_NAME.data(), nullptr);
  if (rc) {
    dout(10) << " failed to make the collection-object kvs" << dendl;
    goto kvdb_out;
  }

  rc = hse_kvdb_kvs_make(kvdb, OBJECT_DATA_KVS_NAME.data(), nullptr);
  if (rc) {
    dout(10) << " failed to make the object-data kvs" << dendl;
    goto kvdb_out;
  }

  rc = hse_kvdb_kvs_make(kvdb, OBJECT_XATTR_KVS_NAME.data(), nullptr);
  if (rc) {
    dout(10) << " failed to make the object-xattr kvs" << dendl;
    goto kvdb_out;
  }

  rc = hse_kvdb_kvs_make(kvdb, OBJECT_OMAP_KVS_NAME.data(), nullptr);
  if (rc) {
    dout(10) << " failed to make the object-omap kvs" << dendl;
    goto kvdb_out;
  }

kvdb_out:
  rc = hse_kvdb_close(kvdb);
err_out:
  hse_kvdb_fini();

  return rc ? -hse_err_to_errno(rc) : 0;
}

void HseStore::set_fsid(uuid_d u)
{
  hse_err_t rc = 0;

  static const char fsid_key[] = GENERAL_METADATA_KEY("fsid");

  // -1 removes NUL byte
  rc = hse_kvs_put(ceph_metadata_kvs, nullptr, fsid_key, sizeof(fsid_key) - 1,
    u.bytes(), u.uuid.size());

  fsid = u;

  if (rc) {
    char buf[256];
    hse_err_to_string(rc, buf, sizeof(buf), nullptr);
    ceph_abort_msgf("Failed to put fsid into the %s KVS: %s", CEPH_METADATA_KVS_NAME, buf);
  }
}

// Process (till hse_kvdb_txn_commit() returns) one transaction
void HseStore::start_one_transaction(Collection *c, Transaction *t)
{
  Transaction::iterator i = t->begin();

  /*
  dout(30) << __func__ << " transaction dump:\n";
  JSONFormatter f(true);
  f.open_object_section("transaction");
  t->dump(&f);
  f.close_section();
  f.flush(*_dout);
  *_dout << dendl;
  */

  // Start a hse transaction.

  std::vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (auto c : i.colls) {
    cvec[j++] = get_collection(c);
  }

  for (int pos = 0; i.have_op(); ++pos) {
    const Transaction::Op *op = i.decode_op();
    int r = 0;

    // no coll or obj
    if (op->op == Transaction::OP_NOP)
      continue;

    // collection operations
    CollectionRef &c = cvec[op->cid];
    switch (op->op) {
    case Transaction::OP_RMCOLL:
      {
        const coll_t &cid = i.get_cid(op->cid);
        r = remove_collection(cid, &c);
        if (!r)
          continue;
      }
      break;

    case Transaction::OP_MKCOLL:
      {
        ceph_assert(!c);
        const coll_t &cid = i.get_cid(op->cid);
        r = create_collection(cid, op->split_bits, &c);
        if (!r)
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
        r = split_collection(c, cvec[op->dest_cid], bits, rem);
        if (!r)
          continue;
      }
      break;

    case Transaction::OP_MERGE_COLLECTION:
      {
        const uint32_t bits = op->split_bits;
	      r = merge_collection(&c, cvec[op->dest_cid], bits);
        if (!r)
          continue;
      }
      break;

    case Transaction::OP_COLL_HINT:
      {
  	    ceph_abort_msg("not supported");
      }
      break;

    case Transaction::OP_COLL_SETATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RMATTR:
      r = -EOPNOTSUPP;
      break;

    case Transaction::OP_COLL_RENAME:
      ceph_abort_msg("not implemented");
      break;
    }

    if (r < 0) {
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

#if 0
    // object operations
    OnodeRef &o = ovec[op->oid];
    if (!o) {
      // these operations implicity create the object
      bool create = false;
      if (op->op == Transaction::OP_TOUCH ||
	  op->op == Transaction::OP_CREATE ||
	  op->op == Transaction::OP_WRITE ||
	  op->op == Transaction::OP_ZERO) {
	create = true;
      }
      ghobject_t oid = i.get_oid(op->oid);
      o = c->get_onode(oid, create);
      if (!create) {
	if (!o || !o->exists) {
	  dout(10) << __func__ << " op " << op->op << " got ENOENT on "
		   << oid << dendl;
	  r = -ENOENT;
	  goto endop;
	}
      }
    }

    switch (op->op) {
    case Transaction::OP_TOUCH:
    case Transaction::OP_CREATE:
	r = _touch(txc, c, o);
      break;

    case Transaction::OP_WRITE:
      {
        uint64_t off = op->off;
        uint64_t len = op->len;
	uint32_t fadvise_flags = i.get_fadvise_flags();
        bufferlist bl;
        i.decode_bl(bl);
	r = _write(txc, c, o, off, len, bl, fadvise_flags);
      }
      break;

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

    case Transaction::OP_REMOVE:
	r = _remove(txc, c, o);
      break;

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
    case Transaction::OP_OMAP_SETKEYS:
      {
	bufferlist aset_bl;
        i.decode_attrset_bl(&aset_bl);
	r = _omap_setkeys(txc, c, o, aset_bl);
      }
      break;
    case Transaction::OP_OMAP_RMKEYS:
      {
	bufferlist keys_bl;
        i.decode_keyset_bl(&keys_bl);
	r = _omap_rmkeys(txc, c, o, keys_bl);
      }
      break;
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

    default:
      derr << "bad op " << op->op << dendl;
      ceph_abort();
    }

  endop:
    if (r < 0) {
      bool ok = false;

      if (r == -ENOENT && !(op->op == Transaction::OP_CLONERANGE ||
			    op->op == Transaction::OP_CLONE ||
			    op->op == Transaction::OP_CLONERANGE2 ||
			    op->op == Transaction::OP_COLL_ADD))
	// -ENOENT is usually okay
	ok = true;
      if (r == -ENODATA)
	ok = true;

      if (!ok) {
	const char *msg = "unexpected error code";

	if (r == -ENOENT && (op->op == Transaction::OP_CLONERANGE ||
			     op->op == Transaction::OP_CLONE ||
			     op->op == Transaction::OP_CLONERANGE2))
	  msg = "ENOENT on clone suggests osd bug";

	if (r == -ENOSPC)
	  // For now, if we hit _any_ ENOSPC, crash, before we do any damage
	  // by partially applying transactions.
	  msg = "ENOSPC from key value store, misconfigured cluster";

	if (r == -ENOTEMPTY) {
	  msg = "ENOTEMPTY suggests garbage data in osd data dir";
	}

	dout(0) << " error " << cpp_strerror(r) << " not handled on operation " << op->op
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
#endif
  }

  //
  // Commit the transaction in hse
  //

  // hse_kvdb_txn_commit()
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
       out->append((char *)buff, (char *)(ptr-buff));
       ptr = &buff[0];
    }
  }
  if (ptr != buff) {
     out->append((char *)buff, (char *)(ptr-buff));
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
 * Convert a coll_t into a 10 bytes "key"
 * The order on coll_t is defined by coll_t::operator<
 * The first byte of the output, letter P, T or M  maintain that order.
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
    key->append("M123456789"); // TYPE_META
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
