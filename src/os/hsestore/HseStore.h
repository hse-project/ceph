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

#ifndef CEPH_HSESTORE_H
#define CEPH_HSESTORE_H

#include <cerrno>
#include <cstdint>
#include <string>
#include <map>
#include <set>
#include <vector>
#include <optional>
#include <hse/hse.h>
#include <hse/hse_limits.h>
#include <string_view>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/functional/hash.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/circular_buffer.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>

#include "common/TrackedOp.h"
#include "common/Finisher.h"
#include "osd/osd_types.h"
#include "os/ObjectStore.h"
#include "os/ObjectMap.h"
#include "os/Transaction.h"
#include "include/uuid.h"


class WaitCond {
  boost::condition_variable   _done_cond;
  boost::mutex                _done_mutex;
  uint64_t                    _val;
  bool                        _done = false;

public:
  void wait()
  {
    boost::unique_lock<boost::mutex> lock(_done_mutex);

    while (!_done)
    {
      _done_cond.wait(lock);
    }
  }
  void wakeup()
  {
    boost::unique_lock<boost::mutex> lock(_done_mutex);

    _done = true;
    _done_cond.notify_one();
  }
  uint64_t get_val()
  {
    return _val;
  }

  WaitCond() : _val(0) {}
  WaitCond(uint64_t val) : _val(val) {}

};

class TransactionSerialization;
class WaitCondTs : WaitCond {
  TransactionSerialization *_ts;

public:
  WaitCondTs(TransactionSerialization *ts);
  ~WaitCondTs();
};

//
// Serialize transaction processing for a same collection.
// It is achieved by serializing the threads calling queue_transactions() on a same
// collection.
// These threads run one by one in the order they entered queue_transactions().
// There is one instance of this class per collection.
//
class TransactionSerialization {

  // Mutex to protect members below.
  boost::mutex _ts_mutex;

  //
  // List of threads waiting in (queue_transactions()) for their turn to
  // process transactions on the collection.
  //
  list <WaitCondTs *> _ts_th_waiting;

  // True is a thread is processing transaction[s] on this collection.
  bool _ts_th_running = 0;

  friend class WaitCondTs;
};


class TxnWaitPersist {
  // Transaction callback that must be called when the transaction is persisted.
  // Ceph calls it the "commit" callback.
  Context *_persist_ctx;
  //
  // Sequence number of the transaction.
  uint64_t _t_seq;

  TxnWaitPersist(Context *ctx, uint64_t t_seq);

  friend class HseStore;
};

class Syncer;

class HseStore : public ObjectStore {
  using hse_oid_t = uint64_t;
  Syncer *_syncer;

  class Collection : public ObjectStore::CollectionImpl {
    HseStore *_store;
    Syncer *_syncer;
    TransactionSerialization _ts;

    // The transactions are assigned a sequence number when they are queued.
    // There is one/distinct sequence per collection.
    // This sequence number increments each time a transaction is queued.
    // Because transactions are serialized per collection, "latest" sequence
    // number also mean "highest" sequence number.

    // sqn for next txn to be queued.
    uint64_t _t_seq_next;

    // txns up to _t_seq_committed_sync are waiting to be synced/persisted
    // When a sync starts, the sync thread copies _t_seq_committed_latest into
    // _t_seq_committed_wait_sync
    uint64_t _t_seq_committed_wait_sync;

    // txns up to _t_seq_persisted_latest have been synced/persisted
    uint64_t _t_seq_persisted_latest;

    void flush() override;
    bool flush_commit(Context *c) override;

    //
    // List of transactions committed and waiting to be persisted.
    //
    std::mutex _committed_wait_persist_mtx; // protect _committed_wait_persist
    std::list<TxnWaitPersist *> _committed_wait_persist;

    // Add a committed transaction (hse_kvdb_txn_commit() returned) in the list of
    // transaction waiting to be persisted.
    void queue_wait_persist(Context *ctx, uint64_t t_seq);

    // Call the Ceph "commit" callback that needs to be called when a transaction is
    // persisted. Does that for all transaction that were hse_kvdb_txn_commit()
    // and have been persisted.
    static void committed_wait_persist_cb(HseStore::Collection *c);

    public:
    // Constructor
    Collection(HseStore *store, coll_t cid);

    friend class Syncer;
    friend class HseStore;
  };
  using CollectionRef = ceph::ref_t<Collection>;

  //
  // Collections
  //

  // rwlock to protect lists of collections (coll_map, new_coll_map).
  // Use std::shared_lock l{coll_lock} to take the lock in read
  // Use std::unique_lock l{coll_lock} to take the lock in write
  ceph::shared_mutex coll_lock = ceph::make_shared_mutex("HseStore::coll_lock");

  ceph::unordered_map<coll_t, CollectionRef> coll_map;
  std::map<coll_t, CollectionRef> new_coll_map;

  //
  // Ceph Finisher worker thread used to call the Ceph callbacks (Context).
  //
  Finisher finisher;

  void start_one_transaction(Collection *c, Transaction *t);
  int remove_collection(coll_t cid, CollectionRef *c);
  int create_collection(coll_t cid, unsigned bits, CollectionRef *c);
  int split_collection(CollectionRef& c, CollectionRef& d, unsigned bits, int rem);
  int merge_collection(CollectionRef *c, CollectionRef& d, unsigned bits);


public:
  HseStore(CephContext *cct, const std::string &path);
  ~HseStore() override;

  std::string get_type() override {
    return "hsestore";
  }

  objectstore_perf_stat_t get_cur_stats() override {
    return {};
  }

  const PerfCounters* get_perf_counters() const override {
    return nullptr;
  }

  int queue_transactions(CollectionHandle &c, std::vector<Transaction> &tls,
      TrackedOpRef op = TrackedOpRef(),
      ThreadPool::TPHandle *handle = NULL) override;

  bool test_mount_in_use() override {
    return kvdb != nullptr;
  }
  int mount() override;
  int umount() override;

  int validate_hobject_key(const hobject_t &obj) const override {
    return 0;
  }
  unsigned get_max_attr_name_length() override {
    // HSE_TODO: random
    return 256;
  }

  int mkfs() override;

  int mkjournal() override {
    return 0;
  }
  bool wants_journal() override {
    return false;
  }
  bool allows_journal() override {
    return false;
  }
  bool needs_journal() override {
    return false;
  }
  bool is_journal_rotational() override {
    return false;
  }

  int statfs(struct store_statfs_t *buf, osd_alert_list_t* alerts = nullptr) override {
    return -EOPNOTSUPP;
  }
  int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf, bool *per_pool_omap) override {
    return -EOPNOTSUPP;
  }

  CollectionHandle open_collection(const coll_t &cid) override;

  CollectionHandle create_new_collection(const coll_t &cid) override;

  void set_collection_commit_queue(const coll_t &cid, ContextQueue *commit_queue) override {}

  bool exists(CollectionHandle &c, const ghobject_t &oid) override;

  int set_collection_opts(CollectionHandle& c, const pool_opts_t &opts) override {
    return -EOPNOTSUPP;
  }

  int stat(CollectionHandle &c, const ghobject_t &oid, struct stat *st, bool allow_eio = false) override {
    return -EOPNOTSUPP;
  }

  int read(
    CollectionHandle &c,
    const ghobject_t &oid,
    uint64_t offset,
    size_t len,
    ceph::buffer::list &bl,
    uint32_t op_flags = 0
  ) override {
    return -EOPNOTSUPP;
  }

  int fiemap(CollectionHandle &c, const ghobject_t &oid,
      uint64_t offset, size_t len, ceph::buffer::list &bl) override {
    return -EOPNOTSUPP;
  }
  int fiemap(CollectionHandle &c, const ghobject_t &oid,
      uint64_t offset, size_t len, std::map<uint64_t, uint64_t> &destmap) override {
    return -EOPNOTSUPP;
  }

  int getattr(CollectionHandle &c, const ghobject_t& oid,
    const char *name, ceph::buffer::ptr& value) override {
    return -EOPNOTSUPP;
  }

  int getattrs(CollectionHandle &c, const ghobject_t &oid,
      std::map<std::string, ceph::buffer::ptr> &aset) override {
    return -EOPNOTSUPP;
  }

  int list_collections(vector<coll_t>& ls) override;

  bool collection_exists(const coll_t& c) override;

  int collection_empty(CollectionHandle &c, bool *empty) override;

  int collection_bits(CollectionHandle &c) override {
    return -EOPNOTSUPP;
  }

  int collection_list(CollectionHandle &c, const ghobject_t &start,
    const ghobject_t &end, int max, std::vector<ghobject_t> *ls, ghobject_t *next) override {
    return -EOPNOTSUPP;
  }

  int omap_get(CollectionHandle &c, const ghobject_t &oid, ceph::buffer::list *header,
    std::map<std::string, ceph::buffer::list> *out) override {
    return -EOPNOTSUPP;
  }

  int omap_get_header(CollectionHandle &c, const ghobject_t &oid,
    ceph::buffer::list *header, bool allow_eio = false) override {
    return -EOPNOTSUPP;
  }

  int omap_get_keys(CollectionHandle &c, const ghobject_t &oid,
    std::set<std::string> *keys) override {
    return -EOPNOTSUPP;
  }

  int omap_get_values(CollectionHandle &c, const ghobject_t &oid,
    const std::set<std::string> &keys,
    std::map<std::string, ceph::buffer::list> *out) override {
    return -EOPNOTSUPP;
  }

#ifdef WITH_SEASTAR
  int omap_get_values(CollectionHandle &c, const ghobject_t &oid,
      const std::optional<std::string> &start_after,
      std::map<std::string, ceph::buffer::list> *out) override {
    return -EOPNOTSUPP;
  }
#endif

  int omap_check_keys(CollectionHandle &c, const ghobject_t &oid,
      const std::set<std::string> &keys, std::set<std::string> *out) override {
    return -EOPNOTSUPP;
  }

  ObjectMap::ObjectMapIterator get_omap_iterator(
      CollectionHandle &c, const ghobject_t &oid) override {
    return {};
  }

  void set_fsid(uuid_d u) override;
  uuid_d get_fsid() override {
    return fsid;
  }

  uint64_t estimate_objects_overhead(uint64_t num_objects) {
    return 64;
  }
private:
  std::string_view kvdb_name;
  uuid_d fsid;

  struct hse_kvdb *kvdb;
  struct hse_kvs *ceph_metadata_kvs;
  struct hse_kvs *collection_kvs;
  struct hse_kvs *collection_object_kvs;
  struct hse_kvs *object_data_kvs;
  struct hse_kvs *object_xattr_kvs;
  struct hse_kvs *object_omap_kvs;

  HseStore::CollectionRef get_collection(coll_t cid);

  friend class Syncer;
};

//
// Handle syncing transactions. Aka making mutation done via transactions persistent.
//
class Syncer {
  HseStore *_store;

  // Work queue used by the syncer, contains sync requests (flush_commit())
  boost::asio::io_service _sync_wq;

  // Thread group for the thread running the _sync_wq
  boost::thread_group _worker_threads;

  // Function called when flush_commit() is called
  static void do_sync(HseStore::Collection *c, Context *ctx,
    uint64_t t_seq_committed_at_flush);


public:
  void post_sync(HseStore::Collection *c, Context *ctx,
    uint64_t t_seq_committed_latest) {
    _sync_wq.post(boost::bind(do_sync, c, ctx, t_seq_committed_latest));

  }

  // Constructor
  Syncer()
  {
    new boost::asio::io_service::work(_sync_wq);
    _worker_threads.create_thread(boost::bind(&boost::asio::io_service::run, &_sync_wq));
  }

  // Destructor
  ~Syncer()
  {
    _sync_wq.stop();
    _worker_threads.join_all();
  }
};

#endif
