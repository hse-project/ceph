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
#include <string_view>

extern "C" {
  #include <hse/hse.h>
  #include <hse/hse_limits.h>
}

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


// Data block in KVS object_data_kvs is 2^DATA_BLOCK_SHIFT
#define DATA_BLOCK_SHIFT 12

#define DATA_BLOCK_LEN (1ULL << DATA_BLOCK_SHIFT)

// Maximum object data block number or omap value block that HseStore can handle.
#define MAX_BLOCK_NB ((1ULL << 32) -1) // INT_MAX
// Maximum amount of data in one object, in bytes.
#define MAX_DATA_LEN ((MAX_BLOCK_NB + 1)*DATA_BLOCK_LEN)

#define OBJECT_DATA_KEY_LEN (sizeof(hse_oid_t) + sizeof(uint32_t))

#define OMAP_BLOCK_SHIFT 20
#define OMAP_BLOCK_LEN (1ULL << OMAP_BLOCK_SHIFT) // 1MiB
// Size of the omap block number at the end of the key used in the kvs object_omap
#define OMAP_BLOCK_NB_LEN (sizeof(uint32_t))



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
  public:
  using hse_oid_t = uint64_t;

  private:
  bool _ready_for_sync = false;
  std::unique_ptr<Syncer> _syncer;
  //
  // Next hse_oid to assign to a new Ceph object.
  std::atomic<uint64_t> _next_hse_oid;

  // Information about one object referenced by a Ceph transaction.
  // The lifetime of an Onode is the same as its transaction.
  // The goal of an Onode is to avoid doing the same processing several times in a transaction.
  // For example:
  // - recomputing ghobject_t2key for each operation of the transaction that uses a same object.
  // - redoing the lookup ghobject_t2key -> hse_oid_t for each operation of the transaction
  //   that uses a same object.
  //
  struct Onode {
    // True if get_onode() has been called for this node.
    bool o_gotten;

    const ghobject_t *o_oid;
    std::string o_ghobject_tkey;

    // True if the object is KVS collection_object_kvs
    bool o_exists;
    hse_oid_t o_hse_oid;
    uint64_t o_data_len; // Total length of the object data (from offset 0 to last byte).

    // Object not yet in KVS collection_object_kvs, but the operation will create the object and
    // put it in the KVS.
    // It is a new object.
    bool o_dirty;

    // Default constructor
    Onode() : o_gotten(false), o_oid(nullptr), o_exists(false), o_hse_oid(0), o_data_len(0),
      o_dirty(false) {
    }
  };

  class Collection : public ObjectStore::CollectionImpl {
    std::string _coll_tkey;
    HseStore *_store;
    TransactionSerialization _ts;

    // The transactions are assigned a sequence number when they are queued.
    // There is one/distinct sequence per collection.
    // This sequence number increments each time a transaction is queued.
    // Because transactions are serialized per collection, "latest" sequence
    // number also mean "highest" sequence number.

    // sqn for next txn to be queued.
    uint64_t _t_seq_next = 1;

    // txns up to _t_seq_committed_sync are waiting to be synced/persisted
    // When a sync starts, the sync thread copies _t_seq_committed_latest into
    // _t_seq_committed_wait_sync
    uint64_t _t_seq_committed_wait_sync = 0;

    // txns up to _t_seq_persisted_latest have been synced/persisted
    uint64_t _t_seq_persisted_latest = 0;

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

    // Onode is a object for which we got its hse_oid.
    void get_onode(Onode& o, const ghobject_t& oid, bool create);

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


  // To iterate over an object map (omap).
  class OmapIteratorImpl;

  //
  // Ceph Finisher worker thread used to call the Ceph callbacks (Context).
  //
  Finisher finisher;

  void start_one_transaction(Collection *c, Transaction *t);
  hse_err_t remove_collection(struct hse_kvdb_opspec *os, coll_t cid, CollectionRef *c);
  hse_err_t create_collection(struct hse_kvdb_opspec *os, coll_t cid, unsigned bits, CollectionRef *c);
  hse_err_t split_collection(struct hse_kvdb_opspec *os, CollectionRef& c, CollectionRef& d,
    unsigned bits, int rem);
  hse_err_t merge_collection(struct hse_kvdb_opspec *os, CollectionRef *c, CollectionRef& d,
      unsigned bits);

  hse_err_t ghobject_t2hse_oid(const coll_t &cid, const ghobject_t &oid, bool& found,
    hse_oid_t& hse_oid, uint64_t& o_data_len);

  void offset2object_data_key(const hse_oid_t &hse_oid, uint64_t offset, std::string *key);
  void collection_object_hse_key(CollectionRef& c, Onode& o, std::string *key);

  hse_err_t kv_create_collection(struct hse_kvdb_opspec *os, const coll_t &cid, unsigned bits);

  hse_err_t kv_write_data(struct hse_kvdb_opspec *os, CollectionRef& c, Onode& o,
    uint64_t offset, size_t length, bufferlist& bl);
  hse_err_t write(struct hse_kvdb_opspec *os, CollectionRef& c, Onode& o, uint64_t offset,
    size_t length, bufferlist& bl, uint32_t fadvise_flags);
  hse_err_t kv_read_data(struct hse_kvdb_opspec *os, Onode& o, uint64_t offset, size_t length,
    bufferlist& bl);

  hse_err_t kv_create_obj(struct hse_kvdb_opspec *os, CollectionRef& c, Onode& o);
  hse_err_t kv_remove_obj(struct hse_kvdb_opspec *os, CollectionRef& c, Onode& o);
  hse_err_t kv_omap_rm_entry(struct hse_kvdb_opspec *os, Onode& o, const std::string& omap_key);
  hse_err_t kv_update_obj_data_len(struct hse_kvdb_opspec *os, CollectionRef& c, Onode& o,
    uint64_t object_data_len);
  hse_err_t kv_omap_put(struct hse_kvdb_opspec *os, CollectionRef& c, Onode& o, std::string& omap_key,
    bufferlist& omap_value);
  hse_err_t kv_omap_read_entry(struct hse_kvdb_opspec *os, struct hse_kvs_cursor *cursor,
    bool ceph_iterator, struct OmapBlk0& blk0, std::string& omap_key, bufferlist *val_bl, bool& found);
  hse_err_t kv_omap_header_read(struct hse_kvdb_opspec *os, Onode& o, ceph::buffer::list *header,
      bool& found);
  hse_err_t kv_omap_create_hse_cursor(struct hse_kvdb_opspec *os, Onode& o,
    struct hse_kvs_cursor **cursor);



  uint32_t object_data_hse_key2block_nb(std::string& object_data_hse_key);
  uint32_t block_offset(uint64_t offset);
  uint32_t end_block_length(uint64_t offset, size_t length);
  uint32_t append_zero_blocks_in_bl(int32_t first_block_nb, int32_t last_block_nb, bufferlist& bl,
    uint32_t first_offset, uint32_t last_length);

  void omap_hse_key(hse_oid_t hse_oid, const std::string& omap_key, uint32_t omap_block_nb,
    std::string *omap_block_hse_key);
  static uint32_t object_omap_hse_key2block_nb(std::string& object_omap_hse_key);
  void object_omap_hse_key2omap_key(std::string& object_omap_kse_key, std::string& omap_key);




  hse_err_t _omap_rmkeys(struct hse_kvdb_opspec *os, CollectionRef& c, Onode& o, bufferlist& keys_bl);
  hse_err_t _omap_setkeys(struct hse_kvdb_opspec *os, CollectionRef& c, Onode& o, bufferlist& keys_bl);

  hse_err_t _setxattrs(struct hse_kvdb_opspec *os, CollectionRef& c,
    Onode& o, const map<string,bufferptr>& aset);
  hse_err_t _rmxattr(struct hse_kvdb_opspec *os, CollectionRef& c,
    Onode& o, const string& name);
  hse_err_t  _rmxattrs(struct hse_kvdb_opspec *os, CollectionRef& c,
    Onode& o);


  hse_err_t hse_kvs_cursor_create_wrapper(struct hse_kvs *kvs, struct hse_kvdb_opspec *opspec,
    const void *filt, size_t filt_len, struct hse_kvs_cursor **cursor);
  hse_err_t hse_kvs_cursor_seek_wrapper(struct hse_kvs_cursor *cursor,
    struct hse_kvdb_opspec *opspec, const void *key, size_t key_len, const void **found,
    size_t * found_len);
  hse_err_t hse_kvs_cursor_read_wrapper(struct hse_kvs_cursor *cursor,
    struct hse_kvdb_opspec *opspec, const void **key, size_t *key_len, const void **val,
    size_t *val_len, bool *eof);
  hse_err_t kv_retrieve_hse_oid(void);
  hse_err_t kv_save_hse_oid(void);



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
    return _kvdb != nullptr;
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

  int statfs(struct store_statfs_t *buf, osd_alert_list_t* alerts = nullptr) override;

  int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf, bool *per_pool_omap) override {
    return -EOPNOTSUPP;
  }

  CollectionHandle open_collection(const coll_t &cid) override;

  CollectionHandle create_new_collection(const coll_t &cid) override;

  void set_collection_commit_queue(const coll_t &cid, ContextQueue *commit_queue) override {}

  bool exists(CollectionHandle &c, const ghobject_t &oid) override;

  // HSE_TODO: determine if we can take use any of the keys defined for pool_opts_t
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
  ) override;

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
    std::map<std::string, ceph::buffer::list> *out) override;

  int omap_get_header(CollectionHandle &c, const ghobject_t &oid,
    ceph::buffer::list *header, bool allow_eio = false) override;

  int omap_get_keys(CollectionHandle &c, const ghobject_t &oid, std::set<std::string> *keys) override;

  int omap_get_values(CollectionHandle &c, const ghobject_t &oid, const std::set<std::string> &keys,
    std::map<std::string, ceph::buffer::list> *out) override;

#ifdef WITH_SEASTAR
  int omap_get_values(CollectionHandle &c, const ghobject_t &oid,
      const std::optional<std::string> &start_after,
      std::map<std::string, ceph::buffer::list> *out) override {
    return -EOPNOTSUPP;
  }
#endif

  int omap_check_keys(CollectionHandle &ch, const ghobject_t &oid,
      const std::set<std::string> &keys, std::set<std::string> *out) override;

  ObjectMap::ObjectMapIterator get_omap_iterator(CollectionHandle &c, const ghobject_t &oid) override;


  void set_fsid(uuid_d u) override { fsid = u; }
  uuid_d get_fsid() override { return fsid; }

  uint64_t estimate_objects_overhead(uint64_t num_objects) {
    return 64;
  }
private:
  std::string_view kvdb_name;
  uuid_d fsid;

  struct hse_kvdb *_kvdb = nullptr;
  struct hse_kvs *_ceph_metadata_kvs = nullptr;;

  // Key is coll_t2key (14 bytes)
  // No value
  // Prefix is coll_t2key
  struct hse_kvs *_collection_kvs = nullptr;

  // Key is coll_t2key (14 bytes) + ghobject_t2key + hse_oid_t (8 bytes)
  // Value is the length of the object data (8 bytes)
  // Prefix is coll_t2key
  struct hse_kvs *_collection_object_kvs = nullptr;

  // Key is hse_oid_t (8 bytes) + data block number (4 bytes)
  // Value is one data block worth of object data, aka 4kib
  // Prefix is hse_oid_t
  struct hse_kvs *_object_data_kvs = nullptr;

  // Key is hse_oid_t (8 bytes) + xattr_key
  // Value is xattr_val
  // Prefix is hse_oid_t
  struct hse_kvs *_object_xattr_kvs = nullptr;

  // For the omap header.
  // Key is the hse_oid_t (8 bytes).
  // Value is the header ( assumed to smaller than 1MiB).
  // Prefix is hse_oid_t
  //
  // For the omap key/value pairs:
  // Key is hse_oid_t (8 bytes) + omap_key + omap_block_number
  // Value is one block (1MiB) of the omap value.
  // An omap value can be any size.
  // If the value is bigger than 1MiB, several blocks are used. They are all 1MiB in size
  // except the last one that may be smaller.
  struct hse_kvs *_object_omap_kvs = nullptr;

  HseStore::CollectionRef get_collection(coll_t cid);

  friend class Syncer;
};


// This is to handle the case where the last block of an omap value is full.
// In this case, when reading (cursor read) the blocks of the value of an omap entry, we only know
// when to stop after having encountered eof or read the first value block of the next omap_entry.
// OmapBlk0 contains what has been read from the first block of the next omap entry.
// Its content will be use when we advance to the next omap entry.
struct OmapBlk0 {
    bool already_read; // true is the below is populated with valid values.
    std::string hse_key;
    const char *hse_val; // Not the whole omap entry value, only the first block.
    size_t hse_val_len;

  OmapBlk0() : already_read(false) {}
};

class HseStore::OmapIteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
  CollectionRef _c;
  Onode _o;
  struct hse_kvs_cursor *_cursor; // cursor on _object_omap_kvs
  struct hse_kvdb_opspec *_os; // options for _cursor

  // Data of the current omap entry. Aka the entry pointed to by this iterator.
  // Updated each time the cursor is re-positioned.
  bool _valid; // false is eof is encountered when moving this cursor.
  std::string _omap_key;
  bufferlist _value; // points on buffers allocated by the connector and will be freed by Ceph
		      // or points on only one buffer allocated by hse and that will be freed
		      // by hse when the cursor moves or is destroyed. In this case the omap value
		      // is smaller (strictly) that the omap block size.

  struct OmapBlk0 _blk0;

public:
  OmapIteratorImpl(CollectionRef c, Onode& o);
  ~OmapIteratorImpl();

  int seek_to_first() override;

  // Make the iterator to point to the first element in the omap whose key is considered
  // to go after k (strict).
  int upper_bound(const string &after) override;

  // Make the iterator to point to the first element in the omap whose key is not
  // considered to go before k (i.e., either it is equivalent or goes after).
  int lower_bound(const string &to) override;

  bool valid() override;
  int next() override;
  string key() override;
  bufferlist value() override;
  int status() override {
    return 0;
  }
};

//
// Handle syncing transactions. Aka making mutation done via transactions persistent.
//
//#define SYNCER_PERIOD_MS 50 // Sync every 50 ms.
#define SYNCER_PERIOD_MS 1000 // Sync every 1 sec
class Syncer {
  HseStore *_store;

  // Work queue used by the syncer, contains sync requests (flush_commit())
  boost::asio::io_context _sync_wq;

  boost::asio::io_context::work _work;
  boost::asio::deadline_timer _timer;

  // Thread group for the thread running the _sync_wq
  boost::thread_group _worker_threads;


  static void timer_cb(const boost::system::error_code& e, boost::asio::deadline_timer* timer,
    HseStore* store);

  // Function called when flush_commit() is called
  static void do_sync(HseStore::Collection *c, Context *ctx,
    uint64_t t_seq_committed_at_flush);

  static void kvdb_sync(HseStore *store);

public:
  void post_sync(HseStore::Collection *c, Context *ctx,
    uint64_t t_seq_committed_latest) {
    _sync_wq.post(boost::bind(do_sync, c, ctx, t_seq_committed_latest));

  }

  // Constructor
  Syncer(HseStore *store) : _store (store), _work(_sync_wq), _timer(_sync_wq, boost::posix_time::milliseconds(SYNCER_PERIOD_MS))
  {

    // Only one thread to serve the work queue to avoid processing work items in parallel.
    _worker_threads.create_thread(boost::bind(&boost::asio::io_service::run, &_sync_wq));

    // Start the timer.
    _timer.async_wait(boost::bind(timer_cb,
        boost::asio::placeholders::error, &_timer, _store));
  }

  // Destructor
  ~Syncer()
  {
    _sync_wq.stop();
    _worker_threads.join_all();
  }
};

#endif
