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

#include "HseStore.h"

#define dout_context cct
#define dout_subsys ceph_subsys_hsestore
#undef dout_prefix
#define dout_prefix *_dout << "hsestore(" << path << ") "

//
// WaitCondTs functions
//

// Constructor, called when entering function queue_transactions()
WaitCondTs::WaitCondTs(TransactionSerialization *ts)
{
  this->_ts = ts;
  std::unique_lock lock(ts->_ts_mutex);
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

  std::unique_lock lock(_ts->_ts_mutex);

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

  std::unique_lock lock(_committed_wait_persist_mtx);
  _committed_wait_persist.push_back(twp);
}

void HseStore::Collection::committed_wait_persist_cb(HseStore::Collection *c)
{

  std::unique_lock lock(c->_committed_wait_persist_mtx);

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
    std::shared_lock l{c->_store->coll_lock};
    for (auto p = c->_store->coll_map.begin(); p != c->_store->coll_map.end(); ++p) {

  		c1 = static_cast<HseStore::Collection*>(p->second.get());
  		c1->_t_seq_committed_wait_sync = c1->_t_seq_next - 1;
    }
    l.unlock();
  	
    // hse_sync()

    // Record the latest transaction persisted.
    l.lock();
    for (auto p = c->_store->coll_map.begin(); p != c->_store->coll_map.end(); ++p) {

      c1 = static_cast<HseStore::Collection*>(p->second.get());
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


HseStore::HseStore(CephContext *cct, const string& path)
  : ObjectStore(cct, path),
    finisher(cct)
{
}
HseStore::~HseStore()
{}


ObjectStore::CollectionHandle HseStore::create_new_collection(const coll_t& cid)
{
  auto c = ceph::make_ref<HseStore::Collection>(this, cid);

  std::unique_lock l{coll_lock};
  new_coll_map[cid] = c;
  return c;
}

int HseStore::list_collections(vector<coll_t>& ls)
{
  std::shared_lock l{coll_lock};

  for (ceph::unordered_map<coll_t, CollectionRef>::iterator p = coll_map.begin();
       p != coll_map.end(); ++p)
    ls.push_back(p->first);
  return 0;
}

bool HseStore::collection_exists(const coll_t& c)
{
  std::shared_lock l{coll_lock};
  return coll_map.count(c);
}

HseStore::CollectionRef HseStore::get_collection(coll_t cid)
{
  std::shared_lock l{coll_lock};
  ceph::unordered_map<coll_t,CollectionRef>::iterator cp = coll_map.find(cid);
  if (cp == coll_map.end())
    return CollectionRef();
  return cp->second;
}
int HseStore::remove_collection(coll_t cid, CollectionRef *c)
{
  ceph_abort_msg("not supported");
  return 0;
} 
int HseStore::create_collection(coll_t cid, unsigned bits, CollectionRef *c)
{
  ceph_abort_msg("not supported");
  return 0;
}
int HseStore::split_collection(CollectionRef& c, CollectionRef& d, unsigned bits, int rem)
{
  ceph_abort_msg("not supported");
  return 0;
}
int HseStore::merge_collection(CollectionRef *c, CollectionRef& d, unsigned bits)
{
  ceph_abort_msg("not supported");
  return 0;
}


int HseStore::queue_transactions(
  ObjectStore::CollectionHandle& ch,
  vector<ceph::os::Transaction>& tls,
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

  for (vector<Transaction>::iterator p = tls.begin(); p != tls.end(); ++p) {
    Context *contexts;

    start_one_transaction(c, &(*p));

    //
    // Invoke the two Ceph apply callbacks.
    //
    contexts = (*p).get_on_applied_sync();
    if (contexts)
      contexts->complete(0);

    contexts = (*p).get_on_applied();
    if (contexts)
      finisher.queue(contexts);
  
    //
    // Queue the transaction till persisted.
    // The "commit" Ceph callback will be invoked later when the transaction
    // is persisted by the syncer.
    //
    c->queue_wait_persist((*p).get_on_commit(), c->_t_seq_next++);
  }

  // The WaitCondTs destructor (variable ts) will wakeup the next queue_transactions() 
  // thread waiting to run on the collection.
  return 0;
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

  vector<CollectionRef> cvec(i.colls.size());
  unsigned j = 0;
  for (vector<coll_t>::iterator p = i.colls.begin(); p != i.colls.end();
       ++p, ++j) {
    cvec[j] = get_collection(*p);
  }

  for (int pos = 0; i.have_op(); ++pos) {
    Transaction::Op *op = i.decode_op();
    int r = 0;

    // no coll or obj
    if (op->op == Transaction::OP_NOP)
      continue;

    // collection operations
    CollectionRef &c = cvec[op->cid];
    switch (op->op) {
    case Transaction::OP_RMCOLL:
      {
        coll_t cid = i.get_cid(op->cid);
	r = remove_collection(cid, &c);
	if (!r)
	  continue;
      }
      break;

    case Transaction::OP_MKCOLL:
      {
	ceph_assert(!c);
        coll_t cid = i.get_cid(op->cid);
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
        uint32_t bits = op->split_bits;
        uint32_t rem = op->split_rem;
	r = split_collection(c, cvec[op->dest_cid], bits, rem);
	if (!r)
	  continue;
      }
      break;

    case Transaction::OP_MERGE_COLLECTION:
      {
        uint32_t bits = op->split_bits;
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
