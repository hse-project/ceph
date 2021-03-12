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

#include "common/TrackedOp.h"
#include "osd/osd_types.h"
#include "os/ObjectStore.h"
#include "os/ObjectMap.h"
#include "os/Transaction.h"
#include "include/uuid.h"

class HseStore : public ObjectStore {
  using hse_oid_t = uint64_t;
public:
  HseStore(CephContext *cct, const std::string &path)
    : ObjectStore(cct, path)
    , kvdb_name(cct->_conf->hsestore_kvdb) {}
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
      ThreadPool::TPHandle *handle = NULL) override {
    return -EOPNOTSUPP;
  }

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

  CollectionHandle open_collection(const coll_t &cid) override {
    return {};
  }

  CollectionHandle create_new_collection(const coll_t &cid) override {
    return {};
  }

  void set_collection_commit_queue(const coll_t &cid, ContextQueue *commit_queue) override {}

  bool exists(CollectionHandle &c, const ghobject_t &oid) override {
    return true;
  }

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

  int list_collections(std::vector<coll_t> &ls) override {
    return -EOPNOTSUPP;
  }

  bool collection_exists(const coll_t& c) override {
    return false;
  }

  int collection_empty(CollectionHandle &c, bool *empty) override {
    return -EOPNOTSUPP;
  }

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

  void set_fsid(uuid_d u) override {}
  uuid_d get_fsid() {
    return {};
  }

  uint64_t estimate_objects_overhead(uint64_t num_objects) {
    return 64;
  }

private:
  std::string_view kvdb_name;

  struct hse_kvdb *kvdb;
  struct hse_kvs *ceph_metadata_kvs;
  struct hse_kvs *collection_object_kvs;
  struct hse_kvs *object_data_kvs;
  struct hse_kvs *object_xattr_kvs;
  struct hse_kvs *object_omap_kvs;
};

#endif
