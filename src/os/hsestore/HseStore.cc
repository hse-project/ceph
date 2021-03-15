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

#include <string_view>
#include <hse/hse.h>

#include "common/debug.h"
#include "HseStore.h"

#define dout_context cct
#define dout_subsys ceph_subsys_hsestore
#undef dout_prefix
#define dout_prefix *_dout << "hsestore(" << path << ") "

static constexpr std::string_view CEPH_METADATA_KVS_NAME = "ceph-metadata";
static constexpr std::string_view COLLECTION_OBJECT_KVS_NAME = "collection-object";
static constexpr std::string_view OBJECT_DATA_KVS_NAME = "object-data";
static constexpr std::string_view OBJECT_XATTR_KVS_NAME = "object-xattr";
static constexpr std::string_view OBJECT_OMAP_KVS_NAME = "object-omap";

HseStore::~HseStore()
{
}

int HseStore::mount()
{
  hse_err_t rc = 0;

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

err_out:
  return rc ? -hse_err_to_errno(rc) : 0;
}

int HseStore::umount()
{
  hse_err_t rc = 0;

  /* HSE_TODO: how to handle error logic here */
  rc = hse_kvdb_kvs_close(ceph_metadata_kvs);
  if (rc) {
    dout(10) << " failed to close the ceph-metadata kvs" << dendl;
    goto err_out;
  }
  ceph_metadata_kvs = nullptr;

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
