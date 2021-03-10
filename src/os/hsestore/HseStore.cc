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

HseStore::~HseStore()
{}
