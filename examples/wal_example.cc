// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_wal_example";
#else
std::string kDBPath = "/tmp/rocksdb_wal_example";
#endif

int main() {
  DB* db;
  Options options;
  options.create_if_missing = true;
  options.max_open_files = -1;
  options.max_total_wal_size = 1;
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions()));
  column_families.push_back(
      ColumnFamilyDescriptor("new_cf", ColumnFamilyOptions()));
  std::vector<ColumnFamilyHandle*> handles;

  Status s = DB::Open(options, kDBPath, &db);

    // create column family
  ColumnFamilyHandle* cf;
  s = db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "new_cf", &cf);
  assert(s.ok());

  // close DB
  s = db->DestroyColumnFamilyHandle(cf);
  assert(s.ok());
  delete db;

  s = DB::Open(options, kDBPath, column_families, &handles, &db);

  std::cout << handles.size() << " open status is : " << s.ToString()  << std::endl;
  db->Put(WriteOptions(), handles[1], Slice("key1"), Slice("value1"));
  db->Put(WriteOptions(), handles[0], Slice("key2"), Slice("value2"));
  db->Put(WriteOptions(), handles[1], Slice("key3"), Slice("value3"));
  db->Put(WriteOptions(), handles[0], Slice("key4"), Slice("value4"));

  db->Flush(FlushOptions(), handles[1]);
  // key5 and key6 will appear in a new WAL
  db->Put(WriteOptions(), handles[1], Slice("key5"), Slice("value5"));
  db->Put(WriteOptions(), handles[0], Slice("key6"), Slice("value6"));

  db->Flush(FlushOptions(), handles[0]);
  // The older WAL will be archived and purged separetely
  
  delete db;

  return 0;
}
