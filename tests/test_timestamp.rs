// Copyright 2020 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod util;

use pretty_assertions::assert_eq;

use rocksdb::{IteratorMode, Options, ReadOptions, WriteBatch, DB};
use util::DBPath;

#[test]
fn timestamp_works() {
    let path = DBPath::new("_rust_rocksdb_timestamp_works");
    {
        let mut db = DB::open_default(&path).unwrap();

        let mut options = Options::default();
        options.set_comparator_with_u64_ts();

        db.create_cf("mytscf", &options).unwrap();
        let cf = db.cf_handle("mytscf").unwrap();
        let mut batch = WriteBatch::default();
        batch.put_cf_with_ts(cf, b"k1", b"v1-1", 1);
        batch.put_cf_with_ts(cf, b"k1", b"v1-2", 2);
        batch.put_cf_with_ts(cf, b"k1", b"v1-256", 256);
        batch.delete_cf_with_ts(cf, b"k1", 257);

        db.write(batch).unwrap();

        let get_with_timestamp = |timestamp: u64| {
            let mut read_opts = ReadOptions::default();
            read_opts.set_timestamp(Some(timestamp));
            db.get_cf_opt(cf, b"k1", &read_opts).unwrap()
        };

        assert_eq!(get_with_timestamp(0), None);
        assert_eq!(get_with_timestamp(1), Some(b"v1-1".to_vec()));
        assert_eq!(get_with_timestamp(2), Some(b"v1-2".to_vec()));
        assert_eq!(get_with_timestamp(256), Some(b"v1-256".to_vec()));
        assert_eq!(get_with_timestamp(257), None);
    }
}

#[test]
fn iterator_test() {
    let path = DBPath::new("_rust_rocksdb_iteratortest");
    {
        let data = [(b"k1", b"v1111"), (b"k2", b"v2222"), (b"k3", b"v3333")];
        let db = DB::open_default(&path).unwrap();

        for (key, value) in &data {
            assert!(db.put(key, value).is_ok());
        }

        let iter = db.iterator(IteratorMode::Start);

        for (idx, (db_key, db_value)) in iter.map(Result::unwrap).enumerate() {
            let (key, value) = data[idx];
            assert_eq!((&key[..], &value[..]), (db_key.as_ref(), db_value.as_ref()));
        }
    }
}
