/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

/**
 * Direct use rocksdb.
 */
public class DirectRocksDBDelegate extends AbstractRocksDBDelegate {

	public DirectRocksDBDelegate(RocksDB delegate) {
		super(delegate);
	}

	@Override
	public void put(ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOpts, byte[] key, byte[] value) throws RocksDBException {
		delegate.put(columnFamilyHandle, writeOpts, key, value);
	}

	@Override
	public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
		return delegate.get(columnFamilyHandle, key);
	}

	@Override
	public void delete(ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOpt, byte[] key) throws RocksDBException {
		delegate.delete(columnFamilyHandle, writeOpt, key);
	}

	@Override
	public void merge(ColumnFamilyHandle columnFamilyHandle, WriteOptions writeOpts, byte[] key, byte[] value) throws RocksDBException {
		delegate.merge(columnFamilyHandle, writeOpts, key, value);
	}
}
