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

package org.apache.flink.runtime.operators.hash;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.commons.lang3.RandomStringUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A non-generic table with RocksDB as storage, mainly to support the storage of SolutionSet in
 * iterative computing, currently only supports records with storage type Tuple 2 and the first
 * field is long.
 * */
public class RocksDBTable<T> {
	private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBTable.class);

	private static final String SOLUTIONSET_COLUMN_FAMILY = "SolutionSet";

	// Max record serialized size in bytes, default 1MB.
	private static final int MAX_RECORD_SERIALIZED_SIZE = 1024 * 1024;

	private final TypeSerializer<T> buildSideSerializer;

	private final TypeComparator<T> buildSideComparator;

	// Used to store the serialized/deserialized key(long) value, this buffer is reusable.
	private final ByteBuffer longByteBuffer = ByteBuffer.allocate(Long.BYTES);

	// Used to store the serialized/deserialized record value, this buffer is reusable.
	private final byte[] data = new byte[MAX_RECORD_SERIALIZED_SIZE];

	private RocksDB db = null;

	private volatile boolean running = true;

	private List<ColumnFamilyHandle> columnFamilyHandles = null;

	private ColumnFamilyHandle solutionSetHandle = null;

	private WriteOptions writeOptions = null;

	public RocksDBTable(
		TypeSerializer<T> buildSideSerializer,
		TypeComparator<T> buildSideComparator) {
		this.buildSideSerializer = buildSideSerializer;
		this.buildSideComparator = buildSideComparator;
	}

	public void open() throws Exception {
		DBOptions dbOptions = new DBOptions()
			.setIncreaseParallelism(4)
			.setUseFsync(false)
			.setMaxOpenFiles(-1)
			.setCreateIfMissing(true)
			.setCreateMissingColumnFamilies(true);

		writeOptions = new WriteOptions()
			.setDisableWAL(true)
			.setSync(false);

		// Pre-defined options for better performance on regular spinning hard disks.
		// TODO(caojianhua): The configuration of rocksdb supports customization.
		final long blockCacheSize = 256 * 1024 * 1024;
		final long blockSize = 128 * 1024;
		final long targetFileSize = 256 * 1024 * 1024;
		final long writeBufferSize = 64 * 1024 * 1024;

		ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions()
			.setCompactionStyle(CompactionStyle.LEVEL)
			.setLevelCompactionDynamicLevelBytes(true)
			.setTargetFileSizeBase(targetFileSize)
			.setMaxBytesForLevelBase(4 * targetFileSize)
			.setWriteBufferSize(writeBufferSize)
			.setMinWriteBufferNumberToMerge(3)
			.setMaxWriteBufferNumber(4)
			.setTableFormatConfig(
				new BlockBasedTableConfig()
					.setBlockCacheSize(blockCacheSize)
					.setBlockSize(blockSize)
					.setFilter(new BloomFilter())
			);

		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions));
		columnFamilyDescriptors.add(new ColumnFamilyDescriptor(SOLUTIONSET_COLUMN_FAMILY.getBytes(), columnFamilyOptions));
		columnFamilyHandles = new ArrayList<>();

		// Store rocksdb data in current workdir, it will automatically clean up as the container is over.
		// TODO(caojianhua): Data storage location supports configurability, making it easy to subsequently specify an SSD disk as storage.
		String dbPath = "./" + RandomStringUtils.randomAlphabetic(20);
		db = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, columnFamilyHandles);
		if (columnFamilyDescriptors.size() != columnFamilyHandles.size()) {
			throw new Exception("Descriptors != Handles, " + columnFamilyHandles.size() + " != " + columnFamilyHandles.size());
		}
		solutionSetHandle = columnFamilyHandles.get(1);
	}

	public void close() {
		this.running = false;

		for (ColumnFamilyHandle handle : columnFamilyHandles) {
			handle.close();
		}

		if (db != null) {
			db.close();
		}
	}

	public void insertOrReplaceRecord(T record) throws Exception {
		db.put(solutionSetHandle, writeOptions, serializeKey(record), serializeRecord(record));
	}

	public void buildTableWithUniqueKey(final MutableObjectIterator<T> input) throws Exception {
		T record = null;
		while (this.running && (record = input.next()) != null) {
			insertOrReplaceRecord(record);
		}
	}

	public RocksDBTableIterator iterator() {
		return new RocksDBTableIterator(db.newIterator(solutionSetHandle));
	}

	public class RocksDBTableIterator {
		private RocksIterator it;

		public RocksDBTableIterator(RocksIterator it) {
			this.it = it;
			this.it.seekToFirst();
		}

		public T next() throws IOException {
			if (it.isValid()) {
				T record = deserializeRecord(it.value(), it.value().length);
				it.next();
				return record;
			} else {
				return null;
			}
		}

		public void close() {
			this.it.close();
		}
	}

	public <PT> RocksDBTableProber<PT> createProber() {
		return new RocksDBTableProber<>();
	}

	public final class RocksDBTableProber<PT> {
		public T getMatchFor(PT probeSideRecord) {
			try {
				int length = db.get(solutionSetHandle, serializeKey(probeSideRecord), data);
				if (length == RocksDB.NOT_FOUND) {
					return null;
				}

				if (length > data.length) {
					throw new Exception("value too large, max serialized value size: " + data.length);
				}
				return deserializeRecord(data, length);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public TypeSerializer<T> getBuildSideSerializer() {
		return buildSideSerializer;
	}

	public TypeComparator<T> getBuildSideComparator() {
		return buildSideComparator;
	}

	private byte[] serializeKey(Object record) throws Exception {
		if (!(record instanceof Tuple2) || !(((Tuple2)record).f0 instanceof Long)) {
			throw new Exception("Invalid record type, " + record.getClass());
		}

		longByteBuffer.clear();
		longByteBuffer.putLong((Long) ((Tuple2)record).f0);
		return longByteBuffer.array();
	}

	private byte[] serializeRecord(T record) throws IOException {
		SingleSegmentOutputView outputView = SingleSegmentOutputView.wrap(data, data.length);
		this.buildSideSerializer.serialize(record, outputView);
		return Arrays.copyOf(data, outputView.getCurrentPositionInSegment());
	}

	private T deserializeRecord(byte[] datas, int size) throws IOException {
		SingleSegmentInputView inputView = SingleSegmentInputView.wrap(datas, size);
		return buildSideSerializer.deserialize(inputView);
	}

	// Encapsulated the value buffer using AbstractPagedOutputView, easy to serialize the record.
	private static final class SingleSegmentOutputView extends AbstractPagedOutputView {

		SingleSegmentOutputView(int segmentSize) {
			super(segmentSize, 0);
		}

		void set(MemorySegment segment) {
			seekOutput(segment, 0);
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException {
			throw new EOFException();
		}

		public static SingleSegmentOutputView wrap(byte[] buffer, int length) {
			SingleSegmentOutputView outputView = new SingleSegmentOutputView(length);
			outputView.set(HeapMemorySegment.FACTORY.wrap(buffer));
			return outputView;
		}
	}

	// Encapsulated the value buffer using AbstractPagedInputView, easy to deserializze the record.
	private static final class SingleSegmentInputView extends AbstractPagedInputView {

		private final int limit;

		SingleSegmentInputView(int limit) {
			super(0);
			this.limit = limit;
		}

		protected void set(MemorySegment segment, int offset) {
			seekInput(segment, offset, this.limit);
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
			throw new EOFException();
		}

		@Override
		protected int getLimitForSegment(MemorySegment segment) {
			return this.limit;
		}

		public static SingleSegmentInputView wrap(byte[] buffer, int size) {
			SingleSegmentInputView inputView = new SingleSegmentInputView(size);
			inputView.set(HeapMemorySegment.FACTORY.wrap(buffer), 0);
			return inputView;
		}
	}
}
