/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The wrapper class of SstFIleWriter.
 */
public class RocksDBSSTFileWriter implements AutoCloseable {

	private static final String SST_FILE_PREFIX = "ingest_";
	private static final String SST_FILE_SUFFIX = ".sst";

	private final StateMetaInfoSnapshot stateMetaInfoSnapshot;

	private final Path tmpSstDir;

	private final KeyGroupDataIterator<byte[], byte[]> iterator;

	/** {@code maxSstSize} is the maximum size of an sst file before flushing it. */
	private final long maxSstSize;

	/**{@code envOptions} are the {@link EnvOptions} provided to the underlying {@link org.rocksdb.SstFileWriter}. */
	private final EnvOptions envOptions;

	/** {@code options} are the {@link Options} provided to the underlying {@link org.rocksdb.SstFileWriter}. */
	private final Options options;

	/** Sst file ID counter to avoid duplication. */
	private final AtomicInteger sstIdCounter;

	/** Record whether there is an exception. */
	private final AtomicReference<Exception> error;

	public RocksDBSSTFileWriter(
		Path tmpSstDir,
		KeyGroupDataIterator<byte[], byte[]> iterator,
		StateMetaInfoSnapshot stateMetaInfoSnapshot,
		long maxSstSize,
		EnvOptions envOptions,
		Options options,
		AtomicInteger sstIdCounter,
		AtomicReference<Exception> error) {
		this.iterator = iterator;
		this.stateMetaInfoSnapshot = stateMetaInfoSnapshot;
		this.tmpSstDir = tmpSstDir;
		this.maxSstSize = maxSstSize;
		this.envOptions = envOptions;
		this.options = options;
		this.sstIdCounter = sstIdCounter;
		this.error = error;
	}

	public Tuple2<StateMetaInfoSnapshot, List<String>> buildSstFiles() throws Exception {
		List<String> sstFiles = new ArrayList<>();
		SstFileWriter sstFileWriter = null;
		long writeBytes = 0L;
		try {
			checkError(); // fail-fast
			while (iterator.hasNext()) {
				if (sstFileWriter == null || writeBytes >= maxSstSize) {
					sstFileWriter = closeAndCreateSstFileWriter(sstFileWriter, sstFiles);
					writeBytes = 0L;
				}
				sstFileWriter.put(iterator.key(), iterator.value());
				writeBytes += iterator.key().length;
				writeBytes += iterator.value().length;
				iterator.next();
				checkError(); // fail-fast
			}
			closeSstFileWriter(sstFileWriter);
			sstFileWriter = null;
			return Tuple2.of(stateMetaInfoSnapshot, sstFiles);
		} catch (RestoreFutureException ignore) {
			// There is no need to throw an exception again, it has already been thrown elsewhere.
			return Tuple2.of(stateMetaInfoSnapshot, Collections.emptyList());
		} catch (Exception e) {
			error.compareAndSet(null, e);
			throw e;
		} finally {
			closeSstFileWriter(sstFileWriter);
		}
	}

	private SstFileWriter closeAndCreateSstFileWriter(SstFileWriter oriFileWriter, List<String> sstFiles) throws RocksDBException {
		if (oriFileWriter != null){
			closeSstFileWriter(oriFileWriter);
		}

		SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options);
		Path sstFile = tmpSstDir.resolve(SST_FILE_PREFIX + "_" + sstIdCounter.getAndIncrement() + SST_FILE_SUFFIX);
		String absolutePath = sstFile.toFile().getAbsolutePath();
		sstFiles.add(absolutePath);
		sstFileWriter.open(absolutePath);
		return sstFileWriter;
	}

	private void closeSstFileWriter(SstFileWriter sstFileWriter) throws RocksDBException {
		if (sstFileWriter != null){
			sstFileWriter.finish();
			sstFileWriter.close();
		}
	}

	private void checkError() throws RestoreFutureException {
		if (error.get() != null) {
			throw new RestoreFutureException();
		}
	}

	private static class RestoreFutureException extends Exception {
		// which means other threads or tasks occurs error
	}

	@Override
	public void close() throws Exception {
		iterator.close();
	}
}
