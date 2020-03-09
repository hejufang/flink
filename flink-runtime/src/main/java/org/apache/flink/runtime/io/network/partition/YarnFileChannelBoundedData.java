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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of {@link BoundedData} that writes directly into a File Channel.
 * The readers are simple file channel readers using a simple dedicated buffer pool.
 */
final class YarnFileChannelBoundedData implements BoundedData {

	private final Path filePath;

	private final FileChannel fileChannel;

	private final ByteBuffer[] headerAndBufferArray;

	private long size;

	private final String finalFilePath;

	YarnFileChannelBoundedData(
			Path filePath,
			FileChannel fileChannel,
			String finalFilePath) {

		this.filePath = checkNotNull(filePath);
		this.fileChannel = checkNotNull(fileChannel);
		this.finalFilePath = finalFilePath;
		this.headerAndBufferArray = BufferReaderWriterUtil.allocatedWriteBufferArray();
	}

	@Override
	public void writeBuffer(Buffer buffer) throws IOException {
		size += BufferReaderWriterUtil.writeToByteChannel(fileChannel, buffer, headerAndBufferArray);
	}

	@Override
	public void finishWrite() throws IOException {
		fileChannel.close();
		try {
			Files.move(filePath, filePath.getFileSystem().getPath(finalFilePath));
		} catch (FileAlreadyExistsException ignored) {
			// Maybe another speculative task finish first.
		}
	}

	@Override
	public Reader createReader(ResultSubpartitionView subpartitionView) throws IOException {
		throw new UnsupportedOperationException("Should not create reader on yarn shuffle file channel!");
	}

	@Override
	public long getSize() {
		return size;
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeQuietly(fileChannel);
	}

	// ------------------------------------------------------------------------

	public static YarnFileChannelBoundedData create(Path filePath, String finalFilePath) throws IOException {
		final FileChannel fileChannel = FileChannel.open(
				filePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);

		return new YarnFileChannelBoundedData(
				filePath,
				fileChannel,
				finalFilePath);
	}

}
