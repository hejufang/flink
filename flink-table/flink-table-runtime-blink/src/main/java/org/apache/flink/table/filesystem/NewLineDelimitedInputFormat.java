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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.RowData;

import java.io.IOException;

/**
 * Used for read Base64 encoded byte data from a newline delimited file.
 * Only available for LocalFileSystem.
 */
public class NewLineDelimitedInputFormat extends DelimitedInputFormat<RowData> {
	private final DeserializationSchema<RowData> deserializationSchema;
	private final CompressCodec codec;

	public NewLineDelimitedInputFormat(
			DeserializationSchema<RowData> deserializationSchema,
			CompressCodec codec,
			Path[] filePaths) {
		super.setFilePaths(filePaths);
		this.deserializationSchema = deserializationSchema;
		this.codec = codec;
	}

	@Override
	public void open(FileInputSplit fileSplit) throws IOException {
		super.open(fileSplit);
		try {
			deserializationSchema.open(UnregisteredMetricsGroup::new);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public RowData readRecord(RowData reuse, byte[] bytes, int offset, int numBytes) throws IOException {
		byte[] msg = new byte[numBytes];
		System.arraycopy(bytes, offset, msg, 0, numBytes);
		switch (codec) {
			case Base64:
				msg = java.util.Base64.getDecoder().decode(msg);
				break;
			case Default:
				break;
			default:
				throw new RuntimeException("Not supported compress codec: " + codec);
		}
		return deserializationSchema.deserialize(msg);
	}
}
