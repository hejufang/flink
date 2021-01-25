/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.loghouse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

/**
 * Gzip Compressor using gzip compress algorithm.
 */
public class GzipCompressor implements Compressor {

	private static final long serialVersionUID = 1L;

	private transient ByteArrayOutputStream baos;

	@Override
	public void open() throws IOException {
		baos = new ByteArrayOutputStream();
	}

	@Override
	public byte[] compress(byte[] input) throws IOException {
		// reuse ByteArrayOutputStream buffer, but reset it's contents.
		baos.reset();
		// GZIPOutputStream cannot be reused, it's constructor will write header to the output.
		GZIPOutputStream gzip = new GZIPOutputStream(baos);
		gzip.write(input);
		gzip.finish();
		// GZIPOutputStream.close() will call the inner OutputStream.close().
		// However, ByteArrayOutputStream.close() has no side effects, it can still be used.
		gzip.close();
		return baos.toByteArray();
	}

	@Override
	public void close() throws IOException {
		baos.close();
	}
}
