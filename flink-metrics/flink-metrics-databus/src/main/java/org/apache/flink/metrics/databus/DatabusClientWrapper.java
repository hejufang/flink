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

package org.apache.flink.metrics.databus;

import org.apache.flink.annotation.VisibleForTesting;

import com.bytedance.data.databus.DatabusClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Wraps databus client to provide sending data in a batch.
 */
public class DatabusClientWrapper {

	private static final int BATCH_SIZE = 100;
	private static final int CODEC = 1;

	private DatabusClient client;

	private byte[][] keys;
	private byte[][] values;
	private int index;

	@VisibleForTesting
	public DatabusClientWrapper() {
		this.keys = new byte[BATCH_SIZE][];
		this.values = new byte[BATCH_SIZE][];
		this.index = 0;
	}

	public DatabusClientWrapper(String channel) {
		if (channel == null) {
			throw new IllegalArgumentException();
		}
		this.client = new DatabusClient(channel);
		this.keys = new byte[BATCH_SIZE][];
		this.values = new byte[BATCH_SIZE][];
		this.index = 0;
	}

	public void addToBuffer(String data) throws IOException {
		keys[index] = data.getBytes(StandardCharsets.UTF_8);
		values[index] = data.getBytes(StandardCharsets.UTF_8);
		index++;
		if (index >= BATCH_SIZE) {
			flush();
		}
	}

	public void flush() throws IOException {
		if (index == BATCH_SIZE) {
			client.sendMultiple(keys, values, CODEC);
		} else {
			client.sendMultiple(Arrays.copyOfRange(keys, 0, index),
				Arrays.copyOfRange(values, 0, index), CODEC);
		}
		reset();
	}

	public void close() {
		if (client != null) {
			client.close();
		}
	}

	protected void reset() {
		index = 0;
	}

	public byte[][] getKeys() {
		return keys;
	}

	public byte[][] getValues() {
		return values;
	}

	public int getIndex() {
		return index;
	}
}
