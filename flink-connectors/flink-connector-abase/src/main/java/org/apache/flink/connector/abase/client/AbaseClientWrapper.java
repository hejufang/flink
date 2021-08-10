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

package org.apache.flink.connector.abase.client;

import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.utils.AbaseClientTableUtils;

import com.bytedance.abase.AbaseClient;
import com.bytedance.abase.AbaseTable;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * abase client wrapper.
 */
public class AbaseClientWrapper implements BaseClient {

	private static final long serialVersionUID = 1L;

	private transient AbaseClient abaseClient;
	private transient AbaseTable abaseTable;
	private final AbaseNormalOptions normalOptions;

	public AbaseClientWrapper(AbaseNormalOptions normalOptions) {
		this.normalOptions = normalOptions;
	}

	@Override
	public void open() {
		this.abaseClient = AbaseClientTableUtils.getAbaseClient(normalOptions);
		this.abaseTable = abaseClient.getTable(normalOptions.getTable());
	}

	@Override
	public void close() throws Exception {
		if (this.abaseClient != null) {
			this.abaseClient.close();
		}
	}

	@Override
	public byte[] get(byte[] key) {
		return this.abaseTable.get(key);
	}

	@Override
	public String get(String key) {
		return this.abaseTable.get(key);
	}

	@Override
	public List<String> hmget(String key, String... fields) {
		return this.abaseTable.hmget(key, fields);
	}

	@Override
	public List<String> lrange(String key, long start, long end) {
		return this.abaseTable.lrange(key, start, end);
	}

	@Override
	public Set<String> smembers(String key) {
		return this.abaseTable.smembers(key);
	}

	@Override
	public Set<String> zrange(String key, long start, long end) {
		return this.abaseTable.zrange(key, start, end);
	}

	@Override
	public Map<String, String> hgetAll(String key) {
		return this.abaseTable.hgetAll(key);
	}

	@Override
	public ClientPipeline pipelined() {
		return new AbasePipelineWrapper(this.abaseTable.pipelined());
	}
}
