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

package org.apache.flink.runtime.shuffle;

import com.bytedance.css.client.ShuffleClient;
import com.bytedance.css.client.stream.CssDiskEpochReader;
import com.bytedance.css.client.stream.CssInputStream;
import com.bytedance.css.common.CssConf;
import com.bytedance.css.common.protocol.CommittedPartitionInfo;
import com.bytedance.css.common.protocol.PartitionGroup;
import com.bytedance.css.common.protocol.PartitionInfo;
import com.bytedance.css.network.client.TransportClientFactory;

import java.io.IOException;
import java.util.List;

/**
 * TestShuffleClient.
 */
public class TestShuffleClient extends ShuffleClient {

	int numberOfReducers;

	public TestShuffleClient() {}

	public TestShuffleClient(int numberOfReducers) {
		this.numberOfReducers = numberOfReducers;
	}

	@Override
	public int pushData(String s, int i, int i1, int i2, int i3, byte[] bytes, int i4, int i5, int i6, int i7) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int pushData(String s, int i, int i1, int i2, int i3, byte[] bytes, int i4, int i5, int i6, int i7, boolean b) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int[] batchPushData(
		String applicationId,
		int shuffleId,
		int mapperId,
		int mapperAttemptId,
		int[] reducerIdArray,
		byte[] data,
		int[] offsetArray,
		int[] lengthArray,
		int numMappers,
		int numPartitions,
		boolean skipCompress) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void mapperEnd(String s, int i, int i1, int i2, int i3) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void mapperClose(String s, int i, int i1, int i2) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<CommittedPartitionInfo> getPartitionInfos(String s, int i, int[] ints, int i1, int i2) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CssDiskEpochReader createEpochReader(String s, int i, List<CommittedPartitionInfo> list, CssConf cssConf) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public CssInputStream readPartitions(String applicationId, int shuffleId, int[] reduceIds, int startMapIndex, int endMapIndex) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean validShuffleStageReady(String s, int i) throws IOException {
		return true;
	}

	@Override
	public int[] getMapperAttempts(int i) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<PartitionInfo> registerShuffle(String s, int i, int i1, int i2) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<PartitionGroup> registerPartitionGroup(String s, int i, int i1, int i2, int i3) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void unregisterShuffle(String s, int i, boolean b) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void registerApplication(String s) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void shutDown() {
		throw new UnsupportedOperationException();
	}

	@Override
	public TransportClientFactory getClientFactory() {
		throw new UnsupportedOperationException();
	}
}
