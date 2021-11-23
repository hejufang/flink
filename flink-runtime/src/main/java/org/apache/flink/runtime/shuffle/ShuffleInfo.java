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

import java.io.Serializable;

/**
 * ShuffleInfo to identify a m*n pair shuffle.
 */
public class ShuffleInfo implements Serializable {
	private static final long serialVersionUID = 66011796L;

	private final int shuffleId;
	private final int mapperBeginIndex;
	private final int mapperEndIndex;
	private final int reducerBeginIndex;
	private final int reducerEndIndex;

	public ShuffleInfo(int shuffleId, int mapperBeginIndex, int mapperEndIndex, int reducerBeginIndex, int reducerEndIndex) {
		this.shuffleId = shuffleId;
		this.mapperBeginIndex = mapperBeginIndex;
		this.mapperEndIndex = mapperEndIndex;
		this.reducerBeginIndex = reducerBeginIndex;
		this.reducerEndIndex = reducerEndIndex;
	}

	public int getShuffleId() {
		return shuffleId;
	}

	public int getMapperBeginIndex() {
		return mapperBeginIndex;
	}

	public int getMapperEndIndex() {
		return mapperEndIndex;
	}

	public int getReducerBeginIndex() {
		return reducerBeginIndex;
	}

	public int getReducerEndIndex() {
		return reducerEndIndex;
	}

	public int getNumberOfMappers() {
		return mapperEndIndex - mapperBeginIndex + 1;
	}

	public int getNumberOfReducers() {
		return reducerEndIndex - reducerBeginIndex + 1;
	}

	@Override
	public String toString() {
		return "ShuffleInfo{" +
			"shuffleId=" + shuffleId +
			", mapperBeginIndex=" + mapperBeginIndex +
			", mapperEndIndex=" + mapperEndIndex +
			", reducerBeginIndex=" + reducerBeginIndex +
			", reducerEndIndex=" + reducerEndIndex +
			'}';
	}
}
