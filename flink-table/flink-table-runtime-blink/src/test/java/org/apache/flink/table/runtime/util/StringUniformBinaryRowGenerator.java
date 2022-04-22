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

package org.apache.flink.table.runtime.util;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.MutableObjectIterator;

/**
 * Uniform generator for binary row, which key and value is String type.
 */
public class StringUniformBinaryRowGenerator implements MutableObjectIterator<BinaryRowData> {

	int numKeys;
	int numVals;
	int keyCnt = 0;
	int valCnt = 0;
	int startKey = 0;
	int startVal = 0;
	boolean repeatKey;
	private StringValue key = new StringValue();
	private StringValue value = new StringValue();

	public StringUniformBinaryRowGenerator(int numKeys, int numVals, boolean repeatKey) {
		this(numKeys, numVals, 0, 0, repeatKey);
	}

	public StringUniformBinaryRowGenerator(
			int numKeys, int numVals, int startKey, int startVal, boolean repeatKey) {
		this.numKeys = numKeys;
		this.numVals = numVals;
		this.startKey = startKey;
		this.startVal = startVal;
		this.repeatKey = repeatKey;
	}

	@Override
	public BinaryRowData next(BinaryRowData reuse) {
		if (!repeatKey) {
			if (valCnt >= numVals + startVal) {
				return null;
			}

			key.setValue(String.valueOf(keyCnt++));
			value.setValue(String.valueOf(valCnt));

			if (keyCnt == numKeys + startKey) {
				keyCnt = startKey;
				valCnt++;
			}
		} else {
			if (keyCnt >= numKeys + startKey) {
				return null;
			}
			key.setValue(String.valueOf(keyCnt));
			value.setValue(String.valueOf(valCnt++));

			if (valCnt == numVals + startVal) {
				valCnt = startVal;
				keyCnt++;
			}
		}

		BinaryRowWriter writer = new BinaryRowWriter(reuse);
		writer.writeString(0, StringData.fromString(this.key.getValue()));
		writer.writeString(1, StringData.fromString(this.value.getValue()));
		writer.complete();
		return reuse;
	}

	@Override
	public BinaryRowData next() {
		key = new StringValue();
		value = new StringValue();
		return next(new BinaryRowData(2));
	}
}
