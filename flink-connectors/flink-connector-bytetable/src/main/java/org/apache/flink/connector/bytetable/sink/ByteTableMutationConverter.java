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

package org.apache.flink.connector.bytetable.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.bytetable.util.ByteArrayWrapper;

import com.bytedance.bytetable.RowMutation;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.io.Serializable;

/**
 * A converter used to converts the input record into ByteTable {@link Mutation}.
 * @param <T> type of input record.
 */
@Internal
public interface ByteTableMutationConverter<T> extends Serializable {

	/**
	 * Initialization method for the function. It is called once before conversion method.
	 */
	void open();

	/**
	 * Converts the input record into ByteTable {@link Mutation}. A mutation can be a
	 * {@link Put} or {@link Delete}.
	 */
	RowMutation convertToMutation(T record) throws IOException;

	ByteArrayWrapper getRowKeyByteArrayWrapper(T record);
}