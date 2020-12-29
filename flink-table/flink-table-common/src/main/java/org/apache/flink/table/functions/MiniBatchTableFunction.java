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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collection;
import java.util.List;

/**
 * Lookup Table Function which want to use mini batch feature should implement {@link MiniBatchTableFunction}.
 *
 * @param <T> The output data type
 */
@PublicEvolving
public abstract class MiniBatchTableFunction<T> extends TableFunction<T> {
	public int batchSize() {
		return -1;
	}

	/**
	 * The bundle method to lookup batch of the keys to improve throughput in most of the case.
	 *
	 * @param keySequenceList Each element of object array type in keySequenceList is the values of join keys(multi key join).
	 * @return The returned list size should be equals to the keySequenceList's size.
	 * Each key sequence in keySequenceList is matching one collection in returned list with the same ordinal index.
	 * Each key sequence can emit multiple rows if more than one row are joined.
	 * If one key sequence cannot find any matched result, the corresponding collection should be null or empty.
	 */
	public abstract List<Collection<T>> eval(List<Object[]> keySequenceList);
}
