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

package com.bytedance.flink.pojo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Schema of input/output values.
 */
public class Schema implements Iterable<String>, Serializable {

	private List<String> fieldNameList;
	private Map<String, Integer> indexMap = new HashMap<>();

	public Schema(List<String> fieldNames) {
		if (fieldNames == null) {
			this.fieldNameList = new ArrayList<>();
			return;
		}
		this.fieldNameList = new ArrayList<>(fieldNames.size());
		for (String field : fieldNames) {
			if (fieldNameList.contains(field)) {
				throw new IllegalArgumentException(String.format("Duplicate field '%s'", field));
			}
			indexMap.put(field, fieldNameList.size());
			fieldNameList.add(field);
		}
	}

	public List<Object> select(List<String> fileds, List<Object> value) {
		List<Object> result = new ArrayList<>();
		for (String field : fileds) {
			result.add(select(field, value));
		}
		return result;
	}

	public Object select(String field, List<Object> value) {
		int index = indexMap.get(field);
		return value.get(index);
	}


	/**
	 * Returns the field on specified position.
	 */
	public String get(int index) {
		if (index >= fieldNameList.size()) {
			throw new IllegalArgumentException(String.format("Index %s out of range.", index));
		}
		return fieldNameList.get(index);
	}

	/**
	 * Returns the position of the specified field.
	 */
	public int fieldIndex(String field) {
		Integer index = indexMap.getOrDefault(field, -1);
		if (index < 0) {
			throw new IllegalArgumentException(field + " does not exist");
		}
		return index;
	}

	/**
	 * Returns true if this contains the specified name of the field.
	 */
	public boolean contains(String field) {
		return indexMap.containsKey(field);
	}

	public int size() {
		return fieldNameList.size();
	}

	public List<String> toList() {
		return new ArrayList<>(fieldNameList);
	}

	@Override
	public Iterator<String> iterator() {
		return null;
	}
}
