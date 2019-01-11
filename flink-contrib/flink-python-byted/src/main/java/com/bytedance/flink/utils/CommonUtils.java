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

package com.bytedance.flink.utils;

import com.bytedance.flink.configuration.Constants;
import org.apache.flink.api.java.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Common utils.
 */
public class CommonUtils {
	private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);

	public static Map mergeMaps(Map<String, Object> common, Map<String, Object> specific) {
		if (common == null) {
			return new HashMap<>(specific);
		}
		if (specific == null) {
			return new HashMap<>(common);
		}
		Map<String, Object> result = new HashMap<>(common);
		result.putAll(specific);
		return result;
	}

	public static List<Object> tupleToList(Tuple tuple) {
		int fields = getTupleFields(tuple);
		List<Object> result = new ArrayList<>();

		for (int i = 0; i < fields; i++) {
			result.add(tuple.getField(i));
		}
		return result;
	}

	public static List<String> parseStringToList(String str, String separator) {
		List<String> result = new ArrayList<>();
		if (str == null || str.isEmpty()) {
			return result;
		}
		String[] itemArray = str.split(separator);
		result = Arrays.asList(itemArray);
		return result;
	}

	public static String loadFileToString(String filePath) throws IOException {
		String content = new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8);
		return content;
	}

	public static Tuple extendTuple(Tuple tuple, int resultFields) {
		int fields = getTupleFields(tuple);

		if (fields < 0 || fields > Constants.FLINK_TUPLE_MAX_FIELD_SIZE) {
			throw new RuntimeException(String.format("resultFields must between [0, %s]",
					Constants.FLINK_TUPLE_MAX_FIELD_SIZE));
		}

		if (resultFields < 0 || resultFields > Constants.FLINK_TUPLE_MAX_FIELD_SIZE) {
			throw new RuntimeException(String.format("resultFields must between [0, %s]",
					Constants.FLINK_TUPLE_MAX_FIELD_SIZE));
		}

		if (fields > resultFields) {
			throw new RuntimeException("resultFields must be greater than or equal to fields");
		}

		Tuple resultTuple = null;
		try {
			resultTuple = Tuple.getTupleClass(resultFields).newInstance();
			int originFileds = tuple.getArity();
			for (int i = 0; i < originFileds; i++) {
				resultTuple.setField(tuple.getField(i), i);
			}
		} catch (InstantiationException e) {
			LOG.error("InstantiationException occurred while creating a resultTuple", e);
		} catch (IllegalAccessException e) {
			LOG.error("IllegalAccessException occurred while creating a resultTuple", e);
		}
		return resultTuple;
	}

	public static int getTupleFields(Tuple tuple) {
		int fields = -1;
		for (int i = 0; i <= Constants.FLINK_TUPLE_MAX_FIELD_SIZE; i++) {
			if (Tuple.getTupleClass(i).isInstance(tuple)) {
				fields = i;
				break;
			}
		}
		return fields;
	}
}
