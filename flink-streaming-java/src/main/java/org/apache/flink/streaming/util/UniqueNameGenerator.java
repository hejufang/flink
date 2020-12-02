/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Unique name generator.
 */
public class UniqueNameGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(UniqueNameGenerator.class);
	private static final String DELIMITER = "_";

	/**
	 * Get unique name for the specific origin name.
	 * If the input prefix has been used before, we append an index suffix;
	 * else, we just return the origin name itself as a unique name.
	 *
	 * @param originName original node name
	 * @param prefixIndexMapPerJob a hashmap for recording node name and related index number
	 * @return the unique stream node name which may be appended a unique index number,
	 * 	       when there is a name conflict after truncating.
	 * */
	public static String appendSuffixIfNotUnique(
			String originName,
			Map<String, Integer> prefixIndexMapPerJob) {
		Integer index = prefixIndexMapPerJob.get(originName);
		if (index == null) {
			prefixIndexMapPerJob.put(originName, 0);
			return originName;
		}
		prefixIndexMapPerJob.put(originName, ++index);
		String uniqueName = originName + DELIMITER + index;
		LOG.info("Replace operator name: '{}' with unique name: '{}'", originName, uniqueName);
		return uniqueName;
	}
}
