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

package org.apache.flink.table.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utilities for parsing method parameters.
 */
public class ParameterParseUtils {
	private static final Logger LOG = LoggerFactory.getLogger(ParameterParseUtils.class);
	// matches comma out of double quotation marks.
	private static final String COMMA_OUT_OF_DOUBLE_QUOTATION_MARKS =
		",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	/**
	 * Try to parse method parameter string, match case for long, float, String in order.
	 * The parameter classes will be stored in paramClassList.
	 * The parsed parameters will be stored in parsedParams.
	 * */
	public static ParameterEntity parse(String paramStr) {
		// split string with comma out of double quotation marks.
		String[] tokens = paramStr.split(COMMA_OUT_OF_DOUBLE_QUOTATION_MARKS, -1);
		return parse(Arrays.asList(tokens));
	}

	public static ParameterEntity parse(List<String> paramList) {
		List<Class> paramClassList = new ArrayList<>();
		List<Object> parsedParams = new ArrayList<>();
		for (String param : paramList) {
			param = param.trim();
			try {
				long longValue = Long.parseLong(param);
				paramClassList.add(long.class);
				parsedParams.add(longValue);
				continue;
			} catch (NumberFormatException e) {
				LOG.debug("{} cannot be converted to long value.", param);
			}
			try {
				float floatValue = Float.parseFloat(param);
				paramClassList.add(float.class);
				parsedParams.add(floatValue);
				continue;
			} catch (NumberFormatException e) {
				LOG.debug("{} cannot be converted to float value.", param);
			}
			if (inDoubleQuotationMarks(param)) {
				param = param.substring(1, param.length() - 1);
			}
			paramClassList.add(String.class);
			parsedParams.add(param);
		}
		return new ParameterEntity(paramClassList, parsedParams);
	}

	private static boolean inDoubleQuotationMarks(String str) {
		if (str.length() < 2) {
			return false;
		}
		return str.startsWith("\"") && str.endsWith("\"");
	}
}
