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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for spliting multiple statements to statement list.
 */
public class SqlSplitUtils {

	private static final Pattern PATTERN_STATEMENT = Pattern.compile("[^\\\\];");

	// Match single line comments, e.g. '-- test table'
	private static final Pattern PATTERN_SINGLE_LINE = Pattern.compile("--.*");

	// Match multiple line comments, e.g. '/* this is a test */'
	private static final Pattern PATTERN_MULTI_LINE = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);

	public static List<String> getSqlList(String context) {
		List<String> sqlList = new ArrayList<>();

		Matcher match = PATTERN_STATEMENT.matcher(context);
		int index = 0;
		while (match.find()) {

			if (isInComment(context, match.start() + 1)
				|| !containsEvenNumberPatterns(context.substring(index, match.start() + 1), '\'')
				|| !containsEvenNumberPatterns(context.substring(index, match.start() + 1), '\"')) {
				continue;
			}

			String str = context.substring(index, match.start() + 1)
				.replaceAll("\\\\;", ";")
				.replaceAll("^;", "");
			String prefix = context.substring(0, index).replaceAll("[^\n]", " ");
			String realStatement = prefix + str;
			sqlList.add(realStatement);
			index = match.start() + 2;
		}

		if (index < context.length() - 1 && context.substring(index).trim().length() != 0) {
			String str = context.substring(index).replaceAll("\\\\;", ";")
				.replaceAll("^;", "");
			String prefix = context.substring(0, index).replaceAll("[^\n]", " ");
			String realStatement = prefix + str;
			sqlList.add(realStatement);
		}
		return sqlList;
	}

	/**
	 * Whether the statement contains even number of patterns.
	 * */
	private static boolean containsEvenNumberPatterns(String statement, char pattern) {
		int count = 0;
		for (int i = 0; i < statement.length(); i++) {
			if (statement.charAt(i) == pattern) {
				count++;
			}
			if (statement.charAt(i) == '\\' && i < statement.length() - 1
				&& statement.charAt(i + 1) == pattern) {
				i++;
			}
		}
		return count % 2 == 0;
	}

	/**
	 * Adjust whether the statement is in a comment closure.
	 *
	 * @param context the whole sql context.
	 * @param index the index of last letter in statement.
	 *
	 * @return whether the statement is in a comment closure.
	 * */
	private static boolean isInComment(String context, int index) {
		Matcher singleMatch = PATTERN_SINGLE_LINE.matcher(context);

		while (singleMatch.find()) {
			int start = singleMatch.start();
			int end = singleMatch.end() - 1;

			if (index > start && index <= end) {
				return true;
			}
		}

		Matcher multiMatch = PATTERN_MULTI_LINE.matcher(context);

		while (multiMatch.find()) {
			int start = multiMatch.start();
			int end = multiMatch.end() - 1;

			if (index > start && index < end) {
				return true;
			}
		}

		return false;
	}
}
