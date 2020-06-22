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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link org.apache.flink.table.utils.SqlSplitUtils}.
 */
public class SqlSplitUtilsTest {

	@Test
	public void testGetSqlList() {
		String sql = "create table t1 (id int, name varchar);\n select id from t1; -- comment";
		List<String> sqlList = SqlSplitUtils.getSqlList(sql);
		ArrayList<String> expectedResult = new ArrayList<>();
		String subSql1 = "create table t1 (id int, name varchar)";
		int whitespaceNum = (subSql1 + ";").length();
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < whitespaceNum; i++) {
			sb.append(" ");
		}
		sb.append("\n select id from t1");
		String subSql2 = sb.toString();
		expectedResult.add(subSql1);
		expectedResult.add(subSql2);
		assertEquals(expectedResult, sqlList);
	}

	@Test
	public void testIsComment() {
		String comment1 = " -- This is a comment \n --comment 2 \n /* comment 3 */";
		String comment2 = " /* -- This is a comment \n comment2 */";
		String statement1 = " -- This is a comment \n -- comment 2 \n select * from tableA";
		String statement2 = " /* This is a comment */ \n select * from tableA";
		String statement3 = "select * from tableA where a = '--' and b = '/* */'";
		assertTrue(SqlSplitUtils.isComment(comment1));
		assertTrue(SqlSplitUtils.isComment(comment2));
		assertFalse(SqlSplitUtils.isComment(statement1));
		assertFalse(SqlSplitUtils.isComment(statement2));
		assertFalse(SqlSplitUtils.isComment(statement3));
	}
}
