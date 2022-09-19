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

package org.apache.flink.connector.jdbc.catalog;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test for {@link MySQLURL}.
 */
public class MySQLURLTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testEquals() {
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build(),
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build()
		);

		assertNotEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build(),
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db1").build()
		);

		assertNotEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build(),
			null
		);

		MySQLURL url = new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build();
		assertEquals(url, url);
	}

	@Test
	public void testHashcode() {
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build().hashCode(),
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build().hashCode()
		);

		assertNotEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build().hashCode(),
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db1").build().hashCode()
		);
	}

	@Test
	public void testBuilderIllegalArgumentException1() {
		exception.expect(IllegalArgumentException.class);
		new MySQLURL.Builder("").build();
	}

	@Test
	public void testBuilderIllegalArgumentException2() {
		exception.expect(IllegalArgumentException.class);
		new MySQLURL.Builder("jdbc:postgresql:").build();
	}

	@Test
	public void testBuilderIllegalArgumentException3() {
		exception.expect(IllegalArgumentException.class);
		new MySQLURL.Builder("jdbc:postgresql://").build();
	}

	@Test
	public void testBuilderIllegalArgumentException4() {
		exception.expect(IllegalArgumentException.class);
		new MySQLURL.Builder("jdbc:mysql:///", true).build();
	}

	@Test
	public void testBuilderIllegalArgumentException5() {
		exception.expect(IllegalArgumentException.class);
		new MySQLURL.Builder("jdbc:mysql:///db?db_consul_w=toutiao.mysql.db_write", true)
			.build();
	}

	@Test
	public void testBuilderIllegalArgumentException6() {
		exception.expect(IllegalArgumentException.class);
		new MySQLURL.Builder("jdbc:mysql:///db?db_consul_r=toutiao.mysql.db_read", true)
			.build();
	}

	@Test
	public void testGetUrl() {
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build().getUrl(null),
			"jdbc:mysql://localhost:3306/?useCursorFetch=true"
		);

		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306").build().getUrl("db"),
			"jdbc:mysql://localhost:3306/db?useCursorFetch=true"
		);
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/").build().getUrl("db"),
			"jdbc:mysql://localhost:3306/db?useCursorFetch=true"
		);
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://").build().getUrl(""),
			"jdbc:mysql:///?useCursorFetch=true"
		);
		assertEquals(
			new MySQLURL
				.Builder("jdbc:mysql://localhost:3306" +
				"?characterEncoding=utf8&useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true")
				.build()
				.getUrl("db"),
			"jdbc:mysql://localhost:3306/db" +
				"?rewriteBatchedStatements=true&serverTimezone=UTC" +
				"&characterEncoding=utf8&useCursorFetch=true&useSSL=false"
		);
	}

	@Test
	public void testGetDefaultUrl() {
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build().getDefaultUrl(),
			"jdbc:mysql://localhost:3306/db?useCursorFetch=true"
		);
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db_1").setDefaultDb("db").build().getDefaultUrl(),
			"jdbc:mysql://localhost:3306/db?useCursorFetch=true"
		);
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db?").build().getDefaultUrl(),
			"jdbc:mysql://localhost:3306/db?useCursorFetch=true"
		);
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db?k_1=v_1&k_2=v_2").build().getDefaultUrl(),
			"jdbc:mysql://localhost:3306/db?k_1=v_1&useCursorFetch=true&k_2=v_2"
		);
	}

	@Test
	public void testToString() {
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build().getDefaultUrl(),
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").build().toString()
		);
	}

	@Test
	public void testSetReadMode() {
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql:///db?db_consul=toutiao.mysql.db_name_write")
				.build()
				.getDefaultUrl(),
			"jdbc:mysql:///db?db_consul=toutiao.mysql.db_name_write&useCursorFetch=true"
		);
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql:///db?db_consul=toutiao.mysql.db_name_write")
				.setReadSource()
				.build()
				.getDefaultUrl(),
			"jdbc:mysql:///db?db_consul=toutiao.mysql.db_name_write&useCursorFetch=true"
		);
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql:///db?db_consul=toutiao.mysql.db_name_write")
				.setReadReplica()
				.build()
				.getDefaultUrl(),
			"jdbc:mysql:///db?db_consul=toutiao.mysql.db_name_write&useCursorFetch=true"
		);

		assertEquals(
			new MySQLURL.Builder("jdbc:mysql:///db" +
				"?db_consul_w=toutiao.mysql.db_name_write&db_consul_r=toutiao.mysql.db_name_read")
				.setReadSource()
				.build()
				.getDefaultUrl(),
			"jdbc:mysql:///db" +
				"?db_consul_w=toutiao.mysql.db_name_write" +
				"&db_consul=toutiao.mysql.db_name_write&db_consul_r=toutiao.mysql.db_name_read&useCursorFetch=true"
		);
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql:///db" +
				"?db_consul_w=toutiao.mysql.db_name_write&db_consul_r=toutiao.mysql.db_name_read")
				.setReadReplica()
				.build()
				.getDefaultUrl(),
			"jdbc:mysql:///db" +
				"?db_consul_w=toutiao.mysql.db_name_write" +
				"&db_consul=toutiao.mysql.db_name_read&db_consul_r=toutiao.mysql.db_name_read&useCursorFetch=true"
		);
	}

	@Test
	public void testGetBaseUrl() {
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db?useSSL=false").build().getBaseUrl(),
			"jdbc:mysql://localhost:3306/"
		);
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://?useSSL=false").build().getBaseUrl(),
			"jdbc:mysql:///"
		);
	}

	@Test
	public void testSetUseCursorFetch() {
		assertEquals(
			new MySQLURL.Builder("jdbc:mysql://localhost:3306/db").setUseCursorFetch().build().toString(),
			"jdbc:mysql://localhost:3306/db?useCursorFetch=true"
		);
	}

	@Test
	public void testFromJobParameters() {
		assertEquals(
			MySQLURL.fromJobParameters("catalog_name", "jdbc:postgresql://", Optional.empty()),
			"jdbc:postgresql://"
		);

		Map<String, String> parametersMap = new HashMap<String, String>() {{
			put("catalog_conf", "{\"catalog_name\":\"READ_SOURCE\"}");
		}};

		assertEquals(
			MySQLURL.fromJobParameters("catalog_name", "jdbc:mysql:///db" +
					"?db_consul_w=toutiao.mysql.db_write&db_consul_r=toutiao.mysql.db_read",
				Optional.of(parametersMap)),
			"jdbc:mysql:///db" +
				"?db_consul_w=toutiao.mysql.db_write" +
				"&db_consul=toutiao.mysql.db_write" +
				"&db_consul_r=toutiao.mysql.db_read" +
				"&useCursorFetch=true"
		);

		parametersMap = new HashMap<String, String>() {{
			put("catalog_conf", "");
		}};

		assertEquals(
			MySQLURL.fromJobParameters("catalog_name", "jdbc:mysql:///db" +
					"?db_consul_w=toutiao.mysql.db_write&db_consul_r=toutiao.mysql.db_read",
				Optional.of(parametersMap)),
			"jdbc:mysql:///db" +
				"?db_consul_w=toutiao.mysql.db_write" +
				"&db_consul=toutiao.mysql.db_read" +
				"&db_consul_r=toutiao.mysql.db_read" +
				"&useCursorFetch=true"
		);

		parametersMap = new HashMap<String, String>() {{
			put("catalog_conf", "{\"catalog_name\":\"READ_REPLICA\"}");
		}};

		assertEquals(
			MySQLURL.fromJobParameters("catalog_name", "jdbc:mysql:///db" +
					"?db_consul_w=toutiao.mysql.db_write&db_consul_r=toutiao.mysql.db_read",
				Optional.of(parametersMap)),
			"jdbc:mysql:///db" +
				"?db_consul_w=toutiao.mysql.db_write" +
				"&db_consul=toutiao.mysql.db_read" +
				"&db_consul_r=toutiao.mysql.db_read" +
				"&useCursorFetch=true"
		);

		parametersMap = new HashMap<String, String>();

		assertEquals(
			MySQLURL.fromJobParameters("catalog_name", "jdbc:mysql:///db" +
					"?db_consul_w=toutiao.mysql.db_write&db_consul_r=toutiao.mysql.db_read",
				Optional.of(parametersMap)),
			"jdbc:mysql:///db" +
				"?db_consul_w=toutiao.mysql.db_write" +
				"&db_consul=toutiao.mysql.db_read" +
				"&db_consul_r=toutiao.mysql.db_read" +
				"&useCursorFetch=true"
		);
	}
}
