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

package org.apache.flink.sql.parser.utils;

import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Tests for {@link TableExtractor}.
 */
public class TableExtractorTest {

	@Test
	public void testExtractTablesInSubQuery() {
		String sql = "select * from (select * from kafka.test.source1) as t";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				Collections.singleton("kafka.test.source1"),
				Collections.emptySet(),
				Collections.emptySet());
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesInInsert() {
		String sql = "insert into sink1 select * from source1";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				Collections.singleton("source1"),
				Collections.singleton("sink1"),
				Collections.emptySet());
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesWithOptions() {
		String sql = "insert into sink1 /*+ OPTIONS('parallelism'='10') */ " +
			" select * from source1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */" +
			" left join dim_table /*+ OPTIONS('lookup.cache.max-rows'='100') */ FOR SYSTEM_TIME" +
			" AS OF source.proc on source.article_id = D.id";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				Collections.singleton("source1"),
				Collections.singleton("sink1"),
				Collections.singleton("dim_table"));
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesInViews() {
		String sql = "create view view1 as select * from source1; \n" +
			"insert into sink1 select * from view1";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				Collections.singleton("source1"),
				Collections.singleton("sink1"),
				Collections.emptySet());
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesInJoin() {
		String sql = "insert into sink1 select * from source1 " +
			"left join source2 on source1.id = source2.id";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				new HashSet<>(Arrays.asList("source1", "source2")),
				Collections.singleton("sink1"),
				Collections.emptySet());
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesWithLikeTable() {
		String sql = "create table source (a int) with ('connector'='test');" +
			"create table like1 with('parallelism'='10') like source ;" +
			"create table like2 with('connector'='rocketmq') like like1;" +
			"select * from like1 left join like2 on like1.id = like2.id";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				new HashSet<>(Arrays.asList("like1", "like2")),
				Collections.emptySet(),
				Collections.emptySet());
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesInJoinWithDimensionTable() {
		String sql = "create table like_table like dim_table;" +
			"select * from source left join like_table FOR SYSTEM_TIME " +
			"AS OF source.proc on source.article_id = D.id";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				Collections.singleton("source"),
				Collections.emptySet(),
				Collections.singleton("like_table"));
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesInWhere() {
		String sql = "select * from source1 where id in (select id from source2)";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				new HashSet<>(Arrays.asList("source1", "source2")),
				Collections.emptySet(),
				Collections.emptySet());
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesInSelectList() {
		String sql = "select if(id in (select id from source2), 0, 1) from source1";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				new HashSet<>(Arrays.asList("source1", "source2")),
				Collections.emptySet(),
				Collections.emptySet());
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesInWith() {
		String sql = "with a as (select id from source1), b as (select id from source2) " +
			"select * from a join b where a.id = b.id";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				new HashSet<>(Arrays.asList("source1", "source2")),
				Collections.emptySet(),
				Collections.emptySet());
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesWithCEP() {
		String sql = "create table ies_antispam_aweme_collect(\n" +
			"    milli_timestamp bigint,\n" +
			"    uid bigint,\n" +
			"    action string,\n" +
			"    event_ts timestamp(3),\n" +
			"    WATERMARK for event_ts AS event_ts - INTERVAL '1' MINUTE\n" +
			")with(\n" +
			"    'connector'='kafka-0.10',\n" +
			"    'topic'='ies_antispam_aweme_collect',\n" +
			"    'scan.startup.mode'='latest-offset',\n" +
			"    'properties.group.id'='dws_douyin_api_sequence_test',\n" +
			"    'properties.cluster'='bmq_security_riskctrl',\n" +
			"    'format'='json'\n" +
			");\n" +
			"\n" +
			"create table mysink(\n" +
			"    uid bigint,\n" +
			"    act1 string,\n" +
			"    act2 string\n" +
			")with(\n" +
			"    'connector'='print'\n" +
			");\n" +
			"\n" +
			"insert into mysink\n" +
			"select\n" +
			"    T.uid,T.act1,T.act2\n" +
			"from\n" +
			"    ies_antispam_aweme_collect\n" +
			"match_recognize(\n" +
			"    partition by uid\n" +
			"    order by event_ts\n" +
			"    measures\n" +
			"        A.action as act1,\n" +
			"        B.action as act2\n" +
			"    pattern (A B)\n" +
			"    define\n" +
			"        A as action='act1',\n" +
			"        B as action='act2'\n" +
			") T";

		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				new HashSet<>(Arrays.asList("ies_antispam_aweme_collect")),
				new HashSet<>(Arrays.asList("mysink")),
				Collections.emptySet());
		extractTables(sql, expectedResult);
	}

	@Test
	public void testExtractTablesWithCEPAndView() {
		String sql =
			"CREATE  TABLE dwd_byte_rtc_media_stats_audio_send (\n" +
			"            ts              AS LONG_TO_TIMESTAMP(COALESCE(event_time, 0)),\n" +
			"            WATERMARK       FOR ts AS (ts - INTERVAL '8' MINUTE),\n" +
			"            device_id       VARCHAR,\n" +
			"            encinputmute    INT,\n" +
			"            audio_level     DOUBLE,\n" +
			"            down_stream_num INT,\n" +
			"            rtc_session_id  VARCHAR,\n" +
			"            event_time      BIGINT,\n" +
			"            dw_is_valid     INT\n" +
			"        ) WITH ();\n" +
			"CREATE  TABLE dwd_byte_rtc_event (\n" +
			"            ts           AS LONG_TO_TIMESTAMP(COALESCE(event_time, 0)),\n" +
			"            WATERMARK    FOR ts AS (ts - INTERVAL '8' MINUTE),\n" +
			"            device_id    VARCHAR,\n" +
			"            event_key    VARCHAR,\n" +
			"            sdk_api_name VARCHAR,\n" +
			"            message      VARCHAR,\n" +
			"            event_time   BIGINT,\n" +
			"            dw_is_valid  INT\n" +
			"        ) WITH ();\n" +
			"CREATE  VIEW union_data AS (\n" +
			"            SELECT  device_id,\n" +
			"                    rtc_session_id,\n" +
			"                    encinputmute,\n" +
			"                    audio_level,\n" +
			"                    down_stream_num,\n" +
			"                    CAST(NULL AS VARCHAR) AS event_key,\n" +
			"                    CAST(NULL AS VARCHAR) AS sdk_api_name,\n" +
			"                    CAST(NULL AS VARCHAR) AS message,\n" +
			"                    event_time,\n" +
			"                    ts\n" +
			"            FROM    dwd_byte_rtc_media_stats_audio_send\n" +
			"            WHERE   dw_is_valid = 1\n" +
			"            UNION ALL\n" +
			"            SELECT  device_id,\n" +
			"                    CAST(NULL AS VARCHAR) AS rtc_session_id,\n" +
			"                    CAST(NULL AS INT) AS encinputmute,\n" +
			"                    CAST(NULL AS DOUBLE) AS audio_level,\n" +
			"                    CAST(NULL AS INT) AS down_stream_num,\n" +
			"                    event_key,\n" +
			"                    sdk_api_name,\n" +
			"                    message,\n" +
			"                    event_time,\n" +
			"                    ts\n" +
			"            FROM    dwd_byte_rtc_event\n" +
			"                    -- only consider valid data\n" +
			"            WHERE   dw_is_valid = 1\n" +
			"            AND     event_key = 'rtc_sdk_api_call'\n" +
			"            AND     sdk_api_name = 'enableLocalAudio'\n" +
			"            AND     message = '{enable: 1}'\n" +
			"        );\n" +
			"CREATE  TABLE print_sink (event_time BIGINT, rtc_session_id VARCHAR, device_id VARCHAR)\n" +
			"        WITH ('connector' = 'print');\n" +
			"INSERT INTO print_sink\n" +
			"SELECT  event_time,\n" +
			"        rtc_session_id,\n" +
			"        did\n" +
			"FROM    union_data MATCH_RECOGNIZE (\n" +
			"            PARTITION BY\n" +
			"                    device_id\n" +
			"            ORDER BY\n" +
			"                    ts MEASURES FIRST(NO_VOICE.event_time) AS event_time,\n" +
			"                    FIRST(NO_VOICE.rtc_session_id) AS rtc_session_id,\n" +
			"                    FIRST(NO_VOICE.device_id) AS did ONE ROW PER MATCH\n" +
			"            AFTER   MATCH SKIP TO LAST NO_VOICE PATTERN (NO_VOICE + HAS_VOICE) WITHIN INTERVAL '1' HOUR DEFINE NO_VOICE AS down_stream_num > 0\n" +
			"            AND     audio_level = 127.0\n" +
			"            AND     encinputmute = 0,\n" +
			"                    --         START_AUDIO_CAPTURE AS event_key = 'rtc_sdk_api_call'\n" +
			"                    -- AND     sdk_api_name = 'enableLocalAudio'\n" +
			"                    -- AND     message = '{enable: 1}',\n" +
			"                    HAS_VOICE AS NOT (\n" +
			"                        down_stream_num > 0\n" +
			"                        AND audio_level = 127.0\n" +
			"                        AND encinputmute = 0\n" +
			"                    )\n" +
			"        )";
		TableExtractor.ExtractResult expectedResult =
			new TableExtractor.ExtractResult(
				new HashSet<>(Arrays.asList("dwd_byte_rtc_media_stats_audio_send", "dwd_byte_rtc_event")),
				new HashSet<>(Arrays.asList("print_sink")),
				Collections.emptySet());
		extractTables(sql, expectedResult);
	}

	private void extractTables(String sql, TableExtractor.ExtractResult expectedResult) {
		try {
			List<SqlNode> sqlNodes = createFlinkParser(sql).parseStmtList().getList();
			TableExtractor.ExtractResult result = TableExtractor.extractTables(sqlNodes);
			Assert.assertEquals(expectedResult, result);
		} catch (SqlParseException e) {
			throw new RuntimeException(e);
		}
	}

	private SqlParser createFlinkParser(String expr) {
		SqlParser.Config parserConfig = SqlParser.configBuilder()
			.setParserFactory(FlinkSqlParserImpl.FACTORY)
			.setLex(Lex.JAVA)
			.setIdentifierMaxLength(256)
			.build();

		return SqlParser.create(expr, parserConfig);
	}
}
