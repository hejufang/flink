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

package org.apache.flink.sql.parser.analyst;

import org.apache.flink.sql.parser.analyst.RefWithDependency.TableRefWithDep;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TableColumnVisitorTest.
 */
public class TableColumnVisitorTest {
	private static final String SQL_SOURCE_TABLE_3 = "create table source_table3(s1 int, s2 int, s3 int)";
	private static final String SQL_SINK_TABLE_3 = "create table sink_table3(sk1 int, sk2 int, sk3 int)";
	private static final String SQL_SOURCE_TABLE_3_WATERMARK =
		"create table source_table_watermark(s1 int, s2 int, s3 timestamp, watermark for s3 as s3 - INTERVAL '5' HOUR)";
	private static final String SQL_SOURCE_A3_PROC_TIME =
		"create table source_table_proc(s1 int, s2 int, s3 as PROCTIME())";

	@Test
	public void testSelectInsert() {
		String[] sqlList = {
			SQL_SOURCE_TABLE_3, SQL_SINK_TABLE_3,
			"insert into sink_table3 select * from source_table3"
		};
		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(new TableColumn("source_table3", "s1")));
		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(new TableColumn("source_table3", "s2")));
		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(new TableColumn("source_table3", "s3")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testSelectSpecificColumns() {
		String[] sqlList = {
			SQL_SOURCE_TABLE_3, SQL_SINK_TABLE_3,
			"insert into sink_table3 select s1 as sk2, s3 as sk3, s2 from source_table3"
		};
		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(new TableColumn("source_table3", "s1")));
		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(new TableColumn("source_table3", "s3")));
		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(new TableColumn("source_table3", "s2")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testBinaryOperator() {
		String[] sqlList = {
			SQL_SOURCE_TABLE_3, SQL_SINK_TABLE_3,
			"insert into sink_table3 " +
				"select s1 + s2, s3 + s2, case when s3 = 0 then s1 else 0 end from source_table3"
		};
		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(
				new TableColumn("source_table3", "s1"),
				new TableColumn("source_table3", "s2")));
		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(
				new TableColumn("source_table3", "s2"),
				new TableColumn("source_table3", "s3")));
		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(
				new TableColumn("source_table3", "s3"),
				new TableColumn("source_table3", "s1")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testJoin() {
		String[] sqlList = {
			SQL_SOURCE_TABLE_3, SQL_SOURCE_TABLE_3_WATERMARK, SQL_SINK_TABLE_3,
			"insert into sink_table3 " +
				"select r.s1, r.s2 + l.s2, l.s3 from " +
				" source_table3 l left join source_table_watermark r on l.s1 = r.s1"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(new TableColumn("source_table_watermark", "s1")));
		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(
				new TableColumn("source_table3", "s2"),
				new TableColumn("source_table_watermark", "s2")));
		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(new TableColumn("source_table3", "s3")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testLookupJoin() {
		String[] sqlList = {
			SQL_SOURCE_TABLE_3, SQL_SOURCE_A3_PROC_TIME, SQL_SINK_TABLE_3,
			"insert into sink_table3 " +
				"select r.s1, r.s2 + l.s2, l.s3 from " +
				" source_table3 l left join source_table_proc for SYSTEM_TIME as of l.proc r on l.s1 = r.s1"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(new TableColumn("source_table_proc", "s1")));
		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(new TableColumn("source_table3", "s2"),
				new TableColumn("source_table_proc", "s2")));
		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(new TableColumn("source_table3", "s3")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testCreateView() {
		String[] sqlList = {
			SQL_SOURCE_TABLE_3, SQL_SOURCE_TABLE_3_WATERMARK, SQL_SINK_TABLE_3,
			"create view v1 as " +
				"select r.s1, r.s2 + l.s2, l.s3 from " +
				" source_table3 l left join source_table_watermark r on l.s1 = r.s1",
			"insert into sink_table3 select * from v1"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(new TableColumn("source_table_watermark", "s1")));
		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(
				new TableColumn("source_table3", "s2"),
				new TableColumn("source_table_watermark", "s2")));
		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(new TableColumn("source_table3", "s3")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testUnionAll() {
		String[] sqlList = {
			SQL_SOURCE_TABLE_3, SQL_SOURCE_A3_PROC_TIME, SQL_SINK_TABLE_3, SQL_SOURCE_TABLE_3_WATERMARK,
			"create view v1 as " +
				"select s1, s2, s3 from source_table3 union all " +
				"select s1, s2, 1 as s3 from source_table_proc union all " +
				"select s1, s2, 1 as s3 from source_table_watermark ",
			"insert into sink_table3 select s1, s2, s3 from v1"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(
				new TableColumn("source_table_proc", "s1"),
				new TableColumn("source_table_watermark", "s1"),
				new TableColumn("source_table3", "s1")));
		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(
				new TableColumn("source_table3", "s2"),
				new TableColumn("source_table_watermark", "s2"),
				new TableColumn("source_table_proc", "s2")));
		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(new TableColumn("source_table3", "s3")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testCommonJoin() {
		String[] sqlList = {
			SQL_SOURCE_TABLE_3, SQL_SINK_TABLE_3,
			"insert into sink_table3 " +
				"select s1, c2, c3 from source_table3, lateral table(func(s2)) as T(c2, c3)"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(
				new TableColumn("source_table3", "s1")));

		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(
				new TableColumn("source_table3", "s2")));

		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(
				new TableColumn("source_table3", "s2")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testRow() {
		String[] sqlList = {
			"create table row_table(s1 Row<r1a int, r1b Row<r1b1 int>>, s2 Row<r2a int>)",
			SQL_SINK_TABLE_3,
			"insert into sink_table3 " +
				"select r1a + r1b1, r1b1 + r2a, r2a + r1b1 from row_table"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(
				new TableColumn("row_table", "s1")));

		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(
				new TableColumn("row_table", "s1"),
				new TableColumn("row_table", "s2")));

		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(
				new TableColumn("row_table", "s1"),
				new TableColumn("row_table", "s2")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testDefaultCatalog() {
		String[] sqlList = {
			"create table row_table(s1 Row<r1a int, r1b Row<r1b1 int>>, s2 Row<r2a int>)",
			SQL_SINK_TABLE_3,
			"insert into sink_table3 " +
				"select r1a + r1b1, r1b1 + r2a, r2a + r1b1 from default_catalog.default_database.row_table"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(
				new TableColumn("row_table", "s1")));

		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(
				new TableColumn("row_table", "s1"),
				new TableColumn("row_table", "s2")));

		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(
				new TableColumn("row_table", "s1"),
				new TableColumn("row_table", "s2")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testWithBinlog() {
		String[] sqlList = {
			"create table row_table(id Row<before_value int, after_value int, after_updated boolean>) " +
				"with ('format' = 'binlog')",
			SQL_SINK_TABLE_3,
			"insert into sink_table3 " +
				"select id.after_value, " +
				"cast(binlog_body.rowdatas[2].after_image[1].`value` as int)," +
				"cast(binlog_header.uint32 as int) from row_table"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(
				new TableColumn("row_table", "id")));

		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(
				new TableColumn("row_table", "binlog_body")));

		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(
				new TableColumn("row_table", "binlog_header")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testWithBinlogDrc() {
		String[] sqlList = {
			"create table row_table(id Row<before_value int, after_value int, after_updated boolean>) " +
				"with ('format' = 'pb_binlog_drc')",
			SQL_SINK_TABLE_3,
			"insert into sink_table3 " +
				"select id.after_value, " +
				"cast(body.rowdatas[2].after_image[1].`value` as int)," +
				"cast(header.uint32 as int) from row_table"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(
				new TableColumn("row_table", "id")));

		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(
				new TableColumn("row_table", "body")));

		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(
				new TableColumn("row_table", "header")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testWithKafkaBinlog() {
		String[] sqlList = {
			"CREATE TABLE binlog_table WITH (\n" +
				"'connector.type' = 'kafka',\n" +
				"'connector.version' = '0.10',\n" +
				"'format.type' = 'pb_binlog')",
			"CREATE TABLE `sink` (\n" +
				"`time` BIGINT,\n" +
				"`dbName` VARCHAR,\n" +
				"`tableName` VARCHAR,\n" +
				"`sql` VARCHAR,\n" +
				"`name` VARCHAR,\n" +
				"`val` VARCHAR)",
			"INSERT INTO `sink`\n" +
				"SELECT\n" +
				"	`header`.`executeTime`,\n" +
				"	`schemaName`,\n" +
				"	`tableName`,\n" +
				"	`sql`,\n" +
				"	`rowDatas`.`afterColumns`.`name`,\n" +
				"	`rowDatas`.`afterColumns`.`value`\n" +
				"FROM `binlog_table`"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink", "time"),
			newTableColumnSet(
				new TableColumn("binlog_table", "header")));

		expects.put(new TableColumn("sink", "dbName"),
			newTableColumnSet(
				new TableColumn("binlog_table", "header")));

		expects.put(new TableColumn("sink", "tableName"),
			newTableColumnSet(
				new TableColumn("binlog_table", "header")));

		expects.put(new TableColumn("sink", "sql"),
			newTableColumnSet(
				new TableColumn("binlog_table", "RowChange")));

		expects.put(new TableColumn("sink", "name"),
			newTableColumnSet(
				new TableColumn("binlog_table", "RowChange")));

		expects.put(new TableColumn("sink", "val"),
			newTableColumnSet(
				new TableColumn("binlog_table", "RowChange")));

		analyseSql(sqlList, expects, TableColumnVisitor.FlinkPropertyVersion.FLINK_1_9);
	}

	@Test
	public void testWithPb() {
		final String pbTable = "test_pb";
		final TableRefWithDep tableRefWithDep = new TableRefWithDep(pbTable);
		tableRefWithDep.addColumnName("col1");
		tableRefWithDep.addColumnName("col2");
		tableRefWithDep.addColumnName("col3");
		SimpleCatalog simpleCatalog = name -> pbTable.equals(name) ? tableRefWithDep : null;

		String[] sqlList = {
			"create table test_pb " +
				"with ('format' = 'pb')",
			SQL_SINK_TABLE_3,
			"insert into sink_table3 " +
				"select col1, col2, col3 from test_pb"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table3", "sk1"),
			newTableColumnSet(
				new TableColumn(pbTable, "col1")));

		expects.put(new TableColumn("sink_table3", "sk2"),
			newTableColumnSet(
				new TableColumn(pbTable, "col2")));

		expects.put(new TableColumn("sink_table3", "sk3"),
			newTableColumnSet(
				new TableColumn(pbTable, "col3")));
		analyseSql(sqlList, expects, simpleCatalog);
	}

	@Test
	public void testMisc() throws Exception {
		String bigSql = "create  table room_info_source(\n" +
			"            room_id BIGINT, --直播间id\n" +
			"            live_start_ts BIGINT, --直播间开始时间\n" +
			"            live_end_ts BIGINT, --直播间结束时间\n" +
			"            fans_num BIGINT, --主播粉丝数\n" +
			"            current_online_ucnt BIGINT, --当前(最新)在线人数\n" +
			"            peak_online_ucnt BIGINT, --最高在线人数\n" +
			"            update_ts BIGINT, --更新时间\n" +
			"            anchor_id BIGINT, --达人id\n" +
			"            anchor_nickname VARCHAR, --达人昵称\n" +
			"            app_id BIGINT, --app_id\n" +
			"            anchor_short_id VARCHAR, -- \n" +
			"            watch_cnt BIGINT, -- 累计观看次数\n" +
			"            watch_ucnt BIGINT, -- 累计观看人数\n" +
			"            calc_column as watch_cnt + watch_ucnt,\n" +
			"            proc as PROCTIME()\n" +
			"        );\n" +
			"\n" +
			"create  table room_stat_final (res_key VARCHAR, res_val VARCHAR);\n" +
			"create  table room_products_abase (room_key varchar, product_ids ARRAY < VARCHAR >);\n" +
			"create  table product_shop_abase (product_id varchar, shop_id VARCHAR);\n" +
			"\n" +
			"create  table es1_lf_sink(\n" +
			"            shop_id bigint,\n" +
			"            content_id bigint,\n" +
			"            product_id bigint,\n" +
			"            live_start_ts bigint,\n" +
			"            live_end_ts bigint,\n" +
			"            app_id BIGINT,\n" +
			"            live_watch_pv bigint,\n" +
			"            live_watch_uv bigint,\n" +
			"            nickname VARCHAR --达人昵称\n" +
			"        );\n" +
			"\n" +
			"create  table es1_hl_sink(\n" +
			"            shop_id bigint,\n" +
			"            content_id bigint,\n" +
			"            product_id bigint,\n" +
			"            live_start_ts bigint,\n" +
			"            live_end_ts bigint,\n" +
			"            app_id BIGINT,\n" +
			"            live_watch_pv bigint,\n" +
			"            live_watch_uv bigint,\n" +
			"            nickname VARCHAR --达人昵称\n" +
			"        );\n" +
			"\n" +
			"create  table es2_lf_sink(\n" +
			"            shop_id bigint,\n" +
			"            room_id bigint,\n" +
			"            start_ts BIGINT, --直播间开始时间\n" +
			"            end_ts BIGINT, --直播间结束时间\n" +
			"            fans_num BIGINT, --主播粉丝数\n" +
			"            now_user_cnt BIGINT, --当前(最新)在线人数\n" +
			"            pcu BIGINT, --最高在线人数\n" +
			"            anchor_id BIGINT, --达人id\n" +
			"            nickname VARCHAR, --达人昵称\n" +
			"            app_id BIGINT, --app_id\n" +
			"            now_user_cnt_final BIGINT, --当前在线人数(关播置为0)\n" +
			"            live_watch_pv bigint,\n" +
			"            live_watch_uv bigint,\n" +
			"            short_id VARCHAR\n" +
			"        );\n" +
			"\n" +
			"create  table es2_hl_sink(\n" +
			"            shop_id bigint,\n" +
			"            room_id bigint,\n" +
			"            start_ts BIGINT, --直播间开始时间\n" +
			"            end_ts BIGINT, --直播间结束时间\n" +
			"            fans_num BIGINT, --主播粉丝数\n" +
			"            now_user_cnt BIGINT, --当前(最新)在线人数\n" +
			"            pcu BIGINT, --最高在线人数\n" +
			"            anchor_id BIGINT, --达人id\n" +
			"            nickname VARCHAR, --达人昵称\n" +
			"            app_id BIGINT, --app_id\n" +
			"            now_user_cnt_final BIGINT, --当前在线人数(关播置为0)\n" +
			"            live_watch_pv bigint,\n" +
			"            live_watch_uv bigint,\n" +
			"            short_id VARCHAR\n" +
			"        );\n" +
			"\n" +
			"-- 直播间粒度的es\n" +
			"create  table es3_lf_sink(\n" +
			"            content_id BIGINT, --直播间id\n" +
			"            live_start_ts BIGINT, --直播间开始时间\n" +
			"            live_end_ts BIGINT, --直播间结束时间\n" +
			"            fans_num BIGINT, --主播粉丝数\n" +
			"            now_user_cnt BIGINT, --当前(最新)在线人数\n" +
			"            peak_online_ucnt BIGINT, --最高在线人数\n" +
			"            anchor_id BIGINT, --达人id\n" +
			"            nickname VARCHAR, --达人昵称\n" +
			"            app_id BIGINT, --app_id\n" +
			"            live_watch_pv bigint,\n" +
			"            live_watch_uv bigint,\n" +
			"            now_user_cnt_final BIGINT, --当前在线人数(关播置为0)\n" +
			"            is_ecom_room INT, -- 是否是历史上的电商直播间\n" +
			"            now_is_ecom_room INT -- 当前是否是电商直播间\n" +
			"        );\n" +
			"\n" +
			"-- 直播间粒度的es\n" +
			"create  table es3_hl_sink(\n" +
			"            content_id BIGINT, --直播间id\n" +
			"            live_start_ts BIGINT, --直播间开始时间\n" +
			"            live_end_ts BIGINT, --直播间结束时间\n" +
			"            fans_num BIGINT, --主播粉丝数\n" +
			"            now_user_cnt BIGINT, --当前(最新)在线人数\n" +
			"            peak_online_ucnt BIGINT, --最高在线人数\n" +
			"            anchor_id BIGINT, --达人id\n" +
			"            nickname VARCHAR, --达人昵称\n" +
			"            app_id BIGINT, --app_id\n" +
			"            live_watch_pv bigint,\n" +
			"            live_watch_uv bigint,\n" +
			"            now_user_cnt_final BIGINT, --当前在线人数(关播置为0)\n" +
			"            is_ecom_room INT, -- 是否是历史上的电商直播间\n" +
			"            now_is_ecom_room INT -- 当前是否是电商直播间\n" +
			"        );\n" +
			"\n" +
			"create table print_sink (\n" +
			"        room_id BIGINT, --直播间id\n" +
			"        live_start_ts BIGINT, --直播间开始时间\n" +
			"        live_end_ts BIGINT, --直播间结束时间\n" +
			"        fans_num BIGINT --主播粉丝数\n" +
			");\n" +
			"\n" +
			"create  view filter_room_info_source as\n" +
			"select  *\n" +
			"from    (\n" +
			"            SELECT  *,\n" +
			"                    ROW_NUMBER() over (\n" +
			"                        PARTITION BY\n" +
			"                                room_id\n" +
			"                        ORDER BY\n" +
			"                                update_ts DESC\n" +
			"                    ) as rn\n" +
			"            from    room_info_source\n" +
			"        ) p\n" +
			"where   rn = 1;\n" +
			"\n" +
			"create  view es3_view as\n" +
			"select  room_id as content_id, --直播间id\n" +
			"        live_start_ts, --直播间开始时间\n" +
			"        live_end_ts, --直播间结束时间\n" +
			"        fans_num, --主播粉丝数\n" +
			"        current_online_ucnt as now_user_cnt, --当前(最新)在线人数\n" +
			"        peak_online_ucnt, --最高在线人数\n" +
			"        anchor_id, --达人id\n" +
			"        anchor_nickname as nickname, --达人昵称\n" +
			"        app_id, --app_id\n" +
			"        watch_cnt as live_watch_pv, --累计在线次数\n" +
			"        watch_ucnt as live_watch_uv, --累计在线人数\n" +
			"        case when live_end_ts = 0 then current_online_ucnt\n" +
			"             else 0\n" +
			"        end as now_user_cnt_final, --当前在线人数(关播置为0)\n" +
			"        case when room_stat_final.res_key is not null then 1\n" +
			"             else 0\n" +
			"        end as is_ecom_room,\n" +
			"        case when room_stat_final.res_val = 'TRUE' then 1 else 0 end as now_is_ecom_room\n" +
			"from    filter_room_info_source\n" +
			"left join\n" +
			"        room_stat_final for SYSTEM_TIME as of filter_room_info_source.proc as room_stat_final\n" +
			"on      concat('ecom_live_room_now_room_id:', CAST(room_id AS VARCHAR)) = room_stat_final.res_key;\n" +
			"\n" +
			"insert into es3_lf_sink\n" +
			"select  content_id,\n" +
			"        live_start_ts,\n" +
			"        live_end_ts,\n" +
			"        fans_num,\n" +
			"        now_user_cnt,\n" +
			"        peak_online_ucnt,\n" +
			"        anchor_id,\n" +
			"        nickname,\n" +
			"        app_id,\n" +
			"        live_watch_pv,\n" +
			"        live_watch_uv,\n" +
			"        now_user_cnt_final,\n" +
			"        is_ecom_room,\n" +
			"        now_is_ecom_room\n" +
			"from    es3_view;\n" +
			"\n" +
			"insert into es3_hl_sink\n" +
			"select  content_id,\n" +
			"        live_start_ts,\n" +
			"        live_end_ts,\n" +
			"        fans_num,\n" +
			"        now_user_cnt,\n" +
			"        peak_online_ucnt,\n" +
			"        anchor_id,\n" +
			"        nickname,\n" +
			"        app_id,\n" +
			"        live_watch_pv,\n" +
			"        live_watch_uv,\n" +
			"        now_user_cnt_final,\n" +
			"        is_ecom_room,\n" +
			"        now_is_ecom_room\n" +
			"from    es3_view;\n" +
			"\n" +
			"create  view mock_view_zyf as\n" +
			"select  room_id,\n" +
			"        live_start_ts,\n" +
			"        live_end_ts\n" +
			"from    (\n" +
			"            SELECT  v.*,\n" +
			"                    P.*\n" +
			"            from    (\n" +
			"                        select  s.live_start_ts as live_end_ts,\n" +
			"                                s.live_end_ts as live_start_ts,\n" +
			"                                s.proc,\n" +
			"                                room_id,\n" +
			"                                r.product_ids\n" +
			"                        from    room_info_source s\n" +
			"                        join    room_products_abase for SYSTEM_TIME as of s.proc as r\n" +
			"                        on      concat(\n" +
			"                                    'room_id_2_product_list:',\n" +
			"                                    CAST(room_id AS VARCHAR)\n" +
			"                                ) = r.room_key\n" +
			"                                group by live_start_ts,live_end_ts,proc,room_id,product_ids\n" +
			"                    ) v,\n" +
			"                    unnest(v.product_ids) as P(product_id)\n" +
			"        ) q\n" +
			"join    product_shop_abase for SYSTEM_TIME as of q.proc as w\n" +
			"on      concat('ecom_dim:product_id:', q.product_id) = w.product_id\n" +
			"where   q.product_id is not null\n" +
			"and     w.shop_id is not null;\n" +
			"\n" +
			"create  view room_shop_product_split_info as\n" +
			"select  room_id,\n" +
			"        live_start_ts,\n" +
			"        live_end_ts,\n" +
			"        fans_num + current_online_ucnt as fans_num,\n" +
			"        current_online_ucnt,\n" +
			"        peak_online_ucnt,\n" +
			"        update_ts,\n" +
			"        q.product_id,\n" +
			"        w.shop_id,\n" +
			"        proc,\n" +
			"        anchor_id, --达人id\n" +
			"        anchor_nickname as nickname, --达人昵称\n" +
			"        app_id, --app_id\n" +
			"        anchor_short_id,\n" +
			"        watch_cnt as live_watch_pv, --累计在线次数\n" +
			"        watch_ucnt as live_watch_uv --累计在线人数\n" +
			"from    (\n" +
			"            SELECT  v.*,\n" +
			"                    P.product_id\n" +
			"            from    (\n" +
			"                        select  s.*,\n" +
			"                                r.product_ids\n" +
			"                        from    filter_room_info_source s\n" +
			"                        join    room_products_abase for SYSTEM_TIME as of s.proc as r\n" +
			"                        on      concat(\n" +
			"                                    'room_id_2_product_list:',\n" +
			"                                    CAST(room_id AS VARCHAR)\n" +
			"                                ) = r.room_key\n" +
			"                    ) v,\n" +
			"                    unnest(v.product_ids) as P(product_id)\n" +
			"        ) q\n" +
			"join    product_shop_abase for SYSTEM_TIME as of q.proc as w\n" +
			"on      concat('ecom_dim:product_id:', q.product_id) = w.product_id\n" +
			"where   q.product_id is not null\n" +
			"and     w.shop_id is not null;\n" +
			"\n" +
			"insert into print_sink\n" +
			"select room_id,\n" +
			"        live_start_ts,\n" +
			"        live_end_ts,\n" +
			"        0L from mock_view_zyf;\n" +
			"\n" +
			"INSERT INTO es1_lf_sink\n" +
			"select  cast(shop_id as bigint) as shop_id,\n" +
			"        cast(room_id as bigint) as room_id,\n" +
			"        cast(product_id as bigint) as product_id,\n" +
			"        live_start_ts,\n" +
			"        live_end_ts,\n" +
			"        app_id,\n" +
			"        live_watch_pv, --累计在线次数\n" +
			"        live_watch_uv, --累计在线人数\n" +
			"        nickname --达人昵称\n" +
			"from    room_shop_product_split_info\n" +
			"where   shop_id is not null\n" +
			"and     product_id is not null;\n" +
			"\n" +
			"INSERT INTO es1_hl_sink\n" +
			"select  cast(shop_id as bigint) as shop_id,\n" +
			"        cast(room_id as bigint) as room_id,\n" +
			"        cast(product_id as bigint) as product_id,\n" +
			"        live_start_ts,\n" +
			"        live_end_ts,\n" +
			"        app_id,\n" +
			"        live_watch_pv, --累计在线次数\n" +
			"        live_watch_uv, --累计在线人数\n" +
			"        nickname --达人昵称\n" +
			"from    room_shop_product_split_info\n" +
			"where   shop_id is not null\n" +
			"and     product_id is not null;\n" +
			"\n" +
			"INSERT into es2_hl_sink\n" +
			"select  cast(shop_id as bigint) as shop_id,\n" +
			"        cast(room_id as bigint) as room_id,\n" +
			"        live_start_ts,\n" +
			"        live_end_ts,\n" +
			"        fans_num,\n" +
			"        current_online_ucnt,\n" +
			"        peak_online_ucnt,\n" +
			"        anchor_id, --达人id\n" +
			"        nickname, --达人昵称\n" +
			"        app_id, --app_id\n" +
			"        case when live_end_ts = 0 then current_online_ucnt\n" +
			"             else 0\n" +
			"        end as now_user_cnt_final, --当前在线人数(关播置为0)\n" +
			"        live_watch_pv, --累计在线次数\n" +
			"        live_watch_uv, --累计在线人数\n" +
			"        anchor_short_id\n" +
			"from    room_shop_product_split_info\n" +
			"where   shop_id is not null;\n" +
			"\n" +
			"INSERT into es2_lf_sink\n" +
			"select  cast(shop_id as bigint) as shop_id,\n" +
			"        cast(room_id as bigint) as room_id,\n" +
			"        live_start_ts,\n" +
			"        live_end_ts,\n" +
			"        fans_num,\n" +
			"        current_online_ucnt,\n" +
			"        peak_online_ucnt,\n" +
			"        anchor_id, --达人id\n" +
			"        nickname, --达人昵称\n" +
			"        app_id, --app_id\n" +
			"        case when live_end_ts = 0 then current_online_ucnt\n" +
			"             else 0\n" +
			"        end as now_user_cnt_final, --当前在线人数(关播置为0)\n" +
			"        live_watch_pv, --累计在线次数\n" +
			"        live_watch_uv, --累计在线人数\n" +
			"        anchor_short_id\n" +
			"from    room_shop_product_split_info\n" +
			"where   shop_id is not null";
		List<SqlNode> sqlNodes = createFlinkParser(bigSql).parseStmtList().getList();
		Map<TableColumn, Set<TableColumn>> result =
			TableColumnVisitor.analyseTableColumnDependency(sqlNodes, null);
		Assert.assertEquals(result.get(new TableColumn("es1_lf_sink", "product_id")),
			newTableColumnSet(new TableColumn("room_products_abase", "product_ids")));
	}

	@Test
	public void testWithUnion() {
		String[] sqlList = {
			"create table row_table1(s1 Row<r1a int, r1b Row<r1b1 int>>, s2 Row<r2a int>, s3 int)",
			"create table row_table2(s1 Row<r1a int, r1b Row<r1b1 int>>, s2 Row<r2a int>, s3 int)",
			"create view union_view as " +
				"select * from row_table1 union all select * from row_table2",
			"create table sink_table4(sk1 int, sk2 int, sk3 int, sk4 int)",
			"insert into sink_table4 " +
				"select r1a, r1b1, r2a, s3 from union_view"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table4", "sk1"),
			newTableColumnSet(
				new TableColumn("row_table1", "s1"),
				new TableColumn("row_table2", "s1")));

		expects.put(new TableColumn("sink_table4", "sk2"),
			newTableColumnSet(
				new TableColumn("row_table1", "s1"),
				new TableColumn("row_table2", "s1")));

		expects.put(new TableColumn("sink_table4", "sk3"),
			newTableColumnSet(
				new TableColumn("row_table1", "s2"),
				new TableColumn("row_table2", "s2")));
		expects.put(new TableColumn("sink_table4", "sk4"),
			newTableColumnSet(
				new TableColumn("row_table1", "s3"),
				new TableColumn("row_table2", "s3")));
		analyseSql(sqlList, expects);
	}

	@Test
	public void testNestedQueryWithRow() {
		String[] sqlList = {
			"CREATE TABLE row_table(r1 ROW<i1 INT, r11 ROW<i11 INT>>, r2 ROW<i21 INT>, i INT)",
			"CREATE TABLE sink_table(i1 INT, i2 INT, i3 INT, i4 INT)",
			"CREATE VIEW v AS " +
				"SELECT i1, i11, i21, i FROM (" +
					"SELECT r1.i1, r1.r11, r2.i21, i FROM row_table" +
				")",
			"INSERT INTO sink_table " +
				"SELECT i1, i11, i21, i FROM v"
		};

		Map<TableColumn, Set<TableColumn>> expects = new HashMap<>();
		expects.put(new TableColumn("sink_table", "i1"),
			newTableColumnSet(new TableColumn("row_table", "r1")));
		expects.put(new TableColumn("sink_table", "i2"),
			newTableColumnSet(new TableColumn("row_table", "r1")));
		expects.put(new TableColumn("sink_table", "i3"),
			newTableColumnSet(new TableColumn("row_table", "r2")));
		expects.put(new TableColumn("sink_table", "i4"),
			newTableColumnSet(new TableColumn("row_table", "i")));
		analyseSql(sqlList, expects);
	}

	private void analyseSql(String[] sqlList, Map<TableColumn, Set<TableColumn>> tableColumnListMap) {
		analyseSql(sqlList, tableColumnListMap, null, TableColumnVisitor.FlinkPropertyVersion.FLINK_1_11);
	}

	private void analyseSql(
			String[] sqlList,
			Map<TableColumn, Set<TableColumn>> tableColumnListMap,
			TableColumnVisitor.FlinkPropertyVersion version) {
		analyseSql(sqlList, tableColumnListMap, null, version);
	}

	private void analyseSql(
			String[] sqlList,
			Map<TableColumn, Set<TableColumn>> tableColumnListMap,
			SimpleCatalog simpleCatalog) {
		analyseSql(sqlList, tableColumnListMap, simpleCatalog, TableColumnVisitor.FlinkPropertyVersion.FLINK_1_11);
	}

	private void analyseSql(
			String[] sqlList,
			Map<TableColumn, Set<TableColumn>> tableColumnListMap,
			SimpleCatalog simpleCatalog,
			TableColumnVisitor.FlinkPropertyVersion propertyVersion) {
		analyseSql(String.join(";", sqlList), tableColumnListMap, simpleCatalog, propertyVersion);
	}

	private void analyseSql(
			String sql,
			Map<TableColumn, Set<TableColumn>> tableColumnListMap,
			SimpleCatalog simpleCatalog,
			TableColumnVisitor.FlinkPropertyVersion propertyVersion) {
		try {
			List<SqlNode> sqlNodes = createFlinkParser(sql).parseStmtList().getList();
			Map<TableColumn, Set<TableColumn>> result =
				TableColumnVisitor.analyseTableColumnDependency(sqlNodes, simpleCatalog, propertyVersion);
			Assert.assertEquals(tableColumnListMap, result);
		} catch (SqlParseException e) {
			throw new RuntimeException(e);
		}
	}

	private Set<TableColumn> newTableColumnSet(TableColumn... tableColumns) {
		return new HashSet<>(Arrays.asList(tableColumns));
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
