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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.stats.StatisticGeneratorTest;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.planner.utils.CatalogTableStatisticsConverter.convertToTableStats;
import static org.apache.flink.table.planner.utils.TableStatsConverter.AVG_LEN;
import static org.apache.flink.table.planner.utils.TableStatsConverter.MAX_LEN;
import static org.apache.flink.table.planner.utils.TableStatsConverter.MAX_VALUE;
import static org.apache.flink.table.planner.utils.TableStatsConverter.MIN_VALUE;
import static org.apache.flink.table.planner.utils.TableStatsConverter.NDV;
import static org.apache.flink.table.planner.utils.TableStatsConverter.NULL_COUNT;
import static org.apache.flink.table.planner.utils.TableStatsConverter.convertToCatalogColumnStatistics;
import static org.apache.flink.table.planner.utils.TableStatsConverter.convertToCatalogTableStatistics;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link TableStatsConverter}.
 */
public class TableStatsConverterTest {

	@Test
	public void testConvertToCatalogTableStatistics_UNKNOWN() {
		assertEquals(CatalogTableStatistics.UNKNOWN, convertToCatalogTableStatistics(TableStats.UNKNOWN));
	}

	@Test
	public void testConvertToCatalogTableStatistics() {
		CatalogTableStatistics statistics = convertToCatalogTableStatistics(genTableStats(true));
		assertEquals(1000L, statistics.getRowCount());
		assertEquals(0L, statistics.getFileCount());
		assertEquals(0L, statistics.getTotalSize());
		assertEquals(0L, statistics.getRawDataSize());
	}

	@Test
	public void testConvertToCatalogColumnStatistics_UNKNOWN() {
		assertEquals(CatalogColumnStatistics.UNKNOWN, convertToCatalogColumnStatistics(TableStats.UNKNOWN));
	}

	@Test
	public void testConvertToCatalogColumnStatistics() {
		CatalogColumnStatistics statistics = convertToCatalogColumnStatistics(genTableStats(true));
		Map<String, CatalogColumnStatisticsDataBase> statisticsData = statistics.getColumnStatisticsData();
		assertEquals(9, statisticsData.size());
		assertEquals(new HashMap<String, String>() {{
			put(NDV, "2");
			put(NULL_COUNT, "0");
			put(AVG_LEN, "1.0");
			put(MAX_LEN, "1");
		}}, statisticsData.get("boolean").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(NDV, "10");
			put(NULL_COUNT, "0");
			put(AVG_LEN, "1.0");
			put(MAX_LEN, "1");
			put(MAX_VALUE, "10");
			put(MIN_VALUE, "1");
		}}, statisticsData.get("byte").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(NDV, "30");
			put(NULL_COUNT, "0");
			put(AVG_LEN, "2.0");
			put(MAX_LEN, "2");
			put(MAX_VALUE, "50");
			put(MIN_VALUE, "-10");
		}}, statisticsData.get("short").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(NULL_COUNT, "10");
			put(AVG_LEN, "4.0");
			put(MAX_LEN, "4");
			put(MAX_VALUE, "100");
			put(MIN_VALUE, "10");
		}}, statisticsData.get("int").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(NDV, "50");
			put(NULL_COUNT, "0");
			put(AVG_LEN, "8.0");
			put(MAX_LEN, "8");
			put(MAX_VALUE, "100");
			put(MIN_VALUE, "10");
		}}, statisticsData.get("long").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(AVG_LEN, "4.0");
			put(MAX_LEN, "4");
			put(MAX_VALUE, "5.0");
			put(MIN_VALUE, "2.0");
		}}, statisticsData.get("float").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(AVG_LEN, "8.0");
			put(MAX_LEN, "8");
			put(MAX_VALUE, "25.0");
			put(MIN_VALUE, "6.0");
		}}, statisticsData.get("double").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(AVG_LEN, "12.0");
			put(MAX_LEN, "12");
			put(MAX_VALUE, "64.15");
			put(MIN_VALUE, "4.05");
		}}, statisticsData.get("decimal").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(NDV, "1000");
			put(NULL_COUNT, "10");
			put(AVG_LEN, "12.0");
			put(MAX_LEN, "12");
		}}, statisticsData.get("date").getProperties());
	}

	@Test
	public void testConvertBetweenTableStatsAndCatalogStatistics() {
		TableStats tableStats = genTableStats(true);
		CatalogTableStatistics tableStatistics = convertToCatalogTableStatistics(tableStats);
		CatalogColumnStatistics columnStatistics = convertToCatalogColumnStatistics(tableStats);
		TableSchema tableSchema = TableSchema.builder()
				.field("boolean", DataTypes.BOOLEAN())
				.field("byte", DataTypes.TINYINT())
				.field("short", DataTypes.SMALLINT())
				.field("int", DataTypes.INT())
				.field("long", DataTypes.BIGINT())
				.field("float", DataTypes.FLOAT())
				.field("double", DataTypes.DOUBLE())
				.field("decimal", DataTypes.DECIMAL(5, 2))
				.field("date", DataTypes.DATE())
				.field("unknown_col", DataTypes.STRING())
				.build();

		TableStats expected = genTableStats(false);
		TableStats actual = convertToTableStats(tableStatistics, columnStatistics, tableSchema);
		StatisticGeneratorTest.assertTableStatsEquals(expected, actual);
	}

	private TableStats genTableStats(boolean addUnknownColumn) {
		Map<String, ColumnStats> columnStatsMap = new HashMap<>();
		columnStatsMap.put("boolean", ColumnStats.Builder.builder()
			.setNdv(2L).setNullCount(0L).setAvgLen(1.0).setMaxLen(1)
			.setMax(null).setMin(null).build());
		columnStatsMap.put("byte", ColumnStats.Builder.builder()
			.setNdv(10L).setNullCount(0L).setAvgLen(1.0).setMaxLen(1)
			.setMax((byte) 10).setMin((byte) 1).build());
		columnStatsMap.put("short", ColumnStats.Builder.builder()
			.setNdv(30L).setNullCount(0L).setAvgLen(2.0).setMaxLen(2)
			.setMax((short) 50).setMin((short) -10).build());
		columnStatsMap.put("int", ColumnStats.Builder.builder()
			.setNdv(null).setNullCount(10L).setAvgLen(4.0).setMaxLen(4)
			.setMax(100).setMin(10).build());
		columnStatsMap.put("long", ColumnStats.Builder.builder()
			.setNdv(50L).setNullCount(0L).setAvgLen(8.0).setMaxLen(8)
			.setMax(100L).setMin(10L).build());
		columnStatsMap.put("float", ColumnStats.Builder.builder()
			.setNdv(null).setNullCount(null).setAvgLen(4.0).setMaxLen(4)
			.setMax(5.0f).setMin(2.0f).build());
		columnStatsMap.put("double", ColumnStats.Builder.builder()
			.setNdv(null).setNullCount(null).setAvgLen(8.0).setMaxLen(8)
			.setMax(25.0d).setMin(6.0d).build());
		columnStatsMap.put("decimal", ColumnStats.Builder.builder()
			.setNdv(null).setNullCount(null).setAvgLen(12.0).setMaxLen(12)
			.setMax(DecimalDataUtils.castFrom("64.15", 5, 2).toBigDecimal())
			.setMin(DecimalDataUtils.castFrom("4.05", 5, 2).toBigDecimal())
			.build());
		columnStatsMap.put("date", ColumnStats.Builder.builder()
			.setNdv(1000L).setNullCount(10L).setAvgLen(12.0).setMaxLen(12)
			.setMax(null).setMin(null).build());
		if (addUnknownColumn) {
			columnStatsMap.put("unknown_col", ColumnStats.Builder.builder()
				.setNdv(null).setNullCount(null).setAvgLen(null).setMaxLen(null)
				.setMax(null).setMin(null).build());
		}
		return new TableStats(1000L, columnStatsMap);
	}
}
