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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.GenericCatalogColumnStatisticsData;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for converting {@link CatalogTableStatistics} and {@link CatalogColumnStatistics} to {@link TableStats}.
 */
public class CatalogTableStatisticsConverter {

	private static final Logger LOG = LoggerFactory.getLogger(CatalogTableStatisticsConverter.class);

	public static TableStats convertToTableStats(
			CatalogTableStatistics tableStatistics,
			CatalogColumnStatistics columnStatistics,
			TableSchema schema) {
		long rowCount;
		if (tableStatistics != null && tableStatistics.getRowCount() >= 0) {
			rowCount = tableStatistics.getRowCount();
		} else {
			rowCount = TableStats.UNKNOWN.getRowCount();
		}

		Map<String, ColumnStats> columnStatsMap;
		if (columnStatistics != null) {
			columnStatsMap = convertToColumnStatsMap(
				columnStatistics.getColumnStatisticsData(), schema);
		} else {
			columnStatsMap = new HashMap<>();
		}
		return new TableStats(rowCount, columnStatsMap);
	}

	@VisibleForTesting
	public static Map<String, ColumnStats> convertToColumnStatsMap(
			Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData,
			TableSchema schema) {
		Map<String, ColumnStats> columnStatsMap = new HashMap<>();
		for (Map.Entry<String, CatalogColumnStatisticsDataBase> entry : columnStatisticsData.entrySet()) {
			if (entry.getValue() == null) {
				continue;
			}
			String fieldName = entry.getKey();
			Optional<DataType> fieldType = schema.getFieldDataType(fieldName);
			if (fieldType.isPresent()) {
				ColumnStats columnStats = convertToColumnStats(entry.getValue(), fieldType.get());
				if (columnStats != null) {
					columnStatsMap.put(fieldName, columnStats);
				}
			} else {
				LOG.warn(fieldName + " is not found in TableSchema: " + schema);
			}
		}
		return columnStatsMap;
	}

	private static ColumnStats convertToColumnStats(
			CatalogColumnStatisticsDataBase columnStatisticsData,
			DataType fieldType) {
		Long ndv = null;
		Long nullCount = columnStatisticsData.getNullCount();
		Double avgLen = null;
		Integer maxLen = null;
		Comparable<?> max = null;
		Comparable<?> min = null;
		if (columnStatisticsData instanceof CatalogColumnStatisticsDataBoolean) {
			CatalogColumnStatisticsDataBoolean booleanData = (CatalogColumnStatisticsDataBoolean) columnStatisticsData;
			avgLen = 1.0;
			maxLen = 1;
			if (null == booleanData.getFalseCount() || null == booleanData.getTrueCount()) {
				ndv = 2L;
			} else if ((booleanData.getFalseCount() == 0 && booleanData.getTrueCount() > 0) ||
					(booleanData.getFalseCount() > 0 && booleanData.getTrueCount() == 0)) {
				ndv = 1L;
			} else {
				ndv = 2L;
			}
		} else if (columnStatisticsData instanceof CatalogColumnStatisticsDataLong) {
			CatalogColumnStatisticsDataLong longData = (CatalogColumnStatisticsDataLong) columnStatisticsData;
			ndv = longData.getNdv();
			avgLen = 8.0;
			maxLen = 8;
			max = longData.getMax();
			min = longData.getMin();
		} else if (columnStatisticsData instanceof CatalogColumnStatisticsDataDouble) {
			CatalogColumnStatisticsDataDouble doubleData = (CatalogColumnStatisticsDataDouble) columnStatisticsData;
			ndv = doubleData.getNdv();
			avgLen = 8.0;
			maxLen = 8;
			max = doubleData.getMax();
			min = doubleData.getMin();
		} else if (columnStatisticsData instanceof CatalogColumnStatisticsDataString) {
			CatalogColumnStatisticsDataString strData = (CatalogColumnStatisticsDataString) columnStatisticsData;
			ndv = strData.getNdv();
			avgLen = strData.getAvgLength();
			maxLen = null == strData.getMaxLength() ? null : strData.getMaxLength().intValue();
		} else if (columnStatisticsData instanceof CatalogColumnStatisticsDataBinary) {
			CatalogColumnStatisticsDataBinary binaryData = (CatalogColumnStatisticsDataBinary) columnStatisticsData;
			avgLen = binaryData.getAvgLength();
			maxLen = null == binaryData.getMaxLength() ? null : binaryData.getMaxLength().intValue();
		} else if (columnStatisticsData instanceof CatalogColumnStatisticsDataDate) {
			CatalogColumnStatisticsDataDate dateData = (CatalogColumnStatisticsDataDate) columnStatisticsData;
			ndv = dateData.getNdv();
			if (dateData.getMax() != null) {
				max = Date.valueOf(DateTimeUtils.unixDateToString((int) dateData.getMax().getDaysSinceEpoch()));
			}
			if (dateData.getMin() != null) {
				min = Date.valueOf(DateTimeUtils.unixDateToString((int) dateData.getMin().getDaysSinceEpoch()));
			}
		} else if (columnStatisticsData instanceof GenericCatalogColumnStatisticsData) {
			boolean hasValue = false;
			Map<String, String> properties = columnStatisticsData.getProperties();
			if (properties.containsKey(TableStatsConverter.NDV)) {
				ndv = Long.valueOf(properties.get(TableStatsConverter.NDV));
				hasValue = true;
			}
			nullCount = null;
			if (properties.containsKey(TableStatsConverter.NULL_COUNT)) {
				nullCount = Long.valueOf(properties.get(TableStatsConverter.NULL_COUNT));
				hasValue = true;
			}
			if (properties.containsKey(TableStatsConverter.MAX_LEN)) {
				maxLen = Integer.valueOf(properties.get(TableStatsConverter.MAX_LEN));
				hasValue = true;
			}
			if (properties.containsKey(TableStatsConverter.AVG_LEN)) {
				avgLen = Double.valueOf(properties.get(TableStatsConverter.AVG_LEN));
				hasValue = true;
			}
			if (properties.containsKey(TableStatsConverter.MAX_VALUE)) {
				max = convertToComparable(properties.get(TableStatsConverter.MAX_VALUE), fieldType);
				hasValue = true;
			}
			if (properties.containsKey(TableStatsConverter.MIN_VALUE)) {
				min = convertToComparable(properties.get(TableStatsConverter.MIN_VALUE), fieldType);
				hasValue = true;
			}
			if (!hasValue) {
				return null;
			}
		} else {
			throw new TableException("Unsupported CatalogColumnStatisticsDataBase: " +
					columnStatisticsData.getClass().getCanonicalName());
		}
		return ColumnStats.Builder
			.builder()
			.setNdv(ndv)
			.setNullCount(nullCount)
			.setAvgLen(avgLen)
			.setMaxLen(maxLen)
			.setMax(max)
			.setMin(min)
			.build();
	}

	private static Comparable<?> convertToComparable(String value, DataType dataType) {
		switch (dataType.getLogicalType().getTypeRoot()) {
			case TINYINT:
				return Byte.valueOf(value);
			case SMALLINT:
				return Short.valueOf(value);
			case INTEGER:
				return Integer.valueOf(value);
			case BIGINT:
				return Long.valueOf(value);
			case FLOAT:
				return Float.valueOf(value);
			case DOUBLE:
				return Double.valueOf(value);
			case DECIMAL:
				return BigDecimal.valueOf(Double.valueOf(value));
			case BOOLEAN:
				return Boolean.valueOf(value);
			case DATE:
				return Date.valueOf(value);
			case VARCHAR:
				return String.valueOf(value);
			case TIME_WITHOUT_TIME_ZONE:
				return Time.valueOf(value);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return new Timestamp(SqlDateTimeUtils.toTimestamp(value, "yyyy-MM-dd'T'HH:mm"));
			default:
				return null;
		}
	}
}
