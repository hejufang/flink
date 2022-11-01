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

package org.apache.flink.connectors.hive;

import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.planner.plan.utils.HiveUtils$;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TestHiveCatalog.
 */
public class TestHiveCatalog extends HiveCatalog {
	private Map<String, BucketInfo> bucketInfoMap = new HashMap<>();

	public TestHiveCatalog(
			String catalogName,
			String defaultDatabase,
			@Nullable HiveConf hiveConf,
			String hiveVersion,
			boolean allowEmbedded,
			boolean allowedToModifyHiveMeta) {
		super(catalogName, defaultDatabase, hiveConf, hiveVersion, allowEmbedded, allowedToModifyHiveMeta);
	}

	@Override
	protected void tryAddBucketInfo(Table hiveTable, Map<String, String> properties) {
		BucketInfo bucketInfo = bucketInfoMap.get(hiveTable.getDbName() + "." + hiveTable.getTableName());
		if (bucketInfo == null) {
			return;
		}

		HiveUtils$.MODULE$.addBucketProperties(bucketInfo.bucketNum,
			bucketInfo.bucketCols, bucketInfo.sortCols, properties);
	}

	@Override
	public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
		super.createTable(tablePath, table, ignoreIfExists);
	}

	public void addBucketInfo(
			String name,
			int bucketNum,
			List<String> bucketCols,
			List<String> sortCols) {
		bucketInfoMap.put(name, new BucketInfo(bucketNum, bucketCols, sortCols));
	}

	private static class BucketInfo {
		private int bucketNum;
		private List<String> bucketCols;
		private List<String> sortCols;

		public BucketInfo(int bucketNum, List<String> bucketCols, List<String> sortCols) {
			this.bucketNum = bucketNum;
			this.bucketCols = bucketCols;
			this.sortCols = sortCols;
		}
	}
}
