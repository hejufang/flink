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

package org.apache.hudi.sink.utils;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Hive synchronization context.
 *
 * <p>Use this context to create the {@link HiveSyncTool} for synchronization.
 * Copied from Hudi, adding relocation logic for hive configs.
 */
public class HiveSyncContext {
  private static final Logger LOG = LoggerFactory.getLogger(HiveSyncContext.class);

  private final HiveSyncConfig syncConfig;
  private final HiveConf hiveConf;
  private final FileSystem fs;

  private HiveSyncContext(HiveSyncConfig syncConfig, HiveConf hiveConf, FileSystem fs) {
    this.syncConfig = syncConfig;
    this.hiveConf = hiveConf;
    this.fs = fs;
  }

  public HiveSyncTool hiveSyncTool() {
    return new HiveSyncTool(this.syncConfig, this.hiveConf, this.fs);
  }

  public static HiveSyncContext create(Configuration conf) {
    HiveSyncConfig syncConfig = buildSyncConfig(conf);
    org.apache.hadoop.conf.Configuration hadoopConf = StreamerUtil.getHadoopConf();
    String path = conf.getString(FlinkOptions.PATH);
    FileSystem fs = FSUtils.getFs(path, hadoopConf);
    String hiveConfLocation = conf.get(FlinkOptions.HIVE_CONF_LOCATION);
    HiveConf hiveConf = new HiveConf();
    if (hiveConfLocation.equals("")) {
      hiveConf.addResource(hadoopConf);
    } else {
      hiveConf.addResource(new org.apache.hadoop.fs.Path(hiveConfLocation));
      hiveConf.verifyAndSet("hive.client.enable.load.functions", "false");
      hiveConf.verifyAndSet("hive.metastore.use.consul", hiveConf.get("hive.metastore.use.consul"));
      // Flink environment has outdated hive conf, we will load this conf from dts-dump conf
      hiveConf.verifyAndSet("hive.metastore.consul.name", "data.olap.catalogservice");
      hiveConf.verifyAndSet("hive.metastore.consul.name.first", "data.olap.catalogservice");
      hiveConf.verifyAndSet("hive.client.namespace", "LF_HL_HIVE");
      hiveConf.verifyAndSet("hive.metastore.uris", hiveConf.get("hive.metastore.uris"));
      hiveConf.verifyAndSet("hive.hms.client.execute.set_token", "true");
      hiveConf.verifyAndSet("hive.client.enable.prefer_ipv6", hiveConf.get("hive.client.enable.prefer_ipv6", "true"));
      hiveConf.verifyAndSet("hive.conf.location", hiveConfLocation);
    }

    // this is a hack, replacing all hive packages to shaded one.
	// 'org.apache.hadoop.hive' will be relocated, hence we use 'apache.hadoop.hive' instead.
    Map<String, String> toBeChanged = new HashMap<>();
    for (Map.Entry<String, String> entry : hiveConf) {
      if (entry.getValue().contains("apache.hadoop.hive") &&
          !entry.getValue().contains("org.apache.flink.hudi.shaded")) {
        LOG.info("replacing " + entry.getKey() + "=" + entry.getValue());
        toBeChanged.put(entry.getKey(), entry.getValue().replaceAll(
                "apache.hadoop.hive" ,
                "apache.flink.hudi.shaded.org.apache.hadoop.hive"
        ));
      }
    }
    for (Map.Entry<String, String> entry : toBeChanged.entrySet()) {
      hiveConf.set(entry.getKey(), entry.getValue());
    }
    return new HiveSyncContext(syncConfig, hiveConf, fs);
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  private static HiveSyncConfig buildSyncConfig(Configuration conf) {
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.basePath = conf.getString(FlinkOptions.PATH);
    hiveSyncConfig.baseFileFormat = conf.getString(FlinkOptions.HIVE_SYNC_FILE_FORMAT);
    hiveSyncConfig.usePreApacheInputFormat = false;
    hiveSyncConfig.databaseName = conf.getString(FlinkOptions.HIVE_SYNC_DB);
    hiveSyncConfig.tableName = conf.getString(FlinkOptions.HIVE_SYNC_TABLE);
    hiveSyncConfig.hiveUser = conf.getString(FlinkOptions.HIVE_SYNC_USERNAME);
    hiveSyncConfig.hivePass = conf.getString(FlinkOptions.HIVE_SYNC_PASSWORD);
    hiveSyncConfig.jdbcUrl = conf.getString(FlinkOptions.HIVE_SYNC_JDBC_URL);
    hiveSyncConfig.partitionFields = Arrays.asList(FilePathUtils.extractHivePartitionKeys(conf));
    hiveSyncConfig.partitionValueExtractorClass = conf.getString(FlinkOptions.HIVE_SYNC_PARTITION_EXTRACTOR_CLASS);
    hiveSyncConfig.useJdbc = conf.getBoolean(FlinkOptions.HIVE_SYNC_USE_JDBC);
    // needs to support metadata table for flink
    hiveSyncConfig.useFileListingFromMetadata = false;
    hiveSyncConfig.verifyMetadataFileListing = false;
    //hiveSyncConfig.ignoreExceptions = conf.getBoolean(FlinkOptions.HIVE_SYNC_IGNORE_EXCEPTIONS);
    hiveSyncConfig.supportTimestamp = conf.getBoolean(FlinkOptions.HIVE_SYNC_SUPPORT_TIMESTAMP);
    hiveSyncConfig.autoCreateDatabase = conf.getBoolean(FlinkOptions.HIVE_SYNC_AUTO_CREATE_DB);
    hiveSyncConfig.decodePartition = conf.getBoolean(FlinkOptions.URL_ENCODE_PARTITIONING);
    hiveSyncConfig.skipROSuffix = conf.getBoolean(FlinkOptions.HIVE_SYNC_SKIP_RO_SUFFIX);
    hiveSyncConfig.assumeDatePartitioning = conf.getBoolean(FlinkOptions.HIVE_SYNC_ASSUME_DATE_PARTITION);
    return hiveSyncConfig;
  }
}
