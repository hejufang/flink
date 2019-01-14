/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.flink.parser;

import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.pojo.BoltConfig;
import com.bytedance.flink.pojo.BoltInfo;
import com.bytedance.flink.pojo.JobConfig;
import com.bytedance.flink.pojo.SpoutConfig;
import com.bytedance.flink.pojo.SpoutInfo;
import com.bytedance.flink.topology.GroupType;
import com.bytedance.flink.topology.Grouping;
import com.bytedance.flink.utils.CommonUtils;
import com.bytedance.flink.utils.KafkaUtils;
import com.bytedance.flink.utils.Validation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Util class for parsing yaml file.
 */
public class ConfigParser {
	private static final Logger LOG = LoggerFactory.getLogger(ConfigParser.class);

	private String confFile;
	private Map<String, Object> allYamlConf;
	private int runMode;

	public ConfigParser(String confFile) {
		this.confFile = confFile;
	}

	public static List<Integer> parseKafkaPartitions(List kafkaRangeStrList) {
		List<Integer> result = new ArrayList<>();
		try {
			for (Object range : kafkaRangeStrList) {
				if (range instanceof Integer) {
					result.add((int) range);
				} else if ((range instanceof String) && ((String) range).contains("-")) {
					String rangeStr = ((String) range).trim();
					String[] s = rangeStr.split("-");
					if (s.length >= 2) {
						int start = Integer.parseInt(s[0]);
						int end = Integer.parseInt(s[1]);
						for (int i = start; i <= end; i++) {
							result.add(i);
						}
					}
				} else {
					LOG.error("Failed to parse kafka partition range item {}", range);
				}
			}
		} catch (Throwable t) {
			LOG.error("Failed to parse kafka partition range: {}.", kafkaRangeStrList, t);
		}
		return result;
	}

	/**
	 * Replace jstorm script to flink script.
	 */
	public static String replaceScript(String oldScript) {
		if (oldScript == null) {
			return null;
		}

		// Replace bolt runner.
		if (oldScript.contains(Constants.BOLT_RUNNER_FLAG)) {
			return Constants.FLINK_BOLT_RUNNER;
		}

		// Replace batch bolt runner.
		if (oldScript.contains(Constants.BATCH_BOLT_RUNNER_FLAG)) {
			return Constants.FLINK_BATCH_RUNNER;
		}

		// Replace new batch bolt runner.
		if (oldScript.contains(Constants.BATCH_BOLT_NEW_RUNNER_FLAG)) {
			return Constants.FLINK_BATCH_RUNNER;
		}

		// Replace kafka spout runner.
		if (oldScript.contains(Constants.KAFKA_SPOUT_RUNNER_FLAG)) {
			return Constants.FLINK_SPOUT_RUNNER;
		}

		// Replace multi kafka spout runner.
		if (oldScript.contains(Constants.MULTI_KAFKA_SPOUT_RUNNER_FLAG)) {
			return Constants.FLINK_MULTI_SPOUT_RUNNER;
		}

		// Replace gevent kafka spout runner.
		if (oldScript.contains(Constants.KAFKA_SPOUT_GEVENT_RUNNER_FLAG)) {
			return Constants.FLINK_GEVENT_SPOUT_RUNNER;
		}

		// Replace nsq spout runner.
		if (oldScript.contains(Constants.NSQ_SPOUT_RUNNER_FLAG)) {
			return Constants.FLINK_NSQ_SPOUT_RUNNER;
		}

		// Replace nsq spout adstat runner.
		if (oldScript.contains(Constants.NSQ_SPOUT_ADSTAT_RUNNER_FLAG)) {
			return Constants.FLINK_NSQ_ADSTAT_SPOUT_RUNNER;
		}

		return oldScript;
	}

	public JobConfig parse() throws IOException {
		JobConfig jobConfig = new JobConfig();
		InputStream input = new FileInputStream(new File(confFile));
		Yaml yaml = new Yaml();
		allYamlConf = (Map<String, Object>) yaml.load(input);
		jobConfig.setJobName((String) allYamlConf.get(Constants.TOPOLOGY_NAME));
		jobConfig.setTopologyYaml(CommonUtils.loadFileToString(confFile));
		jobConfig.setCommonArgs(
			(Map<String, Object>) allYamlConf.getOrDefault(Constants.COMMON_ARGS, new HashMap<>()));
		String ownerStr = (String) allYamlConf.getOrDefault(Constants.OWNERS, "");
		jobConfig.setOwners(CommonUtils.parseStringToList(ownerStr, ","));

		runMode = (Integer) allYamlConf.getOrDefault(Constants.RUN_MODE,
			Constants.RUN_MODE_CLUSTER);
		jobConfig.setRunMode(runMode);

		Map flinkArgs = (Map<Object, Object>) allYamlConf.getOrDefault(Constants.FLINK_ARGS, new HashMap<>());
		String failoverStrategy = (String) flinkArgs.get(Constants.FAILOVER_STRATEGY);
		if (Constants.LOCAL_FAILOVER_STRATEGY.equals(failoverStrategy)) {
			jobConfig.setLocalFailover(true);
		} else {
			jobConfig.setLocalFailover(false);
		}

		jobConfig.setRunSeconds((Integer) allYamlConf.getOrDefault(Constants.RUN_TIME_KEY,
			Constants.RUN_TIME_VAL));
		jobConfig.setEnvironment((Map<Object, Object>) allYamlConf.get(Constants.ENVIRONMENT));
		int maxSpoutPending = (Integer) allYamlConf.getOrDefault(Constants.MAX_SPOUT_PENDING_KEY,
			Constants.MAX_SPOUT_PENDING_VAL);
		jobConfig.setMaxSpoutPending(maxSpoutPending);
		jobConfig.getCommonArgs().put(Constants.MAX_SPOUT_PENDING_KEY, maxSpoutPending);

		int maxBoltPending = (Integer) allYamlConf.getOrDefault(Constants.MAX_BOLT_PENDING_KEY,
			Constants.MAX_BOLT_PENDING_VAL);
		jobConfig.setMaxBoltPending(maxBoltPending);
		jobConfig.getCommonArgs().put(Constants.MAX_BOLT_PENDING_KEY, maxBoltPending);

		boolean isIgnoreMismatchedMsg =
			(boolean) allYamlConf.getOrDefault(Constants.IS_IGNORE_MISMATCHED_MSG, false);
		jobConfig.setIgnoreMismatchedMsg(isIgnoreMismatchedMsg);

		Map<Object, Object> env =
			(Map<Object, Object>) allYamlConf.getOrDefault(Constants.ENVIRONMENT, new HashMap<>());
		jobConfig.setEnvironment(env);

		SpoutConfig spoutConfig = parseSpout(jobConfig);
		BoltConfig boltConfig = parseBolt(jobConfig);
		jobConfig.setSpoutConfig(spoutConfig);
		jobConfig.setBoltConfig(boltConfig);
		return jobConfig;
	}

	public SpoutConfig parseSpout(JobConfig jobConfig) {
		Map<String, Object> spoutConf = (Map<String, Object>) allYamlConf.get(Constants.SPOUT);
		if (spoutConf == null) {
			return null;
		}
		SpoutConfig spoutConfig = new SpoutConfig();
		Map<String, SpoutInfo> spoutInfoMap = new HashMap<>();
		Map<String, Object> spoutsCommonArgs =
			(Map<String, Object>) spoutConf.getOrDefault(Constants.COMMON_ARGS, new HashMap<>());
		spoutsCommonArgs = CommonUtils.mergeMaps(jobConfig.getCommonArgs(), spoutsCommonArgs);
		spoutConfig.setCommonArgs(spoutsCommonArgs);
		ArrayList<Object> spoutList =
			(ArrayList<Object>) spoutConf.getOrDefault(Constants.SPOUT_LIST, new HashMap<>());
		for (Object o : spoutList) {
			SpoutInfo spoutInfo = new SpoutInfo();
			Map<String, Object> map = (Map<String, Object>) o;
			Map<String, Object> spoutArgs =
				(Map<String, Object>) map.getOrDefault(Constants.ARGS, new HashMap<>());
			spoutArgs = CommonUtils.mergeMaps(spoutsCommonArgs, spoutArgs);
			spoutInfo.setArgs(spoutArgs);

			// parse thread num
			if (map.containsKey(Constants.PARALLELISM)) {
				spoutInfo.setParallelism((int) map.get(Constants.PARALLELISM));
			} else {
				spoutInfo.setParallelism((int) spoutArgs.getOrDefault(Constants.PARALLELISM, 1));
			}
			spoutInfo.getArgs().put(Constants.PARALLELISM, spoutInfo.getParallelism());

			// parse multi sources
			if (map.containsKey(Constants.MULTI_SOURCE)) {
				spoutInfo.setKafkaMultiSource((boolean) map.get(Constants.MULTI_SOURCE));
			} else {
				spoutInfo.setKafkaMultiSource(
					(boolean) spoutArgs.getOrDefault(Constants.MULTI_SOURCE, false));
			}
			spoutInfo.getArgs().put(Constants.MULTI_SOURCE, spoutInfo.isKafkaMultiSource());

			// parse spout name
			if (map.containsKey(Constants.SPOUT_NAME)) {
				spoutInfo.setName((String) map.get(Constants.SPOUT_NAME));
			} else {
				spoutInfo.setName((String) spoutArgs.get(Constants.SPOUT_NAME));
			}
			spoutInfo.getArgs().put(Constants.SPOUT_NAME, spoutInfo.getName());

			// parse interpreter
			if (map.containsKey(Constants.INTERPRETER_KEY)) {
				spoutInfo.setInterpreter((String) map.get(Constants.INTERPRETER_KEY));
			} else {
				spoutInfo.setInterpreter((String) spoutArgs.getOrDefault(Constants.INTERPRETER_KEY,
					Constants.INTERPRETER_VAL));
			}
			spoutInfo.getArgs().put(Constants.INTERPRETER_VAL, spoutInfo.getInterpreter());

			// parse script name
			if (map.containsKey(Constants.SCRIPT)) {
				String script = replaceScript((String) map.get(Constants.SCRIPT));
				spoutInfo.setScript(script);
			} else {
				String script = replaceScript((String) spoutArgs.get(Constants.SCRIPT));
				spoutInfo.setScript(script);
			}
			spoutInfo.getArgs().put(Constants.SCRIPT, spoutInfo.getScript());

			// parse output fields
			if (map.containsKey(Constants.OUTPUT_FIELDS)) {
				spoutInfo.setOutputFields((ArrayList<String>) map.get(Constants.OUTPUT_FIELDS));
			} else {
				spoutInfo.setOutputFields(
					(ArrayList<String>) spoutArgs.getOrDefault(Constants.OUTPUT_FIELDS, 1));
			}
			spoutInfo.getArgs().put(Constants.OUTPUT_FIELDS, spoutInfo.getOutputFields());

			// parse kafka spout ext
			int kafkaSpoutExt = 1;
			if (map.containsKey(Constants.KAFKA_SPOUT_EXT_KEY)) {
				kafkaSpoutExt = (Integer) map.get(Constants.KAFKA_SPOUT_EXT_KEY);
			} else {
				kafkaSpoutExt = (Integer) spoutArgs.getOrDefault(Constants.KAFKA_SPOUT_EXT_KEY,
					Constants.KAFKA_SPOUT_EXT_VAL);
			}
			spoutInfo.getArgs().put(Constants.KAFKA_SPOUT_EXT_KEY, kafkaSpoutExt);

			boolean isThreadNumDeterminedByPartition = kafkaSpoutExt == 1;
			if (map.containsKey(Constants.IS_THREAD_NUM_DETREMINED_BY_PARTITION)) {
				isThreadNumDeterminedByPartition =
					(boolean) map.get(Constants.IS_THREAD_NUM_DETREMINED_BY_PARTITION);
			} else {
				isThreadNumDeterminedByPartition =
					(boolean) spoutArgs.getOrDefault(Constants.IS_THREAD_NUM_DETREMINED_BY_PARTITION,
						isThreadNumDeterminedByPartition);
			}
			spoutInfo.setThreadNumDeterminedByPartition(isThreadNumDeterminedByPartition);
			spoutInfo.getArgs().put(Constants.IS_THREAD_NUM_DETREMINED_BY_PARTITION,
				isThreadNumDeterminedByPartition);

			// parse kafka partition number
			if (spoutArgs.containsKey(Constants.PARTITION)) {
				int partitionNum = (int) spoutArgs.get(Constants.PARTITION);
				spoutInfo.setTotalPartition(partitionNum);
			}

			boolean isAutoPartition = true;

			if (spoutArgs.containsKey(Constants.IS_AUTO_PARTITION)) {
				isAutoPartition = (boolean) spoutArgs.get(Constants.IS_AUTO_PARTITION);
			}

			if (spoutInfo.isKafkaMultiSource()) {
				for (Map<String, Object> kafkaSource :
					(List<Map<String, Object>>) spoutArgs.get(Constants.KAFKA_SOURCE)) {
					int partition = KafkaUtils.getPartitionNum(
						(String) kafkaSource.get(Constants.KAFKA_CLUSTER),
						(String) kafkaSource.get(Constants.TOPIC_NAME));
					kafkaSource.put(Constants.TOTAL_PARTITION, partition);
				}
				isAutoPartition = false;
			} else {
				String kafkaCluster = (String) spoutArgs.get(Constants.KAFKA_CLUSTER);
				String topicName = (String) spoutArgs.get(Constants.TOPIC_NAME);
				spoutInfo.setKafkaCluster(kafkaCluster);
				spoutInfo.setKafkaTopic(topicName);
			}
			List kafkaRangeStrList = (List) spoutArgs.get(Constants.KAFKA_PARTITION_RANGE);
			if (kafkaRangeStrList != null && !kafkaRangeStrList.isEmpty()
				&& isCommonKafkaSpout(spoutInfo)) {
				List<Integer> partitionList = parseKafkaPartitions(kafkaRangeStrList);
				if (partitionList != null && !partitionList.isEmpty()) {
					spoutInfo.setPartitionList(partitionList);
					spoutInfo.setPartitionRangeConfigured(true);
					spoutInfo.setTotalPartition(partitionList.size());
					spoutInfo.getArgs().put(Constants.TOTAL_PARTITION, partitionList.size());
					spoutInfo.setThreadNumDeterminedByPartition(true);
					isAutoPartition = false;
				}
			}

			// Thread number of nsq spout is decided by 'partition'
			if (isNsqSpout(spoutInfo)) {
				isAutoPartition = false;
				spoutInfo.setParallelism(spoutInfo.getTotalPartition());
			}

			if (runMode == Constants.RUN_MODE_STANDLONE) {
				isAutoPartition = false;
			}

			spoutInfo.setAutoPartition(isAutoPartition);
			if (isAutoPartition) {
				// We fetch the partition number from kafka if it is configured to be auto partition.
				int partitionNum =
					KafkaUtils.getPartitionNum(spoutInfo.getKafkaCluster(), spoutInfo.getKafkaTopic());
				LOG.info("Adjust kafka partition num to {}", partitionNum);
				spoutInfo.setTotalPartition(partitionNum);
				spoutInfo.getArgs().put(Constants.TOTAL_PARTITION, partitionNum);
			}

			if (spoutInfo.isThreadNumDeterminedByPartition() && !spoutInfo.isKafkaMultiSource()) {
				int partitionNum = spoutInfo.getTotalPartition();
				spoutInfo.setParallelism(partitionNum);
				spoutArgs.put(Constants.PARALLELISM, partitionNum);
				LOG.info("Adjust {} to {}", Constants.PARALLELISM, partitionNum);
			}

			Validation.validateSpoutInfo(spoutInfo);
			spoutInfoMap.put(spoutInfo.getName(), spoutInfo);
		}
		spoutConfig.setSpoutInfoMap(spoutInfoMap);
		return spoutConfig;
	}

	public BoltConfig parseBolt(JobConfig jobConfig) {
		Map<String, Object> boltConf = (Map<String, Object>) allYamlConf.get(Constants.BOLT);
		if (boltConf == null) {
			return null;
		}
		BoltConfig boltConfig = new BoltConfig();
		Map<String, BoltInfo> boltInfoMap = new HashMap<>();
		Map<String, Object> boltsCommonArgs =
			(Map<String, Object>) boltConf.getOrDefault(Constants.COMMON_ARGS, new HashMap<>());
		boltsCommonArgs = CommonUtils.mergeMaps(jobConfig.getCommonArgs(), boltsCommonArgs);
		boltConfig.setCommonArgs(boltsCommonArgs);
		ArrayList<Object> boltList =
			(ArrayList<Object>) boltConf.getOrDefault(Constants.BOLT_LIST, new HashMap<>());
		for (Object o : boltList) {
			BoltInfo boltInfo = new BoltInfo();
			Map<String, Object> map = (Map<String, Object>) o;
			Map<String, Object> boltArgs =
				(Map<String, Object>) map.getOrDefault(Constants.ARGS, new HashMap<>());
			boltArgs = CommonUtils.mergeMaps(boltsCommonArgs, boltArgs);
			boltInfo.setArgs(boltArgs);

			// parse thread num
			if (map.containsKey(Constants.PARALLELISM)) {
				boltInfo.setParallelism((int) map.get(Constants.PARALLELISM));
			} else {
				boltInfo.setParallelism((int) boltArgs.getOrDefault(Constants.PARALLELISM, 1));
			}
			boltInfo.getArgs().put(Constants.PARALLELISM, boltInfo.getParallelism());

			// parse spout name
			if (map.containsKey(Constants.BOLT_NAME)) {
				boltInfo.setName((String) map.get(Constants.BOLT_NAME));
			} else {
				boltInfo.setName((String) boltArgs.get(Constants.BOLT_NAME));
			}
			boltInfo.getArgs().put(Constants.BOLT_NAME, boltInfo.getName());

			// parse interpreter
			if (map.containsKey(Constants.INTERPRETER_KEY)) {
				boltInfo.setInterpreter((String) map.get(Constants.INTERPRETER_KEY));
			} else {
				boltInfo.setInterpreter((String) boltArgs.getOrDefault(Constants.INTERPRETER_KEY,
					Constants.INTERPRETER_VAL));
			}
			boltInfo.getArgs().put(Constants.INTERPRETER_VAL, boltInfo.getInterpreter());

			// parse script name
			if (map.containsKey(Constants.SCRIPT)) {
				String script = replaceScript((String) map.get(Constants.SCRIPT));
				boltInfo.setScript(script);
			} else {
				String script = replaceScript((String) boltArgs.get(Constants.SCRIPT));
				boltInfo.setScript(script);
			}
			boltInfo.getArgs().put(Constants.SCRIPT, boltInfo.getScript());

			// parse output fields
			if (map.containsKey(Constants.OUTPUT_FIELDS)) {
				boltInfo.setOutputFields((ArrayList<String>) map.get(Constants.OUTPUT_FIELDS));
			} else {
				boltInfo.setOutputFields(
					(ArrayList<String>) boltArgs.getOrDefault(Constants.OUTPUT_FIELDS, 1));
			}
			boltInfo.getArgs().put(Constants.OUTPUT_FIELDS, boltInfo.getOutputFields());

			// parse group list (cannot be null)
			boltInfo.setGroupList(
				parseGroupingList((ArrayList<Object>) map.get(Constants.GROUP_LIST)));
			Validation.validateBoltInfo(boltInfo);
			boltInfoMap.put(boltInfo.getName(), boltInfo);
		}
		boltConfig.setBoltInfoMap(boltInfoMap);
		return boltConfig;
	}

	public boolean isCommonKafkaSpout(SpoutInfo spoutInfo) {
		if (spoutInfo == null || spoutInfo.getScript() == null) {
			return false;
		}
		return spoutInfo.getScript().contains(Constants.KAFKA_SPOUT_RUNNER_FLAG);
	}

	public boolean isNsqSpout(SpoutInfo spoutInfo) {
		if (spoutInfo == null || spoutInfo.getScript() == null) {
			return false;
		}
		return spoutInfo.getScript().contains(Constants.NSQ_FLAG);
	}

	public List<Grouping> parseGroupingList(List<Object> groupListConfig) {
		Validation.validateNotEmptyList(groupListConfig, Constants.GROUP_LIST);
		List<Grouping> groupingList = new ArrayList<>();
		for (Object o1 : groupListConfig) {
			Map<String, Object> groupItem = (Map<String, Object>) o1;
			String groupTypeName = (String) groupItem.get(Constants.GROUP_TYPE);
			GroupType groupType = GroupType.parse(groupTypeName);
			String groupField = (String) groupItem.get(Constants.GROUP_FIELD);
			List<Object> inputStreams = (List<Object>) groupItem.get(Constants.STREAM_FROM);
			for (Object o2 : inputStreams) {
				String inputStream = (String) o2;
				groupingList.add(new Grouping(groupType, Collections.singletonList(groupField), inputStream));
			}
		}
		return groupingList;
	}
}
