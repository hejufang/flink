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

package com.bytedance.flink.pojo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Spout info POJO.
 */
public class SpoutInfo implements Serializable {
	private String name;
	private String script;
	private String interpreter;
	private int parallelism;
	private int maxParallelism;
	private String kafkaCluster;
	private String kafkaTopic;
	private List<String> outputFields;
	private Schema outputSchema;
	private boolean isThreadNumDeterminedByPartition;
	private int totalPartition;
	private int consumedPartition;
	private boolean isKafkaMultiSource;
	private List<Integer> partitionList;
	private boolean isPartitionRangeConfigured;
	private boolean isAutoPartition;
	private Map<String, Object> args;
	private String slotShareGroup;
	private String consumerGroup;

	public SpoutInfo() {
	}

	public String getSlotShareGroup() {
		return slotShareGroup;
	}

	public void setSlotShareGroup(String slotShareGroup) {
		this.slotShareGroup = slotShareGroup;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public String getInterpreter() {
		return interpreter;
	}

	public void setInterpreter(String interpreter) {
		this.interpreter = interpreter;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public int getMaxParallelism() {
		return maxParallelism;
	}

	public void setMaxParallelism(int maxParallelism) {
		this.maxParallelism = maxParallelism;
	}

	public String getKafkaCluster() {
		return kafkaCluster;
	}

	public void setKafkaCluster(String kafkaCluster) {
		this.kafkaCluster = kafkaCluster;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}

	public List<String> getOutputFields() {
		return outputFields;
	}

	public void setOutputFields(List<String> outputFields) {
		this.outputFields = outputFields;
	}

	public Schema getOutputSchema() {
		return outputSchema;
	}

	public void setOutputSchema(Schema outputSchema) {
		this.outputSchema = outputSchema;
	}

	public boolean isThreadNumDeterminedByPartition() {
		return isThreadNumDeterminedByPartition;
	}

	public void setThreadNumDeterminedByPartition(boolean threadNumDeterminedByPartition) {
		isThreadNumDeterminedByPartition = threadNumDeterminedByPartition;
	}

	public int getTotalPartition() {
		return totalPartition;
	}

	public void setTotalPartition(int totalPartition) {
		this.totalPartition = totalPartition;
	}

	public int getConsumedPartition() {
		return consumedPartition;
	}

	public void setConsumedPartition(int consumedPartition) {
		this.consumedPartition = consumedPartition;
	}

	public boolean isKafkaMultiSource() {
		return isKafkaMultiSource;
	}

	public void setKafkaMultiSource(boolean kafkaMultiSource) {
		isKafkaMultiSource = kafkaMultiSource;
	}

	public List<Integer> getPartitionList() {
		return partitionList;
	}

	public void setPartitionList(List<Integer> partitionList) {
		this.partitionList = partitionList;
	}

	public boolean isPartitionRangeConfigured() {
		return isPartitionRangeConfigured;
	}

	public void setPartitionRangeConfigured(boolean partitionRangeConfigured) {
		isPartitionRangeConfigured = partitionRangeConfigured;
	}

	public boolean isAutoPartition() {
		return isAutoPartition;
	}

	public void setAutoPartition(boolean autoPartition) {
		isAutoPartition = autoPartition;
	}

	public Map<String, Object> getArgs() {
		return args;
	}

	public void setArgs(Map<String, Object> args) {
		this.args = args;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	@Override
	public String toString() {
		return "SpoutInfo{" +
			"name='" + name + '\'' +
			", script='" + script + '\'' +
			", interpreter='" + interpreter + '\'' +
			", parallelism=" + parallelism +
			", kafkaCluster='" + kafkaCluster + '\'' +
			", kafkaTopic='" + kafkaTopic + '\'' +
			", outputFields=" + outputFields +
			", outputSchema=" + outputSchema +
			", isThreadNumDeterminedByPartition=" + isThreadNumDeterminedByPartition +
			", totalPartition=" + totalPartition +
			", consumedPartition=" + consumedPartition +
			", isKafkaMultiSource=" + isKafkaMultiSource +
			", partitionList=" + partitionList +
			", isPartitionRangeConfigured=" + isPartitionRangeConfigured +
			", isAutoPartition=" + isAutoPartition +
			", slotShareGroup=" + slotShareGroup +
			", args=" + args +
			", consumerGroup=" + consumerGroup +
			'}';
	}
}
