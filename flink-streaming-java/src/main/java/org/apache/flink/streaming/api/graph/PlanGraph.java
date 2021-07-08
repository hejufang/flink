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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import java.util.List;

/**
 * Class representing the plan topology. It contains information
 * from both {@link StreamGraph} and {@link JobGraph}.
 */
public class PlanGraph {
	private List<JobGraphVertex> jobVertices;
	private List<StreamGraphVertex> streamNodes;

	public List<JobGraphVertex> getJobVertices() {
		return jobVertices;
	}

	public void setJobVertices(List<JobGraphVertex> jobVertices) {
		this.jobVertices = jobVertices;
	}

	public List<StreamGraphVertex> getStreamNodes() {
		return streamNodes;
	}

	public void setStreamNodes(List<StreamGraphVertex> streamNodes) {
		this.streamNodes = streamNodes;
	}
}

/**
 * Class representing the vertex in {@link JobGraph}.
 */
class JobGraphVertex {
	/**
	 * Id of {@link JobVertex}.
	 */
	private String id;
	/**
	 * Parallelism of the {@link JobVertex}.
	 */
	private int parallelism;
	/**
	 * Name of {@link JobVertex}.
	 */
	private String name;
	/**
	 * Flag indicating whether this vertex contains operators that use states.
	 */
	private Boolean useState;
	/**
	 * Flag indicating whether the properties of this vertex has been changed comparing
	 * with last version of the job.
	 */
	private Boolean isChanged;
	private List<JobGraphInput> inputs;
	/**
	 * NodeDesc of the contained operators in this vertex.
	 */
	private List<String> containedNodes;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Boolean getUseState() {
		return useState;
	}

	public void setUseState(Boolean useState) {
		this.useState = useState;
	}

	public Boolean getChanged() {
		return isChanged;
	}

	public void setChanged(Boolean changed) {
		isChanged = changed;
	}

	public List<JobGraphInput> getInputs() {
		return inputs;
	}

	public void setInputs(List<JobGraphInput> inputs) {
		this.inputs = inputs;
	}

	public List<String> getContainedNodes() {
		return containedNodes;
	}

	public void setContainedNodes(List<String> containedNodes) {
		this.containedNodes = containedNodes;
	}
}

/**
 * Class representing the input of vertex in {@link JobGraph}.
 */
class JobGraphInput {
	/**
	 * The id of the input {@link JobVertex}.
	 */
	private String id;
	/**
	 * The string representing the {@link StreamPartitioner} on edge linking
	 * the input {@link JobVertex} and the given node.
	 */
	private String shipStrategy;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getShipStrategy() {
		return shipStrategy;
	}

	public void setShipStrategy(String shipStrategy) {
		this.shipStrategy = shipStrategy;
	}
}

/**
 * Class representing the vertex in {@link StreamGraph}.
 */
class StreamGraphVertex {
	/**
	 * Unique identification of the {@link StreamNode}.
	 */
	private String nodeDesc;
	/**
	 * Operator metric name of the {@link StreamNode}.
	 */
	private String name;
	/**
	 * GeneratedOperatorID of {@link OperatorIDPair} of the {@link StreamNode}.
	 */
	private String uid;
	/**
	 * Operator name of the {@link StreamNode}.
	 */
	private String description;
	/**
	 * Parallelism of the {@link StreamNode}.
	 */
	private int parallelism;
	/**
	 * Flag indicating whether the operator of this {@link StreamNode} use states.
	 */
	private Boolean useState;
	/**
	 * Flag indicating whether the properties of this {@link StreamNode} has been changed comparing
	 * with last version of the job.
	 */
	private Boolean isChanged;
	private List<StreamGraphInput> inputs;

	public String getNodeDesc() {
		return nodeDesc;
	}

	public void setNodeDesc(String nodeDesc) {
		this.nodeDesc = nodeDesc;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public int getParallelism() {
		return parallelism;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public Boolean getUseState() {
		return useState;
	}

	public void setUseState(Boolean useState) {
		this.useState = useState;
	}

	public Boolean getChanged() {
		return isChanged;
	}

	public void setChanged(Boolean changed) {
		isChanged = changed;
	}

	public List<StreamGraphInput> getInputs() {
		return inputs;
	}

	public void setInputs(List<StreamGraphInput> inputs) {
		this.inputs = inputs;
	}
}

/**
 * Class representing the input of vertex in {@link StreamGraph}.
 */
class StreamGraphInput {
	/**
	 * String representing NodeDesc of the input {@link StreamNode}.
	 */
	private String nodeDesc;
	/**
	 * The string representing the {@link StreamPartitioner} on edge linking
	 * the input {@link StreamNode} and the given node.
	 */
	private String shipStrategy;

	public String getNodeDesc() {
		return nodeDesc;
	}

	public void setNodeDesc(String nodeDesc) {
		this.nodeDesc = nodeDesc;
	}

	public String getShipStrategy() {
		return shipStrategy;
	}

	public void setShipStrategy(String shipStrategy) {
		this.shipStrategy = shipStrategy;
	}
}
