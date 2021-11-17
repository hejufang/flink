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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class representing the plan topology. It contains information
 * from both {@link StreamGraph} and {@link JobGraph}.
 */
public class PlanGraph {
	private List<JobGraphVertex> jobVertices;
	private List<StreamGraphVertex> streamNodes;
	private Set<String> sources;

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

	public Set<String> getSources() {
		return sources;
	}

	public void setSources(Set<String> sources) {
		this.sources = sources;
	}

	/**
	 * Class representing the vertex in {@link JobGraph}.
	 */
	public static class JobGraphVertex {
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
	public static class JobGraphInput {
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
	public static class StreamGraphVertex {
		private static final int INPUT_WEIGHT = 2;
		private static final int OUTPUT_WEIGHT = 1;
		/**
		 * GeneratedOperatorID of the {@link StreamNode}.
		 */
		private String nodeDesc;
		/**
		 * Operator metric name of the {@link StreamNode}.
		 */
		private String name;
		/**
		 * ID of job vertex this node belongs to.
		 */
		private String jobVertexID;

		private String generatedOperatorID;
		/**
		 * User provided hash of the {@link StreamNode}.
		 */
		private String userProvidedHash;
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
		private boolean hasState;
		/**
		 * Flag indicating whether the properties of this {@link StreamNode} has been changed comparing
		 * with last version of the job.
		 */
		private boolean isChanged;
		private boolean isFromOldGraph;
		private List<StreamGraphEdge> inputs;
		private List<StreamGraphEdge> outputs;

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

		public String getJobVertexID() {
			return jobVertexID;
		}

		public void setJobVertexID(String jobVertexID) {
			this.jobVertexID = jobVertexID;
		}

		public String getGeneratedOperatorID() {
			return generatedOperatorID;
		}

		public void setGeneratedOperatorID(String generatedOperatorID) {
			this.generatedOperatorID = generatedOperatorID;
		}

		public String getUserProvidedHash() {
			return userProvidedHash;
		}

		public void setUserProvidedHash(String userProvidedHash) {
			this.userProvidedHash = userProvidedHash;
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

		public List<StreamGraphEdge> getInputs() {
			return inputs;
		}

		public void setInputs(List<StreamGraphEdge> inputs) {
			this.inputs = inputs;
		}

		public List<StreamGraphEdge> getOutputs() {
			return outputs;
		}

		public void setOutputs(List<StreamGraphEdge> outputs) {
			this.outputs = outputs;
		}

		public boolean isHasState() {
			return hasState;
		}

		public void setHasState(boolean hasState) {
			this.hasState = hasState;
		}

		public boolean getIsChanged() {
			return isChanged;
		}

		public void setIsChanged(boolean changed) {
			isChanged = changed;
		}

		public boolean getIsFromOldGraph() {
			return isFromOldGraph;
		}

		public void setIsFromOldGraph(boolean isFromOldGraph) {
			this.isFromOldGraph = isFromOldGraph;
		}

		public int calcSimilarity(StreamNode other) {
			int similarity = 0;
			if (this.getInputs() != null && other.getInEdges().size() > 0) {
				Set<String> inputNodeName = new HashSet<>();
				for (StreamGraphEdge thisEdge : this.getInputs()) {
					inputNodeName.add(thisEdge.getNodeName());
				}
				for (StreamEdge otherEdge : other.getInEdges()) {
					if (inputNodeName.contains(otherEdge.getSourceOperatorName())) {
						similarity += INPUT_WEIGHT;
					}
				}
			}
			if (this.getOutputs() != null && other.getOutEdges().size() > 0) {
				Set<String> outputNodeName = new HashSet<>();
				for (StreamGraphEdge thisEdge : this.getOutputs()) {
					outputNodeName.add(thisEdge.getNodeName());
				}
				for (StreamEdge otherEdge : other.getOutEdges()) {
					if (outputNodeName.contains(otherEdge.getTargetOperatorName())) {
						similarity += OUTPUT_WEIGHT;
					}
				}
			}
			return similarity;
		}
	}

	/**
	 * Class representing the input of vertex in {@link StreamGraph}.
	 */
	public static class StreamGraphEdge {
		/**
		 * String representing NodeDesc of the input/output {@link StreamNode}.
		 */
		private String nodeDesc;

		private String nodeName;
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

		public String getNodeName() {
			return nodeName;
		}

		public void setNodeName(String nodeName) {
			this.nodeName = nodeName;
		}

		public String getShipStrategy() {
			return shipStrategy;
		}

		public void setShipStrategy(String shipStrategy) {
			this.shipStrategy = shipStrategy;
		}
	}

	/**
	 * Utils for manipulating the plan graph.
	 */
	public static class Utils {
		public static PlanGraph resolvePlanGraph(String planGraphJson) {
			ObjectMapper objectMapper = new ObjectMapper();
			try {
				return objectMapper.readValue(planGraphJson, PlanGraph.class);
			} catch (JsonProcessingException e) {
				throw new IllegalStateException(String.format("The json of plan cannot be resolved." +
					"The json is %s", planGraphJson), e);
			}
		}
	}
}

