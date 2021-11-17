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
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helper class for generating a JSON representation of {@link PlanGraph}.
 */
public class PlanJSONGenerator {
	private final StreamGraph streamGraph;
	private final JobGraph jobGraph;
	private final boolean isEdited;
	private final Map<String, PlanGraph.StreamGraphVertex> streamNodeMappings = new LinkedHashMap<>();
	@Nullable
	private Set<Integer> editedNodes;
	/**
	 * Mapping of generateOperatorIDs and deterministicIDs.
	 */
	@Nullable
	private Map<String, String> deterministicIDMappings;

	public PlanJSONGenerator(StreamGraph streamGraph, JobGraph jobGraph) {
		this.streamGraph = streamGraph;
		this.jobGraph = jobGraph;
		this.isEdited = false;
	}

	public PlanJSONGenerator(
			StreamGraph streamGraph,
			JobGraph jobGraph,
			Set<Integer> editedNodes,
			Map<Integer, byte[]> deterministicIDs) {
		this.streamGraph = streamGraph;
		this.jobGraph = jobGraph;
		this.editedNodes = editedNodes;
		this.deterministicIDMappings = new HashMap<>();
		for (Map.Entry<Integer, OperatorIDPair> e: jobGraph.getOperatorIDMap().entrySet()) {
			this.deterministicIDMappings.put(e.getValue().getGeneratedOperatorID().toString(),
				StringUtils.byteToHexString(deterministicIDs.get(e.getKey())));
		}
		this.isEdited = true;
	}

	public String generatePlan() {
		try {
			PlanGraph planGraph = new PlanGraph();
			StreamGraphVisitor streamGraphVisitor = new StreamGraphVisitor(planGraph);
			streamGraphVisitor.visit();
			JobGraphVisitor jobGraphVisitor = new JobGraphVisitor(planGraph);
			jobGraphVisitor.visit();
			ObjectMapper mapper = new ObjectMapper();
			mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			return mapper.writeValueAsString(planGraph);
		} catch (IOException e) {
			throw new RuntimeException("Failed to generate plan", e);
		}
	}

	private class StreamGraphVisitor {
		private final PlanGraph planGraph;
		private final Map<Integer, OperatorIDPair> operatorIDMap;

		public StreamGraphVisitor(PlanGraph planGraph) {
			this.planGraph = planGraph;
			this.operatorIDMap = jobGraph.getOperatorIDMap();
		}

		private void visit() {
			List<Integer> operatorIDs = new ArrayList<>(streamGraph.getVertexIDs());
			Comparator<Integer> operatorIDComparator = Comparator
				.comparingInt((Integer id) -> streamGraph.getSinkIDs().contains(id) ? 1 : 0)
				.thenComparingInt(id -> id);
			operatorIDs.sort(operatorIDComparator);
			Set<String> sourceHashes = new HashSet<>();
			visit(operatorIDs, sourceHashes);
			planGraph.setStreamNodes(new ArrayList<>(streamNodeMappings.values()));
			planGraph.setSources(sourceHashes);
		}

		private void visit(
				List<Integer> toVisit,
				Set<String> sourceHashes) {
			Integer vertexID = toVisit.get(0);
			StreamNode vertex = streamGraph.getStreamNode(vertexID);
			PlanGraph.StreamGraphVertex node = new PlanGraph.StreamGraphVertex();
			decorateNode(node, vertex);

			if (!streamGraph.getSourceIDs().contains(vertexID)) {
				List<PlanGraph.StreamGraphEdge> inputs = new ArrayList<>();
				node.setInputs(inputs);
				for (StreamEdge inEdge : vertex.getInEdges()) {
					decorateEdge(inputs, inEdge, true);
				}
				List<PlanGraph.StreamGraphEdge> outputs = new ArrayList<>();
				node.setOutputs(outputs);
				for (StreamEdge outEdge : vertex.getOutEdges()) {
					decorateEdge(outputs, outEdge, false);
				}
			} else {
				String generatedOperatorID = operatorIDMap.get(vertex.getId()).getGeneratedOperatorID().toString();
				sourceHashes.add(getDeterministicID(generatedOperatorID));
			}
			streamNodeMappings.put(node.getGeneratedOperatorID(), node);
			toVisit.remove(vertexID);

			// We temporally ignore the iterative data stream as it's not supported now in SQL.
			if (!toVisit.isEmpty()) {
				visit(toVisit, sourceHashes);
			}
		}

		private void decorateNode(PlanGraph.StreamGraphVertex node, StreamNode vertex) {
			OperatorIDPair idPair = operatorIDMap.get(vertex.getId());
			String generatedOperatorID = idPair.getGeneratedOperatorID().toString();
			if (isEdited && editedNodes.contains(vertex.getId())) {
				node.setIsChanged(true);
				// Only when applying old configs to new graph, editedNodes will be set.
				node.setIsFromOldGraph(true);
			}
			node.setNodeDesc(getDeterministicID(generatedOperatorID));
			node.setName(vertex.getOperatorMetricName());
			node.setGeneratedOperatorID(generatedOperatorID);
			node.setUserProvidedHash(vertex.getUserHash());
			node.setDescription(vertex.getOperatorName());
			node.setParallelism(vertex.getParallelism());
			node.setHasState(vertex.isHasState());
		}

		private void decorateEdge(
				List<PlanGraph.StreamGraphEdge> edges,
				StreamEdge streamEdge,
				boolean isInput) {
			PlanGraph.StreamGraphEdge edge = new PlanGraph.StreamGraphEdge();
			edges.add(edge);
			String generatedOperatorID;
			if (isInput) {
				generatedOperatorID = operatorIDMap.get(streamEdge.getSourceId()).getGeneratedOperatorID().toString();
				edge.setNodeName(streamEdge.getSourceOperatorName());
			} else {
				generatedOperatorID = operatorIDMap.get(streamEdge.getTargetId()).getGeneratedOperatorID().toString();
				edge.setNodeName(streamEdge.getTargetOperatorName());
			}
			edge.setNodeDesc(getDeterministicID(generatedOperatorID));
			edge.setShipStrategy(streamEdge.getPartitioner().toString());
		}
	}

	private class JobGraphVisitor {
		private final PlanGraph planGraph;

		public JobGraphVisitor(PlanGraph planGraph) {
			this.planGraph = planGraph;
		}

		private void visit() {
			List<PlanGraph.JobGraphVertex> nodes = new ArrayList<>();
			for (JobVertex vertex : jobGraph.getVertices()) {
				PlanGraph.JobGraphVertex node = new PlanGraph.JobGraphVertex();
				nodes.add(node);
				decorateNode(node, vertex);
				if (!vertex.isInputVertex()) {
					// write the input edge properties
					List<PlanGraph.JobGraphInput> inputs = new ArrayList<>();
					node.setInputs(inputs);
					for (JobEdge edge : vertex.getInputs()) {
						if (edge.getSource() == null) {
							continue;
						}
						decorateEdge(inputs, edge);
					}
				}
				unfoldChain(node, vertex);
			}
			planGraph.setJobVertices(nodes);
		}

		private void decorateNode(PlanGraph.JobGraphVertex node, JobVertex vertex) {
			String name = vertex.getOperatorPrettyName() != null ?
				vertex.getOperatorPrettyName() : vertex.getMetricName();
			// write the core properties
			node.setId(getDeterministicID(vertex.getID().toString()));
			node.setParallelism(vertex.getParallelism());
			node.setName(name);
		}

		private void decorateEdge(List<PlanGraph.JobGraphInput> inputs, JobEdge edge) {
			JobVertex predecessor = edge.getSource().getProducer();
			String shipStrategy = edge.getShipStrategyName();
			PlanGraph.JobGraphInput input = new PlanGraph.JobGraphInput();
			inputs.add(input);
			input.setId(getDeterministicID(predecessor.getID().toString()));
			if (shipStrategy != null) {
				input.setShipStrategy(shipStrategy);
			}
		}

		/**
		 * Unfold the chained operators in one job vertex.
		 */
		private void unfoldChain(PlanGraph.JobGraphVertex node, JobVertex vertex) {
			List<String> containedNodes = new ArrayList<>();
			node.setContainedNodes(containedNodes);
			for (OperatorIDPair id : vertex.getOperatorIDs()) {
				String generatedOperatorID = id.getGeneratedOperatorID().toString();
				containedNodes.add(getDeterministicID(generatedOperatorID));
				streamNodeMappings.get(generatedOperatorID)
					.setJobVertexID(getDeterministicID(vertex.getID().toString()));
			}
		}
	}

	/**
	 * return the deterministic id of the node. When the graph hasn't been edited.
	 * the deterministic id is the same as the generatedID.
	 */
	private String getDeterministicID(String generatedOperatorID) {
		if (isEdited) {
			return deterministicIDMappings.get(generatedOperatorID);
		} else {
			return generatedOperatorID;
		}
	}
}
