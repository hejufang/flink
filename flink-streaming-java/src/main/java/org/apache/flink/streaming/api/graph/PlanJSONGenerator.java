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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Helper class for generating a JSON representation of {@link PlanGraph}.
 */
public class PlanJSONGenerator {
	private final StreamGraph streamGraph;
	private final JobGraph jobGraph;

	public PlanJSONGenerator(StreamGraph streamGraph, JobGraph jobGraph) {
		this.streamGraph = streamGraph;
		this.jobGraph = jobGraph;
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
			List<StreamGraphVertex> streamNodes = new ArrayList<>();
			visit(streamNodes, operatorIDs);
			planGraph.setStreamNodes(streamNodes);
		}

		private void visit(List<StreamGraphVertex> streamNodes, List<Integer> toVisit) {
			Integer vertexID = toVisit.get(0);
			StreamNode vertex = streamGraph.getStreamNode(vertexID);
			StreamGraphVertex node = new StreamGraphVertex();
			decorateNode(node, vertex);

			if (!streamGraph.getSourceIDs().contains(vertexID)) {
				List<StreamGraphInput> inputs = new ArrayList<>();
				node.setInputs(inputs);
				for (StreamEdge inEdge : vertex.getInEdges()) {
					int inputID = inEdge.getSourceId();
					decorateEdge(inputs, inEdge, inputID);
				}
			}
			streamNodes.add(node);
			toVisit.remove(vertexID);

			// We temporally ignore the iterative data stream as it's not supported now in SQL.
			if (!toVisit.isEmpty()) {
				visit(streamNodes, toVisit);
			}
		}

		private void decorateNode(StreamGraphVertex node, StreamNode vertex) {
			String hash = operatorIDMap.get(vertex.getId()).getGeneratedOperatorID().toString();
			// todo: implement the generation logic of nodeDesc so that the nodeDesc will stay unchanged when
			// the topology is changed but the node itself is unchanged.
			node.setNodeDesc(hash);
			node.setName(vertex.getOperatorMetricName());
			node.setUid(hash);
			node.setDescription(vertex.getOperatorName());
			node.setParallelism(vertex.getParallelism());
		}

		private void decorateEdge(List<StreamGraphInput> inputs, StreamEdge inEdge, int mappedInputID) {
			StreamGraphInput input = new StreamGraphInput();
			inputs.add(input);
			input.setNodeDesc(operatorIDMap.get(mappedInputID).getGeneratedOperatorID().toString());
			input.setShipStrategy(inEdge.getPartitioner().toString());
		}
	}

	private class JobGraphVisitor {
		private final PlanGraph planGraph;

		public JobGraphVisitor(PlanGraph planGraph) {
			this.planGraph = planGraph;
		}

		private void visit() {
			List<JobGraphVertex> nodes = new ArrayList<>();
			for (JobVertex vertex : jobGraph.getVertices()) {
				JobGraphVertex node = new JobGraphVertex();
				nodes.add(node);
				decorateNode(node, vertex);
				if (!vertex.isInputVertex()) {
					// write the input edge properties
					List<JobGraphInput> inputs = new ArrayList<>();
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

		private void decorateNode(JobGraphVertex node, JobVertex vertex) {
			String name = vertex.getOperatorPrettyName() != null ?
				vertex.getOperatorPrettyName() : vertex.getMetricName();
			// write the core properties
			node.setId(vertex.getID().toString());
			node.setParallelism(vertex.getParallelism());
			node.setName(name);
		}

		private void decorateEdge(List<JobGraphInput> inputs, JobEdge edge) {
			JobVertex predecessor = edge.getSource().getProducer();
			String shipStrategy = edge.getShipStrategyName();
			JobGraphInput input = new JobGraphInput();
			inputs.add(input);
			input.setId(predecessor.getID().toString());
			if (shipStrategy != null) {
				input.setShipStrategy(shipStrategy);
			}
		}

		/**
		 * Unfold the chained operators in one job vertex.
		 */
		private void unfoldChain(JobGraphVertex node, JobVertex vertex) {
			List<String> containedNodes = new ArrayList<>();
			node.setContainedNodes(containedNodes);
			for (OperatorIDPair id : vertex.getOperatorIDs()) {
				containedNodes.add(id.getGeneratedOperatorID().toString());
			}
		}

	}
}
