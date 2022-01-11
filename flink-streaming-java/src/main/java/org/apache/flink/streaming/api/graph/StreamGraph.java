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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class representing the streaming topology. It contains all the information
 * necessary to build the jobgraph for the execution.
 *
 */
@Internal
public class StreamGraph implements Pipeline {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraph.class);

	public static final String ITERATION_SOURCE_NAME_PREFIX = "IterationSource";

	public static final String ITERATION_SINK_NAME_PREFIX = "IterationSink";

	private String jobName;

	private final ExecutionConfig executionConfig;
	private final CheckpointConfig checkpointConfig;
	private SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();

	private ScheduleMode scheduleMode;

	private boolean chaining;

	private Collection<Tuple2<String, DistributedCache.DistributedCacheEntry>> userArtifacts;

	private TimeCharacteristic timeCharacteristic;

	private GlobalDataExchangeMode globalDataExchangeMode;

	/** Flag to indicate whether to put all vertices into the same slot sharing group by default. */
	private boolean allVerticesInSameSlotSharingGroupByDefault = true;

	private boolean isBatchJob = false;

	/** Flag to indicate whether user-provided hashes will be take accounted into operator id generating.*/
	private boolean userProvidedHashInvolved;

	private Map<Integer, StreamNode> streamNodes;
	private Set<Integer> sources;
	private Set<Integer> boundedSources;
	private Set<Integer> sinks;
	private Map<Integer, Tuple2<Integer, List<String>>> virtualSelectNodes;
	private Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes;
	private Map<Integer, Tuple3<Integer, StreamPartitioner<?>, ShuffleMode>> virtualPartitionNodes;

	protected Map<Integer, String> vertexIDtoBrokerID;
	protected Map<Integer, Long> vertexIDtoLoopTimeout;
	private StateBackend stateBackend;
	private Set<Tuple2<StreamNode, StreamNode>> iterationSourceSinkPairs;

	public StreamGraph(ExecutionConfig executionConfig, CheckpointConfig checkpointConfig, SavepointRestoreSettings savepointRestoreSettings) {
		this.executionConfig = checkNotNull(executionConfig);
		this.checkpointConfig = checkNotNull(checkpointConfig);
		this.savepointRestoreSettings = checkNotNull(savepointRestoreSettings);

		// create an empty new stream graph.
		clear();
	}

	/**
	 * Remove all registered nodes etc.
	 */
	public void clear() {
		streamNodes = new TreeMap<>();
		virtualSelectNodes = new HashMap<>();
		virtualSideOutputNodes = new HashMap<>();
		virtualPartitionNodes = new HashMap<>();
		vertexIDtoBrokerID = new HashMap<>();
		vertexIDtoLoopTimeout  = new HashMap<>();
		iterationSourceSinkPairs = new HashSet<>();
		sources = new LinkedHashSet<>();
		boundedSources = new LinkedHashSet<>();
		sinks = new HashSet<>();
	}

	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	public CheckpointConfig getCheckpointConfig() {
		return checkpointConfig;
	}

	public void setSavepointRestoreSettings(SavepointRestoreSettings savepointRestoreSettings) {
		this.savepointRestoreSettings = savepointRestoreSettings;
	}

	public SavepointRestoreSettings getSavepointRestoreSettings() {
		return savepointRestoreSettings;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void setChaining(boolean chaining) {
		this.chaining = chaining;
	}

	public void setIsBatchJob(boolean isBatchJob) {
		this.isBatchJob = isBatchJob;
	}

	public void setUserProvidedHashInvolved(boolean userProvidedHashInvolved) {
		this.userProvidedHashInvolved = userProvidedHashInvolved;
	}

	public void setStateBackend(StateBackend backend) {
		this.stateBackend = backend;
	}

	public StateBackend getStateBackend() {
		return this.stateBackend;
	}

	public ScheduleMode getScheduleMode() {
		return scheduleMode;
	}

	public void setScheduleMode(ScheduleMode scheduleMode) {
		this.scheduleMode = scheduleMode;
	}

	public Collection<Tuple2<String, DistributedCache.DistributedCacheEntry>> getUserArtifacts() {
		return userArtifacts;
	}

	public void setUserArtifacts(Collection<Tuple2<String, DistributedCache.DistributedCacheEntry>> userArtifacts) {
		this.userArtifacts = userArtifacts;
	}

	public TimeCharacteristic getTimeCharacteristic() {
		return timeCharacteristic;
	}

	public void setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		this.timeCharacteristic = timeCharacteristic;
	}

	public GlobalDataExchangeMode getGlobalDataExchangeMode() {
		return globalDataExchangeMode;
	}

	public void setGlobalDataExchangeMode(GlobalDataExchangeMode globalDataExchangeMode) {
		this.globalDataExchangeMode = globalDataExchangeMode;
	}

	/**
	 * Set whether to put all vertices into the same slot sharing group by default.
	 *
	 * @param allVerticesInSameSlotSharingGroupByDefault indicates whether to put all vertices
	 *                                                   into the same slot sharing group by default.
	 */
	public void setAllVerticesInSameSlotSharingGroupByDefault(boolean allVerticesInSameSlotSharingGroupByDefault) {
		this.allVerticesInSameSlotSharingGroupByDefault = allVerticesInSameSlotSharingGroupByDefault;
	}

	/**
	 * Gets whether to put all vertices into the same slot sharing group by default.
	 *
	 * @return whether to put all vertices into the same slot sharing group by default.
	 */
	public boolean isAllVerticesInSameSlotSharingGroupByDefault() {
		return allVerticesInSameSlotSharingGroupByDefault;
	}

	// Checkpointing

	public boolean isChainingEnabled() {
		return chaining;
	}

	public boolean isIterative() {
		return !vertexIDtoLoopTimeout.isEmpty();
	}

	public <IN, OUT> void addSource(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			SourceOperatorFactory<OUT> operatorFactory,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {
		addOperator(
				vertexID,
				slotSharingGroup,
				coLocationGroup,
				operatorFactory,
				inTypeInfo,
				outTypeInfo,
				operatorName,
				SourceOperatorStreamTask.class);
		sources.add(vertexID);
	}

	public <IN, OUT> void addLegacySource(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {
		addOperator(vertexID, slotSharingGroup, coLocationGroup, operatorFactory, inTypeInfo, outTypeInfo, operatorName);
		sources.add(vertexID);
	}

	public void addBoundedInput(Integer vertexID) {
		boundedSources.add(vertexID);
	}

	public <IN, OUT> void addSink(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {
		addOperator(vertexID, slotSharingGroup, coLocationGroup, operatorFactory, inTypeInfo, outTypeInfo, operatorName);
		sinks.add(vertexID);
	}

	public <IN, OUT> void addOperator(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {
		Class<? extends AbstractInvokable> invokableClass =
				operatorFactory.isStreamSource() ? SourceStreamTask.class : OneInputStreamTask.class;
		addOperator(vertexID, slotSharingGroup, coLocationGroup, operatorFactory, inTypeInfo,
				outTypeInfo, operatorName, invokableClass);
	}

	private <IN, OUT> void addOperator(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName,
			Class<? extends AbstractInvokable> invokableClass) {

		addNode(vertexID, slotSharingGroup, coLocationGroup, invokableClass, operatorFactory, operatorName);
		setSerializers(vertexID, createSerializer(inTypeInfo), null, createSerializer(outTypeInfo));

		if (operatorFactory.isOutputTypeConfigurable() && outTypeInfo != null) {
			// sets the output type which must be know at StreamGraph creation time
			operatorFactory.setOutputType(outTypeInfo, executionConfig);
		}

		if (operatorFactory.isInputTypeConfigurable()) {
			operatorFactory.setInputType(inTypeInfo, executionConfig);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex: {}", vertexID);
		}
	}

	public <IN1, IN2, OUT> void addCoOperator(
			Integer vertexID,
			String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperatorFactory<OUT> taskOperatorFactory,
			TypeInformation<IN1> in1TypeInfo,
			TypeInformation<IN2> in2TypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {

		Class<? extends AbstractInvokable> vertexClass = TwoInputStreamTask.class;

		addNode(vertexID, slotSharingGroup, coLocationGroup, vertexClass, taskOperatorFactory, operatorName);

		TypeSerializer<OUT> outSerializer = createSerializer(outTypeInfo);

		setSerializers(vertexID, in1TypeInfo.createSerializer(executionConfig), in2TypeInfo.createSerializer(executionConfig), outSerializer);

		if (taskOperatorFactory.isOutputTypeConfigurable()) {
			// sets the output type which must be know at StreamGraph creation time
			taskOperatorFactory.setOutputType(outTypeInfo, executionConfig);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("CO-TASK: {}", vertexID);
		}
	}

	public <OUT> void addMultipleInputOperator(
			Integer vertexID,
			String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperatorFactory<OUT> operatorFactory,
			List<TypeInformation<?>> inTypeInfos,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {

		Class<? extends AbstractInvokable> vertexClass = MultipleInputStreamTask.class;

		addNode(vertexID, slotSharingGroup, coLocationGroup, vertexClass, operatorFactory, operatorName);

		setSerializers(vertexID, inTypeInfos, createSerializer(outTypeInfo));

		if (operatorFactory.isOutputTypeConfigurable()) {
			// sets the output type which must be know at StreamGraph creation time
			operatorFactory.setOutputType(outTypeInfo, executionConfig);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("CO-TASK: {}", vertexID);
		}
	}

	protected StreamNode addNode(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			Class<? extends AbstractInvokable> vertexClass,
			StreamOperatorFactory<?> operatorFactory,
			String operatorName) {

		if (streamNodes.containsKey(vertexID)) {
			throw new RuntimeException("Duplicate vertexID " + vertexID);
		}

		StreamNode vertex = new StreamNode(
				vertexID,
				slotSharingGroup,
				coLocationGroup,
				operatorFactory,
				operatorName,
				new ArrayList<OutputSelector<?>>(),
				vertexClass);

		streamNodes.put(vertexID, vertex);

		return vertex;
	}

	/**
	 * Adds a new virtual node that is used to connect a downstream vertex to only the outputs
	 * with the selected names.
	 *
	 * <p>When adding an edge from the virtual node to a downstream node the connection will be made
	 * to the original node, only with the selected names given here.
	 *
	 * @param originalId ID of the node that should be connected to.
	 * @param virtualId ID of the virtual node.
	 * @param selectedNames The selected names.
	 */
	public void addVirtualSelectNode(Integer originalId, Integer virtualId, List<String> selectedNames) {

		if (virtualSelectNodes.containsKey(virtualId)) {
			throw new IllegalStateException("Already has virtual select node with id " + virtualId);
		}

		virtualSelectNodes.put(virtualId,
				new Tuple2<Integer, List<String>>(originalId, selectedNames));
	}

	/**
	 * Adds a new virtual node that is used to connect a downstream vertex to only the outputs with
	 * the selected side-output {@link OutputTag}.
	 *
	 * @param originalId ID of the node that should be connected to.
	 * @param virtualId ID of the virtual node.
	 * @param outputTag The selected side-output {@code OutputTag}.
	 */
	public void addVirtualSideOutputNode(Integer originalId, Integer virtualId, OutputTag outputTag) {

		if (virtualSideOutputNodes.containsKey(virtualId)) {
			throw new IllegalStateException("Already has virtual output node with id " + virtualId);
		}

		// verify that we don't already have a virtual node for the given originalId/outputTag
		// combination with a different TypeInformation. This would indicate that someone is trying
		// to read a side output from an operation with a different type for the same side output
		// id.

		for (Tuple2<Integer, OutputTag> tag : virtualSideOutputNodes.values()) {
			if (!tag.f0.equals(originalId)) {
				// different source operator
				continue;
			}

			if (tag.f1.getId().equals(outputTag.getId()) &&
					!tag.f1.getTypeInfo().equals(outputTag.getTypeInfo())) {
				throw new IllegalArgumentException("Trying to add a side output for the same " +
						"side-output id with a different type. This is not allowed. Side-output ID: " +
						tag.f1.getId());
			}
		}

		virtualSideOutputNodes.put(virtualId, new Tuple2<>(originalId, outputTag));
	}

	/**
	 * Adds a new virtual node that is used to connect a downstream vertex to an input with a
	 * certain partitioning.
	 *
	 * <p>When adding an edge from the virtual node to a downstream node the connection will be made
	 * to the original node, but with the partitioning given here.
	 *
	 * @param originalId ID of the node that should be connected to.
	 * @param virtualId ID of the virtual node.
	 * @param partitioner The partitioner
	 */
	public void addVirtualPartitionNode(
			Integer originalId,
			Integer virtualId,
			StreamPartitioner<?> partitioner,
			ShuffleMode shuffleMode) {

		if (virtualPartitionNodes.containsKey(virtualId)) {
			throw new IllegalStateException("Already has virtual partition node with id " + virtualId);
		}

		virtualPartitionNodes.put(virtualId, new Tuple3<>(originalId, partitioner, shuffleMode));
	}

	/**
	 * Determines the slot sharing group of an operation across virtual nodes.
	 */
	public String getSlotSharingGroup(Integer id) {
		if (virtualSideOutputNodes.containsKey(id)) {
			Integer mappedId = virtualSideOutputNodes.get(id).f0;
			return getSlotSharingGroup(mappedId);
		} else if (virtualSelectNodes.containsKey(id)) {
			Integer mappedId = virtualSelectNodes.get(id).f0;
			return getSlotSharingGroup(mappedId);
		} else if (virtualPartitionNodes.containsKey(id)) {
			Integer mappedId = virtualPartitionNodes.get(id).f0;
			return getSlotSharingGroup(mappedId);
		} else {
			StreamNode node = getStreamNode(id);
			return node.getSlotSharingGroup();
		}
	}

	public void addEdge(Integer upStreamVertexID, Integer downStreamVertexID, int typeNumber) {
		addEdgeInternal(upStreamVertexID,
				downStreamVertexID,
				typeNumber,
				null,
				new ArrayList<String>(),
				null,
				null);

	}

	private void addEdgeInternal(Integer upStreamVertexID,
			Integer downStreamVertexID,
			int typeNumber,
			StreamPartitioner<?> partitioner,
			List<String> outputNames,
			OutputTag outputTag,
			ShuffleMode shuffleMode) {

		if (virtualSideOutputNodes.containsKey(upStreamVertexID)) {
			int virtualId = upStreamVertexID;
			upStreamVertexID = virtualSideOutputNodes.get(virtualId).f0;
			if (outputTag == null) {
				outputTag = virtualSideOutputNodes.get(virtualId).f1;
			}
			addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, null, outputTag, shuffleMode);
		} else if (virtualSelectNodes.containsKey(upStreamVertexID)) {
			int virtualId = upStreamVertexID;
			upStreamVertexID = virtualSelectNodes.get(virtualId).f0;
			if (outputNames.isEmpty()) {
				// selections that happen downstream override earlier selections
				outputNames = virtualSelectNodes.get(virtualId).f1;
			}
			addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, outputNames, outputTag, shuffleMode);
		} else if (virtualPartitionNodes.containsKey(upStreamVertexID)) {
			int virtualId = upStreamVertexID;
			upStreamVertexID = virtualPartitionNodes.get(virtualId).f0;
			if (partitioner == null) {
				partitioner = virtualPartitionNodes.get(virtualId).f1;
			}
			shuffleMode = virtualPartitionNodes.get(virtualId).f2;
			addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, outputNames, outputTag, shuffleMode);
		} else {
			StreamNode upstreamNode = getStreamNode(upStreamVertexID);
			StreamNode downstreamNode = getStreamNode(downStreamVertexID);

			// If no partitioner was specified and the parallelism of upstream and downstream
			// operator matches use forward partitioning, use rebalance otherwise.
			if (partitioner == null && upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
				partitioner = new ForwardPartitioner<Object>();
			} else if (partitioner == null) {
				if (executionConfig.getDefaultPartitioner() == ExecutionConfig.DefaultPartitioner.REBALANCE) {
					partitioner = new RebalancePartitioner<Object>();
				} else {
					partitioner = new RescalePartitioner<Object>();
				}
			}

			if (partitioner instanceof ForwardPartitioner) {
				if (upstreamNode.getParallelism() != downstreamNode.getParallelism()) {
					throw new UnsupportedOperationException("Forward partitioning does not allow " +
							"change of parallelism. Upstream operation: " + upstreamNode + " parallelism: " + upstreamNode.getParallelism() +
							", downstream operation: " + downstreamNode + " parallelism: " + downstreamNode.getParallelism() +
							" You must use another partitioning strategy, such as broadcast, rebalance, shuffle or global.");
				}
			}

			if (shuffleMode == null) {
				shuffleMode = ShuffleMode.UNDEFINED;
			}

			StreamEdge edge = new StreamEdge(upstreamNode, downstreamNode, typeNumber, outputNames, partitioner, outputTag, shuffleMode);

			getStreamNode(edge.getSourceId()).addOutEdge(edge);
			getStreamNode(edge.getTargetId()).addInEdge(edge);
		}
	}

	public <T> void addOutputSelector(Integer vertexID, OutputSelector<T> outputSelector) {
		if (virtualPartitionNodes.containsKey(vertexID)) {
			addOutputSelector(virtualPartitionNodes.get(vertexID).f0, outputSelector);
		} else if (virtualSelectNodes.containsKey(vertexID)) {
			addOutputSelector(virtualSelectNodes.get(vertexID).f0, outputSelector);
		} else {
			getStreamNode(vertexID).addOutputSelector(outputSelector);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Outputselector set for {}", vertexID);
			}
		}

	}

	public void setParallelism(Integer vertexID, int parallelism) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setParallelism(parallelism);
		}
	}

	/**
	 * Called after update parallelism of stream nodes to update all in and out edges, as the {@link StreamPartitioner}
	 * of edges may change as well.
	 */
	private void updateEdgesForStreamNodes(Collection<Integer> nodeIndices) {
		for (Integer vertexID: nodeIndices) {
			StreamNode node = getStreamNode(vertexID);
			if (node != null) {
				for (StreamEdge inEdge: node.getInEdges()) {
					updatePartitionerOfEdge(inEdge);
				}
				for (StreamEdge outEdge: node.getOutEdges()) {
					updatePartitionerOfEdge(outEdge);
				}
			}
		}
	}

	/**
	 * Update the partitioner of edge.
	 * Currently, only {@link ForwardPartitioner}, {@link RebalancePartitioner} and
	 * {@link RescalePartitioner} should be updated after changing the parallelism of
	 * {@link StreamNode}.
	 */
	private void updatePartitionerOfEdge(StreamEdge edge) {
		if (edge.getPartitioner() instanceof ForwardPartitioner) {
			StreamNode upstreamNode = getStreamNode(edge.getSourceId());
			StreamNode downstreamNode = getStreamNode(edge.getTargetId());
			StreamPartitioner<?> newPartitioner;
			if (upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
				newPartitioner = new ForwardPartitioner<>();
			} else if (executionConfig.getDefaultPartitioner() == ExecutionConfig.DefaultPartitioner.REBALANCE) {
				newPartitioner = new RebalancePartitioner<>();
			} else {
				newPartitioner = new RescalePartitioner<>();
			}
			// As the up and down stream node use the same edge object. See details in #addEdgeInternal
			// We can just update the edge from one side.
			edge.setPartitioner(newPartitioner);
		} else if (edge.getPartitioner() instanceof RebalancePartitioner
				|| edge.getPartitioner() instanceof RescalePartitioner) {
			StreamNode upstreamNode = getStreamNode(edge.getSourceId());
			StreamNode downstreamNode = getStreamNode(edge.getTargetId());
			if (upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
				edge.setPartitioner(new ForwardPartitioner<>());
			}
		}
	}

	public void setMaxParallelism(int vertexID, int maxParallelism) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setMaxParallelism(maxParallelism);
		}
	}

	public void setIsUseDefaultParallelism(Integer vertexID, boolean isUseDefaultParallelism) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setUseDefaultParallelism(isUseDefaultParallelism);
		}
	}

	public void setResources(int vertexID, ResourceSpec minResources, ResourceSpec preferredResources) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setResources(minResources, preferredResources);
		}
	}

	public void setManagedMemoryWeight(int vertexID, int managedMemoryWeight) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setManagedMemoryWeight(managedMemoryWeight);
		}
	}

	public void setOneInputStateKey(Integer vertexID, KeySelector<?, ?> keySelector, TypeSerializer<?> keySerializer) {
		StreamNode node = getStreamNode(vertexID);
		node.setStatePartitioners(keySelector);
		node.setStateKeySerializer(keySerializer);
	}

	public void setTwoInputStateKey(Integer vertexID, KeySelector<?, ?> keySelector1, KeySelector<?, ?> keySelector2, TypeSerializer<?> keySerializer) {
		StreamNode node = getStreamNode(vertexID);
		node.setStatePartitioners(keySelector1, keySelector2);
		node.setStateKeySerializer(keySerializer);
	}

	public void setMultipleInputStateKey(Integer vertexID, List<KeySelector<?, ?>> keySelectors, TypeSerializer<?> keySerializer) {
		StreamNode node = getStreamNode(vertexID);
		node.setStatePartitioners(keySelectors.stream().toArray(KeySelector[]::new));
		node.setStateKeySerializer(keySerializer);
	}

	public void setBufferTimeout(Integer vertexID, long bufferTimeout) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setBufferTimeout(bufferTimeout);
		}
	}

	public void setSerializers(Integer vertexID, TypeSerializer<?> in1, TypeSerializer<?> in2, TypeSerializer<?> out) {
		StreamNode vertex = getStreamNode(vertexID);
		vertex.setSerializersIn(in1, in2);
		vertex.setSerializerOut(out);
	}

	private <OUT> void setSerializers(
			Integer vertexID,
			List<TypeInformation<?>> inTypeInfos,
			TypeSerializer<OUT> out) {

		StreamNode vertex = getStreamNode(vertexID);

		vertex.setSerializersIn(
			inTypeInfos.stream()
				.map(typeInfo -> typeInfo.createSerializer(executionConfig))
				.toArray(TypeSerializer[]::new));
		vertex.setSerializerOut(out);
	}

	public void setSerializersFrom(Integer from, Integer to) {
		StreamNode fromVertex = getStreamNode(from);
		StreamNode toVertex = getStreamNode(to);

		toVertex.setSerializersIn(fromVertex.getTypeSerializerOut());
		toVertex.setSerializerOut(fromVertex.getTypeSerializerIn(0));
	}

	public <OUT> void setOutType(Integer vertexID, TypeInformation<OUT> outType) {
		getStreamNode(vertexID).setSerializerOut(outType.createSerializer(executionConfig));
	}

	public void setInputFormat(Integer vertexID, InputFormat<?, ?> inputFormat) {
		getStreamNode(vertexID).setInputFormat(inputFormat);
	}

	public void setOutputFormat(Integer vertexID, OutputFormat<?> outputFormat) {
		getStreamNode(vertexID).setOutputFormat(outputFormat);
	}

	void setTransformationUID(Integer nodeId, String transformationId) {
		StreamNode node = streamNodes.get(nodeId);
		if (node != null) {
			node.setTransformationUID(transformationId);
		}
	}

	void setTransformationUserHash(Integer nodeId, String nodeHash) {
		StreamNode node = streamNodes.get(nodeId);
		if (node != null) {
			node.setUserHash(nodeHash);
		}
	}

	void setHasState(Integer nodeId, boolean hasState) {
		StreamNode node = streamNodes.get(nodeId);
		if (node != null) {
			node.setHasState(hasState);
		}
	}

	public StreamNode getStreamNode(Integer vertexID) {
		return streamNodes.get(vertexID);
	}

	protected Collection<? extends Integer> getVertexIDs() {
		return streamNodes.keySet();
	}

	@VisibleForTesting
	public List<StreamEdge> getStreamEdges(int sourceId) {
		return getStreamNode(sourceId).getOutEdges();
	}

	@VisibleForTesting
	public List<StreamEdge> getStreamEdges(int sourceId, int targetId) {
		List<StreamEdge> result = new ArrayList<>();
		for (StreamEdge edge : getStreamNode(sourceId).getOutEdges()) {
			if (edge.getTargetId() == targetId) {
				result.add(edge);
			}
		}
		return result;
	}

	@VisibleForTesting
	@Deprecated
	public List<StreamEdge> getStreamEdgesOrThrow(int sourceId, int targetId) {
		List<StreamEdge> result = getStreamEdges(sourceId, targetId);
		if (result.isEmpty()) {
			throw new RuntimeException("No such edge in stream graph: " + sourceId + " -> " + targetId);
		}
		return result;
	}

	public Collection<Integer> getSourceIDs() {
		return sources;
	}

	public Collection<Integer> getBoundedSourceIDs() {
		return boundedSources;
	}

	public Collection<Integer> getSinkIDs() {
		return sinks;
	}

	public Collection<StreamNode> getStreamNodes() {
		return streamNodes.values();
	}

	public Set<Tuple2<Integer, StreamOperatorFactory<?>>> getAllOperatorFactory() {
		Set<Tuple2<Integer, StreamOperatorFactory<?>>> operatorSet = new HashSet<>();
		for (StreamNode vertex : streamNodes.values()) {
			operatorSet.add(new Tuple2<>(vertex.getId(), vertex.getOperatorFactory()));
		}
		return operatorSet;
	}

	public String getBrokerID(Integer vertexID) {
		return vertexIDtoBrokerID.get(vertexID);
	}

	public long getLoopTimeout(Integer vertexID) {
		return vertexIDtoLoopTimeout.get(vertexID);
	}

	public Tuple2<StreamNode, StreamNode> createIterationSourceAndSink(
		int loopId,
		int sourceId,
		int sinkId,
		long timeout,
		int parallelism,
		int maxParallelism,
		ResourceSpec minResources,
		ResourceSpec preferredResources) {

		final String coLocationGroup = "IterationCoLocationGroup-" + loopId;

		StreamNode source = this.addNode(sourceId,
			null,
			coLocationGroup,
			StreamIterationHead.class,
			null,
			ITERATION_SOURCE_NAME_PREFIX + "-" + loopId);
		sources.add(source.getId());
		setParallelism(source.getId(), parallelism);
		setMaxParallelism(source.getId(), maxParallelism);
		setResources(source.getId(), minResources, preferredResources);

		StreamNode sink = this.addNode(sinkId,
			null,
			coLocationGroup,
			StreamIterationTail.class,
			null,
			ITERATION_SINK_NAME_PREFIX + "-" + loopId);
		sinks.add(sink.getId());
		setParallelism(sink.getId(), parallelism);
		setMaxParallelism(sink.getId(), parallelism);
		// The tail node is always in the same slot sharing group with the head node
		// so that they can share resources (they do not use non-sharable resources,
		// i.e. managed memory). There is no contract on how the resources should be
		// divided for head and tail nodes at the moment. To be simple, we assign all
		// resources to the head node and set the tail node resources to be zero if
		// resources are specified.
		final ResourceSpec tailResources = minResources.equals(ResourceSpec.UNKNOWN)
			? ResourceSpec.UNKNOWN
			: ResourceSpec.ZERO;
		setResources(sink.getId(), tailResources, tailResources);

		iterationSourceSinkPairs.add(new Tuple2<>(source, sink));

		this.vertexIDtoBrokerID.put(source.getId(), "broker-" + loopId);
		this.vertexIDtoBrokerID.put(sink.getId(), "broker-" + loopId);
		this.vertexIDtoLoopTimeout.put(source.getId(), timeout);
		this.vertexIDtoLoopTimeout.put(sink.getId(), timeout);

		return new Tuple2<>(source, sink);
	}

	public Set<Tuple2<StreamNode, StreamNode>> getIterationSourceSinkPairs() {
		return iterationSourceSinkPairs;
	}

	public StreamNode getSourceVertex(StreamEdge edge) {
		return streamNodes.get(edge.getSourceId());
	}

	public StreamNode getTargetVertex(StreamEdge edge) {
		return streamNodes.get(edge.getTargetId());
	}

	private void removeEdge(StreamEdge edge) {
		getSourceVertex(edge).getOutEdges().remove(edge);
		getTargetVertex(edge).getInEdges().remove(edge);
	}

	private void removeVertex(StreamNode toRemove) {
		Set<StreamEdge> edgesToRemove = new HashSet<>();

		edgesToRemove.addAll(toRemove.getInEdges());
		edgesToRemove.addAll(toRemove.getOutEdges());

		for (StreamEdge edge : edgesToRemove) {
			removeEdge(edge);
		}
		streamNodes.remove(toRemove.getId());
	}

	/**
	 * Gets the assembled {@link JobGraph} with a random {@link JobID}.
	 */
	public JobGraph getJobGraph() {
		return getJobGraph(null);
	}

	/**
	 * Gets the assembled {@link JobGraph} with a specified {@link JobID}.
	 */
	public JobGraph getJobGraph(@Nullable JobID jobID) {
		if (executionConfig.isUseMaxSourceParallelismAsDefaultParallelism()) {
			overwriteDefaultParallelismForNonSourceNode();
		}
		return StreamingJobGraphGenerator.createJobGraph(this, jobID, userProvidedHashInvolved);
	}

	/**
	 * Overwrite parallelism of non-source nodes with the max parallelism of source
	 * if it use default parallelism.
	 *
	 * <p>TODO: Restructure the logical of overwrite parallelism, we can only overwrite the
	 * parallelism of transformations generated by SQL. The current implement is kind of trick and
	 * hard to maintain.
	 */
	private void overwriteDefaultParallelismForNonSourceNode() {
		int maxParallelismOfSource = -1;
		for (int i : getSourceIDs()) {
			maxParallelismOfSource = Math.max(maxParallelismOfSource, getStreamNode(i).getParallelism());
		}
		if (maxParallelismOfSource > 0) {
			Collection<Integer> sourceIDs = getSourceIDs();
			for (StreamNode streamNode : getStreamNodes()) {
				int nodeId = streamNode.getId();
				if (!sourceIDs.contains(nodeId) && streamNode.isUseDefaultParallelism()) {
					LOG.info("Overwrite parallelism of StreamNode: {} from {} " +
						"to {} (max source parallelism).",
						streamNode, streamNode.getParallelism(), maxParallelismOfSource);
					streamNode.setParallelism(maxParallelismOfSource);
				}
			}
		}
	}

	public String getStreamingPlanAsJSON() {
		try {
			return new JSONGenerator(this).getJSON();
		}
		catch (Exception e) {
			throw new RuntimeException("JSON plan creation failed", e);
		}
	}

	private <T> TypeSerializer<T> createSerializer(TypeInformation<T> typeInfo) {
		return typeInfo != null && !(typeInfo instanceof MissingTypeInfo) ?
			typeInfo.createSerializer(executionConfig) : null;
	}

	public boolean isBatchJob() {
		return isBatchJob;
	}

	/**
	 * Get sorted streamNode list.
	 */
	public List<StreamNode> getStreamNodeSortedTopologicallyFromSources() throws InvalidProgramException {

		if (this.streamNodes.isEmpty()) {
			return Collections.emptyList();
		}

		List<StreamNode> sorted = new ArrayList<>(this.streamNodes.size());
		Set<StreamNode> remaining = new LinkedHashSet<>(getStreamNodes());

		// start by finding the streamNode with no input edges
		{
			Iterator<StreamNode> iter = remaining.iterator();
			while (iter.hasNext()) {
				StreamNode streamNode = iter.next();

				if (streamNode.getInEdges().isEmpty()) {
					sorted.add(streamNode);
					iter.remove();
				}
			}
		}

		int startNodePos = 0;

		// traverse from the nodes that were added until we found all elements
		while (!remaining.isEmpty()) {

			// first check if we have more candidates to start traversing from. if not, then the
			// graph is cyclic, which is not permitted
			if (startNodePos >= sorted.size()) {
				throw new InvalidProgramException("The stream graph is cyclic.");
			}

			StreamNode current = sorted.get(startNodePos++);
			addNodesThatHaveNoUpStream(current, sorted, remaining);
		}

		return sorted;
	}

	private void addNodesThatHaveNoUpStream(StreamNode start, List<StreamNode> target, Set<StreamNode> remaining) {

		// forward traverse over all out edges.
		for (StreamEdge outEdge : start.getOutEdges()) {
			StreamNode streamNode = getStreamNode(outEdge.getTargetId());

			// a streamNode can be added, if it has no upstream that are still in the 'remaining' set
			if (!remaining.contains(streamNode)) {
				continue;
			}

			boolean hasNewUpStream = false;

			for (StreamEdge e : streamNode.getInEdges()) {
				// skip the edge through which we came
				if (e == outEdge) {
					continue;
				}

				if (remaining.contains(getStreamNode(e.getSourceId()))) {
					hasNewUpStream = true;
					break;
				}
			}

			if (!hasNewUpStream) {
				target.add(streamNode);
				remaining.remove(streamNode);
				addNodesThatHaveNoUpStream(streamNode, target, remaining);
			}
		}
	}

	/**
	 * Utils for making {@link StreamGraph} working with {@link PlanGraph}.
	 */
	public static class Utils {

		/**
		 * Validate whether the provided {@link PlanGraph} is matched with {@link StreamGraph} generated
		 * from the provided sql statements and apply configs of configs in {@link PlanGraph} into
		 * {@link StreamGraph} if they are matched.
		 *
		 * <p>First, all {@link PlanGraph.StreamGraphVertex} in the {@link PlanGraph} will be put in a map
		 * whose key is nodeDesc( generated by {@link StreamGraphHasherV2}).
		 * Then, the complete {@link StreamGraph} is traversed:
		 * <ul>
		 *     <li>If any of {@link StreamNode} in this graph cannot be found in the {@link PlanGraph},
		 *     an exception will be thrown.
		 *     <li>If a matched {@link PlanGraph.StreamGraphVertex} is found. Its properties will be copied
		 *     into the {@link StreamNode}
		 * </ul>
		 *
		 * <p>Note that only userProvidedHash rather than uid can be set in {@link PlanGraph} for design.
		 * Therefore when userProvidedHash is provided by user, we will also set this hash to
		 * transformationId of node. Doing this will make generatedOperatorID remaining unchanged after
		 * one-off state migration.
		 */
		public static void validateAndApplyConfigToStreamGraph(
				StreamGraph streamGraph,
				PlanGraph planGraph,
				Map<Integer, byte[]> hashes) {
			Map<String, PlanGraph.StreamGraphVertex> streamNodeMap = new HashMap<>();
			for (PlanGraph.StreamGraphVertex node: planGraph.getStreamNodes()) {
				streamNodeMap.put(node.getNodeDesc(), node);
			}

			Set<Integer> visited = new HashSet<>();
			Queue<StreamNode> remaining = new ArrayDeque<>();

			// Start with source nodes
			for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
				remaining.add(streamGraph.getStreamNode(sourceNodeId));
				visited.add(sourceNodeId);
			}

			Set<Integer> parallelismChangedNodes = new HashSet<>();
			StreamNode currentNode;
			while ((currentNode = remaining.poll()) != null) {
				String operatorID = StringUtils.byteToHexString(hashes.get(currentNode.getId()));
				PlanGraph.StreamGraphVertex jsonGraphNode = streamNodeMap.get(operatorID);
				if (jsonGraphNode == null) {
					throw new RuntimeException("The json plan does not match with the sql. Can't not find node:"
						+ currentNode.toString());
				}
				if (currentNode.getParallelism() != jsonGraphNode.getParallelism()) {
					streamGraph.setParallelism(currentNode.getId(), jsonGraphNode.getParallelism());
					parallelismChangedNodes.add(currentNode.getId());
				}
				String userProvidedHash = jsonGraphNode.getUserProvidedHash();
				if (null != userProvidedHash) {
					currentNode.setUserHash(userProvidedHash);
					currentNode.setTransformationUID(userProvidedHash);
				}
				for (StreamEdge outEdge : currentNode.getOutEdges()) {
					StreamNode child = streamGraph.getTargetVertex(outEdge);
					if (!visited.contains(child.getId())) {
						remaining.add(child);
						visited.add(child.getId());
					}
				}
			}
			streamGraph.updateEdgesForStreamNodes(parallelismChangedNodes);
		}

		/**
		 * Apply configs of old {@link PlanGraph} into a new {@link PlanGraph} generated from the provided sql statements.
		 */
		public static String applyOldGraph(String oldGraphJson, StreamGraph streamGraph) {
			PlanGraph oldGraph = PlanGraph.Utils.resolvePlanGraph(oldGraphJson);
			// Reuse StreamGraphHasherV2 to generated deterministic id for nodes in graphs which
			// remain unchanged across job submissions.
			StreamGraphHasher defaultStreamGraphHasher = new StreamGraphHasherV2();
			Map<Integer, byte[]> generatedHashes = defaultStreamGraphHasher
				.traverseStreamGraphAndGenerateHashes(streamGraph);
			Map<String, PlanGraph.StreamGraphVertex> oldStreamNodeMap = new HashMap<>();
			Map<String, List<PlanGraph.StreamGraphVertex>> oldNameNodeMap = new HashMap<>();
			for (PlanGraph.StreamGraphVertex node: oldGraph.getStreamNodes()) {
				oldStreamNodeMap.put(node.getNodeDesc(), node);
				List<PlanGraph.StreamGraphVertex> nodes = oldNameNodeMap
					.computeIfAbsent(node.getDescription(), k -> new ArrayList<>());
				nodes.add(node);
			}
			// If all nodes in old graph and new graph have the same nodeDescs(which are hashes generated by
			// StreamGraphHasherV2), we take them as isomorphic graphs. This behavior is consistent with which
			// in StateAssignmentOperation.
			boolean isIsomorphic = oldGraph.getStreamNodes().size() == streamGraph.getStreamNodes().size();
			Map<String, List<StreamNode>> newNameNodeMap = new HashMap<>();
			for (StreamNode node: streamGraph.getStreamNodes()) {
				String generatedHash = StringUtils.byteToHexString(generatedHashes.get(node.getId()));
				if (!oldStreamNodeMap.containsKey(generatedHash)) {
					isIsomorphic = false;
				}
				List<StreamNode> nodes = newNameNodeMap
					.computeIfAbsent(node.getOperatorName(), k -> new ArrayList<>());
				nodes.add(node);
			}
			ObjectMapper mapper = new ObjectMapper();
			mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			Set<Integer> editedNodes = new HashSet<>();
			Set<Integer> parallelismChangedNodes = new HashSet<>();
			if (isIsomorphic) {
				// When two graphs are isomorphic, nodes will be matched according to nodeDescs.
				for (StreamNode newNode: streamGraph.getStreamNodes()) {
					String generatedHash = StringUtils.byteToHexString(generatedHashes.get(newNode.getId()));
					PlanGraph.StreamGraphVertex oldNode = oldStreamNodeMap.get(generatedHash);
					applyPropertiesToStreamNode(streamGraph, newNode, oldNode, editedNodes, parallelismChangedNodes);
				}
			} else {
				// When two graphs aren't isomorphic, nodes will be matched according to algorithm below
				// which calculates similarity for each pair of new and old nodes having same name and then
				// pairs them by order of similarity.
				for (Map.Entry<String, List<StreamNode>> entry: newNameNodeMap.entrySet()) {
					List<PlanGraph.StreamGraphVertex> oldList = oldNameNodeMap.get(entry.getKey());
					if (oldList != null) {
						if (entry.getValue().size() == 1 && oldList.size() == 1) {
							applyPropertiesToStreamNode(
								streamGraph, entry.getValue().get(0), oldList.get(0), editedNodes, parallelismChangedNodes);
						} else {
							Set<String> matchedOldHashes = new HashSet<>();
							Set<Integer> matchedNewIDs = new HashSet<>();
							Queue<Tuple3<Integer, StreamNode, PlanGraph.StreamGraphVertex>> similarityQueue =
								new PriorityQueue<>(Comparator.comparingInt(o -> -o.f0));
							for (StreamNode newNode : entry.getValue()) {
								for (PlanGraph.StreamGraphVertex oldNode : oldNameNodeMap.get(entry.getKey())) {
									similarityQueue.add(new Tuple3<>(oldNode.calcSimilarity(newNode), newNode, oldNode));
								}
							}
							while (similarityQueue.size() > 0 && matchedOldHashes.size() < oldList.size() &&
									matchedNewIDs.size() < entry.getValue().size()) {
								Tuple3<Integer, StreamNode, PlanGraph.StreamGraphVertex> candidatePair =
									similarityQueue.poll();
								StreamNode newNode = candidatePair.f1;
								PlanGraph.StreamGraphVertex oldNode = candidatePair.f2;
								if (!matchedNewIDs.contains(newNode.getId()) &&
										!matchedOldHashes.contains(oldNode.getNodeDesc())) {
									applyPropertiesToStreamNode(
										streamGraph, newNode, oldNode, editedNodes, parallelismChangedNodes);
									matchedNewIDs.add(newNode.getId());
									matchedOldHashes.add(oldNode.getNodeDesc());
								}
							}
						}
					}
				}
			}
			streamGraph.updateEdgesForStreamNodes(parallelismChangedNodes);
			streamGraph.setUserProvidedHashInvolved(true);
			JobGraph jobGraph = streamGraph.getJobGraph();
			PlanJSONGenerator planJSONGenerator = new PlanJSONGenerator(streamGraph, jobGraph, editedNodes, generatedHashes);
			return planJSONGenerator.generatePlan();
		}

		/**
		 * Copy properties from {@link PlanGraph.StreamGraphVertex} to {@link StreamNode}.
		 *
		 * <p>Parallelism and userProvidedHash will be naturally copied. A special case is that
		 * when the operator has states in itself, the nodeDesc should be used to set userHash
		 * and transformationId to enable states migration.
		 *
		 * <p>Note that only userProvidedHash rather than uid can be set in {@link PlanGraph} for design.
		 * Therefore when userProvidedHash is provided by user, we will also set this hash to
		 * transformationId of node. Doing this will make generatedOperatorID remaining unchanged after
		 * one-off state migration.
		 */
		private static void applyPropertiesToStreamNode(
				StreamGraph newStreamGraph,
				StreamNode newNode,
				PlanGraph.StreamGraphVertex oldNode,
				Set<Integer> editedNodes,
				Set<Integer> parallelismChangedNodes) {
			boolean edited = false;
			if (oldNode.getUserProvidedHash() != null) {
				newNode.setUserHash(oldNode.getUserProvidedHash());
				newNode.setTransformationUID(oldNode.getUserProvidedHash());
				edited = true;
			} else if (newNode.isHasState()) {
				// Only when operator has state, the old operator ID should be inherited.
				newNode.setUserHash(oldNode.getNodeDesc());
				newNode.setTransformationUID(oldNode.getNodeDesc());
				edited = true;
			}
			if (newNode.getParallelism() != oldNode.getParallelism()) {
				newStreamGraph.setParallelism(newNode.getId(), oldNode.getParallelism());
				parallelismChangedNodes.add(newNode.getId());
				edited = true;
			}
			if (edited) {
				editedNodes.add(newNode.getId());
			}
		}
	}
}
