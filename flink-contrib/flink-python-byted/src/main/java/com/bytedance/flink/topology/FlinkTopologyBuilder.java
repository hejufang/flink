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

package com.bytedance.flink.topology;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;

import com.bytedance.flink.component.BatchBoltProcess;
import com.bytedance.flink.component.Bolt;
import com.bytedance.flink.component.BoltWrapper;
import com.bytedance.flink.component.ShellBolt;
import com.bytedance.flink.component.ShellSpout;
import com.bytedance.flink.component.SpoutWrapper;
import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.pojo.BoltConfig;
import com.bytedance.flink.pojo.BoltInfo;
import com.bytedance.flink.pojo.JobConfig;
import com.bytedance.flink.pojo.Schema;
import com.bytedance.flink.pojo.SpoutConfig;
import com.bytedance.flink.pojo.SpoutInfo;
import com.bytedance.flink.utils.CommonUtils;
import com.bytedance.flink.utils.Validation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Flink topology builder.
 */
public class FlinkTopologyBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkTopologyBuilder.class);
	private final HashMap<String, List<Grouping>> unprocessdInputsPerBolt =
		new HashMap<>();
	private HashMap<String, DataStream<Tuple>> availableInputs = new HashMap<>();
	private Set<String> usedSpouts = new HashSet<>();

	private JobConfig jobConfig;
	private Configuration flinkConfig;
	private String jobName;
	private StreamExecutionEnvironment env;
	private String[] flinkYarnArgs;

	public FlinkTopologyBuilder(JobConfig jobConfig, Configuration flinkConfig) {
		this.jobConfig = jobConfig;
		this.flinkConfig = flinkConfig;
		this.jobName = jobConfig.getJobName();
		this.env = StreamExecutionEnvironment.getExecutionEnvironment();
		// Set default parallelism to 1.
		env.setParallelism(1);
	}

	public void run(String jobName) {
		try {
			build();
			if (getJobConfig().getRunMode() == Constants.RUN_MODE_STANDLONE) {
				int runTimeSeconds = jobConfig.getRunSeconds();
				LOG.info("Run job on local mode for {} seconds", runTimeSeconds);
				StreamGraph streamGraph = env.getStreamGraph();
				streamGraph.setJobName(getJobName());
				JobGraph jobGraph = streamGraph.getJobGraph();
				flinkConfig.setString(RestOptions.BIND_PORT, "10000-20000");
				MiniCluster miniCluster = new MiniCluster(
					new MiniClusterConfiguration.Builder()
						.setConfiguration(flinkConfig)
						.setNumTaskManagers(1)
						.setNumSlotsPerTaskManager(jobGraph.getMaximumParallelism())
						.build());
				miniCluster.start();
				miniCluster.submitJob(jobGraph);
				LOG.info("The local cluster will shut down after {} seconds.", jobConfig.getRunSeconds());
				Thread.sleep(runTimeSeconds * 1000);
				LOG.info("Close mini cluster...");
				miniCluster.close();
			} else {
				env.execute(jobName);
			}
		} catch (Exception e) {
			LOG.info("Exception occurred while running the job.", e);
		}
	}

	public void build() {
		addSpouts();
		addBolts();
		addFakeBoltForSingleSpout();
	}

	public void addSpouts() {
		SpoutConfig spoutConfig = jobConfig.getSpoutConfig();
		for (Map.Entry<String, SpoutInfo> entry : spoutConfig.getSpoutInfoMap().entrySet()) {
			String spoutName = entry.getKey();
			SpoutInfo spoutInfo = entry.getValue();
			List<String> outputFileds = spoutInfo.getOutputFields();
			int numOfOutputFileds = outputFileds.size();

			SpoutWrapper<Tuple> spoutWrapper =
				new SpoutWrapper<>(new ShellSpout(spoutInfo), spoutName, numOfOutputFileds);

			DataStreamSource<Tuple> source =
				env.addSource(spoutWrapper, spoutName, getOutputType(outputFileds))
					.setParallelism(spoutInfo.getParallelism());
			availableInputs.put(spoutName, source);
		}
	}

	public void addBolts() {
		BoltConfig boltConfig = jobConfig.getBoltConfig();
		Map<String, BoltInfo> boltInfoMap = new HashMap<>(boltConfig.getBoltInfoMap());

		boolean makeProgress = true;
		while (boltInfoMap.size() > 0) {
			Iterator<Map.Entry<String, BoltInfo>> boltsIterator = boltInfoMap.entrySet().iterator();
			if (!makeProgress) {
				reportNoProgressException(boltInfoMap);
			}
			makeProgress = false;
			while (boltsIterator.hasNext()) {
				Map.Entry<String, BoltInfo> entry = boltsIterator.next();
				String boltName = entry.getKey();
				BoltInfo boltInfo = entry.getValue();
				List<Grouping> groupingList = unprocessdInputsPerBolt.get(boltName);
				if (groupingList == null) {
					groupingList = boltInfo.getGroupList();
					unprocessdInputsPerBolt.put(boltName, groupingList);
				}
				// Number of input Streams of this bolt.
				int numberOfInputs = groupingList.size();
				int inputsAvailable = 0;
				for (Grouping grouping : groupingList) {
					if (availableInputs.containsKey(grouping.getInputStream())) {
						inputsAvailable++;
					}
				}

				if (inputsAvailable != numberOfInputs) {
					// traverse other bolts first until inputs are available
					continue;
				} else {
					makeProgress = true;
					boltsIterator.remove();
				}

				List<DataStream<Tuple>> inputStreams = new ArrayList<>(numberOfInputs);
				for (Grouping grouping : groupingList) {
					String inputStreamName = grouping.getInputStream();
					DataStream<Tuple> inputStream = availableInputs.get(inputStreamName);
					usedSpouts.add(inputStreamName);
					DataStream<Tuple> processedInputStream = processInput(boltName, new ShellBolt(boltInfo),
						grouping, inputStreamName, inputStream);
					inputStreams.add(processedInputStream);
				}

				int parallelism = boltInfo.getParallelism();
				SingleOutputStreamOperator<?> outputStream =
					process(boltName, new ShellBolt(boltInfo), inputStreams).setParallelism(parallelism);
			}
		}
	}

	private void addFakeBoltForSingleSpout() {
		SpoutConfig spoutConfig = jobConfig.getSpoutConfig();
		Map<String, SpoutInfo> spoutInfoMap = spoutConfig.getSpoutInfoMap();
		availableInputs.forEach((spoutId, stream) -> {
			if (!spoutInfoMap.containsKey(spoutId)) {
				return;
			}
			if (usedSpouts.contains(spoutId)) {
				return;
			}
			stream.forward().addSink(new SinkFunction<Tuple>() {
				@Override
				public void invoke(Tuple value, Context context) throws Exception {
					throw new RuntimeException("This is a faked bolt, can't access here.");
				}
			}).name(spoutId + "-FakerBolt").setParallelism(spoutInfoMap.get(spoutId).getParallelism());
		});
	}

	private DataStream<Tuple> processInput(String boltName, Bolt bolt, Grouping grouping,
		String inputStreamName, DataStream<Tuple> inputStream) {
		assert boltName != null;
		assert bolt != null;
		assert grouping != null;
		assert inputStreamName != null;
		assert inputStream != null;

		GroupType groupType = grouping.getGroupType();
		switch (groupType) {
			case SHUFFLE:
				// We use rebalance for 'SHFFULE' in old version, while we change it to real shuffle now.
				inputStream = inputStream.shuffle();
				break;
			case FORWARD:
				inputStream = inputStream.forward();
				break;
			case RESCALE:
				inputStream = inputStream.rescale();
				break;
			case REBALANCE:
				inputStream = inputStream.rebalance();
				break;
			case BROADCAST:
				inputStream = inputStream.broadcast();
				break;
			case KEY_BY:
				List<String> groupFields = grouping.getGroupFields();
				if (groupFields == null || groupFields.isEmpty()) {
					throw new RuntimeException("No grouping fields specified.");
				}
				int[] keyByIndexes = getGroupingFieldIndexes(inputStreamName, grouping.getGroupFields());
				inputStream = inputStream.keyBy(keyByIndexes);
				break;
			default:
				throw new UnsupportedOperationException(
					String.format("Unsupported grouping type: %s", groupType));
		}
		return inputStream;

	}

	private SingleOutputStreamOperator<?> process(String boltName, Bolt bolt,
		List<DataStream<Tuple>> inputStreams) {
		assert (boltName != null);
		assert (bolt != null);
		assert (inputStreams != null) && (!inputStreams.isEmpty());

		BoltInfo boltInfo = bolt.getBoltInfo();
		int parallelism = boltInfo.getParallelism();
		Schema outputSchema = boltInfo.getOutputSchema();

		Iterator<DataStream<Tuple>> iterator = inputStreams.iterator();
		DataStream<Tuple> dataStream = null;
		DataStream<Tuple>[] restStreams = new DataStream[inputStreams.size() - 1];

		if (isInputSameFields(boltInfo)) {
			// If all input streams have the same output fields size, we use 'union'.
			int index = 0;
			while (iterator.hasNext()) {
				if (dataStream == null) {
					dataStream = iterator.next();
				} else {
					restStreams[index] = iterator.next();
					index++;
				}
			}
			if (restStreams.length >= 1) {
				assert dataStream != null;
				dataStream = dataStream.union(restStreams);
			}
		} else {
			int index = 0;
			while (iterator.hasNext()) {
				DataStream<Tuple> tempDataStream = iterator.next();
				if (dataStream == null) {
					dataStream = tempDataStream;
				} else {
					int maxInputFileds = getMaxInputFileds(boltInfo);
					dataStream = dataStream.connect(tempDataStream)
						.flatMap(new CoFlatMapFunction<Tuple, Tuple, Tuple>() {
							@Override
							public void flatMap1(Tuple value, Collector<Tuple> out) throws Exception {
								out.collect(CommonUtils.extendTuple(value, maxInputFileds));
							}

							@Override
							public void flatMap2(Tuple value, Collector<Tuple> out) throws Exception {
								out.collect(CommonUtils.extendTuple(value, maxInputFileds));
							}
						}).name("co-flat-" + boltName + "-" + index)
						.returns(getConnectedInputsTypeInfo(boltInfo))
						.setParallelism(boltInfo.getParallelism());
					index++;
				}
			}
		}

		if (isBatchBolt(boltInfo)) {
			List<Grouping> groupingList = boltInfo.getGroupList();
			Validation.validateNotEmptyList(groupingList, "groupingList");
			Grouping grouping = groupingList.get(0);
			LOG.info("GroupType = {}", grouping.getGroupType());
			LOG.info("grouping = {}", grouping);
			if (!grouping.getGroupType().equals(GroupType.KEY_BY)) {
				throw new RuntimeException("The grouping type before batch bolt must be keyby('fields'|'key_by')");
			}
			long batchIntervalMs = (int) boltInfo.getArgs().getOrDefault(Constants.FLUSH_INTERVAL_SECONDS_KEY,
				Constants.FLUSH_INTERVAL_SECONDS_VAL) * 1000;
			String inputStreamName = grouping.getInputStream();
			List<String> outputFields = getOutputFieldsFromTask(inputStreamName);
			BatchBoltProcess<Tuple, Object> process = new BatchBoltProcess<>(new ShellBolt(boltInfo),
				boltName, outputSchema, batchIntervalMs,
				getOutputType(outputFields));
			assert dataStream != null;
			int[] keyByIndexes = getGroupingFieldIndexes(grouping.getInputStream(), grouping.getGroupFields());

			return dataStream.keyBy(keyByIndexes)
				.process(process).setParallelism(parallelism).name(boltName)
				.returns(Object.class);
		}

		SingleOutputStreamOperator<Tuple> outputStream;

		TypeInformation<Tuple> outType = getOutputType(outputSchema.toList());

		BoltWrapper<Tuple, Tuple> boltWrapper = new BoltWrapper<>(new ShellBolt(boltInfo), boltName,
			outputSchema);
		assert dataStream != null;
		outputStream = dataStream.transform(boltName, outType, boltWrapper);
		if (outType != null) {
			availableInputs.put(boltName, outputStream);
		}
		return outputStream;
	}

	private int getMaxInputFileds(BoltInfo boltInfo) throws IllegalArgumentException {
		List<Grouping> groupList = boltInfo.getGroupList();
		int maxFieldSize = -1;
		for (Grouping grouping : groupList) {
			String inputStreamName = grouping.getInputStream();
			int fieldSizeTemp = getOutputFieldsFromTask(inputStreamName).size();
			maxFieldSize = Math.max(maxFieldSize, fieldSizeTemp);
		}
		return maxFieldSize;
	}

	private TypeInformation<Tuple> getConnectedInputsTypeInfo(BoltInfo boltInfo) throws IllegalArgumentException {
		int maxFieldSize = getMaxInputFileds(boltInfo);
		return getOutputType(maxFieldSize);
	}

	private TypeInformation<Tuple> getOutputType(int numberOfAttributes) throws IllegalArgumentException {
		if (numberOfAttributes < 0) {
			throw new RuntimeException("numberOfAttributes must be greater than or equal to 0.");
		}

		Tuple t;
		if (numberOfAttributes <= Constants.FLINK_TUPLE_MAX_FIELD_SIZE) {
			try {
				t = Tuple.getTupleClass(numberOfAttributes).newInstance();
			} catch (final InstantiationException e) {
				throw new RuntimeException(e);
			} catch (final IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new IllegalArgumentException("Flink supports only a maximum number of 25 attributes");
		}

		for (int i = 0; i < numberOfAttributes; ++i) {
			t.setField(new DefaultComparator(), i);
		}

		return TypeExtractor.getForObject(t);
	}

	private TypeInformation<Tuple> getOutputType(List<String> outputFileds) throws IllegalArgumentException {
		if (outputFileds == null) {
			throw new RuntimeException("OutputFileds must not be null.");
		}

		Tuple t;
		final int numberOfAttributes = outputFileds.size();

		return getOutputType(numberOfAttributes);
	}

	/**
	 * Computes the indexes within the declared output schema of the specified stream, for a list of given
	 * field-grouping attributes.
	 *
	 * @param groupingFields The names of the key fields.
	 * @return array of {@code int}s that contains the index within the output schema for each attribute in the given
	 * list
	 */
	private int[] getGroupingFieldIndexes(String inputStreamName, final List<String> groupingFields) {
		LOG.info("inputStreamName = " + inputStreamName);
		LOG.info("groupingFields = " + groupingFields);
		final int[] fieldIndexes = new int[groupingFields.size()];
		for (int i = 0; i < fieldIndexes.length; ++i) {
			Schema outputSchema = getOutputSchemaFromTask(inputStreamName);
			fieldIndexes[i] = outputSchema.fieldIndex(groupingFields.get(i));
		}
		return fieldIndexes;
	}

	private boolean isBatchBolt(BoltInfo boltInfo) {
		return boltInfo.getScript().contains(Constants.BATCH_BOLT);
	}

	private boolean isInputSameFields(BoltInfo boltInfo) {
		List<Grouping> groupList = boltInfo.getGroupList();
		int fieldSize = -1;
		for (Grouping grouping : groupList) {
			int fieldSizeTemp;
			String inputStreamName = grouping.getInputStream();
			fieldSizeTemp = getOutputFieldsFromTask(inputStreamName).size();
			if (fieldSize < 0) {
				fieldSize = fieldSizeTemp;
			} else if (fieldSize != fieldSizeTemp) {
				return false;
			}
		}
		return true;
	}

	private void reportNoProgressException(Map<String, BoltInfo> boltInfoMap) {
		StringBuilder strBld = new StringBuilder();
		strBld.append("Unable to build Topology. Could not connect the following bolts:");
		for (String boltId : boltInfoMap.keySet()) {
			strBld.append("\n  ");
			strBld.append(boltId);
			strBld.append(": missing input streams [");
			for (Grouping grouping : unprocessdInputsPerBolt
				.get(boltId)) {
				strBld.append("'");
				strBld.append(grouping.getInputStream());
				strBld.append("'; ");
			}
			strBld.append("]");
		}
		throw new RuntimeException(strBld.toString());
	}

	public List<String> getOutputFieldsFromTask (String taskName) {
		Map<String, SpoutInfo> spoutMap = jobConfig.getSpoutConfig().getSpoutInfoMap();
		Map<String, BoltInfo> boltMap = jobConfig.getBoltConfig().getBoltInfoMap();
		if (spoutMap.containsKey(taskName)) {
			return spoutMap.get(taskName).getOutputFields();
		} else if (boltMap.containsKey(taskName)) {
			return boltMap.get(taskName).getOutputFields();
		} else {
			throw new RuntimeException("Task " + taskName + " is not in this job, " +
				"please check your yaml file.");
		}
	}

	public Schema getOutputSchemaFromTask (String taskName) {
		Map<String, SpoutInfo> spoutMap = jobConfig.getSpoutConfig().getSpoutInfoMap();
		Map<String, BoltInfo> boltMap = jobConfig.getBoltConfig().getBoltInfoMap();
		if (spoutMap.containsKey(taskName)) {
			return spoutMap.get(taskName).getOutputSchema();
		} else if (boltMap.containsKey(taskName)) {
			return boltMap.get(taskName).getOutputSchema();
		} else {
			throw new RuntimeException("Task " + taskName + " is not in this job, " +
				"please check your yaml file.");
		}
	}

	public JobConfig getJobConfig() {
		return jobConfig;
	}

	public void setJobConfig(JobConfig jobConfig) {
		this.jobConfig = jobConfig;
	}

	public StreamExecutionEnvironment getEnv() {
		return env;
	}

	public void setEnv(StreamExecutionEnvironment env) {
		this.env = env;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String[] getFlinkYarnArgs() {
		return flinkYarnArgs;
	}

	public void setFlinkYarnArgs(String[] flinkYarnArgs) {
		this.flinkYarnArgs = flinkYarnArgs;
	}
}
