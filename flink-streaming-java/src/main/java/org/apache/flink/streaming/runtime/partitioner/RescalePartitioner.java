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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Partitioner that distributes the data equally by cycling through the output
 * channels. This distributes only to a subset of downstream nodes because
 * {@link org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator} instantiates
 * a {@link DistributionPattern#POINTWISE} distribution pattern when encountering
 * {@code RescalePartitioner}.
 *
 * <p>The subset of downstream operations to which the upstream operation sends
 * elements depends on the degree of parallelism of both the upstream and downstream operation.
 * For example, if the upstream operation has parallelism 2 and the downstream operation
 * has parallelism 4, then one upstream operation would distribute elements to two
 * downstream operations while the other upstream operation would distribute to the other
 * two downstream operations. If, on the other hand, the downstream operation has parallelism
 * 2 while the upstream operation has parallelism 4 then two upstream operations will
 * distribute to one downstream operation while the other two upstream operations will
 * distribute to the other downstream operations.
 *
 * <p>In cases where the different parallelisms are not multiples of each other one or several
 * downstream operations will have a differing number of inputs from upstream operations.
 *
 * @param <T> Type of the elements in the Stream being rescaled
 */
@Internal
public class RescalePartitioner<T> extends StreamPartitioner<T> implements ConfigurableBacklogPartitioner {

	private static final long serialVersionUID = 1L;

	private int lastSendChannel;

	private ResultSubpartition[] channels;

	private int maxBacklogPerChannel;

	@Override
	public void setup(int numberOfChannels, ResultSubpartition[] subpartitions) {
		super.setup(numberOfChannels);

		lastSendChannel = ThreadLocalRandom.current().nextInt(numberOfChannels);
		this.channels = subpartitions;
	}

	@Override
	public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
		for (int i = 1; i <= numberOfChannels; i++) {
			int channel = (lastSendChannel + i) % numberOfChannels;
			if (channels[channel].getApproximateBacklog() <= maxBacklogPerChannel) {
				lastSendChannel = channel;
				return channel;
			}
		}

		lastSendChannel = (lastSendChannel + 1) % numberOfChannels;
		return lastSendChannel;
	}

	@Override
	public void configure(int backlog) {
		this.maxBacklogPerChannel = backlog;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "RESCALE";
	}
}
