/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * {@link ConfigOption}s specific for a single execution of a user program.
 */
@PublicEvolving
public class ExecutionOptions {

	/**
	 * Should be moved to {@code ExecutionCheckpointingOptions} along with
	 * {@code ExecutionConfig#useSnapshotCompression}, which should be put into {@code CheckpointConfig}.
	 */
	public static final ConfigOption<Boolean> SNAPSHOT_COMPRESSION =
		ConfigOptions.key("execution.checkpointing.snapshot-compression")
			.booleanType()
			.defaultValue(false)
		.withDescription("Tells if we should use compression for the state snapshot data or not");

	public static final ConfigOption<Duration> BUFFER_TIMEOUT =
		ConfigOptions.key("execution.buffer-timeout")
			.durationType()
			.defaultValue(Duration.ofMillis(100))
			.withDescription(Description.builder()
				.text("The maximum time frequency (milliseconds) for the flushing of the output buffers. By default " +
					"the output buffers flush frequently to provide low latency and to aid smooth developer " +
					"experience. Setting the parameter can result in three logical modes:")
				.list(
					TextElement.text("A positive value triggers flushing periodically by that interval"),
					TextElement.text("0 triggers flushing after every record thus minimizing latency"),
					TextElement.text("-1 ms triggers flushing only when the output buffer is full thus maximizing " +
						"throughput")
				)
				.build());

	public static final ConfigOption<String> EXEC_SHUFFLE_MODE =
			key("execution.shuffle-mode")
					.stringType()
					.defaultValue("ALL_EDGES_BLOCKING")
					.withDescription(
							Description.builder()
									.text("Sets exec shuffle mode.")
									.linebreak()
									.text("Accepted values are:")
									.list(
											text("%s: All edges will use blocking shuffle.",
													code("ALL_EDGES_BLOCKING")),
											text(
													"%s: Forward edges will use pipelined shuffle, others blocking.",
													code("FORWARD_EDGES_PIPELINED")),
											text(
													"%s: Pointwise edges will use pipelined shuffle, others blocking. " +
															"Pointwise edges include forward and rescale edges.",
													code("POINTWISE_EDGES_PIPELINED")),
											text(
													"%s: All edges will use pipelined shuffle.",
													code("ALL_EDGES_PIPELINED")),
											text(
													"%s: the same as %s. Deprecated.",
													code("batch"), code("ALL_EDGES_BLOCKING")),
											text(
													"%s: the same as %s. Deprecated.",
													code("pipelined"), code("ALL_EDGES_PIPELINED"))
									)
									.text("Note: Blocking shuffle means data will be fully produced before sent to consumer tasks. " +
											"Pipelined shuffle means data will be sent to consumer tasks once produced.")
									.build());
}
