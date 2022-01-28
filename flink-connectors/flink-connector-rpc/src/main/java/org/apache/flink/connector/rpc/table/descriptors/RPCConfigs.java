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

package org.apache.flink.connector.rpc.table.descriptors;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.rpc.FailureHandleStrategy;
import org.apache.flink.connector.rpc.thrift.client.RPCServiceClientWrapper;

import com.bytedance.arch.transport.TransportType;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Configs describe the RPC connector.
 */
public class RPCConfigs {
	// ------------------------------------------------------------------------
	//  Common Options
	// ------------------------------------------------------------------------
	public static final ConfigOption<String> CONSUL = ConfigOptions
		.key("consul")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Consul name of rpc server.");

	public static final ConfigOption<String> THRIFT_SERVICE_CLASS = ConfigOptions
		.key("thrift.service-class")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Rpc service class.");

	public static final ConfigOption<String> THRIFT_METHOD = ConfigOptions
		.key("thrift.method")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. Name of method that will be called.");

	public static final ConfigOption<TransportType> THRIFT_TRANSPORT = ConfigOptions
		.key("thrift.transport")
		.enumType(TransportType.class)
		.defaultValue(TransportType.Framed)
		.withDescription("Optional. Type of transport.");

	public static final ConfigOption<String> PSM = ConfigOptions
		.key("psm")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional. Name of PSM.");

	public static final ConfigOption<String> CLUSTER = ConfigOptions
		.key("cluster")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional. If specified, only connect hosts from the given cluster.");

	public static final ConfigOption<Duration> CONNECTION_TIMEOUT = ConfigOptions
		.key("connection.timeout")
		.durationType()
		.defaultValue(Duration.of(2, ChronoUnit.SECONDS))
		.withDescription("Optional. Connect timeout of a RPC service access operation.");

	public static final ConfigOption<Duration> SOCKET_TIMEOUT = ConfigOptions
		.key("socket.timeout")
		.durationType()
		.defaultValue(Duration.of(2, ChronoUnit.SECONDS))
		.withDescription("Optional. Socket timeout of a RPC service access operation.");

	public static final ConfigOption<Integer> CONNECTION_POOL_SIZE = ConfigOptions
		.key("connection.pool-size")
		.intType()
		.defaultValue(4)
		.withDescription("Optional. Specify the pool size for thrift connection.");

	public static final ConfigOption<Duration> CONSUL_UPDATE_INTERVAL = ConfigOptions
		.key("consul.update-interval")
		.durationType()
		.defaultValue(Duration.of(1, ChronoUnit.MINUTES))
		.withDescription("Optional. Interval for update the consul.");

	public static final ConfigOption<String> SERVICE_CLIENT_IMPL_CLASS = ConfigOptions
		.key("service-client-impl.class")
		.stringType()
		.defaultValue(RPCServiceClientWrapper.class.getName())
		.withDescription("the class name of implementation of service client base.");

	// ------------------------------------------------------------------------
	//  Lookup Options
	// ------------------------------------------------------------------------

	public static final ConfigOption<Boolean> LOOKUP_ASYNC_ENABLED = ConfigOptions
		.key("lookup.async.enabled")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional. Flag indicates whether enable async mode.");

	public static final ConfigOption<Integer> LOOKUP_ASYNC_CONCURRENCY = ConfigOptions
		.key("lookup.async.concurrency")
		.intType()
		.defaultValue(1)
		.withDescription("Optional. The number of concurrent threads for async lookup.");

	public static final ConfigOption<FailureHandleStrategy> LOOKUP_FAILURE_HANDLE_STRATEGY = ConfigOptions
		.key("lookup.failure-handle-strategy")
		.enumType(FailureHandleStrategy.class)
		.defaultValue(FailureHandleStrategy.TASK_FAILURE)
		.withDescription("Optional. Strategy about how to handle the lookup failure.");

	public static final ConfigOption<Boolean> LOOKUP_INFER_SCHEMA = ConfigOptions
		.key("lookup.infer-schema")
		.booleanType()
		.defaultValue(true)
		.withDescription("Optional. Flag of whether to infer table schema from thrift.");
}
