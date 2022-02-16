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

/**
 * A collection of all configuration options that relate to SmartResource.
 */
public class SmartResourceOptions {

	public static final String SMART_RESOURCES_CPU_ESTIMATE_MODE_FLOOR = "floor";
	public static final String SMART_RESOURCES_CPU_ESTIMATE_MODE_ROUND = "round";
	public static final String SMART_RESOURCES_CPU_ESTIMATE_MODE_CEIL = "ceil";
	public static final int SMART_RESOURCES_DURATION_MINUTES_MIN = 60;

	// ------------------------------------------------------------------------
	//  general SmartResource options
	// ------------------------------------------------------------------------

	/**
	 * Option whether the SmartResource should enable on runtime.
	 */
	public static final ConfigOption<Boolean> SMART_RESOURCES_ENABLE = ConfigOptions
		.key("smart-resources.enable_on_runtime")
		.booleanType()
		.defaultValue(false)
		.withDescription("Option whether the SmartResource should enable on runtime.");

	/**
	 * Option whether the SmartResource should enable on runtime.
	 *
	 * @deprecated This option is old parameter, it have been replaced by {@link
	 * #SMART_RESOURCES_ENABLE}.
	 */
	@Deprecated
	public static final ConfigOption<Boolean> SMART_RESOURCES_ENABLE_OLD = ConfigOptions
		.key("smart-resources.enable")
		.booleanType()
		.defaultValue(SMART_RESOURCES_ENABLE.defaultValue())
		.withDescription("Option whether the SmartResource should enable on runtime.");

	/**
	 * Option whether enable SmartResource to adjust TM memory.
	 */
	public static final ConfigOption<Boolean> SMART_RESOURCES_DISABLE_MEM_ADJUST = ConfigOptions
		.key("smart-resources.disable-mem-adjust")
		.booleanType()
		.defaultValue(true)
		.withDescription("Option whether enable SmartResource to adjust TM memory.");

	/**
	 * The service psm name of SmartResource to discovery the server through consul.
	 */
	public static final ConfigOption<String> SMART_RESOURCES_SERVICE_NAME = ConfigOptions
		.key("smart-resources.service-name")
		.stringType()
		.noDefaultValue()
		.withDescription(
			"The service psm name of SmartResource to discovery the server through consul.");

	// ------------------------------------------------------------------------
	// SmartResource options for check-api
	// ------------------------------------------------------------------------

	/**
	 * The config parameter indicates a api which can define whether to adjust resource or not.
	 * SmartResource will check the result through the api before job adjust resource.
	 */
	public static final ConfigOption<String> SMART_RESOURCES_ADJUST_CHECK_API = ConfigOptions
		.key("smart-resources.adjust-check-api")
		.stringType()
		.noDefaultValue()
		.withDescription(
			"The config parameter indicates a api which can define whether to adjust resource or not. "
				+ "SmartResource will * check the result through the api before job adjust resource.");

	/**
	 * The waiting time to request check-api again if its latest request was rejected.
	 */
	public static final ConfigOption<Integer> SMART_RESOURCES_ADJUST_CHECK_BACKOFF_MS = ConfigOptions
		.key("smart-resources.adjust-check-backoff-ms")
		.intType()
		.defaultValue(60 * 1000)
		.withDescription(
			"The waiting time to request check-api again if its latest request was rejected.");

	/**
	 * The timeout time to request check-api.
	 */
	public static final ConfigOption<Integer> SMART_RESOURCES_ADJUST_CHECK_TIMEOUT_MS = ConfigOptions
		.key("smart-resources.adjust-check-timeout-ms")
		.intType()
		.defaultValue(5 * 1000)
		.withDescription("The timeout time to request check-api.");

	// ------------------------------------------------------------------------
	// SmartResource options for resource adjust
	// ------------------------------------------------------------------------

	/**
	 * The job history duration time considered in resource adjustment, SmartResource will adjust
	 * the resource based on historical resource information.
	 */
	public static final ConfigOption<Integer> SMART_RESOURCES_DURATION_MINUTES = ConfigOptions
		.key("smart-resources.duration.minutes")
		.intType()
		.defaultValue(24 * 60)
		.withDescription(
			"The job history duration time considered in resource adjustment, SmartResource "
				+ "will adjust the resource based on historical resource information.");

	/**
	 * Same as above, but this parameter will be deprecated in future for error name, replace by
	 * {@link #SMART_RESOURCES_DURATION_MINUTES}.
	 */
	@Deprecated
	public static final ConfigOption<Integer> SMART_RESOURCES_DURATION_MINUTES_OLD = ConfigOptions
		.key("smart-resources.durtion.minutes")
		.intType()
		.defaultValue(SMART_RESOURCES_DURATION_MINUTES.defaultValue())
		.withDescription(
			"Same as above, but this parameter will be deprecated in future for error name, "
				+ "replace by smart-resources.duration.minutes.");

	/**
	 * The cpu buffer ratio for SmartResource to calculate the adjusted CPU cores.
	 */
	public static final ConfigOption<Double> SMART_RESOURCES_CPU_RESERVE_RATIO = ConfigOptions
		.key("smart-resources.cpu-reserve-ratio")
		.doubleType()
		.defaultValue(0.2)
		.withDescription(
			"The cpu buffer ratio for SmartResource to calculate the adjusted CPU cores.");

	/**
	 * Option whether allows to use CPU thousand quartiles while SmartResource calculating the CPU
	 * cores.
	 */
	public static final ConfigOption<Boolean> SMART_RESOURCES_CPU_ADJUST_DOUBLE_ENABLE = ConfigOptions
		.key("smart-resources.cpu-adjust-double.enable")
		.booleanType()
		.defaultValue(false)
		.withDescription(
			"Option whether allows to use CPU thousand quartiles while SmartResource calculating the CPU cores.");

	/**
	 * Option defines the rounding mode if does not allow CPU thousand quartiles.
	 */
	public static final ConfigOption<String> SMART_RESOURCES_CPU_ESTIMATE_MODE = ConfigOptions
		.key("smart-resources.cpu-estimate-mode")
		.stringType()
		.defaultValue(SMART_RESOURCES_CPU_ESTIMATE_MODE_CEIL)
		.withDescription(
			"Option defines the rounding mode if does not allow CPU thousand quartiles");

	/**
	 * The memory buffer ratio for SmartResource to calculate the adjusted memory.
	 */
	public static final ConfigOption<Double> SMART_RESOURCES_MEM_RESERVE_RATIO = ConfigOptions
		.key("smart-resources.mem-reserve-ratio")
		.doubleType()
		.defaultValue(0.2)
		.withDescription(
			"The memory buffer ratio for SmartResource to calculate the adjusted memory");

	/**
	 * The max memory configuration which SmartResource can adjust.
	 */
	public static final ConfigOption<Integer> SMART_RESOURCES_MEM_MAX_MB = ConfigOptions
		.key("smart-resources.mem-max-mb")
		.intType()
		.defaultValue(60 * 1024)
		.withDescription("The max memory configuration which SmartResource can adjust");

}
