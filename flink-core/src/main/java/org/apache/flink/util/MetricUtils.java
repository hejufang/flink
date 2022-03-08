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

package org.apache.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

/**
 * Utility class to format names for metric reporting.
 */
public class MetricUtils {

	private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);

	public static final int METRICS_OPERATOR_NAME_MAX_LENGTH = 20;
	public static final int METRICS_TASK_NAME_MAX_LENGTH = 100;

	/**
	 * Formats job name for metric reporting.
	 *
	 * @param jobName The job name.
	 * @return The formatted job name.
	 */
	public static String formatJobMetricName(String jobName) {
		if (jobName == null) {
			return null;
		}
		return jobName
			.replaceAll("[^\\w-]", "_");
	}

	public static String formatJobMetricNameOrigin(String jobName) {
		if (jobName == null) {
			return null;
		}
		return replaceSpecialCharacters(jobName);
	}

	/**
	 * Formats operator name for metric reporting.
	 *
	 * @param operatorName The name of the operator (StreamNode).
	 * @return The formatted operator name.
	 */
	public static String formatOperatorMetricName(String operatorName) {
		if (operatorName == null) {
			return null;
		}
		operatorName = replaceSpecialCharacters(operatorName);
		if (operatorName.length() > METRICS_OPERATOR_NAME_MAX_LENGTH) {
			LOG.warn("The operator name {} exceeded the {}-character length limit and was truncated.",
				operatorName, METRICS_OPERATOR_NAME_MAX_LENGTH);
			operatorName = operatorName.substring(0, METRICS_OPERATOR_NAME_MAX_LENGTH);
		}
		return operatorName;
	}

	/**
	 * Formats task name for metric reporting.
	 *
	 * @param taskName The name of the task (JobVertex).
	 * @return The formatted task name.
	 */
	public static String formatTaskMetricName(String taskName) {
		if (taskName == null) {
			return null;
		}
		taskName = replaceSpecialCharacters(taskName);
		if (taskName.length() > METRICS_TASK_NAME_MAX_LENGTH) {
			LOG.warn("The task name {} exceeded the {}-character length limit and was truncated.",
				taskName, METRICS_TASK_NAME_MAX_LENGTH);
			taskName = taskName.substring(0, METRICS_TASK_NAME_MAX_LENGTH);
		}
		return taskName;
	}

	private static String replaceSpecialCharacters(@Nonnull String name) {
		return name
			.replaceAll("\\W", "_")
			.replaceAll("_+", "_");
	}

	// ------------------------------------------------------------------------

	/**
	 * Prevent instantiation of this utility class.
	 */
	private MetricUtils() {
	}
}
