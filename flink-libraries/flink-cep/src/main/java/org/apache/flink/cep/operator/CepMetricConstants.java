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

package org.apache.flink.cep.operator;

/**
 * A collection of CepOperator metric related constant names.
 */
public class CepMetricConstants {

	// ------------------------------------------------------------------------
	//  user pattern metrics
	// ------------------------------------------------------------------------

	public static final String PATTERNS_ADDED_METRIC_NAME = "numPatternsAdded";
	public static final String PATTERNS_DROPPED_METRIC_NAME = "numPatternsDropped";
	public static final String PATTERNS_USED_METRIC_NAME = "numPatternsUsed";

	// ------------------------------------------------------------------------
	//  late records process Metrics
	// ------------------------------------------------------------------------

	public static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
	public static final String LATE_ELEMENTS_OUTPUT_METRIC_NAME = "numLateRecordsOutput";
	public static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
	public static final String LATE_ELEMENTS_OUTPUT_RATE_METRIC_NAME = "lateRecordsOutputRate";
	public static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

	// ------------------------------------------------------------------------
	//  state clear metrics
	// ------------------------------------------------------------------------

	public static final String STATE_CLEANER_TRIGGERD_METRIC_NAME = "numStateCleanerTriggered";
	public static final String STATE_CLEANER_TRIGGERD_RATE_METRIC_NAME = "StateCleanerTriggeredRate";

	// ------------------------------------------------------------------------
	//  match result metrics
	// ------------------------------------------------------------------------

	public static final String MATCHED_SEQUENCES_METRIC_NAME = "numMatchedSequences";
	public static final String UNMATCHED_SEQUENCES_METRIC_NAME = "numUnMatchedSequences";
	public static final String TIME_OUT_MATCHED_SEQUENCES_METRIC_NAME = "numTimeOutSequences";
	public static final String MATCHED_SEQUENCES_RATE_METRIC_NAME = "matchedSequencesRate";
	public static final String UNMATCHED_SEQUENCES_RATE_METRIC_NAME = "unMatchedSequencesRate";
	public static final String TIME_OUT_MATCHED_SEQUENCES_RATE_METRIC_NAME = "timeOutSequencesRate";

	// ------------------------------------------------------------------------
	//  matching time cost metrics
	// ------------------------------------------------------------------------

	public static final String ADVANCE_TIME_METRIC_NAME = "advanceTimeDuration";
	public static final String PROCESS_EVENT_METRIC_NAME = "processEventDuration";

	// ------------------------------------------------------------------------
	//  user logic time cost metrics
	// ------------------------------------------------------------------------

	public static final String PROCESS_MATCHED_METRIC_NAME = "processMatchedDuration";
	public static final String PROCESS_UNMATCHED_METRIC_NAME = "processUnMatchDuration";
	public static final String PROCESS_TIMEOUT_METRIC_NAME = "processTimeoutDuration";
}
