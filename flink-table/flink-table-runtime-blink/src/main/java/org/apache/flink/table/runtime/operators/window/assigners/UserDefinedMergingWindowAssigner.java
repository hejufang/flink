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

package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.functions.WindowFunction;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.operators.window.internal.InternalWindowProcessFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.NavigableSet;

/**
 * User defined merging window assigner.
 */
public class UserDefinedMergingWindowAssigner extends MergingWindowAssigner<TimeWindow> implements InternalTimeWindowAssigner {
	private static final Logger LOG = LoggerFactory.getLogger(UserDefinedMergingWindowAssigner.class);

	// Method is not serializable, so we make it transient, and initialize it in open method.
	private transient Method userAssignWindow;
	private transient Method userMerge;
	private final LogicalType[] logicalTypes;
	private final int[] paramIndices;
	private final WindowFunction windowFunction;
	private final boolean isEventTime;

	public UserDefinedMergingWindowAssigner(
			LogicalType[] logicalTypes,
			int[] paramIndices,
			WindowFunction windowFunction,
			boolean isEventTime) {
		this.logicalTypes = logicalTypes;
		this.paramIndices = paramIndices;
		this.windowFunction = windowFunction;
		this.isEventTime = isEventTime;
	}

	@Override
	public void open(InternalWindowProcessFunction.Context<?, TimeWindow> ctx) throws Exception {
		super.open(ctx);
		// TODO: ref UserDefinedFunction's eval method invocation, using CodeGeneration.
		for (Method method : windowFunction.getClass().getMethods()) {
			if (method.getName().equalsIgnoreCase("assignWindows")) {
				userAssignWindow = method;
			} else if (method.getName().equalsIgnoreCase("mergeWindow")) {
				userMerge = method;
			}
		}
	}

	@Override
	public InternalTimeWindowAssigner withEventTime() {
		return new UserDefinedMergingWindowAssigner(
			logicalTypes,
			paramIndices,
			windowFunction,
			true);
	}

	@Override
	public InternalTimeWindowAssigner withProcessingTime() {
		return new UserDefinedMergingWindowAssigner(
			logicalTypes,
			paramIndices,
			windowFunction,
			false);
	}

	@Override
	public void mergeWindows(TimeWindow newWindow, NavigableSet<TimeWindow> sortedWindows, MergeCallback<TimeWindow> callback) {
		TimeWindow ceiling = sortedWindows.ceiling(newWindow);
		TimeWindow floor = sortedWindows.floor(newWindow);

		Collection<TimeWindow> mergedWindows = new HashSet<>();
		TimeWindow mergeResult = newWindow;
		if (ceiling != null) {
			mergeResult = mergeWindow(mergeResult, ceiling, mergedWindows);
		}
		if (floor != null) {
			mergeResult = mergeWindow(mergeResult, floor, mergedWindows);
		}
		if (!mergedWindows.isEmpty()) {
			// merge happens, add newWindow into the collection as well.
			mergedWindows.add(newWindow);
			callback.merge(mergeResult, mergedWindows);
		}
	}

	private TimeWindow mergeWindow(TimeWindow curWindow, TimeWindow other, Collection<TimeWindow> mergedWindow) {
		try {
			TimeWindow mergeResult = (TimeWindow) userMerge.invoke(windowFunction, curWindow, other);
			if (mergeResult != null) {
				mergedWindow.add(other);
				return mergeResult;
			}
		} catch (IllegalAccessException | InvocationTargetException e) {
			LOG.error("invoking {}.mergeWindow has error.", windowFunction.getClass().getName(), e);
			throw new FlinkRuntimeException(e);
		}
		return curWindow;
	}

	@Override
	public Collection<TimeWindow> assignWindows(BaseRow element, long timestamp) throws IOException {
		// TODO: support literal for params.
		Object[] params = new Object[logicalTypes.length];
		for (int i = 0; i < logicalTypes.length; ++i) {
			params[i] = TypeGetterSetters.get(element, paramIndices[i], logicalTypes[i]);
			if (logicalTypes[i].getTypeRoot() == LogicalTypeRoot.VARCHAR) {
				params[i] = params[i].toString();
			}
		}
		try {
			//noinspection unchecked
			return (Collection<TimeWindow>) userAssignWindow.invoke(windowFunction, params);
		} catch (IllegalAccessException | InvocationTargetException e) {
			LOG.error("invoking {}.assignWindow has error.", windowFunction.getClass().getName(), e);
			throw new FlinkRuntimeException(e);
		}
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return isEventTime;
	}

	@Override
	public String toString() {
		return String.format("UserDefinedMergingWindowAssigner(class=%s, params=%s)",
			windowFunction.getClass().getName(), Arrays.toString(logicalTypes));
	}
}
