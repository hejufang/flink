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

package org.apache.flink.runtime.blacklist.tracker;

import org.apache.flink.runtime.blacklist.BlacklistUtil;
import org.apache.flink.runtime.blacklist.HostFailure;
import org.apache.flink.runtime.blacklist.tracker.handler.FailureHandler;
import org.apache.flink.runtime.throwable.ThrowableAnnotation;
import org.apache.flink.runtime.throwable.ThrowableType;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Route Exception to Failure Handlers.
 */
public class FailureHandlerRouter {
	private static final Logger LOG = LoggerFactory.getLogger(FailureHandlerRouter.class);

	Map<Class<? extends Throwable>, FailureHandler> exceptionClassToHandler;
	Map<ThrowableType, FailureHandler> throwableTypeToHandler;
	Map<BlacklistUtil.FailureType, FailureHandler> failureTypeToHandler;
	FailureHandler defaultFailureHandler;

	public FailureHandlerRouter(FailureHandler defaultFailureHandler) {
		this.exceptionClassToHandler = new HashMap<>();
		this.throwableTypeToHandler = new HashMap<>();
		this.failureTypeToHandler = new HashMap<>();
		this.defaultFailureHandler = Preconditions.checkNotNull(defaultFailureHandler);
	}

	public void registerFailureHandler(
			ThrowableType throwableType,
			FailureHandler failureHandler) {
		registerFailureHandler(
				Collections.emptySet(),
				Collections.singleton(throwableType),
				Collections.emptySet(),
				failureHandler);
	}

	public void registerFailureHandler(
			BlacklistUtil.FailureType failureType,
			FailureHandler failureHandler) {
		registerFailureHandler(
				Collections.emptySet(),
				Collections.emptySet(),
				Collections.singleton(failureType),
				failureHandler);
	}

	public void registerFailureHandler(
			Set<Class<? extends Throwable>> exceptionClasses,
			Set<ThrowableType> throwableTypes,
			Set<BlacklistUtil.FailureType> failureTypes,
			FailureHandler failureHandler) {

		for (Class<? extends Throwable> exceptionClass : exceptionClasses) {
			if (exceptionClassToHandler.containsKey(exceptionClass)) {
				LOG.error("exception class {} already registered to {}, ignore register to {}.",
						exceptionClass,
						exceptionClassToHandler.get(exceptionClass),
						failureHandler);
				continue;
			}
			LOG.info("exception class {} register to {}", exceptionClass, failureHandler);
			exceptionClassToHandler.put(exceptionClass, failureHandler);
		}

		for (ThrowableType throwableType : throwableTypes) {
			if (throwableTypeToHandler.containsKey(throwableType)) {
				LOG.error("throwable type {} already registered to {}, ignore register to {}.",
						throwableType,
						throwableTypeToHandler.get(throwableType),
						failureHandler);
				continue;
			}
			LOG.info("throwable type {} register to {}", throwableType, failureHandler);
			throwableTypeToHandler.put(throwableType, failureHandler);
		}

		for (BlacklistUtil.FailureType failureType : failureTypes) {
			if (failureTypeToHandler.containsKey(failureType)) {
				LOG.error("failure type {} already registered to {}, ignore register to {}.",
						failureType,
						failureTypeToHandler.get(failureType),
						failureHandler);
				continue;
			}
			LOG.info("failure type {} register to {}", failureType, failureHandler);
			failureTypeToHandler.put(failureType, failureHandler);
		}
	}

	public FailureHandler getFailureHandler(HostFailure hostFailure) {
		Class<? extends Throwable> exceptionClass = hostFailure.getException().getClass();
		if (exceptionClassToHandler.containsKey(exceptionClass)) {
			return exceptionClassToHandler.get(exceptionClass);
		}

		ThrowableAnnotation throwableAnnotation = hostFailure.getException().getClass().getAnnotation(ThrowableAnnotation.class);
		ThrowableType throwableType = throwableAnnotation == null ? null : throwableAnnotation.value();
		if (throwableType != null && throwableTypeToHandler.containsKey(throwableType)) {
			return throwableTypeToHandler.get(throwableType);
		}

		if (failureTypeToHandler.containsKey(hostFailure.getFailureType())) {
			return failureTypeToHandler.get(hostFailure.getFailureType());
		}

		return defaultFailureHandler;
	}
}
