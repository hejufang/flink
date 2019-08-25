/*
c * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;

import com.codahale.metrics.SlidingWindowReservoir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * An abstract stub implementation for rich user-defined functions.
 * Rich functions have additional methods for initialization ({@link #open(Configuration)}) and
 * teardown ({@link #close()}), as well as access to their runtime execution context via
 * {@link #getRuntimeContext()}.
 */
@Public
public abstract class AbstractRichFunction implements RichFunction, Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(AggregateFunction.class);
	private static final long serialVersionUID = 1L;
	private static final String METRICS_NAME = "latency";

	public Histogram latencyHistogram;

	// --------------------------------------------------------------------------------------------
	//  Runtime context access
	// --------------------------------------------------------------------------------------------

	private transient RuntimeContext runtimeContext;

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		this.runtimeContext = t;
	}

	@Override
	public RuntimeContext getRuntimeContext() {
		if (this.runtimeContext != null) {
			return this.runtimeContext;
		} else {
			throw new IllegalStateException("The runtime context has not been initialized.");
		}
	}

	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		if (this.runtimeContext == null) {
			throw new IllegalStateException("The runtime context has not been initialized.");
		} else if (this.runtimeContext instanceof IterationRuntimeContext) {
			return (IterationRuntimeContext) this.runtimeContext;
		} else {
			throw new IllegalStateException("This stub is not part of an iteration step function.");
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Default life cycle methods
	// --------------------------------------------------------------------------------------------

	@Override
	public void open(Configuration parameters) throws Exception{
		try {
			com.codahale.metrics.Histogram dropwizardHistogram =
				new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));
			RuntimeContext runtimeContext = this.getRuntimeContext();
			latencyHistogram = runtimeContext.getMetricGroup().histogram(METRICS_NAME,
				new DropwizardHistogramWrapper(dropwizardHistogram));
		} catch (Exception e) {
			// Some RichFunction cannot get runtime context, otherwise it will throw an exception.
			// So we just ignore the exception and continue;
			LOG.info("An exception occurred while init latencyHistogram, we just ignore it since " +
				"some subclass dose not have to use it.");
		}
	}

	@Override
	public void close() throws Exception {}
}
