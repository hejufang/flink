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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PeriodInputFormatSourceFunction.
 * @param <OUT>
 */
public class PeriodInputFormatSourceFunction<OUT> extends InputFormatSourceFunction<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(PeriodInputFormatSourceFunction.class);
	private static final long serialVersionUID = 1L;

	private final long scanIntervalMs;
	private final int countOfScanTimes;
	private transient Configuration configuration;

	public PeriodInputFormatSourceFunction(
			InputFormat<OUT, ?> format,
			TypeInformation<OUT> typeInfo,
			long scanIntervalMs,
			int countOfScanTimes) {
		super(format, typeInfo);
		this.scanIntervalMs = scanIntervalMs;
		this.countOfScanTimes = countOfScanTimes;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.configuration = parameters;
	}

	@Override
	public void run(SourceContext<OUT> ctx) throws Exception {
		int curCnt = 0;
		while (countOfScanTimes < 0 || curCnt < countOfScanTimes) {
			LOG.info("Start run PeriodInputFormatSourceFunction");
			long start = System.currentTimeMillis();
			super.run(ctx);
			ctx.markAsTemporarilyIdle();
			long cost = System.currentTimeMillis() - start;
			if (cost >= scanIntervalMs) {
				LOG.warn("Read once cost {} ms, more than interval {} ms", cost, scanIntervalMs);
			} else {
				Thread.sleep(scanIntervalMs - cost);
			}
			initFormat(configuration);
			curCnt++;
		}
	}
}
