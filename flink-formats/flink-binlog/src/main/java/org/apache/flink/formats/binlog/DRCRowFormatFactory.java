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

package org.apache.flink.formats.binlog;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * DrcRowFormatFactory.
 */
public class DRCRowFormatFactory extends BinlogRowFormatFactory {
	private static final String DRC_FORMAT = "pb_binlog_drc";
	private static final String DRC_HEADER_NAME = "header";
	private static final String DRC_BODY_NAME = "body";

	@Override
	public String factoryIdentifier() {
		return DRC_FORMAT;
	}

	@Override
	protected String getBinlogHeaderName() {
		return DRC_HEADER_NAME;
	}

	@Override
	protected String getBinlogBodyName() {
		return DRC_BODY_NAME;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return Collections.emptySet();
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> optionalOptions = new HashSet<>();
		optionalOptions.addAll(super.requiredOptions());
		optionalOptions.addAll(super.optionalOptions());
		return optionalOptions;
	}

	@Override
	protected String getTargetTable(ReadableConfig formatOptions) {
		return null;
	}
}
