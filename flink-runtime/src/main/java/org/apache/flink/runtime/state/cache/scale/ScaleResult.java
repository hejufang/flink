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

package org.apache.flink.runtime.state.cache.scale;

/**
 * Represents the result of scale.
 * @param <T> The granularity of the scale.
 */
public class ScaleResult<T> {
	private final boolean success;
	private final T scaleSize;
	private final String msg;

	public ScaleResult(boolean success, T scaleSize, String msg) {
		this.success = success;
		this.scaleSize = scaleSize;
		this.msg = msg;
	}
}
