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

package org.apache.flink.runtime.state.cache;

import org.apache.flink.configuration.MemorySize;

/**
 * The listener of the memory size, if a {@link Cache} needs to control the size according
 * to the memory, this interface needs to be implemented.
 */
public interface MemorySizeListener {
	/** When the memory exceeds the limit, the callback will notify the memory overflow. */
	void notifyExceedMemoryLimit(MemorySize maxMemorySize, MemorySize exceedMemorySize);
}
