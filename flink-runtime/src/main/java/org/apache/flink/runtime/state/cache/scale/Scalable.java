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

package org.apache.flink.runtime.state.cache.scale;

/**
 * If a {@link org.apache.flink.runtime.state.cache.Cache} needs to support
 * dynamic scale, then this interface needs to be implemented.
 */
public interface Scalable<T> {
	/**
	 * Scale up the {@link org.apache.flink.runtime.state.cache.Cache} according to the specified size,
	 * and notify the scale result through a {@link ScaleCallback} after the scale is completed.
	 */
	void scaleUp(T size, ScaleCallback callback);

	/**
	 * Scale down the {@link org.apache.flink.runtime.state.cache.Cache} according to the specified size,
	 * and notify the scale result through a {@link ScaleCallback} after the scale is completed.
	 */
	void scaleDown(T size, ScaleCallback callback);
}
