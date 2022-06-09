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

package org.apache.flink.runtime.deployment;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * The interface for deployment classes need to serialized to binary representation and vice-versa.
 */
public interface DeploymentReadableWritable {
	/**
	 * Writes the object's internal data to the given data output view.
	 *
	 * @param out
	 *        the output view to receive the data.
	 * @throws Exception
	 *         thrown if any error occurs while writing to the output stream
	 */
	void write(DataOutputView out) throws Exception;

	/**
	 * Reads the object's internal data from the given data input view.
	 *
	 * @param in
	 *        the input view to read the data from
	 * @throws Exception
	 *         thrown if any error occurs while reading from the input stream
	 */
	void read(DataInputView in) throws Exception;
}
