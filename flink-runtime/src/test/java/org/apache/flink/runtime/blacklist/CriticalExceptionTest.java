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

package org.apache.flink.runtime.blacklist;

import org.apache.flink.runtime.throwable.ThrowableAnnotation;
import org.apache.flink.runtime.throwable.ThrowableType;

/**
 * Test for blacklist tracker.
 */
@ThrowableAnnotation(ThrowableType.CriticalError)
public class CriticalExceptionTest extends Exception {
	static final long serialVersionUID = -7034897190745766930L;

	public CriticalExceptionTest() {
		super();
	}

	public CriticalExceptionTest(String message) {
		super(message);
	}

	public CriticalExceptionTest(String message, Throwable cause) {
		super(message, cause);
	}

	public CriticalExceptionTest(Throwable cause) {
		super(cause);
	}
}
