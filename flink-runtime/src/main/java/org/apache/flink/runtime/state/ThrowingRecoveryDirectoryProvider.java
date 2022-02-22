/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import java.io.File;

/**
 * Implementation of {@link LocalRecoveryDirectoryProvider}.
 */
public class ThrowingRecoveryDirectoryProvider implements LocalRecoveryDirectoryProvider {
	private static final ThrowingRecoveryDirectoryProvider INSTANCE = new ThrowingRecoveryDirectoryProvider();

	private ThrowingRecoveryDirectoryProvider() {}

	@Override
	public File allocationBaseDirectory(long checkpointId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public File subtaskBaseDirectory(long checkpointId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public File subtaskSpecificCheckpointDirectory(long checkpointId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public File selectAllocationBaseDirectory(int idx) {
		throw new UnsupportedOperationException();
	}

	@Override
	public File selectSubtaskBaseDirectory(int idx) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int allocationBaseDirsCount() {
		return 0;
	}

	public static ThrowingRecoveryDirectoryProvider getInstance() {
		return INSTANCE;
	}
}
