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

import java.util.Set;

/**
 * Wrapper of unknown/wrong/right blacked exceptions.
 */
public class BlackedExceptionAccuracy {
	private final Set<Class<? extends Throwable>> unknownBlackedException;
	private final Set<Class<? extends Throwable>> wrongBlackedException;
	private final Set<Class<? extends Throwable>> rightBlackedException;

	public BlackedExceptionAccuracy(Set<Class<? extends Throwable>> unknownBlackedException, Set<Class<? extends Throwable>> wrongBlackedException, Set<Class<? extends Throwable>> rightBlackedException) {
		this.unknownBlackedException = unknownBlackedException;
		this.wrongBlackedException = wrongBlackedException;
		this.rightBlackedException = rightBlackedException;
	}

	public Set<Class<? extends Throwable>> getUnknownBlackedException() {
		return unknownBlackedException;
	}

	public Set<Class<? extends Throwable>> getWrongBlackedException() {
		return wrongBlackedException;
	}

	public Set<Class<? extends Throwable>> getRightBlackedException() {
		return rightBlackedException;
	}

	public boolean hasWrongBlackedException() {
		return !wrongBlackedException.isEmpty();
	}
}
