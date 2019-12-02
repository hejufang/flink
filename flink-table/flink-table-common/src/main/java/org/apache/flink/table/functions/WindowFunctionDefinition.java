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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Set;

/**
 * The function definition of an user-defined window function.
 */
@PublicEvolving
public final class WindowFunctionDefinition implements FunctionDefinition {

	private final String name;
	private final WindowFunction windowFunction;

	public WindowFunctionDefinition(String name, WindowFunction windowFunction) {
		this.name = Preconditions.checkNotNull(name);
		this.windowFunction = Preconditions.checkNotNull(windowFunction);
	}

	public WindowFunction getWindowFunction() {
		return windowFunction;
	}

	@Override
	public FunctionKind getKind() {
		return FunctionKind.WINDOW;
	}

	@Override
	public Set<FunctionRequirement> getRequirements() {
		return windowFunction.getRequirements();
	}

	@Override
	public boolean isDeterministic() {
		return windowFunction.isDeterministic();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		WindowFunctionDefinition that = (WindowFunctionDefinition) o;
		return name.equals(that.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name);
	}

	@Override
	public String toString() {
		return name;
	}
}
