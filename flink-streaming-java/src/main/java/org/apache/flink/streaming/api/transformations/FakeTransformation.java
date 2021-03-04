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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This is used for changing the default parallelism in Table Api / SQL.
 */
@Internal
public class FakeTransformation<T> extends PhysicalTransformation<T> {

	private final Transformation<T> input;

	public FakeTransformation(
			Transformation<T> input,
			String name,
			int parallelism) {
		super(name, input.getOutputType(), parallelism);
		this.input = input;
	}

	/**
	 * Returns the input {@code Transformation} of this {@code FakeTransformation}.
	 */
	public Transformation<T> getInput() {
		return input;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		// do nothing for now.
	}

	@Override
	public List<Transformation<?>> getChildren() {
		return Collections.singletonList(input);
	}
}
