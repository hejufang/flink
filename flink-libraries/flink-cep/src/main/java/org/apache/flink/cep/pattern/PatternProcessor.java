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

package org.apache.flink.cep.pattern;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.functions.MultiplePatternProcessFunctionV2;
import org.apache.flink.core.io.Versioned;

import java.io.Serializable;

/**
 * A pattern processor defines a {@link Pattern}, how to match the pattern, and how to process
 * the found matches.
 *
 * @param <IN> Base type of the elements appearing in the pattern.
 * @param <OUT> Type of produced elements based on found matches.
 */
public interface PatternProcessor<IN, OUT> extends Serializable, Versioned {
	/**
	 * Returns the ID of the pattern processor.
	 *
	 * @return The ID of the pattern processor.
	 */
	String getId();

	/**
	 * Returns if the pattern processor is alive.
	 *
	 * @return If the pattern processor is alive.
	 */
	Boolean getIsAlive();

	/**
	 * Returns the {@link EventMatcher} to determines whether the pattern processor conern current event.
	 *
	 * @return The key selector of the pattern processor.
	 */
	EventMatcher<IN> getEventMatcher();

	/**
	 * Returns the {@link KeySelector} to extract the key of the elements appearing in the pattern.
	 *
	 * <p>Only data with the same key can be chained to match a pattern.<p/>
	 *
	 * @return The key selector of the pattern processor.
	 */
	KeySelector<IN, Object> getKeySelector();

	/**
	 * Returns the {@link Pattern} to be matched.
	 *
	 * @return The pattern of the pattern processor.
	 */
	Pattern<IN, ?> getPattern();

	/**
	 * Get the {@link MultiplePatternProcessFunctionV2} to process the found matches for the pattern.
	 *
	 * @return The pattern process function of the pattern processor.
	 */
	MultiplePatternProcessFunctionV2<IN, OUT> getPatternProcessFunction();
}
