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

package org.apache.flink.table.sources;

/**
 * Adds support for topN push-down to a {@link TableSource}.
 * A {@link TableSource} extending this interface is able to fetch top n records before returning.
 */
public interface TopNableTableSource<T> {

	/**
	 * Return the flag to indicate whether topN push down has been tried. Must return true on
	 * the returned instance of {@link #applyTopN(TopNInfo topN)}.
	 */
	boolean isTopNPushedDown();

	/**
	 * Check and push down the topN to the table source.
	 *
	 * @param topN TopNInfo.
	 * @return A new cloned instance of {@link TableSource}.
	 */
	TableSource<T> applyTopN(TopNInfo topN);
}
