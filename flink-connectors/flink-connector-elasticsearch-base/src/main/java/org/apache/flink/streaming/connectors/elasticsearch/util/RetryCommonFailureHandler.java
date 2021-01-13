/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch.util;

import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;

/**
 * An {@link ActionRequestFailureHandler} that re-adds requests that failed for common failures that need retries.
 */
public class RetryCommonFailureHandler implements ActionRequestFailureHandler {
	private static final Logger LOG = LoggerFactory.getLogger(RetryCommonFailureHandler.class);
	private static final long serialVersionUID = 1L;
	@Override
	public void onFailure(
			ActionRequest action,
			Throwable failure,
			int restStatusCode,
			RequestIndexer indexer) throws Throwable {

		if (restStatusCode == 400 || restStatusCode == 404) {
			//400 and 404 mean that the format of message is wrong.
			LOG.warn("RestCode:{} Action: {}\nException: {}", restStatusCode, action, failure);
			return;
		}

		// 429 works the same as EsRejectedExecutionException.
		if (restStatusCode == 429 ||
			ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
			LOG.warn("EsRejectedExecutionException: ", failure);
		} else if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
			LOG.warn("ES SocketTimeoutException: ", failure);
		} else if (ExceptionUtils.findThrowable(failure, ElasticsearchException.class).isPresent()) {
			ElasticsearchException e = (ElasticsearchException) failure;
			String errMsg = e.getMessage();
			if (errMsg.contains("type=illegal_argument_exception")) {
				LOG.error("RestCode:{} Action: {}\nIllegalArgumentException: {}", restStatusCode, action, e);
				throw failure;
			} else if (errMsg.contains("type=circuit_breaking_exception")) {
				// Works the same as EsRejectedExecutionException.
				LOG.error("CircuitBreakingException", e);
			} else if (errMsg.contains("type=es_rejected_execution_exception")) {
				// Works the same as EsRejectedExecutionException.
				LOG.error("EsRejectedExecutionException", e);
			} else {
				LOG.error("RestCode:{} Action: {}\nOtherElasticsearchException: ", restStatusCode, action, e);
				throw failure;
			}
		}  else {
			LOG.error("RestCode:{} Action: {}\nOtherException: {}", restStatusCode, action, failure);
			throw failure;
		}
		if (action instanceof IndexRequest) {
			indexer.add((IndexRequest) action);
		} else if (action instanceof UpdateRequest) {
			indexer.add((UpdateRequest) action);
		} else if (action instanceof DeleteRequest) {
			indexer.add((DeleteRequest) action);
		}
	}
}
