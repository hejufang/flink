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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.annotation.Internal;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Implementation of a {@link RequestIndexer} that buffers {@link ActionRequest ActionRequests}
 * before re-sending them to the Elasticsearch cluster upon request.
 */
@Internal
@NotThreadSafe
class BufferingNoOpRequestIndexer implements RequestIndexer {

	private static final Logger LOG = LoggerFactory.getLogger(BufferingNoOpRequestIndexer.class);

	private ConcurrentLinkedQueue<ActionRequest> bufferedRequests;
	//ActionRequest Object is created when the request comes, and it uses Object.hashcode method to get its hashcode.
	//One ActionRequest for one action, and it will not be copied anywhere.
	private ConcurrentHashMap<ActionRequest, Integer> retryRequestMap;

	private final int maxRetries;

	BufferingNoOpRequestIndexer(int maxRetries) {
		this.bufferedRequests = new ConcurrentLinkedQueue<ActionRequest>();
		this.retryRequestMap = new ConcurrentHashMap<>();
		this.maxRetries = maxRetries;
	}

	@Override
	public void add(DeleteRequest... deleteRequests) {
		checkAndAdd(deleteRequests);
	}

	@Override
	public void add(IndexRequest... indexRequests) {
		checkAndAdd(indexRequests);
	}

	@Override
	public void add(UpdateRequest... updateRequests) {
		checkAndAdd(updateRequests);
	}

	public void checkAndAdd(ActionRequest... actionRequests) {
		for (ActionRequest request : actionRequests) {
			//"maxRetries == -1(default value)" means unlimited retries;
			if (maxRetries == -1 || checkFailureRequestMap(request)) {
				bufferedRequests.add(request);
			} else {
				throw new RuntimeException(String.format("Request: %s reached max-retries: %d", request, maxRetries));
			}
		}
	}

	void processBufferedRequests(RequestIndexer actualIndexer) {
		for (ActionRequest request : bufferedRequests) {
			if (request instanceof IndexRequest) {
				actualIndexer.add((IndexRequest) request);
			} else if (request instanceof DeleteRequest) {
				actualIndexer.add((DeleteRequest) request);
			} else if (request instanceof UpdateRequest) {
				actualIndexer.add((UpdateRequest) request);
			}
		}
		bufferedRequests.clear();
	}

	boolean checkFailureRequestMap(ActionRequest actionRequest) {
		int retries = retryRequestMap.getOrDefault(actionRequest, 0) + 1;
		if (retries > maxRetries) {
			return false;
		} else {
			retryRequestMap.put(actionRequest, retries);
			return true;
		}
	}

	void removeSuccessfulRequest(ActionRequest actionRequest) {
		retryRequestMap.remove(actionRequest);
	}
}
