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

package org.apache.flink.connectors.rpc.e2e

import org.apache.flink.connectors.rpc.thriftexample.BatchDimMiddlewareService
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosBatchRequest
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosBatchResponse
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosResponse

import java.util.Collections

/**
 * Thrift service test implementation for batch lookup.
 */
class BatchLookupServiceImplTest extends BatchDimMiddlewareService.Iface {

  override def GetDimInfosBatch(request: GetDimInfosBatchRequest): GetDimInfosBatchResponse = {
    val batchResponse = new GetDimInfosBatchResponse()
    val singleResponse = new GetDimInfosResponse()
    singleResponse.setDimensions(Collections.singletonMap("dim0", "value0"))
    singleResponse.setCode(100)
    batchResponse.setResponses(Collections.singletonList(singleResponse))
    batchResponse
  }
}
