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

package org.apache.flink.connector.tos;

import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.storage.tos.Tos;
import com.bytedance.storage.tos.TosClient;
import com.bytedance.storage.tos.TosException;
import com.bytedance.storage.tos.TosProperty;
import com.bytedance.storage.tos.model.PartInfo;
import com.bytedance.storage.tos.model.PartUploadCompleteResult;
import com.bytedance.storage.tos.model.PartUploadInitResult;
import com.bytedance.storage.tos.model.PartUploadRequest;
import com.bytedance.storage.tos.model.PartUploadResult;
import com.bytedance.storage.tos.model.PutObjectResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Tos Sink Client.
 */
public class TosSinkClient {

	private static final Logger LOG = LoggerFactory.getLogger(TosSinkClient.class);

	private final TosClient tosClient;
	private final TosOptions tosOptions;

	private PartUploadRequest request;
	private String uploadID;
	private List<PartInfo> partInfos = new ArrayList<>();
	private int partNum = 0;

	public TosSinkClient(TosOptions tosOptions) {
		this.tosClient = Tos.defaultTosClient(new TosProperty()
			.setAccessKey(tosOptions.getAccessKey())
			.setBucket(tosOptions.getBucket())
			.setCluster(tosOptions.getCluster())
			.setTimeout(tosOptions.getTosTimeoutSeconds())
			.setServiceName(tosOptions.getPsm()));
		//.setConsulAgentIP("10.8.120.36")); for local test

		this.tosOptions = tosOptions;
		LOG.info("init TosClient completed");
	}

	/**
	 * init partUpload and init request.
	 */
	public void open() {
		try {
			PartUploadInitResult result = tosClient.partUploadInit(tosOptions.getObjectKey());
			uploadID = result.getUploadID();
			if (uploadID == null) {
				throw new FlinkRuntimeException("getting uploadID failed, which must not be null");
			}
			request = new PartUploadRequest().setKey(tosOptions.getObjectKey()).setUploadID(uploadID);
			LOG.info("TosSinkClient is opened");
		} catch (TosException e) {
			throw new FlinkRuntimeException("TosSinkClient failed to open", e);
		}
	}

	public void fullUpload(byte[] dataSend) {
		try {
			// do not need to care about the result
			PutObjectResult result = tosClient.putObject(tosOptions.getObjectKey(), dataSend);
			if (result == null) {
				throw new FlinkRuntimeException("putObject failed, the result of which "
					+ "must not be null");
			}
			LOG.info("full upload completed");
		} catch (TosException e) {
			throw new FlinkRuntimeException("full upload failed", e);
		}
	}

	public void partUpload(byte[] dataSend) {
		try {
			request.setPartNumber(partNum).setData(dataSend);
			// partNum identify each part upload, it increments by 1 each time
			partNum++;
			PartUploadResult result = tosClient.partUpload(request);
			if (result == null) {
				throw new FlinkRuntimeException("partUpload failed, the result of which "
					+ "must not be null");
			}
			partInfos.add(result.getPart());
			LOG.info("partUpload completed");
		} catch (TosException e) {
			throw new FlinkRuntimeException("partUpload failed", e);
		}
	}

	public void partUploadComplete() {
		try {
			// do not need to care about the result
			PartUploadCompleteResult result = tosClient.partUploadComplete(tosOptions.getObjectKey(),
				uploadID, partInfos);
			if (result == null) {
				throw new FlinkRuntimeException("partUploadComplete failed, the result of which "
					+ "must not be null");
			}
			LOG.info("partUploadComplete completed");
		} catch (TosException e) {
			throw new FlinkRuntimeException("partUploadComplete failed", e);
		}
	}

	public boolean isPartUpload() {
		return partInfos.size() != 0;
	}
}
