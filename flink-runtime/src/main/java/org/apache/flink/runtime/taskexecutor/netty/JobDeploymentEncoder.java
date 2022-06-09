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

package org.apache.flink.runtime.taskexecutor.netty;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

/**
 * Socket encoder for {@link org.apache.flink.runtime.deployment.JobDeploymentDescriptor}.
 */
public class JobDeploymentEncoder extends MessageToByteEncoder<Serializable> {

	@Override
	protected void encode(
			ChannelHandlerContext channelHandlerContext,
			Serializable serializable,
			ByteBuf byteBuf) throws Exception {
		if (serializable instanceof JobDeployment) {
			JobDeployment jobDeploymentDescriptor = (JobDeployment) serializable;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputView dataOutputView = new DataOutputViewStreamWrapper(baos);
			jobDeploymentDescriptor.write(dataOutputView);
			baos.flush();

			byte[] data = baos.toByteArray();
			int dataSize = data.length;
			byteBuf.writeInt(dataSize);
			byteBuf.writeBytes(data);
			baos.close();
		}
	}
}
