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

import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.ByteToMessageDecoder;

import java.io.ByteArrayInputStream;
import java.util.List;

/**
 * Socket decoder for {@link org.apache.flink.runtime.deployment.JobDeploymentDescriptor}.
 */
public class JobDeploymentDecoder extends ByteToMessageDecoder {

	private static final int BODY_LENGTH = 4;

	@Override
	protected void decode(
			ChannelHandlerContext channelHandlerContext,
			ByteBuf byteBuf,
			List<Object> list) throws Exception {
		if (byteBuf.readableBytes() >= BODY_LENGTH) {
			byteBuf.markReaderIndex();
			int dataSize = byteBuf.readInt();
			if (dataSize < 0 || byteBuf.readableBytes() < 0) {
				return;
			}
			if (byteBuf.readableBytes() < dataSize) {
				byteBuf.resetReaderIndex();
				return;
			}
			byte[] data = new byte[dataSize];
			byteBuf.readBytes(data);
			JobDeployment jd = new JobDeployment();
			jd.read(new DataInputViewStreamWrapper(new ByteArrayInputStream(data)));
			list.add(jd);
		}
	}
}
