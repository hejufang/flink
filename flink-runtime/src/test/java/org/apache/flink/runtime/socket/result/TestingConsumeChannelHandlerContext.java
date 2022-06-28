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

package org.apache.flink.runtime.socket.result;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelProgressivePromise;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.util.Attribute;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.EventExecutor;

import java.net.SocketAddress;
import java.util.function.Consumer;

/**
 * Testing handler context.
 */
public class TestingConsumeChannelHandlerContext implements ChannelHandlerContext {
	private final Consumer<Object> flushConsumer;

	private Channel channel = null;

	private TestingConsumeChannelHandlerContext(Consumer<Object> flushConsumer) {
		this.flushConsumer = flushConsumer;
	}

	private TestingConsumeChannelHandlerContext(Consumer<Object> flushConsumer, Channel channel) {
		this.flushConsumer = flushConsumer;
		this.channel = channel;
	}

	@Override
	public Channel channel() {
		return channel == null ? new TestingConsumeChannel(flushConsumer) : channel;
	}

	@Override
	public EventExecutor executor() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String name() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandler handler() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isRemoved() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext fireChannelRegistered() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext fireChannelUnregistered() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext fireChannelActive() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext fireChannelInactive() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext fireExceptionCaught(Throwable throwable) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext fireUserEventTriggered(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext fireChannelRead(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext fireChannelReadComplete() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext fireChannelWritabilityChanged() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture bind(SocketAddress socketAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress socketAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture disconnect() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture close() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture deregister() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture bind(SocketAddress socketAddress, ChannelPromise channelPromise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress socketAddress, ChannelPromise channelPromise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1, ChannelPromise channelPromise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture disconnect(ChannelPromise channelPromise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture close(ChannelPromise channelPromise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture deregister(ChannelPromise channelPromise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext read() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture write(Object o) {
		return write(o, null);
	}

	@Override
	public ChannelFuture write(Object o, ChannelPromise channelPromise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelHandlerContext flush() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture writeAndFlush(Object o, ChannelPromise channelPromise) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture writeAndFlush(Object o) {
		flushConsumer.accept(o);
		return new DefaultChannelPromise(new EmbeddedChannel());
	}

	@Override
	public ChannelPromise newPromise() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelProgressivePromise newProgressivePromise() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture newSucceededFuture() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelFuture newFailedFuture(Throwable throwable) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPromise voidPromise() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ChannelPipeline pipeline() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ByteBufAllocator alloc() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> Attribute<T> attr(AttributeKey<T> attributeKey) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> boolean hasAttr(AttributeKey<T> attributeKey) {
		throw new UnsupportedOperationException();
	}

	public static TestingConsumeChannelHandlerBuilder newBuilder() {
		return new TestingConsumeChannelHandlerBuilder();
	}

	/**
	 * Builder for testing consume channel handler.
	 */
	public static class TestingConsumeChannelHandlerBuilder {
		private Consumer<Object> flushConsumer;

		private Channel channel;

		public TestingConsumeChannelHandlerBuilder setWriteAndFlushConsumer(Consumer<Object> flushConsumer) {
			this.flushConsumer = flushConsumer;
			return this;
		}

		public TestingConsumeChannelHandlerBuilder setChannel(Channel channel) {
			this.channel = channel;
			return this;
		}

		public TestingConsumeChannelHandlerContext build() {
			return new TestingConsumeChannelHandlerContext(flushConsumer, channel);
		}
	}

	/**
	 * Channel implementation for test.
	 */
	public static class TestingConsumeChannel extends EmbeddedChannel {
		private final Consumer<Object> flushConsumer;

		public TestingConsumeChannel(Consumer<Object> flushConsumer) {
			this.flushConsumer = flushConsumer;
		}

		@Override
		public ChannelFuture writeAndFlush(Object msg) {
			flushConsumer.accept(msg);
			return new DefaultChannelPromise(this);
		}

		@Override
		public boolean isWritable() {
			return true;
		}
	}
}
