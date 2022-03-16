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

package org.apache.flink.metrics;

import org.apache.flink.metrics.warehouse.WarehouseMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * User registers a MessageSet instance to report data to Flink data warehouse.
 */
public class MessageSet<T extends WarehouseMessage> implements Gauge<MessageSet<T>> {

	private static final int CAPACITY = 1 << 20;

	private final LinkedBlockingQueue<Message<T>> queue;
	private final MessageType messageType;

	public MessageSet(MessageType messageType) {
		this.queue = new LinkedBlockingQueue<>(CAPACITY);
		this.messageType = messageType;
	}

	public void addMessage(Message<T> m) {
		m.getMeta().setMessageType(messageType);
		if (queue.size() == CAPACITY) {
			queue.poll();
		}
		queue.offer(m);
	}

	public Collection<Message<T>> drainMessages() {
		List<Message<T>> messages = new ArrayList<>();
		queue.drainTo(messages);
		return messages;
	}

	@Override
	public MessageSet<T> getValue() {
		return this;
	}

	public MessageType getMessageType() {
		return messageType;
	}
}
