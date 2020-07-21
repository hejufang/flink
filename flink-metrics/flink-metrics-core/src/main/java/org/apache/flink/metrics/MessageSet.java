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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * User registers a MessageSet instance to report data to Flink data warehouse.
 */
public class MessageSet implements Gauge<MessageSet> {

	private static final int CAPACITY = 1 << 20;

	private final ArrayBlockingQueue<Message> queue;
	private final MessageType messageType;

	public MessageSet(MessageType messageType) {
		this.queue = new ArrayBlockingQueue<>(CAPACITY);
		this.messageType = messageType;
	}

	public void addMessage(Message m) {
		m.getMeta().setMessageType(messageType);
		queue.add(m);
	}

	public Collection<Message> drainMessages() {
		List<Message> messages = new ArrayList<>();
		queue.drainTo(messages);
		return messages;
	}

	@Override
	public MessageSet getValue() {
		return this;
	}

	public MessageType getMessageType() {
		return messageType;
	}
}
