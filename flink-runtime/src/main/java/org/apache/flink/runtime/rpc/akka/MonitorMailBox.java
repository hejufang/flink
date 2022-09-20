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

package org.apache.flink.runtime.rpc.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.ActorSystem.Settings;
import akka.dispatch.MailboxType;
import akka.dispatch.MessageQueue;
import akka.dispatch.UnboundedMailbox;
import com.typesafe.config.Config;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class MonitorMailBox implements MailboxType {

	private static final Logger LOG = LoggerFactory.getLogger(MonitorMailBox.class);

	private final Map<ActorRef, MessageQueue> monitorMap = new ConcurrentHashMap<>();

	public MonitorMailBox(Settings settings, Config config) {
		super();
		int interval = config.getInt("monitor-interval");
		if (interval <= 0) {
			return;
		}
		Thread monitorThread = new Thread(() -> {
			while (true) {
				/*
				 * if actor is terminated, remove it from monitor map.
				 */
				final Set<ActorRef> removes = new HashSet<>();
				monitorMap.forEach((actor, messageQueue) -> {
					if (actor.isTerminated()) {
						removes.add(actor);
						return;
					}
					try {
						long start = System.nanoTime();
						int count = messageQueue.numberOfMessages();
						/*
						 * no need to report if no messages pile up.
						 */
						if (count > 0 ) {
							LOG.info("[{}] message queue size: {}, the calculation of message queue size costs: {}ns",
								actor.path(), count, System.nanoTime() - start);
						}
					} catch (final Throwable t) {
						LOG.error("error when monitor mail box, actor: {}", actor.path(), t);
					}
				});
				if(CollectionUtils.isNotEmpty(removes)){
					monitorMap.keySet().removeAll(removes);
				}
				try {
					TimeUnit.SECONDS.sleep(interval);
				} catch (InterruptedException e) {
					throw new FlinkRuntimeException(e);
				}
			}
		});
		monitorThread.setName("akka-monitor-thread");
		monitorThread.setDaemon(true);
		monitorThread.start();
	}

	@Override
	public MessageQueue create(Option<ActorRef> owner, Option<ActorSystem> system) {
		if (!owner.isEmpty()) {
			UnboundedMailbox.MessageQueue mq = new UnboundedMailbox.MessageQueue();
			String ownerStr = owner.get().path().toString();
			LOG.info("create monitor mailbox, owner: {}", ownerStr);
			monitorMap.put(owner.get(), mq);
			return mq;
		}
		throw new FlinkRuntimeException("current owner is null.");
	}
}
