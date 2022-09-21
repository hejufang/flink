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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.util.ExceptionUtils;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Simple {@link MultipleComponentLeaderElectionDriver.Listener} implementation for testing
 * purposes.
 */
public final class TestingLeaderElectionListener
	implements MultipleComponentLeaderElectionDriver.Listener {
	private final BlockingQueue<AbstractLeaderElectionEvent> abstractLeaderElectionEvents =
		new ArrayBlockingQueue<>(10);

	@Override
	public void isLeader() {
		put(new AbstractLeaderElectionEvent.IsLeaderEvent());
	}

	@Override
	public void notLeader() {
		put(new AbstractLeaderElectionEvent.NotLeaderEvent());
	}

	@Override
	public void notifyLeaderInformationChange(
		String componentId, LeaderInformation leaderInformation) {
		put(new AbstractLeaderElectionEvent.LeaderInformationChangeEvent(componentId, leaderInformation));
	}

	@Override
	public void notifyAllKnownLeaderInformation(
		Collection<LeaderInformationWithComponentId> leaderInformationWithComponentIds) {
		put(
			new AbstractLeaderElectionEvent.AllKnownLeaderInformationEvent(
				leaderInformationWithComponentIds));
	}

	private void put(AbstractLeaderElectionEvent abstractLeaderElectionEvent) {
		try {
			abstractLeaderElectionEvents.put(abstractLeaderElectionEvent);
		} catch (InterruptedException e) {
			ExceptionUtils.rethrow(e);
		}
	}

	public <T> T await(Class<T> clazz) throws InterruptedException {
		while (true) {
			final AbstractLeaderElectionEvent abstractLeaderElectionEvent = abstractLeaderElectionEvents.take();

			if (clazz.isAssignableFrom(abstractLeaderElectionEvent.getClass())) {
				return clazz.cast(abstractLeaderElectionEvent);
			}
		}
	}

	public <T> Optional<T> await(Class<T> clazz, Duration timeout) throws InterruptedException {
		final Deadline deadline = Deadline.fromNow(timeout);

		while (true) {
			final Duration timeLeft = deadline.timeLeft();

			if (timeLeft.isNegative()) {
				return Optional.empty();
			} else {
				final Optional<AbstractLeaderElectionEvent> optLeaderElectionEvent =
					Optional.ofNullable(
						abstractLeaderElectionEvents.poll(
							timeLeft.toMillis(), TimeUnit.MILLISECONDS));

				if (optLeaderElectionEvent.isPresent()) {
					final AbstractLeaderElectionEvent abstractLeaderElectionEvent = optLeaderElectionEvent.get();

					if (clazz.isAssignableFrom(abstractLeaderElectionEvent.getClass())) {
						return Optional.of(clazz.cast(optLeaderElectionEvent));
					}
				} else {
					return Optional.empty();
				}
			}
		}
	}
}
