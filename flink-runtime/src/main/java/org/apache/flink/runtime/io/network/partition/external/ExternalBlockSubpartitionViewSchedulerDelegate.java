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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.runtime.io.network.partition.ExternalBlockSubpartitionView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A delegate for {@link ExternalBlockSubpartitionViewScheduler} as the framework of subpartition view scheduling.
 * It's an implementation of {@link BlockingQueue} so that it could be easily applied to {@link ThreadPoolExecutor}.
 * Notice that all the interfaces of {@link BlockingQueue} are synchronized so there is no need to synchronize in
 * implementations of {@link ExternalBlockSubpartitionViewScheduler}.
 */
public final class ExternalBlockSubpartitionViewSchedulerDelegate extends AbstractQueue<Runnable>
	implements BlockingQueue<Runnable> {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockSubpartitionViewSchedulerDelegate.class);

	/** Lock used for all public operations. */
	private final ReentrantLock lock;

	/** Condition for blocking when empty. */
	private final Condition notEmpty;

	/** The number of elements in this queue. */
	private int size;

	/** The scheduler to decide the scheduling order of {@link ExternalBlockSubpartitionView}s. */
	private final ExternalBlockSubpartitionViewScheduler scheduler;

	public ExternalBlockSubpartitionViewSchedulerDelegate(ExternalBlockSubpartitionViewScheduler scheduler) {
		this.size = 0;
		this.lock = new ReentrantLock();
		this.notEmpty = lock.newCondition();
		this.scheduler = scheduler;
	}

	@Override
	public boolean offer(Runnable runnable) {
		ExternalBlockSubpartitionView subpartitionView = (ExternalBlockSubpartitionView) runnable;
		if (subpartitionView == null) {
			// Only accept {@link Runnable} of {@link ExternalBlockSubpartitionView} class.
			return false;
		}
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			if (size == Integer.MAX_VALUE) {
				throw new ArithmeticException("Cannot accept subpartition view, queue size reaches Integer.MAX_VALUE");
			}
			scheduler.addToSchedule(subpartitionView);
			size++;
			notEmpty.signal();
		} finally {
			lock.unlock();
		}
		return true;
	}

	/**
	 * The queue is unbounded, this method will never block or return {@code false}.
	 */
	@Override
	public boolean offer(Runnable runnable, long timeout, TimeUnit unit) throws InterruptedException {
		return offer(runnable);
	}

	@Override
	public void put(Runnable runnable) throws InterruptedException {
		offer(runnable);
	}

	@Override
	public Runnable poll() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return dequeue();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Runnable poll(long timeout, TimeUnit unit) throws InterruptedException {
		long nanos = unit.toNanos(timeout);
		final ReentrantLock lock = this.lock;
		lock.lockInterruptibly();
		ExternalBlockSubpartitionView result;
		try {
			while ((result = dequeue()) == null && nanos > 0) {
				nanos = notEmpty.awaitNanos(nanos);
			}
		} finally {
			lock.unlock();
		}
		return result;
	}

	@Override
	public Runnable take() throws InterruptedException {
		lock.lockInterruptibly();
		ExternalBlockSubpartitionView result;
		try {
			while ((result = dequeue()) == null) {
				notEmpty.await();
			}
		} finally {
			lock.unlock();
		}
		return result;
	}

	@Override
	public int drainTo(Collection<? super Runnable> c) {
		return drainTo(c, Integer.MAX_VALUE);
	}

	@Override
	public int drainTo(Collection<? super Runnable> c, int maxElements) {
		if (c == null) {
			throw new NullPointerException();
		}
		if (c == this) {
			throw new IllegalArgumentException();
		}
		if (maxElements <= 0) {
			return 0;
		}
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			int n = Math.min(size, maxElements);
			for (int i = 0; i < n; i++) {
				c.add(dequeue());
			}
			return n;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int size() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			return size;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	public Runnable peek() {
		throw new RuntimeException("Unsupported method.");
	}

	@Override
	public Iterator<Runnable> iterator() {
		throw new RuntimeException("Unsupported method.");
	}

	private final ExternalBlockSubpartitionView dequeue() {
		ExternalBlockSubpartitionView subpartitionView = scheduler.schedule();
		if (subpartitionView != null) {
			int n = size - 1;
			if (n >= 0) {
				size = n;
			}
		} else {
			if (size != 0) {
				LOG.warn("size in ExternalBlockSubpartitionViewScheduler should be 0 because there is no "
					+ "ExternalBlockSubpartitionView, while the actual size is " + size);
				size = 0;
			}
		}
		return subpartitionView;
	}
}
