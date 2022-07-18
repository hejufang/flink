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

package org.apache.flink.runtime.taskmanager;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Thread pool for task execute thread.
 */
public class TaskThreadPoolExecutor extends ThreadPoolExecutor {

	private TaskThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue,
			threadFactory);
	}

	public static TaskThreadPoolExecutor newCachedThreadPool(ThreadFactory threadFactory) {

		checkArgument(threadFactory instanceof TaskThreadFactory);

		return new TaskThreadPoolExecutor(0, Integer.MAX_VALUE,
			60L, TimeUnit.SECONDS,
			new SynchronousQueue<Runnable>(),
			threadFactory);
	}

	public CompletableFuture<Void> submit(Runnable command, String threadName) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		super.submit(wrap(command, threadName, null, future));
		return future;
	}

	public CompletableFuture<Void> submit(Runnable command, String threadName, ClassLoader classLoader) {
		CompletableFuture<Void> future = new CompletableFuture<>();
		super.submit(wrap(command, threadName, classLoader, future));
		return future;
	}

	/**
	 * Wrap runnable.
	 */
	public static Runnable wrap(final Runnable runnable, String threadName, ClassLoader classLoader, CompletableFuture<Void> future) {
		return new Runnable() {
			@Override
			public void run() {
				TaskExecuteThread thread = (TaskExecuteThread) Thread.currentThread();
				String oriThreadName = thread.getName();
				thread.setTaskExecuteThreadName(threadName);
				if (classLoader != null) {
					thread.setContextClassLoader(classLoader);
				}
				try {
					runnable.run();
				} finally {
					thread.setTaskExecuteThreadName(oriThreadName);
				}
				future.complete(null);
			}
		};
	}

	/**
	 * ThreadFactory used to create TaskExecuteThread.
	 */
	public static class TaskThreadFactory implements ThreadFactory {
		private ThreadGroup group;
		private String namePrefix;
		private boolean daemon;
		private Thread.UncaughtExceptionHandler exceptionHandler;
		private final AtomicInteger threadNumber = new AtomicInteger(1);

		public TaskThreadFactory(ThreadGroup group, String namePrefix, boolean daemon, Thread.UncaughtExceptionHandler exceptionHandler) {
			this.group = group;
			this.namePrefix = namePrefix;
			this.daemon = daemon;
			this.exceptionHandler = exceptionHandler;
		}

		public Thread newThread(Runnable r) {
			Thread t = new TaskExecuteThread(group, r,
				namePrefix + threadNumber.getAndIncrement(),
				0);
			t.setDaemon(daemon);
			if (exceptionHandler != null) {
				t.setUncaughtExceptionHandler(exceptionHandler);
			}
			return t;
		}
	}

	static class TaskExecuteThread extends Thread {
		private final Object lock = new Object();

		public TaskExecuteThread(ThreadGroup group, Runnable target, String name, long stackSize) {
			super(group, target, name, stackSize);
		}

		public void interrupt(String name) {
			synchronized (lock) {
				if (getName().equals(name)) {
					super.interrupt();
				}
			}
		}

		public void setTaskExecuteThreadName(String name){
			synchronized (lock){
				super.setName(name);
			}
		}
	}
}
