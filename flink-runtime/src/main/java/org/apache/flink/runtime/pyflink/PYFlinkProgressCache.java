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

package org.apache.flink.runtime.pyflink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manage all pyFlink progress, only for pyFlink.
 * */
public class PYFlinkProgressCache {
	private static final Logger LOGGER = LoggerFactory.getLogger(PYFlinkProgressCache.class);
	private static final String THREAD_NAME_PREFIX = "pyFlink process clean thread - ";
	private static final long PROGRESS_CLEAN_IDLE_DURTION_THRESHOLD = 5 * 60 * 1000;
	private static volatile PYFlinkProgressCache instance;

	private ConcurrentMap<Object, PYFlinkProgress> cache = new ConcurrentHashMap<>();
	private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10, r -> {
		Thread t = new Thread(r);
		t.setDaemon(true);
		t.setName(THREAD_NAME_PREFIX);
		return t;
	});
	private volatile ConcurrentMap<Thread, AtomicBoolean> progressHolders = new ConcurrentHashMap<>();
	private volatile ConcurrentMap<Object, AtomicBoolean> holderFlags = new ConcurrentHashMap<>();

	private PYFlinkProgressCache() {
		executorService.scheduleAtFixedRate(this::unusedProgressCleanProc, 1, 1, TimeUnit.MINUTES);
	}

	public static PYFlinkProgressCache getInstance() {
		if (instance == null) {
			synchronized (PYFlinkProgressCache.class) {
				if (instance == null) {
					instance = new PYFlinkProgressCache();
				}
			}
		}

		return instance;
	}

	public PYFlinkProgress put(Object key, PYFlinkProgress progress) {
		PYFlinkProgress preProgress = cache.put(key, progress);
		AtomicBoolean holder = new AtomicBoolean(true);
		progressHolders.put(Thread.currentThread(), holder);
		AtomicBoolean preHolderFlag = holderFlags.put(key, holder);
		if (preHolderFlag != null) {
			preHolderFlag.set(false);
		}
		return preProgress;
	}

	public PYFlinkProgress get(Object key) {
		return cache.get(key);
	}

	public void clear() {
		LOGGER.info("clean pyFlink progress, size:{}", cache.size());
		for (final AtomicBoolean holderFlag : holderFlags.values()) {
			holderFlag.set(false);
		}

		for (final PYFlinkProgress progress : cache.values()) {
			executorService.submit(() -> {
				try {
					Thread.currentThread().setName(THREAD_NAME_PREFIX + progress);
					LOGGER.info("start clean pyFlink process {}", progress);
					progress.clear();
					LOGGER.info("pyFlink process cleaned {}", progress);
				} catch (Exception e) {
					LOGGER.error("clean pyFlink progress error, " + progress, e);
				} finally {
					Thread.currentThread().setName(THREAD_NAME_PREFIX);
				}
			});
		}
		cache.clear();
	}

	public AtomicBoolean getProgressHolderFlag(Thread key) {
		if (!progressHolders.containsKey(key)) {
			return null;
		}
		return progressHolders.get(key);
	}

	public AtomicBoolean getProgressHolderFlag() {
		return getProgressHolderFlag(Thread.currentThread());
	}

	private void unusedProgressCleanProc() {
		List<Object> progressCleaned = new ArrayList<>();
		for (final Map.Entry<Object, PYFlinkProgress> entry : cache.entrySet()) {
			if (entry.getValue().getIdleDuration() >= PROGRESS_CLEAN_IDLE_DURTION_THRESHOLD) {
				progressCleaned.add(entry.getKey());
				executorService.submit(() -> {
					try {
						Thread.currentThread().setName(THREAD_NAME_PREFIX + entry.getValue());
						LOGGER.info("start clean unused pyFlink process {}", entry.getValue());
						entry.getValue().clear();
						LOGGER.info("unused pyFlink process cleaned {}", entry.getValue());
					} catch (Exception e) {
						LOGGER.error("clean unused pyFlink progress error, " + entry.getValue(), e);
					} finally {
						Thread.currentThread().setName(THREAD_NAME_PREFIX);
					}
				});
			}
		}
		for (Object key : progressCleaned) {
			cache.remove(key);
		}
		if (progressCleaned.size() > 0) {
			LOGGER.info("unused pyFlink progress cleaned, size:{}", progressCleaned.size());
		}
	}
}
