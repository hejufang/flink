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

package org.apache.flink.runtime.state.cache;

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The factory method of using {@link CacheConfiguration} to create {@link Cache},
 * each time a new state is created, a cache is created through {@link CacheStrategyFactory}
 * and registered in {@link CacheManager}, and then the cache is configured according
 * to the registration result of the {@link CacheManager}.
 */
public abstract class CacheStrategyFactory implements Serializable {
	private static final Logger LOG = LoggerFactory.getLogger(CacheStrategyFactory.class);
	private static final String CREATE_METHOD = "createFactory";

	public abstract <K, V> CacheStrategy<K, V> createCacheStrategy();

	public static CacheStrategyFactory createCacheStrategyFactory(CacheConfiguration configuration) throws Exception {
		CacheStrategyFactory fallBackCacheStrategyFactory = new CacheStrategyFactory() {
			@Override
			public <K, V> CacheStrategy<K, V> createCacheStrategy() {
				return new LRUStrategy<>(configuration.getIncrementalRemoveCount());
			}
		};

		String cacheStrategy = configuration.getCacheStrategy();
		switch (cacheStrategy.toLowerCase()) {
			case "lru":
				return fallBackCacheStrategyFactory;
			case "lfu":
				return new CacheStrategyFactory() {
					@Override
					public <K, V> CacheStrategy<K, V> createCacheStrategy() {
						return new LFUStrategy<>(configuration.getIncrementalRemoveCount());
					}
				};
			default:
				try {
					Class<?> clazz = Class.forName(cacheStrategy);
					if (clazz != null) {
						Method method = clazz.getMethod(CREATE_METHOD, Configuration.class);
						if (method != null) {
							Object result = method.invoke(null, configuration);
							if (result != null) {
								return (CacheStrategyFactory) result;
							}
						}
					}
				} catch (ClassNotFoundException cnfe) {
					LOG.warn("Could not find cache strategy class {}.", cacheStrategy);
				} catch (NoSuchMethodException nsme) {
					LOG.warn("Class {} does not has static method {}.", cacheStrategy, CREATE_METHOD);
				} catch (InvocationTargetException ite) {
					LOG.warn("Cannot call static method {} from class {}.", CREATE_METHOD, cacheStrategy);
				} catch (IllegalAccessException iae) {
					LOG.warn("Illegal access while calling method {} from class {}.", CREATE_METHOD, cacheStrategy);
				}

				// fallback in case of an error
				return fallBackCacheStrategyFactory;
		}
	}
}
