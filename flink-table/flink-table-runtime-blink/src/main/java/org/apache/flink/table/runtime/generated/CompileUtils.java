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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.byted.com.bytedance.metrics.UdpMetricsClient;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.apache.curator.shaded.com.google.common.hash.Hashing;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utilities to compile a generated code to a Class.
 */
public final class CompileUtils {

	// used for logging the generated codes to a same place
	private static final Logger CODE_LOG = LoggerFactory.getLogger(CompileUtils.class);

	/**
	 * Cache of compile, Janino generates a new Class Loader and a new Class file every compile
	 * (guaranteeing that the class name will not be repeated). This leads to multiple tasks of
	 * the same process that generate a large number of duplicate class, resulting in a large
	 * number of Meta zone GC (class unloading), resulting in performance bottlenecks. So we add
	 * a cache to avoid this problem.
	 */
	protected static final Cache<ClassKey, Class> COMPILED_CLASS_CACHE = CacheBuilder
		.newBuilder()
		.maximumSize(3000)   // estimated cache size
		.recordStats()
		.build();

	private static final String CLASS_CACHE_METRICS_PREFIX = "flink.class.cache";

	private static final String CLASS_CACHE_METRICS_HIT_RATE = "hit-rate";

	private static final String CLASS_CACHE_METRICS_HIT_COUNT = "hit-count";

	// TODO: find a new way to write metrics in OLAP/Batch scenarios,
	// do not use internal metrics system.
	private static final UdpMetricsClient udpMetricsClient =
		new UdpMetricsClient(CLASS_CACHE_METRICS_PREFIX);

	private static final String FLINK_CLUSTER_NAME = System.getenv("FLINK_CLUSTER_NAME");

	/**
	 * Compiles a generated code to a Class.
	 * @param cl the ClassLoader used to load the class
	 * @param name  the class name
	 * @param code  the generated code
	 * @param <T>   the class type
	 * @return  the compiled class
	 */
	@SuppressWarnings("unchecked")
	public static <T> Class<T> compile(ClassLoader cl, String name, String code) {
		try {
			Class<T> clz = COMPILED_CLASS_CACHE.get(
				new ClassKey(cl.hashCode(), name, code), () -> doCompile(cl, name, code));

			if (FLINK_CLUSTER_NAME != null) {
				udpMetricsClient.emitStoreWithTag(CLASS_CACHE_METRICS_HIT_RATE,
					COMPILED_CLASS_CACHE.stats().hitRate(), "cluster=" + FLINK_CLUSTER_NAME);
				udpMetricsClient.emitStoreWithTag(CLASS_CACHE_METRICS_HIT_COUNT,
					COMPILED_CLASS_CACHE.stats().hitCount(), "cluster=" + FLINK_CLUSTER_NAME);
			}
			return clz;
		} catch (Exception e) {
			throw new FlinkRuntimeException(e.getMessage(), e);
		}
	}

	private static <T> Class<T> doCompile(ClassLoader cl, String name, String code) {
		checkNotNull(cl, "Classloader must not be null.");
		CODE_LOG.debug("Compiling: {} \n\n Code:\n{}", name, code);
		SimpleCompiler compiler = new SimpleCompiler();
		compiler.setParentClassLoader(cl);
		try {
			compiler.cook(code);
		} catch (Throwable t) {
			System.out.println(addLineNumber(code));
			throw new InvalidProgramException(
				"Table program cannot be compiled. This is a bug. Please file an issue.", t);
		}
		try {
			//noinspection unchecked
			return (Class<T>) compiler.getClassLoader().loadClass(name);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Can not load class " + name, e);
		}
	}

	/** Class to use as key for the {@link #COMPILED_CLASS_CACHE}. */
	private static class ClassKey {
		private final int classLoaderId;
		private final String className;
		private final String codeMd5;
		private final int codeLength;

		private ClassKey(int classLoaderId, String className, String code) {
			this.classLoaderId = classLoaderId;
			this.className = className;
			this.codeLength = code.length();
			this.codeMd5 = Hashing.md5().hashBytes(code.getBytes()).toString();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ClassKey classKey = (ClassKey) o;
			return classLoaderId == classKey.classLoaderId &&
				codeLength == classKey.codeLength &&
				Objects.equals(className, classKey.className) &&
				Objects.equals(codeMd5, classKey.codeMd5);
		}

		@Override
		public int hashCode() {
			return Objects.hash(classLoaderId, className, codeMd5, codeLength);
		}

		@Override
		public String toString() {
			return "ClassKey{" +
				"classLoaderId=" + classLoaderId +
				", className='" + className + '\'' +
				", codeMd5Hex='" + codeMd5 + '\'' +
				", codeLength=" + codeLength +
				'}';
		}
	}

	/**
	 * To output more information when an error occurs.
	 * Generally, when cook fails, it shows which line is wrong. This line number starts at 1.
	 */
	private static String addLineNumber(String code) {
		String[] lines = code.split("\n");
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < lines.length; i++) {
			builder.append("/* ").append(i + 1).append(" */").append(lines[i]).append("\n");
		}
		return builder.toString();
	}
}
