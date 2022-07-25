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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.util.ChildFirstClassLoader;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Gives the URLClassLoader a nicer name for debugging purposes.
 */
public class FlinkUserCodeClassLoaders {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkUserCodeClassLoaders.class);

	// the pattern used to parse %ENV_VAR% in class load URLs.
	private static final Pattern parseEnvPattern = Pattern.compile("%[a-zA-Z]\\w*%");

	private FlinkUserCodeClassLoaders() {

	}

	public static URLClassLoader parentFirst(
			URL[] urls,
			ClassLoader parent,
			Consumer<Throwable> classLoadingExceptionHandler) {
		return new ParentFirstClassLoader(urls, parent, classLoadingExceptionHandler);
	}

	public static URLClassLoader childFirst(
			URL[] urls,
			ClassLoader parent,
			String[] alwaysParentFirstPatterns,
			Consumer<Throwable> classLoadingExceptionHandler) {
		return new ChildFirstClassLoader(urls, parent, alwaysParentFirstPatterns, classLoadingExceptionHandler);
	}

	public static URLClassLoader create(
			ResolveOrder resolveOrder,
			URL[] urls,
			ClassLoader parent,
			String[] alwaysParentFirstPatterns,
			Consumer<Throwable> classLoadingExceptionHandler,
			List<URL> externalPlugins) {

		URL[] finalUrls = replaceEnvVarInUrlIfRequired(urls);
		LOG.info("Create FlinkUserCodeClassLoaders resolveOrder = {}", resolveOrder);
		LOG.info("Create FlinkUserCodeClassLoaders urls = {}", Arrays.asList(finalUrls));

		switch (resolveOrder) {
			case CHILD_FIRST:
				return childFirst(wrapWithPluginJars(finalUrls, externalPlugins), parent, alwaysParentFirstPatterns, classLoadingExceptionHandler);
			case PARENT_FIRST:
				return parentFirst(wrapWithPluginJars(finalUrls, externalPlugins), parent, classLoadingExceptionHandler);
			default:
				throw new IllegalArgumentException("Unknown class resolution order: " + resolveOrder);
		}
	}

	/**
	 * Replace the %ENV_VAR% to the actual env var value in URLs.
	 * @param urls the urls waiting to parse and replace
	 * @return the env var replaced urls
	 */
	private static URL[] replaceEnvVarInUrlIfRequired(URL[] urls) {
		URL[] finalUrls = new URL[urls.length];
		for (int i = 0; i < urls.length; i++) {
			String replacedUrl = urls[i].toString();
			Matcher matcher = parseEnvPattern.matcher(replacedUrl);
			while (matcher.find()) {
				String matchedStr = matcher.group();
				String envVar = System.getenv(matchedStr.substring(1, matchedStr.length() - 1));
				if (StringUtils.isNullOrWhitespaceOnly(envVar)) {
					LOG.error("can not find given env var {} in this url {}", matchedStr, urls[i]);
					continue;
				}
				replacedUrl = matcher.replaceFirst(envVar);
				matcher = parseEnvPattern.matcher(replacedUrl);
			}
			try {
				finalUrls[i] = new URL(replacedUrl);
			} catch (MalformedURLException e) {
				// should not be reached
				throw new RuntimeException(e);
			}
		}
		return finalUrls;
	}

	public static URL[] wrapWithPluginJars(URL[] urls, List<URL> externalPlugins) {
		if (!externalPlugins.isEmpty()) {
			LOG.info("Create FlinkUserCodeClassLoaders with plugins urls = {}", externalPlugins);
			List<URL> totalUrls = new ArrayList<>(Arrays.asList(urls));
			totalUrls.addAll(externalPlugins);
			return totalUrls.toArray(new URL[0]);
		} else {
			LOG.info("External plugins is empty.");
		}
		return urls;
	}

	/**
	 * Class resolution order for Flink URL {@link ClassLoader}.
	 */
	public enum ResolveOrder {
		CHILD_FIRST, PARENT_FIRST;

		public static ResolveOrder fromString(String resolveOrder) {
			if (resolveOrder.equalsIgnoreCase("parent-first")) {
				return PARENT_FIRST;
			} else if (resolveOrder.equalsIgnoreCase("child-first")) {
				return CHILD_FIRST;
			} else {
				throw new IllegalArgumentException("Unknown resolve order: " + resolveOrder);
			}
		}
	}

	/**
	 * Regular URLClassLoader that first loads from the parent and only after that from the URLs.
	 */
	public static class ParentFirstClassLoader extends FlinkUserCodeClassLoader {

		ParentFirstClassLoader(URL[] urls, ClassLoader parent, Consumer<Throwable> classLoadingExceptionHandler) {
			super(urls, parent, classLoadingExceptionHandler);
		}
	}
}
