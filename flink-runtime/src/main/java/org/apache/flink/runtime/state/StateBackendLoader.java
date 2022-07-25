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

package org.apache.flink.runtime.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.plugin.DirectoryBasedPluginFinder;
import org.apache.flink.core.plugin.PluginConfig;
import org.apache.flink.core.plugin.PluginDescriptor;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackendFactory;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.FlinkUserCodeClassLoader;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class contains utility methods to load state backends from configurations.
 */
public class StateBackendLoader {
	private static final Logger LOG = LoggerFactory.getLogger(StateBackendLoader.class);

	// ------------------------------------------------------------------------
	//  Configuration shortcut names
	// ------------------------------------------------------------------------

	/** The shortcut configuration name for the MemoryState backend that checkpoints to the JobManager */
	public static final String MEMORY_STATE_BACKEND_NAME = "jobmanager";

	/** The shortcut configuration name for the FileSystem State backend */
	public static final String FS_STATE_BACKEND_NAME = "filesystem";

	/** The shortcut configuration name for the RocksDB State Backend */
	public static final String ROCKSDB_STATE_BACKEND_NAME = "rocksdb";

	/** The shortcut configuration name for the TerarkDB State Backend */
	public static final String TERARKDB_STATE_BACKEND_NAME = "terarkdb";

	public static final String STATE_BACKEND_PLUGIN_PREFIX = "flink-statebackend-";

	// ------------------------------------------------------------------------
	//  Loading the state backend from a configuration 
	// ------------------------------------------------------------------------

	/**
	 * Loads the state backend from the configuration, from the parameter 'state.backend', as defined
	 * in {@link CheckpointingOptions#STATE_BACKEND}.
	 *
	 * <p>The state backends can be specified either via their shortcut name, or via the class name
	 * of a {@link StateBackendFactory}. If a StateBackendFactory class name is specified, the factory
	 * is instantiated (via its zero-argument constructor) and its
	 * {@link StateBackendFactory#createFromConfig(ReadableConfig, ClassLoader)} method is called.
	 *
	 * <p>Recognized shortcut names are '{@value StateBackendLoader#MEMORY_STATE_BACKEND_NAME}',
	 * '{@value StateBackendLoader#FS_STATE_BACKEND_NAME}', and
	 * '{@value StateBackendLoader#ROCKSDB_STATE_BACKEND_NAME}'.
	 *
	 * @param config The configuration to load the state backend from
	 * @param classLoader The class loader that should be used to load the state backend
	 * @param logger Optionally, a logger to log actions to (may be null)
	 *
	 * @return The instantiated state backend.
	 *
	 * @throws DynamicCodeLoadingException
	 *             Thrown if a state backend factory is configured and the factory class was not
	 *             found or the factory could not be instantiated
	 * @throws IllegalConfigurationException
	 *             May be thrown by the StateBackendFactory when creating / configuring the state
	 *             backend in the factory
	 * @throws IOException
	 *             May be thrown by the StateBackendFactory when instantiating the state backend
	 */
	public static StateBackend loadStateBackendFromConfig(
			ReadableConfig config,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

		checkNotNull(config, "config");
		checkNotNull(classLoader, "classLoader");

		final String backendName = config.get(CheckpointingOptions.STATE_BACKEND);
		if (backendName == null) {
			return null;
		}

		// by default the factory class is the backend name 
		String factoryClassName = backendName;

		switch (backendName.toLowerCase()) {
			case MEMORY_STATE_BACKEND_NAME:
				MemoryStateBackend memBackend = new MemoryStateBackendFactory().createFromConfig(config, classLoader);

				if (logger != null) {
					Path memExternalized = memBackend.getCheckpointPath();
					String extern = memExternalized == null ? "" :
							" (externalized to " + memExternalized + ')';
					logger.info("State backend is set to heap memory (checkpoint to JobManager) {}", extern);
				}
				return memBackend;

			case FS_STATE_BACKEND_NAME:
				FsStateBackend fsBackend = new FsStateBackendFactory().createFromConfig(config, classLoader);
				if (logger != null) {
					logger.info("State backend is set to heap memory (checkpoints to filesystem \"{}\")",
							fsBackend.getCheckpointPath());
				}
				return fsBackend;

			case ROCKSDB_STATE_BACKEND_NAME:
			case TERARKDB_STATE_BACKEND_NAME:
				if (!(classLoader instanceof FlinkUserCodeClassLoader)) {
					classLoader = buildUserCodeClassLoader(Collections.emptyList(), Collections.emptyList(), classLoader, (Configuration) config);
				}
				factoryClassName = "org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory";
				// fall through to the 'default' case that uses reflection to load the backend
				// that way we can keep RocksDB in a separate module

			default:
				if (logger != null) {
					logger.info("Loading state backend via factory {}", factoryClassName);
				}

				StateBackendFactory<?> factory;
				try {
					@SuppressWarnings("rawtypes")
					Class<? extends StateBackendFactory> clazz =
							Class.forName(factoryClassName, false, classLoader)
									.asSubclass(StateBackendFactory.class);

					factory = clazz.newInstance();
				}
				catch (ClassNotFoundException e) {
					throw new DynamicCodeLoadingException(
							"Cannot find configured state backend factory class: " + backendName, e);
				}
				catch (ClassCastException | InstantiationException | IllegalAccessException e) {
					throw new DynamicCodeLoadingException("The class configured under '" +
							CheckpointingOptions.STATE_BACKEND.key() + "' is not a valid state backend factory (" +
							backendName + ')', e);
				}

				return factory.createFromConfig(config, classLoader);
		}
	}

	/**
	 * Checks if an application-defined state backend is given, and if not, loads the state
	 * backend from the configuration, from the parameter 'state.backend', as defined
	 * in {@link CheckpointingOptions#STATE_BACKEND}. If no state backend is configured, this instantiates the
	 * default state backend (the {@link MemoryStateBackend}). 
	 *
	 * <p>If an application-defined state backend is found, and the state backend is a
	 * {@link ConfigurableStateBackend}, this methods calls {@link ConfigurableStateBackend#configure(ReadableConfig, ClassLoader)}
	 * on the state backend.
	 *
	 * <p>Refer to {@link #loadStateBackendFromConfig(ReadableConfig, ClassLoader, Logger)} for details on
	 * how the state backend is loaded from the configuration.
	 *
	 * @param config The configuration to load the state backend from
	 * @param classLoader The class loader that should be used to load the state backend
	 * @param logger Optionally, a logger to log actions to (may be null)
	 *
	 * @return The instantiated state backend.
	 *
	 * @throws DynamicCodeLoadingException
	 *             Thrown if a state backend factory is configured and the factory class was not
	 *             found or the factory could not be instantiated
	 * @throws IllegalConfigurationException
	 *             May be thrown by the StateBackendFactory when creating / configuring the state
	 *             backend in the factory
	 * @throws IOException
	 *             May be thrown by the StateBackendFactory when instantiating the state backend
	 */
	public static StateBackend fromApplicationOrConfigOrDefault(
			@Nullable StateBackend fromApplication,
			Configuration config,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {

		checkNotNull(config, "config");
		checkNotNull(classLoader, "classLoader");

		final StateBackend backend;

		// (1) the application defined state backend has precedence
		if (fromApplication != null) {
			if (logger != null) {
				logger.info("Using application-defined state backend: {}", fromApplication);
			}

			// see if this is supposed to pick up additional configuration parameters
			if (fromApplication instanceof ConfigurableStateBackend) {
				// needs to pick up configuration
				if (logger != null) {
					logger.info("Configuring application-defined state backend with job/cluster config");
				}

				backend = ((ConfigurableStateBackend) fromApplication).configure(config, classLoader);
			}
			else {
				// keep as is!
				backend = fromApplication;
			}
		}
		else {
			// (2) check if the config defines a state backend
			final StateBackend fromConfig = loadStateBackendFromConfig(config, classLoader, logger);
			if (fromConfig != null) {
				backend = fromConfig;
			}
			else {
				// (3) use the default
				backend = new MemoryStateBackendFactory().createFromConfig(config, classLoader);
				if (logger != null) {
					logger.info("No state backend has been configured, using default (Memory / JobManager) {}", backend);
				}
			}
		}

		return backend;
	}

	/**
	 * The logic is consistent with that in ClientUtils for future convergence.
	 */
	private static ClassLoader buildUserCodeClassLoader(
		List<URL> jars,
		List<URL> classpaths,
		ClassLoader parent,
		Configuration configuration) {

		URL[] urls = new URL[jars.size() + classpaths.size()];
		for (int i = 0; i < jars.size(); i++) {
			urls[i] = jars.get(i);
		}
		for (int i = 0; i < classpaths.size(); i++) {
			urls[i + jars.size()] = classpaths.get(i);
		}
		final String[] alwaysParentFirstLoaderPatterns = CoreOptions.getParentFirstLoaderPatterns(configuration);
		final String classLoaderResolveOrder =
			configuration.get(CoreOptions.CLASSLOADER_RESOLVE_ORDER);
		FlinkUserCodeClassLoaders.ResolveOrder resolveOrder =
			FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder);
		List<URL> stateBackendPlugins = findStateBackendPlugins(configuration);
		URLClassLoader classLoader = FlinkUserCodeClassLoaders.create(resolveOrder, urls, parent, alwaysParentFirstLoaderPatterns, NOOP_EXCEPTION_HANDLER, stateBackendPlugins);
		return classLoader;
	}

	/**
	 * Use {@link org.apache.flink.core.plugin.PluginFinder} to find the
	 * {@link StateBackend} plugin from the plugins directory.
	 */
	public static List<URL> findStateBackendPlugins(Configuration configuration) {
		try {
			String stateBackend = configuration.get(CheckpointingOptions.STATE_BACKEND);
			String pluginsString = StringUtils.isNullOrWhitespaceOnly(stateBackend) ? null : STATE_BACKEND_PLUGIN_PREFIX + stateBackend;
			PluginConfig pluginConfig = PluginConfig.fromConfiguration(configuration);
			if (!StringUtils.isNullOrWhitespaceOnly(pluginsString) && pluginConfig.getPluginsPath().isPresent()) {
				Set<String> plugins = Arrays.stream(pluginsString.split(";"))
					.map(String::toLowerCase)
					.collect(Collectors.toSet());
				LOG.info("find plugins from {}", pluginConfig.getPluginsPath().get());
				Collection<PluginDescriptor> pluginDescriptors = new DirectoryBasedPluginFinder(pluginConfig.getPluginsPath().get()).findPlugins();
				return pluginDescriptors.stream()
					.filter(p -> plugins.contains(p.getPluginId().toLowerCase()))
					.flatMap((Function<PluginDescriptor, Stream<URL>>) pluginDescriptor -> Arrays.stream(pluginDescriptor.getPluginResourceURLs()))
					.collect(Collectors.toList());
			}
			return new ArrayList<>();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Failed to find required stateBackend plugin.", e);
		}
	}

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated */
	private StateBackendLoader() {}
}
