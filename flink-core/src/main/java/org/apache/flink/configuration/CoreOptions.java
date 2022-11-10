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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.api.common.ExecutionConfig.DefaultPartitioner;
import org.apache.flink.configuration.description.Description;

import org.apache.flink.shaded.guava18.com.google.common.base.Splitter;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * The set of configuration options for core parameters.
 */
@PublicEvolving
@ConfigGroups(groups = {
	@ConfigGroup(name = "Environment", keyPrefix = "env")
})
public class CoreOptions {

	// ------------------------------------------------------------------------
	//  Classloading Parameters
	// ------------------------------------------------------------------------

	/**
	 * Defines the class resolution strategy when loading classes from user code,
	 * meaning whether to first check the user code jar ({@code "child-first"}) or
	 * the application classpath ({@code "parent-first"})
	 *
	 * <p>The default settings indicate to load classes first from the user code jar,
	 * which means that user code jars can include and load different dependencies than
	 * Flink uses (transitively).
	 *
	 * <p>Exceptions to the rules are defined via {@link #ALWAYS_PARENT_FIRST_LOADER_PATTERNS}.
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_CLASS_LOADING)
	public static final ConfigOption<String> CLASSLOADER_RESOLVE_ORDER = ConfigOptions
		.key("classloader.resolve-order")
		.defaultValue("child-first")
		.withDescription("Defines the class resolution strategy when loading classes from user code, meaning whether to" +
			" first check the user code jar (\"child-first\") or the application classpath (\"parent-first\")." +
			" The default settings indicate to load classes first from the user code jar, which means that user code" +
			" jars can include and load different dependencies than Flink uses (transitively).");

	/**
	 * The namespace patterns for classes that are loaded with a preference from the
	 * parent classloader, meaning the application class path, rather than any user code
	 * jar file. This option only has an effect when {@link #CLASSLOADER_RESOLVE_ORDER} is
	 * set to {@code "child-first"}.
	 *
	 * <p>It is important that all classes whose objects move between Flink's runtime and
	 * any user code (including Flink connectors that run as part of the user code) are
	 * covered by these patterns here. Otherwise it is be possible that the Flink runtime
	 * and the user code load two different copies of a class through the different class
	 * loaders. That leads to errors like "X cannot be cast to X" exceptions, where both
	 * class names are equal, or "X cannot be assigned to Y", where X should be a subclass
	 * of Y.
	 *
	 * <p>The following classes are loaded parent-first, to avoid any duplication:
	 * <ul>
	 *     <li>All core Java classes (java.*), because they must never be duplicated.</li>
	 *     <li>All core Scala classes (scala.*). Currently Scala is used in the Flink
	 *         runtime and in the user code, and some Scala classes cross the boundary,
	 *         such as the <i>FunctionX</i> classes. That may change if Scala eventually
	 *         lives purely as part of the user code.</li>
	 *     <li>All Flink classes (org.apache.flink.*). Note that this means that connectors
	 *         and formats (flink-avro, etc) are loaded parent-first as well if they are in the
	 *         core classpath.</li>
	 *     <li>Java annotations and loggers, defined by the following list:
	 *         javax.annotation;org.slf4j;org.apache.log4j;org.apache.logging;org.apache.commons.logging;ch.qos.logback.
	 *         This is done for convenience, to avoid duplication of annotations and multiple
	 *         log bindings.</li>
	 * </ul>
	 */
	@Documentation.Section(Documentation.Sections.EXPERT_CLASS_LOADING)
	public static final ConfigOption<String> ALWAYS_PARENT_FIRST_LOADER_PATTERNS = ConfigOptions
		.key("classloader.parent-first-patterns.default")
		.defaultValue("java.;scala.;org.apache.flink.;com.esotericsoftware.kryo;org.apache.hadoop.;javax.annotation.;org.slf4j;org.apache.log4j;org.apache.logging;org.apache.commons.logging;ch.qos.logback;org.xml;javax.xml;org.apache.xerces;org.w3c")
		.withDeprecatedKeys("classloader.parent-first-patterns")
		.withDescription("A (semicolon-separated) list of patterns that specifies which classes should always be" +
			" resolved through the parent ClassLoader first. A pattern is a simple prefix that is checked against" +
			" the fully qualified class name. This setting should generally not be modified. To add another pattern we" +
			" recommend to use \"classloader.parent-first-patterns.additional\" instead.");

	@Documentation.Section(Documentation.Sections.EXPERT_CLASS_LOADING)
	public static final ConfigOption<String> ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL = ConfigOptions
		.key("classloader.parent-first-patterns.additional")
		.defaultValue("")
		.withDescription("A (semicolon-separated) list of patterns that specifies which classes should always be" +
			" resolved through the parent ClassLoader first. A pattern is a simple prefix that is checked against" +
			" the fully qualified class name. These patterns are appended to \"" + ALWAYS_PARENT_FIRST_LOADER_PATTERNS.key() + "\".");

	@Documentation.Section(Documentation.Sections.EXPERT_CLASS_LOADING)
	public static final ConfigOption<Boolean> FAIL_ON_USER_CLASS_LOADING_METASPACE_OOM = ConfigOptions
		.key("classloader.fail-on-metaspace-oom-error")
		.booleanType()
		.defaultValue(true)
		.withDescription("Fail Flink JVM processes if 'OutOfMemoryError: Metaspace' is " +
			"thrown while trying to load a user code class.");

	@Documentation.Section(Documentation.Sections.EXPERT_CLASS_LOADING)
	public static final ConfigOption<Boolean> USE_SYSTEM_CLASS_LOADER_WHEN_LIBS_OF_USER_CLASS_LOADER_ENABLED =
		ConfigOptions
			.key("classloader.use-system-class-loader-when-libs-of-user-class-loader-is-empty.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to use system class loader when libs of user class loader is empty.");

	public static String[] getParentFirstLoaderPatterns(Configuration config) {
		String base = config.getString(ALWAYS_PARENT_FIRST_LOADER_PATTERNS);
		String append = config.getString(ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL);
		return parseParentFirstLoaderPatterns(base, append);
	}

	/**
	 * Plugin-specific option of {@link #ALWAYS_PARENT_FIRST_LOADER_PATTERNS}. Plugins use this parent first list
	 * instead of the global version.
	 */
	@Documentation.ExcludeFromDocumentation("Plugin classloader list is considered an implementation detail. " +
		"Configuration only included in case to mitigate unintended side-effects of this young feature.")
	public static final ConfigOption<String> PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS = ConfigOptions
		.key("plugin.classloader.parent-first-patterns.default")
		.stringType()
		.defaultValue("java.;scala.;org.apache.flink.;javax.annotation.;org.slf4j;org.apache.log4j;org.apache" +
			".logging;org.apache.commons.logging;ch.qos.logback")
		.withDescription("A (semicolon-separated) list of patterns that specifies which classes should always be" +
			" resolved through the plugin parent ClassLoader first. A pattern is a simple prefix that is checked " +
			" against the fully qualified class name. This setting should generally not be modified. To add another " +
			" pattern we recommend to use \"plugin.classloader.parent-first-patterns.additional\" instead.");

	@Documentation.ExcludeFromDocumentation("Plugin classloader list is considered an implementation detail. " +
		"Configuration only included in case to mitigate unintended side-effects of this young feature.")
	public static final ConfigOption<String> PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL = ConfigOptions
		.key("plugin.classloader.parent-first-patterns.additional")
		.stringType()
		.defaultValue("")
		.withDescription("A (semicolon-separated) list of patterns that specifies which classes should always be" +
			" resolved through the plugin parent ClassLoader first. A pattern is a simple prefix that is checked " +
			" against the fully qualified class name. These patterns are appended to \"" +
			PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS.key() + "\".");

	public static String[] getPluginParentFirstLoaderPatterns(Configuration config) {
		String base = config.getString(PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS);
		String append = config.getString(PLUGIN_ALWAYS_PARENT_FIRST_LOADER_PATTERNS_ADDITIONAL);
		return parseParentFirstLoaderPatterns(base, append);
	}

	private static String[] parseParentFirstLoaderPatterns(String base, String append) {
		Splitter splitter = Splitter.on(';').omitEmptyStrings();
		return Iterables.toArray(Iterables.concat(splitter.split(base), splitter.split(append)), String.class);
	}

	// ------------------------------------------------------------------------
	//  process parameters
	// ------------------------------------------------------------------------

	public static final ConfigOption<String> FLINK_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts")
		.stringType()
		.defaultValue("")
		.withDescription(Description.builder().text("Java options to start the JVM of all Flink processes with.").build());

	public static final ConfigOption<String> FLINK_JM_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts.jobmanager")
		.stringType()
		.defaultValue("")
		.withDescription(Description.builder().text("Java options to start the JVM of the JobManager with.").build());

	public static final ConfigOption<String> FLINK_TM_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts.taskmanager")
		.stringType()
		.defaultValue("")
		.withDescription(Description.builder().text("Java options to start the JVM of the TaskManager with.").build());

	public static final ConfigOption<String> FLINK_HS_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts.historyserver")
		.stringType()
		.defaultValue("")
		.withDescription(Description.builder().text("Java options to start the JVM of the HistoryServer with.").build());

	public static final ConfigOption<String> FLINK_CLI_JVM_OPTIONS = ConfigOptions
		.key("env.java.opts.client")
		.stringType()
		.defaultValue("")
		.withDescription(Description.builder().text("Java options to start the JVM of the Flink Client with.").build());

	/**
	 * This options is here only for documentation generation, it is only
	 * evaluated in the shell scripts.
	 */
	@SuppressWarnings("unused")
	public static final ConfigOption<String> FLINK_LOG_DIR = ConfigOptions
		.key("env.log.dir")
		.noDefaultValue()
		.withDescription("Defines the directory where the Flink logs are saved. It has to be an absolute path." +
			" (Defaults to the log directory under Flink’s home)");

	/**
	 * This options is here only for documentation generation, it is only
	 * evaluated in the shell scripts.
	 */
	@SuppressWarnings("unused")
	public static final ConfigOption<Integer> FLINK_LOG_MAX = ConfigOptions
		.key("env.log.max")
		.defaultValue(5)
		.withDescription("The maximum number of old log files to keep.");

	public static final ConfigOption<String> FLINK_LOG_LEVEL = ConfigOptions
		.key("env.log.level")
		.defaultValue("INFO")
		.withDescription("Override the log.level property on log4j.properties");

	public static final ConfigOption<Boolean> FLINK_GC_LOG_ENABLED = ConfigOptions
		.key("env.gc.log.enabled")
		.booleanType()
		.defaultValue(true)
		.withDescription("Whether enabled gc log.");

	public static final ConfigOption<String> FLINK_GC_LOG_OPTS = ConfigOptions
		.key("env.gc.log.opts")
		.defaultValue("")
		.withDescription("Write all gc configuration by the JVM to a log file, like: -XX:+PrintGCDetails " +
			"-XX:+PrintGCDateStamps");

	public static final ConfigOption<Boolean> FLINK_JVM_ERROR_FILE_ENABLED = ConfigOptions
		.key("env.jvm.error.file")
		.defaultValue(true)
		.withDescription("Generate a error file when jvm crash");

	public static final ConfigOption<Boolean> FLINK_GC_G1_ENABLE = ConfigOptions
		.key("flink.gc.g1")
		.defaultValue(false)
		.withDescription("Use g1 GC in flink JVM.");

	public static final ConfigOption<Integer> FLINK_MAX_GC_PAUSE_MILLIS = ConfigOptions
		.key("flink.gc.MaxGCPauseMillis")
		.defaultValue(50)
		.withDescription("The max GCPauseMillis in G1, just effective while enbale G1 GC.");

	public static final ConfigOption<Boolean> FLINK_GC_THREAD_NUM_USE_CORES = ConfigOptions
		.key("flink.parallel.gc.thread.use.cores")
		.defaultValue(false)
		.withDescription("The gc thread num set to the core num of JM or TM.");

	public static final ConfigOption<Boolean> FLINK_ENABLE_ASYNC_LOGGER = ConfigOptions
		.key("flink.enable-async-logger")
		.defaultValue(false)
		.withDescription("Whether enable use async logger for JM/TM or not.");

	public static final ConfigOption<Boolean> FLINK_DUMP_OOM_ENABLED = ConfigOptions
		.key("env.jvm.dump.oom")
		.defaultValue(false)
		.withDescription("Generate a heap dump when the jvm oom");

	/**
	 * This options is here only for documentation generation, it is only
	 * evaluated in the shell scripts.
	 */
	@SuppressWarnings("unused")
	public static final ConfigOption<String> FLINK_SSH_OPTIONS = ConfigOptions
		.key("env.ssh.opts")
		.noDefaultValue()
		.withDescription("Additional command line options passed to SSH clients when starting or stopping JobManager," +
			" TaskManager, and Zookeeper services (start-cluster.sh, stop-cluster.sh, start-zookeeper-quorum.sh," +
			" stop-zookeeper-quorum.sh).");

	/**
	 * This options is here only for documentation generation, it is only
	 * evaluated in the shell scripts.
	 */
	@SuppressWarnings("unused")
	public static final ConfigOption<String> FLINK_HADOOP_CONF_DIR = ConfigOptions
		.key("env.hadoop.conf.dir")
		.noDefaultValue()
		.withDescription("Path to hadoop configuration directory. It is required to read HDFS and/or YARN" +
			" configuration. You can also set it via environment variable.");

	/**
	 * This options is here only for documentation generation, it is only
	 * evaluated in the shell scripts.
	 */
	@SuppressWarnings("unused")
	public static final ConfigOption<String> FLINK_YARN_CONF_DIR = ConfigOptions
		.key("env.yarn.conf.dir")
		.noDefaultValue()
		.withDescription("Path to yarn configuration directory. It is required to run flink on YARN. You can also" +
			" set it via environment variable.");

	// ------------------------------------------------------------------------
	//  generic io
	// ------------------------------------------------------------------------

	/**
	 * The config parameter defining the directories for temporary files, separated by
	 * ",", "|", or the system's {@link java.io.File#pathSeparator}.
	 */
	@Documentation.OverrideDefault("'LOCAL_DIRS' on Yarn. '_FLINK_TMP_DIR' on Mesos. System.getProperty(\"java.io.tmpdir\") in standalone.")
	@Documentation.Section(Documentation.Sections.COMMON_MISCELLANEOUS)
	public static final ConfigOption<String> TMP_DIRS =
		key("io.tmp.dirs")
			.defaultValue(System.getProperty("java.io.tmpdir"))
			.withDeprecatedKeys("taskmanager.tmp.dirs")
			.withDescription("Directories for temporary files, separated by\",\", \"|\", or the system's java.io.File.pathSeparator.");

	// ------------------------------------------------------------------------
	//  program
	// ------------------------------------------------------------------------

	public static final ConfigOption<Integer> DEFAULT_PARALLELISM = ConfigOptions
		.key("parallelism.default")
		.defaultValue(1)
		.withDescription("Default parallelism for jobs.");

	public static final ConfigOption<DefaultPartitioner> DEFAULT_STREAM_PARTITIONER = ConfigOptions
		.key("stream-partitioner.default")
		.enumType(DefaultPartitioner.class)
		.defaultValue(DefaultPartitioner.RESCALE)
		.withDescription("Default partitioner for StreamGraph.");

	public static final ConfigOption<Boolean> FILTER_OUTDATED_TIMER = ConfigOptions
		.key("timer.recover.filter-outdated")
		.defaultValue(false)
		.withDescription("After the task is restored, whether it is necessary to trigger the " +
			"timer that may have been triggered.");

	// ------------------------------------------------------------------------
	//  file systems
	// ------------------------------------------------------------------------

	/**
	 * The default filesystem scheme, used for paths that do not declare a scheme explicitly.
	 */
	@Documentation.Section(Documentation.Sections.COMMON_MISCELLANEOUS)
	public static final ConfigOption<String> DEFAULT_FILESYSTEM_SCHEME = ConfigOptions
			.key("fs.default-scheme")
			.noDefaultValue()
			.withDescription("The default filesystem scheme, used for paths that do not declare a scheme explicitly." +
				" May contain an authority, e.g. host:port in case of an HDFS NameNode.");

	@Documentation.Section(Documentation.Sections.COMMON_MISCELLANEOUS)
	public static final ConfigOption<String> ALLOWED_FALLBACK_FILESYSTEMS = ConfigOptions
			.key("fs.allowed-fallback-filesystems")
			.stringType()
			.defaultValue("")
			.withDescription("A (semicolon-separated) list of file schemes, for which Hadoop can be used instead " +
				"of an appropriate Flink plugin. (example: s3;wasb)");

	/**
	 * Specifies whether file output writers should overwrite existing files by default.
	 */
	@Documentation.Section(Documentation.Sections.DEPRECATED_FILE_SINKS)
	public static final ConfigOption<Boolean> FILESYTEM_DEFAULT_OVERRIDE =
		key("fs.overwrite-files")
			.defaultValue(false)
			.withDescription("Specifies whether file output writers should overwrite existing files by default. Set to" +
				" \"true\" to overwrite by default,\"false\" otherwise.");

	/**
	 * Specifies whether the file systems should always create a directory for the output, even with a parallelism of one.
	 */
	@Documentation.Section(Documentation.Sections.DEPRECATED_FILE_SINKS)
	public static final ConfigOption<Boolean> FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY =
		key("fs.output.always-create-directory")
			.defaultValue(false)
			.withDescription("File writers running with a parallelism larger than one create a directory for the output" +
				" file path and put the different result files (one per parallel writer task) into that directory." +
				" If this option is set to \"true\", writers with a parallelism of 1 will also create a" +
				" directory and place a single result file into it. If the option is set to \"false\"," +
				" the writer will directly create the file directly at the output path, without creating a containing" +
				" directory.");

	/**
	 * The total number of input plus output connections that a file system for the given scheme may open.
	 * Unlimited be default.
	 */
	public static ConfigOption<Integer> fileSystemConnectionLimit(String scheme) {
		return ConfigOptions.key("fs." + scheme + ".limit.total").defaultValue(-1);
	}

	/**
	 * The total number of input connections that a file system for the given scheme may open.
	 * Unlimited be default.
	 */
	public static ConfigOption<Integer> fileSystemConnectionLimitIn(String scheme) {
		return ConfigOptions.key("fs." + scheme + ".limit.input").defaultValue(-1);
	}

	/**
	 * The total number of output connections that a file system for the given scheme may open.
	 * Unlimited be default.
	 */
	public static ConfigOption<Integer> fileSystemConnectionLimitOut(String scheme) {
		return ConfigOptions.key("fs." + scheme + ".limit.output").defaultValue(-1);
	}

	/**
	 * If any connection limit is configured, this option can be optionally set to define after
	 * which time (in milliseconds) stream opening fails with a timeout exception, if no stream
	 * connection becomes available. Unlimited timeout be default.
	 */
	public static ConfigOption<Long> fileSystemConnectionLimitTimeout(String scheme) {
		return ConfigOptions.key("fs." + scheme + ".limit.timeout").defaultValue(0L);
	}

	/**
	 * If any connection limit is configured, this option can be optionally set to define after
	 * which time (in milliseconds) inactive streams are reclaimed. This option can help to prevent
	 * that inactive streams make up the full pool of limited connections, and no further connections
	 * can be established. Unlimited timeout be default.
	 */
	public static ConfigOption<Long> fileSystemConnectionLimitStreamInactivityTimeout(String scheme) {
		return ConfigOptions.key("fs." + scheme + ".limit.stream-timeout").defaultValue(0L);
	}

	/**
	 * Preload classes for both JobManager and TaskManager.
	 */
	public static final ConfigOption<String> FLINK_PRE_LOAD_CLASS = ConfigOptions
		.key("flink.preload.class")
		.stringType()
		.noDefaultValue()
		.withDescription("Preload classes for both JobManager and TaskManager, multi classes split by ';'.");

	/**
	 * Preload classes for JobManager.
	 */
	public static final ConfigOption<String> FLINK_JM_PRE_LOAD_CLASS = ConfigOptions
		.key("flink.jobmanager.preload.class")
		.stringType()
		.noDefaultValue()
		.withDescription("Preload classes for JobManager, multi classes split by ';'.");

	/**
	 * Preload classes for TaskManager.
	 */
	public static final ConfigOption<String> FLINK_TM_PRE_LOAD_CLASS = ConfigOptions
		.key("flink.taskmanager.preload.class")
		.stringType()
		.noDefaultValue()
		.withDescription("Preload classes for TaskManager, multi classes split by ';'.");

	public static final ConfigOption<Boolean> USE_ADDRESS_AS_HOSTNAME_ENABLE =
		key("endpoint.use-address-as-hostname.enable")
		.booleanType()
		.defaultValue(false)
		.withDescription("Use taskmanager address as hostname, will not lookup hostname by dns.");

	public static final ConfigOption<Boolean> ENDPOINT_USE_MAIN_SCHEDULED_EXECUTOR_ENABLE =
		key("endpoint.use-main-scheduled-executor.enable")
		.booleanType()
		.defaultValue(false)
		.withDescription("Use main scheduled executor in rpc endpoint to manager the scheduled tasks.");

	/**
	 * There're too many logs about job, disable some detail log when the flag is true.
	 */
	public static final ConfigOption<Boolean> FLINK_JOB_LOG_DETAIL_DISABLE = ConfigOptions
		.key("flink.job.log-detail-disable")
		.booleanType()
		.defaultValue(false)
		.withDescription("Use debug level for some logs of job when the flag is true, otherwise use info level.");

	/**
	 * Task will be switched to running when it is submitted to task executor.
	 */
	public static final ConfigOption<Boolean> FLINK_SUBMIT_RUNNING_NOTIFY = ConfigOptions
		.key("flink.job.submit-to-running.notify")
		.booleanType()
		.defaultValue(false)
		.withDescription("The task will be switch to running when it is submitted to task executor, this will reduce the message between jm and tm.");

	public static final ConfigOption<Boolean> IPV6_ENABLED = ConfigOptions
			.key("ipv6.enabled")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether to enable ipv6 in job manager and task manager.");

	public static final ConfigOption<Boolean> DUMP_CONFIGURATION_BY_YAML =
			key("configuration.flink-conf.dump-configuration-by-yaml")
					.booleanType()
					.defaultValue(false)
					.withDescription("Whether dump flink-conf by yaml, " +
							"If ture, it will use snakeYaml to dump the configuration directly.");

	public static final ConfigOption<Boolean> IS_CLUSTER_CHANGED =
		key("flink.submit-twice.is-cluster-changed")
			.booleanType()
			.defaultValue(false)
			.withDescription("Whether app's cluster is changed,  " +
				"when the app need to be submitted twice.");
}
