package org.apache.flink.yarn;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.EnvironmentInformation;

import com.bytedance.metrics.UdpMetricsClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.yarn.Utils.require;

/**
 * Flink version reporter which periodically reports Flink version messages.
 */
public class FlinkVersionReporter implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkVersionReporter.class);
	private static final String FLINK_VERSION_METRICS_PREFIX = "inf.flink";
	private static final String FLINK_VERSION_METRICS_NAME = "version";

	private String tags;
	private Configuration flinkConfig;
	private UdpMetricsClient udpMetricsClient;
	private boolean isRunning = true;

	public FlinkVersionReporter(Configuration flinkConfig) {
		this.flinkConfig = flinkConfig;
		init();
	}

	public void init() {
		udpMetricsClient = new UdpMetricsClient(FLINK_VERSION_METRICS_PREFIX);
		String subVersion = this.flinkConfig.getString(ConfigConstants.FLINK_SUBVERSION_KEY, null);
		String flinkJobType = this.flinkConfig.getString(ConfigConstants.FLINK_JOB_TYPE_KEY, null);
		EnvironmentInformation.RevisionInformation rev =
			EnvironmentInformation.getRevisionInformation();
		String commitId = rev.commitId;
		String commitDate = rev.commitDate;
		if (rev.commitDate != null) {
			commitDate = rev.commitDate.replace(" ", "_");
		}
		String appName = System.getenv().get(YarnConfigKeys.ENV_FLINK_YARN_JOB);
		require(appName != null && !appName.isEmpty(), "AppName not set.");
		// we assume that the format of appName is {jobName}_{owner}
		String jobName = appName;
		String owner = null;
		int index = appName.lastIndexOf("_");
		if (index > 0) {
			jobName = appName.substring(0, index);
			if (index + 1 < appName.length()) {
				owner = appName.substring(index + 1);
			}
		}

		String version = EnvironmentInformation.getVersion();
		tags = String.format("version=%s|commitId=%s|commitDate=%s|jobName=%s",
			version, commitId, commitDate, jobName);
		if (flinkJobType != null && !flinkJobType.isEmpty()) {
			tags = tags + "|flinkJobType=" + flinkJobType;
		}
		if (subVersion != null && !subVersion.isEmpty()) {
			tags = tags + "|subVersion=" + subVersion;
		}
		if (owner != null && !owner.isEmpty()) {
			tags = tags + "|owner=" + owner;
		}
	}

	@Override
	public void run() {
		while (isRunning) {
			try {
				LOG.debug("Emit flink version counter.");
				udpMetricsClient.emitStoreWithTag(FLINK_VERSION_METRICS_NAME, 1, tags);
				Thread.sleep(10 * 1000);
			} catch (IOException | InterruptedException e) {
				LOG.warn("Failed to emit flink version counter.", e);
			}
		}
	}

	public void stop() {
		isRunning = false;
	}
}
