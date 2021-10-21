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

package org.apache.flink.fs.s3presto;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Simple factory for the S3 file system, registered for the <tt>s3p://</tt> scheme.
 */

public class VolcengineStsCredentialsProvider implements AWSCredentialsProvider {

	public static final Logger LOG = LoggerFactory.getLogger(VolcengineStsCredentialsProvider.class);
	private static final Long TOLERANCE_INTERVAL = 10 * 60 * 1000L; // 10 min
	private BasicSessionCredentials credentialsCache;
	private long expireTime = -1;
	private String secretPath;
	private URI uri;
	private static final String ACCESS_KEY_ID_FILE = "AccessKeyId";
	private static final String SECRET_ACCESS_KEY = "SecretAccessKey";
	private static final String SESSION_TOKEN = "SessionToken";
	private static final String EXPIRED_TIME = "ExpiredTime";

	public VolcengineStsCredentialsProvider(URI uri, Configuration configuration) {
		Preconditions.checkArgument(configuration.get(KubernetesConfigOptions.KUBERNETES_SECRETS.key()) != null,
			KubernetesConfigOptions.KUBERNETES_SECRETS.key() + " cannot be null");
		Preconditions.checkArgument(configuration.get(KubernetesConfigOptions.KUBERNETES_SECRETS.key()).split(":").length == 2,
			KubernetesConfigOptions.KUBERNETES_SECRETS.key() + " is invalid, the value should be in form of 'secretName:/mount/path' ");
		this.secretPath = configuration.get(KubernetesConfigOptions.KUBERNETES_SECRETS.key()).split(":")[1];
		this.uri = uri;
		LOG.info("Create VolcengineStsCredentialsProvider with URI: {}, secretPath: {}", this.uri, this.secretPath);
	}

	@Override
	public AWSCredentials getCredentials() throws FlinkRuntimeException {
		Long now = System.currentTimeMillis();
		if (expireTime > now + TOLERANCE_INTERVAL) {
			return credentialsCache;
		}
		synchronized (this) {
			if (expireTime > now + TOLERANCE_INTERVAL) {
				return credentialsCache;
			}
			LOG.info("Credential expired, refresh it. ExpireTime: {}, Now: {}", expireTime, now);
			String accessKeyID, secretAccessKey, sessionToken;
			Long expiredTime;
			try {
				accessKeyID = loadSecretFileToString(ACCESS_KEY_ID_FILE);
				secretAccessKey = loadSecretFileToString(SECRET_ACCESS_KEY);
				sessionToken = loadSecretFileToString(SESSION_TOKEN);
				expiredTime = formatStsTime(loadSecretFileToString(EXPIRED_TIME));
			} catch (IOException e) {
				LOG.error("Read secret files failed", e);
				throw new FlinkRuntimeException(e);
			}
			this.credentialsCache = new BasicSessionCredentials(accessKeyID,
				secretAccessKey, sessionToken);
			this.expireTime = expiredTime;
			return credentialsCache;
		}
	}

	@Override
	public void refresh() {
		LOG.info("VolcengineStsCredentialsProvider refresh");
	}

	private String loadSecretFileToString(String filename) throws IOException {
		Path filePath = Paths.get(this.secretPath, filename);
		String content = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8).trim();
		return content;
	}

	private static Long formatStsTime(String time) {
		if (time == null) {
			return -1L;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		try {
			return sdf.parse(time).getTime();
		} catch (ParseException e) {
			throw new IllegalStateException(e);
		}
	}
}
