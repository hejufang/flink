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

package org.apache.flink.state.table.catalog.resolver;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.time.LocalDate;

/**
 * SavepointLocationResolver.
 */
public class SavepointLocationResolver implements LocationResolver {


	public static final int SAVEPOINT_NUM_SEARCH_DATES_VAL = 7;
	public static final String SAVEPOINT_JOBNAME_PATH_FORMAT = "hdfs:///home/byte_flink_checkpoint_20210220/savepoints/%s/%s";

	private static final String DEFAULT_SEPARATOR = "#";

	@Override
	public Tuple2<String, String> parseDatabase(String database){
		String[] getJobNameAndSavepointId = database.split(DEFAULT_SEPARATOR);
		if (getJobNameAndSavepointId.length != 2) {
			throw new IllegalArgumentException(String.format(
				"Invalid %s format, format must be {JobName}${SavepointID} split by #", database));
		}
		return Tuple2.of(getJobNameAndSavepointId[0], getJobNameAndSavepointId[1]);
	}

	@Override
	public Path getEffectiveSavepointPath(String database) throws IOException {
		return getEffectiveSavepointPath(parseDatabase(database));
	}

	public Path getEffectiveSavepointPath(Tuple2<String, String> jobNameAndSavepointId) throws IOException {
		String jobName = jobNameAndSavepointId.f0;
		String savepointId = jobNameAndSavepointId.f1;
		LocalDate currentDate = LocalDate.now();
		Path effectiveSavepointPath = null;
		for (int i = 0; i < SAVEPOINT_NUM_SEARCH_DATES_VAL; i++) {
			LocalDate date = currentDate.minusDays(i);
			String dateStr = String.format("%04d%02d%02d", date.getYear(), date.getMonthValue(), date.getDayOfMonth());
			Path savepointJobNamePath = new Path(String.format(SAVEPOINT_JOBNAME_PATH_FORMAT, dateStr, jobName));
			effectiveSavepointPath = findEffectiveSavepointPath(savepointJobNamePath, savepointId);
			if (effectiveSavepointPath != null) {
				break;
			}
		}
		return effectiveSavepointPath;
	}

	public Path findEffectiveSavepointPath(Path savepointJobNamePath, String savepointId) throws IOException {
		Path effectiveSavepointPath = null;
		FileSystem fs = savepointJobNamePath.getFileSystem();
		Path potentialEffectiveSavepointPath = new Path(savepointJobNamePath, savepointId);
		// Purely for backward compatibility
		// Old savepoint path: SAVEPOINT_LOCATION_PREFIX/{date}/{jobName}/{UUID}/SAVEPOINT_METADATA
		// New savepoint path: SAVEPOINT_LOCATION_PREFIX/{date}/{jobName}/{namespace}/{UUID}/SAVEPOINT_METADATA
		if (fs.exists(potentialEffectiveSavepointPath)){
			effectiveSavepointPath = potentialEffectiveSavepointPath;
		} else {
			for (FileStatus jobNameSubFileStatus : fs.listStatus(savepointJobNamePath)) {
				Path jobNameSubPath = jobNameSubFileStatus.getPath();
				potentialEffectiveSavepointPath = new Path(jobNameSubPath, savepointId);
				if (fs.exists(potentialEffectiveSavepointPath)) {
					effectiveSavepointPath = potentialEffectiveSavepointPath;
					break;
				}
			}
		}
		return effectiveSavepointPath;
	}
}
