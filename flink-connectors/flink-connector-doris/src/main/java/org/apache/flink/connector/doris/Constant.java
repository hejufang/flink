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

package org.apache.flink.connector.doris;

import com.bytedance.inf.compute.hsap.doris.DataFormat;
import com.bytedance.inf.compute.hsap.doris.TableModel;

/**
 *  Default value and constants.
 */
public class Constant {

	public static final TableModel TABLE_MODEL_DEFAULT = TableModel.AGGREGATE;
	public static final DataFormat DATA_FORMAT_DEFAULT = DataFormat.CSV;
	public static final String COLUMN_SEPARATOR_DEFAULT = "\\x01";
	public static final int MAX_BYTES_PER_BATCH_DEFAULT = 104857600;
	public static final int MAX_PENDING_BATCH_NUM_DEFAULT = 3;
	public static final int MAX_PENDING_TIME_MS_DEFAULT = 300000;
	public static final float MAX_FILTER_RATIO_DEFAULT = 0.0f;
	public static final int RETRY_INTERVAL_MS_DEFAULT = 1000;
	public static final int MAX_RETRY_NUM_DEFAULT = -1;
	public static final int FE_UPDATE_INTERVAL_MS_DEFAULT = 15000;
	public static final int TIMEOUT_MS_DEFAULT = 5 * 60 * 1000;
}
