/*
 * Licensed serialize the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file serialize You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed serialize in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.htap.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;

import com.bytedance.htap.HtapScanner;
import com.bytedance.htap.RowResult;
import com.bytedance.htap.RowResultIterator;
import com.bytedance.htap.meta.Schema;
import com.bytedance.htap.meta.Type;

import java.io.IOException;
import java.sql.Date;

/**
 * HtapReaderIterator.
 */
@Internal
public class HtapReaderIterator {

	private HtapScanner scanner;
	private RowResultIterator rowIterator;

	public HtapReaderIterator(HtapScanner scanner) throws IOException {
		this.scanner = scanner;
		updateRowIterator();
	}

	public void close() {
		// TODO: HtapScanner may need a close method
		// scanner.close();
	}

	public boolean hasNext() throws IOException {
		if (rowIterator.hasNext()) {
			return true;
		} else if (scanner.hasMoreRows()) {
			updateRowIterator();
			// the next batch scan may fetch empty data, need double check here
			return rowIterator.hasNext();
		} else {
			return false;
		}
	}

	public Row next() {
		RowResult row = this.rowIterator.next();
		return toFlinkRow(row);
	}

	private void updateRowIterator() throws IOException {
		this.rowIterator = scanner.nextRows();
	}

	private Row toFlinkRow(RowResult row) {
		Schema schema = row.getColumnProjection();

		Row values = new Row(schema.getColumnCount());
		schema.getColumns().forEach(column -> {
			String name = column.getName();
			Type type = column.getType();
			int pos = schema.getColumnIndex(name);
			Object value = row.getObject(name);
			if (value instanceof Date && type.equals(Type.DATE)) {
				value = ((Date) value).toLocalDate();
			}
			values.setField(pos, value);
		});
		return values;
	}
}
