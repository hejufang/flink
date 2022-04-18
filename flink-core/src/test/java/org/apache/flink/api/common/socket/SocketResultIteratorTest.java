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

package org.apache.flink.api.common.socket;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.SerializedThrowable;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.junit.Assert.assertEquals;

/**
 * Test case for socket result iterator.
 */
public class SocketResultIteratorTest {
	/**
	 * Test string result and complete with last result.
	 *
	 * @throws Exception the thrown exception
	 */
	@Test
	public void testUseResultDirectlyComplete() throws Exception {
		final JobID jobId = new JobID();
		final BlockingQueue<JobSocketResult> resultList = new LinkedBlockingQueue<>();
		SocketResultIterator<String> socketResultIterator = new SocketResultIterator<>(jobId, resultList);
		List<String> valueList = Arrays.asList("1", "2", "3", "4", "5", "6");
		final List<String> readValueList = new ArrayList<>();
		Thread thread = new Thread(() -> {
			while (socketResultIterator.hasNext()) {
				readValueList.add(socketResultIterator.next());
			}
		});
		thread.start();

		for (int i = 0; i < valueList.size() - 1; i++) {
			resultList.add(new JobSocketResult.Builder()
				.setJobId(jobId)
				.setResult(valueList.get(i))
				.setResultStatus(ResultStatus.PARTIAL)
				.build());
		}
		resultList.add(new JobSocketResult.Builder()
				.setJobId(jobId)
				.setResult(valueList.get(valueList.size() - 1))
				.setResultStatus(ResultStatus.COMPLETE)
				.build());
		thread.join();

		assertEquals(valueList, readValueList);
	}

	/**
	 * Test string result and complete with null value.
	 *
	 * @throws Exception the thrown exception
	 */
	@Test
	public void testUseResultDirectlyEmptyComplete() throws Exception {
		final JobID jobId = new JobID();
		final BlockingQueue<JobSocketResult> resultList = new LinkedBlockingQueue<>();
		SocketResultIterator<String> socketResultIterator = new SocketResultIterator<>(jobId, resultList);
		List<String> valueList = Arrays.asList("1", "2", "3", "4", "5", "6");
		final List<String> readValueList = new ArrayList<>();
		Thread thread = new Thread(() -> {
			while (socketResultIterator.hasNext()) {
				readValueList.add(socketResultIterator.next());
			}
		});
		thread.start();

		for (String s : valueList) {
			resultList.add(new JobSocketResult.Builder()
				.setJobId(jobId)
				.setResult(s)
				.setResultStatus(ResultStatus.PARTIAL)
				.build());
		}
		resultList.add(new JobSocketResult.Builder()
			.setJobId(jobId)
			.setResult(null)
			.setResultStatus(ResultStatus.COMPLETE)
			.build());
		thread.join();

		assertEquals(valueList, readValueList);

	}

	/**
	 * Test string result and complete with given failed message.
	 */
	@Test
	public void testUseResultDirectlyFailed() {
		final JobID jobId = new JobID();
		final BlockingQueue<JobSocketResult> resultList = new LinkedBlockingQueue<>();
		SocketResultIterator<String> socketResultIterator = new SocketResultIterator<>(jobId, resultList);
		List<String> valueList = Arrays.asList("1", "2", "3", "4", "5", "6");

		for (String s : valueList) {
			resultList.add(new JobSocketResult.Builder()
				.setJobId(jobId)
				.setResult(s)
				.setResultStatus(ResultStatus.PARTIAL)
				.build());
		}
		resultList.add(new JobSocketResult.Builder()
			.setJobId(jobId)
			.setResult(null)
			.setResultStatus(ResultStatus.FAIL)
			.setSerializedThrowable(new SerializedThrowable(new RuntimeException("Failed with null value")))
			.build());
		assertThrows("Failed with null value", RuntimeException.class, () -> {
			final List<String> readValueList = new ArrayList<>();
			while (socketResultIterator.hasNext()) {
				readValueList.add(socketResultIterator.next());
			}
			return readValueList;
		});
	}

	/**
	 * Test string list result with serializer and complete with last result.
	 */
	@Test
	public void testUseListResultComplete() {
		TypeSerializer<String> serializer = new StringSerializer();
		ListSerializer<String> listSerializer = new ListSerializer<>(serializer);
		final JobID jobId = new JobID();
		final BlockingQueue<JobSocketResult> resultList = new LinkedBlockingQueue<>();
		SocketResultIterator<String> socketResultIterator = new SocketResultIterator<>(jobId, resultList);
		socketResultIterator.registerResultSerializer(serializer);
		List<String> list1 = Arrays.asList("11", "12", "13", "14");
		List<String> list2 = Arrays.asList("21", "22", "23");
		List<String> list3 = Arrays.asList("31", "32", "33", "34");
		List<String> list4 = Arrays.asList("41", "42", "43");

		addListValue(jobId, list1, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list2, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list3, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list4, ResultStatus.COMPLETE, listSerializer, resultList);

		List<String> valueList = new ArrayList<>();
		valueList.addAll(list1);
		valueList.addAll(list2);
		valueList.addAll(list3);
		valueList.addAll(list4);

		List<String> readValueList = new ArrayList<>();
		while (socketResultIterator.hasNext()) {
			readValueList.add(socketResultIterator.next());
		}

		assertEquals(valueList, readValueList);
	}

	private void addListValue(
			JobID jobId,
			List<String> valueList,
			ResultStatus resultStatus,
			ListSerializer<String> listSerializer,
			BlockingQueue<JobSocketResult> resultList) {
		if (valueList == null) {
			resultList.add(new JobSocketResult.Builder()
				.setJobId(jobId)
				.setResult(null)
				.setResultStatus(resultStatus)
				.build());
			return;
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputView dataOutputView = new DataOutputViewStreamWrapper(baos);
		try {
			listSerializer.serialize(valueList, dataOutputView);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		resultList.add(new JobSocketResult.Builder()
			.setJobId(jobId)
			.setResult(baos.toByteArray())
			.setResultStatus(resultStatus)
			.build());
	}

	/**
	 * Test string list result with serializer and complete with null.
	 */
	@Test
	public void testUseListResultNullComplete() {
		TypeSerializer<String> serializer = new StringSerializer();
		ListSerializer<String> listSerializer = new ListSerializer<>(serializer);
		final JobID jobId = new JobID();
		final BlockingQueue<JobSocketResult> resultList = new LinkedBlockingQueue<>();
		SocketResultIterator<String> socketResultIterator = new SocketResultIterator<>(jobId, resultList);
		socketResultIterator.registerResultSerializer(serializer);
		List<String> list1 = Arrays.asList("11", "12", "13", "14");
		List<String> list2 = Arrays.asList("21", "22", "23");
		List<String> list3 = Arrays.asList("31", "32", "33", "34");
		List<String> list4 = Arrays.asList("41", "42", "43");

		addListValue(jobId, list1, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list2, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, null, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list3, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, new ArrayList<>(), ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list4, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, null, ResultStatus.COMPLETE, listSerializer, resultList);

		List<String> valueList = new ArrayList<>();
		valueList.addAll(list1);
		valueList.addAll(list2);
		valueList.addAll(list3);
		valueList.addAll(list4);

		List<String> readValueList = new ArrayList<>();
		while (socketResultIterator.hasNext()) {
			readValueList.add(socketResultIterator.next());
		}

		assertEquals(valueList, readValueList);
	}

	/**
	 * Test string list result with serializer and complete with empty list.
	 */
	@Test
	public void testUseListResultEmptyComplete() {
		TypeSerializer<String> serializer = new StringSerializer();
		ListSerializer<String> listSerializer = new ListSerializer<>(serializer);
		final JobID jobId = new JobID();
		final BlockingQueue<JobSocketResult> resultList = new LinkedBlockingQueue<>();
		SocketResultIterator<String> socketResultIterator = new SocketResultIterator<>(jobId, resultList);
		socketResultIterator.registerResultSerializer(serializer);
		List<String> list1 = Arrays.asList("11", "12", "13", "14");
		List<String> list2 = Arrays.asList("21", "22", "23");
		List<String> list3 = Arrays.asList("31", "32", "33", "34");
		List<String> list4 = Arrays.asList("41", "42", "43");

		addListValue(jobId, list1, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list2, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, null, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list3, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, new ArrayList<>(), ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list4, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, new ArrayList<>(), ResultStatus.COMPLETE, listSerializer, resultList);

		List<String> valueList = new ArrayList<>();
		valueList.addAll(list1);
		valueList.addAll(list2);
		valueList.addAll(list3);
		valueList.addAll(list4);

		List<String> readValueList = new ArrayList<>();
		while (socketResultIterator.hasNext()) {
			readValueList.add(socketResultIterator.next());
		}

		assertEquals(valueList, readValueList);
	}

	/**
	 * Test string list result with serializer and complete with failed message.
	 */
	@Test
	public void testUseListResultFailed() {
		TypeSerializer<String> serializer = new StringSerializer();
		ListSerializer<String> listSerializer = new ListSerializer<>(serializer);
		final JobID jobId = new JobID();
		final BlockingQueue<JobSocketResult> resultList = new LinkedBlockingQueue<>();
		SocketResultIterator<String> socketResultIterator = new SocketResultIterator<>(jobId, resultList);
		socketResultIterator.registerResultSerializer(serializer);
		List<String> list1 = Arrays.asList("11", "12", "13", "14");
		List<String> list2 = Arrays.asList("21", "22", "23");
		List<String> list3 = Arrays.asList("31", "32", "33", "34");
		List<String> list4 = Arrays.asList("41", "42", "43");

		addListValue(jobId, list1, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list2, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, null, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list3, ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, new ArrayList<>(), ResultStatus.PARTIAL, listSerializer, resultList);
		addListValue(jobId, list4, ResultStatus.PARTIAL, listSerializer, resultList);
		resultList.add(new JobSocketResult.Builder()
			.setJobId(jobId)
			.setSerializedThrowable(new SerializedThrowable(new RuntimeException("List result failed")))
			.setResultStatus(ResultStatus.FAIL)
			.build());

		assertThrows("List result failed", RuntimeException.class, () -> {
			List<String> readValueList = new ArrayList<>();
			while (socketResultIterator.hasNext()) {
				readValueList.add(socketResultIterator.next());
			}
			return readValueList;
		});
	}
}
