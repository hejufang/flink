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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.runtime.io.network.partition.ExternalBlockSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Test {@link ExternalBlockSubpartitionViewScheduler} related implementations.
 */
public class ExternalBlockSubpartitionViewSchedulerTest {

	@Test
	public void testCreditBasedSubpartitionViewScheduler() {
		ResultPartitionID resultPartitionID1 = new ResultPartitionID();
		ResultPartitionID resultPartitionID2 = new ResultPartitionID();
		ResultPartitionID resultPartitionID3 = new ResultPartitionID();
		List<ExternalBlockSubpartitionView> expectedSubpartitionViewSequence = new ArrayList<ExternalBlockSubpartitionView>() {{
			// resultPartitionID2 has 9 credits
			add(mockSubpartitionView(resultPartitionID2, 0, 1));
			add(mockSubpartitionView(resultPartitionID2, 1, 4));
			add(mockSubpartitionView(resultPartitionID2, 2, 3));
			add(mockSubpartitionView(resultPartitionID2, 3, 1));
			// resultPartitionID3 has 8 credits
			add(mockSubpartitionView(resultPartitionID3, 0, 6));
			add(mockSubpartitionView(resultPartitionID3, 1, 2));
			// resultPartitionID1 has 5 credits
			add(mockSubpartitionView(resultPartitionID1, 0, 5));
		}};

		List<ExternalBlockSubpartitionView> subpartitionViews = new ArrayList<>(expectedSubpartitionViewSequence);
		Collections.shuffle(subpartitionViews);

		CreditBasedSubpartitionViewScheduler scheduler = new CreditBasedSubpartitionViewScheduler();
		subpartitionViews.forEach(subpartitionView -> scheduler.addToSchedule(subpartitionView));

		assertEquals(3, scheduler.resultPartitionNodeMap.size());
		assertEquals(0, scheduler.cacheList.size());

		for (int i = 0; i < expectedSubpartitionViewSequence.size(); i++) {
			assertExternalBlockSubpartitionViewEquals(expectedSubpartitionViewSequence.get(i), scheduler.schedule());
			if (i == 0) {
				assertEquals(3, scheduler.resultPartitionNodeMap.size());
				assertEquals(3, scheduler.cacheList.size());
			} else if (i == 4) {
				assertEquals(2, scheduler.resultPartitionNodeMap.size());
				assertEquals(1, scheduler.cacheList.size());
			} else if (i == 6) {
				assertEquals(1, scheduler.resultPartitionNodeMap.size());
				assertEquals(0, scheduler.cacheList.size());
			}
		}
		assertNull(scheduler.schedule());
		assertEquals(0, scheduler.resultPartitionNodeMap.size());
		assertEquals(0, scheduler.cacheList.size());
	}

	@Test
	public void testExternalBlockSubpartitionViewSchedulerDelegate() {
		ResultPartitionID resultPartitionID1 = new ResultPartitionID();
		ResultPartitionID resultPartitionID2 = new ResultPartitionID();
		ResultPartitionID resultPartitionID3 = new ResultPartitionID();
		List<ExternalBlockSubpartitionView> expectedSubpartitionViewSequence = new ArrayList<ExternalBlockSubpartitionView>() {{
			add(mockSubpartitionView(resultPartitionID1, 0, 5));
			add(mockSubpartitionView(resultPartitionID1, 1, 6));
			add(mockSubpartitionView(resultPartitionID1, 3, 2));
			add(mockSubpartitionView(resultPartitionID2, 0, 1));
			add(mockSubpartitionView(resultPartitionID2, 1, 4));
			add(mockSubpartitionView(resultPartitionID2, 2, 3));
			add(mockSubpartitionView(resultPartitionID2, 3, 1));
			add(mockSubpartitionView(resultPartitionID3, 0, 6));
			add(mockSubpartitionView(resultPartitionID3, 1, 2));
			add(mockSubpartitionView(resultPartitionID3, 2, 4));
			add(mockSubpartitionView(resultPartitionID3, 3, 5));
		}};
		Collections.shuffle(expectedSubpartitionViewSequence);
		List<ExternalBlockSubpartitionView> subpartitionViews = new ArrayList<>(expectedSubpartitionViewSequence);

		ExternalBlockSubpartitionViewSchedulerDelegate blockingQueue =
			new ExternalBlockSubpartitionViewSchedulerDelegate(new FifoSubpartitionViewScheduler());

		Random rand = new Random();
		int indexToOffer = -1;
		int indexToPollOrTake = -1;
		while (true) {
			// decide whether to offer or to poll/take
			boolean toOffer = rand.nextBoolean();
			if (toOffer) {
				if (indexToOffer >= (expectedSubpartitionViewSequence.size() - 1)) {
					toOffer = false;
				} else {
					++indexToOffer;
				}
			}
			if (!toOffer) {
				if (indexToPollOrTake >= (expectedSubpartitionViewSequence.size() - 1)) {
					break;
				} else if (indexToPollOrTake >= indexToOffer) {
					continue;
				} else {
					++indexToPollOrTake;
				}
			}

			assertEquals(Integer.MAX_VALUE, blockingQueue.remainingCapacity());

			if (toOffer) {
				assertEquals(indexToOffer - indexToPollOrTake - 1, blockingQueue.size());
				blockingQueue.offer(subpartitionViews.get(indexToOffer));
				assertEquals(indexToOffer - indexToPollOrTake, blockingQueue.size());
			} else {
				assertEquals(indexToOffer - indexToPollOrTake + 1, blockingQueue.size());
				// decide whether to poll or take
				boolean toPoll = rand.nextBoolean();
				ExternalBlockSubpartitionView subpartitionView = null;
				if (toPoll) {
					subpartitionView = (ExternalBlockSubpartitionView) blockingQueue.poll();
				} else {
					try {
						subpartitionView = (ExternalBlockSubpartitionView) blockingQueue.take();
					} catch (InterruptedException e) {
						assertTrue("Unexpected exception " + e, false);
					}
				}
				assertExternalBlockSubpartitionViewEquals(
					expectedSubpartitionViewSequence.get(indexToPollOrTake), subpartitionView,
					"indexToPollOrTake " + indexToPollOrTake + ", indexToOffer " + indexToOffer);
				assertEquals(indexToOffer - indexToPollOrTake, blockingQueue.size());
			}
		}
		try {
			// take 1 second to be timed out
			assertNull(blockingQueue.poll(1, TimeUnit.SECONDS));
		} catch (InterruptedException e) {
			assertTrue("Unexpected exception " + e, false);
		}
		assertEquals(expectedSubpartitionViewSequence.size() - 1, indexToOffer);
		assertEquals(expectedSubpartitionViewSequence.size() - 1, indexToPollOrTake);
		assertEquals(0, blockingQueue.size());
	}

	/**
	 * Since {@link ExternalBlockSubpartitionViewSchedulerDelegate} implements
	 * {@link java.util.concurrent.BlockingQueue} and is used in {@link ThreadPoolExecutor}, it should be able to
	 * deal with concurrent operations.
	 */
	@Test
	public void testExternalBlockSubpartitionViewSchedulerDelegateConcurrently() throws Exception {
		final int producerThreadCount = 5;
		final int consumerThreadCount = 3;
		final int subpartitionViewCountPerProducer = 20;

		List<List<ExternalBlockSubpartitionView>> subpartitionViewsPerProducer = new ArrayList<>(producerThreadCount);
		List<List<ExternalBlockSubpartitionView>> subpartitionViewsPerConsumer = new ArrayList<>(consumerThreadCount);
		ExternalBlockSubpartitionViewSchedulerDelegate blockingQueue =
			new ExternalBlockSubpartitionViewSchedulerDelegate(new FifoSubpartitionViewScheduler());
		AtomicBoolean isRunning = new AtomicBoolean(true);

		ThreadPoolExecutor consumerThreadPool = new ThreadPoolExecutor(
			consumerThreadCount, consumerThreadCount, 0L, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue<>());
		for (int i = 0; i < consumerThreadCount; i++) {
			subpartitionViewsPerConsumer.add(new ArrayList<>());

			final int index = i;
			consumerThreadPool.execute(new Runnable() {
				List<ExternalBlockSubpartitionView> subpartitionViews = subpartitionViewsPerConsumer.get(index);
				@Override
				public void run() {
					Random rand = new Random(index);
					while (true) {
						ExternalBlockSubpartitionView subpartitionView = null;
						// decide whether to poll or to take
						boolean toPoll = rand.nextBoolean();
						if (toPoll) {
							subpartitionView = (ExternalBlockSubpartitionView) blockingQueue.poll();
						} else {
							CompletableFuture<ExternalBlockSubpartitionView> future =
								CompletableFuture.supplyAsync(() -> {
									try {
										return (ExternalBlockSubpartitionView) blockingQueue.take();
									} catch (InterruptedException e) {
										return null;
									}
								});
							try {
								subpartitionView = future.get(3, TimeUnit.SECONDS);
							} catch (Exception e) {
								subpartitionView = null;
							}
						}
						if (subpartitionView != null) {
							subpartitionViews.add(subpartitionView);
						} else if (!isRunning.get()) {
							break;
						} else {
							try {
								Thread.sleep(100);
							} catch (InterruptedException e) {
								break;
							}
						}
					}
				}
			});
		}

		ThreadPoolExecutor producerThreadPool = new ThreadPoolExecutor(
			producerThreadCount, producerThreadCount, 0L, TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue<>());
		for (int i = 0; i < producerThreadCount; i++) {
			subpartitionViewsPerProducer.add(new ArrayList<>());

			final int index = i;
			producerThreadPool.execute(new Runnable() {
				List<ExternalBlockSubpartitionView> subpartitionViews = subpartitionViewsPerProducer.get(index);
				@Override
				public void run() {
					Random rand = new Random(index);
					for (int j = 0; j < subpartitionViewCountPerProducer; j++) {
						ExternalBlockSubpartitionView subpartitionView = mockSubpartitionView(
							new ResultPartitionID(), j, j);
						subpartitionViews.add(subpartitionView);
						blockingQueue.offer(subpartitionView);
						int sleepInterval = rand.nextInt(10);
						try {
							Thread.sleep(sleepInterval * 100);
						} catch (InterruptedException e) {
							break;
						}
					}
				}
			});
		}

		try {
			while (producerThreadPool.getCompletedTaskCount() < producerThreadCount) {
				Thread.sleep(100);
			}
			producerThreadPool.shutdown();
			producerThreadPool.awaitTermination(10, TimeUnit.SECONDS);

			// each producer thread produces a view at most every second
			int timeLeftToWait = 2 * subpartitionViewCountPerProducer * 10;
			boolean allViewsConsumed = false;
			while (true) {
				int totalConsumedViews = 0;
				for (int i = 0; i < subpartitionViewsPerConsumer.size(); i++) {
					totalConsumedViews += subpartitionViewsPerConsumer.get(i).size();
				}
				if (totalConsumedViews >= producerThreadCount * subpartitionViewCountPerProducer) {
					allViewsConsumed = true;
					break;
				} else if (timeLeftToWait-- > 0){
					Thread.sleep(100);
				} else {
					break;
				}
			}
			isRunning.set(false);
			consumerThreadPool.shutdown();
			consumerThreadPool.awaitTermination(10, TimeUnit.SECONDS);

			assertTrue("Not all views are consumed", allViewsConsumed);

			for (int i = 0; i < subpartitionViewsPerProducer.size(); i++) {
				List<ExternalBlockSubpartitionView> subpartitionViews = subpartitionViewsPerProducer.get(i);
				assertEquals(subpartitionViewCountPerProducer, subpartitionViews.size());
				for (ExternalBlockSubpartitionView subpartitionView : subpartitionViews) {
					// search it in subpartitionViewsPerConsumer
					boolean beenConsumed = CompletableFuture.supplyAsync(() -> {
						for (List<ExternalBlockSubpartitionView> consumerSubpartitionViews : subpartitionViewsPerConsumer) {
							for (ExternalBlockSubpartitionView consumerSubpartitionView : consumerSubpartitionViews) {
								if (consumerSubpartitionView.getResultPartitionID().equals(
									subpartitionView.getResultPartitionID())
									&& consumerSubpartitionView.getSubpartitionIndex() ==
									subpartitionView.getSubpartitionIndex()) {
									return true;
								}
							}
						}
						return false;
					}).get();
					assertTrue("SubpartitionView hasn't been consumed", beenConsumed);
				}
			}
		} catch (InterruptedException e) {
			assertTrue("Unexpected exception " + e, false);
		}
	}

	private ExternalBlockSubpartitionView mockSubpartitionView(
		ResultPartitionID resultPartitionID, int subpartitionIndex, int credits) {
		ExternalBlockSubpartitionView subpartitionView = mock(ExternalBlockSubpartitionView.class);
		when(subpartitionView.getResultPartitionID()).thenReturn(resultPartitionID);
		//when(subpartitionView.getResultPartitionDir()).thenReturn(resultPartitionID.toString());
		when(subpartitionView.getSubpartitionIndex()).thenReturn(subpartitionIndex);
		when(subpartitionView.getCreditUnsafe()).thenReturn(credits);
		return subpartitionView;
	}

	private void assertExternalBlockSubpartitionViewEquals(
		ExternalBlockSubpartitionView expectedSubpartitionView, ExternalBlockSubpartitionView actualSubpartitionView) {
		assertExternalBlockSubpartitionViewEquals(expectedSubpartitionView, actualSubpartitionView, "");
	}

	private void assertExternalBlockSubpartitionViewEquals(
		ExternalBlockSubpartitionView expectedSubpartitionView, ExternalBlockSubpartitionView actualSubpartitionView,
		String errMessage) {
		assertNotNull(errMessage, expectedSubpartitionView);
		assertNotNull(errMessage, actualSubpartitionView);
		assertEquals(errMessage, expectedSubpartitionView.getResultPartitionID(),
			actualSubpartitionView.getResultPartitionID());
		assertEquals(errMessage, expectedSubpartitionView.getSubpartitionIndex(),
			actualSubpartitionView.getSubpartitionIndex());
	}
}
