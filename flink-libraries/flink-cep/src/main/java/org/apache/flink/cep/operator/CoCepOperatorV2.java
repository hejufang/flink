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

package org.apache.flink.cep.operator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.cep.functions.MultiplePatternProcessFunctionV2;
import org.apache.flink.cep.functions.MultiplePatternTimedOutPartialMatchHandlerV2;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.timestamps.CepTimestampExtractor;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.KeyedCepEvent;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.PatternProcessor;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.cep.utils.CEPUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import com.codahale.metrics.SlidingWindowReservoir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * The CepOperator which can cooperate with PatternProcessor data stream.
 */
public class CoCepOperatorV2<IN>
	extends AbstractStreamOperator<Object>
	implements TwoInputStreamOperator<KeyedCepEvent<IN>, PatternProcessor<IN, ?>, Object>, Triggerable<Object, VoidNamespace> {

	private static final long serialVersionUID = -1243854353417L;

	private static final Logger LOG = LoggerFactory.getLogger(CoCepOperatorV2.class);

	private static final byte[] STATE_CLEANER_TIMER_PAYLOAD = "stateCleanerTimerPayload".getBytes();
	private static final byte[] TIME_ADVANCER_TIMER_PAYLOAD = "timeAdvancerTimerPayload".getBytes();

	private final boolean isProcessingTime;

	private final TypeSerializer<IN> eventSerializer;
	private final TypeSerializer<KeyedCepEvent<IN>> keyedCepEventSerializer;

	///////////////			State			//////////////
	private static final String PATTERN_STATE_NAME = "patternStateName";
	private static final String NFA_STATE_NAME = "nfaStateName";
	private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";
	private static final String KEYED_WATERMARK_STATE_NAME = "keyedWatermarkStateName";
	private static final String STATE_CLEANER_TIMER_LAST_UPDATE_TIME_STATE_NAME = "stateCleanerTimerLastUpdateTimeStateName";

	private transient MapState<Long, List<KeyedCepEvent<IN>>> elementQueueState;
	private transient InternalValueState<Object, String, NFAState> computationStates;
	private transient Map<String, SharedBuffer<IN>> partialMatches;
	/**
	 *  Each key has its own watermark in state.
	 *  The scope of keyedWatermark in only inside CepOperator.
	 */
	private transient ValueState<Long> keyedWatermark;

	private transient ValueState<Long> stateCleanerTimerLastUpdateTime;

	private transient InternalTimerService<VoidNamespace> timerService;

	private Map<String, NFA<IN>> usingNFAs;

	private BroadcastState<String, PatternProcessor> patternStates;

	/**
	 * The last seen watermark. This will be used to
	 * decide if an incoming element is late or not.
	 */
	private long lastWatermark;

	/**
	 * {@link OutputTag} to use for late arriving events. Elements with timestamp smaller than
	 * the current watermark will be emitted to this.
	 */
	private final OutputTag<IN> lateDataOutputTag;

	/** Strategy which element to skip after a match was found. */
	private final AfterMatchSkipStrategy afterMatchSkipStrategy;

	/** Context passed to user function. */
	private transient CoCepOperatorV2.ContextFunctionImpl context;

	/** Main output collector, that sets a proper timestamp to the StreamRecord. */
	private transient TimestampedCollector<Object> collector;

	/** Wrapped RuntimeContext that limits the underlying context features. */
	private transient CepRuntimeContext cepRuntimeContext;

	/** Thin context passed to NFA that gives access to time related characteristics. */
	private transient TimerService cepTimerService;

	private Map<String, String> properties;

	private final long ttlMilliSeconds;

	private final CepTimestampExtractor<IN> timestampExtractor;

	private ValueStateDescriptor<NFAState> nfaStateValueStateDescriptor;

	// ------------------------------------------------------------------------
	// Metrics
	// ------------------------------------------------------------------------

	private transient Counter numLateRecordsDropped;
	private transient Counter numLateRecordsOutput;
	private transient Counter numPatternsAdded;
	private transient Counter numPatternsDropped;
	private transient Counter numPatternsUsed;
	private transient Counter numStateCleanerTriggered;
	private transient Counter numMatchedSequences;
	private transient Counter numUnMatchedSequences;
	private transient Counter numTimeOutSequences;

	private transient Histogram advanceTimeMs;
	private transient Histogram processEventMs;
	private transient Histogram processMatchedMs;
	private transient Histogram processUnMatchMs;
	private transient Histogram processTimeoutMs;

	private transient Gauge<Long> watermarkLatency;

	public CoCepOperatorV2(
		TypeSerializer<IN> eventSerializer,
		TypeSerializer<KeyedCepEvent<IN>> keyedCepEventSerializer,
		boolean isProcessingTime,
		@Nullable AfterMatchSkipStrategy afterMatchSkipStrategy,
		@Nullable OutputTag<IN> lateDataOutputTag,
		@Nullable CepTimestampExtractor<IN> timestampExtractor,
		Map<String, String> properties) {
		this.eventSerializer = Preconditions.checkNotNull(eventSerializer);
		this.keyedCepEventSerializer = Preconditions.checkNotNull(keyedCepEventSerializer);

		this.isProcessingTime = isProcessingTime;
		this.lateDataOutputTag = lateDataOutputTag;
		this.timestampExtractor = timestampExtractor;

		this.usingNFAs = new HashMap<>();
		this.properties = properties;

		this.ttlMilliSeconds = Long.parseLong(properties.getOrDefault(CEPUtils.TTL_KEY, CEPUtils.TTL_DEFAULT_VALUE));
		LOG.info("Properties({}={}).", CEPUtils.TTL_KEY, ttlMilliSeconds);

		if (afterMatchSkipStrategy == null) {
			this.afterMatchSkipStrategy = AfterMatchSkipStrategy.noSkip();
		} else {
			this.afterMatchSkipStrategy = afterMatchSkipStrategy;
		}
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Object>> output) {
		super.setup(containingTask, config, output);

		this.cepRuntimeContext = new CepRuntimeContext(getRuntimeContext());
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		// initializeState through the provided context
		nfaStateValueStateDescriptor = new ValueStateDescriptor<>(NFA_STATE_NAME, new NFAStateSerializer());
		computationStates = (InternalValueState<Object, String, NFAState>) getOrCreateKeyedState(StringSerializer.INSTANCE, nfaStateValueStateDescriptor);

		patternStates = context.getOperatorStateStore().getBroadcastState(
			new MapStateDescriptor<>(PATTERN_STATE_NAME, new StringSerializer(), new KryoSerializer<>(PatternProcessor.class, new ExecutionConfig())));
		partialMatches = new HashMap<>();

		MapStateDescriptor<Long, List<KeyedCepEvent<IN>>> elementQueueStateDesc = new MapStateDescriptor<>(
			EVENT_QUEUE_STATE_NAME,
			LongSerializer.INSTANCE,
			new ListSerializer<>(keyedCepEventSerializer)
		);
		elementQueueState = context.getKeyedStateStore().getMapState(elementQueueStateDesc);

		keyedWatermark = context.getKeyedStateStore().getState(
			new ValueStateDescriptor<>(
				KEYED_WATERMARK_STATE_NAME,
				LongSerializer.INSTANCE
			));

		stateCleanerTimerLastUpdateTime = context.getKeyedStateStore().getState(
			new ValueStateDescriptor<>(
				STATE_CLEANER_TIMER_LAST_UPDATE_TIME_STATE_NAME,
				LongSerializer.INSTANCE
			));
	}

	@Override
	public void open() throws Exception {
		super.open();
		timerService = getInternalTimerService(
			"watermark-callbacks",
			VoidNamespaceSerializer.INSTANCE,
			this);

		context = new CoCepOperatorV2.ContextFunctionImpl();
		collector = new TimestampedCollector<>(output);
		cepTimerService = new CoCepOperatorV2.TimerServiceImpl();

		registerMetrics(metrics);

		Iterator<Map.Entry<String, PatternProcessor>> iter = patternStates.iterator();
		while (iter.hasNext()) {
			PatternProcessor patternProcessor = iter.next().getValue();
			if (patternProcessor != null) {
				LOG.info("Recover a new patternProcessor from state(id={},version={})", patternProcessor.getId(), patternProcessor.getVersion());
				initializeNewPattern(patternProcessor);
			}
		}
	}

	@Override
	public void processElement2(StreamRecord<PatternProcessor<IN, ?>> element) throws Exception {
		PatternProcessor<IN, ?> patternProcessor = element.getValue();
		checkPatternId(patternProcessor);

		final String patternId = patternProcessor.getId();
		numPatternsAdded.inc();

		if (!patternProcessor.getIsAlive()) {
			// disable this pattern
			numPatternsDropped.inc();
			numPatternsUsed.dec();
			disableOldPattern(patternId);
			return;
		}

		if (this.patternStates.contains(patternId) && this.patternStates.get(patternId).getVersion() == patternProcessor.getVersion()) {
			// do nothing if there is an exactly same pattern
			return;
		}

		// update currentNFA
		if (this.usingNFAs.containsKey(patternId)) {
			this.usingNFAs.get(patternId).close();
			disableOldPattern(patternId);
		}
		LOG.info("Initialize a new pattern from upstream(id={},version={})", patternProcessor.getId(), patternProcessor.getVersion());
		initializeNewPattern(patternProcessor);
	}

	private void checkPatternId(PatternProcessor<IN, ?> patternProcessor) {
		Pattern<IN, ?> pattern = patternProcessor.getPattern();
		// If user do not set patternId(default 'unknown') of Pattern, then set it using id of PatternProcessor.
		if ("unknown".equals(pattern.getPatternId())) {
			pattern.setPatternMeta(patternProcessor.getId(), patternProcessor.getId().hashCode());
		}
	}

	private void disableOldPattern(String patternId) throws Exception {
		clearStateForPattern(patternId);
		this.usingNFAs.remove(patternId);
		this.patternStates.remove(patternId);
		this.partialMatches.remove(patternId);
	}

	private void initializeNewPattern(PatternProcessor<IN, ?> patternProcessor) throws Exception {
		String patternId = patternProcessor.getId();
		final NFA<IN> nfa = compileNFA(patternProcessor);
		nfa.open(cepRuntimeContext, new Configuration());
		LOG.info("Using new NFA \n{}", nfa.format());
		this.usingNFAs.put(patternId, nfa);
		this.patternStates.put(patternId, patternProcessor);
		if (!this.partialMatches.containsKey(patternId)) {
			this.partialMatches.put(patternId, new SharedBuffer(new PerPatternKeyedStateStore(patternId, getKeyedStateBackend(), getExecutionConfig()), eventSerializer));
		} else {
			this.partialMatches.get(patternId).getAccessor().clearMemoryCache();
		}
		numPatternsUsed.inc();
		patternProcessor.getPatternProcessFunction().processNewPattern(patternProcessor);
		patternProcessor.getPatternProcessFunction().open(new Configuration());
	}

	private NFA<IN> compileNFA(PatternProcessor<IN, ?> patternProcessor) {
		final boolean timeoutHandling = patternProcessor.getPatternProcessFunction() instanceof MultiplePatternTimedOutPartialMatchHandlerV2;
		Pattern<IN, ?> pattern = patternProcessor.getPattern();
		final NFACompiler.NFAFactory<IN> nfaFactory = NFACompiler.compileFactory(pattern, timeoutHandling, pattern.isAllowSinglePartialMatchPerKey());
		return nfaFactory.createNFA();
	}

	@Override
	public void close() throws Exception {
		super.close();
		for (Map.Entry<String, NFA<IN>> nfa: usingNFAs.entrySet()) {
			if (nfa.getValue() != null) {
				nfa.getValue().close();
				patternStates.get(nfa.getKey()).getPatternProcessFunction().close();
			}
		}
	}

	@Override
	public void processElement1(StreamRecord<KeyedCepEvent<IN>> element) throws Exception {
		if (this.usingNFAs.isEmpty()) {
//			LOG.warn("Current pattern is not defined, drop records...");
			return;
		}

		updateStateCleanerTimer();

		KeyedCepEvent<IN> keyedCepEvent = element.getValue();
		IN event = keyedCepEvent.getEvent();

		if (isProcessingTime) {
			if (timestampExtractor != null){
				long eventTime = timestampExtractor.extractTimestamp(event);
				Long lastkeyedWatermark = keyedWatermark.value();
				Long newKeyedWatermark = timestampExtractor.getCurrentWatermark(eventTime, lastkeyedWatermark);

				if (eventTime > newKeyedWatermark) {
					bufferEvent(keyedCepEvent, eventTime);
					triggerComputeWithWatermark(getSortedTimestamps(), newKeyedWatermark);
				} else if (lateDataOutputTag != null) {
					numLateRecordsOutput.inc();
					output.collect(lateDataOutputTag, new StreamRecord<>(event));
				} else {
					numLateRecordsOutput.inc();
				}
				keyedWatermark.update(newKeyedWatermark);
			} else {
				// there can be no out of order elements in processing time
				// iterate all patterns

				Iterator<Map.Entry<String, PatternProcessor>> iter = this.patternStates.iterator();
				while (iter.hasNext()) {
					Map.Entry<String, PatternProcessor> entry = iter.next();
					NFAState nfaState = getNFAState(entry.getKey());
					long timestamp = getProcessingTimeService().getCurrentProcessingTime();
					advanceTime(nfaState, timestamp);
					if (keyedCepEvent.getPatternProcessorIds().contains(entry.getKey())) {
						processEvent(nfaState, keyedCepEvent.getEvent(), timestamp);
					}
					updateNFA(nfaState);
				}
			}
		} else {

			long timestamp = element.getTimestamp();

			// In event-time processing we assume correctness of the watermark.
			// Events with timestamp smaller than or equal with the last seen watermark are considered late.
			// Late events are put in a dedicated side output, if the user has specified one.

			if (timestamp > lastWatermark) {

				// we have an event with a valid timestamp, so
				// we buffer it until we receive the proper watermark.
				saveRegisterWatermarkTimer(timestamp);
				bufferEvent(keyedCepEvent, timestamp);
			} else if (lateDataOutputTag != null) {
				output.collect(lateDataOutputTag, new StreamRecord<IN>(event));
			} else {
				numLateRecordsDropped.inc();
			}
		}
	}

	private void updateStateCleanerTimer() throws IOException {

		Long lastUpdateTime = stateCleanerTimerLastUpdateTime.value();
		Long newUpdateTime = timerService.currentProcessingTime();
		if (lastUpdateTime != null){
			timerService.deleteProcessingTimeTimer(VoidNamespace.INSTANCE, lastUpdateTime + ttlMilliSeconds, STATE_CLEANER_TIMER_PAYLOAD);
		}
		timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, newUpdateTime + ttlMilliSeconds, STATE_CLEANER_TIMER_PAYLOAD);
		stateCleanerTimerLastUpdateTime.update(newUpdateTime);
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		processWatermark(mark);
	}

	@Override
	public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
		if (timer.getPayload() != null && Arrays.equals(timer.getPayload(), TIME_ADVANCER_TIMER_PAYLOAD)){
			processTimeAdvancerTimer(timer.getTimestamp());
			return;
		}

		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();

		triggerComputeWithWatermark(sortedTimestamps, timerService.currentWatermark());

		if (!sortedTimestamps.isEmpty()) {
			saveRegisterWatermarkTimer(sortedTimestamps.peek());
		}

		updateLastSeenWatermark(timerService.currentWatermark());
	}

	@Override
	public void onProcessingTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {
		byte[] timerPayload = timer.getPayload();
		if (timerPayload != null && Arrays.equals(timerPayload, TIME_ADVANCER_TIMER_PAYLOAD)){
			processTimeAdvancerTimer(timer.getTimestamp());
			return;
		}
		triggerComputeWithWatermark(getSortedTimestamps(), Long.MAX_VALUE);
		if (timerPayload != null && Arrays.equals(timerPayload, STATE_CLEANER_TIMER_PAYLOAD)){
			clearStateForKey();
		}
	}

	private void processTimeAdvancerTimer(long timestamp) throws Exception {

		Iterator<Map.Entry<String, PatternProcessor>> iter = this.patternStates.iterator();
		while (iter.hasNext()) {
			NFAState nfaState = getNFAState(iter.next().getKey());
			//if using processingTime , No need to advanceTime
			advanceTime(nfaState, timestamp);
			updateNFA(nfaState);
		}
	}

	private void updateLastSeenWatermark(long timestamp) {
		this.lastWatermark = timestamp;
	}

	private PriorityQueue<Long> getSortedTimestamps() throws Exception {
		PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
		for (Long timestamp : elementQueueState.keys()) {
			sortedTimestamps.offer(timestamp);
		}
		return sortedTimestamps;
	}

	private void bufferEvent(KeyedCepEvent<IN> keyedCepEvent, long currentTime) throws Exception {
		List<KeyedCepEvent<IN>> elementsForTimestamp = elementQueueState.get(currentTime);
		if (elementsForTimestamp == null) {
			elementsForTimestamp = new ArrayList<>();
		}

		if (getExecutionConfig().isObjectReuseEnabled()) {
			// copy the StreamRecord so that it cannot be changed
			elementsForTimestamp.add(keyedCepEventSerializer.copy(keyedCepEvent));
		} else {
			elementsForTimestamp.add(keyedCepEvent);
		}
		elementQueueState.put(currentTime, elementsForTimestamp);
	}

	/**
	 * trigger compute with given watermark , Elements with a timestamp less than watermark
	 * in elementQueueState will be fed into NFA.
	 * if using processingTime ,we can pass Long.MAX_VALUE to compute all the element.
	 *
	 */
	private void triggerComputeWithWatermark(PriorityQueue<Long> sortedTimestamps, Long watermark) throws Exception {

		HashMap<String, NFAState> tmpComputationStates = new HashMap<>();

		Iterator<Map.Entry<String, PatternProcessor>> iter = this.patternStates.iterator();
		while (iter.hasNext()){
			Map.Entry<String, PatternProcessor> entry = iter.next();
			String patternId = entry.getKey();
			NFAState nfaState = getNFAState(patternId);
			tmpComputationStates.put(patternId, nfaState);
		}

		while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= watermark) {
			long timestamp = sortedTimestamps.poll();
			final List<KeyedCepEvent<IN>> elements = elementQueueState.get(timestamp);
			for (KeyedCepEvent<IN> keyedCepEvent: elements) {
				IN event = keyedCepEvent.getEvent();

				Set<String> patternIds = keyedCepEvent.getPatternProcessorIds();
				for (String patternId: patternIds) {
					if (tmpComputationStates.containsKey(patternId)) {
						NFAState nfaState = tmpComputationStates.get(patternId);

						advanceTime(nfaState, timestamp);

						try {
							processEvent(nfaState, event, timestamp);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				}
			}
			elementQueueState.remove(timestamp);
		}

		Iterator<Map.Entry<String, NFAState>> newNFAIterator = tmpComputationStates.entrySet().iterator();
		while (newNFAIterator.hasNext()) {
			NFAState nfaState = newNFAIterator.next().getValue();
			//if using processingTime , No need to advanceTime
			if (watermark != Long.MAX_VALUE) {
				advanceTime(nfaState, watermark);
			}
			updateNFA(nfaState);
		}
	}

	/**
	 * Registers a timer for {@code current watermark + 1}, this means that we get triggered
	 * whenever the watermark advances, which is what we want for working off the queue of
	 * buffered elements.
	 */
	private void saveRegisterWatermarkTimer(long timestamp) {
		long currentWatermark = timerService.currentWatermark();
		// protect against overflow
		if (currentWatermark + 1 > currentWatermark) {
			timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
		}
	}

	/**
	 * Process the given event by giving it to the NFA and outputting the produced set of matched
	 * event sequences.
	 *
	 * @param nfaState Our NFAState object
	 * @param event The current event to be processed
	 * @param timestamp The timestamp of the event
	 */
	private void processEvent(NFAState nfaState, IN event, long timestamp) throws Exception {
		long processEventStartTime = System.currentTimeMillis();
		try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.get(nfaState.getPatternId()).getAccessor()) {
			sharedBufferAccessor.setCurrentNamespace(nfaState.getPatternId());
			NFA nfa = usingNFAs.get(nfaState.getPatternId());
			Collection<Map<String, List<IN>>> patterns =
				nfa.process(sharedBufferAccessor, nfaState, event, timestamp, afterMatchSkipStrategy, cepTimerService);
			if (nfa.getWindowTime() > 0 && nfaState.isNewStartPartiailMatch()){
				registerTimeAdvancerTimer(timestamp, nfa.getWindowTime());
			}
			nfaState.resetNewStartPartiailMatch();
			processMatchedSequences(nfaState.getPatternId(), patterns, timestamp);
			if (!nfaState.isStateChanged()) {
				this.numUnMatchedSequences.inc();
				long processUnmatchStartTime = System.currentTimeMillis();
				getPatternProcessFun(nfaState.getPatternId()).processUnMatch(event, context, getCurrentKey(), collector);
				long processUnmatchEndTime = System.currentTimeMillis();
				processUnMatchMs.update(processUnmatchEndTime - processUnmatchStartTime);
			}
		}
		long processEventEndTime = System.currentTimeMillis();
		processEventMs.update(processEventEndTime - processEventStartTime);
	}

	private void registerTimeAdvancerTimer(long timestamp, long windowTime) {

		if (isProcessingTime){
			timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, timestamp + windowTime + 1L, TIME_ADVANCER_TIMER_PAYLOAD);
		} else {
			timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp + windowTime + 1L, TIME_ADVANCER_TIMER_PAYLOAD);
		}
	}

	/**
	 * Advances the time for the given NFA to the given timestamp. This means that no more events with timestamp
	 * <b>lower</b> than the given timestamp should be passed to the nfa, This can lead to pruning and timeouts.
	 */
	private void advanceTime(NFAState nfaState, long timestamp) throws Exception {
		long advanceTimeStartMillis = System.currentTimeMillis();
		try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.get(nfaState.getPatternId()).getAccessor()) {
			// output pending states matches
			sharedBufferAccessor.setCurrentNamespace(nfaState.getPatternId());
			Collection<Map<String, List<IN>>> pendingMatches = usingNFAs.get(nfaState.getPatternId()).pendingStateMatches(sharedBufferAccessor, nfaState, timestamp);
			if (!pendingMatches.isEmpty()) {
				processMatchedSequences(nfaState.getPatternId(), pendingMatches, timestamp);
			}

			// output timeout patterns
			Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut =
				usingNFAs.get(nfaState.getPatternId()).advanceTime(sharedBufferAccessor, nfaState, timestamp);
			if (!timedOut.isEmpty()) {
				this.numTimeOutSequences.inc();
				processTimedOutSequences(nfaState.getPatternId(), timedOut);
			}
		}
		long advanceTimeEndMillis = System.currentTimeMillis();
		advanceTimeMs.update(advanceTimeEndMillis - advanceTimeStartMillis);
	}

	private void processMatchedSequences(String patternId, Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {
		long processMatchedSequencesStartTime = System.currentTimeMillis();
		MultiplePatternProcessFunctionV2 function = getPatternProcessFun(patternId);
		setContext(timestamp, patternStates.get(patternId));
		for (Map<String, List<IN>> matchingSequence : matchingSequences) {
			usingNFAs.get(patternId).clearStateWhenOutput();
			this.numMatchedSequences.inc();
			function.processMatch(Tuple2.of(patternId, matchingSequence), context, getCurrentKey(), collector);
		}
		long processMatchedSequencesEndTime = System.currentTimeMillis();
		processMatchedMs.update(processMatchedSequencesEndTime - processMatchedSequencesStartTime);
	}

	private void processTimedOutSequences(String patternId, Collection<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences) throws Exception {
		long processTimeOutStartTime = System.currentTimeMillis();
		MultiplePatternProcessFunctionV2 function = getPatternProcessFun(patternId);
		if (function instanceof MultiplePatternTimedOutPartialMatchHandlerV2) {

			@SuppressWarnings("unchecked")
			MultiplePatternTimedOutPartialMatchHandlerV2<IN> timeoutHandler = (MultiplePatternTimedOutPartialMatchHandlerV2<IN>) function;

			for (Tuple2<Map<String, List<IN>>, Long> matchingSequence : timedOutSequences) {
				setContext(matchingSequence.f1, patternStates.get(patternId));
				usingNFAs.get(patternId).clearStateWhenOutput();
				timeoutHandler.processTimedOutMatch(Tuple2.of(patternId, matchingSequence.f0), getCurrentKey(), context);
			}
		}
		long processTimeOutEndTime = System.currentTimeMillis();
		processTimeoutMs.update(processTimeOutEndTime - processTimeOutStartTime);
	}

	private void setContext(long timestamp, PatternProcessor patternProcessor) {
		if (!isProcessingTime) {
			collector.setAbsoluteTimestamp(timestamp);
		}

		context.setTimestamp(timestamp);
		context.setCurrentPattern(patternProcessor.getPattern());
	}

	private NFAState getNFAState(String patternId) throws Exception {
		computationStates.setCurrentNamespace(patternId);
		NFAState nfaState = computationStates.value();
		if (nfaState != null){
			return nfaState;
		} else {
			return usingNFAs.get(patternId).createInitialNFAState();
		}
	}

	private void updateNFA(NFAState nfaState) throws Exception {
		computationStates.setCurrentNamespace(nfaState.getPatternId());
		if (nfaState.isStateChanged()) {
			nfaState.resetStateChanged();
			computationStates.update(nfaState);
		}
	}

	private void clearStateForKey() {
		numStateCleanerTriggered.inc();
		if (timestampExtractor != null){
			keyedWatermark.clear();
		}
		stateCleanerTimerLastUpdateTime.clear();
		elementQueueState.clear();
		partialMatches.forEach((patternId, sharedBuffer) -> {
			computationStates.setCurrentNamespace(patternId);
			computationStates.clear();
			sharedBuffer.getAccessor().setCurrentNamespace(patternId);
			sharedBuffer.getAccessor().clearKeyedState();
		});
	}

	private void clearStateForPattern(String patternId) throws Exception {
		LOG.info("clear State for patternId {}", patternId);
		// clear computationStates
		getKeyedStateBackend().applyToAllKeys(patternId, StringSerializer.INSTANCE, nfaStateValueStateDescriptor, (key, state) -> {
			state.clear();
		});
		// clear shareBuffer
		partialMatches.get(patternId).getAccessor().clearPatternState(getKeyedStateBackend(), patternId);
	}

	private MultiplePatternProcessFunctionV2 getPatternProcessFun(String patternId) throws Exception {
		return patternStates.get(patternId).getPatternProcessFunction();
	}

	private void registerMetrics(MetricGroup metrics){
		// metrics
		this.numPatternsAdded = metrics.counter(CepMetricConstants.PATTERNS_ADDED_METRIC_NAME);
		this.numPatternsDropped = metrics.counter(CepMetricConstants.PATTERNS_DROPPED_METRIC_NAME);
		this.numPatternsUsed = metrics.counter(CepMetricConstants.PATTERNS_USED_METRIC_NAME);
		this.numLateRecordsDropped = metrics.counter(CepMetricConstants.LATE_ELEMENTS_DROPPED_METRIC_NAME);
		this.numLateRecordsOutput = metrics.counter(CepMetricConstants.LATE_ELEMENTS_OUTPUT_METRIC_NAME);
		this.numStateCleanerTriggered = metrics.counter(CepMetricConstants.STATE_CLEANER_TRIGGERD_METRIC_NAME);
		this.numMatchedSequences = metrics.counter(CepMetricConstants.MATCHED_SEQUENCES_METRIC_NAME);
		this.numUnMatchedSequences = metrics.counter(CepMetricConstants.UNMATCHED_SEQUENCES_METRIC_NAME);
		this.numTimeOutSequences = metrics.counter(CepMetricConstants.TIME_OUT_MATCHED_SEQUENCES_METRIC_NAME);

		this.watermarkLatency = metrics.gauge(CepMetricConstants.WATERMARK_LATENCY_METRIC_NAME, () -> {
			long watermark = timerService.currentWatermark();
			if (watermark < 0) {
				return 0L;
			} else {
				return timerService.currentProcessingTime() - watermark;
			}
		});

		advanceTimeMs = this.getMetricGroup().histogram(CepMetricConstants.ADVANCE_TIME_METRIC_NAME,
			new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))));
		processEventMs = this.getMetricGroup().histogram(CepMetricConstants.PROCESS_EVENT_METRIC_NAME,
			new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))));
		processMatchedMs = this.getMetricGroup().histogram(CepMetricConstants.PROCESS_MATCHED_METRIC_NAME,
			new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))));
		processUnMatchMs = this.getMetricGroup().histogram(CepMetricConstants.PROCESS_UNMATCHED_METRIC_NAME,
			new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))));
		processTimeoutMs = this.getMetricGroup().histogram(CepMetricConstants.PROCESS_TIMEOUT_METRIC_NAME,
			new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500))));
	}

	protected Counter getNumLateRecordsDropped() {
		return numLateRecordsDropped;
	}

	protected Gauge<Long> getWatermarkLatency() {
		return watermarkLatency;
	}

	/**
	 * Gives {@link NFA} access to {@link InternalTimerService} and tells if {@link CepOperator} works in
	 * processing time. Should be instantiated once per operator.
	 */
	private class TimerServiceImpl implements TimerService {

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}

	}

	/**
	 * Implementation of {@link PatternProcessFunction.Context}. Design to be instantiated once per operator.
	 * It serves three methods:
	 *  <ul>
	 *      <li>gives access to currentProcessingTime through {@link InternalTimerService}</li>
	 *      <li>gives access to timestamp of current record (or null if Processing time)</li>
	 *      <li>enables side outputs with proper timestamp of StreamRecord handling based on either Processing or
	 *          Event time</li>
	 *  </ul>
	 */
	private class ContextFunctionImpl implements MultiplePatternProcessFunctionV2.Context {

		private Long timestamp;
		private Pattern<IN, IN> currentPattern;

		@Override
		public <X> void output(final OutputTag<X> outputTag, final X value) {
			final StreamRecord<X> record;
			if (isProcessingTime) {
				record = new StreamRecord<>(value);
			} else {
				record = new StreamRecord<>(value, timestamp());
			}
			output.collect(outputTag, record);
		}

		void setCurrentPattern(Pattern<IN, IN> pattern) {
			this.currentPattern = pattern;
		}

		void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		@Override
		public Pattern<IN, IN> currentPattern() {
			return currentPattern;
		}

		@Override
		public long timestamp() {
			return timestamp;
		}

		@Override
		public long currentProcessingTime() {
			return timerService.currentProcessingTime();
		}
	}

	//////////////////////			Testing Methods			//////////////////////

	@VisibleForTesting
	Map<String, SharedBuffer<IN>> getPartialMatches() throws Exception {
		return partialMatches;
	}

	@VisibleForTesting
	public BroadcastState<String, PatternProcessor> getPatternStates() {
		return patternStates;
	}

	@VisibleForTesting
	public Map<String, NFA<IN>> getUsingNFAs() {
		return usingNFAs;
	}
}
