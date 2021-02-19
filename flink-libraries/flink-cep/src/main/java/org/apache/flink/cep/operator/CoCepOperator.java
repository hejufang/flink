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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.functions.MultiplePatternProcessFunction;
import org.apache.flink.cep.functions.MultiplePatternTimedOutPartialMatchHandler;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.cep.utils.CEPUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cep.utils.CEPUtils.defaultTtlConfig;
import static org.apache.flink.cep.utils.CEPUtils.generateUniqueId;

/**
 * The CepOperator which can cooperate with pattern data stream.
 */
@Internal
public class CoCepOperator<IN, KEY, OUT>
		extends AbstractUdfStreamOperator<OUT, MultiplePatternProcessFunction<IN, OUT>>
		implements TwoInputStreamOperator<IN, Pattern<IN, IN>, OUT>, Triggerable<KEY, VoidNamespace> {

	private static final long serialVersionUID = -1243854353417L;

	private static final Logger LOG = LoggerFactory.getLogger(CoCepOperator.class);

	private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";
	private static final String LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME = "lateRecordsDroppedRate";
	private static final String WATERMARK_LATENCY_METRIC_NAME = "watermarkLatency";

	private final boolean isProcessingTime;

	private final TypeSerializer<IN> inputSerializer;

	///////////////			State			//////////////

	private static final String PATTERN_STATE_NAME = "patternStateName";
	private static final String NFA_STATE_NAME = "nfaStateName";
	private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";

	private transient MapState<Long, List<IN>> elementQueueState;
	private transient MapState<String, NFAState> computationStates;
	private transient Map<String, SharedBuffer<IN>> partialMatches;

	private transient InternalTimerService<VoidNamespace> timerService;

	private Map<String, NFA<IN>> usingNFAs;

	private BroadcastState<String, Pattern> patternStates;

	/**
	 * The last seen watermark. This will be used to
	 * decide if an incoming element is late or not.
	 */
	private long lastWatermark;

	/** Comparator for secondary sorting. Primary sorting is always done on time. */
	private final EventComparator<IN> comparator;

	/**
	 * {@link OutputTag} to use for late arriving events. Elements with timestamp smaller than
	 * the current watermark will be emitted to this.
	 */
	private final OutputTag<IN> lateDataOutputTag;

	/** Strategy which element to skip after a match was found. */
	private final AfterMatchSkipStrategy afterMatchSkipStrategy;

	/** Context passed to user function. */
	private transient CoCepOperator.ContextFunctionImpl context;

	/** Main output collector, that sets a proper timestamp to the StreamRecord. */
	private transient TimestampedCollector<OUT> collector;

	/** Wrapped RuntimeContext that limits the underlying context features. */
	private transient CepRuntimeContext cepRuntimeContext;

	/** Thin context passed to NFA that gives access to time related characteristics. */
	private transient TimerService cepTimerService;

	private List<Pattern<IN, IN>> initialPatterns;

	private Map<String, String> properties;

	private final long ttlMilliSeconds;

	// ------------------------------------------------------------------------
	// Metrics
	// ------------------------------------------------------------------------

	private transient Counter numLateRecordsDropped;
	private transient Meter lateRecordsDroppedRate;
	private transient Gauge<Long> watermarkLatency;

	public CoCepOperator(
			final TypeSerializer<IN> inputSerializer,
			final boolean isProcessingTime,
			@Nullable final EventComparator<IN> comparator,
			@Nullable final AfterMatchSkipStrategy afterMatchSkipStrategy,
			final MultiplePatternProcessFunction<IN, OUT> function,
			@Nullable final OutputTag<IN> lateDataOutputTag,
			final List<Pattern<IN, IN>> initialPatterns,
			Map<String, String> properties) {
		super(function);

		this.inputSerializer = Preconditions.checkNotNull(inputSerializer);

		this.isProcessingTime = isProcessingTime;
		this.comparator = comparator;
		this.lateDataOutputTag = lateDataOutputTag;

		this.initialPatterns = initialPatterns;
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
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		this.cepRuntimeContext = new CepRuntimeContext(getRuntimeContext());
		FunctionUtils.setFunctionRuntimeContext(getUserFunction(), this.cepRuntimeContext);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		// initializeState through the provided context
		MapStateDescriptor<String, NFAState> descriptor = new MapStateDescriptor<>(NFA_STATE_NAME, new StringSerializer(), new NFAStateSerializer());
		descriptor.enableTimeToLive(defaultTtlConfig(this.ttlMilliSeconds));
		computationStates = context.getKeyedStateStore().getMapState(descriptor);

		patternStates = context.getOperatorStateStore().getBroadcastState(
				new MapStateDescriptor<>(PATTERN_STATE_NAME, new StringSerializer(), new KryoSerializer<>(Pattern.class, new ExecutionConfig())));
		partialMatches = new HashMap<>();
//		partialMatches = new SharedBuffer<>(context.getKeyedStateStore(), inputSerializer);

		MapStateDescriptor<Long, List<IN>> elementQueueStateDesc = new MapStateDescriptor<>(
				EVENT_QUEUE_STATE_NAME,
				LongSerializer.INSTANCE,
				new ListSerializer<>(inputSerializer));
		elementQueueStateDesc.enableTimeToLive(defaultTtlConfig(this.ttlMilliSeconds));
		elementQueueState = context.getKeyedStateStore().getMapState(elementQueueStateDesc);
	}

	@Override
	public void open() throws Exception {
		super.open();
		timerService = getInternalTimerService(
				"watermark-callbacks",
				VoidNamespaceSerializer.INSTANCE,
				this);

		context = new CoCepOperator.ContextFunctionImpl();
		collector = new TimestampedCollector<>(output);
		cepTimerService = new CoCepOperator.TimerServiceImpl();

		// metrics
		this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);
		this.lateRecordsDroppedRate = metrics.meter(
				LATE_ELEMENTS_DROPPED_RATE_METRIC_NAME,
				new MeterView(numLateRecordsDropped, 60));
		this.watermarkLatency = metrics.gauge(WATERMARK_LATENCY_METRIC_NAME, () -> {
			long watermark = timerService.currentWatermark();
			if (watermark < 0) {
				return 0L;
			} else {
				return timerService.currentProcessingTime() - watermark;
			}
		});

		Iterator<Map.Entry<String, Pattern>> iter = patternStates.iterator();
		while (iter.hasNext()) {
			Pattern<IN, IN> pattern = (Pattern<IN, IN>) iter.next().getValue();
			if (pattern != null) {
				LOG.info("Recover a new pattern from state(id={},hash={})", pattern.getPatternId(), pattern.getHash());
				initializeNewPattern(pattern);
			}
		}

		if (initialPatterns.size() > 0) {
			for (Pattern<IN, IN> pattern : initialPatterns) {
				if (!this.usingNFAs.containsKey(pattern.getPatternId()) || this.usingNFAs.get(pattern.getPatternId()).getHash() != pattern.getHash()) {
					LOG.info("Initial pattern(id={},hash={})", pattern.getPatternId(), pattern.getHash());
					initializeNewPattern(pattern);
				}
			}
		}
	}

	@Override
	public void processElement2(StreamRecord<Pattern<IN, IN>> element) throws Exception {
		final Pattern<IN, IN> pattern = element.getValue();
		final String patternId = pattern.getPatternId();

		if (pattern.isDisabled()) {
			// disable this pattern
			disableOldPattern(patternId);
			return;
		}

		if (this.patternStates.contains(pattern.getPatternId()) && this.patternStates.get(patternId).getHash() == pattern.getHash()) {
			// do nothing if there is an exactly same pattern
			return;
		}

		// update currentNFA
		if (this.usingNFAs.containsKey(pattern.getPatternId())) {
			this.usingNFAs.get(pattern.getPatternId()).close();
		}
		LOG.info("Initialize a new pattern from upstream(id={},hash={})", pattern.getPatternId(), pattern.getHash());
		initializeNewPattern(pattern);
	}

	// TODO. add the state removal here
	private void disableOldPattern(String patternId) throws Exception {
		this.usingNFAs.remove(patternId);
		this.patternStates.remove(patternId);
		this.partialMatches.remove(patternId);
	}

	// make sure the pattern's hash or the patternId is new
	private void initializeNewPattern(Pattern<IN, IN> pattern) throws Exception {
		String patternId = pattern.getPatternId();
		int hash = pattern.getHash();
		final NFA<IN> nfa = compileNFA(pattern);
		nfa.open(cepRuntimeContext, new Configuration());

		this.usingNFAs.put(patternId, nfa);
		this.patternStates.put(patternId, pattern);
		if (!this.partialMatches.containsKey(patternId)) {
			this.partialMatches.put(patternId, new SharedBuffer<>(generateUniqueId(patternId, hash), getKeyedStateStore(), inputSerializer, ttlMilliSeconds));
		} else {
			this.partialMatches.get(patternId).getAccessor().clearMemoryCache();
		}
		this.userFunction.processNewPattern(pattern);
	}

	private NFA<IN> compileNFA(Pattern<IN, IN> pattern) {
		final boolean timeoutHandling = getUserFunction() instanceof TimedOutPartialMatchHandler;
		final NFACompiler.NFAFactory<IN> nfaFactory = NFACompiler.compileFactory(pattern, timeoutHandling, pattern.isAllowSinglePartialMatchPerKey());
		return nfaFactory.createNFA();
	}

	@Override
	public void close() throws Exception {
		super.close();
		for (Map.Entry<String, NFA<IN>> nfa: usingNFAs.entrySet()) {
			if (nfa.getValue() != null) {
				nfa.getValue().close();
			}
		}
	}

	@Override
	public void processElement1(StreamRecord<IN> element) throws Exception {
		if (this.usingNFAs.isEmpty()) {
//			LOG.warn("Current pattern is not defined, drop records...");
			return;
		}

		if (isProcessingTime) {
			if (comparator == null) {
				// there can be no out of order elements in processing time
				// iterate all patterns

				Iterator<Map.Entry<String, Pattern>> iter = this.patternStates.iterator();
				while (iter.hasNext()) {
					Map.Entry<String, Pattern> entry = iter.next();
					NFAState nfaState = getNFAState(entry.getKey());
					long timestamp = getProcessingTimeService().getCurrentProcessingTime();
					advanceTime(nfaState, timestamp);
					processEvent(nfaState, element.getValue(), timestamp);
					updateNFA(nfaState);
				}
			} else {
				long currentTime = timerService.currentProcessingTime();
				bufferEvent(element.getValue(), currentTime);

				// register a timer for the next millisecond to sort and emit buffered data
				timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, currentTime + 1);
			}

		} else {

			long timestamp = element.getTimestamp();
			IN value = element.getValue();

			// In event-time processing we assume correctness of the watermark.
			// Events with timestamp smaller than or equal with the last seen watermark are considered late.
			// Late events are put in a dedicated side output, if the user has specified one.

			if (timestamp > lastWatermark) {

				// we have an event with a valid timestamp, so
				// we buffer it until we receive the proper watermark.

				saveRegisterWatermarkTimer();

				bufferEvent(value, timestamp);

			} else if (lateDataOutputTag != null) {
				output.collect(lateDataOutputTag, element);
			} else {
				lateRecordsDroppedRate.markEvent();
			}
		}
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		processWatermark(mark);
	}

	/**
	 * Registers a timer for {@code current watermark + 1}, this means that we get triggered
	 * whenever the watermark advances, which is what we want for working off the queue of
	 * buffered elements.
	 */
	private void saveRegisterWatermarkTimer() {
		long currentWatermark = timerService.currentWatermark();
		// protect against overflow
		if (currentWatermark + 1 > currentWatermark) {
			timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, currentWatermark + 1);
		}
	}

	private void bufferEvent(IN event, long currentTime) throws Exception {
		List<IN> elementsForTimestamp = elementQueueState.get(currentTime);
		if (elementsForTimestamp == null) {
			elementsForTimestamp = new ArrayList<>();
		}

		if (getExecutionConfig().isObjectReuseEnabled()) {
			// copy the StreamRecord so that it cannot be changed
			elementsForTimestamp.add(inputSerializer.copy(event));
		} else {
			elementsForTimestamp.add(event);
		}
		elementQueueState.put(currentTime, elementsForTimestamp);
	}

	@Override
	public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {

		// 1) get the queue of pending elements for the key and the corresponding NFA,
		// 2) process the pending elements in event time order and custom comparator if exists
		//		by feeding them in the NFA
		// 3) advance the time to the current watermark, so that expired patterns are discarded.
		// 4) update the stored state for the key, by only storing the new NFA and MapState iff they
		//		have state to be used later.
		// 5) update the last seen watermark.

		// STEP 1
		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();

		// STEP 2
		while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= timerService.currentWatermark()) {
			long timestamp = sortedTimestamps.poll();

			try (Stream<IN> data = sort(elementQueueState.get(timestamp))) {
				final List<IN> elements = data.collect(Collectors.toList());
				Iterator<Map.Entry<String, Pattern>> iter = this.patternStates.iterator();
				while (iter.hasNext()) {
					Map.Entry<String, Pattern> entry = iter.next();
					advanceTime(getNFAState(entry.getKey()), timestamp);

					for (IN event : elements) {
						try {
							processEvent(getNFAState(entry.getKey()), event, timestamp);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				}
			}
			elementQueueState.remove(timestamp);
		}

		Iterator<Map.Entry<String, Pattern>> iter = this.patternStates.iterator();
		while (iter.hasNext()) {
			NFAState nfaState = getNFAState(iter.next().getKey());
			advanceTime(nfaState, timerService.currentWatermark());
			updateNFA(nfaState);
		}

		if (!sortedTimestamps.isEmpty() || !partialMatches.isEmpty()) {
			saveRegisterWatermarkTimer();
		}

		// STEP 5
		updateLastSeenWatermark(timerService.currentWatermark());
	}

	@Override
	public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
		// 1) get the queue of pending elements for the key and the corresponding NFA,
		// 2) process the pending elements in process time order and custom comparator if exists
		//		by feeding them in the NFA
		// 3) update the stored state for the key, by only storing the new NFA and MapState iff they
		//		have state to be used later.

		// STEP 1
		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();

		// STEP 2
		while (!sortedTimestamps.isEmpty()) {
			long timestamp = sortedTimestamps.poll();
			try (Stream<IN> data = sort(elementQueueState.get(timestamp))) {
				final List<IN> elements = data.collect(Collectors.toList());
				Iterator<Map.Entry<String, Pattern>> iter = this.patternStates.iterator();
				while (iter.hasNext()) {
					Map.Entry<String, Pattern> entry = iter.next();
					advanceTime(getNFAState(entry.getKey()), timestamp);

					for (IN event : elements) {
						try {
							processEvent(getNFAState(entry.getKey()), event, timestamp);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				}
			}
			elementQueueState.remove(timestamp);
		}

		// STEP 3
		Iterator<Map.Entry<String, Pattern>> iter = this.patternStates.iterator();
		while (iter.hasNext()) {
			NFAState nfaState = getNFAState(iter.next().getKey());
			updateNFA(nfaState);
		}
	}

	private Stream<IN> sort(Collection<IN> elements) {
		Stream<IN> stream = elements.stream();
		return (comparator == null) ? stream : stream.sorted(comparator);
	}

	private void updateLastSeenWatermark(long timestamp) {
		this.lastWatermark = timestamp;
	}

	private NFAState getNFAState(String patternId) throws Exception {
		NFAState nfaState = computationStates.get(patternId);
		if (nfaState != null && usingNFAs.containsKey(patternId) && usingNFAs.get(patternId).getHash() == nfaState.getHash()) {
			return nfaState;
		} else {
			Preconditions.checkArgument(usingNFAs.get(patternId) != null, "The pattern is not defined. Please check your pattern data stream if using broadcast.");
			NFAState newNFAState = usingNFAs.get(patternId).createInitialNFAState();
			computationStates.put(patternId, newNFAState);
			// clear the data state in shared buffer
			partialMatches.get(patternId).getAccessor().clearKeyedState();
			return newNFAState;
		}
	}

	private void updateNFA(NFAState nfaState) throws Exception {
		if (nfaState.isStateChanged()) {
			nfaState.resetStateChanged();
			computationStates.put(nfaState.getPatternId(), nfaState);
		}
	}

	private PriorityQueue<Long> getSortedTimestamps() throws Exception {
		PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
		for (Long timestamp : elementQueueState.keys()) {
			sortedTimestamps.offer(timestamp);
		}
		return sortedTimestamps;
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
		try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.get(nfaState.getPatternId()).getAccessor()) {
			Collection<Map<String, List<IN>>> patterns =
					usingNFAs.get(nfaState.getPatternId()).process(sharedBufferAccessor, nfaState, event, timestamp, afterMatchSkipStrategy, cepTimerService);
			processMatchedSequences(nfaState.getPatternId(), patterns, timestamp);
			if (patterns.isEmpty()) {
				getUserFunction().processUnMatch(event, context, getCurrentKey(), collector);
			}
		}
	}

	/**
	 * Advances the time for the given NFA to the given timestamp. This means that no more events with timestamp
	 * <b>lower</b> than the given timestamp should be passed to the nfa, This can lead to pruning and timeouts.
	 */
	private void advanceTime(NFAState nfaState, long timestamp) throws Exception {
		try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.get(nfaState.getPatternId()).getAccessor()) {
			// output pending states matches
			Collection<Map<String, List<IN>>> pendingMatches = usingNFAs.get(nfaState.getPatternId()).pendingStateMatches(sharedBufferAccessor, nfaState, timestamp);
			if (!pendingMatches.isEmpty()) {
				processMatchedSequences(nfaState.getPatternId(), pendingMatches, timestamp);
			}

			// output timeout patterns
			Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut =
					usingNFAs.get(nfaState.getPatternId()).advanceTime(sharedBufferAccessor, nfaState, timestamp);
			if (!timedOut.isEmpty()) {
				processTimedOutSequences(nfaState.getPatternId(), timedOut);
			}
		}
	}

	private void processMatchedSequences(String patternId, Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {
		MultiplePatternProcessFunction<IN, OUT> function = getUserFunction();
		setContext(timestamp, patternStates.get(patternId));
		for (Map<String, List<IN>> matchingSequence : matchingSequences) {
			usingNFAs.get(patternId).clearStateWhenOutput();
			function.processMatch(Tuple2.of(patternId, matchingSequence), context, getCurrentKey(), collector);
		}
	}

	private void processTimedOutSequences(String patternId, Collection<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences) throws Exception {
		MultiplePatternProcessFunction<IN, OUT> function = getUserFunction();
		if (function instanceof MultiplePatternTimedOutPartialMatchHandler) {

			@SuppressWarnings("unchecked")
			MultiplePatternTimedOutPartialMatchHandler<IN> timeoutHandler = (MultiplePatternTimedOutPartialMatchHandler<IN>) function;

			for (Tuple2<Map<String, List<IN>>, Long> matchingSequence : timedOutSequences) {
				setContext(matchingSequence.f1, patternStates.get(patternId));
				usingNFAs.get(patternId).clearStateWhenOutput();
				timeoutHandler.processTimedOutMatch(Tuple2.of(patternId, matchingSequence.f0), getCurrentKey(), context);
			}
		}
	}

	private void setContext(long timestamp, Pattern<IN, IN> pattern) {
		if (!isProcessingTime) {
			collector.setAbsoluteTimestamp(timestamp);
		}

		context.setTimestamp(timestamp);
		context.setCurrentPattern(pattern);
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
	private class ContextFunctionImpl implements MultiplePatternProcessFunction.Context {

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
	boolean hasNonEmptySharedBuffer(KEY key) throws Exception {
		setCurrentKey(key);
		return !partialMatches.isEmpty();
	}

	@VisibleForTesting
	boolean hasNonEmptyPQ(KEY key) throws Exception {
		setCurrentKey(key);
		return elementQueueState.keys().iterator().hasNext();
	}

	@VisibleForTesting
	int getPQSize(KEY key) throws Exception {
		setCurrentKey(key);
		int counter = 0;
		for (List<IN> elements : elementQueueState.values()) {
			counter += elements.size();
		}
		return counter;
	}

	protected Counter getNumLateRecordsDropped() {
		return numLateRecordsDropped;
	}

	protected Gauge<Long> getWatermarkLatency() {
		return watermarkLatency;
	}
}
