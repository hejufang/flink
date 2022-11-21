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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.socket.ResultStatus;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystemSafetyNet;
import org.apache.flink.core.fs.Path;
import org.apache.flink.event.shuffle.WarehouseTaskShuffleInfoMessage;
import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.resourcemanager.WorkerExitCode;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataRequest;
import org.apache.flink.runtime.rest.messages.taskmanager.preview.PreviewDataResponse;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceOptions;
import org.apache.flink.runtime.socket.PrintTaskJobResultGateway;
import org.apache.flink.runtime.socket.SocketConstants;
import org.apache.flink.runtime.socket.TaskJobResultGateway;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.cache.CacheManager;
import org.apache.flink.runtime.state.cache.NonCacheManager;
import org.apache.flink.runtime.taskexecutor.BackPressureSampleableTask;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotPayload;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.types.Either;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.LoggerHelper;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TaskManagerExceptionUtils;
import org.apache.flink.util.WrappingRuntimeException;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import static org.apache.flink.runtime.execution.ExecutionState.CREATED;
import static org.apache.flink.runtime.execution.ExecutionState.DEPLOYING;
import static org.apache.flink.runtime.execution.ExecutionState.FINISHED;
import static org.apache.flink.runtime.execution.ExecutionState.RUNNING;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The Task represents one execution of a parallel subtask on a TaskManager.
 * A Task wraps a Flink operator (which may be a user function) and
 * runs it, providing all services necessary for example to consume input data,
 * produce its results (intermediate result partitions) and communicate
 * with the JobManager.
 *
 * <p>The Flink operators (implemented as subclasses of
 * {@link AbstractInvokable} have only data readers, writers, and certain event callbacks.
 * The task connects those to the network stack and actor messages, and tracks the state
 * of the execution and handles exceptions.
 *
 * <p>Tasks have no knowledge about how they relate to other tasks, or whether they
 * are the first attempt to execute the task, or a repeated attempt. All of that
 * is only known to the JobManager. All the task knows are its own runnable code,
 * the task's configuration, and the IDs of the intermediate results to consume and
 * produce (if any).
 *
 * <p>Each Task is run by one dedicated thread.
 */
public class Task implements Runnable, TaskSlotPayload, TaskActions, PartitionProducerStateProvider, CheckpointListener, BackPressureSampleableTask {

	/** The class logger. */
	private static final Logger LOG = LoggerFactory.getLogger(Task.class);

	/** The thread group that contains all task threads. */
	public static final ThreadGroup TASK_THREADS_GROUP = new ThreadGroup("Flink Task Threads");

	/** For atomic state updates. */
	private static final AtomicReferenceFieldUpdater<Task, ExecutionState> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(Task.class, ExecutionState.class, "executionState");

	// ------------------------------------------------------------------------
	//  Constant fields that are part of the initial Task construction
	// ------------------------------------------------------------------------

	/** The job that the task belongs to. */
	private final JobID jobId;

	private final String jobName;
	private final String jobUID;

	/** The vertex in the JobGraph whose code the task executes. */
	private final JobVertexID vertexId;

	/** The vertex index in created order. Only used for metrics. */
	private final int vertexIndexInCreatedOrder;

	/** The execution attempt of the parallel subtask. */
	private final ExecutionAttemptID executionId;

	/** ID which identifies the slot in which the task is supposed to run. */
	private final AllocationID allocationId;

	/** TaskInfo object for this task. */
	private final TaskInfo taskInfo;

	/** The name of the task, including subtask indexes. */
	private final String taskNameWithSubtask;

	/** The metric name of the task, including subtask indexes, mainly used for logging. */
	private final String taskMetricNameWithSubtask;

	/** The thread name of the task, including subtask indexes. */
	private final String taskThreadName;

	/** The job-wide configuration object. */
	private final Configuration jobConfiguration;

	/** The task-specific configuration. */
	private final Configuration taskConfiguration;

	/** The jar files used by this task. */
	private final Collection<PermanentBlobKey> requiredJarFiles;

	/** The classpaths used by this task. */
	private final Collection<URL> requiredClasspaths;

	/** If this is null, then task need to deserialize {@link #serializedExecutionConfig} to get it.*/
	private ExecutionConfig executionConfig;

	/** The name of the class that holds the invokable code. */
	private final String nameOfInvokableClass;

	/** Access to task manager configuration and host names. */
	private final TaskManagerRuntimeInfo taskManagerConfig;

	/** The memory manager to be used by this task. */
	private final MemoryManager memoryManager;

	/** The I/O manager to be used by this task. */
	private final IOManager ioManager;

	/** The BroadcastVariableManager to be used by this task. */
	private final BroadcastVariableManager broadcastVariableManager;

	private final TaskEventDispatcher taskEventDispatcher;

	/** Information provider for external resources. */
	private final ExternalResourceInfoProvider externalResourceInfoProvider;

	/** The manager for state of operators running in this task/slot. */
	private final TaskStateManager taskStateManager;

	/** Serialized version of the job specific execution configuration (see {@link ExecutionConfig}). */
	private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

	private final ResultPartitionWriter[] consumableNotifyingPartitionWriters;

	private final IndexedInputGate[] inputGates;

	/** Connection to the task manager. */
	private final TaskManagerActions taskManagerActions;

	/** Input split provider for the task. */
	private final InputSplitProvider inputSplitProvider;

	/** Checkpoint notifier used to communicate with the CheckpointCoordinator. */
	private final CheckpointResponder checkpointResponder;

	/** The gateway for operators to send messages to the operator coordinators on the Job Manager. */
	private final TaskOperatorEventGateway operatorCoordinatorEventGateway;

	/** GlobalAggregateManager used to update aggregates on the JobMaster. */
	private final GlobalAggregateManager aggregateManager;

	/** The library cache, from which the task can request its class loader. */
	private final LibraryCacheManager.ClassLoaderHandle classLoaderHandle;

	/** The cache for user-defined files that the invokable requires. */
	private final FileCache fileCache;

	/** The service for kvState registration of this task. */
	private final KvStateService kvStateService;

	/** The registry of this task which enables live reporting of accumulators. */
	private final AccumulatorRegistry accumulatorRegistry;

	/** The thread that executes the task. */
	private Thread executingThread;

	private CompletableFuture<Void>  executingThreadFuture;

	/** Parent group for all metrics of this task. */
	private final TaskMetricGroup metrics;

	/** Partition producer state checker to request partition states from. */
	private final PartitionProducerStateChecker partitionProducerStateChecker;

	/** Executor to run future callbacks. */
	private final Executor executor;

	/** Future that is completed once {@link #run()} exits. */
	private final CompletableFuture<ExecutionState> terminationFuture = new CompletableFuture<>();

	/** The cacheManager responsible for managing all caches. */
	private final CacheManager cacheManager;

	/** Gateway for push result task. */
	private final TaskJobResultGateway taskJobResultGateway;

	private final TaskThreadPoolExecutor taskExecutorService;

	private final TaskThreadPoolExecutor taskDaemonExecutor;

	private final ScheduledExecutorService taskCheckStuckExecutor;

	private final boolean downStreamBlocked;

	// ------------------------------------------------------------------------
	//  Fields that control the task execution. All these fields are volatile
	//  (which means that they introduce memory barriers), to establish
	//  proper happens-before semantics on parallel modification
	// ------------------------------------------------------------------------

	/** atomic flag that makes sure the invokable is canceled exactly once upon error. */
	private final AtomicBoolean invokableHasBeenCanceled;

	private final boolean taskSubmitRunning;

	/** The invokable of this task, if initialized. All accesses must copy the reference and
	 * check for null, as this field is cleared as part of the disposal logic. */
	@Nullable
	private volatile AbstractInvokable invokable;

	/** The current execution state of the task. */
	private volatile ExecutionState executionState = ExecutionState.CREATED;

	private final long[] stateTimestamps;

	/** The observed exception, in case the task execution failed. */
	private volatile Throwable failureCause;

	/** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
	private long taskCancellationInterval;

	/** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
	private long taskCancellationTimeout;

	/** This class loader should be set as the context class loader for threads that may dynamically load user code. */
	private ClassLoader userCodeClassLoader;

	private final boolean jobLogDetailDisable;

	/** If true, the task will notify its final status in its execution thread. */
	private final boolean notifyFinalStateInTaskThreadEnable;

	/** If true, the task reaches termination immediately after initializing. */
	private final boolean taskInitializeFinishEnable;

	private final long createTimestamp = System.currentTimeMillis();
	private long initializeFinishTimestamp;
	private long invokeFinishTimestamp;
	private long executeFinishTimestamp;
	private final boolean useTaskThreadPool;

	private static final String FLINK_TASK_SHUFFLE_INFO_METRICS = "task_shuffle_info";
	private WarehouseTaskShuffleInfoMessage warehouseTaskShuffleInfoMessage;
	private MessageSet<WarehouseTaskShuffleInfoMessage> taskShuffleInfoMessageMessageSet;

	/** Cache for invokable class constructor. */
	private final Cache<String, Constructor<? extends AbstractInvokable>> constructorCache;

	/**
	 * <p><b>IMPORTANT:</b> This constructor may not start any work that would need to
	 * be undone in the case of a failing task deployment.</p>
	 */
	public Task(
		JobInformation jobInformation,
		TaskInformation taskInformation,
		ExecutionAttemptID executionAttemptID,
		AllocationID slotAllocationId,
		int subtaskIndex,
		int attemptNumber,
		List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		int targetSlotNumber,
		MemoryManager memManager,
		IOManager ioManager,
		TaskThreadPoolExecutor taskExecutorService,
		TaskThreadPoolExecutor taskDaemonExecutor,
		ScheduledExecutorService taskCheckStuckExecutor,
		ShuffleEnvironment<?, ?> shuffleEnvironment,
		KvStateService kvStateService,
		BroadcastVariableManager bcVarManager,
		TaskEventDispatcher taskEventDispatcher,
		ExternalResourceInfoProvider externalResourceInfoProvider,
		TaskStateManager taskStateManager,
		TaskManagerActions taskManagerActions,
		InputSplitProvider inputSplitProvider,
		CheckpointResponder checkpointResponder,
		TaskOperatorEventGateway operatorCoordinatorEventGateway,
		GlobalAggregateManager aggregateManager,
		LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
		FileCache fileCache,
		TaskManagerRuntimeInfo taskManagerConfig,
		@Nonnull TaskMetricGroup metricGroup,
		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
		PartitionProducerStateChecker partitionProducerStateChecker,
		Executor executor) {
		this(
			jobInformation,
			taskInformation,
			executionAttemptID,
			slotAllocationId,
			subtaskIndex,
			attemptNumber,
			resultPartitionDeploymentDescriptors,
			inputGateDeploymentDescriptors,
			targetSlotNumber,
			memManager,
			ioManager,
			taskExecutorService,
			taskDaemonExecutor,
			taskCheckStuckExecutor,
			shuffleEnvironment,
			kvStateService,
			bcVarManager,
			taskEventDispatcher,
			externalResourceInfoProvider,
			taskStateManager,
			taskManagerActions,
			inputSplitProvider,
			checkpointResponder,
			operatorCoordinatorEventGateway,
			aggregateManager,
			classLoaderHandle,
			fileCache,
			taskManagerConfig,
			metricGroup,
			resultPartitionConsumableNotifier,
			partitionProducerStateChecker,
			executor,
			new NonCacheManager(),
			false,
			false,
			false,
			new PrintTaskJobResultGateway(),
			false,
			null,
			null,
			null);
	}

	public Task(
			JobInformation jobInformation,
			TaskInformation taskInformation,
			ExecutionAttemptID executionAttemptID,
			AllocationID slotAllocationId,
			int subtaskIndex,
			int attemptNumber,
			List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
			List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
			int targetSlotNumber,
			MemoryManager memManager,
			IOManager ioManager,
			TaskThreadPoolExecutor taskExecutorService,
			TaskThreadPoolExecutor taskDaemonExecutor,
			ScheduledExecutorService taskCheckStuckExecutor,
			ShuffleEnvironment<?, ?> shuffleEnvironment,
			KvStateService kvStateService,
			BroadcastVariableManager bcVarManager,
			TaskEventDispatcher taskEventDispatcher,
			ExternalResourceInfoProvider externalResourceInfoProvider,
			TaskStateManager taskStateManager,
			TaskManagerActions taskManagerActions,
			InputSplitProvider inputSplitProvider,
			CheckpointResponder checkpointResponder,
			TaskOperatorEventGateway operatorCoordinatorEventGateway,
			GlobalAggregateManager aggregateManager,
			LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
			FileCache fileCache,
			TaskManagerRuntimeInfo taskManagerConfig,
			@Nonnull TaskMetricGroup metricGroup,
			ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
			PartitionProducerStateChecker partitionProducerStateChecker,
			Executor executor,
			CacheManager cacheManager,
			boolean jobLogDetailDisable,
			boolean taskSubmitRunning,
			boolean notifyFinalStateInTaskThreadEnable,
			TaskJobResultGateway taskJobResultGateway,
			boolean taskInitializeFinishEnable,
			@Nullable ClassLoader userCodeClassLoader,
			@Nullable ExecutionConfig executionConfig,
			Cache<String, Constructor<? extends AbstractInvokable>> constructorCache) {

		Preconditions.checkNotNull(jobInformation);
		Preconditions.checkNotNull(taskInformation);

		Preconditions.checkArgument(0 <= subtaskIndex, "The subtask index must be positive.");
		Preconditions.checkArgument(0 <= attemptNumber, "The attempt number must be positive.");
		Preconditions.checkArgument(0 <= targetSlotNumber, "The target slot number must be positive.");

		this.taskInfo = new TaskInfo(
				taskInformation.getTaskName(),
				taskInformation.getTaskMetricName(),
				taskInformation.getMaxNumberOfSubtasks(),
				subtaskIndex,
				taskInformation.getNumberOfSubtasks(),
				attemptNumber,
				String.valueOf(slotAllocationId));

		this.jobId = jobInformation.getJobId();
		this.jobName = jobInformation.getJobName();
		this.jobUID = jobInformation.getJobUID();
		this.vertexId = taskInformation.getJobVertexId();
		this.vertexIndexInCreatedOrder = taskInformation.getVertexIndexInCreatedOrder();
		this.executionId  = Preconditions.checkNotNull(executionAttemptID);
		this.allocationId = Preconditions.checkNotNull(slotAllocationId);
		this.taskNameWithSubtask = taskInfo.getTaskNameWithSubtasks();
		this.taskMetricNameWithSubtask = taskInfo.getTaskMetricNameWithSubtasks();
		this.jobConfiguration = jobInformation.getJobConfiguration();
		this.taskConfiguration = taskInformation.getTaskConfiguration();
		this.requiredJarFiles = jobInformation.getRequiredJarFileBlobKeys();
		this.requiredClasspaths = jobInformation.getRequiredClasspathURLs();
		this.nameOfInvokableClass = taskInformation.getInvokableClassName();
		this.serializedExecutionConfig = jobInformation.getSerializedExecutionConfig();
		this.userCodeClassLoader = userCodeClassLoader;
		this.executionConfig = executionConfig;

		Configuration tmConfig = taskManagerConfig.getConfiguration();
		this.taskCancellationInterval = tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL);
		this.taskCancellationTimeout = tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT);
		this.useTaskThreadPool = tmConfig.getBoolean(TaskManagerOptions.TASK_THREAD_POOL_ENABLE);
		this.taskThreadName = useTaskThreadPool ? jobId + "-" + taskMetricNameWithSubtask : taskMetricNameWithSubtask;
		this.memoryManager = Preconditions.checkNotNull(memManager);
		this.ioManager = Preconditions.checkNotNull(ioManager);
		this.broadcastVariableManager = Preconditions.checkNotNull(bcVarManager);
		this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
		this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
		this.accumulatorRegistry = new AccumulatorRegistry(jobId, executionId);

		this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
		this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
		this.operatorCoordinatorEventGateway = Preconditions.checkNotNull(operatorCoordinatorEventGateway);
		this.aggregateManager = Preconditions.checkNotNull(aggregateManager);
		this.taskManagerActions = checkNotNull(taskManagerActions);
		this.externalResourceInfoProvider = checkNotNull(externalResourceInfoProvider);

		this.classLoaderHandle = Preconditions.checkNotNull(classLoaderHandle);
		this.fileCache = Preconditions.checkNotNull(fileCache);
		this.kvStateService = Preconditions.checkNotNull(kvStateService);
		this.taskManagerConfig = Preconditions.checkNotNull(taskManagerConfig);
		this.taskExecutorService = taskExecutorService;
		this.taskDaemonExecutor = taskDaemonExecutor;
		this.taskCheckStuckExecutor = taskCheckStuckExecutor;

		this.metrics = metricGroup;

		this.partitionProducerStateChecker = Preconditions.checkNotNull(partitionProducerStateChecker);
		this.executor = Preconditions.checkNotNull(executor);
		this.cacheManager = cacheManager;
		this.taskSubmitRunning = taskSubmitRunning;
		this.notifyFinalStateInTaskThreadEnable = notifyFinalStateInTaskThreadEnable;

		this.taskInitializeFinishEnable = taskInitializeFinishEnable;

		this.stateTimestamps = new long[ExecutionState.values().length];
		markTimestamp(CREATED, System.currentTimeMillis());

		// create the reader and writer structures

		final String taskMetricNameWithSubtaskAndId = taskMetricNameWithSubtask + " (" + executionId + ')';

		final ShuffleIOOwnerContext taskShuffleContext = shuffleEnvironment
			.createShuffleIOOwnerContext(taskMetricNameWithSubtaskAndId, executionId, metrics.getIOMetricGroup());

		// produced intermediate result partitions
		final ResultPartitionWriter[] resultPartitionWriters = shuffleEnvironment.createResultPartitionWriters(
			taskShuffleContext,
			resultPartitionDeploymentDescriptors).toArray(new ResultPartitionWriter[] {});

		this.consumableNotifyingPartitionWriters = ConsumableNotifyingResultPartitionWriterDecorator.decorate(
			resultPartitionDeploymentDescriptors,
			resultPartitionWriters,
			this,
			jobId,
			resultPartitionConsumableNotifier);

		// consumed intermediate result partitions
		final IndexedInputGate[] gates = shuffleEnvironment.createInputGates(
				taskShuffleContext,
				this,
				inputGateDeploymentDescriptors)
			.toArray(new IndexedInputGate[0]);

		this.inputGates = new IndexedInputGate[gates.length];
		int counter = 0;
		for (IndexedInputGate gate : gates) {
			inputGates[counter++] = new InputGateWithMetrics(gate, metrics.getIOMetricGroup().getNumBytesInCounter());
		}

		if (shuffleEnvironment instanceof NettyShuffleEnvironment) {
			//noinspection deprecation
			((NettyShuffleEnvironment) shuffleEnvironment)
				.registerLegacyNetworkMetrics(metrics.getIOMetricGroup(), resultPartitionWriters, gates);
		}

		// init the warehouse messageSet.
		if (tmConfig.getString(ExecutionOptions.EXECUTION_APPLICATION_TYPE).equals(ConfigConstants.FLINK_BATCH_APPLICATION_TYPE)) {
			this.taskShuffleInfoMessageMessageSet = new MessageSet<>(MessageType.TASK_SHUFFLE_INFO);
			this.metrics.getIOMetricGroup().gauge(FLINK_TASK_SHUFFLE_INFO_METRICS, this.taskShuffleInfoMessageMessageSet);
			this.warehouseTaskShuffleInfoMessage = new WarehouseTaskShuffleInfoMessage(taskInfo);
			// set the shuffle service type
			String shuffleServiceType = tmConfig.getBoolean(ShuffleServiceOptions.SHUFFLE_CLOUD_SHUFFLE_MODE) ? ShuffleServiceOptions.CLOUD_SHUFFLE : ShuffleServiceOptions.NETTY_SHUFFLE;
			this.warehouseTaskShuffleInfoMessage.setShuffleServiceType(shuffleServiceType);

			this.warehouseTaskShuffleInfoMessage.setInputShuffleDataBytes(0);
			// the executionState is CREATE while in the initializing phase
			this.warehouseTaskShuffleInfoMessage.setTaskState(executionState.name());
			// add the init task shuffle info
			this.taskShuffleInfoMessageMessageSet.addMessage(new Message<>(this.warehouseTaskShuffleInfoMessage));

			LOG.info("Init the task info warehouseTaskShuffleInfoMessage({}).", warehouseTaskShuffleInfoMessage);
		}

		invokableHasBeenCanceled = new AtomicBoolean(false);

		// finally, create the executing thread, but do not start it
		if (!useTaskThreadPool) {
			executingThread = new Thread(TASK_THREADS_GROUP, this, taskThreadName);
		}

		this.jobLogDetailDisable = jobLogDetailDisable;
		this.taskJobResultGateway = taskJobResultGateway;
		this.constructorCache = constructorCache;

		this.downStreamBlocked = resultPartitionDeploymentDescriptors.stream()
			.map(ResultPartitionDeploymentDescriptor::getPartitionType)
			.anyMatch(ResultPartitionType::isBlocking);
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	@Override
	public JobID getJobID() {
		return jobId;
	}

	public JobVertexID getJobVertexId() {
		return vertexId;
	}

	public int getVertexIndexInCreatedOrder() {
		return vertexIndexInCreatedOrder;
	}

	@Override
	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}

	@Override
	public AllocationID getAllocationId() {
		return allocationId;
	}

	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	public AccumulatorRegistry getAccumulatorRegistry() {
		return accumulatorRegistry;
	}

	public TaskMetricGroup getMetricGroup() {
		return metrics;
	}

	public Thread getExecutingThread() {
		return executingThread;
	}

	public CompletableFuture<Void> getExecutingThreadFuture() {
		return executingThreadFuture;
	}

	@Override
	public CompletableFuture<ExecutionState> getTerminationFuture() {
		return terminationFuture;
	}

	@VisibleForTesting
	long getTaskCancellationInterval() {
		return taskCancellationInterval;
	}

	@VisibleForTesting
	long getTaskCancellationTimeout() {
		return taskCancellationTimeout;
	}

	@Nullable
	@VisibleForTesting
	AbstractInvokable getInvokable() {
		return invokable;
	}

	public boolean isDownStreamBlocked() {
		return downStreamBlocked;
	}

	@Override
	public boolean isBackPressured() {
		if (invokable == null || consumableNotifyingPartitionWriters.length == 0 || executionState != ExecutionState.RUNNING) {
			return false;
		}
		for (int i = 0; i < consumableNotifyingPartitionWriters.length; ++i) {
			if (!consumableNotifyingPartitionWriters[i].isAvailable()) {
				return true;
			}
		}
		return false;
	}

	// ------------------------------------------------------------------------
	//  Task Execution
	// ------------------------------------------------------------------------

	/**
	 * Returns the current execution state of the task.
	 * @return The current execution state of the task.
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	public long getStateTimestamp(ExecutionState state) {
		return this.stateTimestamps[state.ordinal()];
	}

	private void markTimestamp(ExecutionState state, long timestamp) {
		this.stateTimestamps[state.ordinal()] = timestamp;
	}

	/**
	 * Checks whether the task has failed, is canceled, or is being canceled at the moment.
	 * @return True is the task in state FAILED, CANCELING, or CANCELED, false otherwise.
	 */
	public boolean isCanceledOrFailed() {
		return executionState == ExecutionState.CANCELING ||
				executionState == ExecutionState.CANCELED ||
				executionState == ExecutionState.FAILED;
	}

	@Override
	public boolean isRunning() {
		return executionState == ExecutionState.RUNNING;
	}

	/**
	 * If the task has failed, this method gets the exception that caused this task to fail.
	 * Otherwise this method returns null.
	 *
	 * @return The exception that caused the task to fail, or null, if the task has not failed.
	 */
	public Throwable getFailureCause() {
		return failureCause;
	}

	public Map<String, Integer> getNumberOfInputChannels() {
		Map<String, Integer> numberOfChannels = new HashMap<>();
		for (IndexedInputGate inputGate : inputGates) {
			for (int i = 0; i < inputGate.getNumberOfInputChannels(); i++) {
				String channelType = inputGate.getChannelType(i);
				numberOfChannels.put(channelType,
						numberOfChannels.getOrDefault(channelType, 0) + 1);
			}
		}
		return numberOfChannels;
	}

	/**
	 * Starts the task's thread.
	 */
	public void startTaskThread() {
		if (useTaskThreadPool) {
			executingThreadFuture = taskExecutorService.submit(this, taskThreadName, userCodeClassLoader);
		} else {
			executingThread.start();
		}
	}

	/**
	 * The core work method that bootstraps the task and executes its code.
	 */
	@Override
	public void run() {
		try {
			if (useTaskThreadPool) {
				executingThread = Thread.currentThread();
			}
			doRun();
		} catch (Throwable t) {
			LOG.error("Error while run task {}.", taskThreadName, t);
			throw t;
		} finally {
			terminationFuture.complete(executionState);
			if (executingThreadFuture != null) {
				executingThreadFuture.complete(null);
			}
		}
	}

	private void doRun() {
		long startRunTimestamp = System.currentTimeMillis();
		// ----------------------------
		//  Initial State transition
		// ----------------------------
		while (true) {
			ExecutionState current = this.executionState;
			if (current == ExecutionState.CREATED) {
				if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
					// success, we can start our work
					break;
				}
			}
			else if (current == ExecutionState.FAILED) {
				// we were immediately failed. tell the TaskManager that we reached our final state
				notifyFinalState();
				if (metrics != null) {
					metrics.close();
				}
				return;
			}
			else if (current == ExecutionState.CANCELING) {
				if (transitionState(ExecutionState.CANCELING, ExecutionState.CANCELED)) {
					// we were immediately canceled. tell the TaskManager that we reached our final state
					notifyFinalState();
					if (metrics != null) {
						metrics.close();
					}
					return;
				}
			}
			else {
				if (metrics != null) {
					metrics.close();
				}
				throw new IllegalStateException("Invalid state for beginning of operation of task " + this + '.');
			}
		}

		// all resource acquisitions and registrations from here on
		// need to be undone in the end
		Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
		AbstractInvokable invokable = null;

		try {
			// ----------------------------
			//  Task Bootstrap - We periodically
			//  check for canceling as a shortcut
			// ----------------------------

			// activate safety net for task thread
			LOG.debug("Creating FileSystem stream leak safety net for task {}", this);
			FileSystemSafetyNet.initializeSafetyNetForThread();

			// first of all, get a user-code classloader
			// this may involve downloading the job's JAR files and/or classes
			if (jobLogDetailDisable) {
				LOG.debug("Loading JAR files for task {}.", this);
			} else {
				LOG.info("Loading JAR files for task {}.", this);
			}

			if (userCodeClassLoader == null) {
				userCodeClassLoader = createUserCodeClassloader();
			}
			if (executionConfig == null) {
				executionConfig = serializedExecutionConfig.deserializeValue(userCodeClassLoader);
			}

			if (executionConfig.getTaskCancellationInterval() >= 0) {
				// override task cancellation interval from Flink config if set in ExecutionConfig
				taskCancellationInterval = executionConfig.getTaskCancellationInterval();
			}

			if (executionConfig.getTaskCancellationTimeout() >= 0) {
				// override task cancellation timeout from Flink config if set in ExecutionConfig
				taskCancellationTimeout = executionConfig.getTaskCancellationTimeout();
			}

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			// register the task with the network stack
			// this operation may fail if the system does not have enough
			// memory to run the necessary data exchanges
			// the registration must also strictly be undone
			// ----------------------------------------------------------------
			if (jobLogDetailDisable) {
				LOG.debug("Registering task at network: {}.", this);
			} else {
				LOG.info("Registering task at network: {}.", this);
			}

			setupPartitionsAndGates(consumableNotifyingPartitionWriters, inputGates);

			for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
				taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
			}

			// next, kick off the background copying of files for the distributed cache
			try {
				for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
						DistributedCache.readFileInfoFromConfig(jobConfiguration)) {
					LOG.info("Obtaining local cache file for '{}'.", entry.getKey());
					Future<Path> cp = fileCache.createTmpFile(entry.getKey(), entry.getValue(), jobId, executionId);
					distributedCacheEntries.put(entry.getKey(), cp);
				}
			}
			catch (Exception e) {
				throw new Exception(
					String.format("Exception while adding files to distributed cache of task %s (%s).", taskMetricNameWithSubtask, executionId), e);
			}

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  call the user code initialization methods
			// ----------------------------------------------------------------

			TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());

			Environment env = new RuntimeEnvironment(
				jobId,
				jobName,
				jobUID,
				vertexId,
				executionId,
				executionConfig,
				taskInfo,
				jobConfiguration,
				taskConfiguration,
				userCodeClassLoader,
				memoryManager,
				ioManager,
				broadcastVariableManager,
				taskStateManager,
				aggregateManager,
				accumulatorRegistry,
				kvStateRegistry,
				inputSplitProvider,
				distributedCacheEntries,
				consumableNotifyingPartitionWriters,
				inputGates,
				taskEventDispatcher,
				checkpointResponder,
				operatorCoordinatorEventGateway,
				taskManagerConfig,
				metrics,
				this,
				externalResourceInfoProvider,
				cacheManager,
				taskJobResultGateway,
				taskCheckStuckExecutor,
				taskDaemonExecutor);

			// Make sure the user code classloader is accessible thread-locally.
			// We are setting the correct context class loader before instantiating the invokable
			// so that it is available to the invokable during its entire lifetime.
			executingThread.setContextClassLoader(userCodeClassLoader);

			// now load and instantiate the task's invokable code
			if (taskInitializeFinishEnable) {
				invokable = loadAndInstantiateInvokable(userCodeClassLoader, "org.apache.flink.runtime.jobgraph.tasks.NoOpInvokable", env);
			} else {
				invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);
			}

			// ----------------------------------------------------------------
			//  actual task core work
			// ----------------------------------------------------------------

			// we must make strictly sure that the invokable is accessible to the cancel() call
			// by the time we switched to running.
			this.invokable = invokable;
			if (taskCancellationTimeout > 0) {
				invokable.setCancelWatchDog(new TaskCancelerWatchDog(
					executingThread,
					taskManagerActions,
					String.format("Cancellation Watchdog for %s (%s).",
						taskThreadName, executionId),
					taskThreadName,
					taskCancellationTimeout));
			}

			// switch to the RUNNING state, if that fails, we have been canceled/failed in the meantime
			if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
				throw new CancelTaskException();
			}

			// notify everyone that we switched to running
			// if taskSubmitRunning is true, the task will be switch running in job master directly when it is submitted.
			if (!taskSubmitRunning) {
				taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, ExecutionState.RUNNING));
			}

			// make sure the user code classloader is accessible thread-locally
			executingThread.setContextClassLoader(userCodeClassLoader);
			initializeFinishTimestamp = System.currentTimeMillis();

			// run the invokable
			invokable.invoke();
			if (taskInitializeFinishEnable) {
				if (taskInfo.getTaskName().contains(SocketConstants.SOCKET_SINK_NAME_TAG)) {
					taskJobResultGateway.sendResult(jobId, null, ResultStatus.COMPLETE, null);
				}
			}

			invokeFinishTimestamp = System.currentTimeMillis();

			// make sure, we enter the catch block if the task leaves the invoke() method due
			// to the fact that it has been canceled
			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  finalization of a successful execution
			// ----------------------------------------------------------------

			// finish the produced partitions. if this fails, we consider the execution failed.
			for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
				if (partitionWriter != null) {
					partitionWriter.finish();
				}
			}

			// try to mark the task as finished
			// if that fails, the task was canceled/failed in the meantime
			if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
				throw new CancelTaskException();
			}
		}
		catch (Throwable t) {

			// unwrap wrapped exceptions to make stack traces more compact
			if (t instanceof WrappingRuntimeException) {
				t = ((WrappingRuntimeException) t).unwrap();
			}

			// ----------------------------------------------------------------
			// the execution failed. either the invokable code properly failed, or
			// an exception was thrown as a side effect of cancelling
			// ----------------------------------------------------------------

			t = TaskManagerExceptionUtils.tryEnrichTaskManagerError(t);

			try {
				// check if the exception is unrecoverable
				if (ExceptionUtils.isJvmFatalError(t) ||
						(t instanceof OutOfMemoryError && taskManagerConfig.shouldExitJvmOnOutOfMemoryError())) {

					// terminate the JVM immediately
					// don't attempt a clean shutdown, because we cannot expect the clean shutdown to complete
					try {
						LOG.error("Encountered fatal error {} - terminating the JVM", t.getClass().getName(), t);
					} finally {
						Runtime.getRuntime().halt(-1);
					}
				}

				// transition into our final state. we should be either in DEPLOYING, RUNNING, CANCELING, or FAILED
				// loop for multiple retries during concurrent state changes via calls to cancel() or
				// to failExternally()
				while (true) {
					ExecutionState current = this.executionState;

					if (current == ExecutionState.RUNNING || current == ExecutionState.DEPLOYING) {
						if (t instanceof CancelTaskException) {
							if (transitionState(current, ExecutionState.CANCELED)) {
								LOG.error("Task run encountered canceled exception", t);
								cancelInvokable(invokable);
								break;
							}
						}
						else {
							if (transitionState(current, ExecutionState.FAILED, t)) {
								// proper failure of the task. record the exception as the root cause
								failureCause = t;
								cancelInvokable(invokable);

								break;
							}
						}
					}
					else if (current == ExecutionState.CANCELING) {
						if (transitionState(current, ExecutionState.CANCELED)) {
							break;
						}
					}
					else if (current == ExecutionState.FAILED) {
						// in state failed already, no transition necessary any more
						break;
					}
					// unexpected state, go to failed
					else if (transitionState(current, ExecutionState.FAILED, t)) {
						LOG.error("Unexpected state in task {} ({}) during an exception: {}.", taskMetricNameWithSubtask, executionId, current);
						break;
					}
					// else fall through the loop and
				}
			}
			catch (Throwable tt) {
				String message = String.format("FATAL - exception in exception handler of task %s (%s).", taskMetricNameWithSubtask, executionId);
				LOG.error(message, tt);
				notifyFatalError(message, tt, WorkerExitCode.TASKMANAGER_PROCESS_EXCEPTION_ERROR);
			}
		}
		finally {
			executeFinishTimestamp = System.currentTimeMillis();
			try {
				if (jobLogDetailDisable) {
					LOG.debug("Freeing task resources for {} ({}).", taskMetricNameWithSubtask, executionId);
				} else {
					LOG.info("Freeing task resources for {} ({}).", taskMetricNameWithSubtask, executionId);
				}

				// report the task shuffle info to warehouse
				if (this.warehouseTaskShuffleInfoMessage != null && this.taskShuffleInfoMessageMessageSet != null) {
					if (this.executionState.isTerminal()) {
						if (metrics != null) {
							this.warehouseTaskShuffleInfoMessage.setInputShuffleDataBytes(metrics.getIOMetricGroup().getNumBytesInCounter().getCount());
						} else {
							LOG.warn("The metrics has already closed, so could not get the inputShuffleDataBytes from metrics.");
						}
						this.warehouseTaskShuffleInfoMessage.setTaskState(executionState.name());
						this.taskShuffleInfoMessageMessageSet.addMessage(new Message<>(this.warehouseTaskShuffleInfoMessage));
						LOG.info("The finished warehouseTaskShuffleInfoMessage is {}.", warehouseTaskShuffleInfoMessage.toString());
					}
				}

				// clear the reference to the invokable. this helps guard against holding references
				// to the invokable and its structures in cases where this Task object is still referenced
				this.invokable = null;

				// free the network resources
				releaseResources();

				// free memory resources
				if (invokable != null) {
					memoryManager.releaseAll(invokable);
				}

				// remove all of the tasks resources
				fileCache.releaseJob(jobId, executionId);

				// close and de-activate safety net for task thread
				LOG.debug("Ensuring all FileSystem streams are closed for task {}", this);
				FileSystemSafetyNet.closeSafetyNetAndGuardedResourcesForThread();

				notifyFinalState();

				LOG.info("Finish execute task for {} ({}) for job {} with threadStart {} initialize {} invoke {} finish {} cleanUp {} (ms).",
					taskMetricNameWithSubtask,
					executionId,
					jobId,
					startRunTimestamp - createTimestamp,
					initializeFinishTimestamp - startRunTimestamp,
					invokeFinishTimestamp - initializeFinishTimestamp,
					executeFinishTimestamp - invokeFinishTimestamp,
					System.currentTimeMillis() - executeFinishTimestamp);
			}
			catch (Throwable t) {
				// an error in the resource cleanup is fatal
				String message = String.format("FATAL - exception in resource cleanup of task %s (%s).", taskMetricNameWithSubtask, executionId);
				LOG.error(message, t);
				notifyFatalError(message, t, WorkerExitCode.TASKMANAGER_CLEAN_RESOURCE_ERROR);
			}

			// un-register the metrics at the end so that the task may already be
			// counted as finished when this happens
			// errors here will only be logged
			try {
				metrics.close();
			}
			catch (Throwable t) {
				LOG.error("Error during metrics de-registration of task {} ({}).", taskMetricNameWithSubtask, executionId, t);
			}
		}
	}

	@VisibleForTesting
	public static void setupPartitionsAndGates(
		ResultPartitionWriter[] producedPartitions, InputGate[] inputGates) throws IOException {

		for (ResultPartitionWriter partition : producedPartitions) {
			partition.setup();
		}

		// InputGates must be initialized after the partitions, since during InputGate#setup
		// we are requesting partitions
		for (InputGate gate : inputGates) {
			gate.setup();
		}
	}

	/**
	 * Releases resources before task exits. We should also fail the partition to release if the task
	 * has failed, is canceled, or is being canceled at the moment.
	 */
	private void releaseResources() {
		LOG.debug("Release task {} network resources (state: {}).", taskMetricNameWithSubtask, getExecutionState());

		for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
			taskEventDispatcher.unregisterPartition(partitionWriter.getPartitionId());
			if (isCanceledOrFailed()) {
				partitionWriter.fail(getFailureCause());
			} else if (taskInitializeFinishEnable) {
				partitionWriter.releaseFromPartitionManager();
			}
		}

		closeNetworkResources();
		try {
			taskStateManager.close();
		} catch (Exception e) {
			LOG.error("Failed to close task state manager for task {}.", taskMetricNameWithSubtask, e);
		}
	}

	/**
	 * There are two scenarios to close the network resources. One is from {@link TaskCanceler} to early
	 * release partitions and gates. Another is from task thread during task exiting.
	 */
	private void closeNetworkResources() {
		for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
			try {
				partitionWriter.close();
			} catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalError(t);
				LOG.error("Failed to release result partition for task {}.", taskMetricNameWithSubtask, t);
			}
		}

		for (InputGate inputGate : inputGates) {
			try {
				inputGate.close();
			} catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalError(t);
				LOG.error("Failed to release input gate for task {}.", taskMetricNameWithSubtask, t);
			}
		}
	}

	private ClassLoader createUserCodeClassloader() throws Exception {
		long startDownloadTime = System.currentTimeMillis();

		// triggers the download of all missing jar files from the job manager
		final ClassLoader userCodeClassLoader = classLoaderHandle.getOrResolveClassLoader(requiredJarFiles, requiredClasspaths);

		LOG.debug("Getting user code class loader for task {} at library cache manager took {} milliseconds",
				executionId, System.currentTimeMillis() - startDownloadTime);

		return userCodeClassLoader;
	}

	private void notifyFinalState() {
		checkState(executionState.isTerminal());
		if (notifyFinalStateInTaskThreadEnable) {
			taskManagerActions.updateTaskExecutionState(this, new TaskExecutionState(jobId, executionId, executionState, failureCause));
		} else {
			taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, executionState, failureCause));
		}
	}

	private void notifyFatalError(String message, Throwable cause, int exitCode) {
		taskManagerActions.notifyFatalError(message, cause, exitCode);
	}

	/**
	 * Try to transition the execution state from the current state to the new state.
	 *
	 * @param currentState of the execution
	 * @param newState of the execution
	 * @return true if the transition was successful, otherwise false
	 */
	private boolean transitionState(ExecutionState currentState, ExecutionState newState) {
		return transitionState(currentState, newState, null);
	}

	public void transitionFinalState() {
		transitionState(CREATED, DEPLOYING);
		transitionState(DEPLOYING, RUNNING);
		transitionState(RUNNING, FINISHED);
		releaseResources();
		metrics.close();
	}

	/**
	 * Try to transition the execution state from the current state to the new state.
	 *
	 * @param currentState of the execution
	 * @param newState of the execution
	 * @param cause of the transition change or null
	 * @return true if the transition was successful, otherwise false
	 */
	private boolean transitionState(ExecutionState currentState, ExecutionState newState, Throwable cause) {
		if (STATE_UPDATER.compareAndSet(this, currentState, newState)) {
			if (cause == null) {
				if (jobLogDetailDisable) {
					LOG.debug("{} ({}) switched from {} to {}.", taskMetricNameWithSubtask, executionId, currentState, newState);
				} else {
					LOG.info("{} ({}) switched from {} to {}.", LoggerHelper.secMark("taskMetricNameWithSubtask", taskMetricNameWithSubtask), LoggerHelper.secMark("executionId", executionId), LoggerHelper.secMark("executionState", currentState), LoggerHelper.secMark("executionState", newState));
				}
			} else {
				LOG.warn("{} ({}) switched from {} to {} {}.", LoggerHelper.secMark("taskMetricNameWithSubtask", taskMetricNameWithSubtask), LoggerHelper.secMark("executionId", executionId), LoggerHelper.secMark("executionState", currentState), LoggerHelper.secMark("executionState", newState), LoggerHelper.secMark("errMsg", cause));
			}

			try {
				markTimestamp(newState, System.currentTimeMillis());
			} catch (Throwable t) {
				LOG.error("Mark timestamp for {} failed.", taskMetricNameWithSubtask, t);
			}
			return true;
		} else {
			return false;
		}
	}

	// ----------------------------------------------------------------------------------------------------------------
	//  Canceling / Failing the task from the outside
	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Cancels the task execution. If the task is already in a terminal state
	 * (such as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
	 * Otherwise it sets the state to CANCELING, and, if the invokable code is running,
	 * starts an asynchronous thread that aborts that code.
	 *
	 * <p>This method never blocks.</p>
	 */
	public void cancelExecution() {
		LOG.info("Attempting to cancel task {} ({}).", taskMetricNameWithSubtask, executionId);
		cancelOrFailAndCancelInvokable(ExecutionState.CANCELING, null);
	}

	/**
	 * Get preview data of task.
	 */
	public PreviewDataResponse getPreviewData(PreviewDataRequest previewDataRequest) {
		return invokable.getPreviewData(previewDataRequest);
	}

	/**
	 * Whether this task support preview, means is running preview connector.
	 */
	public boolean supportPreview() {
		return invokable.supportPreview();
	}

	/**
	 * Marks task execution failed for an external reason (a reason other than the task code itself
	 * throwing an exception). If the task is already in a terminal state
	 * (such as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
	 * Otherwise it sets the state to FAILED, and, if the invokable code is running,
	 * starts an asynchronous thread that aborts that code.
	 *
	 * <p>This method never blocks.</p>
	 */
	@Override
	public void failExternally(Throwable cause) {
		LOG.info("Attempting to fail task externally {} ({}).", taskMetricNameWithSubtask, executionId);
		cancelOrFailAndCancelInvokable(ExecutionState.FAILED, cause);
	}

	private void cancelOrFailAndCancelInvokable(ExecutionState targetState, Throwable cause) {
		try {
			cancelOrFailAndCancelInvokableInternal(targetState, cause);
		} catch (Throwable t) {
			LOG.error("Fail to cancel task {} ({}).", taskMetricNameWithSubtask, executionId, t);
			if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(t) || t instanceof TaskCancelerWatchDogException) {
				String message = String.format("FATAL - exception in cancelling task %s (%s).", taskMetricNameWithSubtask, executionId);
				notifyFatalError(message, t, WorkerExitCode.TASKMANAGER_CANCEL_TASK_TIMEOUT);
			} else {
				throw t;
			}
		}
	}

	@VisibleForTesting
	void cancelOrFailAndCancelInvokableInternal(ExecutionState targetState, Throwable cause) {
		while (true) {
			ExecutionState current = executionState;

			// if the task is already canceled (or canceling) or finished or failed,
			// then we need not do anything
			if (current.isTerminal() || current == ExecutionState.CANCELING) {
				LOG.info("Task {} is already in state {}", taskMetricNameWithSubtask, current);
				return;
			}

			if (current == ExecutionState.DEPLOYING || current == ExecutionState.CREATED) {
				if (transitionState(current, targetState, cause)) {
					// if we manage this state transition, then the invokable gets never called
					// we need not call cancel on it
					this.failureCause = cause;
					return;
				}
			}
			else if (current == ExecutionState.RUNNING) {
				if (transitionState(ExecutionState.RUNNING, targetState, cause)) {
					// we are canceling / failing out of the running state
					// we need to cancel the invokable

					// copy reference to guard against concurrent null-ing out the reference
					final AbstractInvokable invokable = this.invokable;

					if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
						this.failureCause = cause;

						LOG.info("Triggering cancellation of task code {} ({}).", taskMetricNameWithSubtask, executionId);

						// if a cancellation timeout is set, the watchdog thread kills the process
						// if graceful cancellation does not succeed
						if (taskCancellationTimeout > 0) {
							LOG.info("Task {} starts watchdog thread.", taskMetricNameWithSubtask);
							try {
								String threadName = String.format("Cancellation Watchdog for %s (%s).",
									taskThreadName, executionId);
								Runnable cancelWatchdog = new TaskCancelerWatchDog(
									executingThread,
									taskManagerActions,
									threadName,
									taskThreadName,
									taskCancellationTimeout);
								if (useTaskThreadPool) {
									taskDaemonExecutor.submit(cancelWatchdog, threadName);
								} else {
									Thread watchDogThread = new Thread(
										executingThread.getThreadGroup(),
										cancelWatchdog,
										threadName);
									watchDogThread.setDaemon(true);
									watchDogThread.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);
									watchDogThread.start();
								}
							} catch (Exception e) {
								LOG.error("Task {} starts watchdog thread failed.", taskThreadName, e);
								throw new TaskCancelerWatchDogException(e);
							}
						}

						// because the canceling may block on user code, we cancel from a separate thread
						// we do not reuse the async call handler, because that one may be blocked, in which
						// case the canceling could not continue

						// The canceller calls cancel and interrupts the executing thread once
						Runnable canceler = new TaskCanceler(LOG, this::closeNetworkResources, invokable, executingThread, taskThreadName);

						if (useTaskThreadPool) {
							taskDaemonExecutor.submit(canceler, String.format("Canceler for %s (%s).", taskThreadName, executionId));
						} else {
							Thread cancelThread = new Thread(
								executingThread.getThreadGroup(),
								canceler,
								String.format("Canceler for %s (%s).", taskThreadName, executionId));
							cancelThread.setDaemon(true);
							cancelThread.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);
							cancelThread.start();
						}

						// the periodic interrupting thread - a different thread than the canceller, in case
						// the application code does blocking stuff in its cancellation paths.
						if (invokable.shouldInterruptOnCancel()) {
							LOG.info("Task {} starts interrupting thread.", taskThreadName);
							Runnable interrupter = new TaskInterrupter(
									LOG,
									invokable,
									executingThread,
									taskThreadName,
									taskCancellationInterval);

							if (useTaskThreadPool) {
								taskDaemonExecutor.submit(interrupter, String.format("Canceler/Interrupts for %s (%s).", taskThreadName, executionId));
							} else {
								Thread interruptingThread = new Thread(
									executingThread.getThreadGroup(),
									interrupter,
									String.format("Canceler/Interrupts for %s (%s).", taskThreadName, executionId));
								interruptingThread.setDaemon(true);
								interruptingThread.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);
								interruptingThread.start();
							}
						}
					}
					return;
				}
			}
			else {
				throw new IllegalStateException(String.format("Unexpected state: %s of task %s (%s).",
					current, taskMetricNameWithSubtask, executionId));
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Partition State Listeners
	// ------------------------------------------------------------------------

	@Override
	public void requestPartitionProducerState(
			final IntermediateDataSetID intermediateDataSetId,
			final ResultPartitionID resultPartitionId,
			Consumer<? super ResponseHandle> responseConsumer) {

		final CompletableFuture<ExecutionState> futurePartitionState =
			partitionProducerStateChecker.requestPartitionProducerState(
				jobId,
				intermediateDataSetId,
				resultPartitionId);

		FutureUtils.assertNoException(
			futurePartitionState
				.handle(PartitionProducerStateResponseHandle::new)
				.thenAcceptAsync(responseConsumer, executor));
	}

	// ------------------------------------------------------------------------
	//  Notifications on the invokable
	// ------------------------------------------------------------------------

	/**
	 * Calls the invokable to trigger a checkpoint.
	 *
	 * @param checkpointID The ID identifying the checkpoint.
	 * @param checkpointTimestamp The timestamp associated with the checkpoint.
	 * @param checkpointOptions Options for performing this checkpoint.
	 * @param advanceToEndOfEventTime Flag indicating if the source should inject a {@code MAX_WATERMARK} in the pipeline
	 *                           to fire any registered event-time timers.
	 */
	public void triggerCheckpointBarrier(
			final long checkpointID,
			final long checkpointTimestamp,
			final CheckpointOptions checkpointOptions,
			final boolean advanceToEndOfEventTime) {

		final AbstractInvokable invokable = this.invokable;
		final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointID, checkpointTimestamp);

		if (executionState == ExecutionState.RUNNING && invokable != null) {
			try {
				invokable.triggerCheckpointAsync(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);
			}
			catch (RejectedExecutionException ex) {
				// This may happen if the mailbox is closed. It means that the task is shutting down, so we just ignore it.
				LOG.debug(
					"Triggering checkpoint {} for {} ({}) was rejected by the mailbox",
					checkpointID, taskMetricNameWithSubtask, executionId);
			}
			catch (Throwable t) {
				if (getExecutionState() == ExecutionState.RUNNING) {
					failExternally(new Exception(
						"Error while triggering checkpoint " + checkpointID + " for " +
							taskMetricNameWithSubtask, t));
				} else {
					LOG.debug("Encountered error while triggering checkpoint {} for " +
						"{} ({}) while being not in state running.", checkpointID,
						taskMetricNameWithSubtask, executionId, t);
				}
			}
		}
		else {
			LOG.debug("Declining checkpoint request for non-running task {} ({}).", taskMetricNameWithSubtask, executionId);

			// send back a message that we did not do the checkpoint
			checkpointResponder.declineCheckpoint(jobId, executionId, checkpointID,
					new CheckpointException("Task name with subtask : " + taskMetricNameWithSubtask, CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY));
		}
	}

	@Override
	public void notifyCheckpointComplete(final long checkpointID) {
		final AbstractInvokable invokable = this.invokable;

		if (executionState == ExecutionState.RUNNING && invokable != null) {
			try {
				invokable.notifyCheckpointCompleteAsync(checkpointID);
			}
			catch (RejectedExecutionException ex) {
				// This may happen if the mailbox is closed. It means that the task is shutting down, so we just ignore it.
				LOG.debug(
					"Notify checkpoint complete {} for {} ({}) was rejected by the mailbox",
					checkpointID, taskMetricNameWithSubtask, executionId);
			}
			catch (Throwable t) {
				if (getExecutionState() == ExecutionState.RUNNING) {
					// fail task if checkpoint confirmation failed.
					failExternally(new RuntimeException(
						"Error while confirming checkpoint",
						t));
				}
			}
		}
		else {
			LOG.debug("Ignoring checkpoint commit notification for non-running task {}.", taskMetricNameWithSubtask);
		}
	}

	@Override
	public void notifyCheckpointAborted(final long checkpointID) {
		final AbstractInvokable invokable = this.invokable;

		if (executionState == ExecutionState.RUNNING && invokable != null) {
			try {
				invokable.notifyCheckpointAbortAsync(checkpointID);
			}
			catch (RejectedExecutionException ex) {
				// This may happen if the mailbox is closed. It means that the task is shutting down, so we just ignore it.
				LOG.debug(
					"Notify checkpoint abort {} for {} ({}) was rejected by the mailbox",
					checkpointID, taskMetricNameWithSubtask, executionId);
			}
			catch (Throwable t) {
				if (getExecutionState() == ExecutionState.RUNNING) {
					// fail task if checkpoint aborted notification failed.
					failExternally(new RuntimeException(
						"Error while aborting checkpoint",
						t));
				}
			}
		}
		else {
			LOG.info("Ignoring checkpoint aborted notification for non-running task {}.", taskMetricNameWithSubtask);
		}
	}

	/**
	 * Dispatches an operator event to the invokable task.
	 *
	 * <p>If the event delivery did not succeed, this method throws an exception. Callers can use that
	 * exception for error reporting, but need not react with failing this task (this method takes care
	 * of that).
	 *
	 * @throws FlinkException This method throws exceptions indicating the reason why delivery did not succeed.
	 */
	public void deliverOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> evt) throws FlinkException {
		final AbstractInvokable invokable = this.invokable;

		if (invokable == null || executionState != ExecutionState.RUNNING) {
			throw new TaskNotRunningException("Task is not yet running.");
		}

		try {
			invokable.dispatchOperatorEvent(operator, evt);
		}
		catch (Throwable t) {
			ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

			if (getExecutionState() == ExecutionState.RUNNING) {
				FlinkException e = new FlinkException("Error while handling operator event", t);
				failExternally(e);
				throw e;
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private void cancelInvokable(AbstractInvokable invokable) {
		// in case of an exception during execution, we still call "cancel()" on the task
		if (invokable != null && invokableHasBeenCanceled.compareAndSet(false, true)) {
			try {
				invokable.cancel();
			}
			catch (Throwable t) {
				LOG.error("Error while canceling task {}.", taskMetricNameWithSubtask, t);
			}
		}
	}

	@Override
	public String toString() {
		return String.format("%s (%s) [%s]", taskNameWithSubtask, executionId, executionState);
	}

	@VisibleForTesting
	class PartitionProducerStateResponseHandle implements ResponseHandle {
		private final Either<ExecutionState, Throwable> result;

		PartitionProducerStateResponseHandle(@Nullable ExecutionState producerState, @Nullable Throwable t) {
			this.result = producerState != null ? Either.Left(producerState) : Either.Right(t);
		}

		@Override
		public ExecutionState getConsumerExecutionState() {
			return executionState;
		}

		@Override
		public Either<ExecutionState, Throwable> getProducerExecutionState() {
			return result;
		}

		@Override
		public void cancelConsumption() {
			cancelExecution();
		}

		@Override
		public void failConsumption(Throwable cause) {
			failExternally(cause);
		}
	}

	/**
	 * Instantiates the given task invokable class, passing the given environment (and possibly
	 * the initial task state) to the task's constructor.
	 *
	 * <p>The method will first try to instantiate the task via a constructor accepting both
	 * the Environment and the TaskStateSnapshot. If no such constructor exists, and there is
	 * no initial state, the method will fall back to the stateless convenience constructor that
	 * accepts only the Environment.
	 *
	 * @param classLoader The classloader to load the class through.
	 * @param className The name of the class to load.
	 * @param environment The task environment.
	 *
	 * @return The instantiated invokable task object.
	 *
	 * @throws Throwable Forwards all exceptions that happen during initialization of the task.
	 *                   Also throws an exception if the task class misses the necessary constructor.
	 */
	private AbstractInvokable loadAndInstantiateInvokable(
			ClassLoader classLoader,
			String className,
			Environment environment) throws Throwable {
		Constructor<? extends AbstractInvokable> statelessCtor = getInvokableClassConstructor(classLoader, className);

		// instantiate the class
		try {
			//noinspection ConstantConditions  --> cannot happen
			return statelessCtor.newInstance(environment);
		} catch (InvocationTargetException e) {
			// directly forward exceptions from the eager initialization
			throw e.getTargetException();
		} catch (Exception e) {
			throw new FlinkException("Could not instantiate the task's invokable class.", e);
		}
	}

	private Constructor<? extends AbstractInvokable> getInvokableClassConstructor(
			ClassLoader classLoader,
			String className) throws Throwable {
		Constructor<? extends AbstractInvokable> statelessCtor;

		if (constructorCache != null) {
			statelessCtor = constructorCache.getIfPresent(className);
			if (statelessCtor != null) {
				return statelessCtor;
			}
		}

		final Class<? extends AbstractInvokable> invokableClass;
		try {
			invokableClass = Class.forName(className, true, classLoader)
				.asSubclass(AbstractInvokable.class);
		} catch (Throwable t) {
			throw new Exception("Could not load the task's invokable class.", t);
		}

		try {
			statelessCtor = invokableClass.getConstructor(Environment.class);
		} catch (NoSuchMethodException ee) {
			throw new FlinkException("Task misses proper constructor", ee);
		}

		if (constructorCache != null) {
			constructorCache.put(className, statelessCtor);
		}

		return statelessCtor;
	}

	// ------------------------------------------------------------------------
	//  Task cancellation
	//
	//  The task cancellation uses in total three threads, as a safety net
	//  against various forms of user- and JVM bugs.
	//
	//    - The first thread calls 'cancel()' on the invokable and closes
	//      the input and output connections, for fast thread termination
	//    - The second thread periodically interrupts the invokable in order
	//      to pull the thread out of blocking wait and I/O operations
	//    - The third thread (watchdog thread) waits until the cancellation
	//      timeout and then performs a hard cancel (kill process, or let
	//      the TaskManager know)
	//
	//  Previously, thread two and three were in one thread, but we needed
	//  to separate this to make sure the watchdog thread does not call
	//  'interrupt()'. This is a workaround for the following JVM bug
	//   https://bugs.java.com/bugdatabase/view_bug.do?bug_id=8138622
	// ------------------------------------------------------------------------

	/**
	 * This runner calls cancel() on the invokable, closes input-/output resources,
	 * and initially interrupts the task thread.
	 */
	private static class TaskCanceler implements Runnable {

		private final Logger logger;
		private final Runnable networkResourcesCloser;
		private final AbstractInvokable invokable;
		private final Thread executer;
		private final String taskName;

		TaskCanceler(Logger logger, Runnable networkResourcesCloser, AbstractInvokable invokable, Thread executer, String taskName) {
			this.logger = logger;
			this.networkResourcesCloser = networkResourcesCloser;
			this.invokable = invokable;
			this.executer = executer;
			this.taskName = taskName;
		}

		@Override
		public void run() {
			try {
				// the user-defined cancel method may throw errors.
				// we need do continue despite that
				try {
					invokable.cancel();
				} catch (Throwable t) {
					ExceptionUtils.rethrowIfFatalError(t);
					logger.error("Error while canceling the task {}.", taskName, t);
				}

				// Early release of input and output buffer pools. We do this
				// in order to unblock async Threads, which produce/consume the
				// intermediate streams outside of the main Task Thread (like
				// the Kafka consumer).
				//
				// Don't do this before cancelling the invokable. Otherwise we
				// will get misleading errors in the logs.
				networkResourcesCloser.run();

				// send the initial interruption signal, if requested
				if (invokable.shouldInterruptOnCancel()) {
					executer.interrupt();
				}
			}
			catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalError(t);
				logger.error("Error in the task canceler for task {}.", taskName, t);
			}
		}
	}

	/**
	 * This thread sends the delayed, periodic interrupt calls to the executing thread.
	 */
	private static final class TaskInterrupter implements Runnable {

		/** The logger to report on the fatal condition. */
		private final Logger log;

		/** The invokable task. */
		private final AbstractInvokable task;

		/** The executing task thread that we wait for to terminate. */
		private final Thread executerThread;

		/** The name of the task, for logging purposes. */
		private final String taskName;

		/** The interval in which we interrupt. */
		private final long interruptIntervalMillis;

		TaskInterrupter(
				Logger log,
				AbstractInvokable task,
				Thread executerThread,
				String taskName,
				long interruptIntervalMillis) {

			this.log = log;
			this.task = task;
			this.executerThread = executerThread;
			this.taskName = taskName;
			this.interruptIntervalMillis = interruptIntervalMillis;
		}

		@Override
		public void run() {
			try {
				// we initially wait for one interval
				// in most cases, the threads go away immediately (by the cancellation thread)
				// and we need not actually do anything
				executerThread.join(interruptIntervalMillis);

				// log stack trace where the executing thread is stuck and
				// interrupt the running thread periodically while it is still alive
				while (task.shouldInterruptOnCancel() && executerThread.isAlive()) {

					if (executerThread instanceof TaskThreadPoolExecutor.TaskExecuteThread) {
						if (!executerThread.getName().equals(taskName)) {
							break;
						}
					}

					// build the stack trace of where the thread is stuck, for the log
					StackTraceElement[] stack = executerThread.getStackTrace();
					StringBuilder bld = new StringBuilder();
					for (StackTraceElement e : stack) {
						bld.append(e).append('\n');
					}

					log.warn("Task '{}' did not react to cancelling signal for {} seconds, but is stuck in method:\n {}",
							taskName, (interruptIntervalMillis / 1000), bld);
					if (executerThread instanceof TaskThreadPoolExecutor.TaskExecuteThread) {
						TaskThreadPoolExecutor.TaskExecuteThread taskExecutorThread = (TaskThreadPoolExecutor.TaskExecuteThread) this.executerThread;
						taskExecutorThread.interrupt(taskName);
					} else {
						executerThread.interrupt();
					}
					try {
						executerThread.join(interruptIntervalMillis);
					} catch (InterruptedException e) {
						// we ignore this and fall through the loop
					}
				}
			} catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalError(t);
				log.error("Error in the task canceler for task {}.", taskName, t);
			}
		}
	}

	/**
	 * Watchdog for the cancellation.
	 * If the task thread does not go away gracefully within a certain time, we
	 * trigger a hard cancel action (notify TaskManager of fatal error, which in
	 * turn kills the process).
	 */
	public static class TaskCancelerWatchDog implements Runnable {

		/** The executing task thread that we wait for to terminate. */
		private final Thread executerThread;

		/** The TaskManager to notify if cancellation does not happen in time. */
		private final TaskManagerActions taskManager;

		private CompletableFuture<Void> executingThreadFuture;

		private final String threadName;

		/** The timeout for cancellation. */
		private final long timeoutMillis;

		TaskCancelerWatchDog(
			Thread executerThread,
			TaskManagerActions taskManager,
			String threadName,
			String taskThreadName,
			long timeoutMillis) {

			checkArgument(timeoutMillis > 0);

			this.executerThread = executerThread;
			this.taskManager = taskManager;
			this.threadName = threadName;
			this.timeoutMillis = timeoutMillis;
			if (executerThread instanceof TaskThreadPoolExecutor.TaskExecuteThread) {
				TaskThreadPoolExecutor.TaskExecuteThread taskExecuteThread = (TaskThreadPoolExecutor.TaskExecuteThread) executerThread;
				this.executingThreadFuture = taskExecuteThread.getExecutingThreadFuture(taskThreadName);
			} else {
				this.executingThreadFuture = null;
			}
		}

		public Thread getExecuterThread() {
			return executerThread;
		}

		public String getThreadName() {
			return threadName;
		}

		@Override
		public void run() {
			try {
				final long hardKillDeadline = System.nanoTime() + timeoutMillis * 1_000_000;

				long millisLeft;
				while (executerThread.isAlive()
						&& (millisLeft = (hardKillDeadline - System.nanoTime()) / 1_000_000) > 0) {

					if (executingThreadFuture != null && executingThreadFuture.isDone()) {
						break;
					}

					try {
						executerThread.join(millisLeft);
					}
					catch (InterruptedException ignored) {
						// we don't react to interrupted exceptions, simply fall through the loop
					}
				}

				if (executerThread.isAlive()) {
					if (executingThreadFuture != null && executingThreadFuture.isDone()) {
						return;
					}
					String msg = "Task did not exit gracefully within " + (timeoutMillis / 1000) + " + seconds.";
					taskManager.notifyFatalError(msg, new FlinkRuntimeException(msg), WorkerExitCode.TASKMANAGER_TASK_EXIT_TIMEOUT);
				}
			}
			catch (Throwable t) {
				throw new FlinkRuntimeException("Error in Task Cancellation Watch Dog", t);
			}
		}
	}


	/**
	 * Exception for {@link org.apache.flink.runtime.taskmanager.Task.TaskCancelerWatchDog} when it identified that the TaskCancelerWatchDog
	 * thread create failed.
	 */
	private class TaskCancelerWatchDogException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		public TaskCancelerWatchDogException(Throwable cause) {
			super(cause);
		}
	}
}
