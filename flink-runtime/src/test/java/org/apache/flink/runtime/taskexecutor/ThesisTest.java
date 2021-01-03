package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.partition.NoOpPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.TestingSlotOwner;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.partition.PartitionTable;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ThesisTest extends TestLogger {

	public static final HeartbeatServices HEARTBEAT_SERVICES = new HeartbeatServices(1000L, 1000L);
	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Rule
	public final TestName testName = new TestName();

	private static final Time timeout = Time.milliseconds(10000L);

	private TestingRpcService rpc;

	private BlobCacheService dummyBlobCacheService;

	private TimerService<AllocationID> timerService;

	private Configuration configuration;

	private TaskManagerConfiguration taskManagerConfiguration;

	private JobID jobId;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private TestingHighAvailabilityServices haServices;

	private SettableLeaderRetrievalService resourceManagerLeaderRetriever;

	private SettableLeaderRetrievalService jobManagerLeaderRetriever;

	private NettyShuffleEnvironment nettyShuffleEnvironment;

	@Before
	public void setup() throws IOException {
		rpc = new TestingRpcService();
		timerService = new TimerService<>(TestingUtils.defaultExecutor(), timeout.toMilliseconds());

		dummyBlobCacheService = new BlobCacheService(
			new Configuration(),
			new VoidBlobStore(),
			null);

		configuration = new Configuration();
		taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration);

		jobId = new JobID();

		testingFatalErrorHandler = new TestingFatalErrorHandler();

		haServices = new TestingHighAvailabilityServices();
		resourceManagerLeaderRetriever = new SettableLeaderRetrievalService();
		jobManagerLeaderRetriever = new SettableLeaderRetrievalService();
		haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever);
		haServices.setJobMasterLeaderRetriever(jobId, jobManagerLeaderRetriever);

		nettyShuffleEnvironment = new NettyShuffleEnvironmentBuilder().build();

	}

	@After
	public void teardown() throws Exception {
		if (rpc != null) {
			RpcUtils.terminateRpcService(rpc, timeout);
			rpc = null;
		}

		if (timerService != null) {
			timerService.stop();
			timerService = null;
		}

		if (dummyBlobCacheService != null) {
			dummyBlobCacheService.close();
			dummyBlobCacheService = null;
		}

		if (nettyShuffleEnvironment != null) {
			nettyShuffleEnvironment.close();
		}

		testingFatalErrorHandler.rethrowError();
	}

	@Test
	public void assignPreferredLocation() throws Exception{

		final JobID jobId = new JobID();
		final String jobName = "Thesis test sample";
		final Configuration cfg = new Configuration();

		System.out.println("test start");
		// construct part one of the execution graph
		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");

		v1.setLocation("Europe");
		v2.setLocation("Europe");
		v3.setLocation("Asia");

		v1.setParallelism(3);
		v2.setParallelism(3);
		v3.setParallelism(3);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);

		// this creates an intermediate result for v1
		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		// create results for v2 and v3
		IntermediateDataSet v2result = v2.createAndAddResultDataSet(ResultPartitionType.PIPELINED);
		IntermediateDataSet v3result = v3.createAndAddResultDataSet(ResultPartitionType.PIPELINED);

		SlotSharingGroup ss = new SlotSharingGroup();
		v1.setSlotSharingGroup(ss);
		v2.setSlotSharingGroup(ss);

		v2.setStrictlyCoLocatedWith(v1);
		v1.setStrictlyCoLocatedWith(v2);

		//List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3));

		JobGraph jg = new JobGraph(jobId, jobName, v1, v2, v3);
		jg.setScheduleMode(ScheduleMode.EAGER);
		jg.setAllowQueuedScheduling(true);

		//		ExecutionGraph eg = new ExecutionGraph(
//			TestingUtils.defaultExecutor(),
//			TestingUtils.defaultExecutor(),
//			jobId,
//			jobName,
//			cfg,
//			new SerializedValue<>(new ExecutionConfig()),
//			AkkaUtils.getDefaultTimeout(),
//			new NoRestartStrategy(),
//			slotProvider);
//		try {
//			eg.attachJobGraph(jg.getVerticesSortedTopologicallyFromSources());
//		}
//		catch (JobException e) {
//			e.printStackTrace();
//			fail("Job failed with exception: " + e.getMessage());
//		}


		JobVertex v4 = new JobVertex("vertex4");
		v4.setLocation("Asia");
		v4.setParallelism(3);


		v4.setInvokableClass(AbstractInvokable.class);

		v4.connectDataSetAsInput(v2result, DistributionPattern.ALL_TO_ALL);
		v4.connectDataSetAsInput(v3result, DistributionPattern.ALL_TO_ALL);

		v4.setSlotSharingGroup(ss);
		jg.addVertex(v4);
		CoLocationGroup group = new CoLocationGroup(v1, v2, v4);

		CoLocationConstraint constraint1 = group.getLocationConstraint(0);
		CoLocationConstraint constraint2 = group.getLocationConstraint(1);
		CoLocationConstraint constraint3 = group.getLocationConstraint(2);

		//mock TaskManagerLocations and TaskManager
		ResourceID resourceID1 = new ResourceID("a");
		ResourceID resourceID3 = new ResourceID("b");

		InetAddress address1 = mock(InetAddress.class);
		when(address1.getCanonicalHostName()).thenReturn("Asia");
		when(address1.getHostName()).thenReturn("Asia");
		when(address1.getHostAddress()).thenReturn("127.0.0.1");
		when(address1.getAddress()).thenReturn(new byte[] {127, 0, 0, 1} );

		InetAddress address2 = mock(InetAddress.class);
		when(address2.getCanonicalHostName()).thenReturn("Europe");
		when(address2.getHostName()).thenReturn("Europe");
		when(address2.getHostAddress()).thenReturn("192.168.0.1");
		when(address2.getAddress()).thenReturn(new byte[] {(byte) 192, (byte) 168, 0, 1} );

		TaskManagerLocation one = new TaskManagerLocation(resourceID1, address1, 19871);
		one.setLocation("Asia");
		TaskManagerLocation two = new TaskManagerLocation(resourceID3, address2, 10871);
		two.setLocation("Europe");

		SlotRequestId slotRequestId = new SlotRequestId();
		constraint1.setSlotRequestId(slotRequestId);
		constraint2.setSlotRequestId(slotRequestId);
		constraint3.setSlotRequestId(slotRequestId);

		constraint1.lockLocation(two);
		constraint2.lockLocation(two);
		constraint3.lockLocation(two);

		v1.updateCoLocationGroup(group);
		v2.updateCoLocationGroup(group);
		v4.updateCoLocationGroup(group);
		TaskSlotTable taskSlotTable1 = new TaskSlotTable(Collections.singleton(ResourceProfile.UNKNOWN), timerService);
		TaskSlotTable taskSlotTable2 = new TaskSlotTable(Collections.singleton(ResourceProfile.UNKNOWN), timerService);

		JobLeaderService jobLeaderService1 = new JobLeaderService(one, RetryingRegistrationConfiguration.defaultConfiguration());
		JobLeaderService jobLeaderService2 = new JobLeaderService(one, RetryingRegistrationConfiguration.defaultConfiguration());

		IOManager ioManager = new IOManagerAsync(tmp.newFolder().getAbsolutePath());

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			ioManager.getSpillingDirectories(),
			Executors.directExecutor());

		MemoryManager memoryManager = new MemoryManager(
			4096,
			1,
			4096,
			MemoryType.HEAP,
			false);

		nettyShuffleEnvironment.start();

		KvStateService kvStateService = new KvStateService(new KvStateRegistry(), null, null);
		kvStateService.start();

		TaskManagerServices taskManagerServices1 = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(one)
			.setMemoryManager(memoryManager)
			.setIoManager(ioManager)
			.setShuffleEnvironment(nettyShuffleEnvironment)
			.setKvStateService(kvStateService)
			.setTaskSlotTable(taskSlotTable1)
			.setJobLeaderService(jobLeaderService1)
			.setTaskStateManager(localStateStoresManager)
			.build();

		TaskManagerServices taskManagerServices2 = new TaskManagerServicesBuilder()
			.setTaskManagerLocation(two)
			.setMemoryManager(memoryManager)
			.setIoManager(ioManager)
			.setShuffleEnvironment(nettyShuffleEnvironment)
			.setKvStateService(kvStateService)
			.setTaskSlotTable(taskSlotTable2)
			.setJobLeaderService(jobLeaderService2)
			.setTaskStateManager(localStateStoresManager)
			.build();

		TaskExecutor taskManager1 = createTaskExecutor(taskManagerServices1);
		TaskExecutor taskManager2 = createTaskExecutor(taskManagerServices2);

		final CompletableFuture<LogicalSlot>[] futures1 = new CompletableFuture[3];
		final CompletableFuture<LogicalSlot>[] futures2 = new CompletableFuture[3];
		final CompletableFuture<LogicalSlot>[] futures3 = new CompletableFuture[3];
		final CompletableFuture<LogicalSlot>[] futures4 = new CompletableFuture[3];

		final SimpleAckingTaskManagerGateway europeTaskManagers = new SimpleAckingTaskManagerGateway();
		final SimpleAckingTaskManagerGateway asiaTaskManagers = new SimpleAckingTaskManagerGateway();

		final SimpleSlot[] slots = new SimpleSlot[12];
		//final SimpleSlot[] asiaSlots = new SimpleSlot[6];

		final TestingSlotOwner slotOwner = new TestingSlotOwner();

		for (int i = 0, j=6; i < 6; i++, j++) {

			slots[i] = new SimpleSlot(slotOwner, two, i, europeTaskManagers);
			slots[j] = new SimpleSlot(slotOwner, one, j, asiaTaskManagers);
		}

		for (int i = 0; i < 3; i++) {
			futures1[i] = new CompletableFuture<>();
			futures2[i] = new CompletableFuture<>();
			futures3[i] = new CompletableFuture<>();
			futures4[i] = new CompletableFuture<>();
		}

		ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(3);
		slotProvider.addSlots(v1.getID(), futures1);
		slotProvider.addSlots(v2.getID(), futures2);
		slotProvider.addSlots(v3.getID(), futures3);
		slotProvider.addSlots(v4.getID(), futures4);



		final ExecutionGraph eg = createExecutionGraph(jg, slotProvider);

		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		eg.scheduleForExecution();


//		ExecutionJobVertex executionJobVertex1 = eg.getJobVertex(v1.getID());
//		ExecutionJobVertex executionJobVertex2 = eg.getJobVertex(v2.getID());
//		ExecutionJobVertex executionJobVertex3 = eg.getJobVertex(v3.getID());
//		ExecutionJobVertex executionJobVertex4 = eg.getJobVertex(v4.getID());
//
//		Execution execution = executionJobVertex1.getTaskVertices()[0].getCurrentExecutionAttempt();
//
//		CompletableFuture<Execution> allocationFuture = execution.allocateResourcesForExecution(
//			eg.getSlotProviderStrategy(),
//			LocationPreferenceConstraint.ALL,
//			Collections.emptySet());
//

	}

	private ExecutionGraph createExecutionGraph(JobGraph jobGraph, SlotProvider slotProvider) throws Exception {
		return createExecutionGraph(jobGraph, slotProvider, Time.minutes(10));
	}

	private ExecutionGraph createExecutionGraph(JobGraph jobGraph, SlotProvider slotProvider, Time timeout) throws Exception {
		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			new Configuration(),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			slotProvider,
			getClass().getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			timeout,
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			VoidBlobWriter.getInstance(),
			timeout,
			log,
			NettyShuffleMaster.INSTANCE,
			NoOpPartitionTracker.INSTANCE);
	}

	@Nonnull
	private TaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices) {
		return createTaskExecutor(taskManagerServices, HEARTBEAT_SERVICES);
	}

	private TaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices, HeartbeatServices heartbeatServices) {
		return new TaskExecutor(
			rpc,
			TaskManagerConfiguration.fromConfiguration(configuration),
			haServices,
			taskManagerServices,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			dummyBlobCacheService,
			testingFatalErrorHandler,
			new PartitionTable<>());
	}
}

