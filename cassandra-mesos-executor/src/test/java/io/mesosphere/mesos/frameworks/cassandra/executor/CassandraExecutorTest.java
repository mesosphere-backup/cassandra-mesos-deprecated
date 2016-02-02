/**
 *    Copyright (C) 2015 Mesosphere, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mesosphere.mesos.frameworks.cassandra.executor;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.JmxConnect;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventType;
import org.apache.cassandra.utils.progress.jmx.JMXProgressSupport;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Marker;

import javax.management.ListenerNotFoundException;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationListener;
import javax.management.NotificationFilter;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.FluentIterable.from;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

public class CassandraExecutorTest {

    /**
     * UUT
     */
    private CassandraExecutor executor;

    // TODO: This really should be mocked with mockito, but since the code already exists I
    // didn't see a reason to replace it for now
    private MockExecutorDriver driver;

    @Mock
    private ObjectFactory objectFactory;

    @Mock
    private JmxConnect jmxConnect;

    @Mock
    private StorageServiceMBean storageServiceProxy;

    // Counts the # of listeners for the storageServiceProxy
    private AtomicInteger listenerCount = new AtomicInteger();
    private NotificationBroadcasterSupport notificationBroadcaster = new NotificationBroadcasterSupport();
    private JMXProgressSupport progressEmitter = new JMXProgressSupport(notificationBroadcaster);

    private SequenceAnswer<Integer> repairCommandSeq = new SequenceAnswer<Integer>(1, x -> x + 1);

    @Mock
    private EndpointSnitchInfoMBean endpointSnitchInfoProxy;

    @NotNull
    private final Protos.TaskID taskIdExecutor;

    @NotNull
    private final Protos.TaskID taskIdMetadata;

    @NotNull
    private final Protos.TaskID taskIdServer;

    /**
     * Every JUnit tests is ran in a new instance of this class, therefore we can initialize some constant things in this constructor
     */
    public CassandraExecutorTest() {
        taskIdExecutor = Protos.TaskID.newBuilder().setValue("executor").build();
        taskIdMetadata = Protos.TaskID.newBuilder().setValue("executor.metadata").build();
        taskIdServer = Protos.TaskID.newBuilder().setValue("executor.server").build();
    }

    /**
     * Functional interface to mockito answers allowing easy and
     * readable implementation of returning a determinable sequence
     * of things
     */
    private static class SequenceAnswer<T> implements Answer<T> {
        private T runner;
        private Function<T, T> nextFunction;

        /**
         * Constructs a new SequenceAnswer with a given initial value and
         * a mapping function
         */
        public SequenceAnswer(T firstValue, Function<T, T> nextFunction) {
            this.runner = firstValue;
            this.nextFunction = nextFunction;
        }

        /**
         * Gets the current runner value
         */
        public T getRunner() {
            return runner;
        }

        @Override
        public T answer(InvocationOnMock invocation) {
            T temp = runner;
            runner = nextFunction.apply(runner);
            return temp;
        }
    }

    /**
     * Variadic version of java.util.Arrays.stream
     *
     * @see java.util.Arrays.stream
     */
    private static <T> Stream<T> arrayStream(T... a) {
        return Arrays.stream(a);
    }

    /**
     * Convenience function for emitting repair notifications
     */
    private void emitRepairNotification(ProgressEvent evt) {
        String tag = "repair:" + (repairCommandSeq.getRunner() - 1);
        progressEmitter.progress(tag, evt);
    }

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
        executor = new CassandraExecutor(objectFactory);
        driver = new MockExecutorDriver(executor, Protos.ExecutorID.newBuilder().setValue("executor").build());

        // Used in custom counter answers
        StorageServiceMBean storageServiceProxy2 = mock(StorageServiceMBean.class);

        try {
            when(storageServiceProxy.getOperationMode())
                .thenReturn("NORMAL");
            when(storageServiceProxy.isGossipRunning())
                .thenReturn(true);
            when(storageServiceProxy.isJoined())
                .thenReturn(true);
            when(storageServiceProxy.isNativeTransportRunning())
                .thenReturn(true);
            when(storageServiceProxy.isRPCServerRunning())
                .thenReturn(true);
            when(storageServiceProxy.isInitialized())
                .thenReturn(true);
            when(storageServiceProxy.getReleaseVersion())
                .thenReturn("5.6.7");
            when(storageServiceProxy.getClusterName())
                .thenReturn("mocked-unit-test");
            when(storageServiceProxy.getTokens())
                .thenReturn(Arrays.asList("1", "2"));
            when(storageServiceProxy.getTokens(anyString()))
                .thenReturn(Arrays.asList("1", "2"));
            when(storageServiceProxy.getTokenToEndpointMap())
                .thenReturn(Collections.singletonMap("1", "1.2.3.4"));
            when(storageServiceProxy.getLocalHostId())
                .thenReturn(UUID.randomUUID().toString());
            List<String> keyspaces = Arrays.asList("system", "foo", "bar", "baz");
            when(storageServiceProxy.getKeyspaces())
                .thenReturn(keyspaces);
            when(storageServiceProxy.forceRepairAsync(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), anyBoolean(), anyVararg()))
                .thenAnswer(repairCommandSeq);

            // Implement a counter on the # of listeners
            doAnswer(new Answer<Object>() {
                @Override
                public Object answer(InvocationOnMock invocation) {
                    NotificationListener listener = invocation.getArgumentAt(0, NotificationListener.class);
                    NotificationFilter filter = invocation.getArgumentAt(1, NotificationFilter.class);
                    Object o = invocation.getArgumentAt(2, Object.class);
                    notificationBroadcaster.addNotificationListener(listener, filter, o);
                    listenerCount.getAndIncrement();
                    return null;
                }
            }).when(storageServiceProxy).addNotificationListener(any(NotificationListener.class), any(NotificationFilter.class), anyObject());
            Answer<Object> dAnswer = new Answer<Object>() {
                @Override
                public Object answer(InvocationOnMock invocation) {
                    NotificationListener listener = invocation.getArgumentAt(0, NotificationListener.class);
                    try {
                        notificationBroadcaster.removeNotificationListener(listener);
                    } catch (ListenerNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                    listenerCount.getAndDecrement();
                    return null;
                }
            };
            doAnswer(dAnswer)
                .when(storageServiceProxy)
                .removeNotificationListener(any(NotificationListener.class));
            doAnswer(dAnswer)
                .when(storageServiceProxy)
                .removeNotificationListener(any(NotificationListener.class), any(NotificationFilter.class), anyObject());


            when(endpointSnitchInfoProxy.getRack(anyString()))
                .thenReturn("rack");
            when(endpointSnitchInfoProxy.getDatacenter(anyString()))
                .thenReturn("datacenter"); 
            when(endpointSnitchInfoProxy.getSnitchName())
                .thenReturn("mock-snitch");

            when(jmxConnect.getRuntimeProxy())
                .thenReturn(ManagementFactory.getRuntimeMXBean());
            when(jmxConnect.getStorageServiceProxy())
                .thenReturn(storageServiceProxy);
            when(jmxConnect.getEndpointSnitchInfoProxy())
                .thenReturn(endpointSnitchInfoProxy);
            for (String keyspace : keyspaces) {
                when(jmxConnect.getColumnFamilyNames(keyspace))
                    .thenReturn(arrayStream("a", "b", "c")
                            .map(cf -> keyspace + "_" + cf)
                            .collect(Collectors.toList()));
            }

            when(objectFactory.newJmxConnect((CassandraFrameworkProtos.JmxConnect)notNull()))
                .thenReturn(jmxConnect);
            when(objectFactory.launchCassandraNodeTask((Marker)notNull(), (CassandraFrameworkProtos.CassandraServerRunTask)notNull()))
                .thenReturn(new MockWrappedProcess(42))
                .thenReturn(new MockWrappedProcess(43));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testStartup() throws Exception {
        startServer();
    }

    @Test
    public void testTerminate() throws Exception {
        startServer();

        shutdownServer();

        terminateExecutor();
    }

    @Test
    public void testShutdownAndRestart() throws Exception {
        startServer();

        shutdownServer();

        startServer();

        shutdownServer();
    }

    @Test
    public void testExecutorRepair() throws Exception {
        startServer();

        repairJob();

        repairJob();
    }

    @Test
    public void testExecutorCleanup() throws Exception {
        startServer();

        cleanupJob();

        cleanupJob();
    }

    @Test
    public void testExecutorRepairCleanup() throws Exception {
        startServer();

        repairJob();

        cleanupJob();

        repairJob();

        cleanupJob();
    }

    private void cleanupJob() {
        final CassandraFrameworkProtos.ClusterJobType jobType = CassandraFrameworkProtos.ClusterJobType.CLEANUP;

        assertNull(executor.getCurrentJob());
        assertEquals(0, listenerCount.get());

        final Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(driver.executorInfo.getExecutorId().getValue() + '.' + jobType).build();
        driver.launchTask(
                taskId,
                Protos.CommandInfo.getDefaultInstance(),
                CassandraFrameworkProtos.TaskDetails.newBuilder()
                        .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB)
                        .setNodeJobTask(CassandraFrameworkProtos.NodeJobTask.newBuilder()
                                .setJobType(jobType))
                        .build(),
                "node job task",
                Collections.<Protos.Resource>emptyList());

        taskStartingRunning(taskId);

        try {
            Thread.sleep(50L);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertEquals(0, executor.getCurrentJob().getRemainingKeyspaces().size());
        assertEquals(3, executor.getCurrentJob().getKeyspaceStatus().size());
        assertTrue(executor.getCurrentJob().isFinished());

        driver.frameworkMessage(CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS)
                .build());
        final List<CassandraFrameworkProtos.SlaveStatusDetails> messages = driver.frameworkMessages();
        final FluentIterable<CassandraFrameworkProtos.SlaveStatusDetails> nodeJobStatusMessages = from(messages)
            .filter(new Predicate<CassandraFrameworkProtos.SlaveStatusDetails>() {
                @Override
                public boolean apply(final CassandraFrameworkProtos.SlaveStatusDetails input) {
                    return input.getStatusDetailsType() == CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS;
                }
            });
        assertEquals(1, nodeJobStatusMessages.size());
        assertTrue(nodeJobStatusMessages.get(0).hasNodeJobStatus());
        final List<Protos.TaskStatus> taskStatusList = driver.taskStatusList();
        assertEquals(1, taskStatusList.size());
        assertEquals(taskId, taskStatusList.get(0).getTaskId());
        assertEquals(Protos.TaskState.TASK_FINISHED, taskStatusList.get(0).getState());
        assertEquals(0, driver.taskStatusList().size());

        assertNull(executor.getCurrentJob());
        assertEquals(0, listenerCount.get());
    }

    private void repairJob() {
        final CassandraFrameworkProtos.ClusterJobType jobType = CassandraFrameworkProtos.ClusterJobType.REPAIR;

        assertNull(executor.getCurrentJob());
        assertEquals(0, listenerCount.get());

        final Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(driver.executorInfo.getExecutorId().getValue() + '.' + jobType).build();
        driver.launchTask(
            taskId,
            Protos.CommandInfo.getDefaultInstance(),
            CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB)
                .setNodeJobTask(CassandraFrameworkProtos.NodeJobTask.newBuilder()
                    .setJobType(jobType))
                .build(),
            "node job task",
            Collections.<Protos.Resource>emptyList());

        taskStartingRunning(taskId);

        driver.frameworkMessage(CassandraFrameworkProtos.TaskDetails.newBuilder()
            .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS)
            .build());
        List<CassandraFrameworkProtos.SlaveStatusDetails> messages = driver.frameworkMessages();
        final FluentIterable<CassandraFrameworkProtos.SlaveStatusDetails> nodeJobStatusMessages = from(messages)
            .filter(new Predicate<CassandraFrameworkProtos.SlaveStatusDetails>() {
                @Override
                public boolean apply(final CassandraFrameworkProtos.SlaveStatusDetails input) {
                    return input.getStatusDetailsType() == CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS;
                }
            });
        assertEquals(1, nodeJobStatusMessages.size());
        assertTrue(nodeJobStatusMessages.get(0).hasNodeJobStatus());
        assertEquals(0, driver.taskStatusList().size());

        assertNotNull(executor.getCurrentJob());
        // MBean emits system KS + 3 other KS; one KS is currently processing; 2 KS remaining
        assertEquals(2, executor.getCurrentJob().getRemainingKeyspaces().size());
        assertFalse(executor.getCurrentJob().isFinished());
        assertTrue(listenerCount.get() > 0);

        emitRepairNotification(new ProgressEvent(ProgressEventType.START, 0, 3));
        emitRepairNotification(new ProgressEvent(ProgressEventType.PROGRESS, 1, 3));
        emitRepairNotification(new ProgressEvent(ProgressEventType.PROGRESS, 2, 3));
        emitRepairNotification(new ProgressEvent(ProgressEventType.PROGRESS, 3, 3));
        emitRepairNotification(new ProgressEvent(ProgressEventType.SUCCESS, 3, 3));
        emitRepairNotification(new ProgressEvent(ProgressEventType.COMPLETE, 3, 3));
        //objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.STARTED);
        //// SESSION_SUCCESS is called for each repaired range (one KS/CF repair usually contains a lot of ranges)
        //// Just simulate that here.
        //objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.SESSION_SUCCESS);
        //objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.SESSION_SUCCESS);
        //objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.SESSION_SUCCESS);
        //objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.FINISHED);

        assertEquals(1, executor.getCurrentJob().getRemainingKeyspaces().size());
        assertEquals(1, executor.getCurrentJob().getKeyspaceStatus().size());
        assertFalse(executor.getCurrentJob().isFinished());

        emitRepairNotification(new ProgressEvent(ProgressEventType.START, 0, 0));
        emitRepairNotification(new ProgressEvent(ProgressEventType.SUCCESS, 0, 0));
        emitRepairNotification(new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
        //objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.STARTED);
        //objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.FINISHED);

        assertEquals(0, executor.getCurrentJob().getRemainingKeyspaces().size());
        assertEquals(2, executor.getCurrentJob().getKeyspaceStatus().size());
        assertFalse(executor.getCurrentJob().isFinished());

        driver.frameworkMessage(CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS)
                .build());
        messages = driver.frameworkMessages();
        assertTrue(messages.size() >= 1);
        assertTrue(messages.get(0).hasNodeJobStatus());
        assertTrue(driver.taskStatusList().isEmpty());

        emitRepairNotification(new ProgressEvent(ProgressEventType.START, 0, 0));
        emitRepairNotification(new ProgressEvent(ProgressEventType.SUCCESS, 0, 0));
        emitRepairNotification(new ProgressEvent(ProgressEventType.COMPLETE, 0, 0));
        //objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.STARTED);
        //objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.FINISHED);

        assertEquals(0, executor.getCurrentJob().getRemainingKeyspaces().size());
        assertEquals(3, executor.getCurrentJob().getKeyspaceStatus().size());
        assertTrue(executor.getCurrentJob().isFinished());
        assertEquals(0, listenerCount.get());

        driver.frameworkMessage(CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS)
                .build());
        messages = driver.frameworkMessages();
        assertEquals(1, messages.size());
        assertTrue(messages.get(0).hasNodeJobStatus());
        final List<Protos.TaskStatus> taskStatusList = driver.taskStatusList();
        assertEquals(1, taskStatusList.size());
        assertEquals(taskId, taskStatusList.get(0).getTaskId());
        assertEquals(Protos.TaskState.TASK_FINISHED, taskStatusList.get(0).getState());

        assertNull(executor.getCurrentJob());
        assertEquals(0, listenerCount.get());
    }

    private void startServer() throws com.google.protobuf.InvalidProtocolBufferException, InterruptedException {
        driver.normalRegister();

        driver.launchTask(
            taskIdMetadata,
            Protos.CommandInfo.getDefaultInstance(),
            CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.EXECUTOR_METADATA)
                .setExecutorMetadataTask(CassandraFrameworkProtos.ExecutorMetadataTask.newBuilder()
                    .setExecutorId(driver.executorInfo.getExecutorId().getValue())
                    .setIp("1.2.3.4")
                    .build())
                .build(),
            "metadata task",
            Collections.<Protos.Resource>emptyList());

        List<Protos.TaskStatus> taskStatus = taskStartingRunning(taskIdMetadata);
        final CassandraFrameworkProtos.SlaveStatusDetails slaveStatus = CassandraFrameworkProtos.SlaveStatusDetails.parseFrom(taskStatus.get(1).getData());
        assertNotNull(slaveStatus);

        driver.launchTask(
            taskIdServer,
            Protos.CommandInfo.getDefaultInstance(),
            CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN)
                .setCassandraServerRunTask(CassandraFrameworkProtos.CassandraServerRunTask.newBuilder()
                    .setVersion("2.1.4")
                    .addCommand("somewhere")
                    .setCassandraServerConfig(CassandraFrameworkProtos.CassandraServerConfig.newBuilder()
                        .setCassandraYamlConfig(CassandraFrameworkProtos.TaskConfig.newBuilder())
                        .setTaskEnv(CassandraFrameworkProtos.TaskEnv.newBuilder()))
                    .setJmx(CassandraFrameworkProtos.JmxConnect.newBuilder()
                        .setIp("1.2.3.4")
                        .setJmxPort(42))
                    .setHealthCheckIntervalSeconds(Duration.standardMinutes(5).getStandardSeconds())
                    .build())
                .build(),
            "server task",
            Collections.<Protos.Resource>emptyList());

        taskStatus = driver.taskStatusList();
        assertEquals(2, taskStatus.size());
        assertEquals(taskIdServer, taskStatus.get(0).getTaskId());
        assertEquals(Protos.TaskState.TASK_STARTING, taskStatus.get(0).getState());
        assertEquals(taskIdServer, taskStatus.get(1).getTaskId());
        assertEquals(Protos.TaskState.TASK_RUNNING, taskStatus.get(1).getState());

        taskStatus = driver.taskStatusList();
        assertEquals(0, taskStatus.size());

        //TODO: Make async friendly
        final List<CassandraFrameworkProtos.SlaveStatusDetails> slaveStatusDetailsList = driver.frameworkMessages();
        if (!slaveStatusDetailsList.isEmpty()) {
            assertTrue(slaveStatusDetailsList.get(0).hasHealthCheckDetails());
            assertTrue(slaveStatusDetailsList.get(0).getHealthCheckDetails().getHealthy());
        }
    }

    private void shutdownServer() {
        List<Protos.TaskStatus> taskStatus = driver.taskStatusList();
        assertEquals(0, taskStatus.size());

        driver.killTask(taskIdServer);

        for (int i = 0; i < 50; i++) {
            taskStatus = driver.taskStatusList();
            if (taskStatus.isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                break;
            }
        }
        assertEquals(1, taskStatus.size());
        // server task finished...
        assertEquals(taskIdServer, taskStatus.get(0).getTaskId());
        assertEquals(Protos.TaskState.TASK_FINISHED, taskStatus.get(0).getState());
    }

    private void terminateExecutor() {
        List<Protos.TaskStatus> taskStatus = driver.taskStatusList();
        assertEquals(0, taskStatus.size());

        driver.killTask(taskIdExecutor);

        taskStatus = driver.taskStatusList();
        assertEquals(1, taskStatus.size());
        // server task finished...
        assertEquals(taskIdExecutor, taskStatus.get(0).getTaskId());
        assertEquals(Protos.TaskState.TASK_FINISHED, taskStatus.get(0).getState());

        //TODO: Make async friendly
//        final List<CassandraFrameworkProtos.SlaveStatusDetails> slaveStatusDetailsList = driver.frameworkMessages();
//        assertEquals(0, slaveStatusDetailsList.size());
    }

    private List<Protos.TaskStatus> taskStartingRunning(final Protos.TaskID taskId) {
        final List<Protos.TaskStatus> taskStatus = driver.taskStatusList();
        assertEquals(2, taskStatus.size());
        assertEquals(Protos.TaskState.TASK_STARTING, taskStatus.get(0).getState());
        assertEquals(taskId, taskStatus.get(0).getTaskId());
        assertEquals(Protos.TaskState.TASK_RUNNING, taskStatus.get(1).getState());
        assertEquals(taskId, taskStatus.get(1).getTaskId());
        return taskStatus;
    }

}
