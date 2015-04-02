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

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class CassandraExecutorTest {
    CassandraExecutor executor;
    MockExecutorDriver driver;
    TestObjectFactory objectFactory;
    Protos.TaskID taskIdExecutor;
    Protos.TaskID taskIdMetadata;
    Protos.TaskID taskIdServer;

    @Test
    public void testStartup() throws Exception {
        cleanState();

        startServer();
    }

    @Test
    public void testTerminate() throws Exception {
        cleanState();

        startServer();

        shutdownServer();

        terminateExecutor();
    }

    @Test
    public void testShutdownAndRestart() throws Exception {
        cleanState();

        startServer();

        shutdownServer();

        startServer();

        shutdownServer();
    }

    @Test
    public void testExecutorRepair() throws Exception {
        cleanState();

        startServer();

        repairJob();

        repairJob();
    }

    @Test
    public void testExecutorCleanup() throws Exception {
        cleanState();

        startServer();

        cleanupJob();

        cleanupJob();
    }

    @Test
    public void testExecutorRepairCleanup() throws Exception {
        cleanState();

        startServer();

        repairJob();

        cleanupJob();

        repairJob();

        cleanupJob();
    }

    private void cleanupJob() {
        CassandraFrameworkProtos.ClusterJobType jobType = CassandraFrameworkProtos.ClusterJobType.CLEANUP;

        assertNull(executor.getCurrentJob());
        assertTrue(objectFactory.storageServiceProxy.listeners.isEmpty());

        Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(driver.executorInfo.getExecutorId().getValue() + '.' + jobType).build();
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        assertEquals(0, executor.getCurrentJob().getRemainingKeyspaces().size());
        assertEquals(3, executor.getCurrentJob().getKeyspaceStatus().size());
        assertTrue(executor.getCurrentJob().isFinished());

        driver.frameworkMessage(CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS)
                .build());
        List<CassandraFrameworkProtos.SlaveStatusDetails> messages = driver.frameworkMessages();
        assertEquals(1, messages.size());
        assertTrue(messages.get(0).hasNodeJobStatus());
        List<Protos.TaskStatus> taskStatusList = driver.taskStatusList();
        assertEquals(1, taskStatusList.size());
        assertEquals(taskId, taskStatusList.get(0).getTaskId());
        assertEquals(Protos.TaskState.TASK_FINISHED, taskStatusList.get(0).getState());

        assertNull(executor.getCurrentJob());
        assertTrue(objectFactory.storageServiceProxy.listeners.isEmpty());
    }

    private void repairJob() {
        CassandraFrameworkProtos.ClusterJobType jobType = CassandraFrameworkProtos.ClusterJobType.REPAIR;

        assertNull(executor.getCurrentJob());
        assertTrue(objectFactory.storageServiceProxy.listeners.isEmpty());

        Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(driver.executorInfo.getExecutorId().getValue() + '.' + jobType).build();
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
        assertEquals(1, messages.size());
        assertTrue(messages.get(0).hasNodeJobStatus());
        assertTrue(driver.taskStatusList().isEmpty());

        assertNotNull(executor.getCurrentJob());
        // MBean emits system KS + 3 other KS; one KS is currently processing; 2 KS remaining
        assertEquals(2, executor.getCurrentJob().getRemainingKeyspaces().size());
        assertFalse(executor.getCurrentJob().isFinished());
        assertFalse(objectFactory.storageServiceProxy.listeners.isEmpty());

        objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.STARTED);
        // SESSION_SUCCESS is called for each repaired range (one KS/CF repair usually contains a lot of ranges)
        // Just simulate that here.
        objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.SESSION_SUCCESS);
        objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.SESSION_SUCCESS);
        objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.SESSION_SUCCESS);
        objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.FINISHED);

        assertEquals(1, executor.getCurrentJob().getRemainingKeyspaces().size());
        assertEquals(1, executor.getCurrentJob().getKeyspaceStatus().size());
        assertFalse(executor.getCurrentJob().isFinished());

        objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.STARTED);
        objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.FINISHED);

        assertEquals(0, executor.getCurrentJob().getRemainingKeyspaces().size());
        assertEquals(2, executor.getCurrentJob().getKeyspaceStatus().size());
        assertFalse(executor.getCurrentJob().isFinished());

        driver.frameworkMessage(CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS)
                .build());
        messages = driver.frameworkMessages();
        assertEquals(1, messages.size());
        assertTrue(messages.get(0).hasNodeJobStatus());
        assertTrue(driver.taskStatusList().isEmpty());

        objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.STARTED);
        objectFactory.storageServiceProxy.emitRepairNotification(ActiveRepairService.Status.FINISHED);

        assertEquals(0, executor.getCurrentJob().getRemainingKeyspaces().size());
        assertEquals(3, executor.getCurrentJob().getKeyspaceStatus().size());
        assertTrue(executor.getCurrentJob().isFinished());
        assertTrue(objectFactory.storageServiceProxy.listeners.isEmpty());

        driver.frameworkMessage(CassandraFrameworkProtos.TaskDetails.newBuilder()
                .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS)
                .build());
        messages = driver.frameworkMessages();
        assertEquals(1, messages.size());
        assertTrue(messages.get(0).hasNodeJobStatus());
        List<Protos.TaskStatus> taskStatusList = driver.taskStatusList();
        assertEquals(1, taskStatusList.size());
        assertEquals(taskId, taskStatusList.get(0).getTaskId());
        assertEquals(Protos.TaskState.TASK_FINISHED, taskStatusList.get(0).getState());

        assertNull(executor.getCurrentJob());
        assertTrue(objectFactory.storageServiceProxy.listeners.isEmpty());
    }

    private void startServer() throws com.google.protobuf.InvalidProtocolBufferException {
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
        CassandraFrameworkProtos.SlaveStatusDetails slaveStatus = CassandraFrameworkProtos.SlaveStatusDetails.parseFrom(taskStatus.get(1).getData());
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

        driver.frameworkMessage(CassandraFrameworkProtos.TaskDetails.newBuilder()
            .setType(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.HEALTH_CHECK)
            .build());

        taskStatus = driver.taskStatusList();
        assertEquals(0, taskStatus.size());

        List<CassandraFrameworkProtos.SlaveStatusDetails> slaveStatusDetailsList = driver.frameworkMessages();
        assertEquals(1, slaveStatusDetailsList.size());
        assertTrue(slaveStatusDetailsList.get(0).hasHealthCheckDetails());
        assertTrue(slaveStatusDetailsList.get(0).getHealthCheckDetails().getHealthy());
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
                } catch (InterruptedException e) {
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

        List<CassandraFrameworkProtos.SlaveStatusDetails> slaveStatusDetailsList = driver.frameworkMessages();
        assertEquals(1, slaveStatusDetailsList.size());
        assertTrue(slaveStatusDetailsList.get(0).hasHealthCheckDetails());
        assertFalse(slaveStatusDetailsList.get(0).getHealthCheckDetails().getHealthy());
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

        List<CassandraFrameworkProtos.SlaveStatusDetails> slaveStatusDetailsList = driver.frameworkMessages();
        assertEquals(0, slaveStatusDetailsList.size());
    }

    private List<Protos.TaskStatus> taskStartingRunning(Protos.TaskID taskId) {
        List<Protos.TaskStatus> taskStatus = driver.taskStatusList();
        assertEquals(2, taskStatus.size());
        assertEquals(Protos.TaskState.TASK_STARTING, taskStatus.get(0).getState());
        assertEquals(taskId, taskStatus.get(0).getTaskId());
        assertEquals(Protos.TaskState.TASK_RUNNING, taskStatus.get(1).getState());
        assertEquals(taskId, taskStatus.get(1).getTaskId());
        return taskStatus;
    }

    private void cleanState() {
        objectFactory = new TestObjectFactory();
        executor = new CassandraExecutor(objectFactory);
        driver = new MockExecutorDriver(executor, Protos.ExecutorID.newBuilder().setValue("executor").build());
        taskIdExecutor = Protos.TaskID.newBuilder().setValue("executor").build();
        taskIdMetadata = Protos.TaskID.newBuilder().setValue("executor.metadata").build();
        taskIdServer = Protos.TaskID.newBuilder().setValue("executor.server").build();
    }

}
