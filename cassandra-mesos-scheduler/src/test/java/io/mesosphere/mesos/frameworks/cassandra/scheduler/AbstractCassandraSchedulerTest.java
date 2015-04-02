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
package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.util.ProtoUtils;
import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public abstract class AbstractCassandraSchedulerTest extends AbstractSchedulerTest {
    protected CassandraScheduler scheduler;
    protected MockSchedulerDriver driver;

    protected Protos.TaskInfo[] executorMetadata;
    protected Tuple2<Protos.TaskInfo, CassandraFrameworkProtos.TaskDetails>[] executorServer;

    protected void partiallyFailingClusterJob(CassandraFrameworkProtos.ClusterJobType clusterJobType) throws InvalidProtocolBufferException {
        CassandraFrameworkProtos.ClusterJobStatus currentClusterJob = cluster.getCurrentClusterJob();
        assertNull(currentClusterJob);
        for (CassandraFrameworkProtos.ClusterJobType jobType : CassandraFrameworkProtos.ClusterJobType.values()) {
            assertNull(cluster.getCurrentClusterJob(jobType));
        }

        // simulate API call
        assertTrue(cluster.startClusterTask(clusterJobType));

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);
        for (CassandraFrameworkProtos.ClusterJobType jobType : CassandraFrameworkProtos.ClusterJobType.values()) {
            if (jobType == clusterJobType) {
                assertNotNull(cluster.getCurrentClusterJob(jobType));
            } else {
                assertNull(cluster.getCurrentClusterJob(jobType));
                assertFalse(cluster.startClusterTask(jobType));
            }
        }

        assertFalse(currentClusterJob.hasCurrentNode());
        assertEquals(3, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());

        // launch job on a node
        Protos.TaskInfo taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        Protos.TaskInfo taskInfo1 = taskInfo;
        assertNotNull(taskInfo);
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        // no other slave must produce a task
        Tuple2<Protos.SlaveID, String> currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            if (!slave._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(slave, 3);
            else
                currentSlave = slave;
        }
        assertNotNull(currentSlave);

        // check cluster job
        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);

        assertTrue(currentClusterJob.hasCurrentNode());
        assertEquals(executorIdValue(taskInfo), currentClusterJob.getCurrentNode().getExecutorId());
        assertEquals(clusterJobType, currentClusterJob.getCurrentNode().getJobType());
        assertTrue(currentClusterJob.getCurrentNode().hasStartedTimestamp());
        assertFalse(currentClusterJob.getCurrentNode().hasFinishedTimestamp());
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());

        // check job status submit

        CassandraFrameworkProtos.TaskDetails taskDetails = submitTask(currentSlave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS);
        assertNotNull(taskDetails);

        // simulate job status response

        CassandraFrameworkProtos.NodeJobStatus nodeJobStatus = initialNodeJobStatus(taskInfo, clusterJobType);

        scheduler.frameworkMessage(driver,
                Protos.ExecutorID.newBuilder().setValue(currentClusterJob.getCurrentNode().getExecutorId()).build(),
                currentSlave._1,
                CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                        .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS)
                        .setNodeJobStatus(nodeJobStatus)
                        .build().toByteArray());

        // check cluster job after 1st response

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);

        assertTrue(currentClusterJob.hasCurrentNode());
        assertEquals(executorIdValue(taskInfo), currentClusterJob.getCurrentNode().getExecutorId());
        assertEquals(clusterJobType, currentClusterJob.getCurrentNode().getJobType());
        assertTrue(currentClusterJob.getCurrentNode().hasStartedTimestamp());
        assertFalse(currentClusterJob.getCurrentNode().hasFinishedTimestamp());
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());
        //
        // we cannot compare this one:  assertEquals(nodeJobStatus.getStartedTimestamp(), currentClusterJob.getCurrentNode().getStartedTimestamp());
        assertEquals(Arrays.asList("foo", "bar", "baz"), currentClusterJob.getCurrentNode().getRemainingKeyspacesList());
        assertEquals(0, currentClusterJob.getCurrentNode().getProcessedKeyspacesCount());
        assertTrue(currentClusterJob.getCurrentNode().getRunning());
        assertEquals(taskIdValue(taskInfo), currentClusterJob.getCurrentNode().getTaskId());

        // node has finished ...

        taskDetails = submitTask(currentSlave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS);
        assertNotNull(taskDetails);

        finishJob(currentClusterJob, taskInfo, currentSlave, nodeJobStatus, clusterJobType);
        currentClusterJob = cluster.getCurrentClusterJob();

        // cluster job should have no current node yet

        assertNotNull(currentClusterJob);
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(1, currentClusterJob.getCompletedNodesCount());
        for (CassandraFrameworkProtos.NodeJobStatus jobStatus : currentClusterJob.getCompletedNodesList()) {
            if (jobStatus.getExecutorId().equals(executorIdValue(taskInfo))) {
                assertFalse(jobStatus.hasFailed());
                assertFalse(jobStatus.hasFailureMessage());
            }
        }
        assertFalse(currentClusterJob.hasCurrentNode());

        // 2nd node

        taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        assertNotNull(taskInfo);
        Protos.TaskInfo taskInfo2 = taskInfo;
        assertNotEquals(executorId(taskInfo), executorId(taskInfo1));
        assertNotEquals(taskInfo.getSlaveId(), taskInfo1.getSlaveId());
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        assertNotNull(taskInfo);
        currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            if (!slave._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(slave, 3);
            else
                currentSlave = slave;
        }
        assertNotNull(currentSlave);
        initialNodeJobStatus(taskInfo, clusterJobType);

        // ... just finish 2nd node

        executorTaskError(taskInfo);
        currentClusterJob = cluster.getCurrentClusterJob();

        assertNotNull(currentClusterJob);
        assertEquals(1, currentClusterJob.getRemainingNodesCount());
        assertEquals(2, currentClusterJob.getCompletedNodesCount());
        for (CassandraFrameworkProtos.NodeJobStatus jobStatus : currentClusterJob.getCompletedNodesList()) {
            if (jobStatus.getExecutorId().equals(executorIdValue(taskInfo))) {
                assertTrue(jobStatus.getFailed());
                assertFalse(jobStatus.getFailureMessage().isEmpty());
            }
        }
        assertFalse(currentClusterJob.hasCurrentNode());

        // 3rd node

        taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        assertNotNull(taskInfo);
        assertNotEquals(executorId(taskInfo), executorId(taskInfo1));
        assertNotEquals(taskInfo.getSlaveId(), taskInfo1.getSlaveId());
        assertNotEquals(executorId(taskInfo), executorId(taskInfo2));
        assertNotEquals(taskInfo.getSlaveId(), taskInfo2.getSlaveId());
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        assertNotNull(taskInfo);
        currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            if (!slave._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(slave, 3);
            else
                currentSlave = slave;
        }
        assertNotNull(currentSlave);
        nodeJobStatus = initialNodeJobStatus(taskInfo, clusterJobType);

        // ... just finish 3rd node

        finishJob(currentClusterJob, taskInfo, currentSlave, nodeJobStatus, clusterJobType);
        currentClusterJob = cluster.getCurrentClusterJob();

        // job finished

        assertNull(currentClusterJob);
        for (CassandraFrameworkProtos.ClusterJobType jobType : CassandraFrameworkProtos.ClusterJobType.values()) {
            assertNull(cluster.getCurrentClusterJob(jobType));
        }

        currentClusterJob = cluster.getLastClusterJob(clusterJobType);
        assertNotNull(currentClusterJob);

        assertFalse(currentClusterJob.hasCurrentNode());
        assertEquals(0, currentClusterJob.getRemainingNodesCount());
        assertEquals(3, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertTrue(currentClusterJob.hasFinishedTimestamp());

        // no tasks

        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            noopOnOffer(slave, activeNodes);
        }
    }

    protected void clusterJob(CassandraFrameworkProtos.ClusterJobType clusterJobType) throws InvalidProtocolBufferException {
        CassandraFrameworkProtos.ClusterJobStatus currentClusterJob = cluster.getCurrentClusterJob();
        assertNull(currentClusterJob);
        for (CassandraFrameworkProtos.ClusterJobType jobType : CassandraFrameworkProtos.ClusterJobType.values()) {
            assertNull(cluster.getCurrentClusterJob(jobType));
        }

        // simulate API call
        assertTrue(cluster.startClusterTask(clusterJobType));

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);
        for (CassandraFrameworkProtos.ClusterJobType jobType : CassandraFrameworkProtos.ClusterJobType.values()) {
            if (jobType == clusterJobType) {
                assertNotNull(cluster.getCurrentClusterJob(jobType));
            } else {
                assertNull(cluster.getCurrentClusterJob(jobType));
                assertFalse(cluster.startClusterTask(jobType));
            }
        }

        assertFalse(currentClusterJob.hasCurrentNode());
        assertEquals(3, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());

        // launch job on a node
        Protos.TaskInfo taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        Protos.TaskInfo taskInfo1 = taskInfo;
        assertNotNull(taskInfo);
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        // no other slave must produce a task
        Tuple2<Protos.SlaveID, String> currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            if (!slave._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(slave, 3);
            else
                currentSlave = slave;
        }
        assertNotNull(currentSlave);

        // check cluster job
        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);

        assertTrue(currentClusterJob.hasCurrentNode());
        assertEquals(executorIdValue(taskInfo), currentClusterJob.getCurrentNode().getExecutorId());
        assertEquals(clusterJobType, currentClusterJob.getCurrentNode().getJobType());
        assertTrue(currentClusterJob.getCurrentNode().hasStartedTimestamp());
        assertFalse(currentClusterJob.getCurrentNode().hasFinishedTimestamp());
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());

        // check job status submit

        CassandraFrameworkProtos.TaskDetails taskDetails = submitTask(currentSlave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS);
        assertNotNull(taskDetails);

        // simulate job status response

        CassandraFrameworkProtos.NodeJobStatus nodeJobStatus = initialNodeJobStatus(taskInfo, clusterJobType);

        scheduler.frameworkMessage(driver,
            Protos.ExecutorID.newBuilder().setValue(currentClusterJob.getCurrentNode().getExecutorId()).build(),
            currentSlave._1,
            CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS)
                .setNodeJobStatus(nodeJobStatus)
                .build().toByteArray());

        // check cluster job after 1st response

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);

        assertTrue(currentClusterJob.hasCurrentNode());
        assertEquals(executorIdValue(taskInfo), currentClusterJob.getCurrentNode().getExecutorId());
        assertEquals(clusterJobType, currentClusterJob.getCurrentNode().getJobType());
        assertTrue(currentClusterJob.getCurrentNode().hasStartedTimestamp());
        assertFalse(currentClusterJob.getCurrentNode().hasFinishedTimestamp());
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());
        //
        // we cannot compare this one:  assertEquals(nodeJobStatus.getStartedTimestamp(), currentClusterJob.getCurrentNode().getStartedTimestamp());
        assertEquals(Arrays.asList("foo", "bar", "baz"), currentClusterJob.getCurrentNode().getRemainingKeyspacesList());
        assertEquals(0, currentClusterJob.getCurrentNode().getProcessedKeyspacesCount());
        assertTrue(currentClusterJob.getCurrentNode().getRunning());
        assertEquals(taskIdValue(taskInfo), currentClusterJob.getCurrentNode().getTaskId());

        // node has finished ...

        taskDetails = submitTask(currentSlave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS);
        assertNotNull(taskDetails);

        finishJob(currentClusterJob, taskInfo, currentSlave, nodeJobStatus, clusterJobType);
        currentClusterJob = cluster.getCurrentClusterJob();

        // cluster job should have no current node yet

        assertNotNull(currentClusterJob);
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(1, currentClusterJob.getCompletedNodesCount());
        assertFalse(currentClusterJob.hasCurrentNode());

        // 2nd node

        taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        assertNotNull(taskInfo);
        Protos.TaskInfo taskInfo2 = taskInfo;
        assertNotEquals(executorId(taskInfo), executorId(taskInfo1));
        assertNotEquals(taskInfo.getSlaveId(), taskInfo1.getSlaveId());
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        assertNotNull(taskInfo);
        currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            if (!slave._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(slave, 3);
            else
                currentSlave = slave;
        }
        assertNotNull(currentSlave);
        nodeJobStatus = initialNodeJobStatus(taskInfo, clusterJobType);

        // ... just finish 2nd node

        finishJob(currentClusterJob, taskInfo, currentSlave, nodeJobStatus, clusterJobType);
        currentClusterJob = cluster.getCurrentClusterJob();

        assertNotNull(currentClusterJob);
        assertEquals(1, currentClusterJob.getRemainingNodesCount());
        assertEquals(2, currentClusterJob.getCompletedNodesCount());
        assertFalse(currentClusterJob.hasCurrentNode());

        // 3rd node

        taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        assertNotNull(taskInfo);
        assertNotEquals(executorId(taskInfo), executorId(taskInfo1));
        assertNotEquals(taskInfo.getSlaveId(), taskInfo1.getSlaveId());
        assertNotEquals(executorId(taskInfo), executorId(taskInfo2));
        assertNotEquals(taskInfo.getSlaveId(), taskInfo2.getSlaveId());
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        assertNotNull(taskInfo);
        currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            if (!slave._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(slave, 3);
            else
                currentSlave = slave;
        }
        assertNotNull(currentSlave);
        nodeJobStatus = initialNodeJobStatus(taskInfo, clusterJobType);

        // ... just finish 3rd node

        finishJob(currentClusterJob, taskInfo, currentSlave, nodeJobStatus, clusterJobType);
        currentClusterJob = cluster.getCurrentClusterJob();

        // job finished

        assertNull(currentClusterJob);

        currentClusterJob = cluster.getLastClusterJob(clusterJobType);
        assertNotNull(currentClusterJob);

        assertFalse(currentClusterJob.hasCurrentNode());
        assertEquals(0, currentClusterJob.getRemainingNodesCount());
        assertEquals(3, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertTrue(currentClusterJob.hasFinishedTimestamp());
        for (CassandraFrameworkProtos.NodeJobStatus jobStatus : currentClusterJob.getCompletedNodesList()) {
            assertEquals(clusterJobType, jobStatus.getJobType());
            assertEquals(3, jobStatus.getProcessedKeyspacesCount());
            assertEquals(0, jobStatus.getRemainingKeyspacesCount());
            assertFalse(jobStatus.getRunning());
        }

        // no tasks

        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            noopOnOffer(slave, activeNodes);
        }
    }

    protected void clusterJobFailingNode(CassandraFrameworkProtos.ClusterJobType clusterJobType) throws InvalidProtocolBufferException {
        CassandraFrameworkProtos.ClusterJobStatus currentClusterJob = cluster.getCurrentClusterJob();
        assertNull(currentClusterJob);
        for (CassandraFrameworkProtos.ClusterJobType jobType : CassandraFrameworkProtos.ClusterJobType.values()) {
            assertNull(cluster.getCurrentClusterJob(jobType));
        }

        // simulate API call
        assertTrue(cluster.startClusterTask(clusterJobType));

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);
        for (CassandraFrameworkProtos.ClusterJobType jobType : CassandraFrameworkProtos.ClusterJobType.values()) {
            if (jobType == clusterJobType) {
                assertNotNull(cluster.getCurrentClusterJob(jobType));
            } else {
                assertNull(cluster.getCurrentClusterJob(jobType));
                assertFalse(cluster.startClusterTask(jobType));
            }
        }

        assertFalse(currentClusterJob.hasCurrentNode());
        assertEquals(3, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());

        // launch job on a node
        Protos.TaskInfo taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        Protos.TaskInfo taskInfo1 = taskInfo;
        assertNotNull(taskInfo);
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        // no other slave must produce a task
        Tuple2<Protos.SlaveID, String> currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            if (!slave._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(slave, 3);
            else
                currentSlave = slave;
        }
        assertNotNull(currentSlave);

        // check cluster job
        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);

        assertTrue(currentClusterJob.hasCurrentNode());
        assertEquals(executorIdValue(taskInfo), currentClusterJob.getCurrentNode().getExecutorId());
        assertEquals(clusterJobType, currentClusterJob.getCurrentNode().getJobType());
        assertTrue(currentClusterJob.getCurrentNode().hasStartedTimestamp());
        assertFalse(currentClusterJob.getCurrentNode().hasFinishedTimestamp());
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());

        // check job status submit

        CassandraFrameworkProtos.TaskDetails taskDetails = submitTask(currentSlave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS);
        assertNotNull(taskDetails);

        // simulate job status response

        CassandraFrameworkProtos.NodeJobStatus nodeJobStatus = initialNodeJobStatus(taskInfo, clusterJobType);

        scheduler.frameworkMessage(driver,
            Protos.ExecutorID.newBuilder().setValue(currentClusterJob.getCurrentNode().getExecutorId()).build(),
            currentSlave._1,
            CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS)
                .setNodeJobStatus(nodeJobStatus)
                .build().toByteArray());

        // check cluster job after 1st response

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);

        assertTrue(currentClusterJob.hasCurrentNode());
        assertEquals(executorIdValue(taskInfo), currentClusterJob.getCurrentNode().getExecutorId());
        assertEquals(clusterJobType, currentClusterJob.getCurrentNode().getJobType());
        assertTrue(currentClusterJob.getCurrentNode().hasStartedTimestamp());
        assertFalse(currentClusterJob.getCurrentNode().hasFinishedTimestamp());
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());
        //
        // we cannot compare this one:  assertEquals(nodeJobStatus.getStartedTimestamp(), currentClusterJob.getCurrentNode().getStartedTimestamp());
        assertEquals(Arrays.asList("foo", "bar", "baz"), currentClusterJob.getCurrentNode().getRemainingKeyspacesList());
        assertEquals(0, currentClusterJob.getCurrentNode().getProcessedKeyspacesCount());
        assertTrue(currentClusterJob.getCurrentNode().getRunning());
        assertEquals(taskIdValue(taskInfo), currentClusterJob.getCurrentNode().getTaskId());

        // node 1 task failed
        CassandraFrameworkProtos.NodeJobStatus node = currentClusterJob.getCurrentNode();
        Tuple2<Protos.SlaveID, String> slave = slaveForNode(node);

        scheduler.statusUpdate(driver, Protos.TaskStatus.newBuilder()
            .setTimestamp(1)
            .setTaskId(Protos.TaskID.newBuilder().setValue("TASK"))
            .setSlaveId(slave._1)
            .setMessage("foo bar")
            .setExecutorId(Protos.ExecutorID.newBuilder().setValue(node.getExecutorId()))
            .setHealthy(true)
            .setSource(Protos.TaskStatus.Source.SOURCE_SLAVE)
            .setReason(Protos.TaskStatus.Reason.REASON_EXECUTOR_TERMINATED)
            .setState(Protos.TaskState.TASK_FINISHED)
            .build());

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);
        assertFalse(currentClusterJob.hasCurrentNode());

        launchTask(slave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.EXECUTOR_METADATA);

        // 2nd node

        taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        assertNotNull(taskInfo);
        Protos.TaskInfo taskInfo2 = taskInfo;
        assertNotEquals(executorId(taskInfo), executorId(taskInfo1));
        assertNotEquals(taskInfo.getSlaveId(), taskInfo1.getSlaveId());
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        assertNotNull(taskInfo);
        currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> s : slaves) {
            if (!s._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(s, 3);
            else
                currentSlave = s;
        }
        assertNotNull(currentSlave);
        nodeJobStatus = initialNodeJobStatus(taskInfo, clusterJobType);

        scheduler.frameworkMessage(driver,
            Protos.ExecutorID.newBuilder().setValue(currentClusterJob.getCurrentNode().getExecutorId()).build(),
            currentSlave._1,
            CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS)
                .setNodeJobStatus(nodeJobStatus)
                .build().toByteArray());

        // slave for node 2 lost

        currentClusterJob = cluster.getCurrentClusterJob();
        node = currentClusterJob.getCurrentNode();
        slave = slaveForNode(node);
        scheduler.executorLost(driver, Protos.ExecutorID.newBuilder().setValue(node.getExecutorId()).build(), slave._1, 42);

        // ... just finish 2nd node

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);
        assertFalse(currentClusterJob.hasCurrentNode());

        launchTask(slave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.EXECUTOR_METADATA);

        assertNotNull(currentClusterJob);
        assertEquals(1, currentClusterJob.getRemainingNodesCount());
        assertEquals(2, currentClusterJob.getCompletedNodesCount());
        assertFalse(currentClusterJob.hasCurrentNode());

        // 3rd node

        taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        assertNotNull(taskInfo);
        assertNotEquals(executorId(taskInfo), executorId(taskInfo1));
        assertNotEquals(taskInfo.getSlaveId(), taskInfo1.getSlaveId());
        assertNotEquals(executorId(taskInfo), executorId(taskInfo2));
        assertNotEquals(taskInfo.getSlaveId(), taskInfo2.getSlaveId());
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        assertNotNull(taskInfo);
        currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> s : slaves) {
            if (!s._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(s, 3);
            else
                currentSlave = s;
        }
        assertNotNull(currentSlave);
        nodeJobStatus = initialNodeJobStatus(taskInfo, clusterJobType);

        // ... just finish 3rd node

        finishJob(currentClusterJob, taskInfo, currentSlave, nodeJobStatus, clusterJobType);
        currentClusterJob = cluster.getCurrentClusterJob();

        // job finished

        assertNull(currentClusterJob);

        currentClusterJob = cluster.getLastClusterJob(clusterJobType);
        assertNotNull(currentClusterJob);

        assertFalse(currentClusterJob.hasCurrentNode());
        assertEquals(0, currentClusterJob.getRemainingNodesCount());
        assertEquals(3, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertTrue(currentClusterJob.hasFinishedTimestamp());

        // no tasks

        for (Tuple2<Protos.SlaveID, String> s : slaves) {
            noopOnOffer(s, activeNodes);
        }
    }

    protected Tuple2<Protos.SlaveID, String> slaveForNode(CassandraFrameworkProtos.NodeJobStatus node) {
        for (int i = 0; i < executorMetadata.length; i++) {
            if (node.getExecutorId().equals(executorMetadata[i].getExecutorOrBuilder().getExecutorId().getValue())) {
                return slaves[i];
            }
        }
        return null;
    }

    protected void clusterJobAbort(CassandraFrameworkProtos.ClusterJobType clusterJobType) throws InvalidProtocolBufferException {
        CassandraFrameworkProtos.ClusterJobStatus currentClusterJob = cluster.getCurrentClusterJob();
        assertNull(currentClusterJob);
        for (CassandraFrameworkProtos.ClusterJobType jobType : CassandraFrameworkProtos.ClusterJobType.values()) {
            assertNull(cluster.getCurrentClusterJob(jobType));
        }

        // simulate API call
        assertTrue(cluster.startClusterTask(clusterJobType));

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);
        for (CassandraFrameworkProtos.ClusterJobType jobType : CassandraFrameworkProtos.ClusterJobType.values()) {
            if (jobType == clusterJobType) {
                assertNotNull(cluster.getCurrentClusterJob(jobType));
            } else {
                assertNull(cluster.getCurrentClusterJob(jobType));
                assertFalse(cluster.startClusterTask(jobType));
            }
        }

        assertFalse(currentClusterJob.hasCurrentNode());
        assertEquals(3, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());

        // launch job on a node
        Protos.TaskInfo taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        Protos.TaskInfo taskInfo1 = taskInfo;
        assertNotNull(taskInfo);
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        // no other slave must produce a task
        Tuple2<Protos.SlaveID, String> currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            if (!slave._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(slave, 3);
            else
                currentSlave = slave;
        }
        assertNotNull(currentSlave);

        // check cluster job
        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);

        assertTrue(currentClusterJob.hasCurrentNode());
        assertEquals(executorIdValue(taskInfo), currentClusterJob.getCurrentNode().getExecutorId());
        assertEquals(clusterJobType, currentClusterJob.getCurrentNode().getJobType());
        assertTrue(currentClusterJob.getCurrentNode().hasStartedTimestamp());
        assertFalse(currentClusterJob.getCurrentNode().hasFinishedTimestamp());
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());

        // check job status submit

        CassandraFrameworkProtos.TaskDetails taskDetails = submitTask(currentSlave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS);
        assertNotNull(taskDetails);

        // simulate job status response

        CassandraFrameworkProtos.NodeJobStatus nodeJobStatus = initialNodeJobStatus(taskInfo, clusterJobType);

        scheduler.frameworkMessage(driver,
            Protos.ExecutorID.newBuilder().setValue(currentClusterJob.getCurrentNode().getExecutorId()).build(),
            currentSlave._1,
            CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS)
                .setNodeJobStatus(nodeJobStatus)
                .build().toByteArray());

        // check cluster job after 1st response

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);

        assertTrue(currentClusterJob.hasCurrentNode());
        assertEquals(executorIdValue(taskInfo), currentClusterJob.getCurrentNode().getExecutorId());
        assertEquals(clusterJobType, currentClusterJob.getCurrentNode().getJobType());
        assertTrue(currentClusterJob.getCurrentNode().hasStartedTimestamp());
        assertFalse(currentClusterJob.getCurrentNode().hasFinishedTimestamp());
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());
        //
        // we cannot compare this one:  assertEquals(nodeJobStatus.getStartedTimestamp(), currentClusterJob.getCurrentNode().getStartedTimestamp());
        assertEquals(Arrays.asList("foo", "bar", "baz"), currentClusterJob.getCurrentNode().getRemainingKeyspacesList());
        assertEquals(0, currentClusterJob.getCurrentNode().getProcessedKeyspacesCount());
        assertTrue(currentClusterJob.getCurrentNode().getRunning());
        assertEquals(taskIdValue(taskInfo), currentClusterJob.getCurrentNode().getTaskId());

        // node has finished ...

        taskDetails = submitTask(currentSlave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB_STATUS);
        assertNotNull(taskDetails);

        finishJob(currentClusterJob, taskInfo, currentSlave, nodeJobStatus, clusterJobType);
        currentClusterJob = cluster.getCurrentClusterJob();

        // cluster job should have no current node yet

        assertNotNull(currentClusterJob);
        assertEquals(2, currentClusterJob.getRemainingNodesCount());
        assertEquals(1, currentClusterJob.getCompletedNodesCount());
        assertFalse(currentClusterJob.hasCurrentNode());

        // 2nd node

        taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        assertNotNull(taskInfo);
        assertNotEquals(executorId(taskInfo), executorId(taskInfo1));
        assertNotEquals(taskInfo.getSlaveId(), taskInfo1.getSlaveId());
        assertEquals(executorIdValue(taskInfo) + '.' + clusterJobType.name(), taskIdValue(taskInfo));

        assertNotNull(taskInfo);
        currentSlave = null;
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            if (!slave._1.equals(taskInfo.getSlaveId()))
                noopOnOffer(slave, 3);
            else
                currentSlave = slave;
        }
        assertNotNull(currentSlave);
        nodeJobStatus = initialNodeJobStatus(taskInfo, clusterJobType);

        // ABORT THE CLUSTER JOB

        cluster.abortClusterJob(clusterJobType);

        // ... just finish 2nd node

        finishJob(currentClusterJob, taskInfo, currentSlave, nodeJobStatus, clusterJobType);
        currentClusterJob = cluster.getCurrentClusterJob();

        assertNotNull(currentClusterJob);
        assertEquals(1, currentClusterJob.getRemainingNodesCount());
        assertEquals(2, currentClusterJob.getCompletedNodesCount());
        assertFalse(currentClusterJob.hasCurrentNode());

        // 3rd node

        taskInfo = launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.NODE_JOB);
        assertNull(taskInfo);

        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            noopOnOffer(slave, 3);
        }

        // job finished

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNull(currentClusterJob);

        currentClusterJob = cluster.getLastClusterJob(clusterJobType);
        assertNotNull(currentClusterJob);

        assertFalse(currentClusterJob.hasCurrentNode());
        assertEquals(0, currentClusterJob.getRemainingNodesCount());
        assertEquals(3, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertTrue(currentClusterJob.hasFinishedTimestamp());
        for (CassandraFrameworkProtos.NodeJobStatus jobStatus : currentClusterJob.getCompletedNodesList()) {
            assertEquals(clusterJobType, jobStatus.getJobType());
            assertEquals(3, jobStatus.getProcessedKeyspacesCount());
            assertEquals(0, jobStatus.getRemainingKeyspacesCount());
            assertFalse(jobStatus.getRunning());
        }

        // no tasks

        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            noopOnOffer(slave, activeNodes);
        }
    }

    protected static CassandraFrameworkProtos.NodeJobStatus initialNodeJobStatus(Protos.TaskInfo taskInfo, CassandraFrameworkProtos.ClusterJobType clusterJobType) {
        return CassandraFrameworkProtos.NodeJobStatus.newBuilder()
                .setJobType(clusterJobType)
                .setExecutorId(executorIdValue(taskInfo))
                .setTaskId(taskIdValue(taskInfo))
                .setRunning(true)
                .setStartedTimestamp(System.currentTimeMillis())
                .addAllRemainingKeyspaces(Arrays.asList("foo", "bar", "baz"))
                .build();
    }

    protected void finishJob(CassandraFrameworkProtos.ClusterJobStatus currentClusterJob, Protos.TaskInfo taskInfo, Tuple2<Protos.SlaveID, String> currentSlave, CassandraFrameworkProtos.NodeJobStatus nodeJobStatus, CassandraFrameworkProtos.ClusterJobType clusterJobType) {
        nodeJobStatus = CassandraFrameworkProtos.NodeJobStatus.newBuilder()
                .setJobType(clusterJobType)
                .setExecutorId(executorIdValue(taskInfo))
                .setTaskId(taskIdValue(taskInfo))
                .setRunning(false)
                .setStartedTimestamp(nodeJobStatus.getStartedTimestamp())
                .setFinishedTimestamp(System.currentTimeMillis())
                .addAllProcessedKeyspaces(Arrays.asList(
                        CassandraFrameworkProtos.ClusterJobKeyspaceStatus.newBuilder()
                                .setDuration(1)
                                .setKeyspace("foo")
                                .setStatus("FOO")
                                .build(),
                        CassandraFrameworkProtos.ClusterJobKeyspaceStatus.newBuilder()
                                .setDuration(1)
                                .setKeyspace("bar")
                                .setStatus("BAR")
                                .build(),
                        CassandraFrameworkProtos.ClusterJobKeyspaceStatus.newBuilder()
                                .setDuration(1)
                                .setKeyspace("baz")
                                .setStatus("BAZ")
                                .build()
                ))
            .build();

        executorTaskFinished(taskInfo, CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS)
            .setNodeJobStatus(nodeJobStatus)
            .build());
        scheduler.frameworkMessage(driver,
            Protos.ExecutorID.newBuilder().setValue(currentClusterJob.getCurrentNode().getExecutorId()).build(),
            currentSlave._1,
            CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NODE_JOB_STATUS)
                .setNodeJobStatus(nodeJobStatus)
                        .build().toByteArray());
    }

    protected void addFourthNode() throws InvalidProtocolBufferException {
        startFourthNode();

        fourthNodeRunning();
    }

    protected void fourthNodeRunning() {
        executorTaskRunning(executorMetadata[3]);
        executorTaskRunning(executorServer[3]._1);
        sendHealthCheckResult(executorMetadata[3], healthCheckDetailsSuccess("NORMAL", true));
    }

    protected void startFourthNode() throws InvalidProtocolBufferException {
        executorMetadata[3] = launchExecutor(slaves[3], 4);
        executorTaskRunning(executorMetadata[3]);

        executorServer[3] = launchTask(slaves[3], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);
    }

    protected Protos.TaskInfo[] threeNodeCluster() throws InvalidProtocolBufferException {
        cleanState();

        activeNodes = 3;

        // rollout slaves
        executorMetadata = new Protos.TaskInfo[slaves.length];
        executorMetadata[0] = launchExecutor(slaves[0], 1);
        executorMetadata[1] = launchExecutor(slaves[1], 2);
        executorMetadata[2] = launchExecutor(slaves[2], 3);

        return threeNodeClusterPost();
    }

    protected Protos.TaskInfo[] threeNodeClusterPost() throws InvalidProtocolBufferException {
        executorTaskRunning(executorMetadata[0]);
        executorTaskRunning(executorMetadata[1]);
        executorTaskRunning(executorMetadata[2]);

        // launch servers

        //noinspection unchecked
        executorServer = new Tuple2[slaves.length];

        executorServer[0] = launchTask(slaves[0], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);
        executorServer[1] = launchTask(slaves[1], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);

        executorTaskRunning(executorServer[0]._1);
        executorTaskRunning(executorServer[1]._1);

        sendHealthCheckResult(executorMetadata[0], healthCheckDetailsSuccess("NORMAL", true));
        sendHealthCheckResult(executorMetadata[1], healthCheckDetailsSuccess("NORMAL", true));

        executorServer[2] = launchTask(slaves[2], CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);

        executorTaskRunning(executorServer[2]._1);

        sendHealthCheckResult(executorMetadata[2], healthCheckDetailsSuccess("NORMAL", true));
        return executorMetadata;
    }

    protected void executorTaskError(Protos.TaskInfo taskInfo) {
        scheduler.statusUpdate(driver, Protos.TaskStatus.newBuilder()
            .setExecutorId(executorId(taskInfo))
            .setHealthy(true)
            .setSlaveId(taskInfo.getSlaveId())
            .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR)
            .setTaskId(taskInfo.getTaskId())
            .setTimestamp(System.currentTimeMillis())
            .setState(Protos.TaskState.TASK_ERROR)
                .build());
    }

    protected void executorTaskRunning(Protos.TaskInfo taskInfo) {
        scheduler.statusUpdate(driver, Protos.TaskStatus.newBuilder()
            .setExecutorId(executorId(taskInfo))
            .setHealthy(true)
            .setSlaveId(taskInfo.getSlaveId())
            .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR)
            .setTaskId(taskInfo.getTaskId())
            .setTimestamp(System.currentTimeMillis())
            .setState(Protos.TaskState.TASK_RUNNING)
            .setData(CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.EXECUTOR_METADATA)
                .setExecutorMetadata(CassandraFrameworkProtos.ExecutorMetadata.newBuilder()
                    .setExecutorId(executorIdValue(taskInfo))
                    .setIp("NO_IP!!!")
                    .setWorkdir("/foo/bar/baz"))
                .build().toByteString())
            .build());
    }

    protected void executorTaskFinished(Protos.TaskInfo taskInfo, CassandraFrameworkProtos.SlaveStatusDetails slaveStatusDetails) {
        scheduler.statusUpdate(driver, Protos.TaskStatus.newBuilder()
            .setExecutorId(executorId(taskInfo))
            .setHealthy(true)
            .setSlaveId(taskInfo.getSlaveId())
            .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR)
            .setTaskId(taskInfo.getTaskId())
            .setTimestamp(System.currentTimeMillis())
            .setState(Protos.TaskState.TASK_FINISHED)
            .setData(slaveStatusDetails.toByteString())
            .build());
    }

    protected void sendHealthCheckResult(Protos.TaskInfo taskInfo, CassandraFrameworkProtos.HealthCheckDetails healthCheckDetails) {
        scheduler.frameworkMessage(driver, executorId(taskInfo), taskInfo.getSlaveId(),
            CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.HEALTH_CHECK_DETAILS)
                .setHealthCheckDetails(healthCheckDetails).build().toByteArray());
    }

    protected Protos.TaskInfo launchExecutor(Tuple2<Protos.SlaveID, String> slave, int nodeCount) throws InvalidProtocolBufferException {
        Protos.Offer offer = createOffer(slave);

        scheduler.resourceOffers(driver, Collections.singletonList(offer));

        Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> launchTasks = driver.launchTasks();
        assertTrue(driver.declinedOffers().isEmpty());
        assertTrue(driver.submitTasks().isEmpty());
        assertTrue(driver.killTasks().isEmpty());

        assertEquals(nodeCount, cluster.getClusterState().get().getNodesCount());
        assertEquals(1, launchTasks._2.size());

        Protos.TaskInfo taskInfo = launchTasks._2.iterator().next();

        CassandraFrameworkProtos.TaskDetails taskDetails = taskDetails(taskInfo);
        assertEquals(CassandraFrameworkProtos.TaskDetails.TaskDetailsType.EXECUTOR_METADATA, taskDetails.getType());
        return taskInfo;
    }

    protected Protos.TaskInfo launchTaskOnAny(CassandraFrameworkProtos.TaskDetails.TaskDetailsType taskType) throws InvalidProtocolBufferException {
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            Protos.Offer offer = createOffer(slave);

            scheduler.resourceOffers(driver, Collections.singletonList(offer));

            Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> launchTasks = driver.launchTasks();
            if (!driver.declinedOffers().isEmpty())
                continue;

            assertEquals(1, launchTasks._2.size());
            assertTrue(driver.submitTasks().isEmpty());
            assertTrue(driver.killTasks().isEmpty());

            Protos.TaskInfo taskInfo = launchTasks._2.iterator().next();

            CassandraFrameworkProtos.TaskDetails taskDetails = taskDetails(taskInfo);
            assertEquals(taskType, taskDetails.getType());
            return taskInfo;
        }
        return null;
    }

    protected CassandraFrameworkProtos.TaskDetails submitTask(Tuple2<Protos.SlaveID, String> slave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType taskType) {
        Protos.Offer offer = createOffer(slave);

        scheduler.resourceOffers(driver, Collections.singletonList(offer));

        assertFalse(driver.declinedOffers().isEmpty());
        Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> launchTasks = driver.launchTasks();
        assertTrue(launchTasks._2.isEmpty());
        Collection<Tuple2<Protos.ExecutorID, CassandraFrameworkProtos.TaskDetails>> submitTasks = driver.submitTasks();
        assertTrue(driver.killTasks().isEmpty());

        assertEquals(1, submitTasks.size());

        CassandraFrameworkProtos.TaskDetails taskDetails = submitTasks.iterator().next()._2;
        assertEquals(taskType, taskDetails.getType());
        return taskDetails;
    }

    protected void killTask(Tuple2<Protos.SlaveID, String> slave, String taskID) {
        Protos.Offer offer = createOffer(slave);

        scheduler.resourceOffers(driver, Collections.singletonList(offer));

        assertThat(driver.declinedOffers())
            .hasSize(1);
        assertTrue(driver.launchTasks()._2.isEmpty());
        assertTrue(driver.submitTasks().isEmpty());
        assertThat(driver.killTasks())
            .hasSize(1)
            .contains(Protos.TaskID.newBuilder().setValue(taskID).build());
    }

    protected Tuple2<Protos.TaskInfo, CassandraFrameworkProtos.TaskDetails> launchTask(Tuple2<Protos.SlaveID, String> slave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType taskType) throws InvalidProtocolBufferException {
        Protos.Offer offer = createOffer(slave);

        scheduler.resourceOffers(driver, Collections.singletonList(offer));

        assertTrue(driver.declinedOffers().isEmpty());
        Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> launchTasks = driver.launchTasks();
        assertTrue(driver.submitTasks().isEmpty());
        assertTrue(driver.killTasks().isEmpty());

        assertEquals(1, launchTasks._2.size());

        Protos.TaskInfo taskInfo = launchTasks._2.iterator().next();

        CassandraFrameworkProtos.TaskDetails taskDetails = taskDetails(taskInfo);
        assertEquals(taskType, taskDetails.getType());
        return Tuple2.tuple2(taskInfo, taskDetails);
    }

    protected static CassandraFrameworkProtos.TaskDetails taskDetails(Protos.TaskInfo data) throws InvalidProtocolBufferException {
        return CassandraFrameworkProtos.TaskDetails.parseFrom(data.getData());
    }

    protected void noopOnOffer(Tuple2<Protos.SlaveID, String> slave, int nodeCount) {
        noopOnOffer(slave, nodeCount, false);
    }

    protected void noopOnOffer(Tuple2<Protos.SlaveID, String> slave, int nodeCount, boolean ignoreKills) {
        Protos.Offer offer = createOffer(slave);

        scheduler.resourceOffers(driver, Collections.singletonList(offer));

        Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> launchTasks = driver.launchTasks();
        assertTrue(ProtoUtils.protoToString(driver.submitTasks()), driver.submitTasks().isEmpty());
        boolean noKills = driver.killTasks().isEmpty();
        if (!ignoreKills) {
            assertTrue(noKills);
        }
        List<Protos.OfferID> decl = driver.declinedOffers();
        assertThat(decl)
            .hasSize(1)
            .contains(offer.getId());

        assertEquals(nodeCount, cluster.getClusterState().get().getNodesCount());
        assertEquals(0, launchTasks._2.size());
    }

    protected void noopOnOfferAll() {
        for (Tuple2<Protos.SlaveID, String> slave : slaves) {
            noopOnOffer(slave, activeNodes);
        }
    }

    protected void cleanState() {
        super.cleanState();

        scheduler = new CassandraScheduler(configuration, cluster);

        driver = new MockSchedulerDriver(scheduler);
        driver.callRegistered(Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()).build());
    }

    protected static String executorIdValue(Protos.TaskInfo executorMetadata) {
        return executorId(executorMetadata).getValue();
    }

    protected static String taskIdValue(Protos.TaskInfo taskInfo) {
        return taskInfo.getTaskId().getValue();
    }

    protected static Protos.ExecutorID executorId(Protos.TaskInfo taskInfo) {
        return taskInfo.getExecutor().getExecutorId();
    }

    protected CassandraFrameworkProtos.HealthCheckDetails lastHealthCheckDetails(Protos.TaskInfo executorMetadata) {
        return cluster.lastHealthCheck(executorIdValue(executorMetadata)).getDetails();
    }

    protected Tuple2<Protos.SlaveID, String> slaveForExecutor(String executorId) {
        for (int i = 0; i < executorMetadata.length; i++) {
            if (executorMetadata[i] != null && executorMetadata[i].getExecutor().getExecutorId().getValue().equals(executorId)) {
                return slaves[i];
            }
        }
        return null;
    }

    protected Protos.TaskInfo execForExecutor(String executorId) {
        for (Protos.TaskInfo execMetadata : executorMetadata) {
            if (execMetadata != null && execMetadata.getExecutor().getExecutorId().getValue().equals(executorId)) {
                return execMetadata;
            }
        }
        return null;
    }

    protected Tuple2<Protos.TaskInfo, CassandraFrameworkProtos.TaskDetails> serverTaskForExecutor(String executorId) {
        for (Tuple2<Protos.TaskInfo, CassandraFrameworkProtos.TaskDetails> serverInfo : executorServer) {
            if (serverInfo != null && serverInfo._1.getExecutor().getExecutorId().getValue().equals(executorId)) {
                return serverInfo;
            }
        }
        return null;
    }

    protected static CassandraFrameworkProtos.TaskResources someResources() {
        return CassandraFrameworkProtos.TaskResources.newBuilder()
            .setCpuCores(1)
            .setMemMb(1)
            .setDiskMb(1)
            .build();
    }

    protected static CassandraFrameworkProtos.TaskResources resources(double cores, long mem, long disk) {
        return CassandraFrameworkProtos.TaskResources.newBuilder()
            .setCpuCores(cores)
            .setMemMb(mem)
            .setDiskMb(disk)
            .build();
    }
}
