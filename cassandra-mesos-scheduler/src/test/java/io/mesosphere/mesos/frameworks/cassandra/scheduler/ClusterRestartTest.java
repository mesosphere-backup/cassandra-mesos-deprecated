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

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.util.CassandraFrameworkProtosUtils;
import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.junit.Assert.assertNull;

public class ClusterRestartTest extends AbstractCassandraSchedulerTest {

    @Test
    public void testClusterRestart() throws Exception {
        threeNodeCluster();

        CassandraFrameworkProtos.ClusterJobType clusterJobType = CassandraFrameworkProtos.ClusterJobType.RESTART;

        CassandraFrameworkProtos.ClusterJobStatus currentClusterJob = cluster.getCurrentClusterJob();
        assertNull(currentClusterJob);

        // simulate API call
        cluster.startClusterTask(clusterJobType);

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);

        assertFalse(currentClusterJob.hasCurrentNode());
        assertEquals(3, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());

        String restartingExecutor;
        Set<String> restartedExecutors = new HashSet<>();

        for (int i = 0; i < activeNodes; i++) {

            currentClusterJob = cluster.getCurrentClusterJob();
            assertNotNull(currentClusterJob);
            assertFalse(currentClusterJob.hasCurrentNode());

            noopOnOfferAll();

            currentClusterJob = cluster.getCurrentClusterJob();
            assertNotNull(currentClusterJob);
            assertTrue(currentClusterJob.hasCurrentNode());

            restartingExecutor = currentClusterJob.getCurrentNode().getExecutorId();
            assertTrue(restartedExecutors.add(restartingExecutor));

            // check that node is in RESTART state
            CassandraFrameworkProtos.CassandraNode node = cluster.findNode(restartingExecutor);
            assertNotNull(node);
            assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.RESTART, node.getTargetRunState());

            // simulate server-task stopped
            CassandraFrameworkProtos.CassandraNodeTask taskForNode = CassandraFrameworkProtosUtils.getTaskForNode(node, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER);
            assertNotNull(taskForNode);

            Tuple2<Protos.SlaveID, String> slave = slaveForExecutor(restartingExecutor);
            assertNotNull(slave);
            Protos.TaskInfo execMetadata = execForExecutor(restartingExecutor);
            assertNotNull(execMetadata);
            Tuple2<Protos.TaskInfo, CassandraFrameworkProtos.TaskDetails> execServer = serverTaskForExecutor(restartingExecutor);
            assertNotNull(execServer);

            // verify that kill-task is launched
            killTask(slave, taskForNode.getTaskId());
            // must not repeat CASSANDRA_SERVER_SHUTDOWN since it's already launched
            noopOnOffer(slave, 3, true);

            // simulate server-task has finished
            executorTaskFinished(execServer._1, CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
                .build());

            // check that server-task is no longer present
            node = cluster.findNode(restartingExecutor);
            assertNotNull(node);
            assertNull(CassandraFrameworkProtosUtils.getTaskForNode(node, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));

            // must have current-node - server task not running
            currentClusterJob = cluster.getCurrentClusterJob();
            assertNotNull(currentClusterJob);
            assertTrue(currentClusterJob.hasCurrentNode());
            assertEquals(restartingExecutor, currentClusterJob.getCurrentNode().getExecutorId());

            // verify that CASSANDRA_SERVER_RUN task is launched
            launchTask(slave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);
            // must not repeat CASSANDRA_SERVER_RUN since it's already launched
            noopOnOffer(slave, 3, true);

            // must have current-node - no valid HC received yet
            currentClusterJob = cluster.getCurrentClusterJob();
            assertNotNull(currentClusterJob);
            assertTrue(currentClusterJob.hasCurrentNode());
            assertEquals(restartingExecutor, currentClusterJob.getCurrentNode().getExecutorId());

            executorTaskRunning(execServer._1);
            sendHealthCheckResult(execMetadata, healthCheckDetailsSuccess("NORMAL", true));

            noopOnOffer(slave, 3, true);

        }

        assertThat(restartedExecutors).hasSize(activeNodes);

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNull(currentClusterJob);
    }

    @Test
    public void testClusterRestartWithStoppedNode() throws Exception {
        threeNodeCluster();

        CassandraFrameworkProtos.ClusterJobType clusterJobType = CassandraFrameworkProtos.ClusterJobType.RESTART;

        CassandraFrameworkProtos.ClusterJobStatus currentClusterJob = cluster.getCurrentClusterJob();
        assertNull(currentClusterJob);

        // stop node 2
        CassandraFrameworkProtos.CassandraNode node2 = cluster.nodeStop(slaves[1]._2);
        assertNotNull(node2);
        CassandraFrameworkProtos.CassandraNodeTask node1serverTask = CassandraFrameworkProtosUtils.getTaskForNode(node2, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER);
        assertNotNull(node1serverTask);
        killTask(slaves[1], node1serverTask.getTaskId());
        // simulate server-task has finished
        executorTaskFinished(executorServer[1]._1, CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build());


        // simulate API call
        cluster.startClusterTask(clusterJobType);

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNotNull(currentClusterJob);

        assertFalse(currentClusterJob.hasCurrentNode());
        assertEquals(3, currentClusterJob.getRemainingNodesCount());
        assertEquals(0, currentClusterJob.getCompletedNodesCount());
        assertEquals(clusterJobType, currentClusterJob.getJobType());
        assertFalse(currentClusterJob.getAborted());
        assertTrue(currentClusterJob.hasStartedTimestamp());
        assertFalse(currentClusterJob.hasFinishedTimestamp());

        String restartingExecutor;
        Set<String> restartedExecutors = new HashSet<>();

        for (int i = 0; i < activeNodes - 1; i++) {

            currentClusterJob = cluster.getCurrentClusterJob();
            assertNotNull(currentClusterJob);
            assertFalse(currentClusterJob.hasCurrentNode());

            noopOnOfferAll();

            currentClusterJob = cluster.getCurrentClusterJob();
            assertNotNull(currentClusterJob);
            assertTrue(currentClusterJob.hasCurrentNode());

            restartingExecutor = currentClusterJob.getCurrentNode().getExecutorId();
            assertTrue(restartedExecutors.add(restartingExecutor));
            // verify that the stopped node is NOT restarted
            assertNotEquals(executorMetadata[1].getExecutor().getExecutorId(), restartingExecutor);

            // check that node is in RESTART state
            CassandraFrameworkProtos.CassandraNode node = cluster.findNode(restartingExecutor);
            assertNotNull(node);
            assertEquals(CassandraFrameworkProtos.CassandraNode.TargetRunState.RESTART, node.getTargetRunState());

            // simulate server-task stopped
            CassandraFrameworkProtos.CassandraNodeTask taskForNode = CassandraFrameworkProtosUtils.getTaskForNode(node, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER);
            assertNotNull(taskForNode);

            Tuple2<Protos.SlaveID, String> slave = slaveForExecutor(restartingExecutor);
            assertNotNull(slave);
            Protos.TaskInfo execMetadata = execForExecutor(restartingExecutor);
            assertNotNull(execMetadata);
            Tuple2<Protos.TaskInfo, CassandraFrameworkProtos.TaskDetails> execServer = serverTaskForExecutor(restartingExecutor);
            assertNotNull(execServer);

            // verify that kill-task is launched
            killTask(slave, taskForNode.getTaskId());
            // must not repeat CASSANDRA_SERVER_SHUTDOWN since it's already launched
            noopOnOffer(slave, 3, true);

            // simulate server-task has finished
            executorTaskFinished(execServer._1, CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
                .build());

            // check that server-task is no longer present
            node = cluster.findNode(restartingExecutor);
            assertNotNull(node);
            assertNull(CassandraFrameworkProtosUtils.getTaskForNode(node, CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER));

            // must have current-node - server task not running
            currentClusterJob = cluster.getCurrentClusterJob();
            assertNotNull(currentClusterJob);
            assertTrue(currentClusterJob.hasCurrentNode());
            assertEquals(restartingExecutor, currentClusterJob.getCurrentNode().getExecutorId());

            // verify that CASSANDRA_SERVER_RUN task is launched
            launchTask(slave, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);
            // must not repeat CASSANDRA_SERVER_RUN since it's already launched
            noopOnOffer(slave, 3, true);

            // must have current-node - no valid HC received yet
            currentClusterJob = cluster.getCurrentClusterJob();
            assertNotNull(currentClusterJob);
            assertTrue(currentClusterJob.hasCurrentNode());
            assertEquals(restartingExecutor, currentClusterJob.getCurrentNode().getExecutorId());

            executorTaskRunning(execServer._1);
            sendHealthCheckResult(execMetadata, healthCheckDetailsSuccess("NORMAL", true));

            noopOnOffer(slave, 3, true);

        }

        assertThat(restartedExecutors).hasSize(activeNodes - 1);

        currentClusterJob = cluster.getCurrentClusterJob();
        assertNull(currentClusterJob);
    }
}
