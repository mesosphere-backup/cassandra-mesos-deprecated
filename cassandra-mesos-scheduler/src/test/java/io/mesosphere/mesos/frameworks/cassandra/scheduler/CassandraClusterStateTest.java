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

import java.util.*;


import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

public class CassandraClusterStateTest extends AbstractSchedulerTest {

    @Test
    public void testLaunchNewCluster() {

        cleanState();

        // rollout slave #1

        CassandraFrameworkProtos.ExecutorMetadata executorMetadata1 = launchExecutor(cluster, slaves[0], 1);

        // next offer must return nothing for the same slave !

        noopOnOffer(cluster, slaves[0], 1);

        //
        // at this point the executor for slave #1 is known but the server must not be started because we can't fulfil
        // the seed-node-count requirement
        //

        cluster.addExecutorMetadata(executorMetadata1);
        noopOnOffer(cluster, slaves[0], 1);
        assertEquals(Collections.singletonList("127.1.1.1"), cluster.getSeedNodeIps());
        assertThat(clusterState.nodeCounts()).isEqualTo(new NodeCounts(1, 1));

        // rollout slave #2

        CassandraFrameworkProtos.ExecutorMetadata executorMetadata2 = launchExecutor(cluster, slaves[1], 2);

        // rollout slave #3

        CassandraFrameworkProtos.ExecutorMetadata executorMetadata3 = launchExecutor(cluster, slaves[2], 3);

        // next offer must return nothing for the same slave !

        noopOnOffer(cluster, slaves[1], 3);

        // next offer must return nothing for the same slave !

        noopOnOffer(cluster, slaves[2], 3);

        //
        // at this point all three slave have got metadata tasks
        //

        noopOnOffer(cluster, slaves[1], 3);
        noopOnOffer(cluster, slaves[2], 3);

        assertThat(clusterState.nodeCounts()).isEqualTo(new NodeCounts(3, 2));

        cluster.addExecutorMetadata(executorMetadata2);

        assertThat(clusterState.nodeCounts()).isEqualTo(new NodeCounts(3, 2));

        //
        // now there are enough executor metadata to start the seed nodes - but not the non-seed nodes
        //

        // node must not start (it's not a seed node and no seed node is running)
        noopOnOffer(cluster, slaves[2], 3);

        launchServer(cluster, slaves[0]);
        launchServer(cluster, slaves[1]);
        // still - not able to start node #3
        noopOnOffer(cluster, slaves[2], 3);

        //
        // executor for node #3 started
        //

        cluster.addExecutorMetadata(executorMetadata3);
        // still - not able to start node #3
        noopOnOffer(cluster, slaves[2], 3);

        //
        // simulate some health check states
        //

        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsFailed());
        assertThat(lastHealthCheckDetails(executorMetadata1))
            .isNot(healthy());
        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsFailed());
        assertThat(lastHealthCheckDetails(executorMetadata2))
            .isNot(healthy());
        noopOnOffer(cluster, slaves[2], 3);

        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsSuccess("JOINING", false));
        assertThat(lastHealthCheckDetails(executorMetadata1))
            .has(operationMode("JOINING"))
            .is(healthy());
        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsFailed());
        // still - not able to start node #3
        noopOnOffer(cluster, slaves[2], 3);

        //
        // one seed has started up
        //

        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));
        assertThat(lastHealthCheckDetails(executorMetadata1))
            .has(operationMode("NORMAL"))
            .is(healthy());
        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsFailed());
        assertThat(lastHealthCheckDetails(executorMetadata2))
            .isNot(healthy());
        // node#3 can start now
        launchServer(cluster, slaves[2]);

    }

    @Test
    public void testServerTaskRemove() {

        cleanState();

        // rollout slaves
        CassandraFrameworkProtos.ExecutorMetadata executorMetadata1 = launchExecutor(cluster, slaves[0], 1);
        CassandraFrameworkProtos.ExecutorMetadata executorMetadata2 = launchExecutor(cluster, slaves[1], 2);
        CassandraFrameworkProtos.ExecutorMetadata executorMetadata3 = launchExecutor(cluster, slaves[2], 3);

        cluster.addExecutorMetadata(executorMetadata1);
        cluster.addExecutorMetadata(executorMetadata2);
        cluster.addExecutorMetadata(executorMetadata3);

        // launch servers

        launchServer(cluster, slaves[0]);
        launchServer(cluster, slaves[1]);

        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));
        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));

        launchServer(cluster, slaves[2]);

        cluster.recordHealthCheck(executorMetadata3.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));

        // cluster now up with 3 running nodes

        // server-task no longer running
        CassandraFrameworkProtos.CassandraNodeTask serverTask = CassandraFrameworkProtosUtils.getTaskForNode(cluster.cassandraNodeForHostname(slaves[0]._2).get(), CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER);
        assertNotNull(serverTask);
        cluster.removeTask(serverTask.getTaskId(), Protos.TaskStatus.getDefaultInstance());

        // server-task cannot start again
        launchServer(cluster, slaves[0]);
    }

    private CassandraFrameworkProtos.ExecutorMetadata launchServer(CassandraCluster cluster, Tuple2<Protos.SlaveID, String> slave) {
        return launchTask(cluster, slave, -1, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);
    }

    private CassandraFrameworkProtos.ExecutorMetadata launchExecutor(CassandraCluster cluster, Tuple2<Protos.SlaveID, String> slave, int nodeCount) {
        return launchTask(cluster, slave, nodeCount, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.EXECUTOR_METADATA);
    }

    private CassandraFrameworkProtos.ExecutorMetadata launchTask(CassandraCluster cluster, Tuple2<Protos.SlaveID, String> slave, int nodeCount, CassandraFrameworkProtos.TaskDetails.TaskDetailsType taskType) {
        Protos.Offer offer = createOffer(slave);
        TasksForOffer tasksForOffer = cluster.getTasksForOffer(offer);

        if (nodeCount >= 0)
            assertEquals(nodeCount, cluster.getClusterState().get().getNodesCount());
        assertNotNull(tasksForOffer);
        assertTrue(tasksForOffer.hasExecutor());
        assertEquals(1, tasksForOffer.getLaunchTasks().size());
        assertEquals(0, tasksForOffer.getSubmitTasks().size());

        CassandraFrameworkProtos.CassandraNodeTask launchTask = tasksForOffer.getLaunchTasks().get(0);
        assertEquals(taskType, launchTask.getTaskDetails().getType());
        return executorMetadataFor(slave, tasksForOffer.getExecutor().getExecutorId());
    }

    private void noopOnOffer(CassandraCluster cluster, Tuple2<Protos.SlaveID, String> slave, int nodeCount) {
        Protos.Offer offer = createOffer(slave);
        TasksForOffer tasksForOffer = cluster.getTasksForOffer(offer);

        assertNull(tasksForOffer);
        assertEquals(nodeCount, cluster.getClusterState().get().getNodesCount());
    }

    private static CassandraFrameworkProtos.ExecutorMetadata executorMetadataFor(Tuple2<Protos.SlaveID, String> slave, String executorID) {
        return CassandraFrameworkProtos.ExecutorMetadata.newBuilder()
                .setExecutorId(executorID)
                .setIp(slave._2)
                .setWorkdir("/foo/bar/baz")
                .build();
    }

    private CassandraFrameworkProtos.HealthCheckDetails lastHealthCheckDetails(CassandraFrameworkProtos.ExecutorMetadata executorMetadata) {
        return cluster.lastHealthCheck(executorMetadata.getExecutorId()).getDetails();
    }
}
