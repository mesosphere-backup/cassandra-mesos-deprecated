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
import org.assertj.core.util.Lists;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.Collections;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.mesosphere.mesos.util.ProtoUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class CassandraClusterStateTest extends AbstractSchedulerTest {

    @Test
    public void testLaunchNewCluster() {

        cleanState();

        // rollout slave #1

        final CassandraFrameworkProtos.ExecutorMetadata executorMetadata1 = launchExecutor(cluster, slaves[0], 1);

        // next offer must return nothing for the same slave !

        noopOnOffer(cluster, slaves[0], 1);

        //
        // at this point the executor for slave #1 is known but the server must not be started because we can't fulfil
        // the seed-node-count requirement
        //

        cluster.addExecutorMetadata(executorMetadata1);
        noopOnOffer(cluster, slaves[0], 1);
        assertEquals(Collections.singletonList("127.1.1.1"), cluster.getSeedNodeIps(false));
        assertThat(clusterState.nodeCounts()).isEqualTo(new NodeCounts(1, 1));

        // rollout slave #2

        final CassandraFrameworkProtos.ExecutorMetadata executorMetadata2 = launchExecutor(cluster, slaves[1], 2);

        // rollout slave #3

        final CassandraFrameworkProtos.ExecutorMetadata executorMetadata3 = launchExecutor(cluster, slaves[2], 3);

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
        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));

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
        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));
        launchServer(cluster, slaves[2]);

    }

    @Test
    public void testServerTaskRemove() {

        cleanState();

        // rollout slaves
        final CassandraFrameworkProtos.ExecutorMetadata executorMetadata1 = launchExecutor(cluster, slaves[0], 1);
        final CassandraFrameworkProtos.ExecutorMetadata executorMetadata2 = launchExecutor(cluster, slaves[1], 2);
        final CassandraFrameworkProtos.ExecutorMetadata executorMetadata3 = launchExecutor(cluster, slaves[2], 3);

        cluster.addExecutorMetadata(executorMetadata1);
        cluster.addExecutorMetadata(executorMetadata2);
        cluster.addExecutorMetadata(executorMetadata3);

        // launch servers
        launchServer(cluster, slaves[0]);

        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));

        launchServer(cluster, slaves[1]);

        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));

        launchServer(cluster, slaves[2]);

        cluster.recordHealthCheck(executorMetadata3.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));

        // cluster now up with 3 running nodes

        // server-task no longer running
        final CassandraFrameworkProtos.CassandraNodeTask serverTask = CassandraFrameworkProtosUtils.getTaskForNode(cluster.cassandraNodeForHostname(slaves[0]._2).get(), CassandraFrameworkProtos.CassandraNodeTask.NodeTaskType.SERVER);
        assertNotNull(serverTask);
        cluster.removeTask(serverTask.getTaskId(), Protos.TaskStatus.getDefaultInstance());

        // server-task cannot start again
        launchServer(cluster, slaves[0]);
    }

    @Test
    public void allResourcesAvailableBeforeLaunchingExecutor() throws Exception {
        cleanState();

        final String role = "*";
        final Protos.Offer offer = Protos.Offer.newBuilder()
            .setFrameworkId(frameworkId)
            .setHostname("localhost")
            .setId(Protos.OfferID.newBuilder().setValue(randomID()))
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave_1"))
            .addResources(cpu(0.1, role))
            .addResources(mem(0.1, role))
            .addResources(disk(0.1, role))
            .addResources(ports(Lists.<Long>emptyList(), role))
            .build();
        final Marker marker = MarkerFactory.getMarker("offerId:" + offer.getId().getValue() + ",hostname:" + offer.getHostname());
        assertThat(cluster._getTasksForOffer(marker, offer)).isNull();
    }

    @NotNull
    private CassandraFrameworkProtos.ExecutorMetadata launchServer(@NotNull final CassandraCluster cluster, @NotNull final Tuple2<Protos.SlaveID, String> slave) {
        return launchTask(cluster, slave, -1, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.CASSANDRA_SERVER_RUN);
    }

    @NotNull
    private CassandraFrameworkProtos.ExecutorMetadata launchExecutor(@NotNull final CassandraCluster cluster, @NotNull final Tuple2<Protos.SlaveID, String> slave, final int nodeCount) {
        return launchTask(cluster, slave, nodeCount, CassandraFrameworkProtos.TaskDetails.TaskDetailsType.EXECUTOR_METADATA);
    }

    @NotNull
    private CassandraFrameworkProtos.ExecutorMetadata launchTask(@NotNull final CassandraCluster cluster, @NotNull final Tuple2<Protos.SlaveID, String> slave, final int nodeCount, @NotNull final CassandraFrameworkProtos.TaskDetails.TaskDetailsType taskType) {
        final Protos.Offer offer = createOffer(slave);
        final TasksForOffer tasksForOffer = cluster.getTasksForOffer(offer);

        if (nodeCount >= 0)
            assertEquals(nodeCount, cluster.getClusterState().get().getNodesCount());
        assertNotNull(tasksForOffer);
        assertEquals(1, tasksForOffer.getLaunchTasks().size());
        assertEquals(0, tasksForOffer.getSubmitTasks().size());

        final CassandraFrameworkProtos.CassandraNodeTask launchTask = tasksForOffer.getLaunchTasks().get(0);
        assertEquals(taskType, launchTask.getTaskDetails().getType());
        return executorMetadataFor(slave, tasksForOffer.getExecutor().getExecutorId());
    }

    private void noopOnOffer(@NotNull final CassandraCluster cluster, @NotNull final Tuple2<Protos.SlaveID, String> slave, final int nodeCount) {
        final Protos.Offer offer = createOffer(slave);
        final TasksForOffer tasksForOffer = cluster.getTasksForOffer(offer);

        assertNull(tasksForOffer);
        assertEquals(nodeCount, cluster.getClusterState().get().getNodesCount());
    }

    @NotNull
    private static CassandraFrameworkProtos.ExecutorMetadata executorMetadataFor(@NotNull final Tuple2<Protos.SlaveID, String> slave, @NotNull final String executorID) {
        return CassandraFrameworkProtos.ExecutorMetadata.newBuilder()
                .setExecutorId(executorID)
                .setIp(slave._2)
                .setWorkdir("/foo/bar/baz")
                .build();
    }

    @NotNull
    private CassandraFrameworkProtos.HealthCheckDetails lastHealthCheckDetails(@NotNull final CassandraFrameworkProtos.ExecutorMetadata executorMetadata) {
        return checkNotNull(cluster.lastHealthCheck(executorMetadata.getExecutorId())).getDetails();
    }
}
