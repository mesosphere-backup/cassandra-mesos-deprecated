/**
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
package io.mesosphere.mesos.frameworks.cassandra;

import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class CassandraClusterStateTest extends AbstractSchedulerTest {

    @Test
    public void testLaunchNewCluster() {

        cleanState();

        // rollout slave #1

        CassandraFrameworkProtos.ExecutorMetadata executorMetadata1 = launchExecutor(cluster, slave1, 1);

        // next offer must return nothing for the same slave !

        noopOnOffer(cluster, slave1, 1);

        //
        // at this point the executor for slave #1 is known but the server must not be started because we can't fulfil
        // the seed-node-count requirement
        //

        cluster.addExecutorMetadata(executorMetadata1);
        noopOnOffer(cluster, slave1, 1);
        assertEquals(Collections.singletonList("127.1.1.1"), cluster.getSeedNodes());
        assertEquals(Integer.valueOf(1), clusterState.nodeCounts()._1);
        assertEquals(Integer.valueOf(1), clusterState.nodeCounts()._2);

        // rollout slave #2

        CassandraFrameworkProtos.ExecutorMetadata executorMetadata2 = launchExecutor(cluster, slave2, 2);

        // rollout slave #3

        CassandraFrameworkProtos.ExecutorMetadata executorMetadata3 = launchExecutor(cluster, slave3, 3);

        // next offer must return nothing for the same slave !

        noopOnOffer(cluster, slave2, 3);

        // next offer must return nothing for the same slave !

        noopOnOffer(cluster, slave3, 3);

        //
        // at this point all three slave have got metadata tasks
        //

        noopOnOffer(cluster, slave2, 3);
        noopOnOffer(cluster, slave3, 3);

        assertEquals(Integer.valueOf(3), clusterState.nodeCounts()._1);
        assertEquals(Integer.valueOf(2), clusterState.nodeCounts()._2);

        cluster.addExecutorMetadata(executorMetadata2);

        assertEquals(Integer.valueOf(3), clusterState.nodeCounts()._1);
        assertEquals(Integer.valueOf(2), clusterState.nodeCounts()._2);

        //
        // now there are enough executor metadata to start the seed nodes - but not the non-seed nodes
        //

        // node must not start (it's not a seed node and no seed node is running)
        noopOnOffer(cluster, slave3, 3);

        launchServer(cluster, slave1);
        launchServer(cluster, slave2);
        // still - not able to start node #3
        noopOnOffer(cluster, slave3, 3);

        //
        // executor for node #3 started
        //

        cluster.addExecutorMetadata(executorMetadata3);
        // still - not able to start node #3
        noopOnOffer(cluster, slave3, 3);

        //
        // simulate some health check states
        //

        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsFailed());
        assertFalse(cluster.lastHealthCheck(executorMetadata1.getExecutorId()).getDetails().getHealthy());
        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsFailed());
        assertFalse(cluster.lastHealthCheck(executorMetadata2.getExecutorId()).getDetails().getHealthy());
        // still - not able to start node #3
        noopOnOffer(cluster, slave3, 3);

        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsSuccess("JOINING", false));
        assertTrue(cluster.lastHealthCheck(executorMetadata1.getExecutorId()).getDetails().getHealthy());
        assertEquals("JOINING", cluster.lastHealthCheck(executorMetadata1.getExecutorId()).getDetails().getInfo().getOperationMode());
        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsFailed());
        // still - not able to start node #3
        noopOnOffer(cluster, slave3, 3);

        //
        // one seed has started up
        //

        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));
        assertTrue(cluster.lastHealthCheck(executorMetadata1.getExecutorId()).getDetails().getHealthy());
        assertEquals("NORMAL", cluster.lastHealthCheck(executorMetadata1.getExecutorId()).getDetails().getInfo().getOperationMode());
        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsFailed());
        assertFalse(cluster.lastHealthCheck(executorMetadata2.getExecutorId()).getDetails().getHealthy());
        // node#3 can start now
        launchServer(cluster, slave3);

    }

    @Test
    public void testServerTaskRemove() {

        cleanState();

        // rollout slaves
        CassandraFrameworkProtos.ExecutorMetadata executorMetadata1 = launchExecutor(cluster, slave1, 1);
        CassandraFrameworkProtos.ExecutorMetadata executorMetadata2 = launchExecutor(cluster, slave2, 2);
        CassandraFrameworkProtos.ExecutorMetadata executorMetadata3 = launchExecutor(cluster, slave3, 3);

        cluster.addExecutorMetadata(executorMetadata1);
        cluster.addExecutorMetadata(executorMetadata2);
        cluster.addExecutorMetadata(executorMetadata3);

        // launch servers

        launchServer(cluster, slave1);
        launchServer(cluster, slave2);

        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));
        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));

        launchServer(cluster, slave3);

        cluster.recordHealthCheck(executorMetadata3.getExecutorId(), healthCheckDetailsSuccess("NORMAL", true));

        // cluster now up with 3 running nodes

        // server-task no longer running
        cluster.removeTask(cluster.cassandraNodeForHostname(slave1._2).get().getServerTask().getTaskId());

        // server-task cannot start again
        launchServer(cluster, slave1);
    }

    private CassandraFrameworkProtos.ExecutorMetadata launchServer(CassandraCluster cluster, Tuple2<Protos.SlaveID, String> slave) {
        Protos.Offer offer = createOffer(slave);
        List<CassandraFrameworkProtos.CassandraNodeTask> launchTasks = new ArrayList<>();
        List<CassandraFrameworkProtos.TaskDetails> submitTasks = new ArrayList<>();
        CassandraFrameworkProtos.CassandraNodeExecutor nodeExecutor = cluster.getTasksForOffer(offer, launchTasks, submitTasks);

        assertNotNull(nodeExecutor);
        assertEquals(1, launchTasks.size());
        assertEquals(0, submitTasks.size());
        CassandraFrameworkProtos.CassandraNodeTask launchTask = launchTasks.get(0);
        assertEquals(CassandraFrameworkProtos.TaskDetails.TaskType.CASSANDRA_SERVER_RUN, launchTask.getTaskDetails().getTaskType());
        return executorMetadataFor(slave, nodeExecutor.getExecutorId());
    }

    private CassandraFrameworkProtos.ExecutorMetadata launchExecutor(CassandraCluster cluster, Tuple2<Protos.SlaveID, String> slave, int nodeCount) {
        Protos.Offer offer = createOffer(slave);
        List<CassandraFrameworkProtos.CassandraNodeTask> launchTasks = new ArrayList<>();
        List<CassandraFrameworkProtos.TaskDetails> submitTasks = new ArrayList<>();
        CassandraFrameworkProtos.CassandraNodeExecutor nodeExecutor = cluster.getTasksForOffer(offer, launchTasks, submitTasks);

        assertEquals(nodeCount, cluster.getClusterState().getNodesCount());
        assertNotNull(nodeExecutor);
        assertEquals(1, launchTasks.size());
        assertEquals(0, submitTasks.size());
        CassandraFrameworkProtos.CassandraNodeTask launchTask = launchTasks.get(0);
        assertEquals(CassandraFrameworkProtos.TaskDetails.TaskType.EXECUTOR_METADATA, launchTask.getTaskDetails().getTaskType());
        return executorMetadataFor(slave, nodeExecutor.getExecutorId());
    }

    private void noopOnOffer(CassandraCluster cluster, Tuple2<Protos.SlaveID, String> slave, int nodeCount) {
        Protos.Offer offer = createOffer(slave);
        List<CassandraFrameworkProtos.CassandraNodeTask> launchTasks = new ArrayList<>();
        List<CassandraFrameworkProtos.TaskDetails> submitTasks = new ArrayList<>();
        CassandraFrameworkProtos.CassandraNodeExecutor nodeExecutor = cluster.getTasksForOffer(offer, launchTasks, submitTasks);

        assertEquals(nodeCount, cluster.getClusterState().getNodesCount());
        assertEquals(0, launchTasks.size());
        assertEquals(0, submitTasks.size());
        assertNull(nodeExecutor);
    }

    private static CassandraFrameworkProtos.ExecutorMetadata executorMetadataFor(Tuple2<Protos.SlaveID, String> slave1, String executorID) {
        return CassandraFrameworkProtos.ExecutorMetadata.newBuilder()
                .setExecutorId(executorID)
                .setIp(slave1._2)
                .build();
    }
}
