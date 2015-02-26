package io.mesosphere.mesos.frameworks.cassandra;

import io.mesosphere.mesos.util.SystemClock;
import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos;
import org.apache.mesos.state.InMemoryState;
import org.apache.mesos.state.State;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class CassandraClusterStateTest {

    State state;

    private final Protos.FrameworkID frameworkId = Protos.FrameworkID.newBuilder().setValue(randomID()).build();

    @Test
    public void testLaunchNewCluster() {

        // start with clean state
        state = new InMemoryState();

        PersistedCassandraFrameworkConfiguration configuration = new PersistedCassandraFrameworkConfiguration(
                state,
                "test-cluster",
                "2.1.2",
                3, // node count
                2, // seed count
                2, // CPUs
                4096, // memMb
                4096, // diskMb
                10, // health-check
                10 // bootstrap-grace-time
        );

        PersistedCassandraClusterHealthCheckHistory healthCheckHistory = new PersistedCassandraClusterHealthCheckHistory(state);
        PersistedCassandraClusterState clusterState = new PersistedCassandraClusterState(state);

        CassandraCluster cluster = new CassandraCluster(new SystemClock(),
                "http://127.0.0.1:65535",
                new ExecutorCounter(state, 0L),
                clusterState,
                healthCheckHistory,
                configuration);

        // our slaves

        Tuple2<Protos.SlaveID, String> slave1 = Tuple2.tuple2(Protos.SlaveID.newBuilder().setValue(randomID()).build(), "127.1.1.1");
        Tuple2<Protos.SlaveID, String> slave2 = Tuple2.tuple2(Protos.SlaveID.newBuilder().setValue(randomID()).build(), "127.2.2.2");
        Tuple2<Protos.SlaveID, String> slave3 = Tuple2.tuple2(Protos.SlaveID.newBuilder().setValue(randomID()).build(), "127.3.3.3");

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

        cluster.recordHealthCheck(executorMetadata1.getExecutorId(), healthCheckDetailsSuccess("RUNNING", true));
        assertTrue(cluster.lastHealthCheck(executorMetadata1.getExecutorId()).getDetails().getHealthy());
        assertEquals("RUNNING", cluster.lastHealthCheck(executorMetadata1.getExecutorId()).getDetails().getInfo().getOperationMode());
        cluster.recordHealthCheck(executorMetadata2.getExecutorId(), healthCheckDetailsFailed());
        assertFalse(cluster.lastHealthCheck(executorMetadata2.getExecutorId()).getDetails().getHealthy());
        // node#3 can start now
        launchServer(cluster, slave3);
        
    }

    private static CassandraFrameworkProtos.HealthCheckDetails healthCheckDetailsFailed() {
        return CassandraFrameworkProtos.HealthCheckDetails.newBuilder()
                .setHealthy(false)
                .setMsg("foo bar")
                .build();
    }

    private static CassandraFrameworkProtos.HealthCheckDetails healthCheckDetailsSuccess(String operationMode, boolean joined) {
        return CassandraFrameworkProtos.HealthCheckDetails.newBuilder()
                .setHealthy(true)
                .setInfo(CassandraFrameworkProtos.NodeInfo.newBuilder()
                    .setClusterName("cluster-name")
                        .setDataCenter("dc")
                        .setRack("rac")
                        .setJoined(joined)
                        .setOperationMode(operationMode)
                        .setUptimeMillis(1234)
                        .setVersion("2.1.2")
                )
                .build();
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

    private Protos.Offer createOffer(Tuple2<Protos.SlaveID, String> slave) {
        Protos.Offer.Builder builder = Protos.Offer.newBuilder()
                .setFrameworkId(frameworkId)
                .setHostname(slave._2)
                .setId(Protos.OfferID.newBuilder().setValue(randomID()))
                .setSlaveId(slave._1);

        builder.addResources(Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8d)));
        builder.addResources(Protos.Resource.newBuilder()
                .setName("mem")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)));
        builder.addResources(Protos.Resource.newBuilder()
                .setName("disk")
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(8192)));
        builder.addResources(Protos.Resource.newBuilder()
                .setName("ports")
                .setType(Protos.Value.Type.RANGES)
                .setRanges(Protos.Value.Ranges.newBuilder()
                        .addRange(Protos.Value.Range.newBuilder().setBegin(7000).setEnd(10000)))
                );

        return builder.build();
    }

    private static String randomID() {
        return UUID.randomUUID().toString();
    }
}
