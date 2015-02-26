package io.mesosphere.mesos.frameworks.cassandra;

import io.mesosphere.mesos.util.SystemClock;
import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos;
import org.apache.mesos.state.InMemoryState;
import org.apache.mesos.state.State;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

        Protos.Offer offer;
        final List<CassandraFrameworkProtos.CassandraNodeTask> launchTasks = new ArrayList<>();
        final List<CassandraFrameworkProtos.TaskDetails> submitTasks = new ArrayList<>();
        CassandraFrameworkProtos.CassandraNodeExecutor nodeExecutor;
        CassandraFrameworkProtos.CassandraNodeTask launchTask;

        // rollout slave #1

        offer = createOffer(slave1);
        launchTasks.clear();
        submitTasks.clear();
        nodeExecutor = cluster.getTasksForOffer(offer, launchTasks, submitTasks);

        assertEquals(1, cluster.getClusterState().getNodesCount());
        assertNotNull(nodeExecutor);
        assertEquals(1, launchTasks.size());
        assertEquals(0, submitTasks.size());
        launchTask = launchTasks.get(0);
        assertEquals(CassandraFrameworkProtos.TaskDetails.TaskType.EXECUTOR_METADATA, launchTask.getTaskDetails().getTaskType());

        // next offer must return nothing for the same slave !

        offer = createOffer(slave1);
        launchTasks.clear();
        submitTasks.clear();
        nodeExecutor = cluster.getTasksForOffer(offer, launchTasks, submitTasks);

        assertEquals(1, cluster.getClusterState().getNodesCount());
        assertEquals(0, launchTasks.size());
        assertEquals(0, submitTasks.size());
        assertNull(nodeExecutor);

    }

    private Protos.Offer createOffer(Tuple2<Protos.SlaveID, String> slave) {
        return Protos.Offer.newBuilder()
                .setFrameworkId(frameworkId)
                .setHostname(slave._2)
                .setId(Protos.OfferID.newBuilder().setValue(randomID()))
                .setSlaveId(slave._1)
                .build();
    }

    private static String randomID() {
        return UUID.randomUUID().toString();
    }
}
