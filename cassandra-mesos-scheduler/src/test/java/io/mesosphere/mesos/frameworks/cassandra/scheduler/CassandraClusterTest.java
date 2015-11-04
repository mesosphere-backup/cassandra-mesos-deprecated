package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNodeExecutor;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.FileDownload;
import io.mesosphere.mesos.util.Clock;
import io.mesosphere.mesos.util.SystemClock;
import org.apache.mesos.state.InMemoryState;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.util.ProtoUtils.*;
import static org.apache.mesos.Protos.*;
import static org.assertj.core.api.Assertions.assertThat;

public class CassandraClusterTest {

    @Test
    public void removeExecutor_cleansAllTasksAndExecutorInfo() throws Exception {
        final InMemoryState state = new InMemoryState();
        final Clock clock = new SystemClock();
        final ExecutorCounter execCounter = new ExecutorCounter(state, 0);
        final PersistedCassandraClusterState clusterState = new PersistedCassandraClusterState(state);
        final PersistedCassandraClusterHealthCheckHistory healthCheckHistory = new PersistedCassandraClusterHealthCheckHistory(state);
        final PersistedCassandraClusterJobs jobsState = new PersistedCassandraClusterJobs(state);
        final PersistedCassandraFrameworkConfiguration configuration = new PersistedCassandraFrameworkConfiguration(
            state, "cassandra.unit-test", 15, 15, "2.1.4", 1.0, 64, 64, 32, 1, 1, "*", "./backup", ".", true, false,
            "rack0", "dc0", Collections.<CassandraFrameworkProtos.ExternalDc>emptyList(), "cassandra.unit-test"
        );
        final CassandraCluster cluster1 = new CassandraCluster(
            clock,
            "http://localhost:1234/",
            execCounter,
            clusterState,
            healthCheckHistory,
            jobsState,
            configuration,
            new SeedManager(configuration, new ObjectMapper(), clock)
        );

        final Offer offer1 = Offer.newBuilder()
            .setId(OfferID.newBuilder().setValue("1"))
            .setFrameworkId(FrameworkID.newBuilder().setValue("fw1"))
            .setSlaveId(SlaveID.newBuilder().setValue("slave1"))
            .setHostname("localhost")
            .addAllResources(newArrayList(
                cpu(4, "*"),
                mem(4096, "*"),
                disk(20 * 1024, "*"),
                ports(newArrayList(7000L, 7001L, 7199L, 9042L, 9160L, 10000L), "*")
            ))
            .build();
        final TasksForOffer tasksForOffer = cluster1.getTasksForOffer(offer1);

        assertThat(tasksForOffer).isNotNull();
        //noinspection ConstantConditions
        assertThat(tasksForOffer.getExecutor()).isNotNull();

        final List<CassandraNode> nodes1 = clusterState.nodes();
        assertThat(nodes1).hasSize(1);
        final CassandraNode node1 = nodes1.get(0);
        assertThat(node1.getTasksList()).hasSize(1);
        assertThat(node1.hasCassandraNodeExecutor()).isTrue();
        final CassandraNodeExecutor executor1 = node1.getCassandraNodeExecutor();
        assertThat(executor1.getDownloadList()).hasSize(3);
        final List<FileDownload> from1 = newArrayList(
            from(executor1.getDownloadList())
                .filter(new Predicate<FileDownload>() {
                    @Override
                    public boolean apply(final FileDownload input) {
                        return input.getDownloadUrl().startsWith("http://localhost:1234/");
                    }
                })
        );
        assertThat(from1).hasSize(3);

        cluster1.removeExecutor("cassandra.unit-test.node.0.executor");

        final List<CassandraNode> afterRemove1 = clusterState.nodes();
        assertThat(afterRemove1).hasSize(1);
        final CassandraNode nodeAfterRemove1 = clusterState.nodes().get(0);
        assertThat(nodeAfterRemove1.getTasksList()).isEmpty();
        assertThat(nodeAfterRemove1.hasCassandraNodeExecutor()).isFalse();


        final CassandraCluster cluster2 = new CassandraCluster(
            clock,
            "http://localhost:4321/",
            execCounter,
            clusterState,
            healthCheckHistory,
            jobsState,
            configuration,
            new SeedManager(configuration, new ObjectMapper(), clock)
        );

        final Offer offer2 = Offer.newBuilder(offer1)
            .setId(OfferID.newBuilder().setValue("2"))
            .build();

        final TasksForOffer tasksForOffer2 = cluster2.getTasksForOffer(offer2);
        assertThat(tasksForOffer2).isNotNull();
        //noinspection ConstantConditions
        assertThat(tasksForOffer2.getExecutor()).isNotNull();

        final List<CassandraNode> nodes2 = clusterState.nodes();
        assertThat(nodes2).hasSize(1);
        final CassandraNode node2 = nodes2.get(0);
        assertThat(node2.getTasksList()).hasSize(1);
        assertThat(node2.hasCassandraNodeExecutor()).isTrue();
        final CassandraNodeExecutor executor2 = node2.getCassandraNodeExecutor();
        assertThat(executor2.getDownloadList()).hasSize(3);
        final List<FileDownload> from2 = newArrayList(
            from(executor2.getDownloadList())
                .filter(new Predicate<FileDownload>() {
                    @Override
                    public boolean apply(final FileDownload input) {
                        return input.getDownloadUrl().startsWith("http://localhost:4321/");
                    }
                })
        );
        assertThat(from2).hasSize(3);

    }
}
