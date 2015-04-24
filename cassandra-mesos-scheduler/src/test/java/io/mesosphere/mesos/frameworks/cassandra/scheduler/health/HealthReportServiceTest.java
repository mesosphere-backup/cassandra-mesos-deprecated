package io.mesosphere.mesos.frameworks.cassandra.scheduler.health;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ListMultimap;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;
import io.mesosphere.mesos.util.Clock;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

import static com.google.common.base.Optional.of;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.frameworks.cassandra.scheduler.health.HealthReportService.getClusterHealthReport;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class HealthReportServiceTest {

    @Mock
    private Clock clock;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void brandNewClusterIsUnhealthy() throws Exception {
        final int numberOfNodes = 3;
        final int numberOfSeeds = 1;

        final CassandraClusterState.Builder clusterState =
            CassandraClusterState.newBuilder();

        final CassandraFrameworkConfiguration.Builder config =
            CassandraFrameworkConfiguration.newBuilder()
                .setFrameworkName("cassandra.testing")
                .setTargetNumberOfNodes(numberOfNodes)
                .setTargetNumberOfSeeds(numberOfSeeds);

        final CassandraClusterHealthCheckHistory.Builder healthCheckHistory =
            CassandraClusterHealthCheckHistory.newBuilder()
                .setMaxEntriesPerNode(5);

        final Instant now = Instant.now();
        final Long healthCheckExpiration = now.minus(Duration.standardMinutes(5)).getMillis();
        when(clock.now()).thenReturn(now);

        final ClusterHealthReport report = getClusterHealthReport(
            clock,
            new ClusterHealthEvaluationContext(
                clusterState.build(),
                config.build(),
                healthCheckHistory.build()
            )
        );

        assertThat(report.isHealthy()).isFalse();
        final ListMultimap<String, ClusterHealthEvaluationResult<?>> index = from(report.getResults())
            .index(new Function<ClusterHealthEvaluationResult<?>, String>() {
                @Override
                public String apply(final ClusterHealthEvaluationResult<?> input) {
                    return input.getName();
                }
            });

        final ClusterHealthEvaluationResult<?> nodeCount = index.get("nodeCount").get(0);
        assertThat(nodeCount.getExpected()).isEqualTo(3);
        assertThat(nodeCount.getActual()).isEqualTo(0);
        assertThat(nodeCount.isOk()).isFalse();

        final ClusterHealthEvaluationResult<?> seedCount = index.get("seedCount").get(0);
        assertThat(seedCount.getExpected()).isEqualTo(1);
        assertThat(seedCount.getActual()).isEqualTo(0);
        assertThat(seedCount.isOk()).isFalse();

        final ClusterHealthEvaluationResult<?> allHealthy = index.get("allHealthy").get(0);
        assertThat(allHealthy.getExpected()).isEqualTo(newArrayList(true, true, true));
        assertThat(allHealthy.getActual()).isEqualTo(newArrayList());
        assertThat(allHealthy.isOk()).isFalse();

        final ClusterHealthEvaluationResult<?> operatingModeNormal = index.get("operatingModeNormal").get(0);
        assertThat(operatingModeNormal.getExpected()).isEqualTo(newArrayList(normal(), normal(), normal()));
        assertThat(operatingModeNormal.getActual()).isEqualTo(newArrayList());
        assertThat(operatingModeNormal.isOk()).isFalse();

        final ClusterHealthEvaluationResult<?> lastHealthCheckNewerThan = index.get("lastHealthCheckNewerThan").get(0);
        assertThat(lastHealthCheckNewerThan.getExpected()).isEqualTo(newArrayList(healthCheckExpiration, healthCheckExpiration, healthCheckExpiration));
        assertThat(lastHealthCheckNewerThan.getActual()).isEqualTo(newArrayList());
        assertThat(lastHealthCheckNewerThan.isOk()).isFalse();
    }

    @Test
    public void initializedClusterIsHealthy() throws Exception {
        final int numberOfNodes = 3;
        final int numberOfSeeds = 2;
        final Instant now = Instant.now();
        final Long healthCheckExpiration = now.minus(Duration.standardMinutes(5)).getMillis();

        final CassandraClusterState.Builder clusterState =
            CassandraClusterState.newBuilder()
                .addNodes(
                    CassandraNode.newBuilder()
                        .setHostname("host1")
                        .setSeed(true)
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.METADATA)
                                .setTaskId("host1-metadata")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.SERVER)
                                .setTaskId("host1-server")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                )
                .addNodes(
                    CassandraNode.newBuilder()
                        .setHostname("host2")
                        .setSeed(true)
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.METADATA)
                                .setTaskId("host1-metadata")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.SERVER)
                                .setTaskId("host1-server")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                )
                .addNodes(
                    CassandraNode.newBuilder()
                        .setHostname("host3")
                        .setSeed(false)
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.METADATA)
                                .setTaskId("host1-metadata")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.SERVER)
                                .setTaskId("host1-server")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                );

        final CassandraFrameworkConfiguration.Builder config =
            CassandraFrameworkConfiguration.newBuilder()
                .setFrameworkName("cassandra.testing")
                .setHealthCheckIntervalSeconds(60)
                .setBootstrapGraceTimeSeconds(120)
                .setTargetNumberOfNodes(numberOfNodes)
                .setTargetNumberOfSeeds(numberOfSeeds);

        final CassandraClusterHealthCheckHistory.Builder healthCheckHistory =
            CassandraClusterHealthCheckHistory.newBuilder()
                .setMaxEntriesPerNode(5)
                .addEntries(
                    HealthCheckHistoryEntry.newBuilder()
                        .setTimestampStart(1)
                        .setTimestampEnd(healthCheckExpiration)
                        .setExecutorId("exec1")
                        .setDetails(
                            HealthCheckDetails.newBuilder()
                                .setHealthy(true)
                                .setInfo(
                                    NodeInfo.newBuilder()
                                        .setOperationMode("NORMAL")
                                )
                        )
                )
                .addEntries(
                    HealthCheckHistoryEntry.newBuilder()
                        .setTimestampStart(1)
                        .setTimestampEnd(healthCheckExpiration)
                        .setExecutorId("exec2")
                        .setDetails(
                            HealthCheckDetails.newBuilder()
                                .setHealthy(true)
                                .setInfo(
                                    NodeInfo.newBuilder()
                                        .setOperationMode("NORMAL")
                                )
                        )
                )
                .addEntries(
                    HealthCheckHistoryEntry.newBuilder()
                        .setTimestampStart(1)
                        .setTimestampEnd(healthCheckExpiration + 1)
                        .setExecutorId("exec3")
                        .setDetails(
                            HealthCheckDetails.newBuilder()
                                .setHealthy(true)
                                .setInfo(
                                    NodeInfo.newBuilder()
                                        .setOperationMode("NORMAL")
                                )
                        )
                );

        when(clock.now()).thenReturn(now);

        final ClusterHealthReport report = getClusterHealthReport(
            clock,
            new ClusterHealthEvaluationContext(
                clusterState.build(),
                config.build(),
                healthCheckHistory.build()
            )
        );

        assertThat(report.isHealthy()).isTrue();
        final ListMultimap<String, ClusterHealthEvaluationResult<?>> index = from(report.getResults())
            .index(new Function<ClusterHealthEvaluationResult<?>, String>() {
                @Override
                public String apply(final ClusterHealthEvaluationResult<?> input) {
                    return input.getName();
                }
            });

        final ClusterHealthEvaluationResult<?> nodeCount = index.get("nodeCount").get(0);
        assertThat(nodeCount.getExpected()).isEqualTo(3);
        assertThat(nodeCount.getActual()).isEqualTo(3);
        assertThat(nodeCount.isOk()).isTrue();

        final ClusterHealthEvaluationResult<?> seedCount = index.get("seedCount").get(0);
        assertThat(seedCount.getExpected()).isEqualTo(2);
        assertThat(seedCount.getActual()).isEqualTo(2);
        assertThat(seedCount.isOk()).isTrue();

        final ClusterHealthEvaluationResult<?> allHealthy = index.get("allHealthy").get(0);
        assertThat(allHealthy.getExpected()).isEqualTo(newArrayList(true, true, true));
        assertThat(allHealthy.getActual()).isEqualTo(newArrayList(true, true, true));
        assertThat(allHealthy.isOk()).isTrue();

        final ClusterHealthEvaluationResult<?> operatingModeNormal = index.get("operatingModeNormal").get(0);
        assertThat(operatingModeNormal.getExpected()).isEqualTo(newArrayList(normal(), normal(), normal()));
        assertThat(operatingModeNormal.getActual()).isEqualTo(newArrayList(normal(), normal(), normal()));
        assertThat(operatingModeNormal.isOk()).isTrue();

        final ClusterHealthEvaluationResult<?> lastHealthCheckNewerThan = index.get("lastHealthCheckNewerThan").get(0);
        assertThat(lastHealthCheckNewerThan.getExpected()).isEqualTo(newArrayList(healthCheckExpiration, healthCheckExpiration, healthCheckExpiration));
        assertThat(lastHealthCheckNewerThan.getActual()).isEqualTo(newArrayList(healthCheckExpiration, healthCheckExpiration, healthCheckExpiration + 1));
        assertThat(lastHealthCheckNewerThan.isOk()).isTrue();
    }

    @Test
    public void nodeThatUsedToBeUnhealthyButIsNowHealthyIsSeenAsHealthy() throws Exception {
        final int numberOfNodes = 1;
        final int numberOfSeeds = 1;

        final CassandraClusterState.Builder clusterState =
            CassandraClusterState.newBuilder();

        final CassandraFrameworkConfiguration.Builder config =
            CassandraFrameworkConfiguration.newBuilder()
                .setFrameworkName("cassandra.testing")
                .setTargetNumberOfNodes(numberOfNodes)
                .setTargetNumberOfNodes(numberOfSeeds);

        final CassandraClusterHealthCheckHistory.Builder healthCheckHistory =
            CassandraClusterHealthCheckHistory.newBuilder()
                .setMaxEntriesPerNode(5)
                .addEntries(
                    HealthCheckHistoryEntry.newBuilder()
                        .setExecutorId("exec1")
                        .setTimestampStart(1)
                        .setTimestampEnd(5)
                        .setDetails(
                            HealthCheckDetails.newBuilder()
                                .setInfo(
                                    NodeInfo.newBuilder()
                                        .setOperationMode("NORMAL")
                                )
                        )
                );
        testOperationalModeActuallyIs(clusterState, config, healthCheckHistory, normal(), true);
        healthCheckHistory.addEntries(
            HealthCheckHistoryEntry.newBuilder()
                .setExecutorId("exec1")
                .setTimestampStart(6)
                .setTimestampEnd(500)
                .setDetails(
                    HealthCheckDetails.newBuilder()
                        .setInfo(
                            NodeInfo.newBuilder()
                                .setOperationMode("LEAVING")
                        )
                )
        );
        testOperationalModeActuallyIs(clusterState, config, healthCheckHistory, of("LEAVING"), false);
        healthCheckHistory.addEntries(
            HealthCheckHistoryEntry.newBuilder()
                .setExecutorId("exec1")
                .setTimestampStart(501)
                .setTimestampEnd(1000)
                .setDetails(
                    HealthCheckDetails.newBuilder()
                        .setInfo(
                            NodeInfo.newBuilder()
                                .setOperationMode("STARTING")
                        )
                )
        );
        testOperationalModeActuallyIs(clusterState, config, healthCheckHistory, of("STARTING"), false);
        healthCheckHistory.addEntries(
            HealthCheckHistoryEntry.newBuilder()
                .setExecutorId("exec1")
                .setTimestampStart(1001)
                .setTimestampEnd(1500)
                .setDetails(
                    HealthCheckDetails.newBuilder()
                        .setInfo(
                            NodeInfo.newBuilder()
                                .setOperationMode("NORMAL")
                        )
                )
        );
        testOperationalModeActuallyIs(clusterState, config, healthCheckHistory, normal(), true);

    }

    @Test
    public void allNodesHaveServerTasks() throws Exception {
        final int numberOfNodes = 3;
        final int numberOfSeeds = 1;

        final CassandraClusterState.Builder clusterState =
            CassandraClusterState.newBuilder()
                .addNodes(
                    CassandraNode.newBuilder()
                        .setSeed(true)
                        .setHostname("host1")
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.METADATA)
                                .setTaskId("host1-metadata")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.SERVER)
                                .setTaskId("host1-server")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                )
                .addNodes(
                    CassandraNode.newBuilder()
                        .setSeed(false)
                        .setHostname("host2")
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.METADATA)
                                .setTaskId("host2-metadata")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.SERVER)
                                .setTaskId("host2-server")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                )
                .addNodes(
                    CassandraNode.newBuilder()
                        .setSeed(false)
                        .setHostname("host3")
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.METADATA)
                                .setTaskId("host3-metadata")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.SERVER)
                                .setTaskId("host3-server")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                )
            ;

        final CassandraFrameworkConfiguration.Builder config =
            CassandraFrameworkConfiguration.newBuilder()
                .setFrameworkName("cassandra.testing")
                .setTargetNumberOfNodes(numberOfNodes)
                .setTargetNumberOfSeeds(numberOfSeeds);

        final CassandraClusterHealthCheckHistory.Builder healthCheckHistory =
            CassandraClusterHealthCheckHistory.newBuilder()
                .setMaxEntriesPerNode(5);

        final ClusterHealthEvaluationResult<List<Boolean>> result =
            ClusterStateEvaluations.nodesHaveServerTask()
                .apply(
                    new ClusterHealthEvaluationContext(
                        clusterState.build(),
                        config.build(),
                        healthCheckHistory.build()
                    )
                );

        assertThat(result.getExpected()).hasSize(3);
        assertThat(result.getActual()).hasSize(3);
        assertThat(result.isOk()).isTrue();
    }

    @Test
    public void allNodesHaveServerTasks_oneNodeWithOnlyMetadataTask() throws Exception {
        final int numberOfNodes = 3;
        final int numberOfSeeds = 1;

        final CassandraClusterState.Builder clusterState =
            CassandraClusterState.newBuilder()
                .addNodes(
                    CassandraNode.newBuilder()
                        .setSeed(true)
                        .setHostname("host1")
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.METADATA)
                                .setTaskId("host1-metadata")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.SERVER)
                                .setTaskId("host1-server")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                )
                .addNodes(
                    CassandraNode.newBuilder()
                        .setSeed(false)
                        .setHostname("host2")
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.METADATA)
                                .setTaskId("host2-metadata")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.SERVER)
                                .setTaskId("host2-server")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                )
                .addNodes(
                    CassandraNode.newBuilder()
                        .setSeed(false)
                        .setHostname("host3")
                        .addTasks(
                            CassandraNodeTask.newBuilder()
                                .setType(CassandraNodeTask.NodeTaskType.METADATA)
                                .setTaskId("host3-metadata")
                                .setResources(TaskResources.getDefaultInstance())
                        )
                );

        final CassandraFrameworkConfiguration.Builder config =
            CassandraFrameworkConfiguration.newBuilder()
                .setFrameworkName("cassandra.testing")
                .setTargetNumberOfNodes(numberOfNodes)
                .setTargetNumberOfSeeds(numberOfSeeds);

        final CassandraClusterHealthCheckHistory.Builder healthCheckHistory =
            CassandraClusterHealthCheckHistory.newBuilder()
                .setMaxEntriesPerNode(5);

        final ClusterHealthEvaluationResult<List<Boolean>> result =
            ClusterStateEvaluations.nodesHaveServerTask()
                .apply(
                    new ClusterHealthEvaluationContext(
                        clusterState.build(),
                        config.build(),
                        healthCheckHistory.build()
                    )
                );

        assertThat(result.getExpected()).hasSize(3);
        assertThat(result.getActual()).hasSize(3);
        assertThat(result.isOk()).isFalse();
    }

    @NotNull
    private static Optional<String> normal() {
        return of("NORMAL");
    }

    private static void testOperationalModeActuallyIs(
        @NotNull final CassandraClusterState.Builder clusterState,
        @NotNull final CassandraFrameworkConfiguration.Builder config,
        @NotNull final CassandraClusterHealthCheckHistory.Builder healthCheckHistory,
        @NotNull final Optional<String> expectedActualValue,
        final boolean ok) {
        final ClusterHealthEvaluationResult<List<Optional<String>>> result = ClusterStateEvaluations.operatingModeNormal().apply(
            new ClusterHealthEvaluationContext(
                clusterState.build(),
                config.build(),
                healthCheckHistory.build()
            )
        );

        assertThat(result.getExpected()).isEqualTo(newArrayList(normal()));
        assertThat(result.getActual()).isEqualTo(newArrayList(expectedActualValue));
        assertThat(result.isOk()).isEqualTo(ok);
    }

}
