package io.mesosphere.mesos.frameworks.cassandra.scheduler.health;

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraClusterHealthCheckHistory;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraClusterState;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraFrameworkConfiguration;
import org.jetbrains.annotations.NotNull;

final class ClusterHealthEvaluationContext {
    @NotNull
    final CassandraClusterState state;
    @NotNull
    final CassandraFrameworkConfiguration config;
    @NotNull
    final CassandraClusterHealthCheckHistory healthCheckHistory;

    public ClusterHealthEvaluationContext(
        @NotNull final CassandraFrameworkProtos.CassandraClusterState state,
        @NotNull final CassandraFrameworkProtos.CassandraFrameworkConfiguration config,
        @NotNull final CassandraFrameworkProtos.CassandraClusterHealthCheckHistory healthCheckHistory
    ) {
        this.state = state;
        this.config = config;
        this.healthCheckHistory = healthCheckHistory;
    }
}
