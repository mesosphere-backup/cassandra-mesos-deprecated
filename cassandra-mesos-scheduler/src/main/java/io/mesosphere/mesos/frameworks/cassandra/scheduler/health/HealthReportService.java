package io.mesosphere.mesos.frameworks.cassandra.scheduler.health;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.PersistedCassandraClusterHealthCheckHistory;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.PersistedCassandraClusterState;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.PersistedCassandraFrameworkConfiguration;
import io.mesosphere.mesos.util.Clock;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.frameworks.cassandra.scheduler.health.ClusterStateEvaluations.*;

public final class HealthReportService {
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthReportService.class);

    private static final Duration HEATH_CHECK_EXPIRATION_DURATION = Duration.standardMinutes(5);

    @NotNull
    private final PersistedCassandraClusterState clusterState;
    @NotNull
    private final PersistedCassandraFrameworkConfiguration config;
    @NotNull
    private final PersistedCassandraClusterHealthCheckHistory healthCheckHistory;
    @NotNull
    private final Clock clock;

    public HealthReportService(
        @NotNull final PersistedCassandraClusterState clusterState,
        @NotNull final PersistedCassandraFrameworkConfiguration config,
        @NotNull final PersistedCassandraClusterHealthCheckHistory healthCheckHistory,
        @NotNull final Clock clock
    ) {
        this.clusterState = clusterState;
        this.config = config;
        this.healthCheckHistory = healthCheckHistory;
        this.clock = clock;
    }

    @NotNull
    public ClusterHealthReport generateClusterHealthReport() {
        LOGGER.debug("> generateClusterHealthReport()");
        final ClusterHealthEvaluationContext context = new ClusterHealthEvaluationContext(
            clusterState.get(),
            config.get(),
            healthCheckHistory.get()
        );
        final ClusterHealthReport clusterHealthReport = getClusterHealthReport(clock, context);
        LOGGER.trace("< generateClusterHealthReport() = {}", clusterHealthReport);
        return clusterHealthReport;
    }

    @SuppressWarnings("unchecked")
    @NotNull
    @VisibleForTesting
    static ClusterHealthReport getClusterHealthReport(
        @NotNull final Clock clock,
        @NotNull final ClusterHealthEvaluationContext context
    ) {
        final long healthCheckTimeThreshold = clock.now().minus(HEATH_CHECK_EXPIRATION_DURATION).getMillis();
        final List<ClusterHealthEvaluationEntry<?>> evaluationEntries = newArrayList(
            nodeCount(),
            seedCount(),
            allHealthy(),
            operatingModeNormal(),
            lastHealthCheckNewerThan(healthCheckTimeThreshold),
            nodesHaveServerTask()
        );
        final List<ClusterHealthEvaluationResult<?>> results = newArrayList(
            from(evaluationEntries)
                .transform(new Function<ClusterHealthEvaluationEntry<?>, ClusterHealthEvaluationResult<?>>() {
                    @Override
                    public ClusterHealthEvaluationResult<?> apply(final ClusterHealthEvaluationEntry<?> input) {
                        return input.apply(context);
                    }
                })
        );
        return new ClusterHealthReport(results);
    }

}
