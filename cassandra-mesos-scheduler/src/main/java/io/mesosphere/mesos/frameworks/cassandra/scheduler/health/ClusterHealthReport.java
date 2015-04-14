package io.mesosphere.mesos.frameworks.cassandra.scheduler.health;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import org.jetbrains.annotations.NotNull;

import java.util.List;

import static com.google.common.collect.FluentIterable.from;

public final class ClusterHealthReport {

    private static final Function<ClusterHealthEvaluationResult<?>, Boolean> TO_IS_OK =
        new Function<ClusterHealthEvaluationResult<?>, Boolean>() {
            @Override
            public Boolean apply(final ClusterHealthEvaluationResult<?> input) {
                return input.isOk();
            }
        };

    private final boolean healthy;
    @NotNull
    private final List<ClusterHealthEvaluationResult<?>> results;

    public ClusterHealthReport(@NotNull final List<ClusterHealthEvaluationResult<?>> results) {
        this.results = results;
        healthy = from(results)
            .transform(TO_IS_OK)
            .allMatch(Predicates.equalTo(true));
    }

    public boolean isHealthy() {
        return healthy;
    }

    @NotNull
    public List<ClusterHealthEvaluationResult<?>> getResults() {
        return results;
    }

    @Override
    public String toString() {
        return "ClusterHealthReport{" +
            "healthy=" + healthy +
            ", results=" + results +
            '}';
    }
}
