package io.mesosphere.mesos.frameworks.cassandra.scheduler.health;

import org.jetbrains.annotations.NotNull;

public final class ClusterHealthEvaluationResult<T> {
    @NotNull
    private final String name;
    private final boolean ok;
    @NotNull
    private final T expected;
    @NotNull
    private final T actual;

    public ClusterHealthEvaluationResult(
        @NotNull final String name,
        final boolean ok,
        @NotNull final T expected,
        @NotNull final T actual
    ) {
        this.name = name;
        this.expected = expected;
        this.actual = actual;
        this.ok = ok;
    }

    @NotNull
    public String getName() {
        return name;
    }

    public boolean isOk() {
        return ok;
    }

    @NotNull
    public T getExpected() {
        return expected;
    }

    @NotNull
    public T getActual() {
        return actual;
    }

    @Override
    public String toString() {
        return "ClusterHealthEvaluationResult{" +
            "name='" + name + '\'' +
            ", ok=" + ok +
            ", expected=" + expected +
            ", actual=" + actual +
            '}';
    }
}
