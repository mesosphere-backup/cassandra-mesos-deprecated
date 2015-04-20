package io.mesosphere.mesos.frameworks.cassandra.scheduler.health;

import com.google.common.base.Function;
import org.jetbrains.annotations.NotNull;

import static com.google.common.base.Preconditions.checkNotNull;

final class ClusterHealthEvaluationEntry<T> implements Function<ClusterHealthEvaluationContext, ClusterHealthEvaluationResult<T>> {

    @NotNull
    private final String name;
    @NotNull
    private final Function<ClusterHealthEvaluationContext, T> expectedExtractor;
    @NotNull
    private final Function<ClusterHealthEvaluationContext, T> actualExtractor;
    @NotNull
    private final Equivalence<T> equivalence;

    public ClusterHealthEvaluationEntry(
        @NotNull final String name,
        @NotNull final Function<ClusterHealthEvaluationContext, T> expectedExtractor,
        @NotNull final Function<ClusterHealthEvaluationContext, T> actualExtractor
    ) {
        this(name, expectedExtractor, actualExtractor, ClusterHealthEvaluationEntry.<T>defaultEquivalence());
    }

    public ClusterHealthEvaluationEntry(
        @NotNull final String name,
        @NotNull final Function<ClusterHealthEvaluationContext, T> expectedExtractor,
        @NotNull final Function<ClusterHealthEvaluationContext, T> actualExtractor,
        @NotNull final Equivalence<T> equivalence
    ) {
        this.name = name;
        this.expectedExtractor = expectedExtractor;
        this.actualExtractor = actualExtractor;
        this.equivalence = equivalence;
    }

    @Override
    @NotNull
    public ClusterHealthEvaluationResult<T> apply(final ClusterHealthEvaluationContext input) {
        final T expected = checkNotNull(expectedExtractor.apply(input));
        final T actual = checkNotNull(actualExtractor.apply(input));
        final boolean equivalent = equivalence.apply(expected, actual);
        return new ClusterHealthEvaluationResult<>(
            name,
            equivalent,
            expected,
            actual
        );
    }

    public interface Equivalence<T> {
        boolean apply(@NotNull final T a, @NotNull final T b);
    }

    @NotNull
    static <T> DefaultEquivalence<T> defaultEquivalence() {
        return new DefaultEquivalence<>();
    }

    static final class DefaultEquivalence<T> implements Equivalence<T> {
        @Override
        public boolean apply(@NotNull final T a, @NotNull final T b) {
            return a.equals(b);
        }
    }
}
