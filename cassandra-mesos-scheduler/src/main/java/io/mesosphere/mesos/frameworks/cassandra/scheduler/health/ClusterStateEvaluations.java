package io.mesosphere.mesos.frameworks.cassandra.scheduler.health;

import com.google.common.base.*;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Ordering;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNode;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraNodeTask;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.health.ClusterHealthEvaluationEntry.Equivalence;
import io.mesosphere.mesos.util.Tuple2;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.HealthCheckHistoryEntry;
import static io.mesosphere.mesos.util.Functions.listFullOf;
import static io.mesosphere.mesos.util.Functions.zip;

final class ClusterStateEvaluations {

    private static final Ordering<Comparable> REVERSE = Ordering.natural().reverse();

    private ClusterStateEvaluations() {}

    @NotNull
    public static ClusterHealthEvaluationEntry<Integer> nodeCount() {
        return new ClusterHealthEvaluationEntry<>(
            "nodeCount",
            new Function<ClusterHealthEvaluationContext, Integer>() {
                @Override
                public Integer apply(final ClusterHealthEvaluationContext input) {
                    return input.config.getTargetNumberOfNodes();
                }
            },
            new Function<ClusterHealthEvaluationContext, Integer>() {
                @Override
                public Integer apply(final ClusterHealthEvaluationContext input) {
                    return input.state.getNodesList().size();
                }
            });
    }

    @NotNull
    public static ClusterHealthEvaluationEntry<Integer> seedCount() {
        return new ClusterHealthEvaluationEntry<>(
            "seedCount",
            new Function<ClusterHealthEvaluationContext, Integer>() {
                @Override
                public Integer apply(final ClusterHealthEvaluationContext input) {
                    return input.config.getTargetNumberOfSeeds();
                }
            },
            new Function<ClusterHealthEvaluationContext, Integer>() {
                @Override
                public Integer apply(final ClusterHealthEvaluationContext input) {
                    return from(input.state.getNodesList())
                        .filter(new Predicate<CassandraNode>() {
                            @Override
                            public boolean apply(final CassandraNode input) {
                                return input.getSeed();
                            }
                        })
                        .size();
                }
            });
    }

    @NotNull
    public static ClusterHealthEvaluationEntry<List<Boolean>> allHealthy() {
        return new ClusterHealthEvaluationEntry<>(
            "allHealthy",
            new Function<ClusterHealthEvaluationContext, List<Boolean>>() {
                @Override
                public List<Boolean> apply(final ClusterHealthEvaluationContext input) {
                    return listFullOf(input.config.getTargetNumberOfNodes(), literalValue(true));
                }
            },
            new Function<ClusterHealthEvaluationContext, List<Boolean>>() {
                @Override
                public List<Boolean> apply(final ClusterHealthEvaluationContext input) {
                    return newArrayList(
                        getLatestHealthCheckHistoryEntries(input)
                            .transform(new Function<HealthCheckHistoryEntry, Boolean>() {
                                @Override
                                public Boolean apply(final HealthCheckHistoryEntry input) {
                                    return input.getDetails() != null && input.getDetails().getHealthy();
                                }
                            })
                    );
                }
            }
        );
    }

    @NotNull
    public static ClusterHealthEvaluationEntry<List<Optional<String>>> operatingModeNormal() {
        return new ClusterHealthEvaluationEntry<>(
            "operatingModeNormal",
            new Function<ClusterHealthEvaluationContext, List<Optional<String>>>() {
                @Override
                public List<Optional<String>> apply(final ClusterHealthEvaluationContext input) {
                    return listFullOf(input.config.getTargetNumberOfNodes(), literalValue(Optional.of("NORMAL")));
                }
            },
            new Function<ClusterHealthEvaluationContext, List<Optional<String>>>() {
                @Override
                public List<Optional<String>> apply(final ClusterHealthEvaluationContext input) {
                    return newArrayList(
                        getLatestHealthCheckHistoryEntries(input)
                            .transform(new Function<HealthCheckHistoryEntry, Optional<String>>() {
                                @Override
                                public Optional<String> apply(final HealthCheckHistoryEntry input) {
                                    if (input.getDetails() == null) {
                                        return Optional.absent();
                                    }
                                    if (input.getDetails().getInfo() == null) {
                                        return Optional.absent();
                                    }
                                    return Optional.of(input.getDetails().getInfo().getOperationMode());
                                }
                            })
                    );
                }
            }
        );
    }

    @NotNull
    public static ClusterHealthEvaluationEntry<List<Long>> lastHealthCheckNewerThan(final long timestamp) {
        return new ClusterHealthEvaluationEntry<>(
            "lastHealthCheckNewerThan",
            new Function<ClusterHealthEvaluationContext, List<Long>>() {
                @Override
                public List<Long> apply(final ClusterHealthEvaluationContext input) {
                    return listFullOf(input.config.getTargetNumberOfNodes(), literalValue(timestamp));
                }
            },
            new Function<ClusterHealthEvaluationContext, List<Long>>() {
                @Override
                public List<Long> apply(final ClusterHealthEvaluationContext input) {
                    return newArrayList(
                        getLatestHealthCheckHistoryEntries(input)
                            .transform(new Function<HealthCheckHistoryEntry, Long>() {
                                @Override
                                public Long apply(final HealthCheckHistoryEntry input) {
                                    return input.getTimestampEnd();
                                }
                            })
                    );
                }
            },
            new Equivalence<List<Long>>() {
                public boolean apply(@NotNull final List<Long> a, @NotNull final List<Long> b) {
                    return a.size() == b.size()
                        && from(zip(a, b))
                            .transform(new Function<Tuple2<Long, Long>, Boolean>() {
                                @Override
                                public Boolean apply(final Tuple2<Long, Long> input) {
                                    return input._1 <= input._2;
                                }
                            })
                            .allMatch(Predicates.equalTo(true));
                }
            }
        );
    }

    @NotNull
    public static ClusterHealthEvaluationEntry<List<Boolean>> nodesHaveServerTask() {
        return new ClusterHealthEvaluationEntry<>(
            "nodesHaveServerTask",
            new Function<ClusterHealthEvaluationContext, List<Boolean>>() {
                @Override
                public List<Boolean> apply(final ClusterHealthEvaluationContext input) {
                    return listFullOf(input.config.getTargetNumberOfNodes(), literalValue(true));
                }
            },
            new Function<ClusterHealthEvaluationContext, List<Boolean>>() {
                @Override
                public List<Boolean> apply(final ClusterHealthEvaluationContext input) {
                    return newArrayList(
                        from(input.state.getNodesList())
                            .transform(new Function<CassandraNode, Boolean>() {
                                @Override
                                public Boolean apply(final CassandraNode input) {
                                    final FluentIterable<CassandraNodeTask> nodesWithServerTasks = from(input.getTasksList())
                                        .filter(new Predicate<CassandraNodeTask>() {
                                            @Override
                                            public boolean apply(final CassandraNodeTask input) {
                                                return input.getType() == CassandraNodeTask.NodeTaskType.SERVER;
                                            }
                                        });
                                    return !nodesWithServerTasks.isEmpty();
                                }
                            })
                    );
                }
            }
        );
    }

    @NotNull
    private static FluentIterable<HealthCheckHistoryEntry> getLatestHealthCheckHistoryEntries(@NotNull final ClusterHealthEvaluationContext input) {
        final ListMultimap<String, HealthCheckHistoryEntry> index = from(input.healthCheckHistory.getEntriesList())
            .index(new Function<HealthCheckHistoryEntry, String>() {
                @Override
                public String apply(final HealthCheckHistoryEntry input) {
                    return input.getExecutorId();
                }
            });

        return from(index.asMap().entrySet())
            .transform(new Function<Map.Entry<String, Collection<HealthCheckHistoryEntry>>, HealthCheckHistoryEntry>() {
                @Override
                public HealthCheckHistoryEntry apply(final Map.Entry<String, Collection<HealthCheckHistoryEntry>> input) {
                    final ImmutableList<HealthCheckHistoryEntry> sorted = from(input.getValue())
                        .toSortedList(new Comparator<HealthCheckHistoryEntry>() {
                            @Override
                            public int compare(final HealthCheckHistoryEntry o1, final HealthCheckHistoryEntry o2) {
                                return REVERSE.compare(o1.getTimestampEnd(), o2.getTimestampEnd());
                            }
                        });

                    return sorted.get(0);
                }
            });
    }

    @NotNull
    private static <A> Supplier<A> literalValue(@NotNull final A a) {
        return Suppliers.ofInstance(a);
    }
}
