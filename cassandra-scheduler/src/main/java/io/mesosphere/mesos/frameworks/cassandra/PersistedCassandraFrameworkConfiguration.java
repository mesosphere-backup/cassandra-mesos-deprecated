package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraFrameworkConfiguration;
import io.mesosphere.mesos.util.ProtoUtils;
import org.apache.mesos.state.State;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;

public final class PersistedCassandraFrameworkConfiguration extends StatePersistedObject<CassandraFrameworkConfiguration> {

    public PersistedCassandraFrameworkConfiguration(
        @NotNull final State state,
        @NotNull final String frameworkName,
        @NotNull final String cassandraVersion,
        final int numberOfNodes,
        final double cpuCores,
        final long memMb,
        final long diskMb,
        final long healthCheckIntervalSeconds,
        final String mesosRoles
    ) {
        super(
            "CassandraFrameworkConfiguration",
            state,
            new Supplier<CassandraFrameworkConfiguration>() {
                @Override
                public CassandraFrameworkConfiguration get() {
                    return CassandraFrameworkConfiguration.newBuilder()
                        .setFrameworkName(frameworkName)
                        .setCassandraVersion(cassandraVersion)
                        .setNumberOfNodes(numberOfNodes)
                        .setCpuCores(cpuCores)
                        .setMemMb(memMb)
                        .setDiskMb(diskMb)
                        .setHealthCheckIntervalSeconds(healthCheckIntervalSeconds)
                        .setMesosRole(mesosRoles)
                        .build();
                }
            },
            new Function<byte[], CassandraFrameworkConfiguration>() {
                @Override
                public CassandraFrameworkConfiguration apply(final byte[] input) {
                    try {
                        return CassandraFrameworkConfiguration.parseFrom(input);
                    } catch (InvalidProtocolBufferException e) {
                        throw new ProtoUtils.RuntimeInvalidProtocolBufferException(e);
                    }
                }
            },
            new Function<CassandraFrameworkConfiguration, byte[]>() {
                @Override
                public byte[] apply(final CassandraFrameworkConfiguration input) {
                    return input.toByteArray();
                }
            }
        );
    }

    @NotNull
    public Optional<String> frameworkId() {
        return Optional.fromNullable(get().getFrameworkId());
    }

    public void frameworkId(@NotNull final String frameworkId) {
        setValue(
            CassandraFrameworkConfiguration.newBuilder(get())
                .setFrameworkId(frameworkId)
                .build()
        );
    }

    public double cpuCores() {
        return get().getCpuCores();
    }

    public long memMb() {
        return get().getMemMb();
    }

    public long diskMb() {
        return get().getDiskMb();
    }

    @NotNull
    public Duration healthCheckInterval() {
        return Duration.standardSeconds(get().getHealthCheckIntervalSeconds());
    }

    @NotNull
    public String frameworkName() {
        return get().getFrameworkName();
    }

    public int numberOfNodes() {
        return get().getNumberOfNodes();
    }

    @NotNull
    public String cassandraVersion() {
        return get().getCassandraVersion();
    }

    @NotNull
    public String mesosRole() {
        return get().getMesosRole();
    }
}
