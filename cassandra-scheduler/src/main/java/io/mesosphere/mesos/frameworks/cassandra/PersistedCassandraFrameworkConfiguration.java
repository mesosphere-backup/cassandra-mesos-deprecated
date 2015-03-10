/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        final int numberOfSeeds,
        final double cpuCores,
        final long memMb,
        final long diskMb,
        final long healthCheckIntervalSeconds,
        final long bootstrapGraceTimeSec
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
                        .setNumberOfSeeds(numberOfSeeds)
                        .setCpuCores(cpuCores)
                        .setMemMb(memMb)
                        .setDiskMb(diskMb)
                        .setHealthCheckIntervalSeconds(healthCheckIntervalSeconds)
                        .setBootstrapGraceTimeSeconds(bootstrapGraceTimeSec)
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

    public void healthCheckInterval(Duration interval) {
        setValue(
                CassandraFrameworkConfiguration.newBuilder(get())
                        .setHealthCheckIntervalSeconds(interval.getStandardSeconds())
                        .build()
        );
    }

    @NotNull
    public Duration bootstrapGraceTimeSeconds() {
        return Duration.standardSeconds(get().getBootstrapGraceTimeSeconds());
    }

    public void bootstrapGraceTimeSeconds(Duration interval) {
        setValue(
                CassandraFrameworkConfiguration.newBuilder(get())
                        .setBootstrapGraceTimeSeconds(interval.getStandardSeconds())
                        .build()
        );
    }

    @NotNull
    public String frameworkName() {
        return get().getFrameworkName();
    }

    public int numberOfNodes() {
        return get().getNumberOfNodes();
    }

    public void numberOfNodes(int numberOfNodes) {
        CassandraFrameworkConfiguration config = get();
        if (numberOfNodes <= 0 || config.getNumberOfSeeds() > numberOfNodes || numberOfNodes < config.getNumberOfNodes())
            throw new IllegalArgumentException("Cannot set number of nodes to " + numberOfNodes + ", current #nodes=" + config.getNumberOfNodes() + " #seeds=" + config.getNumberOfSeeds());

        setValue(
                CassandraFrameworkConfiguration.newBuilder(config)
                        .setNumberOfNodes(numberOfNodes)
                        .build()
        );
    }

    public int numberOfSeeds() {
        return get().getNumberOfSeeds();
    }

    public void numberOfSeeds(int seedCount) {
        CassandraFrameworkConfiguration config = get();
        if (seedCount <= 0 || seedCount > config.getNumberOfNodes())
            throw new IllegalArgumentException("Cannot set number of seeds to " + seedCount + ", current #nodes=" + config.getNumberOfNodes() + " #seeds=" + config.getNumberOfSeeds());

        // TODO changing the number of seeds requires rewriting cassandra.yaml (without restart)

        setValue(
                CassandraFrameworkConfiguration.newBuilder(config)
                        .setNumberOfSeeds(seedCount)
                        .build()
        );
    }

    @NotNull
    public String cassandraVersion() {
        return get().getCassandraVersion();
    }
}
