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
        @NotNull final CassandraFrameworkProtos.CassandraConfigRole defaultConfigRole,
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
                        .setDefaultConfigRole(fillConfigRoleGaps(defaultConfigRole))
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

    public static CassandraFrameworkProtos.CassandraConfigRole fillConfigRoleGaps(CassandraFrameworkProtos.CassandraConfigRole configRole) {
        CassandraFrameworkProtos.CassandraConfigRole.Builder builder = CassandraFrameworkProtos.CassandraConfigRole.newBuilder(configRole);
        if (builder.hasMemMb()) {
            if (!builder.hasMemJavaHeapMb()) {
                builder.setMemJavaHeapMb(Math.min(builder.getMemMb() / 2, 16384));
            }
            if (!builder.hasMemAssumeOffHeapMb()) {
                builder.setMemAssumeOffHeapMb(builder.getMemMb() - builder.getMemJavaHeapMb());
            }
        } else  {
            if (builder.hasMemJavaHeapMb()) {
                if (!builder.hasMemAssumeOffHeapMb()) {
                    builder.setMemAssumeOffHeapMb(builder.getMemJavaHeapMb());
                }
            } else {
                if (builder.hasMemAssumeOffHeapMb()) {
                    builder.setMemJavaHeapMb(builder.getMemAssumeOffHeapMb());
                } else {
                    throw new IllegalArgumentException("Config role is missing memory configuration");
                }
            }
            builder.setMemMb(builder.getMemJavaHeapMb() + builder.getMemAssumeOffHeapMb());
        }
        return builder.build();
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

    public CassandraFrameworkProtos.CassandraConfigRole getDefaultConfigRole() {
        return get().getDefaultConfigRole();
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

    public int numberOfNodes(int numberOfNodes) {
        CassandraFrameworkProtos.CassandraConfigRole configRole = getDefaultConfigRole();
        int newNodeCount = numberOfNodes - configRole.getNumberOfNodes();
        if (numberOfNodes <= 0 || configRole.getNumberOfSeeds() > numberOfNodes || newNodeCount <= 0)
            throw new IllegalArgumentException("Cannot set number of nodes to " + numberOfNodes + ", current #nodes=" + configRole.getNumberOfNodes() + " #seeds=" + configRole.getNumberOfSeeds());

        setDefaultConfigRole(CassandraFrameworkProtos.CassandraConfigRole.newBuilder(configRole)
            .setNumberOfNodes(numberOfNodes)
            .build());

        return newNodeCount;
    }

    private void setDefaultConfigRole(CassandraFrameworkProtos.CassandraConfigRole configRole) {
        setValue(
            CassandraFrameworkConfiguration.newBuilder(get())
                .setDefaultConfigRole(configRole)
                .build()
        );
    }

    @NotNull
    public String mesosRole() {
        return getDefaultConfigRole().getMesosRole();
    }
}
