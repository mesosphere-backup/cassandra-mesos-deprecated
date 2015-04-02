/**
 *    Copyright (C) 2015 Mesosphere, Inc.
 *
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
package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.CassandraFrameworkConfiguration;
import io.mesosphere.mesos.util.ProtoUtils;
import org.apache.mesos.state.State;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;

public final class PersistedCassandraFrameworkConfiguration extends StatePersistedObject<CassandraFrameworkConfiguration> {

    public PersistedCassandraFrameworkConfiguration(
        @NotNull final State state,
        @NotNull final String frameworkName,
        final long healthCheckIntervalSeconds,
        final long bootstrapGraceTimeSec,
        final String cassandraVersion,
        final double cpuCores,
        final long diskMb,
        final long memMb,
        final long javeHeapMb,
        final int executorCount,
        final int seedCount,
        final String mesosRole,
        final String dataDirectory,
        final boolean jmxLocal,
        final boolean jmxNoAuthentication
    ) {
        super(
            "CassandraFrameworkConfiguration",
            state,
            new Supplier<CassandraFrameworkConfiguration>() {
                @Override
                public CassandraFrameworkConfiguration get() {
                    CassandraFrameworkProtos.CassandraConfigRole.Builder configRole = CassandraFrameworkProtos.CassandraConfigRole.newBuilder()
                        .setCassandraVersion(cassandraVersion)
                        .setResources(CassandraFrameworkProtos.TaskResources.newBuilder()
                            .setCpuCores(cpuCores)
                            .setDiskMb(diskMb)
                            .setMemMb(memMb))
                        .setMesosRole(mesosRole)
                        .setPreDefinedDataDirectory(dataDirectory)
                        .setTaskEnv(CassandraFrameworkProtos.TaskEnv.newBuilder()
                            .addVariables(CassandraFrameworkProtos.TaskEnv.Entry.newBuilder()
                                .setName("LOCAL_JMX")
                                .setValue(jmxLocal ? "yes" : "no"))
                            .addVariables(CassandraFrameworkProtos.TaskEnv.Entry.newBuilder()
                                .setName("CASSANDRA_JMX_NO_AUTHENTICATION")
                                .setValue(jmxNoAuthentication ? "yes" : "no")));
                    if (javeHeapMb > 0) {
                        configRole.setMemJavaHeapMb(javeHeapMb);
                    }
                    return CassandraFrameworkConfiguration.newBuilder()
                        .setFrameworkName(frameworkName)
                        .setDefaultConfigRole(fillConfigRoleGaps(configRole))
                        .setHealthCheckIntervalSeconds(healthCheckIntervalSeconds)
                        .setBootstrapGraceTimeSeconds(bootstrapGraceTimeSec)
                        .setInitialNumberOfNodes(executorCount)
                        .setInitialNumberOfSeeds(seedCount)
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

    public static CassandraFrameworkProtos.CassandraConfigRole.Builder fillConfigRoleGaps(CassandraFrameworkProtos.CassandraConfigRole.Builder configRole) {
        long memMb = configRole.getResources().getMemMb();
        if (memMb > 0L) {
            if (!configRole.hasMemJavaHeapMb()) {
                configRole.setMemJavaHeapMb(Math.min(memMb / 2, 16384));
            }
            if (!configRole.hasMemAssumeOffHeapMb()) {
                configRole.setMemAssumeOffHeapMb(memMb - configRole.getMemJavaHeapMb());
            }
        } else  {
            if (configRole.hasMemJavaHeapMb()) {
                if (!configRole.hasMemAssumeOffHeapMb()) {
                    configRole.setMemAssumeOffHeapMb(configRole.getMemJavaHeapMb());
                }
            } else {
                if (configRole.hasMemAssumeOffHeapMb()) {
                    configRole.setMemJavaHeapMb(configRole.getMemAssumeOffHeapMb());
                } else {
                    throw new IllegalArgumentException("Config role is missing memory configuration");
                }
            }
            configRole.setResources(CassandraFrameworkProtos.TaskResources.newBuilder(configRole.getResources())
                .setMemMb(configRole.getMemJavaHeapMb() + configRole.getMemAssumeOffHeapMb()));
        }
        return configRole;
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

    @NotNull
    public String mesosRole() {
        return getDefaultConfigRole().getMesosRole();
    }
}
