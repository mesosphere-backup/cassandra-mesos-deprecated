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
package io.mesosphere.mesos.util;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.mesos.Protos.*;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newTreeSet;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.TaskEnv;

public final class ProtoUtils {

    private static final Pattern PROTO_TO_STRING = Pattern.compile(" *\\n *");

    private ProtoUtils() {
    }

    public static final class RuntimeInvalidProtocolBufferException extends RuntimeException {
        public RuntimeInvalidProtocolBufferException(final InvalidProtocolBufferException cause) {
            super(cause);
        }
    }

    @NotNull
    public static String protoToString(@NotNull final Object any) {
        return PROTO_TO_STRING.matcher(any.toString()).replaceAll(" ").trim();
    }

    @NotNull
    public static TaskID taskId(@NotNull final String name) {
        return TaskID.newBuilder()
            .setValue(name)
            .build();
    }

    @NotNull
    public static Resource cpu(@NotNull final double value, @NotNull final String role) {
        return scalarResource("cpus", value, role);
    }

    @NotNull
    public static Resource mem(@NotNull final double value, @NotNull final String role) {
        return scalarResource("mem", value, role);
    }

    @NotNull
    public static Resource disk(@NotNull final double value, @NotNull final String role) {
        return scalarResource("disk", value, role);
    }

    @NotNull
    public static Resource ports(@NotNull final Iterable<Long> ports, @NotNull final String role) {
        return Resource.newBuilder()
            .setName("ports")
            .setType(Value.Type.RANGES)
            .setRole(role)
            .setRanges(
                Value.Ranges.newBuilder().addAllRange(
                    from(ports).transform(longToRange())
                )
                    .build()
            )
            .build();
    }

    @NotNull
    public static Resource scalarResource(@NotNull final String name, final double value, @NotNull final String role) {
        return Resource.newBuilder()
            .setName(name)
            .setType(Value.Type.SCALAR)
            .setRole(role)
            .setScalar(
                Value.Scalar.newBuilder()
                    .setValue(value)
                    .build()
            )
            .build();
    }

    @NotNull
    public static ExecutorInfo executorInfo(
        @NotNull final ExecutorID executorId,
        @NotNull final String name,
        @NotNull final String source,
        @NotNull final CommandInfo cmd,
        @NotNull final List<Resource> resources
    ) {
        return ExecutorInfo.newBuilder()
            .setExecutorId(executorId)
            .setName(name)
            .setSource(source)
            .setSource("java")
            .addAllResources(resources)
            .setCommand(cmd)
            .build();
    }

    @NotNull
    public static ExecutorID executorId(@NotNull final String executorId) {
        return ExecutorID.newBuilder().setValue(executorId).build();
    }

    @NotNull
    public static CommandInfo commandInfo(@NotNull final String cmd, @NotNull final Environment environment, @NotNull final List<CommandInfo.URI> uris) {
        return CommandInfo.newBuilder()
            .setValue(cmd)
            .setEnvironment(environment)
            .addAllUris(newArrayList(uris))
            .build();
    }

    @NotNull
    public static TaskEnv taskEnvFromMap(@NotNull final Map<String, String> map) {
        final TaskEnv.Builder builder = TaskEnv.newBuilder();
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            builder.addVariables(
                TaskEnv.Entry.newBuilder()
                    .setName(entry.getKey())
                    .setValue(entry.getValue())
                    .build()
            );
        }
        return builder.build();
    }

    @NotNull
    public static Credential getCredential(@NotNull final String principal, @NotNull final Optional<String> secret) {
        if (secret.isPresent()) {
            return Credential.newBuilder()
                .setPrincipal(principal)
                .setSecret(ByteString.copyFrom(secret.get().getBytes()))
                .build();
        } else {
            return Credential.newBuilder()
                .setPrincipal(principal)
                .build();
        }
    }

    @NotNull
    public static CommandInfo.URI commandUri(@NotNull final String input, final boolean shouldExtract) {
        return CommandInfo.URI.newBuilder().setValue(input).setExtract(shouldExtract).build();
    }

    @NotNull
    public static Optional<Double> resourceValueDouble(@NotNull final Optional<Resource> resource) {
        if (resource.isPresent()) {
            if (resource.get().getType() != Value.Type.SCALAR) {
                throw new IllegalArgumentException("Resource must be of type SCALAR");
            }
            return Optional.of(resource.get().getScalar().getValue());
        } else {
            return Optional.absent();
        }
    }

    @NotNull
    public static Optional<Long> resourceValueLong(@NotNull final Optional<Resource> resource) {
        if (resource.isPresent()) {
            if (resource.get().getType() != Value.Type.SCALAR) {
                throw new IllegalArgumentException("Resource must be of type SCALAR");
            }
            final long value = (long) resource.get().getScalar().getValue();
            return Optional.of(value);
        } else {
            return Optional.absent();
        }
    }

    @NotNull
    public static TreeSet<Long> resourceValueRange(@NotNull final Optional<Resource> resource) {
        if (resource.isPresent()) {
            return resourceValueRange(resource.get());
        } else {
            return newTreeSet();
        }
    }

    @NotNull
    public static TreeSet<Long> resourceValueRange(@NotNull final Resource resource) {
        if (resource.getType() != Value.Type.RANGES) {
            throw new IllegalArgumentException("Resource must be of type RANGES");
        }

        final TreeSet<Long> set = newTreeSet();
        for (final Value.Range range : resource.getRanges().getRangeList()) {
            final long begin = range.getBegin();
            final long end = range.getEnd();
            for (long l = begin; l <= end; l++) {
                set.add(l);
            }
        }
        return set;
    }

    @NotNull
    public static Function<Resource, String> resourceToName() {
        return ResourceToName.INSTANCE;
    }

    @NotNull
    public static Function<Long, Value.Range> longToRange() {
        return LongToRange.INSTANCE;
    }

    @NotNull
    public static FrameworkID frameworkId(final String frameworkName) {
        return FrameworkID.newBuilder().setValue(frameworkName).build();
    }

    private static final class ResourceToName implements Function<Resource, String> {
        private static final ResourceToName INSTANCE = new ResourceToName();

        @Override
        public String apply(final Resource input) {
            return input.getName();
        }
    }

    private static final class LongToRange implements Function<Long, Value.Range> {
        private static final LongToRange INSTANCE = new LongToRange();

        @Override
        public Value.Range apply(final Long input) {
            return Value.Range.newBuilder().setBegin(input).setEnd(input).build();
        }
    }
}
