package io.mesosphere.mesos.util;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import org.apache.mesos.Protos.*;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newTreeSet;
import static io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos.TaskEnv;

public final class ProtoUtils {

    private ProtoUtils() {
    }

    @NotNull
    public static String protoToString(@NotNull final Object any) {
        return any.toString().replaceAll(" *\\n *", " ");
    }

    @NotNull
    public static TaskID taskId(@NotNull final String name) {
        return TaskID.newBuilder()
            .setValue(name)
            .build();
    }

    @NotNull
    public static Resource cpu(final double value) {
        return scalarResource("cpus", value);
    }

    @NotNull
    public static Resource mem(final double value) {
        return scalarResource("mem", value);
    }

    @NotNull
    public static Resource disk(final double value) {
        return scalarResource("disk", value);
    }

    @NotNull
    public static Resource ports(@NotNull final Iterable<Long> ports) {
        return Resource.newBuilder()
            .setName("ports")
            .setType(Value.Type.RANGES)
            .setRanges(
                Value.Ranges.newBuilder().addAllRange(
                    from(ports).transform(LongToRange())
                )
                .build()
            )
            .build();
    }

    @NotNull
    public static Resource scalarResource(@NotNull final String name, final double value) {
        return Resource.newBuilder()
            .setName(name)
            .setType(Value.Type.SCALAR)
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
        @NotNull final Resource... resources
    ) {
        return ExecutorInfo.newBuilder()
            .setExecutorId(executorId)
            .setName(name)
            .setSource(source)
            .setSource("java")
            .addAllResources(newArrayList(resources))
            .setCommand(cmd)
            .build();
    }

    @NotNull
    public static ExecutorID executorId(@NotNull final String executorId) {
        return ExecutorID.newBuilder().setValue(executorId).build();
    }

    /**
     * @param cmd  the command string
     * @param uris the URI's to be downloaded by the fetcher
     * @return a command info using the specified {@code cmd} and {@code uris}. Each uri in {@code uris} will
     * be converted to a {@link org.apache.mesos.Protos.CommandInfo.URI} with extract set to {@code false}
     */
    @NotNull
    public static CommandInfo commandInfo(@NotNull final String cmd, @NotNull final String... uris) {
        return commandInfo(cmd, newArrayList(from(newArrayList(uris)).transform(Functions.doNotExtract())));
    }

    @NotNull
    public static CommandInfo commandInfo(@NotNull final String cmd, @NotNull final CommandInfo.URI... uris) {
        return commandInfo(cmd, newArrayList(uris));
    }

    @NotNull
    public static CommandInfo commandInfo(@NotNull final String cmd, @NotNull final List<CommandInfo.URI> uris) {
        return commandInfo(cmd, emptyEnvironment(), uris);
    }

    @NotNull
    public static CommandInfo commandInfo(@NotNull final String cmd, @NotNull final Environment environment, @NotNull final CommandInfo.URI... uris) {
        return commandInfo(cmd, environment, newArrayList(uris));
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
    public static Environment environmentFromMap(@NotNull final Map<String, String> map) {
        final Environment.Builder builder = Environment.newBuilder();
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            builder.addVariables(
                Environment.Variable.newBuilder()
                    .setName(entry.getKey())
                    .setValue(entry.getValue())
                    .build()
            );
        }
        return builder.build();
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
    public static Environment emptyEnvironment() {
        return Environment.newBuilder().build();
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

    /**
     * @param input the resource
     * @return A {@link org.apache.mesos.Protos.CommandInfo.URI} with the {@code input} with extract set to {@code false}
     */
    @NotNull
    public static CommandInfo.URI commandUri(@NotNull final String input) {
        return commandUri(input, false);
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
    public static Function<Long, Value.Range> LongToRange() {
        return LongToRange.INSTANCE;
    }

    @lombok.Value
    private static final class ResourceToName implements Function<Resource, String> {
        private static final ResourceToName INSTANCE = new ResourceToName();

        @Override
        public String apply(final Resource input) {
            return input.getName();
        }
    }

    @lombok.Value
    private static final class LongToRange implements Function<Long, Value.Range> {
        private static final LongToRange INSTANCE = new LongToRange();

        @Override
        public Value.Range apply(final Long input) {
            return Value.Range.newBuilder().setBegin(input).setEnd(input).build();
        }
    }
}
