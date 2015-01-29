package io.mesosphere.mesos.util;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.protobuf.ByteString;
import org.apache.mesos.Protos.*;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;

public final class ProtoUtils {

    private ProtoUtils() {
    }

    @NotNull
    public static String protoToString(@NotNull final Object any) {
        return any.toString().replaceAll("\\n", " ");
    }

    @NotNull
    public static TaskID taskId(@NotNull final String name) {
        return TaskID.newBuilder()
                .setValue(name)
                .build();
    }

    @NotNull
    public static Resource cpu(final double value) {
        return resource("cpus", value);
    }

    @NotNull
    public static Resource mem(final double value) {
        return resource("mem", value);
    }

    @NotNull
    public static Resource disk(final double value) {
        return resource("disk", value);
    }

    @NotNull
    public static Resource resource(@NotNull final String name, final double value) {
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
            @NotNull final CommandInfo cmd,
            @NotNull final Resource... resources
    ) {
        return ExecutorInfo.newBuilder()
                .setExecutorId(executorId)
                .setName(name)
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
        return commandInfo(cmd, newArrayList(FluentIterable.from(newArrayList(uris)).transform(Functions.doNotExtract())));
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
    public static Environment emptyEnvironment() {
        return Environment.newBuilder().build();
    }

    @NotNull
    public static TaskStatus taskStatus(@NotNull final ExecutorID executorId, @NotNull final TaskID taskId, @NotNull final TaskState stats) {
        return TaskStatus.newBuilder()
                .setExecutorId(executorId)
                .setTaskId(taskId)
                .setState(stats)
                .setSource(TaskStatus.Source.SOURCE_EXECUTOR)
                .build();
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
     * @param input    the resource
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

}
