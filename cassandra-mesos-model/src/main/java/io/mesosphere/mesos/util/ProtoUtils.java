package io.mesosphere.mesos.util;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.protobuf.ByteString;
import org.apache.mesos.Protos.*;
import org.jetbrains.annotations.NotNull;

import static com.google.common.collect.Lists.newArrayList;

public final class ProtoUtils {

    private ProtoUtils() {}

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

    @NotNull
    public static CommandInfo commandInfo(@NotNull final String cmd, @NotNull final String... uris) {
        return CommandInfo.newBuilder()
                .setValue(cmd)
                .addAllUris(FluentIterable.from(newArrayList(uris)).transform(Functions.doNotExtract()))
                .build();
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

    @NotNull
    public static CommandInfo.URI commandUri(@NotNull final String input, final boolean shouldExtract) {
        return CommandInfo.URI.newBuilder().setValue(input).setExtract(shouldExtract).build();
    }

}
