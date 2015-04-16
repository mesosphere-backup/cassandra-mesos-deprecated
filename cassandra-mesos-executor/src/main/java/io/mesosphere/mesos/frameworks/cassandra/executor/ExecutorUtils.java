package io.mesosphere.mesos.frameworks.cassandra.executor;

import com.google.protobuf.ByteString;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

final class ExecutorUtils {
    @NotNull
    static Protos.TaskStatus taskStatus(
        @NotNull final Protos.TaskInfo taskInfo,
        @NotNull final Protos.TaskState state
    ) {
        return taskStatus(taskInfo, state, nullSlaveStatusDetails());
    }

    @NotNull
    static Protos.TaskStatus taskStatus(
        @NotNull final Protos.TaskInfo taskInfo,
        @NotNull final Protos.TaskState state,
        @NotNull final CassandraFrameworkProtos.SlaveStatusDetails details
    ) {
        return taskStatus(taskInfo.getExecutor().getExecutorId(), taskInfo.getTaskId(), state, details);
    }

    @NotNull
    static Protos.TaskStatus taskStatus(
        @NotNull final Protos.ExecutorID executorId,
        @NotNull final Protos.TaskID taskId,
        @NotNull final Protos.TaskState state,
        @NotNull final CassandraFrameworkProtos.SlaveStatusDetails details
    ) {
        return Protos.TaskStatus.newBuilder()
            .setExecutorId(executorId)
            .setTaskId(taskId)
            .setState(state)
            .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR)
            .setData(ByteString.copyFrom(details.toByteArray()))
            .build();
    }

    static void serverProcessNotRunning(@NotNull final ExecutorDriver driver, @NotNull final Protos.TaskInfo task) {
        final Protos.TaskStatus taskStatus =
            slaveErrorDetails(task, "cassandra server process not running", null, CassandraFrameworkProtos.SlaveErrorDetails.ErrorType.PROCESS_NOT_RUNNING);
        driver.sendStatusUpdate(taskStatus);
    }

    @NotNull
    static Protos.TaskStatus slaveErrorDetails(
        @NotNull final Protos.TaskInfo task,
        @Nullable final String msg,
        @Nullable final String details,
        @NotNull final CassandraFrameworkProtos.SlaveErrorDetails.ErrorType errorType
    ) {
        return slaveErrorDetails(task, msg, details, null, errorType);
    }

    @NotNull
    static Protos.TaskStatus slaveErrorDetails(
        @NotNull final Protos.TaskInfo task,
        @Nullable final String msg,
        @Nullable final String details,
        @Nullable final Integer exitCode,
        @NotNull final CassandraFrameworkProtos.SlaveErrorDetails.ErrorType errorType) {
        final CassandraFrameworkProtos.SlaveErrorDetails.Builder slaveErrorDetails = CassandraFrameworkProtos.SlaveErrorDetails.newBuilder()
            .setErrorType(errorType);
        if (msg != null)
            slaveErrorDetails.setMsg(msg);
        if (details != null)
            slaveErrorDetails.setDetails(details);
        if (exitCode != null)
            slaveErrorDetails.setProcessExitCode(exitCode);
        final CassandraFrameworkProtos.SlaveStatusDetails slaveStatusDetails = CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.ERROR_DETAILS)
            .setSlaveErrorDetails(slaveErrorDetails)
            .build();
        return taskStatus(task, Protos.TaskState.TASK_ERROR, slaveStatusDetails);
    }

    @NotNull
    static CassandraFrameworkProtos.SlaveStatusDetails nullSlaveStatusDetails() {
        return CassandraFrameworkProtos.SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build();
    }
}
