package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import lombok.Value;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos.TaskDetails;
import static org.apache.mesos.Protos.ExecutorInfo;
import static org.apache.mesos.Protos.TaskInfo;

@Value
public final class SuperTask {
    @NotNull
    private final String hostname;
    @NotNull
    private final TaskInfo taskInfo;
    @NotNull
    private final ExecutorInfo executorInfo;
    @NotNull
    private final TaskDetails taskDetails;

    @NotNull
    public static Predicate<SuperTask> hostnameEq(@NotNull final String hostname) {
        return new HostnameEq(hostname);
    }

    @NotNull
    public static Predicate<SuperTask> taskDetailsTypeEq(@NotNull final TaskDetails.TaskType taskType) {
        return new TaskDetailsTypeEq(taskType);
    }

    @NotNull
    public static Predicate<SuperTask> executorIdEq(@NotNull final Protos.ExecutorID executorID) {
        return new ExecutorIDEq(executorID);
    }

    @NotNull
    public static Predicate<SuperTask> taskIdEq(@NotNull final Protos.TaskID taskID) {
        return new TaskIdEq(taskID);
    }

    @NotNull
    public static Function<SuperTask, Protos.TaskID> toTaskId() {
        return ToTaskId.INSTANCE;
    }

    @NotNull
    public static Function<SuperTask, Protos.ExecutorID> toExecutorId() {
        return ToExecutorId.INSTANCE;
    }

    @Value
    private static final class HostnameEq implements Predicate<SuperTask> {
        @NotNull
        private final String hostname;

        @Override
        public boolean apply(final SuperTask superTask) {
            return hostname.equals(superTask.getHostname());
        }
    }

    @Value
    private static final class TaskDetailsTypeEq implements Predicate<SuperTask> {
        @NotNull
        private final TaskDetails.TaskType taskType;

        @Override
        public boolean apply(final SuperTask item) {
            return taskType == item.getTaskDetails().getTaskType();
        }
    }

    @Value
    private static final class ExecutorIDEq implements Predicate<SuperTask> {
        @NotNull
        private final Protos.ExecutorID executorID;

        @Override
        public boolean apply(final SuperTask item) {
            return executorID.equals(item.getExecutorInfo().getExecutorId());
        }
    }

    @Value
    private static final class TaskIdEq implements Predicate<SuperTask> {
        @NotNull
        private final Protos.TaskID taskID;

        @Override
        public boolean apply(final SuperTask item) {
            return taskID.equals(item.getTaskInfo().getTaskId());
        }
    }

    @Value
    private static final class ToTaskId implements Function<SuperTask, Protos.TaskID> {
        private static final ToTaskId INSTANCE = new ToTaskId();
        @Override
        public Protos.TaskID apply(final SuperTask input) {
            return input.getTaskInfo().getTaskId();
        }
    }

    @Value
    private static final class ToExecutorId implements Function<SuperTask, Protos.ExecutorID> {
        private static final ToExecutorId INSTANCE = new ToExecutorId();

        @Override
        public Protos.ExecutorID apply(final SuperTask input) {
            return input.executorInfo.getExecutorId();
        }
    }

}
