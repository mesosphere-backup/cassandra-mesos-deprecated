package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Value;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos.*;
import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

public final class CassandraExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraExecutor.class);

    private AtomicReference<Process> process = new AtomicReference<>(null);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void registered(final ExecutorDriver driver, final ExecutorInfo executorInfo, final FrameworkInfo frameworkInfo, final SlaveInfo slaveInfo) {
        LOGGER.debug("registered(driver : {}, executorInfo : {}, frameworkInfo : {}, slaveInfo : {})", driver, protoToString(executorInfo), protoToString(frameworkInfo), protoToString(slaveInfo));
    }

    @Override
    public void reregistered(final ExecutorDriver driver, final SlaveInfo slaveInfo) {
        LOGGER.debug("reregistered(driver : {}, slaveInfo : {})", driver, protoToString(slaveInfo));
    }

    @Override
    public void disconnected(final ExecutorDriver driver) {
        LOGGER.debug("disconnected(driver : {})", driver);
    }

    @Override
    public void launchTask(final ExecutorDriver driver, final TaskInfo task) {
        final Marker taskIdMarker = MarkerFactory.getMarker(task.getTaskId().getValue());
        LOGGER.debug(taskIdMarker, "> launchTask(driver : {}, task : {})", driver, protoToString(task));
        try {
            driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_STARTING));
            final ByteString data = task.getData();
            final TaskDetails taskDetails = TaskDetails.parseFrom(data);
            LOGGER.debug(taskIdMarker, "received taskDetails: {}", protoToString(taskDetails));
            switch (taskDetails.getTaskType()) {
                case SLAVE_METADATA:
                    final SlaveMetadata slaveMetadata = collectSlaveMetadata();
                    final SlaveStatusDetails details = SlaveStatusDetails.newBuilder()
                        .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.SLAVE_METADATA)
                        .setSlaveMetadata(slaveMetadata)
                        .build();
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_RUNNING, details));
                    break;
                case CASSANDRA_NODE_RUN:
                    final Process cassandraProcess = launchCassandraNodeTask(taskIdMarker, taskDetails.getCassandraNodeRunTask());
                    process.set(cassandraProcess);
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_STARTING));
                    // TODO(BenWhitehead) this should really come from the first successful health check, but stubbed for now.
                    scheduledExecutorService.schedule(new TaskStateChange(driver, task, TaskState.TASK_RUNNING), 15, TimeUnit.SECONDS);
                    break;
                case CASSANDRA_NODE_SHUTDOWN:
                    process.get().destroy();
                    process.set(null);
                    driver.sendStatusUpdate(taskStatus(task, TaskState.TASK_FINISHED));
                    break;
                case CASSANDRA_NODE_HEALTH_CHECK:
                    final CassandraNodeHealthCheckTask healthCheckTask = taskDetails.getCassandraNodeHealthCheckTask();
                    LOGGER.info(taskIdMarker, "Received healthCheckTask: {}", protoToString(healthCheckTask));
                    final CassandraNodeHealthCheckDetails healthCheck = performHealthCheck(healthCheckTask);
                    final SlaveStatusDetails healthCheckDetails = SlaveStatusDetails.newBuilder()
                        .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.HEALTH_CHECK_DETAILS)
                        .setCassandraNodeHealthCheckDetails(healthCheck)
                        .build();
                    final TaskState state = healthCheck.getHealthy() ? TaskState.TASK_FINISHED : TaskState.TASK_ERROR;
                    driver.sendStatusUpdate(taskStatus(task, state, healthCheckDetails));
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            final String msg = "Error deserializing task data to type: " + TaskDetails.class.getName();
            LOGGER.error(taskIdMarker, msg, e);
            final SlaveStatusDetails details = SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.ERROR_DETAILS)
                .setSlaveErrorDetails(
                    SlaveErrorDetails.newBuilder()
                        .setMsg(msg)
                )
                .build();
            final TaskStatus taskStatus = taskStatus(task, TaskState.TASK_ERROR, details);
            driver.sendStatusUpdate(taskStatus);
        } catch (IOException e) {
            final String msg = "Error starting task due to exception.";
            LOGGER.error(taskIdMarker, msg, e);
            final SlaveStatusDetails details = SlaveStatusDetails.newBuilder()
                .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.ERROR_DETAILS)
                .setSlaveErrorDetails(
                    SlaveErrorDetails.newBuilder()
                        .setMsg(msg)
                        .setDetails(e.getMessage())
                )
                .build();
            final TaskStatus taskStatus = taskStatus(task, TaskState.TASK_ERROR, details);
            driver.sendStatusUpdate(taskStatus);
        }
        LOGGER.debug(taskIdMarker, "< launchTask(driver : {}, task : {})", driver, protoToString(task));
    }

    @Override
    public void killTask(final ExecutorDriver driver, final TaskID taskId) {
        LOGGER.debug("killTask(driver : {}, taskId : {})", driver, protoToString(taskId));
    }

    @Override
    public void frameworkMessage(final ExecutorDriver driver, final byte[] data) {
        LOGGER.debug("frameworkMessage(driver : {}, data : {})", driver, data);
    }

    @Override
    public void shutdown(final ExecutorDriver driver) {
        LOGGER.debug("shutdown(driver : {})", driver);
    }

    @Override
    public void error(final ExecutorDriver driver, final String message) {
        LOGGER.debug("error(driver : {}, message : {})", driver, message);
    }

    @NotNull
    private SlaveMetadata collectSlaveMetadata() throws UnknownHostException {
        return SlaveMetadata.newBuilder()
            .setIp(InetAddress.getLocalHost().getHostAddress()) //TODO(BenWhitehead): This resolution may have to be more sophisticated
            .build();
    }

    @NotNull
    private Process launchCassandraNodeTask(@NotNull final Marker taskIdMarker, @NotNull final CassandraNodeRunTask cassandraNodeTask) throws IOException {
        for (final TaskFile taskFile : cassandraNodeTask.getTaskFilesList()) {
            final File file = new File(taskFile.getOutputPath());
            LOGGER.debug(taskIdMarker, "Overwriting file {}", file);
            Files.createParentDirs(file);
            Files.write(taskFile.getData().toByteArray(), file);
        }
        final ProcessBuilder processBuilder = new ProcessBuilder(cassandraNodeTask.getCommandList())
            .directory(new File(System.getProperty("user.dir")))
            .redirectOutput(new File("cassandra-stdout.log"))
            .redirectError(new File("cassandra-stderr.log"));
        for (final TaskEnv.Entry entry : cassandraNodeTask.getTaskEnv().getVariablesList()) {
            processBuilder.environment().put(entry.getName(), entry.getValue());
        }
        processBuilder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        LOGGER.debug("Starting Process: {}", processBuilderToString(processBuilder));
        return processBuilder.start();
    }

    @NotNull
    private CassandraNodeHealthCheckDetails performHealthCheck(@NotNull final CassandraNodeHealthCheckTask healthCheckTask) {
        return CassandraNodeHealthCheckDetails.newBuilder()
            .setHealthy(true)
            .build();
    }

    public static void main(final String[] args) {
        final MesosExecutorDriver driver = new MesosExecutorDriver(new CassandraExecutor());
        final int status;
        switch (driver.run()) {
            case DRIVER_STOPPED:
                status = 0;
                break;
            case DRIVER_ABORTED:
                status = 1;
                break;
            case DRIVER_NOT_STARTED:
                status = 2;
                break;
            default:
                status = 3;
                break;
        }
        driver.stop();

        System.exit(status);
    }

    @NotNull
    private static TaskStatus taskStatus(
        @NotNull final TaskInfo taskInfo,
        @NotNull final TaskState state
    ) {
        return taskStatus(taskInfo, state, nullSlaveStatusDetails());
    }

    @NotNull
    private static TaskStatus taskStatus(
        @NotNull final TaskInfo taskInfo,
        @NotNull final TaskState state,
        @NotNull final SlaveStatusDetails details
    ) {
        return TaskStatus.newBuilder()
            .setExecutorId(taskInfo.getExecutor().getExecutorId())
            .setTaskId(taskInfo.getTaskId())
            .setState(state)
            .setSource(TaskStatus.Source.SOURCE_EXECUTOR)
            .setData(ByteString.copyFrom(details.toByteArray()))
            .build();
    }

    @NotNull
    private static SlaveStatusDetails nullSlaveStatusDetails() {
        return SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(SlaveStatusDetails.StatusDetailsType.NULL_DETAILS)
            .build();
    }

    @NotNull
    private static String processBuilderToString(@NotNull final ProcessBuilder builder) {
        final StringBuilder sb = new StringBuilder("ProcessBuilder{\n");
        sb.append("directory() = ").append(builder.directory());
        sb.append(",\n");
        sb.append("command() = ").append(Joiner.on(" ").join(builder.command()));
        sb.append(",\n");
        sb.append("environment() = ").append(Joiner.on("\n").withKeyValueSeparator("->").join(builder.environment()));
        sb.append("\n}");
        return sb.toString();
    }

    @Value
    private static class TaskStateChange implements Runnable {
        @NotNull
        private final ExecutorDriver driver;
        @NotNull
        private final TaskInfo task;
        @NotNull
        private final TaskState state;

        @Override
        public void run() {
            LOGGER.debug("Sending {} for {}", state, protoToString(task.getTaskId()));
            driver.sendStatusUpdate(taskStatus(task, state));
        }
    }

}
