package io.mesosphere.mesos.frameworks.cassandra;

import io.mesosphere.mesos.util.ProtoUtils;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

public final class CassandraExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraExecutor.class);

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
        LOGGER.debug("launchTask(driver : {}, task : {})", driver, protoToString(task));
        driver.sendStatusUpdate(ProtoUtils.taskStatus(task.getExecutor().getExecutorId(), task.getTaskId(), TaskState.TASK_RUNNING));
        if (!"keep-alive".equals(task.getData().toStringUtf8())) {
            scheduledExecutorService.schedule(new TaskStateChange(driver, task), 60, TimeUnit.SECONDS);
        }
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

    private static class TaskStateChange implements Runnable {
        @NotNull
        private final ExecutorDriver driver;
        @NotNull
        private final TaskInfo task;

        public TaskStateChange(@NotNull final ExecutorDriver driver, @NotNull final TaskInfo task) {
            this.driver = driver;
            this.task = task;
        }

        @Override
        public void run() {
            LOGGER.debug("Sending TASK_FINISHED for {}", protoToString(task.getTaskId()));
            driver.sendStatusUpdate(ProtoUtils.taskStatus(task.getExecutor().getExecutorId(), task.getTaskId(), TaskState.TASK_FINISHED));
        }
    }
}
