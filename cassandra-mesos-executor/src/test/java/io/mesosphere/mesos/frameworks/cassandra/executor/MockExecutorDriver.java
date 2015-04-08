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
package io.mesosphere.mesos.frameworks.cassandra.executor;

import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MockExecutorDriver implements ExecutorDriver {

    @NotNull
    final Executor executor;
    @NotNull
    final Protos.ExecutorInfo executorInfo;
    @NotNull
    final Protos.FrameworkInfo frameworkInfo;
    @NotNull
    final Protos.SlaveInfo slaveInfo;

    @NotNull
    private List<Protos.TaskStatus> taskStatusList = new ArrayList<>();
    @NotNull
    private List<CassandraFrameworkProtos.SlaveStatusDetails> frameworkMessages = new ArrayList<>();

    public MockExecutorDriver(@NotNull final Executor executor, @NotNull final Protos.ExecutorID executorId) {
        this.executor = executor;

        slaveInfo = Protos.SlaveInfo.newBuilder()
                .setHostname("localhost")
                .setId(Protos.SlaveID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setPort(42)
                .build();
        frameworkInfo = Protos.FrameworkInfo.newBuilder()
                .setHostname(slaveInfo.getHostname())
                .setId(Protos.FrameworkID.newBuilder().setValue(UUID.randomUUID().toString()))
                .setName("some-framework")
                .setUser("me-myself-and-i")
                .build();
        executorInfo = Protos.ExecutorInfo.newBuilder()
                .setExecutorId(executorId)
                .setCommand(Protos.CommandInfo.getDefaultInstance())
                .setContainer(Protos.ContainerInfo.newBuilder()
                        .setType(Protos.ContainerInfo.Type.MESOS))
                .setFrameworkId(frameworkInfo.getId())
                .setSource("source")
                .build();
    }

    @Override
    public Protos.Status start() {
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status stop() {
        return Protos.Status.DRIVER_STOPPED;
    }

    @Override
    public Protos.Status abort() {
        return Protos.Status.DRIVER_ABORTED;
    }

    @Override
    public Protos.Status join() {
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status run() {
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status sendStatusUpdate(final Protos.TaskStatus status) {
        taskStatusList.add(status);
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status sendFrameworkMessage(final byte[] data) {
        try {
            frameworkMessages.add(CassandraFrameworkProtos.SlaveStatusDetails.parseFrom(data));
        } catch (final InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        return Protos.Status.DRIVER_RUNNING;
    }

    @NotNull
    public List<Protos.TaskStatus> taskStatusList() {
        try {
            return taskStatusList;
        } finally {
            taskStatusList = new ArrayList<>();
        }
    }

    @NotNull
    public List<CassandraFrameworkProtos.SlaveStatusDetails> frameworkMessages() {
        try {
            return frameworkMessages;
        } finally {
            frameworkMessages = new ArrayList<>();
        }
    }

    public void normalRegister() {
        executor.registered(this, executorInfo, frameworkInfo, slaveInfo);
    }

    public void launchTask(
        @NotNull final Protos.TaskID taskId,
        @NotNull final Protos.CommandInfo commandInfo,
        @NotNull final CassandraFrameworkProtos.TaskDetails taskDetails,
        @NotNull final String name,
        @NotNull final Iterable<? extends Protos.Resource> resources
    ) {
        executor.launchTask(this, Protos.TaskInfo.newBuilder()
            .setTaskId(taskId)
            .setCommand(commandInfo)
            .setData(taskDetails.toByteString())
            .setExecutor(executorInfo)
            .setName(name)
            .addAllResources(resources)
            .setSlaveId(slaveInfo.getId())
            .build());
    }

    public void frameworkMessage(final CassandraFrameworkProtos.TaskDetails taskDetails) {
        executor.frameworkMessage(this, taskDetails.toByteArray());
    }

    public void killTask(final Protos.TaskID taskId) {
        executor.killTask(this, taskId);
    }
}
