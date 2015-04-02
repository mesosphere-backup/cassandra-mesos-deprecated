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
import com.google.common.base.Supplier;
import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.util.ProtoUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.state.State;
import org.jetbrains.annotations.NotNull;

public final class PersistedCassandraClusterJobs extends StatePersistedObject<CassandraFrameworkProtos.CassandraClusterJobs> {
    public PersistedCassandraClusterJobs(@NotNull final State state) {
        super(
            "CassandraClusterJobs",
            state,
            new Supplier<CassandraFrameworkProtos.CassandraClusterJobs>() {
                @Override
                public CassandraFrameworkProtos.CassandraClusterJobs get() {
                    return CassandraFrameworkProtos.CassandraClusterJobs.newBuilder().build();
                }
            },
            new Function<byte[], CassandraFrameworkProtos.CassandraClusterJobs>() {
                @Override
                public CassandraFrameworkProtos.CassandraClusterJobs apply(final byte[] input) {
                    try {
                        return CassandraFrameworkProtos.CassandraClusterJobs.parseFrom(input);
                    } catch (InvalidProtocolBufferException e) {
                        throw new ProtoUtils.RuntimeInvalidProtocolBufferException(e);
                    }
                }
            },
            new Function<CassandraFrameworkProtos.CassandraClusterJobs, byte[]>() {
                @Override
                public byte[] apply(final CassandraFrameworkProtos.CassandraClusterJobs input) {
                    return input.toByteArray();
                }
            }
        );
    }

    public void setCurrentJob(CassandraFrameworkProtos.ClusterJobStatus current) {
        CassandraFrameworkProtos.CassandraClusterJobs.Builder builder = CassandraFrameworkProtos.CassandraClusterJobs.newBuilder(get());
        if (current == null) {
            builder.clearCurrentClusterJob();
        } else {
            builder.setCurrentClusterJob(current);
        }
        setValue(builder.build());
    }

    public void removeTaskForCurrentNode(Protos.TaskStatus status, CassandraFrameworkProtos.ClusterJobStatus currentJob) {
        CassandraFrameworkProtos.ClusterJobStatus.Builder builder = CassandraFrameworkProtos.ClusterJobStatus.newBuilder(currentJob);

        CassandraFrameworkProtos.NodeJobStatus.Builder currentNode = CassandraFrameworkProtos.NodeJobStatus.newBuilder(builder.getCurrentNode())
            .setFailed(true)
            .setFailureMessage(
                    "TaskStatus:" + status.getState()
                    + ", reason:" + status.getReason()
                    + ", source:" + status.getSource()
                    + ", healthy:" + status.getHealthy()
                    + ", message:" + status.getMessage()
            );

        setCurrentJob(builder.addCompletedNodes(currentNode)
            .clearCurrentNode()
            .build());
    }

    public void updateJobCurrentNode(CassandraFrameworkProtos.ClusterJobStatus currentJob, CassandraFrameworkProtos.NodeJobStatus currentNode) {
        CassandraFrameworkProtos.ClusterJobStatus.Builder builder = CassandraFrameworkProtos.ClusterJobStatus.newBuilder(currentJob)
            .clearRemainingNodes()
            .setCurrentNode(currentNode);

        for (String nodeExecutorId : currentJob.getRemainingNodesList()) {
            if (!nodeExecutorId.equals(currentNode.getExecutorId())) {
                builder.addRemainingNodes(nodeExecutorId);
            }
        }

        setCurrentJob(builder.build());
    }

    public void finishJob(CassandraFrameworkProtos.ClusterJobStatus currentJob) {
        CassandraFrameworkProtos.CassandraClusterJobs.Builder clusterJobsBuilder = CassandraFrameworkProtos.CassandraClusterJobs.newBuilder()
            .addLastClusterJobs(currentJob);

        for (CassandraFrameworkProtos.ClusterJobStatus clusterJobStatus : get().getLastClusterJobsList()) {
            if (clusterJobStatus.getJobType() != currentJob.getJobType()) {
                clusterJobsBuilder.addLastClusterJobs(clusterJobStatus);
            }
        }

        setValue(clusterJobsBuilder.build());
    }

    public void clearClusterJobCurrentNode(@NotNull String executorId) {
        CassandraFrameworkProtos.CassandraClusterJobs clusterJobs = get();
        if (!clusterJobs.hasCurrentClusterJob()) {
            return;
        }
        CassandraFrameworkProtos.ClusterJobStatus current = clusterJobs.getCurrentClusterJob();
        if (!current.hasCurrentNode()) {
            return;
        }
        CassandraFrameworkProtos.NodeJobStatus currentNode = current.getCurrentNode();
        if (currentNode.getExecutorId().equals(executorId)) {
            setCurrentJob(CassandraFrameworkProtos.ClusterJobStatus.newBuilder(current)
                .clearCurrentNode()
                .addCompletedNodes(CassandraFrameworkProtos.NodeJobStatus.newBuilder(currentNode)
                    .setFailed(true)
                    .setFailureMessage("Task finished without any additional information"))
                .build());
        }
    }
}
