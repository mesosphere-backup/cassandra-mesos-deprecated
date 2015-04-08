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

import com.google.common.base.Optional;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;

public abstract class ClusterJobHandler {
    @NotNull
    protected final CassandraCluster cluster;
    @NotNull
    protected final PersistedCassandraClusterJobs jobsState;

    protected ClusterJobHandler(@NotNull final CassandraCluster cluster, @NotNull final PersistedCassandraClusterJobs jobsState) {
        this.cluster = cluster;
        this.jobsState = jobsState;
    }

    public void onTaskRemoved(@NotNull final Protos.TaskStatus status, @NotNull final CassandraFrameworkProtos.ClusterJobStatus clusterJob) {
        jobsState.removeTaskForCurrentNode(status, clusterJob);
    }

    public abstract void handleTaskOffer(@NotNull CassandraFrameworkProtos.ClusterJobStatus currentJob, @NotNull String executorId, @NotNull Optional<CassandraFrameworkProtos.CassandraNode> nodeForExecutorId, @NotNull TasksForOffer tasksForOffer);

    public abstract void onNodeJobStatus(@NotNull CassandraFrameworkProtos.ClusterJobStatus currentJob, @NotNull CassandraFrameworkProtos.NodeJobStatus nodeJobStatus);

    protected final void nodeFinished(@NotNull final CassandraFrameworkProtos.NodeJobStatus nodeJobStatus, @NotNull final CassandraFrameworkProtos.ClusterJobStatus currentJob) {
        final CassandraFrameworkProtos.ClusterJobStatus.Builder builder = CassandraFrameworkProtos.ClusterJobStatus.newBuilder(currentJob)
            .clearCurrentNode()
            .clearRemainingNodes()
            .addCompletedNodes(nodeJobStatus);

        for (final String nodeExecutorId : currentJob.getRemainingNodesList()) {
            if (!nodeExecutorId.equals(nodeJobStatus.getExecutorId())) {
                builder.addRemainingNodes(nodeExecutorId);
            }
        }

        if (builder.getRemainingNodesCount() == 0) {
            jobsState.finishJob(builder
                .setFinishedTimestamp(System.currentTimeMillis())
                .build());
        } else {
            jobsState.setCurrentJob(builder.build());
        }
    }
}
