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

public abstract class ClusterJobHandler {
    protected final CassandraCluster cluster;
    protected final PersistedCassandraClusterJobs jobsState;

    protected ClusterJobHandler(CassandraCluster cluster, PersistedCassandraClusterJobs jobsState) {
        this.cluster = cluster;
        this.jobsState = jobsState;
    }

    public void onTaskRemoved(Protos.TaskStatus status, CassandraFrameworkProtos.ClusterJobStatus clusterJob) {
        jobsState.removeTaskForCurrentNode(status, clusterJob);
    }

    public abstract void handleTaskOffer(CassandraFrameworkProtos.ClusterJobStatus currentJob, String executorId, Optional<CassandraFrameworkProtos.CassandraNode> nodeForExecutorId, TasksForOffer tasksForOffer);

    public abstract void onNodeJobStatus(CassandraFrameworkProtos.ClusterJobStatus currentJob, CassandraFrameworkProtos.NodeJobStatus nodeJobStatus);

    protected void nodeFinished(CassandraFrameworkProtos.NodeJobStatus nodeJobStatus, CassandraFrameworkProtos.ClusterJobStatus currentJob) {
        CassandraFrameworkProtos.ClusterJobStatus.Builder builder = CassandraFrameworkProtos.ClusterJobStatus.newBuilder(currentJob)
            .clearCurrentNode()
            .clearRemainingNodes()
            .addCompletedNodes(nodeJobStatus);

        for (String nodeExecutorId : currentJob.getRemainingNodesList()) {
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
