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
package io.mesosphere.mesos.frameworks.cassandra.scheduler.api;

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Optional;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;

import java.io.IOException;

public abstract class AbstractApiController {

    protected final CassandraCluster cluster;

    protected AbstractApiController(CassandraCluster cluster) {
        this.cluster = cluster;
    }

    protected void writeSeedIps(JsonGenerator json) throws IOException {
        json.writeArrayFieldStart("seeds");
        for (String seed : cluster.getSeedNodeIps()) {
            json.writeString(seed);
        }
        json.writeEndArray();
    }

    protected void writeClusterJob(JsonGenerator json, String name, CassandraFrameworkProtos.ClusterJobStatus jobStatus) throws IOException {
        if (jobStatus != null && jobStatus.hasJobType()) {
            json.writeObjectFieldStart(name);

            json.writeStringField("type", jobStatus.getJobType().name());

            json.writeNumberField("started", jobStatus.getStartedTimestamp());
            if (jobStatus.hasFinishedTimestamp()) {
                json.writeNumberField("finished", jobStatus.getFinishedTimestamp());
            } else {
                json.writeNullField("finished");
            }
            json.writeBooleanField("aborted", jobStatus.getAborted());

            json.writeArrayFieldStart("remainingNodes");
            for (String node : jobStatus.getRemainingNodesList()) {
                json.writeString(node);
            }
            json.writeEndArray();

            if (jobStatus.hasCurrentNode()) {
                json.writeObjectFieldStart("currentNode");
                writeNodeJobStatus(json, jobStatus.getCurrentNode());
            } else {
                json.writeNullField("currentNode");
            }

            json.writeArrayFieldStart("completedNodes");
            for (CassandraFrameworkProtos.NodeJobStatus nodeJobStatus : jobStatus.getCompletedNodesList()) {
                json.writeStartObject();
                writeNodeJobStatus(json, nodeJobStatus);
            }
            json.writeEndArray();

            json.writeEndObject();
        } else {
            json.writeNullField(name);
        }
    }

    protected void writeNodeJobStatus(JsonGenerator json, CassandraFrameworkProtos.NodeJobStatus nodeJobStatus) throws IOException {
        json.writeStringField("executorId", nodeJobStatus.getExecutorId());
        json.writeStringField("taskId", nodeJobStatus.getTaskId());
        Optional<CassandraFrameworkProtos.CassandraNode> node = cluster.cassandraNodeForExecutorId(nodeJobStatus.getExecutorId());
        if (node.isPresent()) {
            json.writeStringField("hostname", node.get().getHostname());
            json.writeStringField("ip", node.get().getIp());
        }
        if (nodeJobStatus.getFailed()) {
            json.writeObjectFieldStart("failure");
            json.writeBooleanField("failed", nodeJobStatus.getFailed());
            json.writeStringField("message", nodeJobStatus.getFailureMessage());
            json.writeEndObject();
        }

        if (nodeJobStatus.hasStartedTimestamp()) {
            json.writeNumberField("startedTimestamp", nodeJobStatus.getStartedTimestamp());
        } else {
            json.writeNullField("startedTimestamp");
        }
        if (nodeJobStatus.hasFinishedTimestamp()) {
            json.writeNumberField("finishedTimestamp", nodeJobStatus.getFinishedTimestamp());
        } else {
            json.writeNullField("finishedTimestamp");
        }

        json.writeObjectFieldStart("processedKeyspaces");
        for (CassandraFrameworkProtos.ClusterJobKeyspaceStatus clusterJobKeyspaceStatus : nodeJobStatus.getProcessedKeyspacesList()) {
            json.writeObjectFieldStart(clusterJobKeyspaceStatus.getKeyspace());
            json.writeStringField("status", clusterJobKeyspaceStatus.getStatus());
            json.writeNumberField("durationMillis", clusterJobKeyspaceStatus.getDuration());
            json.writeEndObject();
        }
        json.writeEndObject();

        json.writeArrayFieldStart("remainingKeyspaces");
        for (String keyspace : nodeJobStatus.getRemainingKeyspacesList()) {
            json.writeString(keyspace);
        }
        json.writeEndArray();

        json.writeEndObject();
    }
}
