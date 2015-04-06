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

import com.fasterxml.jackson.core.JsonFactory;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.ClusterJobUtils;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("/cluster/cleanup")
@Produces("application/json")
public final class ClusterCleanupController {

    @NotNull
    private final JsonFactory factory;
    @NotNull
    private final CassandraCluster cluster;

    public ClusterCleanupController(@NotNull CassandraCluster cluster, final @NotNull JsonFactory factory) {
        this.factory = factory;
        this.cluster = cluster;
    }

    /**
     * Starts a cluster-wide cleanup.
     *
     *     Example: <pre>{@code {
     * "started" : true
     * }}</pre>
     */
    @POST
    @Path("/start")
    public Response cleanupStart() {
        return ClusterJobUtils.startJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.CLEANUP);
    }

    /**
     * Aborts a cluster-wide cleanup after the current node has finished.
     *
     *     Example: <pre>{@code {
     * "aborted" : true
     * }}</pre>
     */
    @POST
    @Path("/abort")
    public Response cleanupAbort() {
        return ClusterJobUtils.abortJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.CLEANUP);
    }

    /**
     * Returns the status of the current cleanup.
     *
     *     Example: <pre>{@code {
     * "running" : true,
     * "cleanup" : {
     *     "type" : "CLEANUP",
     *     "started" : 1426687019998,
     *     "finished" : null,
     *     "aborted" : false,
     *     "remainingNodes" : [ "cassandra.node.0.executor" ],
     *     "currentNode" : null,
     *     "completedNodes" : [ {
     *         "executorId" : "cassandra.node.1.executor",
     *         "taskId" : "cassandra.node.1.executor.CLEANUP",
     *         "hostname" : "localhost",
     *         "ip" : "127.0.0.1",
     *         "processedKeyspaces" : {
     *             "system_traces" : {
     *             "status" : "SUCCESS",
     *             "durationMillis" : 20
     *         }
     *         },
     *         "remainingKeyspaces" : [ ]
     *     } ]
     * }
     * }}</pre>
     */
    @GET
    @Path("/status")
    public Response cleanupStatus() {
        return ClusterJobUtils.jobStatus(cluster, factory, CassandraFrameworkProtos.ClusterJobType.CLEANUP, "cleanup");
    }

    /**
     * Returns the status of the last cleanup.
     * See {@link #cleanupStatus()} for an response example except that `running` field is exchanged with a field
     * called `present`.
     */
    @GET
    @Path("/last")
    public Response lastCleanup() {
        return ClusterJobUtils.lastJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.CLEANUP, "cleanup");
    }

}
