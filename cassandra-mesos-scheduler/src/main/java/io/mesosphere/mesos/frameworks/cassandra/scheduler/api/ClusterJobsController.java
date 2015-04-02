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
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/")
public final class ClusterJobsController extends AbstractApiController {

    public ClusterJobsController(CassandraCluster cluster) {
        super(cluster);
    }

    /**
     * Starts a cluster-restart / restart of all cassandra server processes.
     *
     *     Example: <pre>{@code {
     * "started" : true
     * }}</pre>
     */
    @POST
    @Path("/cluster/restart/start")
    public Response clusterRestartStart() {
        return startJob(CassandraFrameworkProtos.ClusterJobType.RESTART);
    }

    /**
     * Aborts a cluster-restart / restart of all cassandra server processes after the current node has finished.
     *
     *     Example: <pre>{@code {
     * "aborted" : true
     * }}</pre>
     */
    @POST
    @Path("/cluster/restart/abort")
    public Response clusterRestartAbort() {
        return abortJob(CassandraFrameworkProtos.ClusterJobType.RESTART);
    }

    /**
     * Returns the status of the current cluster-restart / restart of all cassandra server processes.
     *
     *     Example: <pre>{@code {
     * "running" : true,
     * "repair" : {
     *     "type" : "RESTART",
     *     "started" : 1426686829672,
     *     "finished" : null,
     *     "aborted" : false,
     *     "remainingNodes" : [ ],
     *     "currentNode" : {
     *         "executorId" : "cassandra.node.0.executor",
     *         "taskId" : "cassandra.node.0.executor.RESTART",
     *         "hostname" : "127.0.0.2",
     *         "ip" : "127.0.0.2",
     *         "processedKeyspaces" : { },
     *         "remainingKeyspaces" : [ ]
     *     },
     *     "completedNodes" : [ {
     *         "executorId" : "cassandra.node.1.executor",
     *         "taskId" : "cassandra.node.1.executor.RESTART",
     *         "hostname" : "localhost",
     *         "ip" : "127.0.0.1",
     *         "processedKeyspaces" : { },
     *         "remainingKeyspaces" : [ ]
     *     } ]
     * }
     * }}</pre>
     */
    @GET
    @Path("/cluster/restart/status")
    public Response clusterRestartStatus() {
        return jobStatus(CassandraFrameworkProtos.ClusterJobType.RESTART, "clusterRestart");
    }

    /**
     * Returns the status of the last cluster-restart / restart of all cassandra server processes.
     * See {@link #clusterRestartStatus()} for an response example except that `running` field is exchanged with a field
     * called `present`.
     */
    @GET
    @Path("/cluster/restart/last")
    public Response lastClusterRestart() {
        return lastJob(CassandraFrameworkProtos.ClusterJobType.RESTART, "clusterRestart");
    }

    /**
     * Starts a cluster-wide repair.
     *
     *     Example: <pre>{@code {
     * "started" : true
     * }}</pre>
     */
    @POST
    @Path("/repair/start")
    public Response repairStart() {
        return startJob(CassandraFrameworkProtos.ClusterJobType.REPAIR);
    }

    /**
     * Starts a cluster-wide cleanup.
     *
     *     Example: <pre>{@code {
     * "started" : true
     * }}</pre>
     */
    @POST
    @Path("/cleanup/start")
    public Response cleanupStart() {
        return startJob(CassandraFrameworkProtos.ClusterJobType.CLEANUP);
    }

    /**
     * Aborts a cluster-wide repair after the current node has finished.
     *
     *     Example: <pre>{@code {
     * "aborted" : true
     * }}</pre>
     */
    @POST
    @Path("/repair/abort")
    public Response repairAbort() {
        return abortJob(CassandraFrameworkProtos.ClusterJobType.REPAIR);
    }

    /**
     * Aborts a cluster-wide cleanup after the current node has finished.
     *
     *     Example: <pre>{@code {
     * "aborted" : true
     * }}</pre>
     */
    @POST
    @Path("/cleanup/abort")
    public Response cleanupAbort() {
        return abortJob(CassandraFrameworkProtos.ClusterJobType.CLEANUP);
    }

    /**
     * Returns the status of the current repair.
     *
     *     Example: <pre>{@code {
     * "running" : true,
     * "repair" : {
     *     "type" : "REPAIR",
     *     "started" : 1426686829672,
     *     "finished" : null,
     *     "aborted" : false,
     *     "remainingNodes" : [ ],
     *     "currentNode" : {
     *         "executorId" : "cassandra.node.0.executor",
     *         "taskId" : "cassandra.node.0.executor.REPAIR",
     *         "hostname" : "127.0.0.2",
     *         "ip" : "127.0.0.2",
     *         "processedKeyspaces" : { },
     *         "remainingKeyspaces" : [ ]
     *     },
     *     "completedNodes" : [ {
     *         "executorId" : "cassandra.node.1.executor",
     *         "taskId" : "cassandra.node.1.executor.REPAIR",
     *         "hostname" : "localhost",
     *         "ip" : "127.0.0.1",
     *         "processedKeyspaces" : {
     *             "system_traces" : {
     *             "status" : "FINISHED",
     *             "durationMillis" : 2490
     *             }
     *         },
     *         "remainingKeyspaces" : [ ]
     *     } ]
     * }
     * }}</pre>
     */
    @GET
    @Path("/repair/status")
    public Response repairStatus() {
        return jobStatus(CassandraFrameworkProtos.ClusterJobType.REPAIR, "repair");
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
    @Path("/cleanup/status")
    public Response cleanupStatus() {
        return jobStatus(CassandraFrameworkProtos.ClusterJobType.CLEANUP, "cleanup");
    }

    /**
     * Returns the status of the last repair.
     * See {@link #repairStatus()} for an response example except that `running` field is exchanged with a field
     * called `present`.
     */
    @GET
    @Path("/repair/last")
    public Response lastRepair() {
        return lastJob(CassandraFrameworkProtos.ClusterJobType.REPAIR, "repair");
    }

    /**
     * Returns the status of the last cleanup.
     * See {@link #cleanupStatus()} for an response example except that `running` field is exchanged with a field
     * called `present`.
     */
    @GET
    @Path("/cleanup/last")
    public Response lastCleanup() {
        return lastJob(CassandraFrameworkProtos.ClusterJobType.CLEANUP, "cleanup");
    }

    private Response startJob(CassandraFrameworkProtos.ClusterJobType type) {
        final boolean started = cluster.startClusterTask(type);
        return buildStreamingResponse(new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                json.writeBooleanField("started", started);
            }
        });
    }

    private Response abortJob(CassandraFrameworkProtos.ClusterJobType type) {
        final boolean aborted = cluster.abortClusterJob(type);
        return buildStreamingResponse(new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                json.writeBooleanField("aborted", aborted);
            }
        });
    }

    private Response jobStatus(final CassandraFrameworkProtos.ClusterJobType type, final String name) {
        final CassandraFrameworkProtos.ClusterJobStatus repairJob = cluster.getCurrentClusterJob(type);
        return buildStreamingResponse(new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                json.writeBooleanField("running", repairJob != null);
                writeClusterJob(json, name, repairJob);
            }
        });
    }

    private Response lastJob(CassandraFrameworkProtos.ClusterJobType type, final String name) {
        final CassandraFrameworkProtos.ClusterJobStatus repairJob = cluster.getLastClusterJob(type);
        return buildStreamingResponse(new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                json.writeBooleanField("present", repairJob != null);
                writeClusterJob(json, name, repairJob);
            }
        });
    }
}
