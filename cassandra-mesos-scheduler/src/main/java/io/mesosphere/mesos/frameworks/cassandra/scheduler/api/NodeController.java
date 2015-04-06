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
import com.fasterxml.jackson.core.JsonGenerator;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.ReplaceNodePreconditionFailed;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.SeedChangeException;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.JaxRsUtils;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

@Path("/node")
@Produces("application/json")
public final class NodeController {

    @NotNull
    private final CassandraCluster cluster;
    @NotNull
    private final JsonFactory factory;

    public NodeController(@NotNull CassandraCluster cluster, @NotNull final JsonFactory factory) {
        this.cluster = cluster;
        this.factory = factory;
    }

    /**
     * Retrieve a list of all nodes including their status.
     *
     *     <pre>{@code {
     * "replaceNodes" : [ ],
     * "nodesToAcquire" : 0,
     * "nodes" : [ {
     *     "tasks" : {
     *         "METADATA" : {
     *             "cpuCores" : 0.1,
     *             "diskMb" : 16,
     *             "memMb" : 16,
     *             "taskId" : "cassandra.node.0.executor"
     *         },
     *         "SERVER" : {
     *             "cpuCores" : 2.0,
     *             "diskMb" : 2048,
     *             "memMb" : 2048,
     *             "taskId" : "cassandra.node.0.executor.server"
     *         }
     *     },
     *     "executorId" : "cassandra.node.0.executor",
     *     "ip" : "127.0.0.2",
     *     "hostname" : "127.0.0.2",
     *     "targetRunState" : "RUN",
     *     "jmxPort" : 64112,
     *     "seedNode" : true,
     *     "cassandraDaemonPid" : 6104,
     *     "lastHealthCheck" : 1426686217128,
     *     "healthCheckDetails" : {
     *         "healthy" : true,
     *         "msg" : "",
     *         "version" : "2.1.4",
     *         "operationMode" : "NORMAL",
     *         "clusterName" : "cassandra",
     *         "dataCenter" : "DC1",
     *         "rack" : "RAC1",
     *         "endpoint" : "127.0.0.2",
     *         "hostId" : "4207396e-6aa0-432e-97d9-1a4df3c1057f",
     *         "joined" : true,
     *         "gossipInitialized" : true,
     *         "gossipRunning" : true,
     *         "nativeTransportRunning" : true,
     *         "rpcServerRunning" : true,
     *         "tokenCount" : 256,
     *         "uptimeMillis" : 29072
     *     }
     * }, {
     *     "tasks" : {
     *     "METADATA" : {
     *         "cpuCores" : 0.1,
     *         "diskMb" : 16,
     *         "memMb" : 16,
     *         "taskId" : "cassandra.node.1.executor"
     *     },
     *     "SERVER" : {
     *         "cpuCores" : 2.0,
     *         "diskMb" : 2048,
     *         "memMb" : 2048,
     *         "taskId" : "cassandra.node.1.executor.server"
     *     }
     *     },
     *     "executorId" : "cassandra.node.1.executor",
     *     "ip" : "127.0.0.1",
     *     "hostname" : "localhost",
     *     "targetRunState" : "RUN",
     *     "jmxPort" : 64113,
     *     "seedNode" : false,
     *     "cassandraDaemonPid" : 6127,
     *     "lastHealthCheck" : 1426686217095,
     *     "healthCheckDetails" : {
     *         "healthy" : true,
     *         "msg" : "",
     *         "version" : "2.1.4",
     *         "operationMode" : "JOINING",
     *         "clusterName" : "cassandra",
     *         "dataCenter" : "",
     *         "rack" : "",
     *         "endpoint" : "",
     *         "hostId" : "",
     *         "joined" : true,
     *         "gossipInitialized" : true,
     *         "gossipRunning" : true,
     *         "nativeTransportRunning" : false,
     *         "rpcServerRunning" : false,
     *         "tokenCount" : 0,
     *         "uptimeMillis" : 16936
     *     }
     * } ]
     * }}</pre>
     */
    @GET
    @Path("/all")
    public Response nodes() {
        return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                CassandraFrameworkProtos.CassandraClusterState clusterState = cluster.getClusterState().get();

                json.writeArrayFieldStart("replaceNodes");
                for (String ip : clusterState.getReplaceNodeIpsList()) {
                    json.writeString(ip);
                }
                json.writeEndArray();

                json.writeNumberField("nodesToAcquire", clusterState.getNodesToAcquire());

                json.writeArrayFieldStart("nodes");
                for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.getNodesList()) {
                    json.writeStartObject();

                    if (cassandraNode.hasReplacementForIp()) {
                        json.writeStringField("replacementForIp", cassandraNode.getReplacementForIp());
                    }

                    json.writeObjectFieldStart("tasks");
                    for (CassandraFrameworkProtos.CassandraNodeTask cassandraNodeTask : cassandraNode.getTasksList()) {
                        JaxRsUtils.writeTask(json, cassandraNodeTask);
                    }
                    json.writeEndObject();
// TODO                cassandraNode.getDataVolumesList();

                    if (!cassandraNode.hasCassandraNodeExecutor()) {
                        json.writeNullField("executorId");
                        json.writeNullField("workdir");
                    } else {
                        json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                        CassandraFrameworkProtos.ExecutorMetadata executorMetadata = cluster.metadataForExecutor(cassandraNode.getCassandraNodeExecutor().getExecutorId());
                        if (executorMetadata != null) {
                            json.writeStringField("workdir", executorMetadata.getWorkdir());
                        } else {
                            json.writeNullField("workdir");
                        }
                    }
                    json.writeStringField("ip", cassandraNode.getIp());
                    json.writeStringField("hostname", cassandraNode.getHostname());
                    json.writeStringField("targetRunState", cassandraNode.getTargetRunState().name());
                    json.writeNumberField("jmxPort", cassandraNode.getJmxConnect().getJmxPort());
                    json.writeBooleanField("seedNode", cassandraNode.getSeed());
                    if (!cassandraNode.hasCassandraDaemonPid()) {
                        json.writeNullField("cassandraDaemonPid");
                    } else {
                        json.writeNumberField("cassandraDaemonPid", cassandraNode.getCassandraDaemonPid());
                    }

                    CassandraFrameworkProtos.HealthCheckHistoryEntry lastHealthCheck =
                            cassandraNode.hasCassandraNodeExecutor() ? cluster.lastHealthCheck(cassandraNode.getCassandraNodeExecutor().getExecutorId()) : null;

                    if (lastHealthCheck != null) {
                        json.writeNumberField("lastHealthCheck", lastHealthCheck.getTimestampEnd());
                    } else {
                        json.writeNullField("lastHealthCheck");
                    }

                    if (lastHealthCheck != null) {
                        json.writeObjectFieldStart("healthCheckDetails");

                        CassandraFrameworkProtos.HealthCheckDetails hcd = lastHealthCheck.getDetails();

                        json.writeBooleanField("healthy", hcd.getHealthy());
                        json.writeStringField("msg", hcd.getMsg());

                        json.writeStringField("version", hcd.getInfo().getVersion());
                        json.writeStringField("operationMode", hcd.getInfo().getOperationMode());
                        json.writeStringField("clusterName", hcd.getInfo().getClusterName());
                        json.writeStringField("dataCenter", hcd.getInfo().getDataCenter());
                        json.writeStringField("rack", hcd.getInfo().getRack());
                        json.writeStringField("endpoint", hcd.getInfo().getEndpoint());
                        json.writeStringField("hostId", hcd.getInfo().getHostId());
                        json.writeBooleanField("joined", hcd.getInfo().getJoined());
                        json.writeBooleanField("gossipInitialized", hcd.getInfo().getGossipInitialized());
                        json.writeBooleanField("gossipRunning", hcd.getInfo().getGossipRunning());
                        json.writeBooleanField("nativeTransportRunning", hcd.getInfo().getNativeTransportRunning());
                        json.writeBooleanField("rpcServerRunning", hcd.getInfo().getRpcServerRunning());
                        json.writeNumberField("tokenCount", hcd.getInfo().getTokenCount());
                        json.writeNumberField("uptimeMillis", hcd.getInfo().getUptimeMillis());

                        json.writeEndObject();
                    } else {
                        json.writeNullField("healthCheckDetails");
                    }

                    final List<CassandraFrameworkProtos.DataVolume> dataVolumes = cassandraNode.getDataVolumesList();
                    json.writeArrayFieldStart("dataVolumes");
                    for (final CassandraFrameworkProtos.DataVolume volume : dataVolumes) {
                        json.writeStartObject();
                        json.writeStringField("path", volume.getPath());
                        if (volume.hasSizeMb()) {
                            json.writeNumberField("size", volume.getSizeMb());
                        }
                        json.writeEndObject();
                    }
                    json.writeEndArray();

                    json.writeEndObject();
                }
                json.writeEndArray();
            }
        });
    }

    /**
     * Returns a JSON with the IP addresses of all seed nodes and native, thrift and JMX port numbers.
     *
     * Example:
     * <pre>{@code {
     * "nativePort" : 9042,
     * "rpcPort" : 9160,
     * "jmxPort" : 7199,
     * "seeds" : [ "127.0.0.1" ]
     * }}</pre>
     */
    @GET
    @Path("/seed/all")
    public Response seedNodes() {
        final CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration().get();
        return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {

                json.writeNumberField("nativePort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_NATIVE));
                json.writeNumberField("rpcPort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_RPC));
                json.writeNumberField("jmxPort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_JMX));

                JaxRsUtils.writeSeedIps(cluster, json);
            }
        });
    }

    /**
     * Allows to make a non-seed node a seed node. The node is specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * Must be submitted using HTTP method {@code POST}.
     *
     * Example: <pre>{@code {
     * "ip" : "127.0.0.1",
     * "hostname" : "localhost",
     * "executorId" : "cassandra.node.1.executor",
     * "oldSeedState" : "false",
     * "success" : "false",
     * "error" : "Some error message"
     * }}</pre>
     *
     * <pre>{@code {
     * "ip" : "127.0.0.1",
     * "hostname" : "localhost",
     * "executorId" : "cassandra.node.1.executor",
     * "oldSeedState" : "false",
     * "success" : "true",
     * "seedState" : "true"
     * }}</pre>
     */
    @POST
    @Path("/{node}/make-seed")
    public Response nodeMakeSeed(@PathParam("node") String node) {
        return nodeUpdateSeed(node, true);
    }

    /**
     * Allows to make a seed node a non-seed node. The node is specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * Must be submitted using HTTP method {@code POST}.
     *
     * Example: <pre>{@code {
     * "ip" : "127.0.0.1",
     * "hostname" : "localhost",
     * "executorId" : "cassandra.node.1.executor",
     * "oldSeedState" : "true",
     * "success" : "false",
     * "error" : "Some error message"
     * }}</pre>
     *
     * <pre>{@code {
     * "ip" : "127.0.0.1",
     * "hostname" : "localhost",
     * "executorId" : "cassandra.node.1.executor",
     * "oldSeedState" : "true",
     * "success" : "true",
     * "seedState" : "false"
     * }}</pre>
     */
    @POST
    @Path("/{node}/make-non-seed")
    public Response nodeMakeNonSeed(@PathParam("node") String node) {
        return nodeUpdateSeed(node, false);
    }

    private Response nodeUpdateSeed(String node, final boolean seed) {
        final CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.findNode(node);
        if (cassandraNode == null) {
            return Response.status(404).build();
        }

        try {
            final boolean seedChanged = cluster.setNodeSeed(cassandraNode, seed);

            return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
                @Override
                public void write(JsonGenerator json) throws IOException {
                    json.writeStringField("ip", cassandraNode.getIp());
                    json.writeStringField("hostname", cassandraNode.getHostname());
                    if (!cassandraNode.hasCassandraNodeExecutor()) {
                        json.writeNullField("executorId");
                    } else {
                        json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                    }
                    json.writeBooleanField("oldSeedState", cassandraNode.getSeed());

                    if (seedChanged) {
                        json.writeBooleanField("success", true);
                        json.writeBooleanField("seedState", seed);
                    } else {
                        json.writeBooleanField("success", false);
                        json.writeBooleanField("seedState", cassandraNode.getSeed());
                    }

                }
            });
        } catch (final SeedChangeException e) {
            return JaxRsUtils.buildStreamingResponse(factory, Response.Status.BAD_REQUEST, new StreamingJsonResponse() {
                @Override
                public void write(JsonGenerator json) throws IOException {
                    json.writeStringField("ip", cassandraNode.getIp());
                    json.writeStringField("hostname", cassandraNode.getHostname());
                    if (!cassandraNode.hasCassandraNodeExecutor()) {
                        json.writeNullField("executorId");
                    } else {
                        json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                    }
                    json.writeBooleanField("oldSeedState", cassandraNode.getSeed());

                    json.writeBooleanField("success", false);
                    json.writeStringField("error", e.getMessage());
                }
            });
        }

    }


    /**
     * Sets requested state of the Cassandra process to 'stop' for the node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     */
    @POST
    @Path("/{node}/stop")
    public Response nodeStop(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeStop(node);

        return nodeStatusUpdate(cassandraNode);
    }

    /**
     * Sets requested state of the Cassandra process to 'run' for the node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     */
    @POST
    @Path("/{node}/start")
    public Response nodeStart(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeRun(node);

        return nodeStatusUpdate(cassandraNode);
    }

    /**
     * Sets requested state of the Cassandra process to 'restart' for the node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     */
    @POST
    @Path("/{node}/restart")
    public Response nodeRestart(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeRestart(node);

        return nodeStatusUpdate(cassandraNode);
    }

    /**
     * Sets requested state of the Cassandra process to 'terminate' for the node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     * Note that a terminated node cannot be restarted.
     */
    @POST
    @Path("/{node}/terminate")
    public Response nodeTerminate(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeTerminate(node);

        return nodeStatusUpdate(cassandraNode);
    }

    /**
     * Submit intent to replace the already terminated node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     */
    @POST
    @Path("/{node}/replace")
    public Response nodeReplace(@PathParam("node") String node) {
        final CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.findNode(node);

        if (cassandraNode == null) {
            return JaxRsUtils.buildStreamingResponse(factory, Response.Status.NOT_FOUND, new StreamingJsonResponse() {
                @Override
                public void write(JsonGenerator json) throws IOException {
                    json.writeBooleanField("success", false);
                    json.writeStringField("reason", "No such node");
                }
            });
        }

        try {
            cluster.replaceNode(node);
        } catch (ReplaceNodePreconditionFailed replaceNodePreconditionFailed) {
            return JaxRsUtils.buildStreamingResponse(factory, Response.Status.BAD_REQUEST, new StreamingJsonResponse() {
                @Override
                public void write(JsonGenerator json) throws IOException {
                    json.writeStringField("ipToReplace", cassandraNode.getIp());
                    json.writeBooleanField("success", false);
                    json.writeStringField("reason", "No such node");
                }
            });
        }

        return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                json.writeStringField("ipToReplace", cassandraNode.getIp());
                json.writeBooleanField("success", true);
                json.writeStringField("hostname", cassandraNode.getHostname());
                json.writeStringField("targetRunState", cassandraNode.getTargetRunState().name());
            }
        });
    }

    private Response nodeStatusUpdate(final CassandraFrameworkProtos.CassandraNode cassandraNode) {
        if (cassandraNode == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        return JaxRsUtils.buildStreamingResponse(
                factory, new StreamingJsonResponse() {
                    @Override
                    public void write(JsonGenerator json) throws IOException {
                        json.writeStringField("ip", cassandraNode.getIp());
                        json.writeStringField("hostname", cassandraNode.getHostname());
                        if (!cassandraNode.hasCassandraNodeExecutor()) {
                            json.writeNullField("executorId");
                        } else {
                            json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                        }
                        json.writeStringField("targetRunState", cassandraNode.getTargetRunState().name());
                    }
                }
        );
    }
}
