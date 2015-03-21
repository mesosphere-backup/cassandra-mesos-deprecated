/**
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
package io.mesosphere.mesos.frameworks.cassandra;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.google.common.base.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

@Path("/")
public final class ApiController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiController.class);

    private final CassandraCluster cluster;

    public ApiController(CassandraCluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Basially a poor man's index page.
     */
    @GET
    public Response indexPage(@Context UriInfo uriInfo) {
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            String baseUrl = uriInfo.getBaseUri().toString();

            json.writeStringField("configuration", baseUrl + "config");
            json.writeStringField("seedNodes", baseUrl + "seed-nodes");
            json.writeStringField("allNodes", baseUrl + "nodes");

            json.writeObjectFieldStart("repair");
            json.writeStringField("start", baseUrl + "repair/start");
            json.writeStringField("status", baseUrl + "repair/status");
            json.writeStringField("lastStatus", baseUrl + "repair/last");
            json.writeStringField("abort", baseUrl + "repair/abort");
            json.writeEndObject();

            json.writeObjectFieldStart("cleanup");
            json.writeStringField("start", baseUrl + "cleanup/start");
            json.writeStringField("status", baseUrl + "cleanup/status");
            json.writeStringField("lastStatus", baseUrl + "cleanup/last");
            json.writeStringField("abort", baseUrl + "cleanup/abort");
            json.writeEndObject();

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to JSON doc", e);
            return Response.serverError().build();
        }

        return Response.ok(sw.toString(), "application/json").build();
    }

    /**
     * Returns a JSON with the IP addresses of all seed nodes and native, thrift and JMX port numbers.
     *
     *     Example:
     *     <pre>{@code {
     * "nativePort" : 9042,
     * "rpcPort" : 9160,
     * "jmxPort" : 7199,
     * "seeds" : [ "127.0.0.1" ]
     * }}</pre>
     */
    @GET
    @Path("/seed-nodes")
    public Response seedNodes() {
        StringWriter sw = new StringWriter();
        try {
            CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration().get();

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            json.writeNumberField("nativePort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_NATIVE));
            json.writeNumberField("rpcPort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_RPC));
            json.writeNumberField("jmxPort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_JMX));

            writeSeedIps(json);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build seed list", e);
            return Response.serverError().build();
        }

        return Response.ok(sw.toString(), "application/json").build();
    }

    private void writeSeedIps(JsonGenerator json) throws IOException {
        json.writeArrayFieldStart("seeds");
        for (String seed : cluster.getSeedNodeIps()) {
            json.writeString(seed);
        }
        json.writeEndArray();
    }

    /**
     * Returns the configuration as JSON.
     *
     *     Example: <pre>{@code {
     * "frameworkName" : "cassandra",
     * "frameworkId" : "20150318-143436-16777343-5050-5621-0000",
     * "defaultConfigRole" : {
     *     "cassandraVersion" : "2.1.2",
     *     "targetNodeCount" : 2,
     *     "seedNodeCount" : 1,
     *     "diskMb" : 2048,
     *     "cpuCores" : 2.0,
     *     "memJavaHeapMb" : 1024,
     *     "memAssumeOffHeapMb" : 1024,
     *     "memMb" : 2048,
     *     "taskEnv" : null
     * },
     * "nativePort" : 9042,
     * "rpcPort" : 9160,
     * "storagePort" : 7000,
     * "sslStoragePort" : 7001,
     * "seeds" : [ "127.0.0.1" ],
     * "healthCheckIntervalSeconds" : 10,
     * "bootstrapGraceTimeSeconds" : 0,
     * "currentClusterTask" : null,
     * "lastRepair" : null,
     * "lastCleanup" : null,
     * "nextPossibleServerLaunchTimestamp" : 1426685858805
     * }}</pre>
     */
    @GET
    @Path("/config")
    public Response config() {
        StringWriter sw = new StringWriter();
        try {
            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration().get();

            json.writeStringField("frameworkName", configuration.getFrameworkName());
            json.writeStringField("frameworkId", configuration.getFrameworkId());
            json.writeStringField("clusterName", configuration.getFrameworkName());

            CassandraFrameworkProtos.CassandraConfigRole configRole = configuration.getDefaultConfigRole();
            json.writeObjectFieldStart("defaultConfigRole");
            writeConfigRole(json, configRole);
            json.writeEndObject();

            json.writeNumberField("nativePort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_NATIVE));
            json.writeNumberField("rpcPort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_RPC));
            json.writeNumberField("storagePort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_STORAGE));
            json.writeNumberField("sslStoragePort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_STORAGE_SSL));

            writeSeedIps(json);

            json.writeNumberField("healthCheckIntervalSeconds", configuration.getHealthCheckIntervalSeconds());
            json.writeNumberField("bootstrapGraceTimeSeconds", configuration.getBootstrapGraceTimeSeconds());

            CassandraFrameworkProtos.ClusterJobStatus currentTask = cluster.getCurrentClusterJob();
            writeClusterJob(json, "currentClusterTask", currentTask);

            CassandraFrameworkProtos.ClusterJobStatus lastRepair = cluster.getLastClusterJob(CassandraFrameworkProtos.ClusterJobType.REPAIR);
            writeClusterJob(json, "lastRepair", lastRepair);

            CassandraFrameworkProtos.ClusterJobStatus lastCleanup = cluster.getLastClusterJob(CassandraFrameworkProtos.ClusterJobType.CLEANUP);
            writeClusterJob(json, "lastCleanup", lastCleanup);

            json.writeNumberField("nextPossibleServerLaunchTimestamp", cluster.nextPossibleServerLaunchTimestamp());

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to all nodes list", e);
            return Response.serverError().build();
        }

        return Response.ok(sw.toString(), "application/json").build();
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
     *         "version" : "2.1.2",
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
     *         "version" : "2.1.2",
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
    @Path("/qaReportResources/text")
    public Response qaReportResourcesText() {
        CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration().get();
        int jmxPort = CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_JMX);

        StringWriter sw = new StringWriter();
        try (PrintWriter pw = new PrintWriter(sw)) {
            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            pw.println("JMX: " + jmxPort);

            CassandraFrameworkProtos.CassandraClusterState clusterState = cluster.getClusterState().get();
            for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.getNodesList()) {

                if (!cassandraNode.hasCassandraNodeExecutor()) {
                    continue;
                }

                pw.println("IP: " + cassandraNode.getIp());
                pw.println("BASE: http://" + cassandraNode.getIp() + ":5051/");

                for (String logFile : cluster.getNodeLogFiles(cassandraNode)) {
                    pw.println("LOG: " + logFile);
                }

            }
        } catch (Exception e) {
            LOGGER.error("Failed to all nodes list", e);
            return Response.serverError().build();
        }

        return Response.ok(sw.toString(), "text/plain").build();
    }

    @GET
    @Path("/qaReportResources")
    public Response qaReportResources() {
        CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration().get();
        int jmxPort = CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_JMX);

        StringWriter sw = new StringWriter();
        try {
            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());

            json.writeStartObject();

            json.writeNumberField("jmxPort", jmxPort);

            CassandraFrameworkProtos.CassandraClusterState clusterState = cluster.getClusterState().get();
            json.writeObjectFieldStart("nodes");
            for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.getNodesList()) {

                if (!cassandraNode.hasCassandraNodeExecutor()) {
                    continue;
                }

                CassandraFrameworkProtos.ExecutorMetadata executorMetadata = cluster.metadataForExecutor(cassandraNode.getCassandraNodeExecutor().getExecutorId());
                if (executorMetadata == null) {
                    continue;
                }

                json.writeObjectFieldStart(cassandraNode.getCassandraNodeExecutor().getExecutorId());
                String workdir = executorMetadata.getWorkdir();
                json.writeStringField("workdir", workdir);

                json.writeStringField("slaveBaseUri", "http://" + cassandraNode.getIp() + ":5051/");

                json.writeStringField("ip", cassandraNode.getIp());
                json.writeStringField("hostname", cassandraNode.getHostname());
                json.writeStringField("targetRunState", cassandraNode.getTargetRunState().name());
                json.writeNumberField("jmxPort", cassandraNode.getJmxConnect().getJmxPort());

                json.writeBooleanField("live", cluster.isLiveNode(cassandraNode));

                json.writeArrayFieldStart("logfiles");
                for (String logFile : cluster.getNodeLogFiles(cassandraNode)) {
                    json.writeString(logFile);
                }
                json.writeEndArray();

                json.writeEndObject();

            }
            json.writeEndObject();

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to all nodes list", e);
            return Response.serverError().build();
        }

        return Response.ok(sw.toString(), "application/json").build();
    }

    @GET
    @Path("/nodes")
    public Response nodes() {
        StringWriter sw = new StringWriter();
        try {
            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());

            json.writeStartObject();
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
                    writeTask(json, cassandraNodeTask);
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

                json.writeEndObject();
            }
            json.writeEndArray();
            json.writeEndObject();

            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to all nodes list", e);
            return Response.serverError().build();
        }

        return Response.ok(sw.toString(), "application/json").build();
    }

    private static void writeConfigRole(JsonGenerator json, CassandraFrameworkProtos.CassandraConfigRole configRole) throws IOException {
        json.writeStringField("cassandraVersion", configRole.getCassandraVersion());
        json.writeNumberField("targetNodeCount", configRole.getNumberOfNodes());
        json.writeNumberField("seedNodeCount", configRole.getNumberOfSeeds());
        json.writeNumberField("diskMb", configRole.getResources().getDiskMb());
        json.writeNumberField("cpuCores", configRole.getResources().getCpuCores());
        if (configRole.hasMemJavaHeapMb()) {
            json.writeNumberField("memJavaHeapMb", configRole.getMemJavaHeapMb());
        }
        if (configRole.hasMemAssumeOffHeapMb()) {
            json.writeNumberField("memAssumeOffHeapMb", configRole.getMemAssumeOffHeapMb());
        }
        json.writeNumberField("memMb", configRole.getResources().getMemMb());

        if (!configRole.hasTaskEnv()) {
            json.writeNullField("taskEnv");
        } else {
            json.writeObjectFieldStart("taskEnv");
            for (CassandraFrameworkProtos.TaskEnv.Entry entry : configRole.getTaskEnv().getVariablesList()) {
                json.writeStringField(entry.getName(), entry.getValue());
            }
            json.writeEndObject();
            json.writeObjectFieldStart("cassandraYaml");
            for (CassandraFrameworkProtos.TaskConfig.Entry entry : configRole.getCassandraYamlConfig().getVariablesList()) {
                if (entry.hasLongValue()) {
                    json.writeNumberField(entry.getName(), entry.getLongValue());
                }
                if (entry.hasStringValue()) {
                    json.writeStringField(entry.getName(), entry.getStringValue());
                }
            }
            json.writeEndObject();
        }
    }

    private static void writeTask(JsonGenerator json, CassandraFrameworkProtos.CassandraNodeTask task) throws IOException {
        if (task != null && task.hasTaskId()) {
            json.writeObjectFieldStart(task.getType().toString());
            json.writeNumberField("cpuCores", task.getResources().getCpuCores());
            json.writeNumberField("diskMb", task.getResources().getDiskMb());
            json.writeNumberField("memMb", task.getResources().getMemMb());
            json.writeStringField("taskId", task.getTaskId());
            json.writeEndObject();
        }
    }

    /**
     * Variant of the live Cassandra nodes endpoint that produces JSON.
     * Allows to retrieve multiple live nodes limited to 3 nodes by default. The limit can be changed with the
     * query parameter {@code limit}.
     *
     *     Example: <pre>{@code {
     * "nativePort" : 9042,
     * "rpcPort" : 9160,
     * "jmxPort" : 7199,
     * "liveNodes" : [ "127.0.0.1", "127.0.0.2" ]
     * }}</pre>
     */
    @GET
    @Path("/live-nodes")
    @Produces("application/json")
    public Response liveEndpointsJson(@QueryParam("limit") @DefaultValue("3") int limit) {
        return liveEndpoints("json", limit);
    }

    /**
     * Variant of the live Cassandra nodes endpoint that produces plain text.
     * Allows to retrieve multiple live nodes limited to 3 nodes by default. The limit can be changed with the
     * query parameter {@code limit}.
     *
     *     Example: <pre>{@code NATIVE: 9042
     * RPC: 9160
     * JMX: 7199
     * IP: 127.0.0.1
     * IP: 127.0.0.2
     * }</pre>
     */
    @GET
    @Path("/live-nodes/text")
    @Produces("text/plain")
    public Response liveEndpointsText(@QueryParam("limit") @DefaultValue("3") int limit) {
        return liveEndpoints("text", limit);
    }

    /**
     * Variant of the live Cassandra nodes endpoint that produces partial command line for cqlsh.
     */
    @GET
    @Path("/live-nodes/cqlsh")
    @Produces("text/x-cassandra-cqlsh")
    public Response liveEndpointsCqlsh() {
        return liveEndpoints("cqlsh", 1);
    }

    /**
     * Variant of the live Cassandra nodes endpoint that produces partial command line for nodetool.
     */
    @GET
    @Path("/live-nodes/nodetool")
    @Produces("text/x-cassandra-nodetool")
    public Response liveEndpointsNodetool() {
        return liveEndpoints("nodetool", 1);
    }

    /**
     * Variant of the live Cassandra nodes endpoint that produces partial command line for cassandra-stress.
     * Allows to retrieve multiple nodes limited to 3 nodes by default. The limit can be changed with the
     * query parameter {@code limit}.
     */
    @GET
    @Path("/live-nodes/stress")
    @Produces("text/x-cassandra-stress")
    public Response liveEndpointsStress(@QueryParam("limit") @DefaultValue("3") int limit) {
        return liveEndpoints("stress", limit);
    }

    private Response liveEndpoints(String forTool, int limit) {
        List<CassandraFrameworkProtos.CassandraNode> liveNodes = cluster.liveNodes(limit);

        if (liveNodes.isEmpty()) {
            return Response.status(400).build();
        }

        CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration().get();

        int nativePort = CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_NATIVE);
        int rpcPort = CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_RPC);
        int jmxPort = CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_JMX);

        CassandraFrameworkProtos.CassandraNode first = liveNodes.get(0);

        StringWriter sw = new StringWriter();
        try {
            switch (forTool) {
                case "cqlsh":
                    // return a string: "HOST PORT"
                    return Response.ok(first.getIp() + ' ' + nativePort).build();
                case "stress":
                    // cassandra-stress options:
                    // -node NODE1,NODE2,...
                    // -port [native=NATIVE_PORT] [thrift=THRIFT_PORT] [jmx=JMX_PORT]
                    StringBuilder sb = new StringBuilder();
                    sb.append("-node ");
                    for (int i = 0; i < liveNodes.size(); i++) {
                        if (i > 0) {
                            sb.append(',');
                        }
                        sb.append(liveNodes.get(i).getIp());
                    }
                    sb.append(" -port native=")
                        .append(nativePort)
                        .append(" thrift=")
                        .append(rpcPort)
                        .append(" jmx=")
                        .append(jmxPort);
                    return Response.ok(sb.toString()).build();
                case "nodetool":
                    // nodetool options:
                    // -h HOST
                    // -p JMX_PORT
                    return Response.ok("-h " + first.getIp() + " -p " + first.getJmxConnect().getJmxPort()).build();
                case "json":
                    // produce a simple JSON with the native port and live node IPs
                    JsonFactory factory = new JsonFactory();
                    JsonGenerator json = factory.createGenerator(sw);
                    json.setPrettyPrinter(new DefaultPrettyPrinter());
                    json.writeStartObject();

                    json.writeStringField("clusterName", configuration.getFrameworkName());
                    json.writeNumberField("nativePort", nativePort);
                    json.writeNumberField("rpcPort", rpcPort);
                    json.writeNumberField("jmxPort", jmxPort);

                    json.writeArrayFieldStart("liveNodes");
                    for (CassandraFrameworkProtos.CassandraNode liveNode : liveNodes) {
                        json.writeString(liveNode.getIp());
                    }
                    json.writeEndArray();

                    json.writeEndObject();
                    json.close();
                    return Response.ok(sw.toString()).build();
                case "text":
                    // produce a simple text with the native port in the first line and one line per live node IP
                    sb = new StringBuilder();
                    sb.append("NATIVE: ").append(nativePort).append('\n');
                    sb.append("RPC: ").append(rpcPort).append('\n');
                    sb.append("JMX: ").append(jmxPort).append('\n');
                    for (CassandraFrameworkProtos.CassandraNode liveNode : liveNodes) {
                        sb.append("IP: ").append(liveNode.getIp()).append('\n');
                    }
                    return Response.ok(sb.toString()).build();
            }

            return Response.status(404).build();
        } catch (Exception e) {
            LOGGER.error("Failed to all nodes list", e);
            return Response.serverError().build();
        }
    }

    // cluster scaling

    /**
     * Allows to scale out the Cassandra cluster by increasing the number of nodes.
     * Requires the query parameter {@code nodes} defining the desired number of total nodes.
     * Must be submitted using HTTP method {@code POST}.
     */
    @POST
    @Path("/scale/nodes")
    public Response updateNodeCount(@QueryParam("nodes") int nodeCount) {
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CassandraFrameworkProtos.CassandraConfigRole configRole = cluster.getConfiguration().getDefaultConfigRole();
            json.writeNumberField("oldNodeCount", configRole.getNumberOfNodes());
            json.writeNumberField("seedNodeCount", configRole.getNumberOfSeeds());
            int newCount = cluster.updateNodeCount(nodeCount);
            json.writeBooleanField("applied", newCount == nodeCount);
            json.writeNumberField("newNodeCount", newCount);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }


    // repair + cleanup stuff

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
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            boolean started = cluster.startClusterTask(type);
            json.writeBooleanField("started", started);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    private Response abortJob(CassandraFrameworkProtos.ClusterJobType type) {
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            boolean aborted = cluster.abortClusterJob(type);
            json.writeBooleanField("aborted", aborted);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    private Response jobStatus(CassandraFrameworkProtos.ClusterJobType type, String name) {
        StringWriter sw = new StringWriter();
        try {

            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CassandraFrameworkProtos.ClusterJobStatus repairJob = cluster.getCurrentClusterJob(type);
            json.writeBooleanField("running", repairJob != null);
            writeClusterJob(json, name, repairJob);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    private Response lastJob(CassandraFrameworkProtos.ClusterJobType type, String name) {
        StringWriter sw = new StringWriter();
        try {

            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CassandraFrameworkProtos.ClusterJobStatus repairJob = cluster.getLastClusterJob(type);
            json.writeBooleanField("present", repairJob != null);
            writeClusterJob(json, name, repairJob);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    private void writeClusterJob(JsonGenerator json, String name, CassandraFrameworkProtos.ClusterJobStatus jobStatus) throws IOException {
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

    private void writeNodeJobStatus(JsonGenerator json, CassandraFrameworkProtos.NodeJobStatus nodeJobStatus) throws IOException {
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

    //

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
     *  <p></p>
     *  <pre>{@code {
     * "ip" : "127.0.0.1",
     * "hostname" : "localhost",
     * "executorId" : "cassandra.node.1.executor",
     * "oldSeedState" : "false",
     * "success" : "true",
     * "seedState" : "true"
     * }}</pre>
     */
    @POST
    @Path("/node/seed/{node}")
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
     *  <p></p>
     *  <pre>{@code {
     * "ip" : "127.0.0.1",
     * "hostname" : "localhost",
     * "executorId" : "cassandra.node.1.executor",
     * "oldSeedState" : "true",
     * "success" : "true",
     * "seedState" : "false"
     * }}</pre>
     */
    @POST
    @Path("/node/non-seed/{node}")
    public Response nodeMakeNonSeed(@PathParam("node") String node) {
        return nodeUpdateSeed(node, false);
    }

    private Response nodeUpdateSeed(String node, boolean seed) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.findNode(node);
        if (cassandraNode == null) {
            Response.status(404).build();
        }

        int status = 200;
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            if (cassandraNode == null) {
                status = 404;
            } else {

                json.writeStringField("ip", cassandraNode.getIp());
                json.writeStringField("hostname", cassandraNode.getHostname());
                if (!cassandraNode.hasCassandraNodeExecutor()) {
                    json.writeNullField("executorId");
                } else {
                    json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                }
                json.writeBooleanField("oldSeedState", cassandraNode.getSeed());

                try {
                    if (cluster.setNodeSeed(cassandraNode, false)) {
                        json.writeBooleanField("success", true);
                        json.writeBooleanField("seedState", seed);
                    } else {
                        json.writeBooleanField("success", false);
                        json.writeBooleanField("seedState", cassandraNode.getSeed());
                    }
                } catch (SeedChangeException e) {
                    status = 400;
                    json.writeBooleanField("success", false);
                    json.writeStringField("error", e.getMessage());
                }

            }

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.status(status).entity(sw.toString()).type("application/json").build();
    }

    // node run / stop / restart

    /**
     * Sets requested state of the Cassandra process to 'stop' for the node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     */
    @POST
    @Path("/node/stop/{node}")
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
    @Path("/node/run/{node}")
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
    @Path("/node/restart/{node}")
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
    @Path("/node/terminate/{node}")
    public Response nodeTerminate(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeTerminate(node);

        return nodeStatusUpdate(cassandraNode);
    }

    private static Response nodeStatusUpdate(CassandraFrameworkProtos.CassandraNode cassandraNode) {

        int status = 200;
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            if (cassandraNode == null) {
                status = 404;
            } else {
                json.writeStringField("ip", cassandraNode.getIp());
                json.writeStringField("hostname", cassandraNode.getHostname());
                if (!cassandraNode.hasCassandraNodeExecutor()) {
                    json.writeNullField("executorId");
                } else {
                    json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                }
                json.writeStringField("targetRunState", cassandraNode.getTargetRunState().name());
            }

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.status(status).entity(sw.toString()).type("application/json").build();
    }

    /**
     * Submit intent to replace the already terminated node specified using the path parameter `node`.
     * The `node` parameter can be either the IP address, the hostname or the executor ID.
     * The request must be submitted using HTTP method `POST`.
     */
    @POST
    @Path("/node/replace/{node}")
    public Response nodeReplace(@PathParam("node") String node) {
        int status = 200;
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.findNode(node);

            if (cassandraNode == null) {
                status = 404;
                json.writeBooleanField("success", false);
                json.writeStringField("reason", "No such node");
            } else {
                json.writeStringField("ipToReplace", cassandraNode.getIp());
                try {
                    cluster.replaceNode(node);

                    json.writeBooleanField("success", true);
                    json.writeStringField("hostname", cassandraNode.getHostname());
                    json.writeStringField("targetRunState", cassandraNode.getTargetRunState().name());
                } catch (ReplaceNodePreconditionFailed e) {
                    json.writeBooleanField("success", false);
                    json.writeStringField("reason", e.getMessage());
                }
            }

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.status(status).entity(sw.toString()).type("application/json").build();
    }
}
