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
import io.mesosphere.mesos.util.CassandraFrameworkProtosUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;

@Path("/")
public final class ApiController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiController.class);

    private final CassandraCluster cluster;

    public ApiController(CassandraCluster cluster) {
        this.cluster = cluster;
    }

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
        for (String seed : cluster.getSeedNodes())
            json.writeString(seed);
        json.writeEndArray();
    }

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
// TODO          json.writeStringField("clusterName", configuration.getFrameworkId());

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

                writeTask(json, "serverTask", CassandraFrameworkProtosUtils.getTaskForNode(cassandraNode, CassandraFrameworkProtos.CassandraNodeTask.TaskType.SERVER));
                writeTask(json, "metadataTask", CassandraFrameworkProtosUtils.getTaskForNode(cassandraNode, CassandraFrameworkProtos.CassandraNodeTask.TaskType.METADATA));
// TODO                cassandraNode.getDataVolumesList();

                if (!cassandraNode.hasCassandraNodeExecutor()) {
                    json.writeNullField("executorId");
                } else {
                    json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
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

                if (lastHealthCheck != null)
                    json.writeNumberField("lastHealthCheck", lastHealthCheck.getTimestampEnd());
                else
                    json.writeNullField("lastHealthCheck");

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
                    json.writeStringField("endoint", hcd.getInfo().getEndpoint());
                    json.writeStringField("hostId", hcd.getInfo().getHostId());
                    json.writeBooleanField("joined", hcd.getInfo().getJoined());
                    json.writeBooleanField("gossipInitialized", hcd.getInfo().getGossipInitialized());
                    json.writeBooleanField("gossipRunning", hcd.getInfo().getGossipRunning());
                    json.writeBooleanField("nativeTransportRunning", hcd.getInfo().getNativeTransportRunning());
                    json.writeBooleanField("rpcServerRunning", hcd.getInfo().getRpcServerRunning());
                    json.writeNumberField("tokenCount", hcd.getInfo().getTokenCount());
                    json.writeNumberField("uptimeMillis", hcd.getInfo().getUptimeMillis());

                    json.writeEndObject();
                } else
                    json.writeNullField("healthCheckDetails");

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

    private void writeConfigRole(JsonGenerator json, CassandraFrameworkProtos.CassandraConfigRole configRole) throws IOException {
        json.writeStringField("cassandraVersion", configRole.getCassandraVersion());
        json.writeNumberField("targetNodeCount", configRole.getNumberOfNodes());
        json.writeNumberField("seedNodeCount", configRole.getNumberOfSeeds());
        json.writeNumberField("diskMb", configRole.getDiskMb());
        json.writeNumberField("cpuCores", configRole.getCpuCores());
        if (configRole.hasMemJavaHeapMb()) {
            json.writeNumberField("memJavaHeapMb", configRole.getMemJavaHeapMb());
        }
        if (configRole.hasMemAssumeOffHeapMb()) {
            json.writeNumberField("memAssumeOffHeapMb", configRole.getMemAssumeOffHeapMb());
        }
        if (configRole.hasMemMb()) {
            json.writeNumberField("memMb", configRole.getMemMb());
        }

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

    private static void writeTask(JsonGenerator json, String name, CassandraFrameworkProtos.CassandraNodeTask task) throws IOException {
        if (task != null && task.hasTaskId()) {
            json.writeObjectFieldStart(name);
            json.writeStringField("type", task.getTaskDetails().getTaskType().toString());
            json.writeNumberField("cpuCores", task.getCpuCores());
            json.writeNumberField("diskMb", task.getDiskMb());
            json.writeNumberField("memMb", task.getMemMb());
            json.writeStringField("executorId", task.getExecutorId());
            json.writeStringField("taskId", task.getTaskId());
            json.writeEndObject();
        }
        else
            json.writeNullField(name);
    }

    @GET
    @Path("/live-nodes")
    @Produces("application/json")
    public Response liveEndpointsJson(@QueryParam("limit") @DefaultValue("3") int limit) {
        return liveEndpoints("json", limit);
    }

    @GET
    @Path("/live-nodes/text")
    @Produces("text/plain")
    public Response liveEndpointsText(@QueryParam("limit") @DefaultValue("3") int limit) {
        return liveEndpoints("text", limit);
    }

    @GET
    @Path("/live-nodes/cqlsh")
    @Produces("text/x-cassandra-cqlsh")
    public Response liveEndpointsCqlsh() {
        return liveEndpoints("cqlsh", 1);
    }

    @GET
    @Path("/live-nodes/nodetool")
    @Produces("text/x-cassandra-nodetool")
    public Response liveEndpointsNodetool() {
        return liveEndpoints("nodetool", 1);
    }

    @GET
    @Path("/live-nodes/stress")
    @Produces("text/x-cassandra-stress")
    public Response liveEndpointsStress(@QueryParam("limit") @DefaultValue("3") int limit) {
        return liveEndpoints("stress", 1);
    }

    private Response liveEndpoints(String forTool, int limit) {
        List<CassandraFrameworkProtos.CassandraNode> liveNodes = cluster.liveNodes(limit);

        if (liveNodes.isEmpty())
            return Response.status(400).build();

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
                        if (i > 0)
                            sb.append(',');
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
                    sb.append(nativePort).append('\n');
                    for (CassandraFrameworkProtos.CassandraNode liveNode : liveNodes) {
                        sb.append(liveNode.getIp()).append('\n');
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

    @GET
    @Path("/repair/start")
    public Response repairStart() {
        return startJob(CassandraFrameworkProtos.ClusterJobType.REPAIR);
    }

    @GET
    @Path("/cleanup/start")
    public Response cleanupStart() {
        return startJob(CassandraFrameworkProtos.ClusterJobType.CLEANUP);
    }

    @GET
    @Path("/repair/abort")
    public Response repairAbort() {
        return abortJob(CassandraFrameworkProtos.ClusterJobType.REPAIR);
    }

    @GET
    @Path("/cleanup/abort")
    public Response cleanupAbort() {
        return abortJob(CassandraFrameworkProtos.ClusterJobType.CLEANUP);
    }

    @GET
    @Path("/repair/status")
    public Response repairStatus() {
        return jobStatus(CassandraFrameworkProtos.ClusterJobType.REPAIR, "repair");
    }

    @GET
    @Path("/cleanup/status")
    public Response cleanupStatus() {
        return jobStatus(CassandraFrameworkProtos.ClusterJobType.CLEANUP, "cleanup");
    }

    @GET
    @Path("/repair/last")
    public Response lastRepair() {
        return lastJob(CassandraFrameworkProtos.ClusterJobType.REPAIR, "repair");
    }

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
            if (jobStatus.hasFinishedTimestamp())
                json.writeNumberField("finished", jobStatus.getFinishedTimestamp());
            else
                json.writeNullField("finished");
            json.writeBooleanField("aborted", jobStatus.getAborted());

            json.writeArrayFieldStart("remainingNodes");
            for (String node : jobStatus.getRemainingNodesList()) {
                json.writeString(node);
            }
            json.writeEndArray();

            if (jobStatus.hasCurrentNode()) {
                json.writeObjectFieldStart("currentNode");
                writeClusterJobNode(json, jobStatus.getCurrentNode());
            }
            else
                json.writeNullField("currentNode");

            json.writeArrayFieldStart("completedNodes");
            for (CassandraFrameworkProtos.NodeJobStatus nodeJobStatus : jobStatus.getCompletedNodesList()) {
                json.writeStartObject();
                writeClusterJobNode(json, nodeJobStatus);
            }
            json.writeEndArray();

            json.writeEndObject();
        } else
            json.writeNullField(name);
    }

    private void writeClusterJobNode(JsonGenerator json, CassandraFrameworkProtos.NodeJobStatus nodeJobStatus) throws IOException {
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

        json.writeObjectFieldStart("processedKeyspaces");
        for (CassandraFrameworkProtos.ClusterJobKeyspaceStatus clusterJobKeyspaceStatus : nodeJobStatus.getProcessedKeyspacesList()) {
            json.writeObjectFieldStart(clusterJobKeyspaceStatus.getKeyspace());
            json.writeStringField("status", clusterJobKeyspaceStatus.getStatus());
            json.writeNumberField("durationMillis", clusterJobKeyspaceStatus.getDuration());
            json.writeEndObject();
        }
        json.writeEndObject();

        json.writeArrayFieldStart("remainingKeyspaces");
        for (String keyspace : nodeJobStatus.getRemainingKeyspacesList())
            json.writeString(keyspace);
        json.writeEndArray();

        json.writeEndObject();
    }

    // node run / stop / restart

    @POST
    @Path("/node/stop/{node}")
    public Response nodeStop(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeStop(node);

        return nodeStatusUpdate(cassandraNode);
    }

    @POST
    @Path("/node/run/{node}")
    public Response nodeStart(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeRun(node);

        return nodeStatusUpdate(cassandraNode);
    }

    @POST
    @Path("/node/restart/{node}")
    public Response nodeRestart(@PathParam("node") String node) {
        CassandraFrameworkProtos.CassandraNode cassandraNode = cluster.nodeRestart(node);

        return nodeStatusUpdate(cassandraNode);
    }

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
