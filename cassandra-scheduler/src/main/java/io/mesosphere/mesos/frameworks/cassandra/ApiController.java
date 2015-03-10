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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringWriter;

@Path("/")
public final class ApiController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiController.class);

    private final CassandraCluster cluster;

    public ApiController(CassandraCluster cluster) {
        this.cluster = cluster;
    }

    @GET
    @Path("/")
    @Produces("text/html")
    public String indexPage() {
        return
            "<a href=\"config\">Configuration</a> <br/>" +
            "<a href=\"nodes\">All nodes</a> <br/>" +
            "<a href=\"seed-nodes\">List of seed nodes</a> <br/>" +
            "<a href=\"repair/start\">Start repair</a> <br/>" +
            "<a href=\"repair/status\">Current repair status</a> <br/>" +
            "<a href=\"repair/last\">Last repair</a> <br/>" +
            "<a href=\"repair/abort\">Abort current repair</a> <br/>" +
            "<a href=\"cleanup/start\">Start cleanup</a> <br/>" +
            "<a href=\"cleanup/status\">Current cleanup status</a> <br/>" +
            "<a href=\"cleanup/last\">Last cleanup</a> <br/>" +
            "<a href=\"cleanup/abort\">Abort current cleanup</a> <br/>";
    }

    @GET
    @Path("/seed-nodes")
    @Produces("application/json")
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
    @Produces("application/json")
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
            json.writeStringField("cassandraVersion", configuration.getCassandraVersion());
            json.writeNumberField("numberOfNodes", configuration.getNumberOfNodes());
            json.writeNumberField("numbeOfSeeds", configuration.getNumberOfSeeds());

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
    @Produces("application/json")
    public Response nodes() {
        StringWriter sw = new StringWriter();
        try {
            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());

            json.writeStartObject();
            CassandraFrameworkProtos.CassandraClusterState clusterState = cluster.getClusterState().get();
            json.writeArrayFieldStart("nodes");
            for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.getNodesList()) {
                json.writeStartObject();

                writeTask(json, "serverTask", cassandraNode.getServerTask());
                writeTask(json, "metadataTask", cassandraNode.getMetadataTask());
// TODO                cassandraNode.getDataVolumesList();

                json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                json.writeStringField("ip", cassandraNode.getIp());
                json.writeStringField("hostname", cassandraNode.getHostname());
                json.writeNumberField("jmxPort", cassandraNode.getJmxConnect().getJmxPort());
// TODO               json.writeStringField("status", executorMetadata.getStatus().name());

                CassandraFrameworkProtos.HealthCheckHistoryEntry lastHealthCheck = cluster.lastHealthCheck(cassandraNode.getCassandraNodeExecutor().getExecutorId());

                if (lastHealthCheck != null)
                    json.writeNumberField("lastHealthCheck", lastHealthCheck.getTimestamp());
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

    // cluster scaling

    @GET
    @Path("/scale/nodes")
    @Produces("application/json")
    public Response updateNodeCount(@QueryParam("nodes") int nodeCount) {
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            json.writeNumberField("oldNodeCount", cluster.getConfiguration().get().getNumberOfNodes());
            json.writeNumberField("seedNodeCount", cluster.getConfiguration().get().getNumberOfSeeds());
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
    @Produces("application/json")
    public Response repairStart() {
        return startJob(CassandraFrameworkProtos.ClusterJobType.REPAIR);
    }

    @GET
    @Path("/cleanup/start")
    @Produces("application/json")
    public Response cleanupStart() {
        return startJob(CassandraFrameworkProtos.ClusterJobType.CLEANUP);
    }

    @GET
    @Path("/repair/abort")
    @Produces("application/json")
    public Response repairAbort() {
        return abortJob(CassandraFrameworkProtos.ClusterJobType.REPAIR);
    }

    @GET
    @Path("/cleanup/abort")
    @Produces("application/json")
    public Response cleanupAbort() {
        return abortJob(CassandraFrameworkProtos.ClusterJobType.CLEANUP);
    }

    @GET
    @Path("/repair/status")
    @Produces("application/json")
    public Response repairStatus() {
        return jobStatus(CassandraFrameworkProtos.ClusterJobType.REPAIR, "repair");
    }

    @GET
    @Path("/cleanup/status")
    @Produces("application/json")
    public Response cleanupStatus() {
        return jobStatus(CassandraFrameworkProtos.ClusterJobType.CLEANUP, "cleanup");
    }

    @GET
    @Path("/repair/last")
    @Produces("application/json")
    public Response lastRepair() {
        return lastJob(CassandraFrameworkProtos.ClusterJobType.REPAIR, "repair");
    }

    @GET
    @Path("/cleanup/last")
    @Produces("application/json")
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
}
