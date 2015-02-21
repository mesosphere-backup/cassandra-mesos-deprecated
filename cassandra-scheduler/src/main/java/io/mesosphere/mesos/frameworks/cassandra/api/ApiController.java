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
package io.mesosphere.mesos.frameworks.cassandra.api;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import io.mesosphere.mesos.frameworks.cassandra.CassandraTaskProtos;
import io.mesosphere.mesos.frameworks.cassandra.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

@Path("/")
public final class ApiController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApiController.class);

    @GET
    @Path("/")
    @Produces("text/html")
    public String helloWorld() {
        return "<a href=\"all-nodes\">All nodes</a> <br/>" +
                "<a href=\"seed-nodes\">List of seed nodes</a> <br/>" +
                "<br/>" +
                "<a href=\"repair/start\">Start repair</a> <br/>" +
                "<a href=\"repair/status\">Current repair status</a> <br/>" +
                "<a href=\"repair/last\">Last repair</a> <br/>" +
                "<a href=\"repair/abort\">Abort current repair</a> <br/>" +
                "<br/>" +
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
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            json.writeNumberField("native_port", CassandraCluster.singleton().getNativePort());
            json.writeNumberField("rpc_port", CassandraCluster.singleton().getRpcPort());

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
        for (String seed : CassandraCluster.singleton().seedsIpList())
            json.writeString(seed);
        json.writeEndArray();
    }

    @GET
    @Path("/status")
    @Produces("application/json")
    public Response allNodes() {
        StringWriter sw = new StringWriter();
        try {
            CassandraCluster cluster = CassandraCluster.singleton();

            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            json.writeStringField("cluster_name", cluster.getName());
            json.writeStringField("target_version", cluster.getCassandraVersion());
            json.writeNumberField("target_node_count", cluster.getNodeCount());

            json.writeNumberField("seed_node_count", cluster.getSeedNodeCount());

            json.writeNumberField("native_port", cluster.getNativePort());
            json.writeNumberField("rpc_port", cluster.getRpcPort());
            json.writeNumberField("storage_port", cluster.getStoragePort());
            json.writeNumberField("ssl_storage_port", cluster.getSslStoragePort());

            ClusterJob currentJob = cluster.currentClusterJob();
            if (currentJob == null)
                json.writeNullField("current_cluster_job");
            else {
                json.writeObjectFieldStart("current_cluster_job");
                json.writeStringField("type", currentJob.getClass().getSimpleName());
                json.writeEndObject();
            }

            writeSeedIps(json);

            json.writeNumberField("health_check_interval_millis", cluster.getHealthCheckIntervalMillis());

            json.writeObjectFieldStart("node_bootstrap");
            json.writeNumberField("bootstrap_grace_time_millis", cluster.getBootstrapGraceTimeMillis());
            json.writeBooleanField("could_add_node", cluster.canAddNode());
            json.writeNumberField("next_possible_node_launch", cluster.getNextNodeLaunchTime());
            json.writeEndObject();

            json.writeArrayFieldStart("nodes");
            for (ExecutorMetadata executorMetadata : cluster.allNodes()) {
                json.writeStartObject();

                json.writeStringField("executor_id", executorMetadata.getExecutorId().getValue());
                json.writeStringField("ip", executorMetadata.getIp());
                json.writeStringField("hostname", executorMetadata.getHostname());
                json.writeNumberField("jmx_port", executorMetadata.getJmxPort());
                json.writeStringField("status", executorMetadata.getStatus().name());

                long lhc = executorMetadata.getLastHealthCheck();
                if (lhc > 0)
                    json.writeNumberField("last_health_check", lhc);
                else
                    json.writeNullField("last_health_check");

                long lr = executorMetadata.getLastRepair();
                if (lr > 0)
                    json.writeNumberField("last_repair", lr);
                else
                    json.writeNullField("last_repair");

                long lc = executorMetadata.getLastCleanup();
                if (lc > 0)
                    json.writeNumberField("last_cleanup", lc);
                else
                    json.writeNullField("last_cleanup");

                CassandraTaskProtos.CassandraNodeHealthCheckDetails hcd = executorMetadata.getLastHealthCheckDetails();
                if (hcd != null) {
                    json.writeObjectFieldStart("health_check_details");

                    json.writeBooleanField("healthy", hcd.getHealthy());
                    json.writeStringField("msg", hcd.getMsg());

                    json.writeStringField("version", hcd.getInfo().getVersion());
                    json.writeStringField("operation_mode", hcd.getInfo().getOperationMode());
                    json.writeStringField("cluster_name", hcd.getInfo().getClusterName());
                    json.writeStringField("data_center", hcd.getInfo().getDataCenter());
                    json.writeStringField("rack", hcd.getInfo().getRack());
                    json.writeStringField("endoint", hcd.getInfo().getEndpoint());
                    json.writeStringField("host_id", hcd.getInfo().getHostId());
                    json.writeBooleanField("joined", hcd.getInfo().getJoined());
                    json.writeBooleanField("gossip_initialized", hcd.getInfo().getGossipInitialized());
                    json.writeBooleanField("gossip_running", hcd.getInfo().getGossipRunning());
                    json.writeBooleanField("native_transport_running", hcd.getInfo().getNativeTransportRunning());
                    json.writeBooleanField("rpc_server_running", hcd.getInfo().getRpcServerRunning());
                    json.writeNumberField("token_count", hcd.getInfo().getTokenCount());
                    json.writeNumberField("uptime_millis", hcd.getInfo().getUptimeMillis());

                    json.writeEndObject();
                } else
                    json.writeNullField("health_check_details");

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

            CassandraCluster cluster = CassandraCluster.singleton();
            json.writeNumberField("old_node_count", cluster.getNodeCount());
            json.writeNumberField("seed_node_count", cluster.getSeedNodeCount());
            int newCount = cluster.updateNodeCount(nodeCount);
            json.writeBooleanField("applied", newCount == nodeCount);
            json.writeNumberField("new_node_count", newCount);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    // cleanup stuff

    @GET
    @Path("/cleanup/start")
    @Produces("application/json")
    public Response cleanupStart() {
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            boolean started = CassandraCluster.singleton().cleanupStart(null);
            json.writeBooleanField("started", started);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    @GET
    @Path("/cleanup/status")
    @Produces("application/json")
    public Response cleanupStatus() {
        StringWriter sw = new StringWriter();
        try {

            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CleanupJob cleanupJob = CassandraCluster.singleton().currentClusterJob(CleanupJob.class);
            json.writeBooleanField("running", cleanupJob != null);
            writeClusterJob(json, "cleanup", cleanupJob);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    @GET
    @Path("/cleanup/last")
    @Produces("application/json")
    public Response lastCleanup() {
        StringWriter sw = new StringWriter();
        try {

            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CleanupJob cleanupJob = CassandraCluster.singleton().lastCompletedJob(CleanupJob.class);
            json.writeBooleanField("present", cleanupJob != null);
            writeClusterJob(json, "cleanup", cleanupJob);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    // repair stuff

    @GET
    @Path("/repair/start")
    @Produces("application/json")
    public Response repairStart() {
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            boolean started = CassandraCluster.singleton().repairStart();
            json.writeBooleanField("started", started);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    @GET
    @Path("/repair/abort")
    @Produces("application/json")
    public Response repairAbort() {
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            boolean aborted = CassandraCluster.singleton().abortClusterJob(RepairJob.class);
            json.writeBooleanField("aborted", aborted);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    @GET
    @Path("/repair/status")
    @Produces("application/json")
    public Response repairStatus() {
        StringWriter sw = new StringWriter();
        try {

            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            RepairJob repairJob = CassandraCluster.singleton().currentClusterJob(RepairJob.class);
            json.writeBooleanField("running", repairJob != null);
            writeClusterJob(json, "repair", repairJob);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    @GET
    @Path("/repair/last")
    @Produces("application/json")
    public Response lastRepair() {
        StringWriter sw = new StringWriter();
        try {

            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            RepairJob repairJob = CassandraCluster.singleton().lastCompletedJob(RepairJob.class);
            json.writeBooleanField("present", repairJob != null);
            writeClusterJob(json, "repair", repairJob);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    private void writeClusterJob(JsonGenerator json, String type, ClusterJob<CassandraTaskProtos.KeyspaceJobStatus> clusterJob) throws IOException {
        if (clusterJob != null) {
            json.writeObjectFieldStart(type + "_job");

            json.writeNumberField("started", clusterJob.getStartedTimestamp());
            if (clusterJob.getFinishedTimestamp() != null)
                json.writeNumberField("finished", clusterJob.getFinishedTimestamp());
            else
                json.writeNullField("finished");
            json.writeBooleanField("aborted", clusterJob.isAborted());

            json.writeArrayFieldStart("processed_nodes");
            for (Map.Entry<String, CassandraTaskProtos.KeyspaceJobStatus> ipToStatus : clusterJob.getProcessedNodes().entrySet()) {
                json.writeObjectFieldStart(ipToStatus.getKey());

                json.writeBooleanField("running", ipToStatus.getValue().getRunning());
                json.writeArrayFieldStart("remaining_keyspaces");
                for (String ks : ipToStatus.getValue().getRemainingKeyspacesList())
                    json.writeString(ks);
                json.writeEndArray();

                json.writeObjectFieldStart("processed_keyspaces");
                for (CassandraTaskProtos.JobKeyspaceStatus jobKeyspaceStatus : ipToStatus.getValue().getProcessedKeyspacesList()) {
                    json.writeObjectFieldStart(jobKeyspaceStatus.getKeyspace());
                    json.writeStringField("status", jobKeyspaceStatus.getStatus());
                    json.writeNumberField("duration_millis", jobKeyspaceStatus.getDuration());
                    json.writeEndObject();
                }
                json.writeEndObject();

                json.writeEndObject();
            }
            json.writeEndArray();

            json.writeStringField("current_node", clusterJob.getCurrentNodeIp());

            json.writeArrayFieldStart("remaining_nodes");
            for (String ip : clusterJob.getRemainingNodeIps())
                json.writeString(ip);
            json.writeEndArray();

            json.writeEndObject();
        } else
            json.writeNullField("repair_job");
    }
}
