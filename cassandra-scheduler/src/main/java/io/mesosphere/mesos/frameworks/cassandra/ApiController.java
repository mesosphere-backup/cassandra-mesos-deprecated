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

    @GET
    @Path("/")
    @Produces("text/html")
    public String indexPage() {
        return "<a href=\"status\">All nodes</a> <br/>" +
                "<a href=\"seed-nodes\">List of seed nodes</a> <br/>" +
                "<a href=\"repair/start\">Start repair</a> <br/>" +
                "<a href=\"repair/status\">Current repair status</a> <br/>" +
                "<a href=\"repair/last\">Last repair</a> <br/>" +
                "<a href=\"repair/abort\">Abort current repair</a> <br/>";
    }

    @GET
    @Path("/seed-nodes")
    @Produces("application/json")
    public Response seedNodes() {
        StringWriter sw = new StringWriter();
        try {
            CassandraCluster cluster = CassandraCluster.singleton();
            CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration();

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            json.writeNumberField("native_port", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_NATIVE));
            json.writeNumberField("rpc_port", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_RPC));

            writeSeedIps(json);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build seed list", e);
            return Response.serverError().build();
        }

        return Response.ok(sw.toString(), "application/json").build();
    }

    private static void writeSeedIps(JsonGenerator json) throws IOException {
        json.writeArrayFieldStart("seeds");
        for (String seed : CassandraCluster.singleton().getSeedNodes())
            json.writeString(seed);
        json.writeEndArray();
    }

    @GET
    @Path("/status")
    @Produces("application/json")
    public Response status() {
        StringWriter sw = new StringWriter();
        try {
            CassandraCluster cluster = CassandraCluster.singleton();

            // TODO don't write to StringWriter - stream to response as the nodes list might get very long

            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CassandraFrameworkProtos.CassandraClusterState clusterState = cluster.getClusterState();
            CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration();

            json.writeStringField("framework_name", configuration.getFrameworkName());
            json.writeStringField("framework_id", configuration.getFrameworkId());
// TODO          json.writeStringField("cluster_name", configuration.getFrameworkId());
            json.writeStringField("cassandra_version", configuration.getCassandraVersion());
            json.writeNumberField("target_node_count", configuration.getNumberOfNodes());
            json.writeNumberField("seed_node_count", configuration.getNumberOfSeeds());

// TODO            json.writeNumberField("seed_node_count", cluster.getSeedNodeCount());

            json.writeNumberField("native_port", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_NATIVE));
            json.writeNumberField("rpc_port", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_RPC));
            json.writeNumberField("storage_port", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_STORAGE));
            json.writeNumberField("ssl_storage_port", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_STORAGE_SSL));

// TODO           ClusterJob currentJob = cluster.currentClusterJob();
//            if (currentJob == null)
//                json.writeNullField("current_cluster_job");
//            else {
//                json.writeObjectFieldStart("current_cluster_job");
//                json.writeStringField("type", currentJob.getClass().getSimpleName());
//                json.writeEndObject();
//            }

            writeSeedIps(json);

            json.writeNumberField("health_check_interval_seconds", configuration.getHealthCheckIntervalSeconds());

            json.writeObjectFieldStart("node_bootstrap");
            json.writeNumberField("bootstrap_grace_time_seconds", configuration.getBootstrapGraceTimeSeconds());
// TODO            json.writeBooleanField("could_add_node", cluster.canAddNode());
//            json.writeNumberField("next_possible_node_launch", cluster.getNextNodeLaunchTime());
            json.writeEndObject();

            json.writeArrayFieldStart("nodes");
            for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.getNodesList()) {
                json.writeStartObject();

                writeTask(json, "server_task", cassandraNode.getServerTask());
                writeTask(json, "metadata_task", cassandraNode.getMetadataTask());
                json.writeBooleanField("has_node_executor", cassandraNode.hasCassandraNodeExecutor());
// TODO                cassandraNode.getDataVolumesList();

                json.writeStringField("executor_id", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                json.writeStringField("ip", cassandraNode.getIp());
                json.writeStringField("hostname", cassandraNode.getHostname());
                json.writeNumberField("jmx_port", cassandraNode.getJmxConnect().getJmxPort());
// TODO               json.writeStringField("status", executorMetadata.getStatus().name());

                CassandraFrameworkProtos.HealthCheckHistoryEntry lastHealthCheck = CassandraCluster.singleton().lastHealthCheck(cassandraNode.getCassandraNodeExecutor().getExecutorId());

                if (lastHealthCheck != null)
                    json.writeNumberField("last_health_check", lastHealthCheck.getTimestamp());
                else
                    json.writeNullField("last_health_check");

// TODO                long lr = executorMetadata.getLastRepair();
//                if (lr > 0)
//                    json.writeNumberField("last_repair", lr);
//                else
//                    json.writeNullField("last_repair");

                if (lastHealthCheck != null) {
                    json.writeObjectFieldStart("health_check_details");

                    CassandraFrameworkProtos.HealthCheckDetails hcd = lastHealthCheck.getDetails();

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

    private static void writeTask(JsonGenerator json, String name, CassandraFrameworkProtos.CassandraNodeTask task) throws IOException {
        if (task != null && task.hasTaskId()) {
            json.writeObjectFieldStart(name);
            json.writeStringField("type", task.getTaskDetails().getTaskType().toString());
            json.writeNumberField("cpu_cores", task.getCpuCores());
            json.writeNumberField("disk_mb", task.getDiskMb());
            json.writeNumberField("mem_mb", task.getMemMb());
            json.writeStringField("executor_id", task.getExecutorId());
            json.writeStringField("task_id", task.getTaskId());
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

            CassandraCluster cluster = CassandraCluster.singleton();
            json.writeNumberField("old_node_count", cluster.getConfiguration().getNumberOfNodes());
            json.writeNumberField("seed_node_count", cluster.getConfiguration().getNumberOfSeeds());
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


    // repair stuff
//
//    @GET
//    @Path("/repair/start")
//    @Produces("application/json")
//    public Response repairStart() {
//        StringWriter sw = new StringWriter();
//        try {
//            JsonFactory factory = new JsonFactory();
//            JsonGenerator json = factory.createGenerator(sw);
//            json.setPrettyPrinter(new DefaultPrettyPrinter());
//            json.writeStartObject();
//
//            boolean started = CassandraCluster.singleton().repairStart();
//            json.writeBooleanField("started", started);
//
//            json.writeEndObject();
//            json.close();
//        } catch (Exception e) {
//            LOGGER.error("Failed to build JSON response", e);
//            return Response.serverError().build();
//        }
//        return Response.ok(sw.toString(), "application/json").build();
//    }
//
//    @GET
//    @Path("/repair/abort")
//    @Produces("application/json")
//    public Response repairAbort() {
//        StringWriter sw = new StringWriter();
//        try {
//            JsonFactory factory = new JsonFactory();
//            JsonGenerator json = factory.createGenerator(sw);
//            json.setPrettyPrinter(new DefaultPrettyPrinter());
//            json.writeStartObject();
//
//            boolean aborted = CassandraCluster.singleton().repairAbort();
//            json.writeBooleanField("aborted", aborted);
//
//            json.writeEndObject();
//            json.close();
//        } catch (Exception e) {
//            LOGGER.error("Failed to build JSON response", e);
//            return Response.serverError().build();
//        }
//        return Response.ok(sw.toString(), "application/json").build();
//    }
//
//    @GET
//    @Path("/repair/status")
//    @Produces("application/json")
//    public Response repairStatus() {
//        StringWriter sw = new StringWriter();
//        try {
//
//            // TODO don't write to StringWriter - stream to response as the nodes list might get very long
//
//            JsonFactory factory = new JsonFactory();
//            JsonGenerator json = factory.createGenerator(sw);
//            json.setPrettyPrinter(new DefaultPrettyPrinter());
//            json.writeStartObject();
//
//            RepairJob repairJob = CassandraCluster.singleton().getCurrentRepair();
//            json.writeBooleanField("running", repairJob != null);
//            writeRepairJob(json, repairJob);
//
//            json.writeEndObject();
//            json.close();
//        } catch (Exception e) {
//            LOGGER.error("Failed to build JSON response", e);
//            return Response.serverError().build();
//        }
//        return Response.ok(sw.toString(), "application/json").build();
//    }
//
//    @GET
//    @Path("/repair/last")
//    @Produces("application/json")
//    public Response lastRepair() {
//        StringWriter sw = new StringWriter();
//        try {
//
//            // TODO don't write to StringWriter - stream to response as the nodes list might get very long
//
//            JsonFactory factory = new JsonFactory();
//            JsonGenerator json = factory.createGenerator(sw);
//            json.setPrettyPrinter(new DefaultPrettyPrinter());
//            json.writeStartObject();
//
//            RepairJob repairJob = CassandraCluster.singleton().getLastRepair();
//            json.writeBooleanField("present", repairJob != null);
//            writeRepairJob(json, repairJob);
//
//            json.writeEndObject();
//            json.close();
//        } catch (Exception e) {
//            LOGGER.error("Failed to build JSON response", e);
//            return Response.serverError().build();
//        }
//        return Response.ok(sw.toString(), "application/json").build();
//    }
//
//    private void writeRepairJob(JsonGenerator json, RepairJob repairJob) throws IOException {
//        if (repairJob != null) {
//            json.writeObjectFieldStart("repair_job");
//
//            json.writeNumberField("started", repairJob.getStartedTimestamp());
//            if (repairJob.getFinishedTimestamp() != null)
//                json.writeNumberField("finished", repairJob.getFinishedTimestamp());
//            else
//                json.writeNullField("finished");
//            json.writeBooleanField("aborted", repairJob.isAborted());
//
//            json.writeArrayFieldStart("repaired_nodes");
//            for (Map.Entry<String, CassandraTaskProtos.CassandraNodeRepairStatus> ipToStatus : repairJob.getRepairedNodes().entrySet()) {
//                json.writeObjectFieldStart(ipToStatus.getKey());
//
//                json.writeBooleanField("running", ipToStatus.getValue().getRunning());
//                json.writeArrayFieldStart("remaining_keyspaces");
//                for (String ks : ipToStatus.getValue().getRemainingKeyspacesList())
//                    json.writeString(ks);
//                json.writeEndArray();
//
//                json.writeObjectFieldStart("repaired_keyspaces");
//                for (CassandraTaskProtos.KeyspaceRepairStatus keyspaceRepairStatus : ipToStatus.getValue().getRepairedKeyspacesList()) {
//                    json.writeObjectFieldStart(keyspaceRepairStatus.getKeyspace());
//                    json.writeStringField("status", keyspaceRepairStatus.getStatus());
//                    json.writeNumberField("duration_millis", keyspaceRepairStatus.getDuration());
//                    json.writeEndObject();
//                }
//                json.writeEndObject();
//
//                json.writeEndObject();
//            }
//            json.writeEndArray();
//
//            json.writeStringField("current_node", repairJob.getCurrentNodeIp());
//
//            json.writeArrayFieldStart("remaining_nodes");
//            for (String ip : repairJob.getRemainingNodeIps())
//                json.writeString(ip);
//            json.writeEndArray();
//
//            json.writeEndObject();
//        } else
//            json.writeNullField("repair_job");
//    }
}
