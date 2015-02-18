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
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
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
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            json.writeNumberField("native_port", CassandraCluster.instance().getNativePort());
            json.writeNumberField("rpc_port", CassandraCluster.instance().getRpcPort());
            json.writeNumberField("jmx_port", CassandraCluster.instance().getJmxPort());

            json.writeArrayFieldStart("seeds");
            for (String seed : CassandraCluster.instance().seedsIpList())
                json.writeString(seed);
            json.writeEndArray();

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build seed list", e);
            return Response.serverError().build();
        }

        return Response.ok(sw.toString(), "application/json").build();
    }

    @GET
    @Path("/all-nodes")
    @Produces("application/json")
    public Response allNodes() {
        StringWriter sw = new StringWriter();
        try {
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            json.writeStringField("cluster_name", CassandraCluster.instance().getName());
            json.writeNumberField("native_port", CassandraCluster.instance().getNativePort());
            json.writeNumberField("rpc_port", CassandraCluster.instance().getRpcPort());
            json.writeNumberField("jmx_port", CassandraCluster.instance().getJmxPort());
            json.writeNumberField("storage_port", CassandraCluster.instance().getStoragePort());
            json.writeNumberField("ssl_storage_port", CassandraCluster.instance().getSslStoragePort());

            json.writeArrayFieldStart("nodes");
            for (ExecutorMetadata executorMetadata : CassandraCluster.instance().allNodes()) {
                json.writeStartObject();

                json.writeStringField("executor_id", executorMetadata.getExecutorId().getValue());
                json.writeStringField("ip", executorMetadata.getIp());
                json.writeStringField("hostname", executorMetadata.getHostname());

                Instant lhc = executorMetadata.getLastHealthCheck();
                if (lhc != null)
                    json.writeNumberField("last_health_check", lhc.getMillis());
                else
                    json.writeNullField("last_health_check");

                CassandraTaskProtos.CassandraNodeHealthCheckDetails hcd = executorMetadata.getLastHealthCheckDetails();
                if (hcd != null) {
                    json.writeObjectFieldStart("health_check_details");

                    hcd.getHealthy();
                    hcd.getMsg();

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
                    json.writeNumberField("uptime_millis", hcd.getInfo().getUptimeMillis());
                    json.writeStringField("version", hcd.getInfo().getVersion());

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

            boolean started = CassandraCluster.instance().repairStart();
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

            boolean aborted = CassandraCluster.instance().repairAbort();
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
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CassandraCluster.RepairJob repairJob = CassandraCluster.instance().getCurrentRepair();
            json.writeBooleanField("running", repairJob != null);
            writeRepairJob(json, repairJob);

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
            JsonFactory factory = new JsonFactory();
            JsonGenerator json = factory.createGenerator(sw);
            json.setPrettyPrinter(new DefaultPrettyPrinter());
            json.writeStartObject();

            CassandraCluster.RepairJob repairJob = CassandraCluster.instance().getLastRepair();
            json.writeBooleanField("present", repairJob != null);
            writeRepairJob(json, repairJob);

            json.writeEndObject();
            json.close();
        } catch (Exception e) {
            LOGGER.error("Failed to build JSON response", e);
            return Response.serverError().build();
        }
        return Response.ok(sw.toString(), "application/json").build();
    }

    private void writeRepairJob(JsonGenerator json, CassandraCluster.RepairJob repairJob) throws IOException {
        if (repairJob != null) {
            json.writeObjectFieldStart("repair_job");

            json.writeNumberField("started", repairJob.getStartedTimestamp());
            if (repairJob.getFinishedTimestamp() != null)
                json.writeNumberField("finished", repairJob.getFinishedTimestamp());
            else
                json.writeNullField("finished");
            json.writeBooleanField("aborted", repairJob.isAborted());

            json.writeArrayFieldStart("repaired_nodes");
            for (Map.Entry<String, CassandraTaskProtos.CassandraNodeRepairStatus> ipToStatus : repairJob.getRepairedNodes().entrySet()) {
                json.writeObjectFieldStart(ipToStatus.getKey());

                json.writeBooleanField("running", ipToStatus.getValue().getRunning());
                json.writeArrayFieldStart("remaining_keyspaces");
                for (String ks : ipToStatus.getValue().getRemainingKeyspacesList())
                    json.writeString(ks);
                json.writeEndArray();

                json.writeObjectFieldStart("repaired_keyspaces");
                for (CassandraTaskProtos.KeyspaceRepairStatus keyspaceRepairStatus : ipToStatus.getValue().getRepairedKeyspacesList()) {
                    json.writeObjectFieldStart(keyspaceRepairStatus.getKeyspace());
                    json.writeStringField("status", keyspaceRepairStatus.getStatus());
                    json.writeNumberField("duration_millis", keyspaceRepairStatus.getDuration());
                    json.writeEndObject();
                }
                json.writeEndObject();

                json.writeEndObject();
            }
            json.writeEndArray();

            json.writeStringField("current_node", repairJob.getCurrentNodeIp());

            json.writeArrayFieldStart("remaining_nodes");
            for (String ip : repairJob.getRemainingNodeIps())
                json.writeString(ip);
            json.writeEndArray();

            json.writeEndObject();
        } else
            json.writeNullField("repair_job");
    }
}
