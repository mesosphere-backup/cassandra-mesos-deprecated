package io.mesosphere.mesos.frameworks.cassandra.scheduler.api;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.JaxRsUtils;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.PrintWriter;

@Path("/qa/report")
@Produces("application/json")
public final class QaReportController {

    @NotNull
    private final CassandraCluster cluster;
    @NotNull
    private final JsonFactory factory;

    public QaReportController(@NotNull CassandraCluster cluster, @NotNull final JsonFactory factory) {
        this.cluster = cluster;
        this.factory = factory;
    }

    @GET
    @Path("/resources")
    @Produces("text/plain")
    public Response qaReportResourcesText() {
        return JaxRsUtils.buildStreamingResponse(Response.Status.OK, "text/plain", new StreamingTextResponse() {
            @Override
            public void write(PrintWriter pw) {
                CassandraFrameworkProtos.CassandraClusterState clusterState = cluster.getClusterState().get();
                for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.getNodesList()) {

                    if (!cassandraNode.hasCassandraNodeExecutor()) {
                        continue;
                    }

                    pw.println("JMX_PORT: " + cassandraNode.getJmxConnect().getJmxPort());
                    pw.println("JMX_IP: " + cassandraNode.getJmxConnect().getIp());
                    pw.println("NODE_IP: " + cassandraNode.getIp());
                    pw.println("BASE: http://" + cassandraNode.getIp() + ":5051/");

                    for (String logFile : cluster.getNodeLogFiles(cassandraNode)) {
                        pw.println("LOG: " + logFile);
                    }

                }
            }
        });
    }

    @GET
    @Path("/resources")
    public Response qaReportResources() {
        return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {

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
                    json.writeStringField("jmxIp", cassandraNode.getJmxConnect().getIp());
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
            }
        });
    }

}
