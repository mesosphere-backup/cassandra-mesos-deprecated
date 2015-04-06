package io.mesosphere.mesos.frameworks.cassandra.scheduler.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.google.common.base.Optional;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraScheduler;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.api.StreamingJsonResponse;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.api.StreamingTextResponse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;

public final class JaxRsUtils {

    private JaxRsUtils() {}


    public static void writeSeedIps(@NotNull final CassandraCluster cluster, @NotNull JsonGenerator json) throws IOException {
        json.writeArrayFieldStart("seeds");
        for (String seed : cluster.getSeedNodeIps()) {
            json.writeString(seed);
        }
        json.writeEndArray();
    }

    public static void writeClusterJob(@NotNull final CassandraCluster cluster, @NotNull JsonGenerator json, @NotNull String name, @Nullable ClusterJobStatus jobStatus) throws IOException {
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
                writeNodeJobStatus(cluster, json, jobStatus.getCurrentNode());
            } else {
                json.writeNullField("currentNode");
            }

            json.writeArrayFieldStart("completedNodes");
            for (NodeJobStatus nodeJobStatus : jobStatus.getCompletedNodesList()) {
                json.writeStartObject();
                writeNodeJobStatus(cluster, json, nodeJobStatus);
            }
            json.writeEndArray();

            json.writeEndObject();
        } else {
            json.writeNullField(name);
        }
    }

    public static void writeNodeJobStatus(@NotNull final CassandraCluster cluster, @NotNull JsonGenerator json, @NotNull NodeJobStatus nodeJobStatus) throws IOException {
        json.writeStringField("executorId", nodeJobStatus.getExecutorId());
        json.writeStringField("taskId", nodeJobStatus.getTaskId());
        Optional<CassandraNode> node = cluster.cassandraNodeForExecutorId(nodeJobStatus.getExecutorId());
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
        for (ClusterJobKeyspaceStatus clusterJobKeyspaceStatus : nodeJobStatus.getProcessedKeyspacesList()) {
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

    @NotNull
    public static Response buildStreamingResponse(@NotNull final JsonFactory factory, @NotNull final StreamingJsonResponse jsonResponse) {
        return buildStreamingResponse(factory, Response.Status.OK, jsonResponse);
    }

    @NotNull
    public static Response buildStreamingResponse(@NotNull final JsonFactory factory, @NotNull Response.Status status, @NotNull final StreamingJsonResponse jsonResponse) {
        return Response.status(status).entity(new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try (JsonGenerator json = factory.createGenerator(output)) {
                    json.setPrettyPrinter(new DefaultPrettyPrinter());
                    json.writeStartObject();

                    jsonResponse.write(json);

                    json.writeEndObject();
                }
            }
        }).type("application/json").build();
    }

    @NotNull
    public static Response buildStreamingResponse(@NotNull Response.Status status, @NotNull String type, @NotNull final StreamingTextResponse textResponse) {
        return Response.status(status).entity(new StreamingOutput() {
            @Override
            public void write(OutputStream output) throws IOException, WebApplicationException {
                try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(output))) {
                    textResponse.write(printWriter);
                }
            }
        }).type(type).build();
    }

    public static void writeConfigRole(JsonGenerator json, CassandraConfigRole configRole) throws IOException {
        json.writeStringField("cassandraVersion", configRole.getCassandraVersion());
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
            for (TaskEnv.Entry entry : configRole.getTaskEnv().getVariablesList()) {
                json.writeStringField(entry.getName(), entry.getValue());
            }
            json.writeEndObject();
            json.writeObjectFieldStart("cassandraYaml");
            for (TaskConfig.Entry entry : configRole.getCassandraYamlConfig().getVariablesList()) {
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

    public static void writeTask(JsonGenerator json, CassandraNodeTask task) throws IOException {
        if (task != null && task.hasTaskId()) {
            json.writeObjectFieldStart(task.getType().toString());
            json.writeNumberField("cpuCores", task.getResources().getCpuCores());
            json.writeNumberField("diskMb", task.getResources().getDiskMb());
            json.writeNumberField("memMb", task.getResources().getMemMb());
            json.writeStringField("taskId", task.getTaskId());
            json.writeStringField("taskName", CassandraScheduler.getTaskName(task.getTaskName(), task.getTaskId()));
            json.writeEndObject();
        }
    }
}
