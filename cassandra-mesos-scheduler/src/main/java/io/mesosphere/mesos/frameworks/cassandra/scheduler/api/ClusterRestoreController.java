package io.mesosphere.mesos.frameworks.cassandra.scheduler.api;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.ClusterJobUtils;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.JaxRsUtils;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/cluster/restore")
@Produces("application/json")
public final class ClusterRestoreController {

    @NotNull
    private final CassandraCluster cluster;
    @NotNull
    private final JsonFactory factory;

    public ClusterRestoreController(@NotNull final CassandraCluster cluster, @NotNull final JsonFactory factory) {
        this.cluster = cluster;
        this.factory = factory;
    }

    /**
     * Starts a cluster-wide restore.
     *
     *     Example: <pre>{@code {
     * "started" : true
     * }}</pre>
     */
    @POST
    @Path("/start")
    public Response restoreStart(@QueryParam("name") String name) {
        if (name == null) {
            return JaxRsUtils.buildStreamingResponse(factory, Response.Status.BAD_REQUEST, new StreamingJsonResponse() {
                @Override
                public void write(final JsonGenerator json) throws IOException {
                    json.writeBooleanField("started", false);
                    json.writeStringField("error", "name is required");
                }
            });
        }

        return ClusterJobUtils.startJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.RESTORE, name);
    }

    /**
     * Aborts a cluster-wide restore after the current node has finished.
     *
     *     Example: <pre>{@code {
     * "aborted" : true
     * }}</pre>
     */
    @POST
    @Path("/abort")
    public Response restoreAbort() {
        return ClusterJobUtils.abortJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.RESTORE);
    }

    /**
     * Returns the status of the current restore.
     *
     *  Example: <pre>{@code {
     * "running" : true,
     * "restore" : {
     *     "type" : "RESTORE",
     *     "started" : 1426686829672,
     *     "finished" : null,
     *     "aborted" : false,
     *     "remainingNodes" : [ ],
     *     "currentNode" : {
     *         "executorId" : "cassandra.node.0.executor",
     *         "taskId" : "cassandra.node.0.executor.RESTORE",
     *         "hostname" : "127.0.0.2",
     *         "ip" : "127.0.0.2",
     *         "processedKeyspaces" : { },
     *         "remainingKeyspaces" : [ ]
     *     },
     *     "completedNodes" : [ {
     *         "executorId" : "cassandra.node.1.executor",
     *         "taskId" : "cassandra.node.1.executor.RESTORE",
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
    @Path("/status")
    public Response restoreStatus() {
        return ClusterJobUtils.jobStatus(cluster, factory, CassandraFrameworkProtos.ClusterJobType.RESTORE, "restore");
    }

    /**
     * Returns the status of the last restore.
     * See {@link #restoreStatus()} for an response example except that `running` field is exchanged with a field
     * called `present`.
     */
    @GET
    @Path("/last")
    public Response lastRestore() {
        return ClusterJobUtils.lastJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.RESTORE, "restore");
    }

}
