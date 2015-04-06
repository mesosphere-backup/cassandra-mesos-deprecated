package io.mesosphere.mesos.frameworks.cassandra.scheduler.api;

import com.fasterxml.jackson.core.JsonFactory;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.ClusterJobUtils;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("/cluster/repair")
@Produces("application/json")
public final class ClusterRepairController {

    @NotNull
    private final CassandraCluster cluster;
    @NotNull
    private final JsonFactory factory;

    public ClusterRepairController(@NotNull final CassandraCluster cluster, @NotNull final JsonFactory factory) {
        this.cluster = cluster;
        this.factory = factory;
    }

    /**
     * Starts a cluster-wide repair.
     *
     *     Example: <pre>{@code {
     * "started" : true
     * }}</pre>
     */
    @POST
    @Path("/start")
    public Response repairStart() {
        return ClusterJobUtils.startJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.REPAIR);
    }

    /**
     * Aborts a cluster-wide repair after the current node has finished.
     *
     *     Example: <pre>{@code {
     * "aborted" : true
     * }}</pre>
     */
    @POST
    @Path("/abort")
    public Response repairAbort() {
        return ClusterJobUtils.abortJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.REPAIR);
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
    @Path("/status")
    public Response repairStatus() {
        return ClusterJobUtils.jobStatus(cluster, factory, CassandraFrameworkProtos.ClusterJobType.REPAIR, "repair");
    }

    /**
     * Returns the status of the last repair.
     * See {@link #repairStatus()} for an response example except that `running` field is exchanged with a field
     * called `present`.
     */
    @GET
    @Path("/last")
    public Response lastRepair() {
        return ClusterJobUtils.lastJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.REPAIR, "repair");
    }

}
