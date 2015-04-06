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

@Path("/cluster/rolling-restart")
@Produces("application/json")
public final class ClusterRollingRestartController {

    @NotNull
    private final CassandraCluster cluster;
    @NotNull
    private final JsonFactory factory;

    public ClusterRollingRestartController(@NotNull final CassandraCluster cluster, @NotNull final JsonFactory factory) {
        this.cluster = cluster;
        this.factory = factory;
    }

    /**
     * Starts a cluster-restart / restart of all cassandra server processes.
     *
     *     Example: <pre>{@code {
     * "started" : true
     * }}</pre>
     */
    @POST
    @Path("/start")
    public Response clusterRestartStart() {
        return ClusterJobUtils.startJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.RESTART);
    }

    /**
     * Aborts a cluster-restart / restart of all cassandra server processes after the current node has finished.
     *
     *     Example: <pre>{@code {
     * "aborted" : true
     * }}</pre>
     */
    @POST
    @Path("/abort")
    public Response clusterRestartAbort() {
        return ClusterJobUtils.abortJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.RESTART);
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
    @Path("/status")
    public Response clusterRestartStatus() {
        return ClusterJobUtils.jobStatus(cluster, factory, CassandraFrameworkProtos.ClusterJobType.RESTART, "clusterRestart");
    }

    /**
     * Returns the status of the last cluster-restart / restart of all cassandra server processes.
     * See {@link #clusterRestartStatus()} for an response example except that `running` field is exchanged with a field
     * called `present`.
     */
    @GET
    @Path("/last")
    public Response lastClusterRestart() {
        return ClusterJobUtils.lastJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.RESTART, "clusterRestart");
    }

}
