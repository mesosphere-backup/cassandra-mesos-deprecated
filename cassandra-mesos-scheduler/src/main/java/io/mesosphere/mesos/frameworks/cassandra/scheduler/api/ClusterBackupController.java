package io.mesosphere.mesos.frameworks.cassandra.scheduler.api;

import com.fasterxml.jackson.core.JsonFactory;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.ClusterJobUtils;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;

@Path("/cluster/backup")
@Produces("application/json")
public final class ClusterBackupController {

    @NotNull
    private final CassandraCluster cluster;
    @NotNull
    private final JsonFactory factory;

    public ClusterBackupController(@NotNull final CassandraCluster cluster, @NotNull final JsonFactory factory) {
        this.cluster = cluster;
        this.factory = factory;
    }

    /**
     * Starts a cluster-wide backup.
     *
     *     Example: <pre>{@code {
     * "started" : true
     * }}</pre>
     */
    @POST
    @Path("/start")
    public Response backupStart(@QueryParam("name") String name) {
        if (name == null) {
            name = "backup-" + System.currentTimeMillis();
        }

        return ClusterJobUtils.startJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.BACKUP, name);
    }

    /**
     * Aborts a cluster-wide backup after the current node has finished.
     *
     *     Example: <pre>{@code {
     * "aborted" : true
     * }}</pre>
     */
    @POST
    @Path("/abort")
    public Response backupAbort() {
        return ClusterJobUtils.abortJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.BACKUP);
    }

    /**
     * Returns the status of the current backup.
     *
     *     Example: <pre>{@code {
     * "running" : true,
     * "backup" : {
     *     "type" : "BACKUP",
     *     "started" : 1426686829672,
     *     "finished" : null,
     *     "aborted" : false,
     *     "remainingNodes" : [ ],
     *     "currentNode" : {
     *         "executorId" : "cassandra.node.0.executor",
     *         "taskId" : "cassandra.node.0.executor.BACKUP",
     *         "hostname" : "127.0.0.2",
     *         "ip" : "127.0.0.2",
     *         "processedKeyspaces" : { },
     *         "remainingKeyspaces" : [ ]
     *     },
     *     "completedNodes" : [ {
     *         "executorId" : "cassandra.node.1.executor",
     *         "taskId" : "cassandra.node.1.executor.BACKUP",
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
    public Response backupStatus() {
        return ClusterJobUtils.jobStatus(cluster, factory, CassandraFrameworkProtos.ClusterJobType.BACKUP, "backup");
    }

    /**
     * Returns the status of the last backup.
     * See {@link #backupStatus()} for an response example except that `running` field is exchanged with a field
     * called `present`.
     */
    @GET
    @Path("/last")
    public Response lastBackup() {
        return ClusterJobUtils.lastJob(cluster, factory, CassandraFrameworkProtos.ClusterJobType.BACKUP, "backup");
    }

}
