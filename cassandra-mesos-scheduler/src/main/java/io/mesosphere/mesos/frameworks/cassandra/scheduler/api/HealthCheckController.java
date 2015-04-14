package io.mesosphere.mesos.frameworks.cassandra.scheduler.api;

import io.mesosphere.mesos.frameworks.cassandra.scheduler.health.ClusterHealthReport;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.health.HealthReportService;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("/health")
public final class HealthCheckController {
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckController.class);

    @NotNull
    private final HealthReportService healthReportService;

    public HealthCheckController(@NotNull final HealthReportService healthReportService) {
        this.healthReportService = healthReportService;
    }

    @GET
    @Path("/process")
    public Response process() {
        return Response.ok("{\"healthy\":true}", "application/json").build();
    }

    @GET
    @Path("/cluster")
    public Response cluster() {
        final ClusterHealthReport report = healthReportService.generateClusterHealthReport();
        boolean healthy = report.isHealthy();
        LOGGER.info("Cluster Health Report Generated. result: healthy = {}", healthy);
        if (healthy) {
            return Response.ok("{\"healthy\":true}", "application/json").build();
        } else {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GET
    @Path("/cluster/report")
    @Produces("application/json")
    public ClusterHealthReport clusterHealthReport() {
        return healthReportService.generateClusterHealthReport();
    }

}
