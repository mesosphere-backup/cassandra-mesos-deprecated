package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Optional;
import io.mesosphere.mesos.frameworks.cassandra.http.FileResourceHttpHandler;
import io.mesosphere.mesos.frameworks.cassandra.http.HttpServer;
import io.mesosphere.mesos.frameworks.cassandra.util.Env;
import io.mesosphere.mesos.util.ProtoUtils;
import io.mesosphere.mesos.util.SystemClock;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Scheduler;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(final String[] args) {
        final Optional<String> portOption = Env.option("PORT0");
        if (!portOption.isPresent()) {
            LOGGER.error("PORT0 must be defined");
            throw new SystemExitException(5);
        }

        final int port0 = Integer.parseInt(portOption.get());
        final String executorFilePath = Env.option("EXECUTOR_FILE_PATH").or(workingDir("/cassandra-executor.jar"));
        final String jdkTarPath = Env.option("JDK_FILE_PATH").or(workingDir("/jdk.tar.gz"));
        final String cassandraTarPath = Env.option("CASSANDRA_FILE_PATH").or(workingDir("/cassandra.tar.gz"));

        final int       executorCount           = Integer.parseInt(     Env.option("CASSANDRA_NODE_COUNT").or("3"));
        final double    resourceCpuCores        = Double.parseDouble(   Env.option("CASSANDRA_RESOURCE_CPU_CORES").or("2.0"));
        final long      resourceMemoryMegabytes = Long.parseLong(       Env.option("CASSANDRA_RESOURCE_MEM_MB").or("2048"));
        final long      resourceDiskMegabytes   = Long.parseLong(       Env.option("CASSANDRA_RESOURCE_DISK_MB").or("2048"));
        final long      healthCheckIntervalSec  = Long.parseLong(       Env.option("CASSANDRA_HEALTH_CHECK_INTERVAL_SECONDS").or("60"));

        final String frameworkName = frameworkName(Env.option("CASSANDRA_CLUSTER_NAME"));
        final FrameworkInfo.Builder frameworkBuilder =
                FrameworkInfo.newBuilder()
                        .setFailoverTimeout(Period.days(7).getSeconds())
                        .setUser("") // Have Mesos fill in the current user.
                        .setName(frameworkName)
                        .setCheckpoint(true);


        final String bindInterface = Env.option("BIND_INTERFACE").or("0.0.0.0");
        final HttpServer httpServer = HttpServer.newBuilder()
                .withBindPort(port0)
                .withBindInterface(bindInterface)
                .withPathHandler(
                        "/cassandra-executor.jar",
                        new FileResourceHttpHandler(
                                "cassandra-executor.jar",
                                "application/java-archive",
                                executorFilePath
                        )
                )
                .withPathHandler(
                        "/jdk.tar.gz",
                        new FileResourceHttpHandler(
                                "jdk.tar.gz",
                                "application/x-gzip",
                                jdkTarPath
                        )
                )
                .withPathHandler(
                        "/cassandra.tar.gz",
                        new FileResourceHttpHandler(
                                "cassandra.tar.gz",
                                "application/x-gzip",
                                cassandraTarPath
                        )
                )
                .build();
        httpServer.start();
        LOGGER.info("Started http server on {}", httpServer.getBoundAddress());
        final Scheduler scheduler = new CassandraScheduler(
            new SystemClock(),
            frameworkName,
            httpServer,
            executorCount,
            resourceCpuCores,
            resourceMemoryMegabytes,
            resourceDiskMegabytes,
            healthCheckIntervalSec
        );

        final String mesosMasterZkUrl = Env.option("MESOS_ZK").or("zk://localhost:2181/mesos");
        final MesosSchedulerDriver driver;
        final Optional<Credential> credentials = getCredential();
        if (credentials.isPresent()) {
            frameworkBuilder.setPrincipal(credentials.get().getPrincipal());
            driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), mesosMasterZkUrl, credentials.get());
        } else {
            frameworkBuilder.setPrincipal("cassandra-framework");
            driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), mesosMasterZkUrl);
        }

        final int status;
        switch (driver.run()) {
            case DRIVER_STOPPED:
                status = 0;
                break;
            case DRIVER_ABORTED:
                status = 1;
                break;
            case DRIVER_NOT_STARTED:
                status = 2;
                break;
            default:
                status = 3;
                break;
        }

        // Ensure that the driver process terminates.
        driver.stop();
        try {
            httpServer.close();
        } catch (final IOException ignored) {}

        System.exit(status);
    }

    private static String workingDir(final String defaultFileName) {
        return System.getProperty("user.dir") + defaultFileName;
    }

    static String frameworkName(final Optional<String> clusterName) {
        if (clusterName.isPresent()) {
            return "cassandra." + clusterName.get();
        } else {
            return "cassandra";
        }
    }

    static Optional<Credential> getCredential() {
        final boolean auth = Boolean.valueOf(Env.option("MESOS_AUTHENTICATE").or("false"));
        if (auth){
            LOGGER.info("Enabling authentication for the framework");

            final String principal = Env.get("DEFAULT_PRINCIPAL");
            final Optional<String> secret = Env.option("DEFAULT_SECRET");

            return Optional.of(ProtoUtils.getCredential(principal, secret));
        } else {
            return Optional.absent();
        }
    }

    @Value
    @EqualsAndHashCode(callSuper = false)
    private static class SystemExitException extends RuntimeException {
        private final int status;
    }
}
