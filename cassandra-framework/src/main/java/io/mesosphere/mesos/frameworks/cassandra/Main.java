package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Optional;
import io.mesosphere.mesos.frameworks.cassandra.http.CassandraYamlHttpHandler;
import io.mesosphere.mesos.frameworks.cassandra.http.FileResourceHttpHandler;
import io.mesosphere.mesos.frameworks.cassandra.http.HttpServer;
import io.mesosphere.mesos.frameworks.cassandra.util.Env;
import io.mesosphere.mesos.util.ProtoUtils;
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
        final String executorFilePath = Env.getOrElse("EXECUTOR_FILE_PATH", workingDir("/cassandra-executor.jar"));
        final String jdkTarPath = Env.getOrElse("JDK_FILE_PATH", workingDir("/jdk.tar.gz"));
        final String cassandraTarPath = Env.getOrElse("CASSANDRA_FILE_PATH", workingDir("/cassandra.tar.gz"));

        final int executorCount = Integer.parseInt(Env.getOrElse("CASS_EXEC_COUNT", "3"));
        final int executorTaskCount = Integer.parseInt(Env.getOrElse("CASS_EXEC_TASK_COUNT", "4"));

        final String frameworkName = frameworkName(Env.option("CASSANDRA_CLUSTER_NAME"));
        final FrameworkInfo.Builder frameworkBuilder =
                FrameworkInfo.newBuilder()
                        .setFailoverTimeout(Period.days(7).getSeconds())
                        .setUser("") // Have Mesos fill in the current user.
                        .setName(frameworkName)
                        .setCheckpoint(true);


        final Config config = new Config();
        final String bindInterface = Env.getOrElse("BIND_INTERFACE", "0.0.0.0");
        final HttpServer httpServer = HttpServer.newBuilder()
                .withBindPort(port0)
                .withBindInterface(bindInterface)
                .withPathHandler("/cassandra.yaml", new CassandraYamlHttpHandler(config))
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
        final Scheduler scheduler = new CassandraScheduler(frameworkName, httpServer, executorCount, executorTaskCount);

        final String mesosMasterZkUrl = Env.getOrElse("MESOS_ZK", "zk://localhost:2181/mesos");
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
        if (Env.get("MESOS_AUTHENTICATE") != null) {
            LOGGER.info("Enabling authentication for the framework");

            final String principal = Env.get("DEFAULT_PRINCIPAL");
            final Optional<String> secret = Env.option("DEFAULT_SECRET");

            if (principal == null) {
                System.err.println("Expecting authentication principal in the environment");
                throw new SystemExitException(5);
            }

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
