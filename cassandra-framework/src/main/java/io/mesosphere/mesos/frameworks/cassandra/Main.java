package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import io.mesosphere.mesos.frameworks.cassandra.util.Env;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Scheduler;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(final String[] args) {

        final FrameworkInfo.Builder frameworkBuilder =
                FrameworkInfo.newBuilder()
                        .setFailoverTimeout(Period.days(7).getSeconds())
                        .setUser("") // Have Mesos fill in the current user.
                        .setName(frameworkName(Env.option("CASSANDRA_CLUSTER_NAME")))
                        .setCheckpoint(true);

        final Scheduler scheduler = new CassandraScheduler();

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

        System.exit(status);
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
                throw new SystemExitException(1);
            }

            return Optional.of(getCredential(principal, secret));
        } else {
            return Optional.absent();
        }
    }

    @NotNull
    static Credential getCredential(@NotNull final String principal, @NotNull final Optional<String> secret) {
        if (secret.isPresent()) {
            return Credential.newBuilder()
                    .setPrincipal(principal)
                    .setSecret(ByteString.copyFrom(secret.get().getBytes()))
                    .build();
        } else {
            return Credential.newBuilder()
                    .setPrincipal(principal)
                    .build();
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = false)
    private static class SystemExitException extends RuntimeException {
        private final int status;
    }
}
