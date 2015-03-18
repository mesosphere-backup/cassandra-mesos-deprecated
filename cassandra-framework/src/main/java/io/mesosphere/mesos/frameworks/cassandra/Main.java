/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Optional;
import io.mesosphere.mesos.frameworks.cassandra.files.FileResourceController;
import io.mesosphere.mesos.frameworks.cassandra.util.Env;
import io.mesosphere.mesos.util.Clock;
import io.mesosphere.mesos.util.ProtoUtils;
import io.mesosphere.mesos.util.SystemClock;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.state.State;
import org.apache.mesos.state.ZooKeeperState;
import org.glassfish.grizzly.http.server.HttpHandlerRegistration;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.http.server.ServerConfiguration;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpContainer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpContainerProvider;
import org.glassfish.jersey.grizzly2.httpserver.internal.LocalizationMessages;
import org.glassfish.jersey.server.ResourceConfig;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.ws.rs.ProcessingException;
import java.io.IOException;
import java.net.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.mesosphere.mesos.util.ProtoUtils.frameworkId;

public final class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    @Language("RegExp")
    private static final String userAndPass     = "[^/@]+";
    @Language("RegExp")
    private static final String hostAndPort     = "[A-z0-9-.]+(?::\\d+)?";
    @Language("RegExp")
    private static final String zkNode          = "[^/]+";
    @Language("RegExp")
    private static final String REGEX = "^zk://((?:" + userAndPass + "@)?(?:" + hostAndPort + "(?:," + hostAndPort + ")*))(/" + zkNode + "(?:/" + zkNode + ")*)$";
    private static final String validZkUrl = "zk://host1:port1,host2:port2,.../path";
    private static final Pattern zkURLPattern = Pattern.compile(REGEX);

    public static void main(final String[] args) {
        int status;
        try {
            final Handler[] handlers = LogManager.getLogManager().getLogger("").getHandlers();
            for (final Handler handler : handlers) {
                handler.setLevel(Level.OFF);
            }
            org.slf4j.LoggerFactory.getLogger("slf4j-logging").debug("Installing SLF4JLogging");
            SLF4JBridgeHandler.install();
            status = _main();
        } catch (SystemExitException e) {
            LOGGER.error(e.getMessage());
            status = e.status;
        } catch (UnknownHostException e) {
            LOGGER.error("Unable to resolve local interface for http server");
            status = 6;
        } catch (Throwable e) {
            LOGGER.error("Unhandled fatal exception", e);
            status = 10;
        }

        System.exit(status);
    }

    private static int _main() throws UnknownHostException {
        final Optional<String> portOption = Env.option("PORT0");
        if (!portOption.isPresent()) {
            throw new SystemExitException("Environment variable PORT0 must be defined", 5);
        }

        final int port0 = Integer.parseInt(portOption.get());

        final int       executorCount           = Integer.parseInt(     Env.option("CASSANDRA_NODE_COUNT").or("3"));
        final int       seedCount               = Integer.parseInt(     Env.option("CASSANDRA_SEED_COUNT").or("2"));
        final double    resourceCpuCores        = Double.parseDouble(   Env.option("CASSANDRA_RESOURCE_CPU_CORES").or("2.0"));
        final long      resourceMemoryMegabytes = Long.parseLong(       Env.option("CASSANDRA_RESOURCE_MEM_MB").or("2048"));
        final long      resourceDiskMegabytes   = Long.parseLong(       Env.option("CASSANDRA_RESOURCE_DISK_MB").or("2048"));
        final long      healthCheckIntervalSec  = Long.parseLong(       Env.option("CASSANDRA_HEALTH_CHECK_INTERVAL_SECONDS").or("60"));
        final long      bootstrapGraceTimeSec   = Long.parseLong(       Env.option("CASSANDRA_BOOTSTRAP_GRACE_TIME_SECONDS").or("120"));
        final String    cassandraVersion        =                       "2.1.2"; // TODO Env.option("CASSANDRA_VERSION").or("2.1.2");
        final String    frameworkName           = frameworkName(        Env.option("CASSANDRA_CLUSTER_NAME"));
        final String    zkUrl                   =                       Env.option("CASSANDRA_ZK").or("zk://localhost:2181/cassandra-mesos");
        final long      zkTimeoutMs             = Long.parseLong(       Env.option("CASSANDRA_ZK_TIMEOUT_MS").or("10000"));
        final String    mesosMasterZkUrl        =                       Env.option("MESOS_ZK").or("zk://localhost:2181/mesos");
        final long      failoverTimeout         = Long.parseLong(       Env.option("CASSANDRA_FAILOVER_TIMEOUT_SECONDS").or(String.valueOf(Period.days(7).toStandardSeconds().getSeconds())));
        final String    mesosRole               =                       Env.option("CASSANDRA_FRAMEWORK_MESOS_ROLE").or("*");

        final Matcher matcher = zkURLPattern.matcher(zkUrl);

        if (!matcher.matches()) {
            throw new SystemExitException(String.format("Invalid zk url format: '%s' expected '%s'", zkUrl, validZkUrl), 7);
        }

        final State state = new ZooKeeperState(
            matcher.group(1),
            zkTimeoutMs,
            TimeUnit.MILLISECONDS,
            matcher.group(2)
        );

        if (seedCount > executorCount || seedCount <= 0 || executorCount <= 0) {
            throw new IllegalArgumentException("number of nodes (" + executorCount + ") and/or number of seeds (" + seedCount + ") invalid");
        }

        CassandraFrameworkProtos.CassandraConfigRole.Builder defaultConfigRole = CassandraFrameworkProtos.CassandraConfigRole.newBuilder()
            .setCassandraVersion(cassandraVersion)
            .setCpuCores(resourceCpuCores)
            .setDiskMb(resourceDiskMegabytes)
            .setNumberOfNodes(executorCount)
            .setNumberOfSeeds(seedCount)
            .setMemMb(resourceMemoryMegabytes)
            .setMesosRole(mesosRole);
        final PersistedCassandraFrameworkConfiguration configuration = new PersistedCassandraFrameworkConfiguration(
            state,
            frameworkName,
            defaultConfigRole.build(),
            healthCheckIntervalSec,
            bootstrapGraceTimeSec
        );


        final FrameworkInfo.Builder frameworkBuilder =
            FrameworkInfo.newBuilder()
                .setFailoverTimeout(failoverTimeout)
                .setUser("") // Have Mesos fill in the current user.
                .setName(frameworkName)
                .setRole(mesosRole)
                .setCheckpoint(true);

        final Optional<String> frameworkId = configuration.frameworkId();
        if (frameworkId.isPresent()) {
            frameworkBuilder.setId(frameworkId(frameworkId.get()));
        }

        final URI httpServerBaseUri = URI.create(String.format("http://%s:%d/", formatInetAddress(InetAddress.getLocalHost()), port0));

        final Clock clock = new SystemClock();
        final CassandraCluster cassandraCluster = new CassandraCluster(
            clock,
            httpServerBaseUri.toString(),
            new ExecutorCounter(state, 0L),
            new PersistedCassandraClusterState(state, executorCount),
            new PersistedCassandraClusterHealthCheckHistory(state),
            new PersistedCassandraClusterJobs(state),
            configuration
        );
        final Scheduler scheduler = new CassandraScheduler(
            configuration,
            cassandraCluster
        );

        final ResourceConfig rc = new ResourceConfig()
            .register(new FileResourceController())
            .register(new ApiController(cassandraCluster));
        final HttpServer httpServer = createHttpServer(port0, rc);

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

        httpServer.shutdownNow();
        // Ensure that the driver process terminates.
        driver.stop();
        return status;
    }

    private static HttpServer createHttpServer(int port, ResourceConfig rc) {
        GrizzlyHttpContainer handler = new GrizzlyHttpContainerProvider().createContainer(GrizzlyHttpContainer.class, rc);

        String contextPath = "";

        TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance()
            .build();

        final NetworkListener listener = new NetworkListener("grizzly", false);
        listener.setTransport(transport);
        listener.setSecure(false);

        final HttpServer server = new HttpServer();
        server.addListener(listener);

        // Map the path to the processor.
        final ServerConfiguration config = server.getServerConfiguration();
        if (handler != null) {
            config.addHttpHandler(handler,
                HttpHandlerRegistration.bulder()
                    .contextPath(contextPath)
                    .build()
            );
        }

        config.setPassTraceRequest(true);

        try {
            // Not a clean hack, but this is a trick to bind the transport to any address (0.0.0.0)
            transport.bind(port);

            // Start the server.
            server.start();
        } catch (final IOException ex) {
            server.shutdownNow();
            throw new ProcessingException(LocalizationMessages.FAILED_TO_START_SERVER(ex.getMessage()), ex);
        }

        return server;
    }

    static String frameworkName(final Optional<String> clusterName) {
        if (clusterName.isPresent()) {
            return "cassandra." + clusterName.get();
        } else {
            return "cassandra";
        }
    }

    @NotNull
    private static String formatInetAddress(@NotNull final InetAddress inetAddress) {
        if (inetAddress instanceof Inet4Address) {
            final Inet4Address address = (Inet4Address) inetAddress;
            return address.getHostAddress();
        } else if (inetAddress instanceof Inet6Address) {
            final Inet6Address address = (Inet6Address) inetAddress;
            return String.format("[%s]", address.getHostAddress());
        } else {
            throw new IllegalArgumentException("InetAddress type: " + inetAddress.getClass().getName() + " is not supported");
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

    private static class SystemExitException extends RuntimeException {
        private final int status;

        public SystemExitException(final String message, final int status) {
            super(message);
            this.status = status;
        }
    }
}
