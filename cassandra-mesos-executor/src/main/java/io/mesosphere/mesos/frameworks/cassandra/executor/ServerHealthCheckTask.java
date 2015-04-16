package io.mesosphere.mesos.frameworks.cassandra.executor;

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.HealthCheckDetails;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.SlaveStatusDetails;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.SlaveStatusDetails.StatusDetailsType;
import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.JmxConnect;
import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.Nodetool;
import org.apache.mesos.ExecutorDriver;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.rmi.ConnectException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.mesosphere.mesos.util.ProtoUtils.protoToString;

public final class ServerHealthCheckTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerHealthCheckTask.class);

    @NotNull
    private final ExecutorDriver driver;
    @Nullable
    private final JmxConnect jmxConnect;

    public ServerHealthCheckTask(@NotNull final ExecutorDriver driver, @Nullable final JmxConnect jmxConnect) {
        this.driver = driver;
        this.jmxConnect = jmxConnect;
    }

    @Override
    public void run() {
        LOGGER.info("Performing health check of server task");
        final SlaveStatusDetails slaveStatusDetails = SlaveStatusDetails.newBuilder()
            .setStatusDetailsType(StatusDetailsType.HEALTH_CHECK_DETAILS)
            .setHealthCheckDetails(doHealthCheck())
            .build();
        LOGGER.info("Performed health check of server task. Result: {}", protoToString(slaveStatusDetails));
        driver.sendFrameworkMessage(slaveStatusDetails.toByteArray());
    }

    private HealthCheckDetails doHealthCheck() {
        final HealthCheckDetails.Builder builder = HealthCheckDetails.newBuilder();

        if (jmxConnect == null) {
            return builder.setHealthy(false)
                .setMsg("no JMX connect to Cassandra process")
                .build();
        }

        try {
            final CassandraFrameworkProtos.NodeInfo info = buildInfo();
            builder.setHealthy(true)
                .setInfo(info);
            LOGGER.info("Healthcheck succeeded: operationMode:{} joined:{} gossip:{} native:{} rpc:{} uptime:{}s endpoint:{}, dc:{}, rack:{}, hostId:{}, version:{}",
                info.getOperationMode(),
                info.getJoined(),
                info.getGossipRunning(),
                info.getNativeTransportRunning(),
                info.getRpcServerRunning(),
                info.getUptimeMillis() / 1000,
                info.getEndpoint(),
                info.getDataCenter(),
                info.getRack(),
                info.getHostId(),
                info.getVersion());
        } catch (final Exception e) {
            //noinspection ThrowableResultOfMethodCallIgnored
            final ConnectException connectException = findConnectException(e);
            if (connectException != null) {
                LOGGER.info("Health check failed: {}", connectException.getMessage().replaceAll("\\n\\t", ""));
            } else {
                LOGGER.warn("Health check failed due to unexpected exception.", e);
                builder.setHealthy(false)
                    .setMsg(e.toString());
            }
        }

        return builder.build();
    }

    @NotNull
    private CassandraFrameworkProtos.NodeInfo buildInfo() throws UnknownHostException {
        final Nodetool nodetool = new Nodetool(checkNotNull(jmxConnect));

        // C* should be considered healthy, if the information can be collected.
        // All flags can be manually set by any administrator and represent a valid state.

        final String operationMode = nodetool.getOperationMode();
        final boolean joined = nodetool.isJoined();
        final boolean gossipInitialized = nodetool.isGossipInitialized();
        final boolean gossipRunning = nodetool.isGossipRunning();
        final boolean nativeTransportRunning = nodetool.isNativeTransportRunning();
        final boolean rpcServerRunning = nodetool.isRPCServerRunning();

        final boolean valid = "NORMAL".equals(operationMode);

        LOGGER.info("Cassandra node status: operationMode={}, joined={}, gossipInitialized={}, gossipRunning={}, nativeTransportRunning={}, rpcServerRunning={}",
            operationMode, joined, gossipInitialized, gossipRunning, nativeTransportRunning, rpcServerRunning);

        final CassandraFrameworkProtos.NodeInfo.Builder builder = CassandraFrameworkProtos.NodeInfo.newBuilder()
            .setOperationMode(operationMode)
            .setJoined(joined)
            .setGossipInitialized(gossipInitialized)
            .setGossipRunning(gossipRunning)
            .setNativeTransportRunning(nativeTransportRunning)
            .setRpcServerRunning(rpcServerRunning)
            .setUptimeMillis(nodetool.getUptimeInMillis())
            .setVersion(nodetool.getVersion())
            .setClusterName(nodetool.getClusterName());

        if (valid) {
            final String endpoint = nodetool.getEndpoint();
            builder.setEndpoint(endpoint)
                .setTokenCount(nodetool.getTokenCount())
                .setDataCenter(nodetool.getDataCenter(endpoint))
                .setRack(nodetool.getRack(endpoint))
                .setHostId(nodetool.getHostID());
        }

        return builder.build();
    }

    @Nullable
    private static ConnectException findConnectException(@Nullable final Throwable t) {
        if (t == null) {
            return null;
        } else if (t instanceof ConnectException) {
            return (ConnectException) t;
        } else {
            return findConnectException(t.getCause());
        }
    }

}
