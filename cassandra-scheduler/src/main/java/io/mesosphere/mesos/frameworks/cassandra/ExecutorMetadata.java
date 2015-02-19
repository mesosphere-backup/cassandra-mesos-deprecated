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

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicReference;

public class ExecutorMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorMetadata.class);

    private final Protos.ExecutorID executorId;

    private String hostname;
    private String ip;
    private int jmxPort;

    private volatile long lastHealthCheck;
    private volatile CassandraTaskProtos.CassandraNodeHealthCheckDetails lastHealthCheckDetails;

    private volatile long lastRepair;

    private Protos.TaskID mainTaskId;
    private Protos.ExecutorInfo executorInfo;

    private final AtomicReference<ExecutorStatus> status = new AtomicReference<>(ExecutorStatus.ROLLING_OUT);

    public ExecutorMetadata(Protos.ExecutorID executorId) {
        this.executorId = executorId;
    }

    public void executorLost() {
        // TODO do what ?
    }

    public void clear() {
        lastHealthCheck = 0L;
        ip = null;
        hostname = null;
    }

    public void repairDone(long now) {
        lastRepair = now;
    }

    public long getLastRepair() {
        return lastRepair;
    }

    public void updateHealthCheck(long now, CassandraTaskProtos.CassandraNodeHealthCheckDetails healthCheckDetails) {
        lastHealthCheck = now;
        this.lastHealthCheckDetails = healthCheckDetails;
    }

    public void updateHostname(String hostname, int jmxPort) {
        this.hostname = hostname;
        if (this.ip == null && hostname != null)
            try {
                InetAddress iadr = InetAddress.getByName(hostname);
                this.ip = iadr.getHostAddress();
            } catch (UnknownHostException e) {
                LOGGER.error("Failed to resolve host name '" + hostname + "' to IP.", e);
                throw new RuntimeException("Failed to resolve host name '" + hostname + '\'', e);
            }

        this.jmxPort = fixLocalhostJmxPort(jmxPort);
    }

    private int fixLocalhostJmxPort(int jmxPort) {
        // this is a hack for C* clusters running on localhost - i.e. development only !!

        // If we are running on cluster on the localhost, we must use a dynamically assigned port.
        try {
            if (InetAddress.getByName(ip).isLoopbackAddress())
                try (ServerSocket serverSocket = new ServerSocket(0)) {
                    jmxPort = serverSocket.getLocalPort();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
        } catch (UnknownHostException e) {
            // ignore that here...
        }

        return jmxPort;
    }

    public Protos.ExecutorID getExecutorId() {
        return executorId;
    }

    public String getHostname() {
        return hostname;
    }

    public String getIp() {
        return ip;
    }

    public int getJmxPort() {
        return jmxPort;
    }

    public long getLastHealthCheck() {
        return lastHealthCheck;
    }

    public CassandraTaskProtos.CassandraNodeHealthCheckDetails getLastHealthCheckDetails() {
        return lastHealthCheckDetails;
    }

    public Protos.ExecutorInfo getExecutorInfo() {
        return executorInfo;
    }

    public void setExecutorInfo(Protos.ExecutorInfo executorInfo) {
        this.executorInfo = executorInfo;
    }

    public Protos.TaskID getMainTaskId() {
        return mainTaskId;
    }

    public void setMainTaskId(Protos.TaskID mainTaskId) {
        this.mainTaskId = mainTaskId;
    }

    @Override
    public String toString() {
        return "ExecutorMetadata{" +
                "executorId=" + executorId.getValue() +
                ", hostname='" + hostname + '\'' +
                ", ip='" + ip + '\'' +
                ", jmxPort=" + jmxPort +
                ", lastHealthCheck=" + lastHealthCheck +
                '}';
    }

    public Protos.TaskID createTaskId(String suffix) {
        return Protos.TaskID.newBuilder()
                .setValue(mainTaskId.getValue() + suffix)
                .build();
    }

    public void rolledOut() {
        stateChange(ExecutorStatus.ROLLING_OUT, ExecutorStatus.ROLLED_OUT);
    }

    public void launched() {
        stateChange(ExecutorStatus.LAUNCHING, ExecutorStatus.LAUNCHED);
    }

    public void setRunning() {
        stateChange(ExecutorStatus.LAUNCHED, ExecutorStatus.RUNNING);
    }

    public boolean shouldTriggerLaunch() {
        if (status.get() != ExecutorStatus.ROLLED_OUT)
            return false;

        stateChange(ExecutorStatus.ROLLED_OUT, ExecutorStatus.LAUNCHING);
        return true;
    }

    public void notLaunched() {
        stateChange(ExecutorStatus.LAUNCHING, ExecutorStatus.ROLLED_OUT);
    }

    private boolean stateChange(ExecutorStatus cmp, ExecutorStatus next) {
        boolean done = status.compareAndSet(cmp, next);
        assert done :
                "Failed to change state for node " + hostname + " on executor " + executorId.getValue() + " from " + cmp + " to " + next + " - state=" +status;
        LOGGER.info("Executor {} on {}/{} state changed from {} to {}", executorId.getValue(), hostname, ip, cmp, next);
        return true;
    }

    public boolean isLaunched() {
        return status.get() == ExecutorStatus.LAUNCHED;
    }

    public boolean isRunning() {
        return status.get() == ExecutorStatus.RUNNING;
    }

    public ExecutorStatus getStatus() {
        return status.get();
    }

    public static enum ExecutorStatus {
        ROLLING_OUT,

        ROLLED_OUT,

        LAUNCHING,

        LAUNCHED,

        RUNNING
    }
}
