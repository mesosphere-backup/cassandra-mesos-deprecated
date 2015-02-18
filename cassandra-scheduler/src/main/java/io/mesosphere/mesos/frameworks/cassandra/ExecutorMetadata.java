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
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ExecutorMetadata {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorMetadata.class);

    private final Protos.ExecutorID executorId;
    private String hostname;
    private String ip;
    private volatile Instant lastHealthCheck;
    private volatile CassandraTaskProtos.SlaveMetadata slaveMetadata;
    private CassandraTaskProtos.CassandraNodeHealthCheckDetails lastHealthCheckDetails;

    public ExecutorMetadata(Protos.ExecutorID executorId) {
        this.executorId = executorId;
    }

    public CassandraTaskProtos.SlaveMetadata getSlaveMetadata() {
        return slaveMetadata;
    }

    public void setSlaveMetadata(CassandraTaskProtos.SlaveMetadata slaveMetadata) {
        this.slaveMetadata = slaveMetadata;
        if (slaveMetadata != null && slaveMetadata.hasIp())
            ip = slaveMetadata.getIp();
    }

    public Instant getLastHealthCheck() {
        return lastHealthCheck;
    }

    public CassandraTaskProtos.CassandraNodeHealthCheckDetails getLastHealthCheckDetails() {
        return lastHealthCheckDetails;
    }

    public void clear() {
        lastHealthCheck = null;
        slaveMetadata = null;
        ip = null;
        hostname = null;
    }

    public void updateHealthCheck(Instant now, CassandraTaskProtos.CassandraNodeHealthCheckDetails healthCheckDetails) {
        lastHealthCheck = now;
        this.lastHealthCheckDetails = healthCheckDetails;
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

    public void updateHostname(String hostname) {
        this.hostname = hostname;
        if (this.ip == null && hostname != null)
            try {
                InetAddress iadr = InetAddress.getByName(hostname);
                this.ip = iadr.getHostAddress();
            } catch (UnknownHostException e) {
                LOGGER.error("Failed to resolve host name '" + hostname + "' to IP.", e);
                throw new RuntimeException("Failed to resolve host name '" + hostname + '\'', e);
            }
    }

    @Override
    public String toString() {
        return "ExecutorMetadata{" +
                "executorId=" + executorId.getValue() +
                ", hostname='" + hostname + '\'' +
                ", ip='" + ip + '\'' +
                ", lastHealthCheck=" + lastHealthCheck +
                '}';
    }
}
