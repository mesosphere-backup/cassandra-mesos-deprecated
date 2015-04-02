/**
 *    Copyright (C) 2015 Mesosphere, Inc.
 *
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
package io.mesosphere.mesos.frameworks.cassandra.executor.jmx;

import java.net.UnknownHostException;
import java.util.Map;

public class Nodetool {
    private final JmxConnect jmxConnect;

    public Nodetool(JmxConnect jmxConnect) {
        this.jmxConnect = jmxConnect;
    }

    public boolean isNativeTransportRunning() {
        return jmxConnect.getStorageServiceProxy().isNativeTransportRunning();
    }

    public boolean isRPCServerRunning() {
        return jmxConnect.getStorageServiceProxy().isRPCServerRunning();
    }

    /**
     * Whether the node has joined the ring.
     */
    public boolean isJoined() {
        return jmxConnect.getStorageServiceProxy().isJoined();
    }

    /**
     * to determine if gossip is disabled
     */
    public boolean isGossipInitialized() {
        return jmxConnect.getStorageServiceProxy().isInitialized();
    }

    /**
     * whether gossip is running or not
     */
    public boolean isGossipRunning() {
        return jmxConnect.getStorageServiceProxy().isGossipRunning();
    }

    /**
     * Get the operational mode.
     * Valid operation modes are:
     * <ul>
     *     <li>STARTING</li>
     *     <li>NORMAL</li>
     *     <li>JOINING</li>
     *     <li>LEAVING</li>
     *     <li>DECOMMISSIONED</li>
     *     <li>MOVING</li>
     *     <li>DRAINING</li>
     *     <li>DRAINED</li>
     * </ul>
     */
    public String getOperationMode() {
        return jmxConnect.getStorageServiceProxy().getOperationMode();
    }

    public long getUptimeInMillis() {
        return jmxConnect.getRuntimeProxy().getUptime();
    }

    public String getHostID() {
        return jmxConnect.getStorageServiceProxy().getLocalHostId();
    }

    public int getTokenCount() {
        return jmxConnect.getStorageServiceProxy().getTokens().size();
    }

    public String getVersion() {
        return jmxConnect.getStorageServiceProxy().getReleaseVersion();
    }

    public String getClusterName() {
        return jmxConnect.getStorageServiceProxy().getClusterName();
    }

    public String getDataCenter(String endpoint) throws UnknownHostException {
        return jmxConnect.getEndpointSnitchInfoProxy().getDatacenter(endpoint);
    }

    public String getRack(String endpoint) throws UnknownHostException {
        return jmxConnect.getEndpointSnitchInfoProxy().getRack(endpoint);
    }

    public String getEndpoint() {
        // Try to find the endpoint using the local token, doing so in a crazy manner
        // to maintain backwards compatibility with the MBean interface
        String stringToken = jmxConnect.getStorageServiceProxy().getTokens().get(0);
        Map<String, String> tokenToEndpoint = jmxConnect.getStorageServiceProxy().getTokenToEndpointMap();

        for (Map.Entry<String, String> pair : tokenToEndpoint.entrySet()) {
            if (pair.getKey().equals(stringToken)) {
                return pair.getValue();
            }
        }

        throw new InconsistentNodeException("Could not find myself in the endpoint list, something is very wrong!  Is the Cassandra node fully started?");
    }
}
