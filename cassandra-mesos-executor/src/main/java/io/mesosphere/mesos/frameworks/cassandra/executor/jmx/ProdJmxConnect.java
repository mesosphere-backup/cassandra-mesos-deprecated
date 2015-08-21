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

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JMX interface class to Cassandra node.
 * The JMX via RMI connection is lazily created when the first call requires it.
 *
 * @author Robert Stupp
 */
public class ProdJmxConnect implements JmxConnect {
    private static final String DEFATULT_CASSANDRA_JMX_HOST = "127.0.0.1";
    private static final int DEFAULT_CASSANDRA_JMX_PORT = 7199;

    private static final String STORAGE_SERVICE_NAME = "org.apache.cassandra.db:type=StorageService";
    private static final String ENDPOINT_SNITCH_INFO_NAME = "org.apache.cassandra.db:type=EndpointSnitchInfo";

    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";

    @NotNull
    private final String host;
    private final int port;
    @Nullable
    private final String username;
    @Nullable
    private final String password;

    private JMXConnector jmxc;
    private MBeanServerConnection mbeanServerConn;

    private StorageServiceMBean ssProxy;
    private EndpointSnitchInfoMBean snitchProxy;
    private RuntimeMXBean runtimeProxy;

    @NotNull
    private final ReentrantLock lock = new ReentrantLock();

    public ProdJmxConnect(@Nullable String host, int port, @Nullable final String username, @Nullable final String password) {
        if (host == null || host.trim().isEmpty())
            host = DEFATULT_CASSANDRA_JMX_HOST;
        if (port <= 0)
            port = DEFAULT_CASSANDRA_JMX_PORT;

        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public ProdJmxConnect(@Nullable final String host, final int port) {
        this(host, port, null, null);
    }

    public ProdJmxConnect(@NotNull final CassandraFrameworkProtos.JmxConnect jmxInfo) {
        this(jmxInfo.getIp(), jmxInfo.getJmxPort());
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (jmxc == null) {
                return;
            }

            jmxc.close();
        } finally {
            lock.unlock();

            jmxc = null;
            mbeanServerConn = null;
            ssProxy = null;
            snitchProxy = null;
            runtimeProxy = null;
        }
    }

    private void connect() throws IOException {
        lock.lock();
        try {
            if (jmxc != null) {
                return;
            }

            final JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
            final Map<String, Object> env = new HashMap<>();
            if (username != null) {
                final String[] creds = {username, password};
                env.put(JMXConnector.CREDENTIALS, creds);
            }
            jmxc = JMXConnectorFactory.connect(jmxUrl, env);
            mbeanServerConn = jmxc.getMBeanServerConnection();
        } finally {
            lock.unlock();
        }
    }

    @NotNull
    private <T> T newProxy(final String name, final Class<T> type) {
        lock.lock();
        try {
            connect();
            return JMX.newMBeanProxy(mbeanServerConn, new ObjectName(name), type);
        } catch (final Exception e) {
            throw new JmxRuntimeException("Failed to create proxy for " + name, e);
        } finally {
            lock.unlock();
        }
    }

    @NotNull
    private <T> T newPlatformProxy(final String name, final Class<T> type) {
        lock.lock();
        try {
            connect();
            return ManagementFactory.newPlatformMXBeanProxy(mbeanServerConn, name, type);
        } catch (final Exception e) {
            throw new JmxRuntimeException("Failed to create proxy for " + name, e);
        } finally {
            lock.unlock();
        }
    }

    @NotNull
    public RuntimeMXBean getRuntimeProxy() {
        if (runtimeProxy == null) {
            runtimeProxy = newPlatformProxy(ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
        }
        return runtimeProxy;
    }

    @NotNull
    public StorageServiceMBean getStorageServiceProxy() {
        if (ssProxy == null)
            ssProxy = newProxy(STORAGE_SERVICE_NAME, StorageServiceMBean.class);
        return ssProxy;
    }

    @NotNull
    public EndpointSnitchInfoMBean getEndpointSnitchInfoProxy() {
        if (snitchProxy == null) {
            snitchProxy = newProxy(ENDPOINT_SNITCH_INFO_NAME, EndpointSnitchInfoMBean.class);
        }
        return snitchProxy;
    }

    @NotNull
    public ColumnFamilyStoreMBean getColumnFamilyStoreProxy(@NotNull final String keyspace, @NotNull final String table) {
        final String beanName = "org.apache.cassandra.db:type=ColumnFamilies,keyspace=" + keyspace  + ",columnfamily=" + table;
        return newProxy(beanName, ColumnFamilyStoreMBean.class);
    }

    @NotNull
    public List<String> getColumnFamilyNames(@NotNull final String keyspace) {
        try {
            final ObjectName query = new ObjectName("org.apache.cassandra.db:type=ColumnFamilies,keyspace=" + keyspace + ",*");
            final Set<ObjectName> cfObjects = mbeanServerConn.queryNames(query, null);
            final List<String> r = new ArrayList<>();
            for(final ObjectName n : cfObjects)
            {
                final String cfName = n.getKeyProperty("columnfamily");
                r.add(cfName);
            }
            return r;
        } catch (final Exception e) {
            throw new JmxRuntimeException("Failed to get column family names for keyspace " + keyspace, e);
        }
    }
}
