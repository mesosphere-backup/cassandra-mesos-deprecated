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
package io.mesosphere.mesos.frameworks.cassandra.jmx;

import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.HintedHandOffManagerMBean;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.gms.GossiperMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingServiceMBean;
import org.apache.cassandra.service.*;
import org.apache.cassandra.streaming.StreamManagerMBean;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JMX interface class to Cassandra node.
 * The JMX via RMI connection is lazily created when the first call requires it.
 *
 * @author Robert Stupp
 */
public class JmxConnect implements Closeable {
    private static final String DEFATULT_CASSANDRA_JMX_HOST = "127.0.0.1";
    private static final int DEFAULT_CASSANDRA_JMX_PORT = 7199;

    private static final String STORAGE_SERVICE_NAME = "org.apache.cassandra.db:type=StorageService";
    private static final String ENDPOINT_SNITCH_INFO_NAME = "org.apache.cassandra.db:type=EndpointSnitchInfo";

    private static final String fmtUrl = "service:jmx:rmi:///jndi/rmi://[%s]:%d/jmxrmi";

    private static final String GOSSIPER_MBEAN_NAME = "org.apache.cassandra.net:type=Gossiper";

    private final String host;
    private final int port;
    private final String username;
    private final String password;

    private JMXConnector jmxc;
    private MBeanServerConnection mbeanServerConn;

    private StorageServiceMBean ssProxy;
    private MessagingServiceMBean msProxy;
    private StreamManagerMBean streamProxy;
    private CompactionManagerMBean compactionProxy;
    private FailureDetectorMBean fdProxy;
    private CacheServiceMBean cacheService;
    private StorageProxyMBean spProxy;
    private HintedHandOffManagerMBean hhProxy;
    private GCInspectorMXBean gcProxy;
    private GossiperMBean gossProxy;
    private EndpointSnitchInfoMBean snitchProxy;
    private MemoryMXBean memProxy;
    private RuntimeMXBean runtimeProxy;

    private final ReentrantLock lock = new ReentrantLock();

    public JmxConnect(String host, int port, String username, String password) {
        if (host == null || host.trim().isEmpty())
            host = DEFATULT_CASSANDRA_JMX_HOST;
        if (port <= 0)
            port = DEFAULT_CASSANDRA_JMX_PORT;

        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    public JmxConnect(String host, int port) {
        this(host, port, null, null);
    }

    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            if (jmxc == null)
                return;

            jmxc.close();
        } finally {
            lock.unlock();

            jmxc = null;
            mbeanServerConn = null;
            ssProxy = null;
            msProxy = null;
            streamProxy = null;
            compactionProxy = null;
            fdProxy = null;
            cacheService = null;
            spProxy = null;
            hhProxy = null;
            gcProxy = null;
            gossProxy = null;
            snitchProxy = null;
            memProxy = null;
            runtimeProxy = null;
        }
    }

    private void connect() throws IOException {
        lock.lock();
        try {
            if (jmxc != null)
                return;

            JMXServiceURL jmxUrl = new JMXServiceURL(String.format(fmtUrl, host, port));
            Map<String, Object> env = new HashMap<>();
            if (username != null) {
                String[] creds = {username, password};
                env.put(JMXConnector.CREDENTIALS, creds);
            }
            jmxc = JMXConnectorFactory.connect(jmxUrl, env);
            mbeanServerConn = jmxc.getMBeanServerConnection();
        } finally {
            lock.unlock();
        }
    }

    private <T> T newProxy(String name, Class<T> type) {
        lock.lock();
        try {
            connect();
            return JMX.newMBeanProxy(mbeanServerConn, new ObjectName(name), type);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    private <T> T newPlatformProxy(String name, Class<T> type) {
        lock.lock();
        try {
            connect();
            return ManagementFactory.newPlatformMXBeanProxy(mbeanServerConn, name, type);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public MemoryMXBean getMemoryProxy() {
        if (memProxy == null)
            memProxy = newPlatformProxy(ManagementFactory.MEMORY_MXBEAN_NAME, MemoryMXBean.class);
        return memProxy;
    }

    public RuntimeMXBean getRuntimeProxy() {
        if (runtimeProxy == null)
            runtimeProxy = newPlatformProxy(ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
        return runtimeProxy;
    }

    public MessagingServiceMBean getMessagingServiceProxy() {
        if (msProxy == null)
            msProxy = newProxy(MessagingService.MBEAN_NAME, MessagingServiceMBean.class);
        return msProxy;
    }

    public StorageProxyMBean getStorageProxy() {
        if (spProxy == null)
            spProxy = newProxy(StorageProxy.MBEAN_NAME, StorageProxyMBean.class);
        return spProxy;
    }

    public StorageServiceMBean getStorageServiceProxy() {
        if (ssProxy == null)
            ssProxy = newProxy(STORAGE_SERVICE_NAME, StorageServiceMBean.class);
        return ssProxy;
    }

    public EndpointSnitchInfoMBean getEndpointSnitchInfoProxy() {
        if (snitchProxy == null)
            snitchProxy = newProxy(ENDPOINT_SNITCH_INFO_NAME, EndpointSnitchInfoMBean.class);
        return snitchProxy;
    }

    public StreamManagerMBean getStreamManagerProxy() {
        if (streamProxy == null)
            streamProxy = newProxy(StreamManagerMBean.OBJECT_NAME, StreamManagerMBean.class);
        return streamProxy;
    }

    public CacheServiceMBean getCacheServiceProxy() {
        if (cacheService == null)
            cacheService = newProxy(CacheService.MBEAN_NAME, CacheServiceMBean.class);
        return cacheService;
    }

    public CompactionManagerMBean getCompactionManagerProxy() {
        if (compactionProxy == null)
            compactionProxy = newProxy(CompactionManager.MBEAN_OBJECT_NAME, CompactionManagerMBean.class);
        return compactionProxy;
    }

    public FailureDetectorMBean getFailureDetectorProxy() {
        if (fdProxy == null)
            fdProxy = newProxy(FailureDetector.MBEAN_NAME, FailureDetectorMBean.class);
        return fdProxy;
    }

    public GCInspectorMXBean getGCInspectorProxy() {
        if (gcProxy == null)
            gcProxy = newProxy(GCInspector.MBEAN_NAME, GCInspectorMXBean.class);
        return gcProxy;
    }

    public GossiperMBean getGossiperProxy() {
        if (gossProxy == null)
            gossProxy = newProxy(GOSSIPER_MBEAN_NAME, GossiperMBean.class);
        return gossProxy;
    }

    public HintedHandOffManagerMBean getHintedHandOffManagerProxy() {
        if (hhProxy == null)
            hhProxy = newProxy(HintedHandOffManager.MBEAN_NAME, HintedHandOffManagerMBean.class);
        return hhProxy;
    }

}
