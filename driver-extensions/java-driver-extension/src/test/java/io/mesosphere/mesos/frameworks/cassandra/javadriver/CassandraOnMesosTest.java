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
package io.mesosphere.mesos.frameworks.cassandra.javadriver;

import com.datastax.driver.core.Cluster;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.*;
import java.util.List;

import static org.junit.Assert.*;

public class CassandraOnMesosTest {
    private HttpServer httpServer;
    private URI httpServerBaseUri;

    @Test
    public void testBuilder() throws Exception {
        Cluster.Builder clusterBuilder =
            CassandraOnMesos.forClusterBuilder(Cluster.builder())
                .withApiEndpoint(httpServerBaseUri)
                .withNumberOfContactPoints(2)
                .build();

        List<InetSocketAddress> contactPoints = clusterBuilder.getContactPoints();
        assertEquals(2, contactPoints.size());
        assertNotNull(clusterBuilder.getClusterName());
        assertTrue(contactPoints.contains(new InetSocketAddress("127.0.0.1", 111)));
        assertTrue(contactPoints.contains(new InetSocketAddress("127.0.0.2", 111)));
    }

    @Before
    public void before() {
        try {
            try (ServerSocket sock = new ServerSocket(0)) {
                httpServerBaseUri = URI.create(String.format("http://%s:%d/", formatInetAddress(InetAddress.getLocalHost()), sock.getLocalPort()));
            }

            ResourceConfig rc = new ResourceConfig()
                .register(new TestController());
            httpServer = GrizzlyHttpServerFactory.createHttpServer(httpServerBaseUri, rc);
            httpServer.start();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @After
    public void cleanup() {
        if (httpServer != null)
            httpServer.shutdown();
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
}