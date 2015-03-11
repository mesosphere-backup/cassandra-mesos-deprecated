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
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.json.JsonReadContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;

/**
 * Configures the <a href="https://github.com/datastax/java-driver">DataStax Java Driver</a> with native protocol port and contact points from the information
 * available from the Cassandra-on-Mesos framework.
 * <p>
 *     You need to have the base-API-URI provided the framework.
 *     A typical base-API-URI looks like this: {@code http://192.168.1.2:18080/}
 * </p>
 * <p>
 *     To use the DataStax Java Driver, construct the {@link Cluster} instance as follows:
 * </p>
 *     <pre><code>
 *         Cluster.Builder clusterBuilder =
 *             CassandraOnMesos.forClusterBuilder(Cluster.builder())
 *                 .withApiEndpoint(httpServerBaseUri)
 *                 .withNumberOfContactPoints(2)
 *                 .build();
 *         // the Cluster.Builder instance is now configured with two live initial contact points
 *         clusterBuilder.withClusterName(...);
 *         Cluster cluster = clusterBuilder.build();
 *     </code></pre>
 *
 * <p>
 *     See <a href="https://github.com/datastax/java-driver">DataStax Java Driver</a>.
 * </p>
 */
public final class CassandraOnMesos {
    private final Cluster.Builder clusterBuilder;

    private URI apiEndpoint;
    private int numberOfContactPoints = 3;

    private CassandraOnMesos(Cluster.Builder clusterBuilder) {
        this.clusterBuilder = clusterBuilder;
    }

    @NotNull
    public static CassandraOnMesos forClusterBuilder(@NotNull Cluster.Builder clusterBuilder) {
        if (clusterBuilder == null) {
            throw new NullPointerException("Must specify clusterBuilder");
        }
        return new CassandraOnMesos(clusterBuilder);
    }

    public URI getApiEndpoint() {
        return apiEndpoint;
    }

    /**
     * Set the base-API-URI provided the Cassandra-on-Mesos framework.
     * A typical base-API-URI looks like this: {@code http://192.168.1.2:18080/}
     */
    @NotNull
    public CassandraOnMesos withApiEndpoint(@NotNull URI apiEndpoint) {
        if (apiEndpoint == null) {
            throw new NullPointerException("Must specify apiEndpoint");
        }
        this.apiEndpoint = apiEndpoint;
        return this;
    }

    public int getNumberOfContactPoints() {
        return numberOfContactPoints;
    }

    /**
     * Set the number of contact points (live C* nodes) to configure.
     * Defaults to 3.
     */
    @NotNull
    public CassandraOnMesos withNumberOfContactPoints(int numberOfContactPoints) {
        this.numberOfContactPoints = numberOfContactPoints;
        return this;
    }

    /**
     * Fetches information about live nodes from Cassandra-on-Mesos framework and updates the
     * {@link Cluster.Builder} instance with correct native protocol port and contact points.
     */
    public Cluster.Builder build() throws IOException {
        if (apiEndpoint == null) {
            throw new NullPointerException("Must configure apiEndpoint on CassandraOnMeos");
        }

        URI uri = apiEndpoint.resolve("/live-nodes?limit=" + numberOfContactPoints);
        HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
        try {
            connection.connect();
            if (connection.getResponseCode() != 200) {
                throw new IOException("Got HTTP/" + connection.getResponseCode() + " for " + uri.toString());
            }

            try (InputStream inputStram = connection.getInputStream()) {
                JsonParser parser = new JsonFactory().createParser(inputStram);
                parser.setCodec(new ObjectMapper());
                JsonNode jsonDoc = parser.readValueAs(JsonNode.class);
                clusterBuilder
                    .withClusterName(jsonDoc.get("clusterName").asText())
                    .withPort(jsonDoc.get("nativePort").asInt());
                for (JsonNode liveNodeIp : jsonDoc.get("liveNodes")) {
                    clusterBuilder.addContactPoint(liveNodeIp.asText());
                }
            }

            return clusterBuilder;
        } finally {
            connection.disconnect();
        }
    }
}
