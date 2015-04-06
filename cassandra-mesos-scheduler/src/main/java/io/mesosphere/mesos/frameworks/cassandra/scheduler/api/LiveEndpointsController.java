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
package io.mesosphere.mesos.frameworks.cassandra.scheduler.api;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.JaxRsUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

@Path("/live-nodes")
public final class LiveEndpointsController {
    private static final Logger LOGGER = LoggerFactory.getLogger(LiveEndpointsController.class);
    @NotNull
    private final CassandraCluster cluster;
    @NotNull
    private final JsonFactory factory;

    public LiveEndpointsController(@NotNull CassandraCluster cluster, @NotNull final JsonFactory factory) {
        this.cluster = cluster;
        this.factory = factory;
    }

    /**
     * Variant of the live Cassandra nodes endpoint that produces JSON.
     * Allows to retrieve multiple live nodes limited to 3 nodes by default. The limit can be changed with the
     * query parameter {@code limit}.
     *
     *     Example: <pre>{@code {
     * "nativePort" : 9042,
     * "rpcPort" : 9160,
     * "jmxPort" : 7199,
     * "liveNodes" : [ "127.0.0.1", "127.0.0.2" ]
     * }}</pre>
     */
    @GET
    @Produces("application/json")
    public Response liveEndpointsJson(@QueryParam("limit") @DefaultValue("3") int limit) {
        return liveEndpoints("json", limit);
    }

    /**
     * Variant of the live Cassandra nodes endpoint that produces plain text.
     * Allows to retrieve multiple live nodes limited to 3 nodes by default. The limit can be changed with the
     * query parameter {@code limit}.
     *
     *     Example: <pre>{@code NATIVE: 9042
     * RPC: 9160
     * JMX: 7199
     * IP: 127.0.0.1
     * IP: 127.0.0.2
     * }</pre>
     */
    @GET
    @Path("/text")
    @Produces("text/plain")
    public Response liveEndpointsText(@QueryParam("limit") @DefaultValue("3") int limit) {
        return liveEndpoints("text", limit);
    }

    /**
     * Variant of the live Cassandra nodes endpoint that produces partial command line for cqlsh.
     */
    @GET
    @Path("/cqlsh")
    @Produces("text/x-cassandra-cqlsh")
    public Response liveEndpointsCqlsh() {
        return liveEndpoints("cqlsh", 1);
    }

    /**
     * Variant of the live Cassandra nodes endpoint that produces partial command line for nodetool.
     */
    @GET
    @Path("/nodetool")
    @Produces("text/x-cassandra-nodetool")
    public Response liveEndpointsNodetool() {
        return liveEndpoints("nodetool", 1);
    }

    /**
     * Variant of the live Cassandra nodes endpoint that produces partial command line for cassandra-stress.
     * Allows to retrieve multiple nodes limited to 3 nodes by default. The limit can be changed with the
     * query parameter {@code limit}.
     */
    @GET
    @Path("/stress")
    @Produces("text/x-cassandra-stress")
    public Response liveEndpointsStress(@QueryParam("limit") @DefaultValue("3") int limit) {
        return liveEndpoints("stress", limit);
    }

    private Response liveEndpoints(String forTool, int limit) {
        final List<CassandraFrameworkProtos.CassandraNode> liveNodes = cluster.liveNodes(limit);

        if (liveNodes.isEmpty()) {
            return Response.status(400).build();
        }

        final CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration().get();

        final int nativePort = CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_NATIVE);
        final int rpcPort = CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_RPC);
        final int jmxPort = CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_JMX);

        CassandraFrameworkProtos.CassandraNode first = liveNodes.get(0);

        try {
            switch (forTool) {
                case "cqlsh":
                    // return a string: "HOST PORT"
                    return Response.ok(first.getIp() + ' ' + nativePort).build();
                case "stress":
                    // cassandra-stress options:
                    // -node NODE1,NODE2,...
                    // -port [native=NATIVE_PORT] [thrift=THRIFT_PORT] [jmx=JMX_PORT]
                    StringBuilder sb = new StringBuilder();
                    sb.append("-node ");
                    for (int i = 0; i < liveNodes.size(); i++) {
                        if (i > 0) {
                            sb.append(',');
                        }
                        sb.append(liveNodes.get(i).getIp());
                    }
                    sb.append(" -port native=")
                        .append(nativePort)
                        .append(" thrift=")
                        .append(rpcPort)
                        .append(" jmx=")
                        .append(jmxPort);
                    return Response.ok(sb.toString()).build();
                case "nodetool":
                    // nodetool options:
                    // -h HOST
                    // -p JMX_PORT
                    return Response.ok("-h " + first.getJmxConnect().getIp() + " -p " + first.getJmxConnect().getJmxPort()).build();
                case "json":
                    // produce a simple JSON with the native port and live node IPs
                    return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
                        @Override
                        public void write(JsonGenerator json) throws IOException {
                            json.writeStringField("clusterName", configuration.getFrameworkName());
                            json.writeNumberField("nativePort", nativePort);
                            json.writeNumberField("rpcPort", rpcPort);
                            json.writeNumberField("jmxPort", jmxPort);

                            json.writeArrayFieldStart("liveNodes");
                            for (CassandraFrameworkProtos.CassandraNode liveNode : liveNodes) {
                                json.writeString(liveNode.getIp());
                            }
                            json.writeEndArray();
                        }
                    });
                case "text":
                    // produce a simple text with the native port in the first line and one line per live node IP
                    return JaxRsUtils.buildStreamingResponse(Response.Status.OK, "text/plain", new StreamingTextResponse() {
                        @Override
                        public void write(PrintWriter pw) {
                            pw.println("NATIVE: " + nativePort);
                            pw.println("RPC: " + rpcPort);
                            pw.println("JMX: " + jmxPort);
                            for (CassandraFrameworkProtos.CassandraNode liveNode : liveNodes) {
                                pw.println("IP: " + liveNode.getIp());
                            }
                        }
                    });
            }

            return Response.status(404).build();
        } catch (Exception e) {
            LOGGER.error("Failed to all nodes list", e);
            return Response.serverError().build();
        }
    }
}
