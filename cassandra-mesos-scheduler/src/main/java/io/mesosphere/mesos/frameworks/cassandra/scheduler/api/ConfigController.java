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
import io.mesosphere.mesos.frameworks.cassandra.scheduler.NodeCounts;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.PersistedCassandraFrameworkConfiguration;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.util.JaxRsUtils;
import org.jetbrains.annotations.NotNull;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/config")
@Produces("application/json")
public final class ConfigController {

    @NotNull
    private final JsonFactory factory;
    @NotNull
    private final CassandraCluster cluster;

    public ConfigController(@NotNull final CassandraCluster cluster, @NotNull final JsonFactory factory) {
        this.cluster = cluster;
        this.factory = factory;
    }

    /**
     * Returns the configuration as JSON.
     *
     *     Example: <pre>{@code {
     * "frameworkName" : "cassandra",
     * "frameworkId" : "20150318-143436-16777343-5050-5621-0000",
     * "defaultConfigRole" : {
     *     "cassandraVersion" : "2.1.4",
     *     "targetNodeCount" : 2,
     *     "seedNodeCount" : 1,
     *     "diskMb" : 2048,
     *     "cpuCores" : 2.0,
     *     "memJavaHeapMb" : 1024,
     *     "memAssumeOffHeapMb" : 1024,
     *     "memMb" : 2048,
     *     "taskEnv" : null
     * },
     * "nativePort" : 9042,
     * "rpcPort" : 9160,
     * "storagePort" : 7000,
     * "sslStoragePort" : 7001,
     * "seeds" : [ "127.0.0.1" ],
     * "healthCheckIntervalSeconds" : 10,
     * "bootstrapGraceTimeSeconds" : 0,
     * "currentClusterTask" : null,
     * "lastRepair" : null,
     * "lastCleanup" : null,
     * "nextPossibleServerLaunchTimestamp" : 1426685858805
     * }}</pre>
     */
    @GET
    public Response config() {
        return JaxRsUtils.buildStreamingResponse(factory, new StreamingJsonResponse() {
            @Override
            public void write(final JsonGenerator json) throws IOException {

                final PersistedCassandraFrameworkConfiguration configuration = cluster.getConfiguration();
                final CassandraFrameworkProtos.CassandraFrameworkConfiguration config = configuration.get();

                json.writeStringField("frameworkName", config.getFrameworkName());
                json.writeStringField("frameworkId", config.getFrameworkId());
                json.writeStringField("clusterName", config.getFrameworkName());
                json.writeNumberField("targetNumberOfNodes", config.getTargetNumberOfNodes());
                json.writeNumberField("targetNumberOfSeeds", config.getTargetNumberOfSeeds());

                final NodeCounts nodeCounts = cluster.getClusterState().nodeCounts();
                json.writeNumberField("currentNumberOfNodes", nodeCounts.getNodeCount());
                json.writeNumberField("currentNumberOfSeeds", nodeCounts.getSeedCount());
                json.writeNumberField("nodesToAcquire", CassandraCluster.numberOfNodesToAcquire(nodeCounts, configuration));
                json.writeNumberField("seedsToAcquire", CassandraCluster.numberOfSeedsToAcquire(nodeCounts, configuration));

                final CassandraFrameworkProtos.CassandraConfigRole configRole = config.getDefaultConfigRole();
                json.writeObjectFieldStart("defaultConfigRole");
                JaxRsUtils.writeConfigRole(json, configRole);
                json.writeEndObject();

                json.writeNumberField("nativePort", CassandraCluster.getPortMapping(config, CassandraCluster.PORT_NATIVE));
                json.writeNumberField("rpcPort", CassandraCluster.getPortMapping(config, CassandraCluster.PORT_RPC));
                json.writeNumberField("storagePort", CassandraCluster.getPortMapping(config, CassandraCluster.PORT_STORAGE));
                json.writeNumberField("sslStoragePort", CassandraCluster.getPortMapping(config, CassandraCluster.PORT_STORAGE_SSL));

                JaxRsUtils.writeSeedIps(cluster, json);

                json.writeNumberField("healthCheckIntervalSeconds", config.getHealthCheckIntervalSeconds());
                json.writeNumberField("bootstrapGraceTimeSeconds", config.getBootstrapGraceTimeSeconds());

                final CassandraFrameworkProtos.ClusterJobStatus currentTask = cluster.getCurrentClusterJob();
                JaxRsUtils.writeClusterJob(cluster, json, "currentClusterTask", currentTask);

                final CassandraFrameworkProtos.ClusterJobStatus lastRepair = cluster.getLastClusterJob(CassandraFrameworkProtos.ClusterJobType.REPAIR);
                JaxRsUtils.writeClusterJob(cluster, json, "lastRepair", lastRepair);

                final CassandraFrameworkProtos.ClusterJobStatus lastCleanup = cluster.getLastClusterJob(CassandraFrameworkProtos.ClusterJobType.CLEANUP);
                JaxRsUtils.writeClusterJob(cluster, json, "lastCleanup", lastCleanup);

                json.writeNumberField("nextPossibleServerLaunchTimestamp", cluster.nextPossibleServerLaunchTimestamp());

            }
        });
    }

}
