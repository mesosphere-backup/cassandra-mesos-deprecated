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

import com.fasterxml.jackson.core.JsonGenerator;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.CassandraCluster;
import io.mesosphere.mesos.frameworks.cassandra.scheduler.NodeCounts;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

@Path("/")
public final class ConfigAndStatusController extends AbstractApiController {
    public ConfigAndStatusController(CassandraCluster cluster) {
        super(cluster);
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
    @Path("/config")
    public Response config() {
        return buildStreamingResponse(new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {

                CassandraFrameworkProtos.CassandraFrameworkConfiguration configuration = cluster.getConfiguration().get();

                json.writeStringField("frameworkName", configuration.getFrameworkName());
                json.writeStringField("frameworkId", configuration.getFrameworkId());
                json.writeStringField("clusterName", configuration.getFrameworkName());
                json.writeNumberField("initialNumberOfNodes", configuration.getInitialNumberOfNodes());
                json.writeNumberField("initialNumberOfSeeds", configuration.getInitialNumberOfSeeds());

                NodeCounts nodeCounts = cluster.getClusterState().nodeCounts();
                json.writeNumberField("currentNumberOfNodes", nodeCounts.getNodeCount());
                json.writeNumberField("currentNumberOfSeeds", nodeCounts.getSeedCount());
                json.writeNumberField("nodesToAcquire", cluster.getClusterState().get().getNodesToAcquire());
                json.writeNumberField("seedsToAcquire", cluster.getClusterState().get().getSeedsToAcquire());

                CassandraFrameworkProtos.CassandraConfigRole configRole = configuration.getDefaultConfigRole();
                json.writeObjectFieldStart("defaultConfigRole");
                writeConfigRole(json, configRole);
                json.writeEndObject();

                json.writeNumberField("nativePort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_NATIVE));
                json.writeNumberField("rpcPort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_RPC));
                json.writeNumberField("storagePort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_STORAGE));
                json.writeNumberField("sslStoragePort", CassandraCluster.getPortMapping(configuration, CassandraCluster.PORT_STORAGE_SSL));

                writeSeedIps(json);

                json.writeNumberField("healthCheckIntervalSeconds", configuration.getHealthCheckIntervalSeconds());
                json.writeNumberField("bootstrapGraceTimeSeconds", configuration.getBootstrapGraceTimeSeconds());

                CassandraFrameworkProtos.ClusterJobStatus currentTask = cluster.getCurrentClusterJob();
                writeClusterJob(json, "currentClusterTask", currentTask);

                CassandraFrameworkProtos.ClusterJobStatus lastRepair = cluster.getLastClusterJob(CassandraFrameworkProtos.ClusterJobType.REPAIR);
                writeClusterJob(json, "lastRepair", lastRepair);

                CassandraFrameworkProtos.ClusterJobStatus lastCleanup = cluster.getLastClusterJob(CassandraFrameworkProtos.ClusterJobType.CLEANUP);
                writeClusterJob(json, "lastCleanup", lastCleanup);

                json.writeNumberField("nextPossibleServerLaunchTimestamp", cluster.nextPossibleServerLaunchTimestamp());

            }
        });
    }

    @GET
    @Path("/qaReportResources/text")
    public Response qaReportResourcesText() {
        return buildStreamingResponse(Response.Status.OK, "text/plain", new StreamingTextResponse() {
            @Override
            public void write(PrintWriter pw) {
                CassandraFrameworkProtos.CassandraClusterState clusterState = cluster.getClusterState().get();
                for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.getNodesList()) {

                    if (!cassandraNode.hasCassandraNodeExecutor()) {
                        continue;
                    }

                    pw.println("JMX_PORT: " + cassandraNode.getJmxConnect().getJmxPort());
                    pw.println("JMX_IP: " + cassandraNode.getJmxConnect().getIp());
                    pw.println("NODE_IP: " + cassandraNode.getIp());
                    pw.println("BASE: http://" + cassandraNode.getIp() + ":5051/");

                    for (String logFile : cluster.getNodeLogFiles(cassandraNode)) {
                        pw.println("LOG: " + logFile);
                    }

                }
            }
        });
    }

    @GET
    @Path("/qaReportResources")
    public Response qaReportResources() {
        return buildStreamingResponse(new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {

                CassandraFrameworkProtos.CassandraClusterState clusterState = cluster.getClusterState().get();
                json.writeObjectFieldStart("nodes");
                for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.getNodesList()) {

                    if (!cassandraNode.hasCassandraNodeExecutor()) {
                        continue;
                    }

                    CassandraFrameworkProtos.ExecutorMetadata executorMetadata = cluster.metadataForExecutor(cassandraNode.getCassandraNodeExecutor().getExecutorId());
                    if (executorMetadata == null) {
                        continue;
                    }

                    json.writeObjectFieldStart(cassandraNode.getCassandraNodeExecutor().getExecutorId());
                    String workdir = executorMetadata.getWorkdir();
                    json.writeStringField("workdir", workdir);

                    json.writeStringField("slaveBaseUri", "http://" + cassandraNode.getIp() + ":5051/");

                    json.writeStringField("ip", cassandraNode.getIp());
                    json.writeStringField("hostname", cassandraNode.getHostname());
                    json.writeStringField("targetRunState", cassandraNode.getTargetRunState().name());
                    json.writeStringField("jmxIp", cassandraNode.getJmxConnect().getIp());
                    json.writeNumberField("jmxPort", cassandraNode.getJmxConnect().getJmxPort());

                    json.writeBooleanField("live", cluster.isLiveNode(cassandraNode));

                    json.writeArrayFieldStart("logfiles");
                    for (String logFile : cluster.getNodeLogFiles(cassandraNode)) {
                        json.writeString(logFile);
                    }
                    json.writeEndArray();

                    json.writeEndObject();

                }
                json.writeEndObject();
            }
        });
    }

    /**
     * Retrieve a list of all nodes including their status.
     *
     *     <pre>{@code {
     * "replaceNodes" : [ ],
     * "nodesToAcquire" : 0,
     * "nodes" : [ {
     *     "tasks" : {
     *         "METADATA" : {
     *             "cpuCores" : 0.1,
     *             "diskMb" : 16,
     *             "memMb" : 16,
     *             "taskId" : "cassandra.node.0.executor"
     *         },
     *         "SERVER" : {
     *             "cpuCores" : 2.0,
     *             "diskMb" : 2048,
     *             "memMb" : 2048,
     *             "taskId" : "cassandra.node.0.executor.server"
     *         }
     *     },
     *     "executorId" : "cassandra.node.0.executor",
     *     "ip" : "127.0.0.2",
     *     "hostname" : "127.0.0.2",
     *     "targetRunState" : "RUN",
     *     "jmxPort" : 64112,
     *     "seedNode" : true,
     *     "cassandraDaemonPid" : 6104,
     *     "lastHealthCheck" : 1426686217128,
     *     "healthCheckDetails" : {
     *         "healthy" : true,
     *         "msg" : "",
     *         "version" : "2.1.4",
     *         "operationMode" : "NORMAL",
     *         "clusterName" : "cassandra",
     *         "dataCenter" : "DC1",
     *         "rack" : "RAC1",
     *         "endpoint" : "127.0.0.2",
     *         "hostId" : "4207396e-6aa0-432e-97d9-1a4df3c1057f",
     *         "joined" : true,
     *         "gossipInitialized" : true,
     *         "gossipRunning" : true,
     *         "nativeTransportRunning" : true,
     *         "rpcServerRunning" : true,
     *         "tokenCount" : 256,
     *         "uptimeMillis" : 29072
     *     }
     * }, {
     *     "tasks" : {
     *     "METADATA" : {
     *         "cpuCores" : 0.1,
     *         "diskMb" : 16,
     *         "memMb" : 16,
     *         "taskId" : "cassandra.node.1.executor"
     *     },
     *     "SERVER" : {
     *         "cpuCores" : 2.0,
     *         "diskMb" : 2048,
     *         "memMb" : 2048,
     *         "taskId" : "cassandra.node.1.executor.server"
     *     }
     *     },
     *     "executorId" : "cassandra.node.1.executor",
     *     "ip" : "127.0.0.1",
     *     "hostname" : "localhost",
     *     "targetRunState" : "RUN",
     *     "jmxPort" : 64113,
     *     "seedNode" : false,
     *     "cassandraDaemonPid" : 6127,
     *     "lastHealthCheck" : 1426686217095,
     *     "healthCheckDetails" : {
     *         "healthy" : true,
     *         "msg" : "",
     *         "version" : "2.1.4",
     *         "operationMode" : "JOINING",
     *         "clusterName" : "cassandra",
     *         "dataCenter" : "",
     *         "rack" : "",
     *         "endpoint" : "",
     *         "hostId" : "",
     *         "joined" : true,
     *         "gossipInitialized" : true,
     *         "gossipRunning" : true,
     *         "nativeTransportRunning" : false,
     *         "rpcServerRunning" : false,
     *         "tokenCount" : 0,
     *         "uptimeMillis" : 16936
     *     }
     * } ]
     * }}</pre>
     */
    @GET
    @Path("/nodes")
    public Response nodes() {
        return buildStreamingResponse(new StreamingJsonResponse() {
            @Override
            public void write(JsonGenerator json) throws IOException {
                CassandraFrameworkProtos.CassandraClusterState clusterState = cluster.getClusterState().get();

                json.writeArrayFieldStart("replaceNodes");
                for (String ip : clusterState.getReplaceNodeIpsList()) {
                    json.writeString(ip);
                }
                json.writeEndArray();

                json.writeNumberField("nodesToAcquire", clusterState.getNodesToAcquire());

                json.writeArrayFieldStart("nodes");
                for (CassandraFrameworkProtos.CassandraNode cassandraNode : clusterState.getNodesList()) {
                    json.writeStartObject();

                    if (cassandraNode.hasReplacementForIp()) {
                        json.writeStringField("replacementForIp", cassandraNode.getReplacementForIp());
                    }

                    json.writeObjectFieldStart("tasks");
                    for (CassandraFrameworkProtos.CassandraNodeTask cassandraNodeTask : cassandraNode.getTasksList()) {
                        writeTask(json, cassandraNodeTask);
                    }
                    json.writeEndObject();
// TODO                cassandraNode.getDataVolumesList();

                    if (!cassandraNode.hasCassandraNodeExecutor()) {
                        json.writeNullField("executorId");
                        json.writeNullField("workdir");
                    } else {
                        json.writeStringField("executorId", cassandraNode.getCassandraNodeExecutor().getExecutorId());
                        CassandraFrameworkProtos.ExecutorMetadata executorMetadata = cluster.metadataForExecutor(cassandraNode.getCassandraNodeExecutor().getExecutorId());
                        if (executorMetadata != null) {
                            json.writeStringField("workdir", executorMetadata.getWorkdir());
                        } else {
                            json.writeNullField("workdir");
                        }
                    }
                    json.writeStringField("ip", cassandraNode.getIp());
                    json.writeStringField("hostname", cassandraNode.getHostname());
                    json.writeStringField("targetRunState", cassandraNode.getTargetRunState().name());
                    json.writeNumberField("jmxPort", cassandraNode.getJmxConnect().getJmxPort());
                    json.writeBooleanField("seedNode", cassandraNode.getSeed());
                    if (!cassandraNode.hasCassandraDaemonPid()) {
                        json.writeNullField("cassandraDaemonPid");
                    } else {
                        json.writeNumberField("cassandraDaemonPid", cassandraNode.getCassandraDaemonPid());
                    }

                    CassandraFrameworkProtos.HealthCheckHistoryEntry lastHealthCheck =
                        cassandraNode.hasCassandraNodeExecutor() ? cluster.lastHealthCheck(cassandraNode.getCassandraNodeExecutor().getExecutorId()) : null;

                    if (lastHealthCheck != null) {
                        json.writeNumberField("lastHealthCheck", lastHealthCheck.getTimestampEnd());
                    } else {
                        json.writeNullField("lastHealthCheck");
                    }

                    if (lastHealthCheck != null) {
                        json.writeObjectFieldStart("healthCheckDetails");

                        CassandraFrameworkProtos.HealthCheckDetails hcd = lastHealthCheck.getDetails();

                        json.writeBooleanField("healthy", hcd.getHealthy());
                        json.writeStringField("msg", hcd.getMsg());

                        json.writeStringField("version", hcd.getInfo().getVersion());
                        json.writeStringField("operationMode", hcd.getInfo().getOperationMode());
                        json.writeStringField("clusterName", hcd.getInfo().getClusterName());
                        json.writeStringField("dataCenter", hcd.getInfo().getDataCenter());
                        json.writeStringField("rack", hcd.getInfo().getRack());
                        json.writeStringField("endpoint", hcd.getInfo().getEndpoint());
                        json.writeStringField("hostId", hcd.getInfo().getHostId());
                        json.writeBooleanField("joined", hcd.getInfo().getJoined());
                        json.writeBooleanField("gossipInitialized", hcd.getInfo().getGossipInitialized());
                        json.writeBooleanField("gossipRunning", hcd.getInfo().getGossipRunning());
                        json.writeBooleanField("nativeTransportRunning", hcd.getInfo().getNativeTransportRunning());
                        json.writeBooleanField("rpcServerRunning", hcd.getInfo().getRpcServerRunning());
                        json.writeNumberField("tokenCount", hcd.getInfo().getTokenCount());
                        json.writeNumberField("uptimeMillis", hcd.getInfo().getUptimeMillis());

                        json.writeEndObject();
                    } else {
                        json.writeNullField("healthCheckDetails");
                    }

                    final List<CassandraFrameworkProtos.DataVolume> dataVolumes = cassandraNode.getDataVolumesList();
                    json.writeArrayFieldStart("dataVolumes");
                    for (final CassandraFrameworkProtos.DataVolume volume : dataVolumes) {
                        json.writeStartObject();
                        json.writeStringField("path", volume.getPath());
                        if (volume.hasSizeMb()) {
                            json.writeNumberField("size", volume.getSizeMb());
                        }
                        json.writeEndObject();
                    }
                    json.writeEndArray();

                    json.writeEndObject();
                }
                json.writeEndArray();
            }
        });
    }

    private static void writeConfigRole(JsonGenerator json, CassandraFrameworkProtos.CassandraConfigRole configRole) throws IOException {
        json.writeStringField("cassandraVersion", configRole.getCassandraVersion());
        json.writeNumberField("diskMb", configRole.getResources().getDiskMb());
        json.writeNumberField("cpuCores", configRole.getResources().getCpuCores());
        if (configRole.hasMemJavaHeapMb()) {
            json.writeNumberField("memJavaHeapMb", configRole.getMemJavaHeapMb());
        }
        if (configRole.hasMemAssumeOffHeapMb()) {
            json.writeNumberField("memAssumeOffHeapMb", configRole.getMemAssumeOffHeapMb());
        }
        json.writeNumberField("memMb", configRole.getResources().getMemMb());

        if (!configRole.hasTaskEnv()) {
            json.writeNullField("taskEnv");
        } else {
            json.writeObjectFieldStart("taskEnv");
            for (CassandraFrameworkProtos.TaskEnv.Entry entry : configRole.getTaskEnv().getVariablesList()) {
                json.writeStringField(entry.getName(), entry.getValue());
            }
            json.writeEndObject();
            json.writeObjectFieldStart("cassandraYaml");
            for (CassandraFrameworkProtos.TaskConfig.Entry entry : configRole.getCassandraYamlConfig().getVariablesList()) {
                if (entry.hasLongValue()) {
                    json.writeNumberField(entry.getName(), entry.getLongValue());
                }
                if (entry.hasStringValue()) {
                    json.writeStringField(entry.getName(), entry.getStringValue());
                }
            }
            json.writeEndObject();
        }
    }

    private static void writeTask(JsonGenerator json, CassandraFrameworkProtos.CassandraNodeTask task) throws IOException {
        if (task != null && task.hasTaskId()) {
            json.writeObjectFieldStart(task.getType().toString());
            json.writeNumberField("cpuCores", task.getResources().getCpuCores());
            json.writeNumberField("diskMb", task.getResources().getDiskMb());
            json.writeNumberField("memMb", task.getResources().getMemMb());
            json.writeStringField("taskId", task.getTaskId());
            json.writeEndObject();
        }
    }
}
