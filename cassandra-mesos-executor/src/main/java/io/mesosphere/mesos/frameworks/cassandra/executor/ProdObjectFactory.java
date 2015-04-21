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
package io.mesosphere.mesos.frameworks.cassandra.executor;

import com.fasterxml.jackson.dataformat.yaml.snakeyaml.Yaml;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.JmxConnect;
import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.ProdJmxConnect;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.io.*;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos.*;

// production object factory - there's another implementation, that's used for mocked unit tests
final class ProdObjectFactory implements ObjectFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProdObjectFactory.class);

    @NotNull
    @Override
    public JmxConnect newJmxConnect(@NotNull final CassandraFrameworkProtos.JmxConnect jmx) {
        return new ProdJmxConnect(jmx);
    }

    @Override
    @NotNull
    public WrappedProcess launchCassandraNodeTask(@NotNull final Marker taskIdMarker, @NotNull final CassandraServerRunTask serverRunTask) throws LaunchNodeException {
        try {
            writeCassandraServerConfig(taskIdMarker, serverRunTask.getVersion(), serverRunTask.getCassandraServerConfig());
        } catch (final IOException e) {
            throw new LaunchNodeException("Failed to prepare instance files", e);
        }

        final ProcessBuilder processBuilder = new ProcessBuilder(serverRunTask.getCommandList())
                .directory(new File(System.getProperty("user.dir")))
                .redirectOutput(new File("cassandra-stdout.log"))
                .redirectError(new File("cassandra-stderr.log"));
        for (final TaskEnv.Entry entry : serverRunTask.getCassandraServerConfig().getTaskEnv().getVariablesList()) {
            processBuilder.environment().put(entry.getName(), entry.getValue());
        }
        processBuilder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Starting Process: {}", processBuilderToString(processBuilder));
        try {
            return new ProdWrappedProcess(processBuilder.start());
        } catch (final IOException e) {
            throw new LaunchNodeException("Failed to start process", e);
        }
    }

    @Override
    public void updateCassandraServerConfig(
        @NotNull final Marker taskIdMarker,
        @NotNull final CassandraServerRunTask cassandraServerRunTask,
        @NotNull final UpdateConfigTask updateConfigTask
    ) throws ConfigChangeException {
        try {
            writeCassandraServerConfig(taskIdMarker, cassandraServerRunTask.getVersion(), updateConfigTask.getCassandraServerConfig());
        } catch (final IOException e) {
            throw new ConfigChangeException("Failed to update instance files", e);
        }
    }

    private static void writeCassandraServerConfig(
        @NotNull final Marker taskIdMarker,
        @NotNull final String version,
        @NotNull final CassandraServerConfig serverConfig
    ) throws IOException {
        for (final TaskFile taskFile : serverConfig.getTaskFilesList()) {
            final File file = new File(taskFile.getOutputPath());
            if (LOGGER.isDebugEnabled())
                LOGGER.debug(taskIdMarker, "Overwriting file {}", file);
            Files.createParentDirs(file);
            Files.write(taskFile.getData().toByteArray(), file);
        }

        modifyCassandraYaml(taskIdMarker, version, serverConfig);
        modifyCassandraEnvSh(taskIdMarker, version, serverConfig);
        modifyCassandraRackdc(taskIdMarker, version);
    }

    @NotNull
    private static String processBuilderToString(@NotNull final ProcessBuilder builder) {
        return "ProcessBuilder{\n" +
                "directory() = " + builder.directory() + ",\n" +
                "command() = " + Joiner.on(" ").join(builder.command()) + ",\n" +
                "environment() = " + Joiner.on("\n").withKeyValueSeparator("->").join(builder.environment()) + "\n}";
    }

    private static void modifyCassandraRackdc(
        @NotNull final Marker taskIdMarker,
        @NotNull final String version
    ) throws IOException {

        LOGGER.info(taskIdMarker, "Building cassandra-rackdc.properties");

        final Properties props = new Properties();
        props.put("dc", "DC1");
        props.put("rack", "RAC1");
        // Add a suffix to a datacenter name. Used by the Ec2Snitch and Ec2MultiRegionSnitch to append a string to the EC2 region name.
        //props.put("dc_suffix", "");
        // Uncomment the following line to make this snitch prefer the internal ip when possible, as the Ec2MultiRegionSnitch does.
        //props.put("prefer_local", "true");
        final File cassandraRackDc = new File("apache-cassandra-" + version + "/conf/cassandra-rackdc.properties");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(cassandraRackDc))) {
            props.store(bw, "Created by Apache Mesos Cassandra framework");
        }
    }

    private static void modifyCassandraEnvSh(
        @NotNull final Marker taskIdMarker,
        @NotNull final String version,
        @NotNull final CassandraServerConfig cassandraServerConfig
    ) throws IOException {
        int jmxPort = 0;
        String localJmx = "yes";
        boolean noJmxAuth = false;
        for (final TaskEnv.Entry entry : cassandraServerConfig.getTaskEnv().getVariablesList()) {
            if ("JMX_PORT".equals(entry.getName())) {
                jmxPort = Integer.parseInt(entry.getValue());
            } else if ("LOCAL_JMX".equals(entry.getName())) {
                localJmx = entry.getValue();
            } else if ("CASSANDRA_JMX_NO_AUTHENTICATION".equals(entry.getName())) {
                noJmxAuth = "no".equals(entry.getValue());
            }
        }

        LOGGER.info(taskIdMarker, "Building cassandra-env.sh");

        // Unfortunately it is not possible to pass JMX_PORT as an environment variable to C* startup -
        // it is explicitly set in cassandra-env.sh

        final File cassandraEnvSh = new File("apache-cassandra-" + version + "/conf/cassandra-env.sh");

        LOGGER.info(taskIdMarker, "Reading cassandra-env.sh");
        final List<String> lines = Files.readLines(cassandraEnvSh, Charset.forName("UTF-8"));
        for (int i = 0; i < lines.size(); i++) {
            final String line = lines.get(i);
            if (line.startsWith("JMX_PORT=")) {
                lines.set(i, "JMX_PORT=" + jmxPort);
            } else if (line.startsWith("LOCAL_JMX=")) {
                lines.set(i, "LOCAL_JMX=" + localJmx);
            } else if (noJmxAuth) {
                if (line.contains("JVM_OPTS=\"$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=")) {
                    lines.set(i, "JVM_OPTS=\"$JVM_OPTS -Dcom.sun.management.jmxremote.authenticate=false\"");
                }
            }
        }
        LOGGER.info(taskIdMarker, "Writing cassandra-env.sh");
        try (PrintWriter pw = new PrintWriter(new FileWriter(cassandraEnvSh))) {
            for (final String line : lines)
                pw.println(line);
        }
    }

    @SuppressWarnings("unchecked")
    private static void modifyCassandraYaml(
        @NotNull final Marker taskIdMarker,
        @NotNull final String version,
        @NotNull final CassandraServerConfig cassandraServerConfig
    ) throws IOException {
        LOGGER.info(taskIdMarker, "Building cassandra.yaml");

        final File cassandraYaml = new File("apache-cassandra-" + version + "/conf/cassandra.yaml");

        final Yaml yaml = new Yaml();
        final Map<String, Object> yamlMap;
        LOGGER.info(taskIdMarker, "Reading cassandra.yaml");
        try (BufferedReader br = new BufferedReader(new FileReader(cassandraYaml))) {
            yamlMap = (Map<String, Object>) yaml.load(br);
        }
        LOGGER.info(taskIdMarker, "Modifying cassandra.yaml");
        for (final TaskConfig.Entry entry : cassandraServerConfig.getCassandraYamlConfig().getVariablesList()) {
            switch (entry.getName()) {
                case "seeds":
                    final List<Map<String, Object>> seedProviderList = (List<Map<String, Object>>) yamlMap.get("seed_provider");
                    final Map<String, Object> seedProviderMap = seedProviderList.get(0);
                    final List<Map<String, Object>> parameters = (List<Map<String, Object>>) seedProviderMap.get("parameters");
                    final Map<String, Object> parametersMap = parameters.get(0);
                    parametersMap.put("seeds", entry.getStringValue());
                    break;
                default:
                    if (entry.hasStringValue()) {
                        yamlMap.put(entry.getName(), entry.getStringValue());
                    } else if (entry.hasLongValue()) {
                        yamlMap.put(entry.getName(), entry.getLongValue());
                    } else if (!entry.getStringValuesList().isEmpty()) {
                        yamlMap.put(entry.getName(), entry.getStringValuesList());
                    }
            }
        }
        if (LOGGER.isDebugEnabled()) {
            final StringWriter sw = new StringWriter();
            yaml.dump(yamlMap, sw);
            LOGGER.debug("cassandra.yaml result: {}", sw);
        }
        LOGGER.info(taskIdMarker, "Writing cassandra.yaml");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(cassandraYaml))) {
            yaml.dump(yamlMap, bw);
        }
    }

}
