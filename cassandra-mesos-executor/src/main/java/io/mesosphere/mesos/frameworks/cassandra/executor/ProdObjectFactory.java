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

// production object factory - there's another implementation, that's used for mocked unit tests
final class ProdObjectFactory implements ObjectFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProdObjectFactory.class);

    @Override
    public JmxConnect newJmxConnect(CassandraFrameworkProtos.JmxConnect jmx) {
        return new ProdJmxConnect(jmx);
    }

    @Override
    @NotNull
    public WrappedProcess launchCassandraNodeTask(@NotNull final Marker taskIdMarker, @NotNull final CassandraFrameworkProtos.CassandraServerRunTask serverRunTask) throws LaunchNodeException {
        try {
            writeCassandraServerConfig(taskIdMarker, serverRunTask, serverRunTask.getCassandraServerConfig());
        } catch (IOException e) {
            throw new LaunchNodeException("Failed to prepare instance files", e);
        }

        final ProcessBuilder processBuilder = new ProcessBuilder(serverRunTask.getCommandList())
                .directory(new File(System.getProperty("user.dir")))
                .redirectOutput(new File("cassandra-stdout.log"))
                .redirectError(new File("cassandra-stderr.log"));
        for (final CassandraFrameworkProtos.TaskEnv.Entry entry : serverRunTask.getCassandraServerConfig().getTaskEnv().getVariablesList()) {
            processBuilder.environment().put(entry.getName(), entry.getValue());
        }
        processBuilder.environment().put("JAVA_HOME", System.getProperty("java.home"));
        if (LOGGER.isDebugEnabled())
            LOGGER.debug("Starting Process: {}", processBuilderToString(processBuilder));
        try {
            return new ProdWrappedProcess(processBuilder.start());
        } catch (IOException e) {
            throw new LaunchNodeException("Failed to start process", e);
        }
    }

    @Override
    public void updateCassandraServerConfig(@NotNull Marker taskIdMarker, @NotNull CassandraFrameworkProtos.CassandraServerRunTask cassandraServerRunTask, @NotNull CassandraFrameworkProtos.UpdateConfigTask updateConfigTask) throws ConfigChangeException {
        try {
            writeCassandraServerConfig(taskIdMarker, cassandraServerRunTask, updateConfigTask.getCassandraServerConfig());
        } catch (IOException e) {
            throw new ConfigChangeException("Failed to update instance files", e);
        }
    }

    private static void writeCassandraServerConfig(
            @NotNull Marker taskIdMarker,
            @NotNull CassandraFrameworkProtos.CassandraServerRunTask cassandraNodeTask,
            @NotNull CassandraFrameworkProtos.CassandraServerConfig serverConfig) throws IOException {
        for (final CassandraFrameworkProtos.TaskFile taskFile : serverConfig.getTaskFilesList()) {
            final File file = new File(taskFile.getOutputPath());
            if (LOGGER.isDebugEnabled())
                LOGGER.debug(taskIdMarker, "Overwriting file {}", file);
            Files.createParentDirs(file);
            Files.write(taskFile.getData().toByteArray(), file);
        }

        modifyCassandraYaml(taskIdMarker, cassandraNodeTask);
        modifyCassandraEnvSh(taskIdMarker, cassandraNodeTask);
        modifyCassandraRackdc(taskIdMarker, cassandraNodeTask, serverConfig);
    }

    @NotNull
    private static String processBuilderToString(@NotNull final ProcessBuilder builder) {
        return "ProcessBuilder{\n" +
                "directory() = " + builder.directory() + ",\n" +
                "command() = " + Joiner.on(" ").join(builder.command()) + ",\n" +
                "environment() = " + Joiner.on("\n").withKeyValueSeparator("->").join(builder.environment()) + "\n}";
    }

    private static void modifyCassandraRackdc(Marker taskIdMarker, CassandraFrameworkProtos.CassandraServerRunTask cassandraNodeTask, CassandraFrameworkProtos.CassandraServerConfig serverConfig) throws IOException {

        LOGGER.info(taskIdMarker, "Building cassandra-rackdc.properties");

        Properties props = new Properties();
        props.put("dc", "DC1");
        props.put("rack", "RAC1");
        // Add a suffix to a datacenter name. Used by the Ec2Snitch and Ec2MultiRegionSnitch to append a string to the EC2 region name.
        //props.put("dc_suffix", "");
        // Uncomment the following line to make this snitch prefer the internal ip when possible, as the Ec2MultiRegionSnitch does.
        //props.put("prefer_local", "true");
        File cassandraRackDc = new File("apache-cassandra-" + cassandraNodeTask.getVersion() + "/conf/cassandra-rackdc.properties");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(cassandraRackDc))) {
            props.store(bw, "Created by Apache Mesos Cassandra framework");
        }
    }

    private static void modifyCassandraEnvSh(Marker taskIdMarker, CassandraFrameworkProtos.CassandraServerRunTask cassandraNodeTask) throws IOException {
        int jmxPort = 0;
        String localJmx = "yes";
        boolean noJmxAuth = false;
        for (CassandraFrameworkProtos.TaskEnv.Entry entry : cassandraNodeTask.getCassandraServerConfig().getTaskEnv().getVariablesList()) {
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

        File cassandraEnvSh = new File("apache-cassandra-" + cassandraNodeTask.getVersion() + "/conf/cassandra-env.sh");

        LOGGER.info(taskIdMarker, "Reading cassandra-env.sh");
        List<String> lines = Files.readLines(cassandraEnvSh, Charset.forName("UTF-8"));
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
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
            for (String line : lines)
                pw.println(line);
        }
    }

    @SuppressWarnings("unchecked")
    private static void modifyCassandraYaml(Marker taskIdMarker, CassandraFrameworkProtos.CassandraServerRunTask cassandraNodeTask) throws IOException {
        LOGGER.info(taskIdMarker, "Building cassandra.yaml");

        File cassandraYaml = new File("apache-cassandra-" + cassandraNodeTask.getVersion() + "/conf/cassandra.yaml");

        Yaml yaml = new Yaml();
        Map<String, Object> yamlMap;
        LOGGER.info(taskIdMarker, "Reading cassandra.yaml");
        try (BufferedReader br = new BufferedReader(new FileReader(cassandraYaml))) {
            yamlMap = (Map<String, Object>) yaml.load(br);
        }
        LOGGER.info(taskIdMarker, "Modifying cassandra.yaml");
        for (CassandraFrameworkProtos.TaskConfig.Entry entry : cassandraNodeTask.getCassandraServerConfig().getCassandraYamlConfig().getVariablesList()) {
            switch (entry.getName()) {
                case "seeds":
                    List<Map<String, Object>> seedProviderList = (List<Map<String, Object>>) yamlMap.get("seed_provider");
                    Map<String, Object> seedProviderMap = seedProviderList.get(0);
                    List<Map<String, Object>> parameters = (List<Map<String, Object>>) seedProviderMap.get("parameters");
                    Map<String, Object> parametersMap = parameters.get(0);
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
            StringWriter sw = new StringWriter();
            yaml.dump(yamlMap, sw);
            LOGGER.debug("cassandra.yaml result: {}", sw);
        }
        LOGGER.info(taskIdMarker, "Writing cassandra.yaml");
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(cassandraYaml))) {
            yaml.dump(yamlMap, bw);
        }
    }

}
