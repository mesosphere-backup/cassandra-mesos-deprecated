package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Joiner;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Please not this object is not thread and should not be used from more than one thread.
 */
public final class CassandraYaml {
    private static final ClasspathLoadingShim CLASSPATH_LOADING_SHIM = new ClasspathLoadingShim();

    private static final Joiner joiner = Joiner.on(",");
    
    @NotNull
    private final Yaml yaml;
    @NotNull
    private final LinkedHashMap<String, Object> yamlMap;

    @SuppressWarnings("unchecked")
    private CassandraYaml(@NotNull final InputStream is) {
        yaml = new Yaml();
        yamlMap = (LinkedHashMap<String, Object>) yaml.load(is);
    }

    @NotNull
    public static CassandraYaml defaultCassandraYaml() {
        try (final InputStream is = CLASSPATH_LOADING_SHIM.getClass().getResourceAsStream("/cassandra.yaml")) {
            return new CassandraYaml(is);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    public CassandraYaml setClusterName(@NotNull final String clusterName) {
        yamlMap.put("cluster_name", clusterName);
        return this;
    }

    @NotNull
    public CassandraYaml setListenAddress(@NotNull final String listenAddress) {
        yamlMap.put("listen_address", listenAddress);
        return this;
    }

    @NotNull
    public CassandraYaml setRpcAddress(@NotNull final String listenAddress) {
        yamlMap.put("rpc_address", listenAddress);
        return this;
    }

    @NotNull
    @SuppressWarnings("unchecked")
    public CassandraYaml setSeeds(@NotNull final List<String> seeds) {
        final ArrayList<LinkedHashMap<String, Object>> seedProvider = (ArrayList<LinkedHashMap<String, Object>>) yamlMap.get("seed_provider");
        final LinkedHashMap<String, Object> seedProviderMap = seedProvider.get(0);
        final ArrayList<LinkedHashMap<String, Object>> parameters = (ArrayList<LinkedHashMap<String, Object>>) seedProviderMap.get("parameters");
        final LinkedHashMap<String, Object> parameters0 = parameters.get(0);

        parameters0.put("seeds", joiner.join(seeds));
        return this;
    }

    @NotNull
    public String dump() {
        return yaml.dump(yamlMap);
    }

    /**
     * A dummy class that can be instantiated inside the classloader that loaded CassandraYaml
     * so that we can then look up classpath resources from the classloader.
     */
    private static final class ClasspathLoadingShim {}
}
