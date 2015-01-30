package io.mesosphere.mesos.frameworks.cassandra;

import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public final class Config {

    @NotNull
    @SuppressWarnings("unchecked")
    public String getCassandraYaml() {
        final InputStream is = this.getClass().getResourceAsStream("/cassandra.yaml");

        final Yaml yaml = new Yaml();
        final LinkedHashMap<String, Object> cassandraYaml = (LinkedHashMap<String, Object>) yaml.load(is);

        final ArrayList<LinkedHashMap<String, Object>> seedProvider = (ArrayList<LinkedHashMap<String, Object>>) cassandraYaml.get("seed_provider");
        final LinkedHashMap<String, Object> seedProviderMap = seedProvider.get(0);
        final ArrayList<LinkedHashMap<String, Object>> parameters = (ArrayList<LinkedHashMap<String, Object>>) seedProviderMap.get("parameters");
        final LinkedHashMap<String, Object> parameters0 = parameters.get(0);

        parameters0.put("seeds", "127.0.0.2");

        return yaml.dump(cassandraYaml);
    }
}
