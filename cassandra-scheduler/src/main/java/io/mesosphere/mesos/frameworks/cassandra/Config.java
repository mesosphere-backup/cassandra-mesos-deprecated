package io.mesosphere.mesos.frameworks.cassandra;

import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public final class Config {

    @NotNull
    @SuppressWarnings("unchecked")
    public String getCassandraYaml() {
        try {
            final File cassandraYamlFile = new File("/home/ben.whitehead/opt/cassandra/apache-cassandra-2.1.0/conf/cassandra.yaml");
            final FileInputStream is = new FileInputStream(cassandraYamlFile);

            final Yaml yaml = new Yaml();
            final LinkedHashMap<String, Object> cassandraYaml = (LinkedHashMap<String, Object>) yaml.load(is);

            final ArrayList<LinkedHashMap<String, Object>> seedProvider = (ArrayList<LinkedHashMap<String,Object>>) cassandraYaml.get("seed_provider");
            final LinkedHashMap<String, Object> seedProviderMap = seedProvider.get(0);
            final ArrayList<LinkedHashMap<String, Object>> parameters = (ArrayList<LinkedHashMap<String, Object>>) seedProviderMap.get("parameters");
            final LinkedHashMap<String, Object> parameters0 = parameters.get(0);

            parameters0.put("seeds", "127.0.0.2");

            return yaml.dump(cassandraYaml);
        } catch (final FileNotFoundException e) {
            return "";
        }
    }
}
