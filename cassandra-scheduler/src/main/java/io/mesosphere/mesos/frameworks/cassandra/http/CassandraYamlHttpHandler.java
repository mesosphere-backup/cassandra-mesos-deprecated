package io.mesosphere.mesos.frameworks.cassandra.http;

import io.mesosphere.mesos.frameworks.cassandra.Config;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.Methods;

import java.nio.ByteBuffer;

public final class CassandraYamlHttpHandler implements HttpHandler {
    private final Config config;

    public CassandraYamlHttpHandler(final Config config) {
        this.config = config;
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        if (exchange.getRequestMethod().equals(Methods.GET)) {
            final HeaderMap headers = exchange.getResponseHeaders();
            headers.put(Headers.CONTENT_TYPE, "application/x-yaml; charset=utf-8");
            headers.put(Headers.CONTENT_DISPOSITION, "attachment; filename=\"cassandra.yaml\"");
            exchange.getResponseSender().send(ByteBuffer.wrap(config.getCassandraYaml().getBytes("UTF-8")));
        } else {
            exchange.setResponseCode(405).getResponseSender().close();
        }
    }
}
