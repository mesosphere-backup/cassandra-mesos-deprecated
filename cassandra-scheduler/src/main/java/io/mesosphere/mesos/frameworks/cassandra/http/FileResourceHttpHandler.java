package io.mesosphere.mesos.frameworks.cassandra.http;

import com.google.common.io.Files;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.Methods;
import org.jetbrains.annotations.NotNull;

import java.io.File;

public final class FileResourceHttpHandler implements HttpHandler {

    @NotNull
    private final String fileName;
    @NotNull
    private final String contentType;
    @NotNull
    private final File resource;

    public FileResourceHttpHandler(@NotNull final String fileName, @NotNull final String contentType, @NotNull final String resourcePath) {
        this.fileName = fileName;
        this.contentType = contentType;
        final File file = new File(resourcePath);
        if (!file.exists() || !file.canRead()) {
            throw new IllegalArgumentException("Unable to read specified resource: " + resourcePath + " for file: " + fileName);
        }
        this.resource = file;
    }

    @Override
    public void handleRequest(final HttpServerExchange exchange) throws Exception {
        if (exchange.getRequestMethod().equals(Methods.GET)) {
            final HeaderMap headers = exchange.getResponseHeaders();
            headers.put(Headers.CONTENT_TYPE, contentType);
            headers.put(Headers.CONTENT_DISPOSITION, String.format("attachment; filename=\"%s\"", fileName));
            exchange.getResponseSender().send(Files.map(resource));
        } else {
            exchange.setResponseCode(405).getResponseSender().close();
        }
    }
}
