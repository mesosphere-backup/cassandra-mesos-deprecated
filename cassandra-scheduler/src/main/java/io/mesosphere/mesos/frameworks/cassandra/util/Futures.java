package io.mesosphere.mesos.frameworks.cassandra.util;

import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class Futures {
    public Futures() {}

    /**
     * Will throw a RuntimeException wrapping any exception that may happen
     * java.util.concurrent.ExecutionException
     * java.lang.InterruptedException
     */
    public static <A> A await(@NotNull final Future<A> f) {
        try {
            return f.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Will throw a RuntimeException wrapping any exception that may happen
     * java.util.concurrent.ExecutionException
     * java.lang.InterruptedException
     * java.util.concurrent.TimeoutException
     */
    public static <A> A await(@NotNull final Future<A> f, @NotNull final Duration timeout) {
        try {
            return f.get(timeout.getMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
