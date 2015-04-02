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
package io.mesosphere.mesos.frameworks.cassandra.scheduler.util;

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
