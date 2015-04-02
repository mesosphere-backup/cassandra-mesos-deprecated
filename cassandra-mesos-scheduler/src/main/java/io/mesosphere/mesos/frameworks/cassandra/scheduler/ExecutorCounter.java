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
package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;

import static io.mesosphere.mesos.frameworks.cassandra.scheduler.util.Futures.await;

public final class ExecutorCounter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorCounter.class);

    public static final String VARIABLE_NAME = "executor-counter";
    @NotNull
    private final State state;
    private final long defaultValue;

    @NotNull
    private Variable var;

    public ExecutorCounter(final @NotNull State state, final long defaultValue) {
        this.state = state;
        this.defaultValue = defaultValue;

        this.var = await(state.fetch(VARIABLE_NAME));
    }

    public long get() {
        LOGGER.debug("> get()");
        final byte[] value = var.value();
        final long retVal;
        if (value.length > 0) {
            retVal = new BigInteger(value).longValue();
        } else {
            retVal = defaultValue;
        }
        LOGGER.trace("< get() = {}", retVal);
        return retVal;
    }

    public long getAndIncrement() {
        LOGGER.debug("> getAndIncrement()");
        final long l = get();
        final long increment = l + 1;
        var = setValue(BigInteger.valueOf(increment));
        LOGGER.trace("< getAndIncrement() = {}", l);
        return l;
    }

    private Variable setValue(@NotNull final BigInteger newValue) {
        return await(state.store(var.mutate(newValue.toByteArray())));
    }
}
