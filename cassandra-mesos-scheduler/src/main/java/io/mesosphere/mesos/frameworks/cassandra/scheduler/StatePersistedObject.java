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

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.mesosphere.mesos.frameworks.cassandra.scheduler.util.Futures.await;

public abstract class StatePersistedObject<A> {

    @NotNull
    private final State state;
    @NotNull
    private final A defaultValue;
    @NotNull
    private final Function<byte[], A> deserializer;
    @NotNull
    private final Function<A, byte[]> serializer;

    @NotNull
    private Variable var;

    @Nullable
    private A parsedValue = null;


    public StatePersistedObject(
        @NotNull final String variableName,
        @NotNull final State state,
        @NotNull final Supplier<A> defaultValue,
        @NotNull final Function<byte[], A> deserializer,
        @NotNull final Function<A, byte[]> serializer
    ) {
        this.deserializer = deserializer;
        this.serializer = serializer;
        this.defaultValue = defaultValue.get();
        this.state = state;
        this.var = await(state.fetch(variableName));
    }

    @NotNull
    public final A get() {
        if (parsedValue == null) {
            parsedValue = parseValue();
        }
        return checkNotNull(parsedValue);
    }

    private A parseValue() {
        final byte[] value = var.value();
        final A retVal;
        if (value.length > 0) {
            retVal = deserializer.apply(value);
        } else {
            final A newValue = defaultValue;
            var = store(newValue);
            retVal = newValue;
        }
        return retVal;
    }

    protected final void setValue(@NotNull final A newValue) {
        var = store(newValue);
        parsedValue = newValue;
    }

    private Variable store(final A newValue) {
        return await(state.store(var.mutate(serializer.apply(newValue))));
    }
}
