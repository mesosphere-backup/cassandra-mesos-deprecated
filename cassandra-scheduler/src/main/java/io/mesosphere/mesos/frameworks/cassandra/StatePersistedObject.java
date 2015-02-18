package io.mesosphere.mesos.frameworks.cassandra;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.mesosphere.mesos.frameworks.cassandra.util.Futures.await;

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
    public A get() {
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
            retVal = defaultValue;
        }
        return retVal;
    }

    public void setValue(@NotNull final A newValue) {
        var = await(state.store(var.mutate(serializer.apply(newValue))));
        parsedValue = newValue;
    }
}
