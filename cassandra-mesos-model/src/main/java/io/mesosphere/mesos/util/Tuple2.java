package io.mesosphere.mesos.util;

import lombok.Value;
import org.jetbrains.annotations.NotNull;

@Value
public final class Tuple2<T1, T2> {

    @NotNull
    public final T1 _1;
    @NotNull
    public final T2 _2;

    @NotNull
    public static <T1, T2> Tuple2<T1, T2> tuple2(@NotNull final T1 t1, @NotNull final T2 t2) {
        return new Tuple2<>(t1, t2);
    }

}
