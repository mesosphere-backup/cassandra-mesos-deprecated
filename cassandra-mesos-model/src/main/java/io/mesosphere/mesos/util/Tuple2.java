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
package io.mesosphere.mesos.util;

import org.jetbrains.annotations.NotNull;

public final class Tuple2<T1, T2> {

    @NotNull
    public final T1 _1;
    @NotNull
    public final T2 _2;

    private Tuple2(@NotNull final T1 _1, @NotNull final T2 _2) {
        this._1 = _1;
        this._2 = _2;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Tuple2 tuple2 = (Tuple2) o;

        if (!_1.equals(tuple2._1)) return false;
        if (!_2.equals(tuple2._2)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = _1.hashCode();
        result = 31 * result + _2.hashCode();
        return result;
    }

    @NotNull
    public static <T1, T2> Tuple2<T1, T2> tuple2(@NotNull final T1 t1, @NotNull final T2 t2) {
        return new Tuple2<>(t1, t2);
    }

}
