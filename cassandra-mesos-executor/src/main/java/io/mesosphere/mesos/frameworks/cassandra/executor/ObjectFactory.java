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
package io.mesosphere.mesos.frameworks.cassandra.executor;

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.JmxConnect;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Marker;

public interface ObjectFactory {

    JmxConnect newJmxConnect(CassandraFrameworkProtos.JmxConnect jmx);

    WrappedProcess launchCassandraNodeTask(@NotNull Marker taskIdMarker,
                                           @NotNull CassandraFrameworkProtos.CassandraServerRunTask cassandraServerRunTask) throws LaunchNodeException;

    void updateCassandraServerConfig(@NotNull Marker taskIdMarker,
                                     @NotNull CassandraFrameworkProtos.CassandraServerRunTask cassandraServerRunTask,
                                     @NotNull CassandraFrameworkProtos.UpdateConfigTask updateConfigTask) throws ConfigChangeException;
}
