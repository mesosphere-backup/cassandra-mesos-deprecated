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
package io.mesosphere.mesos.frameworks.cassandra.executor.jmx;

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;

public class NodeTruncateJob extends AbstractNodeJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeTruncateJob.class);

    @NotNull
    private final ExecutorService executorService;
    private Future<?> truncateFuture;

    public NodeTruncateJob(
            @NotNull final Protos.TaskID taskId,
            @NotNull final ExecutorService executorService)
    {
        super(taskId);
        this.executorService = executorService;
    }

    @NotNull
    @Override
    public CassandraFrameworkProtos.ClusterJobType getType() {
        return CassandraFrameworkProtos.ClusterJobType.TRUNCATE;
    }

    public boolean start(@NotNull final JmxConnect jmxConnect) {
        if (!super.start(jmxConnect)) {
            return false;
        }

        LOGGER.info("Initiated truncate for keyspaces {}", getRemainingKeyspaces());
        return true;
    }

    @Override
    public void startNextKeyspace() {
        final String keyspace = super.nextKeyspace();
        if (keyspace == null) {
            return;
        }

        truncateFuture = executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Starting truncate on keyspace {}", keyspace);
                    keyspaceStarted();

                    final List<String> tables = checkNotNull(jmxConnect).getColumnFamilyNames(keyspace);

                    for (final String table : tables) {
                        LOGGER.info("Truncating {}/{}", keyspace, table);
                        checkNotNull(jmxConnect).getStorageServiceProxy().truncate(keyspace, table);
                    }

                    keyspaceFinished(SUCCESS, keyspace);
                } catch (final Exception e) {
                    LOGGER.error("Failed to truncate keyspace " + keyspace, e);
                    keyspaceFinished(FAILURE, keyspace);
                } finally {
                    startNextKeyspace();
                }
            }
        });

        LOGGER.info("Submitted truncate for keyspace {}", keyspace);
    }

    @Override
    public void close() {
        if (truncateFuture != null) {
            truncateFuture.cancel(true);
            truncateFuture = null;
        }

        super.close();
    }
}
