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
import io.mesosphere.mesos.frameworks.cassandra.executor.BackupManager;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;

public class NodeRestoreJob extends AbstractNodeJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeRestoreJob.class);

    @NotNull
    private final ExecutorService executorService;
    @NotNull
    private final String backupDir;

    private final boolean truncateTables;

    private Future<?> restoreFeature;

    public NodeRestoreJob(
            @NotNull final Protos.TaskID taskId,
            @NotNull final String backupDir,
            final boolean truncateTables,
            @NotNull final ExecutorService executorService)
    {
        super(taskId);
        this.backupDir = backupDir;
        this.truncateTables = truncateTables;
        this.executorService = executorService;
    }

    @NotNull
    @Override
    public CassandraFrameworkProtos.ClusterJobType getType() {
        return CassandraFrameworkProtos.ClusterJobType.RESTORE;
    }

    public boolean start(@NotNull final JmxConnect jmxConnect) {
        if (!super.start(jmxConnect)) {
            return false;
        }

        LOGGER.info("Initiated restore from '{}' for keyspaces {}", backupDir, getRemainingKeyspaces());

        return true;
    }

    @Override
    public void startNextKeyspace() {
        final String keyspace = super.nextKeyspace();
        if (keyspace == null) {
            return;
        }

        restoreFeature = executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Starting restore on keyspace {}", keyspace);
                    keyspaceStarted();

                    final BackupManager backupManager = new BackupManager(checkNotNull(jmxConnect), backupDir);
                    backupManager.restore(keyspace, truncateTables);

                    keyspaceFinished("SUCCESS", keyspace);
                } catch (final Exception e) {
                    LOGGER.error("Failed to restore keyspace " + keyspace, e);
                    keyspaceFinished("FAILURE", keyspace);
                } finally {
                    startNextKeyspace();
                }
            }
        });

        LOGGER.info("Submitted restore for keyspace {}", keyspace);
    }

    @Override
    public void close() {
        if (restoreFeature != null) {
            restoreFeature.cancel(true);
            restoreFeature = null;
        }

        super.close();
    }
}
