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
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;

public class NodeBackupJob extends AbstractNodeJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeBackupJob.class);

    @NotNull
    private final ExecutorService executorService;
    @NotNull
    private final String backupDir;
    @NotNull
    private final String backupName;

    private Future<?> backupFuture;

    public NodeBackupJob(
            @NotNull final Protos.TaskID taskId,
            @NotNull final String backupDir,
            @NotNull final String backupName,
            @NotNull final ExecutorService executorService)
    {
        super(taskId);
        this.backupDir = backupDir;
        this.backupName = backupName;
        this.executorService = executorService;
    }

    @NotNull
    @Override
    public CassandraFrameworkProtos.ClusterJobType getType() {
        return CassandraFrameworkProtos.ClusterJobType.BACKUP;
    }

    public boolean start(@NotNull final JmxConnect jmxConnect) {
        if (!super.start(jmxConnect)) {
            return false;
        }

        LOGGER.info("Initiated backup '{}' into '{}' for keyspaces {}", backupName, backupDir, getRemainingKeyspaces());

        return true;
    }

    @Override
    public void startNextKeyspace() {
        final String keyspace = super.nextKeyspace();
        if (keyspace == null) {
            return;
        }

        backupFuture = executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Starting backup on keyspace {}", keyspace);
                    keyspaceStarted();
                    final StorageServiceMBean storageServiceProxy = checkNotNull(jmxConnect).getStorageServiceProxy();

                    LOGGER.info("Creating snapshot of keyspace {}", keyspace);
                    storageServiceProxy.takeSnapshot(backupName, keyspace);

                    LOGGER.info("Copying snapshot of keyspace {}", keyspace);
                    copyKeyspaceSnapshot(keyspace);

                    LOGGER.info("Clearing snapshot of keyspace {}", keyspace);
                    storageServiceProxy.clearSnapshot(backupName, keyspace);

                    keyspaceFinished("SUCCESS", keyspace);
                } catch (final Exception e) {
                    LOGGER.error("Failed to backup keyspace " + keyspace, e);
                    keyspaceFinished("FAILURE", keyspace);
                } finally {
                    startNextKeyspace();
                }
            }
        });

        LOGGER.info("Submitted backup for keyspace {}", keyspace);
    }

    private void copyKeyspaceSnapshot(final String keyspace) throws IOException {
        final List<String> tables = jmxConnect.getColumnFamilyNames(keyspace);
        for (final String table : tables)
            copyTableSnapshot(keyspace, table);
    }

    private void copyTableSnapshot(final String keyspace, final String table) throws IOException {
        final File srcDir = findTableSnapshotDir(keyspace, table, backupName);
        final File destDir = new File(backupDir, backupName + "/" + keyspace + "/" + table);

        destDir.mkdirs();

        for (final File file : srcDir.listFiles()) {
            if (file.isFile()) {
                Files.copy(file.toPath(), new File(destDir, file.getName()).toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }

    private File findTableSnapshotDir(final String keyspace, final String table, final String backupName) {
        final File dataDir = new File(jmxConnect.getStorageServiceProxy().getAllDataFileLocations()[0]);
        final File keySpaceDir = new File(dataDir, keyspace);

        final File tableDir = findTableDir(keySpaceDir, table);
        final File snapshotDir = new File(tableDir, "/snapshots/" + backupName);
        if (!snapshotDir.exists()) throw new IllegalStateException("Snapshot dir does not exist: " + snapshotDir);

        return snapshotDir;
    }

    private File findTableDir(final File keyspaceDir, final String table) {
        final File[] files = keyspaceDir.listFiles();
        if (files != null)
            for (final File file : files) {
                if (file.isDirectory() && file.getName().startsWith(table + "-")) {
                    return file;
                }
            }

        throw new IllegalStateException("Failed to found table dir for table " + table + " in keyspace " + keyspaceDir);
    }

    @Override
    public void close() {
        if (backupFuture != null) {
            backupFuture.cancel(true);
            backupFuture = null;
        }

        super.close();
    }
}
