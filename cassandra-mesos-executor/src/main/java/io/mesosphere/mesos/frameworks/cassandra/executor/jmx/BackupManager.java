package io.mesosphere.mesos.frameworks.cassandra.executor.jmx;

import org.apache.cassandra.service.StorageServiceMBean;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class BackupManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(BackupManager.class);
    private static final List<String> IGNORED_KEYSPACES = Arrays.asList("system", "system_traces");

    @NotNull
    private final JmxConnect jmxConnect;

    @NotNull
    private final String backupDir;

    public BackupManager(@NotNull final JmxConnect jmxConnect, @NotNull final String backupDir) {
        this.jmxConnect = jmxConnect;
        this.backupDir = backupDir;
    }

    public void backup(@NotNull final String keyspace, @NotNull final String backupName) throws IOException {
        if (IGNORED_KEYSPACES.contains(keyspace)) return;
        final StorageServiceMBean storageServiceProxy = jmxConnect.getStorageServiceProxy();

        LOGGER.info("Creating snapshot of keyspace {}", keyspace);
        storageServiceProxy.takeSnapshot(backupName, keyspace);

        LOGGER.info("Copying backup of keyspace {}", keyspace);
        copyKeyspaceSnapshot(backupName, keyspace);

        LOGGER.info("Clearing snapshot of keyspace {}", keyspace);
        storageServiceProxy.clearSnapshot(backupName, keyspace);
    }

    private void copyKeyspaceSnapshot(final String backupName, final String keyspace) throws IOException {
        final List<String> tables = jmxConnect.getColumnFamilyNames(keyspace);
        for (final String table : tables)
            copyTableSnapshot(backupName, keyspace, table);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void copyTableSnapshot(final String backupName, final String keyspace, final String table) throws IOException {
        final File srcDir = findTableSnapshotDir(keyspace, table, backupName);
        final File destDir = new File(backupDir, backupName + "/" + keyspace + "/" + table);
        destDir.mkdirs();

        final File[] files = srcDir.listFiles();
        if (files != null) {
            for (final File file : files) {
                if (file.isFile()) {
                    Files.copy(file.toPath(), new File(destDir, file.getName()).toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }

    private File findTableSnapshotDir(final String keyspace, final String table, final String backupName) {
        final File dataDir = new File(jmxConnect.getStorageServiceProxy().getAllDataFileLocations()[0]);
        final File keyspaceDir = new File(dataDir, keyspace);

        final File tableDir = findTableDir(keyspaceDir, table);
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

    public void restore(@NotNull final String keyspace, @NotNull final String backupName) throws IOException, TimeoutException {
        if (IGNORED_KEYSPACES.contains(keyspace)) return;
        final List<String> tables = jmxConnect.getColumnFamilyNames(keyspace);

        for (final String table : tables) {
            LOGGER.info("Truncating {}/{}", keyspace, table);
            jmxConnect.getStorageServiceProxy().truncate(keyspace, table);

            LOGGER.info("Restoring backup of {}/{}", keyspace, table);
            restoreTableSnapshot(backupName, keyspace, table);

            LOGGER.info("Reloading SSTables for {}/{}", keyspace, table);
            jmxConnect.getColumnFamilyStoreProxy(keyspace, table).loadNewSSTables();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void restoreTableSnapshot(final String backupName, final String keyspace, final String table) throws IOException {
        final File dataDir = new File(jmxConnect.getStorageServiceProxy().getAllDataFileLocations()[0]);
        final File keyspaceDir = new File(dataDir, keyspace);

        final File srcDir = new File(backupDir, backupName + "/" + keyspace + "/" + table);
        final File destDir = findTableDir(keyspaceDir, table);
        destDir.mkdirs();

        final File[] files = srcDir.listFiles();
        if (files != null) {
            for (final File file : files) {
                if (file.isFile()) {
                    Files.copy(file.toPath(), new File(destDir, file.getName()).toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }
    }
}
