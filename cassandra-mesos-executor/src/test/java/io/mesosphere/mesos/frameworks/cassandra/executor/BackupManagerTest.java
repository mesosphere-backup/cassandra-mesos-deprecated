package io.mesosphere.mesos.frameworks.cassandra.executor;

import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.JmxConnect;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BackupManagerTest {
    public static final String KEYSPACE = "k";
    public static final String TABLE = "t";
    public static final String SNAPSHOT = "s";

    private File dataDir;
    private File backupDir;

    @Mock
    private JmxConnect mockJmxConnect;
    @Mock
    private StorageServiceMBean mockStorageServiceMBean;
    @Mock
    private ColumnFamilyStoreMBean mockColumnFamilyStoreMBean;

    private BackupManager backupManager;

    @Before
    public void before() throws IOException {
        backupDir = Files.createTempDirectory(getClass().getSimpleName() + "-backup-").toFile();
        dataDir = Files.createTempDirectory(getClass().getSimpleName() + "-data-").toFile();

        MockitoAnnotations.initMocks(this);
        backupManager = new BackupManager(mockJmxConnect, backupDir.toString());
    }

    @After
    public void after() {
        delete(dataDir);
        delete(backupDir);
    }

    @Test
    public void testFindTableDir() throws IOException {
        File keyspaceDir = new File(dataDir, KEYSPACE);
        File tableDir = new File(keyspaceDir, TABLE + "-123");

        try { backupManager.findTableDir(keyspaceDir, TABLE); fail(); }
        catch (IllegalStateException e) {}

        Files.createDirectories(keyspaceDir.toPath());
        try { backupManager.findTableDir(keyspaceDir, TABLE); fail(); }
        catch (IllegalStateException e) {}

        Files.createDirectories(tableDir.toPath());
        File dir = backupManager.findTableDir(keyspaceDir, TABLE);
        assertEquals(tableDir, dir);
    }

    @Test
    public void testFindTableSnapshotDir() throws IOException {
        File dataDir = new File(this.dataDir.toURI());

        when(mockJmxConnect.getStorageServiceProxy()).thenReturn(mockStorageServiceMBean);
        when(mockStorageServiceMBean.getAllDataFileLocations()).thenReturn(new String[] {dataDir.toString()});
        try { backupManager.findTableSnapshotDir(KEYSPACE, TABLE, SNAPSHOT); fail(); }
        catch (IllegalStateException e) {}

        File snapshotDir = new File(dataDir, KEYSPACE + "/" + TABLE + "-123/snapshots/" + SNAPSHOT);
        Files.createDirectories(snapshotDir.toPath());

        File dir = backupManager.findTableSnapshotDir(KEYSPACE, TABLE, SNAPSHOT);
        assertEquals(snapshotDir, dir);
    }

    @Test
    public void testCopyTableSnapshot() throws IOException {
        createCassandraDirs(KEYSPACE, TABLE, SNAPSHOT, true);

        when(mockJmxConnect.getStorageServiceProxy()).thenReturn(mockStorageServiceMBean);
        when(mockStorageServiceMBean.getAllDataFileLocations()).thenReturn(new String[] {dataDir.toString()});
        backupManager.copyTableSnapshot(SNAPSHOT, KEYSPACE, TABLE);

        assertTrue(new File(backupDir, KEYSPACE + "/" + TABLE + "/data.db").exists());
        assertTrue(new File(backupDir, KEYSPACE + "/" + TABLE + "/index.db").exists());
    }

    @Test
    public void testBackup() throws IOException {
        createCassandraDirs(KEYSPACE, TABLE, SNAPSHOT, true);

        when(mockJmxConnect.getStorageServiceProxy()).thenReturn(mockStorageServiceMBean);
        when(mockStorageServiceMBean.getAllDataFileLocations()).thenReturn(new String[] {dataDir.toString()});
        when(mockJmxConnect.getColumnFamilyNames(KEYSPACE)).thenReturn(Arrays.asList(TABLE));
        backupManager.backup(KEYSPACE, SNAPSHOT);
        InOrder inOrder = inOrder(mockStorageServiceMBean);
        inOrder.verify(mockStorageServiceMBean).takeSnapshot(SNAPSHOT, KEYSPACE);
        inOrder.verify(mockStorageServiceMBean).getAllDataFileLocations();
        inOrder.verify(mockStorageServiceMBean).clearSnapshot(SNAPSHOT, KEYSPACE);

        assertTrue(new File(backupDir, KEYSPACE).isDirectory());
        assertTrue(new File(backupDir, KEYSPACE + "/" + TABLE).isDirectory());
        assertTrue(new File(backupDir, KEYSPACE + "/" + TABLE + "/data.db").isFile());
        assertTrue(new File(backupDir, KEYSPACE + "/" + TABLE + "/index.db").isFile());
    }

    @Test
    public void testRestoreTableSnapshot() throws IOException {
        createCassandraDirs(KEYSPACE, TABLE, SNAPSHOT, false);
        createBackupDirs(KEYSPACE, TABLE);

        when(mockJmxConnect.getStorageServiceProxy()).thenReturn(mockStorageServiceMBean);
        when(mockStorageServiceMBean.getAllDataFileLocations()).thenReturn(new String[] {dataDir.toString()});
        backupManager.restoreTableSnapshot(KEYSPACE, TABLE);
        verify(mockStorageServiceMBean).getAllDataFileLocations();

        assertTrue(new File(dataDir, KEYSPACE + "/" + TABLE + "-0/data.db").isFile());
        assertTrue(new File(dataDir, KEYSPACE + "/" + TABLE + "-0/index.db").isFile());
    }

    @Test
    public void testRestore() throws IOException, TimeoutException {
        createCassandraDirs(KEYSPACE, TABLE, SNAPSHOT, false);
        createBackupDirs(KEYSPACE, TABLE);

        when(mockJmxConnect.getColumnFamilyNames(KEYSPACE)).thenReturn(Arrays.asList(TABLE));
        when(mockJmxConnect.getColumnFamilyStoreProxy(KEYSPACE, TABLE)).thenReturn(mockColumnFamilyStoreMBean);
        when(mockJmxConnect.getStorageServiceProxy()).thenReturn(mockStorageServiceMBean);
        when(mockStorageServiceMBean.getAllDataFileLocations()).thenReturn(new String[] {dataDir.toString()});
        backupManager.restore(KEYSPACE);
        verify(mockColumnFamilyStoreMBean).loadNewSSTables();
        verify(mockStorageServiceMBean).getAllDataFileLocations();

        assertTrue(new File(dataDir, KEYSPACE + "/" + TABLE + "-0/data.db").isFile());
        assertTrue(new File(dataDir, KEYSPACE + "/" + TABLE + "-0/index.db").isFile());
    }

    private void createCassandraDirs(String keyspace, String table, String snapshot, boolean createFiles) throws IOException {
        File tableDir = new File(dataDir, keyspace + "/" + table + "-0");
        File snapshotDir = new File(tableDir, "snapshots/" + snapshot);
        assertTrue(snapshotDir.mkdirs());

        if (createFiles) {
            Files.createFile(new File(snapshotDir, "data.db").toPath());
            Files.createFile(new File(snapshotDir, "index.db").toPath());
        }
    }

    private void createBackupDirs(String keyspace, String table) throws IOException {
        File tableDir = new File(backupDir, keyspace + "/" + table);
        assertTrue(tableDir.mkdirs());

        Files.createFile(new File(tableDir, "data.db").toPath());
        Files.createFile(new File(tableDir, "index.db").toPath());
    }

    private void delete(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();

            if (children != null) {
                for (File child: children) {
                    delete(child);
                }
            }
        }

        assertTrue(file.delete());
    }
}
