package io.mesosphere.mesos.frameworks.cassandra.executor;

import io.mesosphere.mesos.frameworks.cassandra.executor.jmx.JmxConnect;
import org.apache.cassandra.db.ColumnFamilyStoreMBean;
import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BackupManagerTest {
    public static final String KEYSPACE = "k";
    public static final String TABLE = "t";
    public static final String SNAPSHOT = "s";

    private File dataDir;
    private File backupDir;

    private BackupManagerTest.TestJmxConnect jmxConnect;
    private BackupManager backupManager;

    @Before
    public void before() throws IOException {
        backupDir = Files.createTempDirectory(getClass().getSimpleName() + "-backup-").toFile();
        dataDir = Files.createTempDirectory(getClass().getSimpleName() + "-data-").toFile();

        jmxConnect = new TestJmxConnect("" + dataDir);
        backupManager = new BackupManager(jmxConnect, "" + backupDir);
    }

    @After
    public void after() {
        delete(dataDir);
        delete(backupDir);
    }

    @Test
    public void testBackup() throws IOException {
        createCassandraDirs(KEYSPACE, TABLE, SNAPSHOT);

        backupManager.backup(KEYSPACE, SNAPSHOT);
        assertEquals(Arrays.asList("takeSnapshot", "clearSnapshot"), jmxConnect.getInvocations());

        assertTrue(new File(backupDir, KEYSPACE).isDirectory());
        assertTrue(new File(backupDir, KEYSPACE + "/" + TABLE).isDirectory());
        assertTrue(new File(backupDir, KEYSPACE + "/" + TABLE + "/data.db").isFile());
    }

    @Test
    public void testRestore() throws IOException, TimeoutException {
        createCassandraDirs(KEYSPACE, TABLE, SNAPSHOT);
        createBackupDirs(KEYSPACE, TABLE);

        backupManager.restore(KEYSPACE, true);
        assertEquals(Arrays.asList("truncate", "loadNewSSTables"), jmxConnect.getInvocations());
        assertTrue(new File(backupDir, KEYSPACE + "/" + TABLE + "/data.db").isFile());
    }

    private void createCassandraDirs(String keyspace, String table, String snapshot) throws IOException {
        File tableDir = new File(dataDir, keyspace + "/" + table + "-0");
        File snapshotDir = new File(tableDir, "snapshots/" + snapshot);
        assertTrue(snapshotDir.mkdirs());

        Files.createFile(new File(snapshotDir, "data.db").toPath());
    }

    private void createBackupDirs(String keyspace, String table) throws IOException {
        File tableDir = new File(backupDir, keyspace + "/" + table);
        assertTrue(tableDir.mkdirs());
        Files.createFile(new File(tableDir, "data.db").toPath());
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

    class TestJmxConnect implements JmxConnect {
        private String dataDir;
        private List<String> invocations = new ArrayList<String>();

        TestJmxConnect(String dataDir) {
            this.dataDir = dataDir;
        }

        public List<String> getInvocations() { return Collections.unmodifiableList(invocations); }

        @NotNull
        @Override
        public RuntimeMXBean getRuntimeProxy() {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public StorageServiceMBean getStorageServiceProxy() {
            return newStorageService();
        }

        @NotNull
        @Override
        public EndpointSnitchInfoMBean getEndpointSnitchInfoProxy() {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public ColumnFamilyStoreMBean getColumnFamilyStoreProxy(@NotNull String keyspace, @NotNull String table) {
            return newColumnFamilyStore();
        }

        @NotNull
        @Override
        public List<String> getColumnFamilyNames(@NotNull String keyspace) {
            return Arrays.asList(TABLE);
        }

        @Override
        public void close() throws IOException {}

        private StorageServiceMBean newStorageService() {
            return (StorageServiceMBean) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{StorageServiceMBean.class},
                    new InvocationHandler() {
                        @Override
                        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                            String name = method.getName();
                            switch (name) {
                                case "takeSnapshot":
                                    invocations.add("takeSnapshot");
                                    return null;
                                case "clearSnapshot":
                                    invocations.add("clearSnapshot");
                                    return null;
                                case "truncate":
                                    invocations.add("truncate");
                                    return null;
                                case "getAllDataFileLocations":
                                    return new String[]{dataDir};
                                default:
                                    throw new UnsupportedOperationException(name);

                            }
                        }
                    });
        }

        private ColumnFamilyStoreMBean newColumnFamilyStore() {
            return (ColumnFamilyStoreMBean) Proxy.newProxyInstance(getClass().getClassLoader(), new Class[]{ColumnFamilyStoreMBean.class},
                    new InvocationHandler() {
                        @Override
                        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                            String name = method.getName();
                            switch (name) {
                                case "loadNewSSTables":
                                    invocations.add("loadNewSSTables");
                                    return null;
                                default:
                                    throw new UnsupportedOperationException(name);

                            }
                        }
                    });
        }
    }
}
