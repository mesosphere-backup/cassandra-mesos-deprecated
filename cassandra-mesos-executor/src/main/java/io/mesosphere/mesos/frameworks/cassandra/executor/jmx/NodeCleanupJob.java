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
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class NodeCleanupJob extends AbstractNodeJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeCleanupJob.class);
    private final ExecutorService executorService;
    private Future<?> cleanupFuture;

    public NodeCleanupJob(Protos.TaskID taskId, ExecutorService executorService) {
        super(taskId);

        this.executorService = executorService;
    }

    @Override
    public CassandraFrameworkProtos.ClusterJobType getType() {
        return CassandraFrameworkProtos.ClusterJobType.CLEANUP;
    }

    public boolean start(JmxConnect jmxConnect) {
        if (!super.start(jmxConnect)) {
            return false;
        }

        LOGGER.info("Initiated cleanup job for keyspaces {}", getRemainingKeyspaces());

        return true;
    }

    @Override
    public void startNextKeyspace() {
        final String keyspace = super.nextKeyspace();
        if (keyspace == null) {
            return;
        }

        cleanupFuture = executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    LOGGER.info("Starting cleanup on keyspace {}", keyspace);
                    keyspaceStarted();
                    List<String> cfNames = jmxConnect.getColumnFamilyNames(keyspace);
                    for (String cfName : cfNames) {
                        int status = jmxConnect.getStorageServiceProxy().forceKeyspaceCleanup(keyspace, cfName);
                        CompactionManager.AllSSTableOpStatus s = CompactionManager.AllSSTableOpStatus.SUCCESSFUL;
                        for (CompactionManager.AllSSTableOpStatus st : CompactionManager.AllSSTableOpStatus.values()) {
                            if (st.statusCode == status) {
                                s = st;
                            }
                        }
                        LOGGER.info("Cleanup of {}.{} returned with {}", keyspace, cfName, s);
                    }
                    keyspaceFinished("SUCCESS", keyspace);
                } catch (Exception e) {
                    LOGGER.error("Failed to cleanup keyspace " + keyspace, e);
                    keyspaceFinished("FAILURE", keyspace);
                } finally {
                    startNextKeyspace();
                }
            }
        });

        LOGGER.info("Submitted cleanup for keyspace {}", keyspace);
    }

    @Override
    public void close() {
        if (cleanupFuture != null) {
            cleanupFuture.cancel(true);
            cleanupFuture = null;
        }

        super.close();
    }

}
