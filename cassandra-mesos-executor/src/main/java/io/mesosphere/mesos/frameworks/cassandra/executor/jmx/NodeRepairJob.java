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
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.jmx.JMXNotificationProgressListener;
import org.apache.mesos.Protos;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationListener;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class NodeRepairJob extends AbstractNodeJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeRepairJob.class);

    private final Map<Integer, String> commandToKeyspace = new HashMap<>();

    private final NodeRepairJobNotificationListener notificationListener = new NodeRepairJobNotificationListener();

    public NodeRepairJob(final Protos.TaskID taskId) {
        super(taskId);
    }

    @NotNull
    @Override
    public CassandraFrameworkProtos.ClusterJobType getType() {
        return CassandraFrameworkProtos.ClusterJobType.REPAIR;
    }

    public boolean start(@NotNull final JmxConnect jmxConnect) {
        if (!super.start(jmxConnect)) {
            return false;
        }

        jmxConnect.getStorageServiceProxy().addNotificationListener(notificationListener, null, null);

        LOGGER.info("Initiated repair job for keyspaces {}", getRemainingKeyspaces());

        return true;
    }

    public void startNextKeyspace() {
        while (true) {
            final String keyspace = super.nextKeyspace();
            if (keyspace == null) {
                return;
            }

            LOGGER.info("Starting repair on keyspace {}", keyspace);

            // do 'nodetool repair' in local-DC and on node's primary range
            final int commandNo = checkNotNull(jmxConnect).getStorageServiceProxy().forceRepairAsync(keyspace, false, true, true, false);
            if (commandNo == 0) {
                LOGGER.info("Nothing to repair for keyspace {}", keyspace);
                continue;
            }

            commandToKeyspace.put(commandNo, keyspace);

            LOGGER.info("Submitted repair for keyspace {} with cmd#{}", keyspace, commandNo);
            break;
        }
    }

    protected void cleanupAfterJobFinished() {
        try {
            super.cleanupAfterJobFinished();
            checkNotNull(jmxConnect).getStorageServiceProxy().removeNotificationListener(notificationListener);
            close();
        } catch (final ListenerNotFoundException ignored) {
            // ignore this
        }
    }

    /**
     * Inner class for listening to progress notifications.
     */
    protected class NodeRepairJobNotificationListener extends JMXNotificationProgressListener {

        @Override
        public boolean isInterestedIn(@NotNull final String tag) {
            try {
                return commandToKeyspace.containsKey(tagToCommand(tag));
            } catch (ArrayIndexOutOfBoundsException e) {
                return false;
            } catch (NumberFormatException e) {
                return false;
            }
        }

        /**
         * Converts a tag of the form "repair:<cmd>" to a the command
         */
        protected int tagToCommand(@NotNull final String tag) throws ArrayIndexOutOfBoundsException, NumberFormatException {
            return Integer.parseInt(tag.split(":")[1]);
        }

        @Override
        public void progress(@NotNull final String tag, @NotNull final ProgressEvent event) {
            final int cmd = tagToCommand(tag);
            final String keyspace = commandToKeyspace.get(cmd);

            switch (event.getType()) {
                case COMPLETE:
                    LOGGER.info("Received COMPLETE notification about repair for keyspace {}", keyspace);
                    keyspaceFinished("FINISHED", keyspace);
                    startNextKeyspace();
                    break;
                case START:
                    LOGGER.info("Received START notification about repair for keyspace {}", keyspace);
                    keyspaceStarted();
                    break;
                case PROGRESS:
                    LOGGER.info("Received PROGRESS notification about repair for keyspace {}: {}/{}", keyspace, event.getProgressCount(), event.getTotal());
                    break;
            }
        }
    }
}
