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

public class NodeRepairJob extends AbstractNodeJob implements NotificationListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeRepairJob.class);

    private final Map<Integer, String> commandToKeyspace = new HashMap<>();

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

        jmxConnect.getStorageServiceProxy().addNotificationListener(this, null, null);

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
            checkNotNull(jmxConnect).getStorageServiceProxy().removeNotificationListener(this);
            close();
        } catch (final ListenerNotFoundException ignored) {
            // ignore this
        }
    }

    @Override
    public void handleNotification(final Notification notification, final Object handback) {
        if (!"repair".equals(notification.getType())) {
            return;
        }

        final int[] result = (int[]) notification.getUserData();
        final int repairCommandNo = result[0];
        final ActiveRepairService.Status status = ActiveRepairService.Status.values()[result[1]];

        final String keyspace = commandToKeyspace.get(repairCommandNo);

        switch (status) {
            case STARTED:
                LOGGER.info("Received STARTED notification about repair for keyspace {} with cmd#{}, timetamp={}, message={}",
                        keyspace, repairCommandNo, notification.getTimeStamp(), notification.getMessage());
                keyspaceStarted();
                break;
            case SESSION_SUCCESS:
                LOGGER.debug("Received SESSION_SUCCESS notification about repair for keyspace {} with cmd#{}, timetamp={}, message={}",
                        keyspace, repairCommandNo, notification.getTimeStamp(), notification.getMessage());
                break;
            case SESSION_FAILED:
                LOGGER.warn("Received SESSION_FAILED notification about repair for keyspace {} with cmd#{}, timetamp={}, message={}",
                        keyspace, repairCommandNo, notification.getTimeStamp(), notification.getMessage());
                break;
            case FINISHED:
                LOGGER.info("Received FINISHED notification about repair for keyspace {} with cmd#{}, timetamp={}, message={}",
                        keyspace, repairCommandNo, notification.getTimeStamp(), notification.getMessage());

                keyspaceFinished(status.name(), keyspace);

                startNextKeyspace();

                break;
        }

        // TODO also allow StorageServiceMBean.forceTerminateAllRepairSessions

        /*
        TODO handle these, too !!!

        else if (JMXConnectionNotification.NOTIFS_LOST.equals(notification.getType()))
        {
            String message = String.format("[%s] Lost notification. You should check server log for repair status of keyspace %s",
                                           format.format(notification.getTimeStamp()),
                                           keyspace);
            out.println(message);
        }
        else if (JMXConnectionNotification.FAILED.equals(notification.getType())
                 || JMXConnectionNotification.CLOSED.equals(notification.getType()))
        {
            String message = String.format("JMX connection closed. You should check server log for repair status of keyspace %s"
                                           + "(Subsequent keyspaces are not going to be repaired).",
                                           keyspace);
            error = new IOException(message);
            condition.signalAll();
        }

         */
    }

}
