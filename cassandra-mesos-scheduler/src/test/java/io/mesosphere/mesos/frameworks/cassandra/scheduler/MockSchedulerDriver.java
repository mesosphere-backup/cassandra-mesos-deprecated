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
package io.mesosphere.mesos.frameworks.cassandra.scheduler;

import com.google.protobuf.InvalidProtocolBufferException;
import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import io.mesosphere.mesos.util.Tuple2;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class MockSchedulerDriver implements SchedulerDriver {
    @NotNull
    protected final Scheduler scheduler;
    @NotNull
    private final Protos.MasterInfo masterInfo;
    @NotNull
    private Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> launchTasks = clearLaunchTasks();
    @NotNull
    private Collection<Tuple2<Protos.ExecutorID, CassandraFrameworkProtos.TaskDetails>> submitTasks = clearSubmitTasks();
    @NotNull
    private Collection<Protos.TaskID> killTasks = clearKillTasks();

    @NotNull
    private List<Protos.OfferID> declinedOffers = new ArrayList<>();

    public MockSchedulerDriver(@NotNull final Scheduler scheduler) {
        this.scheduler = scheduler;
        masterInfo = Protos.MasterInfo.newBuilder()
                .setHostname("127.0.0.1")
                .setId(UUID.randomUUID().toString())
                .setIp(0x7f000001)
                .setPort(42)
                .build();
    }

    @Override
    public Protos.Status launchTasks(final Collection<Protos.OfferID> offerIds, final Collection<Protos.TaskInfo> tasks, final Protos.Filters filters) {
        launchTasks = Tuple2.tuple2(offerIds, tasks);
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status killTask(final Protos.TaskID taskId) {
        killTasks.add(taskId);
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status declineOffer(final Protos.OfferID offerId, final Protos.Filters filters) {
        declinedOffers.add(offerId);
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status sendFrameworkMessage(final Protos.ExecutorID executorId, final Protos.SlaveID slaveId, final byte[] data) {
        try {
            submitTasks.add(Tuple2.tuple2(executorId, CassandraFrameworkProtos.TaskDetails.parseFrom(data)));
        } catch (final InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        return Protos.Status.DRIVER_RUNNING;
    }

    //

    @Override
    public Protos.Status start() {
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status stop(final boolean failover) {
        return Protos.Status.DRIVER_STOPPED;
    }

    @Override
    public Protos.Status stop() {
        return Protos.Status.DRIVER_STOPPED;
    }

    @Override
    public Protos.Status abort() {
        return Protos.Status.DRIVER_ABORTED;
    }

    @Override
    public Protos.Status join() {
        // this is a blocking operation
        throw new UnsupportedOperationException();
    }

    @Override
    public Protos.Status run() {
        start();
        return join();
    }

    @Override
    public Protos.Status requestResources(final Collection<Protos.Request> requests) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Protos.Status launchTasks(final Collection<Protos.OfferID> offerIds, final Collection<Protos.TaskInfo> tasks) {
        return launchTasks(offerIds, tasks, Protos.Filters.getDefaultInstance());
    }

    @Override
    public Protos.Status launchTasks(final Protos.OfferID offerId, final Collection<Protos.TaskInfo> tasks, final Protos.Filters filters) {
        return launchTasks(Collections.singleton(offerId), tasks, filters);
    }

    @Override
    public Protos.Status launchTasks(final Protos.OfferID offerId, final Collection<Protos.TaskInfo> tasks) {
        return launchTasks(Collections.singleton(offerId), tasks);
    }

    @Override
    public Protos.Status declineOffer(final Protos.OfferID offerId) {
        return declineOffer(offerId, Protos.Filters.getDefaultInstance());
    }

    @Override
    public Protos.Status reviveOffers() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Protos.Status reconcileTasks(final Collection<Protos.TaskStatus> statuses) {
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status acknowledgeStatusUpdate(final Protos.TaskStatus status) {
        return Protos.Status.DRIVER_RUNNING;
    }

    public void callRegistered(@NotNull final Protos.FrameworkID frameworkId) {
        scheduler.registered(this, frameworkId, masterInfo);
    }

    public void callReRegistered() {
        scheduler.reregistered(this, masterInfo);
    }

    @NotNull
    public List<Protos.OfferID> declinedOffers() {
        try {
            return declinedOffers;
        } finally {
            declinedOffers = new ArrayList<>();
        }
    }

    @NotNull
    public Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> launchTasks() {
        try {
            return launchTasks;
        } finally {
            launchTasks = clearLaunchTasks();
        }
    }

    @NotNull
    public Collection<Protos.TaskID> killTasks() {
        try {
            return killTasks;
        } finally {
            killTasks = clearKillTasks();
        }
    }

    @NotNull
    public Collection<Tuple2<Protos.ExecutorID, CassandraFrameworkProtos.TaskDetails>> submitTasks() {
        try {
            return submitTasks;
        } finally {
            submitTasks = clearSubmitTasks();
        }
    }

    @NotNull
    private static Tuple2<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>> clearLaunchTasks() {
        return Tuple2.<Collection<Protos.OfferID>, Collection<Protos.TaskInfo>>tuple2(Collections.<Protos.OfferID>emptyList(), Collections.<Protos.TaskInfo>emptyList());
    }

    @NotNull
    private static Collection<Tuple2<Protos.ExecutorID, CassandraFrameworkProtos.TaskDetails>> clearSubmitTasks() {
        return new ArrayList<>();
    }

    @NotNull
    private static Collection<Protos.TaskID> clearKillTasks() {
        return new ArrayList<>();
    }
}
