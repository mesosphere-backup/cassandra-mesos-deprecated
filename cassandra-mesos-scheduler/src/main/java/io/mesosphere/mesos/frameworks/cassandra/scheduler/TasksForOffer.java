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

import io.mesosphere.mesos.frameworks.cassandra.CassandraFrameworkProtos;
import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.List;

public class TasksForOffer {
    private final CassandraFrameworkProtos.CassandraNodeExecutor executor;
    private final List<CassandraFrameworkProtos.CassandraNodeTask> launchTasks;
    private final List<CassandraFrameworkProtos.TaskDetails> submitTasks;
    private final List<Protos.TaskID> killTasks;

    public TasksForOffer(CassandraFrameworkProtos.CassandraNodeExecutor executor) {
        this.executor = executor;
        this.launchTasks = new ArrayList<>();
        this.submitTasks = new ArrayList<>();
        this.killTasks = new ArrayList<>();
    }

    public CassandraFrameworkProtos.CassandraNodeExecutor getExecutor() {
        return executor;
    }

    public List<CassandraFrameworkProtos.CassandraNodeTask> getLaunchTasks() {
        return launchTasks;
    }

    public List<CassandraFrameworkProtos.TaskDetails> getSubmitTasks() {
        return submitTasks;
    }

    public List<Protos.TaskID> getKillTasks() {
        return killTasks;
    }

    public boolean hasAnyTask() {
        return !submitTasks.isEmpty() || !launchTasks.isEmpty() || !killTasks.isEmpty();
    }

    public boolean hasExecutor() {
        return executor != null;
    }
}
