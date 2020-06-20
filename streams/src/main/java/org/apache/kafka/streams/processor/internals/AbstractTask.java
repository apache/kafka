/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;

import java.util.Collection;
import java.util.Set;
import org.slf4j.Logger;

import static org.apache.kafka.streams.processor.internals.Task.State.CLOSED;
import static org.apache.kafka.streams.processor.internals.Task.State.CREATED;

public abstract class AbstractTask implements Task {
    private Task.State state = CREATED;
    protected Set<TopicPartition> inputPartitions;
    protected ProcessorTopology topology;

    protected final TaskId id;
    protected final StateDirectory stateDirectory;
    protected final ProcessorStateManager stateMgr;

    AbstractTask(final TaskId id,
                 final ProcessorTopology topology,
                 final StateDirectory stateDirectory,
                 final ProcessorStateManager stateMgr,
                 final Set<TopicPartition> inputPartitions) {
        this.id = id;
        this.stateMgr = stateMgr;
        this.topology = topology;
        this.inputPartitions = inputPartitions;
        this.stateDirectory = stateDirectory;
    }

    @Override
    public TaskId id() {
        return id;
    }

    @Override
    public Set<TopicPartition> inputPartitions() {
        return inputPartitions;
    }

    @Override
    public Collection<TopicPartition> changelogPartitions() {
        return stateMgr.changelogPartitions();
    }

    @Override
    public void markChangelogAsCorrupted(final Collection<TopicPartition> partitions) {
        stateMgr.markChangelogAsCorrupted(partitions);
    }

    @Override
    public StateStore getStore(final String name) {
        return stateMgr.getStore(name);
    }

    @Override
    public boolean isClosed() {
        return state() == State.CLOSED;
    }

    @Override
    public final Task.State state() {
        return state;
    }

    @Override
    public void revive() {
        if (state == CLOSED) {
            transitionTo(CREATED);
        } else {
            throw new IllegalStateException("Illegal state " + state() + " while reviving task " + id);
        }
    }

    final void transitionTo(final Task.State newState) {
        final State oldState = state();

        if (oldState.isValidTransition(newState)) {
            state = newState;
        } else {
            throw new IllegalStateException("Invalid transition from " + oldState + " to " + newState);
        }
    }

    static void executeAndMaybeSwallow(final boolean clean,
                                       final Runnable runnable,
                                       final String name,
                                       final Logger log) {
        try {
            runnable.run();
        } catch (final RuntimeException e) {
            if (clean) {
                throw e;
            } else {
                log.debug("Ignoring error in unclean {}", name);
            }
        }
    }

    @Override
    public void update(final Set<TopicPartition> topicPartitions, final Map<String, List<String>> nodeToSourceTopics) {
        this.inputPartitions = topicPartitions;
        topology.updateSourceTopics(nodeToSourceTopics);
    }
}
