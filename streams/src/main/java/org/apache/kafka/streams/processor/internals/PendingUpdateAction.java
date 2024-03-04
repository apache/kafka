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

import org.apache.kafka.common.TopicPartition;

import java.util.Objects;
import java.util.Set;

public class PendingUpdateAction {

    enum Action {
        UPDATE_INPUT_PARTITIONS,
        CLOSE_REVIVE_AND_UPDATE_INPUT_PARTITIONS,
        RECYCLE,
        SUSPEND,
        ADD_BACK,
        CLOSE_CLEAN
    }

    private final Set<TopicPartition> inputPartitions;
    private final Action action;

    private PendingUpdateAction(final Action action, final Set<TopicPartition> inputPartitions) {
        this.action = action;
        this.inputPartitions = inputPartitions;
    }

    private PendingUpdateAction(final Action action) {
        this(action, null);
    }

    public static PendingUpdateAction createUpdateInputPartition(final Set<TopicPartition> inputPartitions) {
        Objects.requireNonNull(inputPartitions, "Set of input partitions to update is null!");
        return new PendingUpdateAction(Action.UPDATE_INPUT_PARTITIONS, inputPartitions);
    }

    public static PendingUpdateAction createCloseReviveAndUpdateInputPartition(final Set<TopicPartition> inputPartitions) {
        Objects.requireNonNull(inputPartitions, "Set of input partitions to update is null!");
        return new PendingUpdateAction(Action.CLOSE_REVIVE_AND_UPDATE_INPUT_PARTITIONS, inputPartitions);
    }

    public static PendingUpdateAction createRecycleTask(final Set<TopicPartition> inputPartitions) {
        Objects.requireNonNull(inputPartitions, "Set of input partitions to update is null!");
        return new PendingUpdateAction(Action.RECYCLE, inputPartitions);
    }

    public static PendingUpdateAction createSuspend() {
        return new PendingUpdateAction(Action.SUSPEND);
    }

    public static PendingUpdateAction createAddBack() {
        return new PendingUpdateAction(Action.ADD_BACK);
    }

    public static PendingUpdateAction createCloseClean() {
        return new PendingUpdateAction(Action.CLOSE_CLEAN);
    }

    public Set<TopicPartition> getInputPartitions() {
        if (action != Action.UPDATE_INPUT_PARTITIONS && action != Action.CLOSE_REVIVE_AND_UPDATE_INPUT_PARTITIONS && action != Action.RECYCLE) {
            throw new IllegalStateException("Action type " + action + " does not have a set of input partitions!");
        }
        return inputPartitions;
    }

    public Action getAction() {
        return action;
    }

    @Override
    public String toString() {
        return "PendingUpdateAction{" +
            "inputPartitions=" + inputPartitions +
            ", action=" + action +
            '}';
    }
}