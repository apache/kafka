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

import java.util.Collection;
import org.apache.kafka.common.TopicPartition;

/**
 * See {@link StoreChangelogReader}.
 */
public interface ChangelogRegister {
    /**
     * Register a state store for restoration.
     *
     * @param partition the state store's changelog partition for restoring
     * @param stateManager the state manager used for restoring (one per task)
     */
    void register(final TopicPartition partition, final ProcessorStateManager stateManager);

    /**
     * Unregisters and removes the passed in partitions from the set of changelogs
     * @param removedPartitions the set of partitions to remove
     */
    void unregister(final Collection<TopicPartition> removedPartitions);
}
