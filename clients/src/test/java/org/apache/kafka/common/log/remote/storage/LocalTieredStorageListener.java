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
package org.apache.kafka.common.log.remote.storage;

import org.apache.kafka.common.*;
import org.slf4j.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Used to notify users of change in the local storage, such as the addition of a directory,
 * a segment or offset file.
 *
 * Unlike {@link LocalTieredStorageTraverser}, the intent is to generate instant notification,
 * rather than walking the directory structure of the storage at a given point in time.
 */
public interface LocalTieredStorageListener {

    /**
     * Called when the directory hosting segments for a topic-partition has been created.
     */
    void onTopicPartitionCreated(TopicPartition topicPartition);

    /**
     * Called when a segment has been copied to the local remote storage.
     *
     * @param remoteFileset The set of files offloaded for the segment, including indexes.
     */
    void onSegmentOffloaded(RemoteLogSegmentFileset remoteFileset);

    /**
     * Delegates to a list of listeners in insertion order.
     * Failures (escaped non-error exceptions) of one listener does not prevent execution of the next ones in the list.
     */
    final class LocalTieredStorageListeners implements LocalTieredStorageListener {
        private static final Logger LOGGER = LoggerFactory.getLogger(LocalTieredStorageListener.class);
        private final List<LocalTieredStorageListener> listeners = new CopyOnWriteArrayList<>();

        public void add(final LocalTieredStorageListener listener) {
            listeners.add(Objects.requireNonNull(listener));
        }

        @Override
        public void onTopicPartitionCreated(final TopicPartition topicPartition) {
            for (final LocalTieredStorageListener listener: listeners) {
                try {
                    listener.onTopicPartitionCreated(topicPartition);

                } catch (Exception e) {
                    LOGGER.error("Caught failure from listener", e);
                }
            }
        }

        @Override
        public void onSegmentOffloaded(RemoteLogSegmentFileset remoteFileset) {
            for (final LocalTieredStorageListener listener: listeners) {
                try {
                    listener.onSegmentOffloaded(remoteFileset);

                } catch (Exception e) {
                    LOGGER.error("Caught failure from listener", e);
                }
            }
        }
    }
}
