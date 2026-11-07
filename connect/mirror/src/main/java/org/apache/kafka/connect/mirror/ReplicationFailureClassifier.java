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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.Uuid;

/**
 * Decides whether an {@code OffsetOutOfRangeException} means genuine data loss or a
 * benign topic reset, based on whether the topic's UUID has changed.
 *
 * <p>Deliberately free of any Connect/consumer/AdminClient dependency, so it's
 * unit-testable without a broker; all I/O stays in {@link MirrorSourceTask}.
 */
public class ReplicationFailureClassifier {

    public enum Decision {
        TOPIC_RESET,
        DATA_LOSS
    }

    /**
     * @param previousTopicId last-cached UUID, or null if never observed
     * @param currentTopicId  freshly-fetched UUID, or null if the fetch failed
     */
    public Decision classify(Uuid previousTopicId, Uuid currentTopicId) {
        boolean bothKnownAndDifferent = previousTopicId != null
                && currentTopicId != null
                && !previousTopicId.equals(currentTopicId);

        if (bothKnownAndDifferent) {
            return Decision.TOPIC_RESET;
        }
        // Fail safe: same ID, or either ID unknown -> treat as data loss.
        return Decision.DATA_LOSS;
    }
}