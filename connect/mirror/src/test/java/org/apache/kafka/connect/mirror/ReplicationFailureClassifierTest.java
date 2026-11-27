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
import org.junit.jupiter.api.Test;

import static org.apache.kafka.connect.mirror.ReplicationFailureClassifier.Decision.DATA_LOSS;
import static org.apache.kafka.connect.mirror.ReplicationFailureClassifier.Decision.TOPIC_RESET;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicationFailureClassifierTest {

    private final ReplicationFailureClassifier classifier = new ReplicationFailureClassifier();

    @Test
    void classifiesAsTopicResetWhenTopicIdChanged() {
        Uuid oldId = Uuid.randomUuid();
        Uuid newId = Uuid.randomUuid();
        assertEquals(TOPIC_RESET, classifier.classify(oldId, newId));
    }

    @Test
    void classifiesAsDataLossWhenTopicIdUnchanged() {
        Uuid sameId = Uuid.randomUuid();
        assertEquals(DATA_LOSS, classifier.classify(sameId, sameId));
    }

    @Test
    void classifiesAsDataLossWhenCurrentIdCouldNotBeFetched() {
        Uuid oldId = Uuid.randomUuid();
        assertEquals(DATA_LOSS, classifier.classify(oldId, null));
    }

    @Test
    void classifiesAsDataLossWhenNoPreviousIdWasEverCached() {
        Uuid currentId = Uuid.randomUuid();
        assertEquals(DATA_LOSS, classifier.classify(null, currentId));
    }

    @Test
    void classifiesAsDataLossWhenBothIdsUnknown() {
        assertEquals(DATA_LOSS, classifier.classify(null, null));
    }
}