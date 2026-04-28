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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class AcknowledgementCommitCallbackHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AcknowledgementCommitCallbackHandler.class);
    private final AcknowledgementCommitCallback acknowledgementCommitCallback;
    private boolean enteredCallback = false;

    AcknowledgementCommitCallbackHandler(AcknowledgementCommitCallback acknowledgementCommitCallback) {
        this.acknowledgementCommitCallback = acknowledgementCommitCallback;
    }

    public boolean hasEnteredCallback() {
        return enteredCallback;
    }

    void onComplete(List<Map<TopicIdPartition, Acknowledgements>> acknowledgementsMapList) {
        acknowledgementsMapList.forEach(acknowledgementsMap -> acknowledgementsMap.forEach((partition, acknowledgements) -> {
            KafkaException exception = acknowledgements.getAcknowledgeException();
            Set<Long> offsets = acknowledgements.getAcknowledgementsTypeMap().keySet();
            Set<Long> offsetsCopy = Set.copyOf(offsets);
            enteredCallback = true;
            try {
                acknowledgementCommitCallback.onComplete(Map.of(partition, offsetsCopy), exception);
            } catch (Exception e) {
                LOG.error("Exception thrown by acknowledgement commit callback", e);
            } finally {
                enteredCallback = false;
            }
        }));
    }
}
