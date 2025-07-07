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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.apache.kafka.common.TopicIdPartition;

import java.util.HashMap;
import java.util.Map;

public class ShareAcknowledgementCommitCallbackEvent extends BackgroundEvent {

    private final Map<TopicIdPartition, Acknowledgements> acknowledgementsMap;

    public ShareAcknowledgementCommitCallbackEvent(Map<TopicIdPartition, Acknowledgements> acknowledgementsMap) {
        super(Type.SHARE_ACKNOWLEDGEMENT_COMMIT_CALLBACK);
        this.acknowledgementsMap = new HashMap<>(acknowledgementsMap);
    }

    public Map<TopicIdPartition, Acknowledgements> acknowledgementsMap() {
        return acknowledgementsMap;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", acknowledgementsMap=" + acknowledgementsMap;
    }
}