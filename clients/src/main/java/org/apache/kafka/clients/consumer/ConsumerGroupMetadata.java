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
package org.apache.kafka.clients.consumer;

import java.util.Optional;

/**
 * A metadata struct containing the consumer group information.
 * Note: Any change to this class is considered public and requires a KIP.
 */
public class ConsumerGroupMetadata {
    final private String groupId;
    final private int generationId;
    final private String memberId;
    final Optional<String> groupInstanceId;

    public ConsumerGroupMetadata(String groupId, int generationId, String memberId, Optional<String> groupInstanceId) {
        this.groupId = groupId;
        this.generationId = generationId;
        this.memberId = memberId;
        this.groupInstanceId = groupInstanceId;
    }

    public String groupId() {
        return groupId;
    }

    public int generationId() {
        return generationId;
    }

    public String memberId() {
        return memberId;
    }

    public Optional<String> groupInstanceId() {
        return groupInstanceId;
    }
}
