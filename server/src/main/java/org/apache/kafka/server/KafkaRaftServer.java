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
package org.apache.kafka.server;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;

public class KafkaRaftServer {
    public static final String METADATA_TOPIC = Topic.CLUSTER_METADATA_TOPIC_NAME;
    public static final TopicPartition METADATA_PARTITION = Topic.CLUSTER_METADATA_TOPIC_PARTITION;
    public static final Uuid METADATA_TOPIC_ID = Uuid.METADATA_TOPIC_ID;

    public enum ProcessRole {
        BrokerRole("broker"),
        ControllerRole("controller");

        private final String roleName;

        ProcessRole(String roleName) {
            this.roleName = roleName;
        }

        @Override
        public String toString() {
            return roleName;
        }
    }
}
