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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.annotation.InterfaceStability;
import java.util.Map;


/**
 * The result of {@link AdminClient#alterReplicaLogDirs(Map, AlterReplicaLogDirsOptions)}.
 */
@InterfaceStability.Evolving
public class AlterReplicaLogDirsResult {
    private final Map<TopicPartitionReplica, KafkaFuture<Void>> futures;

    AlterReplicaLogDirsResult(Map<TopicPartitionReplica, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     *
     * Return a map from replica to future which can be used to check the status of individual replica movement.
     *
     * Possible error code:
     *
     * LOG_DIR_NOT_FOUND (57)
     * KAFKA_STORAGE_ERROR (56)
     * REPLICA_NOT_AVAILABLE (9)
     * UNKNOWN (-1)
     */
    public Map<TopicPartitionReplica, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * Return a future which succeeds if all the replica movement have succeeded
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
