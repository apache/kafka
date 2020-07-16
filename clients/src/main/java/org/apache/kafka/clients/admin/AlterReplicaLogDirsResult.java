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

import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.errors.UnknownServerException;

/**
 * The result of {@link Admin#alterReplicaLogDirs(Map, AlterReplicaLogDirsOptions)}.
 *
 * To retrieve the detailed result per specified {@link TopicPartitionReplica}, use {@link #values()}. To retrieve the
 * overall result only, use {@link #all()}.
 */
@InterfaceStability.Evolving
public class AlterReplicaLogDirsResult {
    private final Map<TopicPartitionReplica, KafkaFuture<Void>> futures;

    AlterReplicaLogDirsResult(Map<TopicPartitionReplica, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from {@link TopicPartitionReplica} to {@link KafkaFuture} which holds the status of individual
     * replica movement.
     *
     * To check the result of individual replica movement, call {@link KafkaFuture#get()} from the value contained
     * in the returned map. If there is no error, it will return silently; if not, an {@link Exception} will be thrown
     * like the following:
     *
     * <ul>
     *   <li>{@link CancellationException}: The task was canceled.</li>
     *   <li>{@link InterruptedException}: Interrupted while joining I/O thread.</li>
     *   <li>{@link ExecutionException}: Execution failed with the following causes:</li>
     *   <ul>
     *     <li>{@link ClusterAuthorizationException}: Authorization failed. (CLUSTER_AUTHORIZATION_FAILED, 31)</li>
     *     <li>{@link InvalidTopicException}: The specified topic name is too long. (INVALID_TOPIC_EXCEPTION, 17)</li>
     *     <li>{@link LogDirNotFoundException}: The specified log directory is not found in the broker. (LOG_DIR_NOT_FOUND, 57)</li>
     *     <li>{@link ReplicaNotAvailableException}: The replica does not exist on the broker. (REPLICA_NOT_AVAILABLE, 9)</li>
     *     <li>{@link KafkaStorageException}: Disk error occurred. (KAFKA_STORAGE_ERROR, 56)</li>
     *     <li>{@link UnknownServerException}: Unknown. (UNKNOWN_SERVER_ERROR, -1)</li>
     *   </ul>
     * </ul>
     */
    public Map<TopicPartitionReplica, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * Return a {@link KafkaFuture} which succeeds on {@link KafkaFuture#get()} if all the replica movement have succeeded.
     * if not, it throws an {@link Exception} described in {@link #values()} method.
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
