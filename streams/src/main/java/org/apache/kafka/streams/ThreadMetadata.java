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
package org.apache.kafka.streams;

import java.util.Set;

/**
 * Metadata of a stream thread.
 */
public interface ThreadMetadata {


    /**
     * State of the stream thread
     *
     * @return the state
     */
    String threadState();

    /**
     * Name of the stream thread
     *
     * @return the name
     */
    String threadName();

    /**
     * Metadata of the active tasks assigned to the stream thread.
     *
     * @return metadata of the active tasks
     */
    Set<TaskMetadata> activeTasks();

    /**
     * Metadata of the standby tasks assigned to the stream thread.
     *
     * @return metadata of the standby tasks

     */
    Set<TaskMetadata> standbyTasks();

    /**
     * Client ID of the Kafka consumer used by the stream thread.
     *
     * @return client ID of the Kafka consumer
     */
    String consumerClientId();

    /**
     * Client ID of the restore Kafka consumer used by the stream thread
     *
     * @return client ID of the restore Kafka consumer
     */
    String restoreConsumerClientId();

    /**
     * Client IDs of the Kafka producers used by the stream thread.
     *
     * @return client IDs of the Kafka producers
     */
    Set<String> producerClientIds();

    /**
     * Client ID of the admin client used by the stream thread.
     *
     * @return client ID of the admin client
     */
    String adminClientId();

    /**
     * Compares the specified object with this ThreadMetadata. Returns {@code true} if and only if the specified object is
     * also a ThreadMetadata and both {@code threadName()} are equal, {@code threadState()} are equal, {@code activeTasks()} contain the same
     * elements, {@code standbyTasks()} contain the same elements, {@code mainConsumerClientId()} are equal, {@code restoreConsumerClientId()}
     * are equal, {@code producerClientIds()} are equal, {@code producerClientIds} contain the same elements, and {@code adminClientId()} are equal.
     *
     * @return {@code true} if this object is the same as the obj argument; {@code false} otherwise.
     */
    boolean equals(Object o);

    /**
     * Returns the hash code value for this ThreadMetadata. The hash code of a list is defined to be the result of the following calculation:
     * <pre>
     * {@code
     * Objects.hash(
     *             threadName,
     *             threadState,
     *             activeTasks,
     *             standbyTasks,
     *             mainConsumerClientId,
     *             restoreConsumerClientId,
     *             producerClientIds,
     *             adminClientId
     *             );
     * }
     * </pre>
     *
     * @return a hash code value for this object.
     */
    int hashCode();
}
