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
 * Represents the state of a single thread running within a {@link KafkaStreams} application.
 */
public interface ThreadMetadata {


    /**
     * This function will return the state of the Thread
     * @return the state of the Thread
     */
    String threadState();

    /**
     * This function will return the name of the Thread
     *
     * @return the name of the Thread
     */
    String threadName();

    /**
     * This function will return the set of the {@link TaskMetadata} for the current active tasks
     *
     * @return a set of metadata for the active tasks
     */
    Set<TaskMetadata> activeTasks();

    /**
     * This function will return the set of the {@link TaskMetadata} for the current standby tasks
     *
     * @return a set of metadata for the standby tasks
     */
    Set<TaskMetadata> standbyTasks();

    /**
     * This function will return the Client Id for the consumer
     *
     * @return the consumer Client Id
     */
    String consumerClientId();

    /**
     * This function will return the Client id for the restore consumer
     *
     * @return the restore consumer Client Id
     */
    String restoreConsumerClientId();

    /**
     * This function will return the set of Client Ids for the producers
     *
     * @return set of producer Client Ids
     */
    Set<String> producerClientIds();

    /**
     * This function will return the Client Id for the admin client
     *
     * @return the admin Client Id
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
     *             adminClientId);
     * </pre>
     *
     * @return a hash code value for this object.
     */
    int hashCode();
}
