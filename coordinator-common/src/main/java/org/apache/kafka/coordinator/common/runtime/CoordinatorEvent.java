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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.TopicPartition;

/**
 * The base event type used by all events processed in the
 * coordinator runtime.
 */
public interface CoordinatorEvent extends EventAccumulator.Event<TopicPartition> {

    /**
     * Executes the event.
     */
    void run();

    /**
     * Completes the event with the provided exception.
     *
     * @param exception An exception if the processing of the event failed or null otherwise.
     */
    void complete(Throwable exception);

    /**
     * @return The created time in milliseconds.
     */
    long createdTimeMs();
}
