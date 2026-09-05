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

import org.apache.kafka.common.KafkaException;

/**
 * A step of a chained coordinator write operation.
 *
 * @param <T> The type of the response.
 * @param <U> The type of the records.
 */
@FunctionalInterface
public interface CoordinatorWriteOperationStep<T, U> {
    /**
     * Invoked on the coordinator event loop after the records of the previous step
     * have been replayed to the state machine and appended to the current batch. The
     * records of a step are appended atomically; atomicity across steps is NOT
     * guaranteed: if this step (or a later one) fails, the records of earlier steps
     * still commit. Every step must therefore leave the state machine in a
     * consistent, commit-safe state on its own.
     *
     * @return A result containing this step's records and either a continuation or
     *         the terminal response.
     * @throws KafkaException
     */
    CoordinatorOperationResult<T, U> generateRecordsAndResult() throws KafkaException;
}
