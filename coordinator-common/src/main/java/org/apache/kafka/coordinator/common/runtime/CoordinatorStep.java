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

import java.util.List;
import java.util.Objects;

/**
 * An intermediate result of a chained coordinator write operation: records to append
 * and replay, followed by a continuation that is invoked once they have been applied
 * to the state machine.
 *
 * Unlike {@link CoordinatorResult}, a step cannot carry a response or an append
 * future: only the terminal result of a chain does. This is enforced by the type
 * system rather than by convention.
 *
 * @param records The records to append and replay before {@code next} is invoked.
 * @param next    The continuation.
 * @param <T> The type of the response.
 * @param <U> The type of the records.
 */
public record CoordinatorStep<T, U>(
    List<U> records,
    CoordinatorWriteOperationStep<T, U> next
) implements CoordinatorOperationResult<T, U> {
    public CoordinatorStep {
        Objects.requireNonNull(records);
        Objects.requireNonNull(next);
    }
}
