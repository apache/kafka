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

/**
 * The result of one step of a coordinator write operation. A write operation is either
 * a single {@link CoordinatorResult} (the common case, unchanged), or a chain of
 * intermediate {@link CoordinatorStep}s terminated by a {@link CoordinatorResult}. Each
 * step in a chain observes the state machine after the previous step's records have
 * been replayed, so it never needs to reason about not-yet-applied changes.
 *
 * @param <T> The type of the response.
 * @param <U> The type of the records.
 */
public sealed interface CoordinatorOperationResult<T, U> permits CoordinatorStep, CoordinatorResult {
    /**
     * @return The records produced by this step.
     */
    List<U> records();
}
