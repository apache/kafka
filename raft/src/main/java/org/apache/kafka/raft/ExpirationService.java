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
package org.apache.kafka.raft;

import java.util.concurrent.CompletableFuture;

public interface ExpirationService {
    /**
     * Get a new completable future which will automatically fail exceptionally with a
     * {@link org.apache.kafka.common.errors.TimeoutException} if not completed before
     * the provided time limit expires.
     *
     * @param timeoutMs the duration in milliseconds before the future is completed exceptionally
     * @param <T> arbitrary future type (the service must set no expectation on the this type)
     * @return the completable future
     */
    <T> CompletableFuture<T> failAfter(long timeoutMs);
}
