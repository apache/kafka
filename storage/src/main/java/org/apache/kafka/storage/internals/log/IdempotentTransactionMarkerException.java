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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.protocol.Errors;

/**
 * Indicates that a transaction marker was received as part of an idempotent retry
 * and should be treated as a successful no-op rather than an error.
 *
 * <p>This exception is thrown when:
 * <ul>
 *   <li>A TV2 transaction marker arrives with the same epoch as current</li>
 *   <li>No transaction is currently ongoing (currentTxnFirstOffset is empty)</li>
 * </ul>
 *
 * <p>Common scenarios include coordinator recovery and network-induced retries.
 * Callers should catch this exception and treat it as a successful operation.
 */
public class IdempotentTransactionMarkerException extends KafkaException {

    public IdempotentTransactionMarkerException() {
        super();
    }

    public static boolean isInstanceOf(Throwable t) {
        return Errors.maybeUnwrapException(t) instanceof IdempotentTransactionMarkerException;
    }
}
