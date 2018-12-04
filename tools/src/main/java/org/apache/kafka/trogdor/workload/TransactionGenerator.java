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
package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Generates actions that should be taken by a producer that uses transactions.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = UniformTransactionsGenerator.class, name = "uniform"),
})
public interface TransactionGenerator {
    enum TransactionAction {
        BEGIN_TRANSACTION, COMMIT_TRANSACTION, ABORT_TRANSACTION, NO_OP
    }

    /**
     * Returns the next action that the producer should take in regards to transactions.
     * This method should be called every time before a producer sends a message.
     * This means that most of the time it should return #{@link TransactionAction#NO_OP}
     * to signal the producer that its next step should be to send a message.
     */
    TransactionAction nextAction();
}
