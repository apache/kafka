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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A uniform transactions generator where every N records are grouped in a separate transaction
 */
public class UniformTransactionsGenerator implements TransactionGenerator {

    private final int messagesPerTransaction;
    private int messagesInTransaction = -1;

    @JsonCreator
    public UniformTransactionsGenerator(@JsonProperty("messagesPerTransaction") int messagesPerTransaction) {
        if (messagesPerTransaction < 1)
            throw new IllegalArgumentException("Cannot have less than one message per transaction.");

        this.messagesPerTransaction = messagesPerTransaction;
    }

    @JsonProperty
    public int messagesPerTransaction() {
        return messagesPerTransaction;
    }

    @Override
    public synchronized TransactionAction nextAction() {
        if (messagesInTransaction == -1) {
            messagesInTransaction = 0;
            return TransactionAction.BEGIN_TRANSACTION;
        }
        if (messagesInTransaction == messagesPerTransaction) {
            messagesInTransaction = -1;
            return TransactionAction.COMMIT_TRANSACTION;
        }

        messagesInTransaction += 1;
        return TransactionAction.NO_OP;
    }
}
