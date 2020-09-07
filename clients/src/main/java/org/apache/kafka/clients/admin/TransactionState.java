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
package org.apache.kafka.clients.admin;

import java.util.HashMap;

public enum TransactionState {
    ONGOING("Ongoing"),
    PREPARE_ABORT("PrepareAbort"),
    PREPARE_COMMIT("PrepareCommit"),
    COMPLETE_ABORT("CompleteAbort"),
    COMPLETE_COMMIT("CompleteCommit"),
    EMPTY("Empty"),
    PREPARE_EPOCH_FENCE("PrepareEpochFence"),
    UNKNOWN("Unknown");

    private final static HashMap<String, TransactionState> NAME_TO_ENUM;

    static {
        NAME_TO_ENUM = new HashMap<>();
        for (TransactionState state : TransactionState.values()) {
            NAME_TO_ENUM.put(state.name, state);
        }
    }

    private final String name;

    TransactionState(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    public static TransactionState parse(String name) {
        TransactionState state = NAME_TO_ENUM.get(name);
        return state == null ? UNKNOWN : state;
    }

}
