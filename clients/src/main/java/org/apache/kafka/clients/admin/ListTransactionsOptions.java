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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Options for {@link Admin#listTransactions()}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListTransactionsOptions extends AbstractOptions<ListTransactionsOptions> {
    private Set<TransactionState> filteredStates = Collections.emptySet();
    private Set<Long> filteredProducerIds = Collections.emptySet();

    public ListTransactionsOptions filterStates(Set<TransactionState> states) {
        this.filteredStates = new HashSet<>(states);
        return this;
    }

    public ListTransactionsOptions filterProducerIds(Set<Long> producerIdFilters) {
        this.filteredProducerIds = new HashSet<>(producerIdFilters);
        return this;
    }

    /**
     * Returns the list of States that are requested or empty if no states have been specified
     */
    public Set<TransactionState> filteredStates() {
        return filteredStates;
    }

    public Set<Long> filteredProducerIds() {
        return filteredProducerIds;
    }


}
