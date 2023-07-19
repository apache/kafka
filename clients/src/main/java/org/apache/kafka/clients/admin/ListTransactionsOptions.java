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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
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

    /**
     * Filter only the transactions that are in a specific set of states. If no filter
     * is specified or if the passed set of states is empty, then transactions in all
     * states will be returned.
     *
     * @param states the set of states to filter by
     * @return this object
     */
    public ListTransactionsOptions filterStates(Collection<TransactionState> states) {
        this.filteredStates = new HashSet<>(states);
        return this;
    }

    /**
     * Filter only the transactions from producers in a specific set of producerIds.
     * If no filter is specified or if the passed collection of producerIds is empty,
     * then the transactions of all producerIds will be returned.
     *
     * @param producerIdFilters the set of producerIds to filter by
     * @return this object
     */
    public ListTransactionsOptions filterProducerIds(Collection<Long> producerIdFilters) {
        this.filteredProducerIds = new HashSet<>(producerIdFilters);
        return this;
    }

    /**
     * Returns the set of states to be filtered or empty if no states have been specified.
     *
     * @return the current set of filtered states (empty means that no states are filtered and all
     *         all transactions will be returned)
     */
    public Set<TransactionState> filteredStates() {
        return filteredStates;
    }

    /**
     * Returns the set of producerIds that are being filtered or empty if none have been specified.
     *
     * @return the current set of filtered states (empty means that no producerIds are filtered and
     *         all transactions will be returned)
     */
    public Set<Long> filteredProducerIds() {
        return filteredProducerIds;
    }

    @Override
    public String toString() {
        return "ListTransactionsOptions(" +
            "filteredStates=" + filteredStates +
            ", filteredProducerIds=" + filteredProducerIds +
            ", timeoutMs=" + timeoutMs +
            ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListTransactionsOptions that = (ListTransactionsOptions) o;
        return Objects.equals(filteredStates, that.filteredStates) &&
            Objects.equals(filteredProducerIds, that.filteredProducerIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filteredStates, filteredProducerIds);
    }
}
