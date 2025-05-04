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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Options for {@link Admin#listTransactions()}.
 */
public class ListTransactionsOptions extends AbstractOptions<ListTransactionsOptions> {
    private Set<TransactionState> filteredStates = Collections.emptySet();
    private Set<Long> filteredProducerIds = Collections.emptySet();

    private long filteredDuration = -1L;
    private String filteredTransactionalIdPattern;
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
     * Filter only the transactions that are running longer than the specified duration.
     * If no filter is specified or if the passed duration ms is less than 0,
     * then the all transactions will be returned.
     *
     * @param durationMs the duration in milliseconds to filter by
     * @return this object
     */
    public ListTransactionsOptions filterOnDuration(long durationMs) {
        this.filteredDuration = durationMs;
        return this;
    }

    /**
     * Filter only the transactions that match with the given transactional ID pattern.
     * If the filter is null or if the passed string is empty,
     * then all the transactions will be returned.
     *
     * @param pattern the transactional ID regular expression pattern to filter by
     * @return this object
     */
    public ListTransactionsOptions filterOnTransactionalIdPattern(String pattern) {
        this.filteredTransactionalIdPattern = pattern;
        return this;
    }

    /**
     * Returns the set of states to be filtered or empty if no states have been specified.
     *
     * @return the current set of filtered states (empty means that no states are filtered and
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

    /**
     * Returns the duration ms value being filtered.
     *
     * @return the current duration filter value in ms (negative value means transactions are not filtered by duration)
     */
    public long filteredDuration() {
        return filteredDuration;
    }

    /**
     * Returns transactional ID being filtered.
     *
     * @return the current transactional ID pattern filter (empty means no transactional IDs are filtered and all
     *         transactions will be returned)
     */
    public String filteredTransactionalIdPattern() {
        return filteredTransactionalIdPattern;
    }

    @Override
    public String toString() {
        return "ListTransactionsOptions(" +
            "filteredStates=" + filteredStates +
            ", filteredProducerIds=" + filteredProducerIds +
            ", filteredDuration=" + filteredDuration +
            ", filteredTransactionalIdPattern=" + filteredTransactionalIdPattern +
            ", timeoutMs=" + timeoutMs +
            ')';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ListTransactionsOptions that = (ListTransactionsOptions) o;
        return Objects.equals(filteredStates, that.filteredStates) &&
            Objects.equals(filteredProducerIds, that.filteredProducerIds) &&
            Objects.equals(filteredDuration, that.filteredDuration) &&
            Objects.equals(filteredTransactionalIdPattern, that.filteredTransactionalIdPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filteredStates, filteredProducerIds, filteredDuration, filteredTransactionalIdPattern);
    }
}
