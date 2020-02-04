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
package org.apache.kafka.streams;

import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.Objects;

/**
 * Represents all the query options that a user can provide to control the kind of stores we would like to fetch.
 * The options could indicate - whether we want stale stores to be included (or)
 * we want to filter for only stores belonging to a partition.
 * If no specific partition is specified the default behavior is to fetch the stores for all the partitions
 * available on that instance for that particular store name.
 */
public class StoreQueryParams<T> {

    private Integer partition;
    private boolean staleStores;
    private final String storeName;
    private final QueryableStoreType<T> queryableStoreType;

    private StoreQueryParams(final String storeName, final QueryableStoreType<T>  queryableStoreType, final Integer partition, final boolean staleStores) {
        this.storeName = storeName;
        this.queryableStoreType = queryableStoreType;
        this.partition = partition;
        this.staleStores = staleStores;
    }

    public static <T> StoreQueryParams<T> fromNameAndType(final String storeName,
                                                          final QueryableStoreType<T>  queryableStoreType) {
        return new StoreQueryParams<T>(storeName, queryableStoreType, null, false);
    }

    /**
     * Set a specific partition that should be queried exclusively.
     *
     * @param partition   The specific integer partition to be fetched from the stores list by using {@link StoreQueryParams}.
     *
     * @return StoreQueryParams a new {@code StoreQueryParams} instance configured with the specified partition
     */
    public StoreQueryParams<T> withPartition(final Integer partition) {
        return new StoreQueryParams<T>(this.storeName(), this.queryableStoreType(), partition, this.staleStores);
    }

    /**
     * Enable querying of stale state stores, i.e., allow to query active tasks during restore as well as standby tasks.
     *
     * @return StoreQueryParams a new {@code StoreQueryParams} instance configured with serving from stale stores enabled
     */
    public StoreQueryParams<T> enableStaleStores() {
        return new StoreQueryParams<T>(this.storeName(), this.queryableStoreType(), this.partition, true);
    }

    /**
     * Get the name of the state store that should be queried.
     *
     * @return String state store name
     */
    public String storeName() {
        return storeName;
    }

    /**
     * Get the queryable store type for which key is queried by the user.
     *
     * @return QueryableStoreType type of queryable store
     */
    public QueryableStoreType<T> queryableStoreType() {
        return queryableStoreType;
    }

    /**
     * Get the store partition that will be queried.
     * If the method returns {@code null}, it would mean that no specific partition has been requested,
     * so all the local partitions for the store will be queried.
     *
     * @return Integer partition
     */
    public Integer partition() {
        return partition;
    }

    /**
     * Get the flag staleStores. If {@code true}, include standbys and recovering stores along with running stores.
     *
     * @return boolean staleStores
     */
    public boolean staleStoresEnabled() {
        return staleStores;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof StoreQueryParams)) {
            return false;
        }
        final StoreQueryParams storeQueryParams = (StoreQueryParams) obj;
        return Objects.equals(storeQueryParams.partition, partition)
                && Objects.equals(storeQueryParams.staleStores, staleStores)
                && Objects.equals(storeQueryParams.storeName, storeName)
                && Objects.equals(storeQueryParams.queryableStoreType, queryableStoreType);
    }

    @Override
    public String toString() {
        return "StoreQueryParams {" +
                "partition=" + partition +
                ", staleStores=" + staleStores +
                ", storeName=" + storeName +
                ", queryableStoreType=" + queryableStoreType +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, staleStores, storeName, queryableStoreType);
    }
}