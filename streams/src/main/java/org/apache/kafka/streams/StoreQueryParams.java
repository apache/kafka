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

import java.util.Objects;

/**
 * Represents all the query options that a user can provide to state what kind of stores it is expecting. The options would be whether a user would want to enable/disable stale stores* or whether it knows the list of partitions that it specifically wants to fetch. If this information is not provided the default behavior is to fetch the stores for all the partitions available on that instance* for that particular store name.
 * It contains a list of partitions, which for a point queries can be populated from the  KeyQueryMetadata.
 */
public class StoreQueryParams {

    private final Integer partition;
    private final boolean includeStaleStores;

    public static final StoreQueryParams withPartitionAndStaleStoresDisabled(final Integer partition) {
        return new StoreQueryParams(partition, false);
    }

    public static final StoreQueryParams withPartitionAndStaleStoresEnabled(final Integer partition) {
        return new StoreQueryParams(partition, true);
    }

    public static final StoreQueryParams withAllPartitionAndStaleStoresDisabled() {
        return new StoreQueryParams(null, false);
    }

    public static final StoreQueryParams withAllPartitionAndStaleStoresEnabled() {
        return new StoreQueryParams(null, true);
    }

    private StoreQueryParams(final Integer partition, final boolean includeStaleStores) {
        this.partition = partition;
        this.includeStaleStores = includeStaleStores;
    }


    /**
     * Get the partition to be used to fetch list of Queryable store from QueryableStoreProvider.
     *
     * @return an Integer partition
     */
    public Integer getPartition() {
        return partition;
    }

    /**
     * Get the flag includeStaleStores. If true, include standbys and recovering stores along with running stores
     *
     * @return boolean includeStaleStores
     */
    public boolean includeStaleStores() {
        return includeStaleStores;
    }

    /**
     * Get whether the store query params are fetching all partitions or a single partition.
     *
     * @return boolean. True, if all partitions are requests or false if a specific partition is requested
     */
    public boolean getAllLocalPartitions() {
        return partition == null ? true : false;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof StoreQueryParams)) {
            return false;
        }
        final StoreQueryParams storeQueryParams = (StoreQueryParams) obj;
        return Objects.equals(storeQueryParams.partition, partition)
                && Objects.equals(storeQueryParams.includeStaleStores, includeStaleStores);
    }


    @Override
    public String toString() {
        return "StoreQueryParams {" +
                "partition=" + partition +
                ", includeStaleStores=" + includeStaleStores +
                '}';
    }


    @Override
    public int hashCode() {
        return Objects.hash(partition, includeStaleStores);
    }
}