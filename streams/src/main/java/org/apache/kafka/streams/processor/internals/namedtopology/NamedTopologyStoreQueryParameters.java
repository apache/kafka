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
package org.apache.kafka.streams.processor.internals.namedtopology;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;

import java.util.Objects;

public class NamedTopologyStoreQueryParameters<T> extends StoreQueryParameters<T> {

    final String topologyName;

    private NamedTopologyStoreQueryParameters(final String topologyName,
                                              final String storeName,
                                              final QueryableStoreType<T>  queryableStoreType,
                                              final Integer partition,
                                              final boolean staleStores) {
        super(storeName, queryableStoreType, partition, staleStores);
        this.topologyName = topologyName;
    }

    public static <T> NamedTopologyStoreQueryParameters<T> fromNamedTopologyAndStoreNameAndType(final String topologyName,
                                                                                                final String storeName,
                                                                                                final QueryableStoreType<T> queryableStoreType) {
        return new NamedTopologyStoreQueryParameters<>(topologyName, storeName, queryableStoreType, null, false);
    }

    /**
     * See {@link StoreQueryParameters#withPartition(Integer)}
     */
    public NamedTopologyStoreQueryParameters<T> withPartition(final Integer partition) {
        return new NamedTopologyStoreQueryParameters<>(this.topologyName(), this.storeName(), this.queryableStoreType(), partition, this.staleStoresEnabled());
    }

    /**
     * See {@link StoreQueryParameters#enableStaleStores()}
     */
    public NamedTopologyStoreQueryParameters<T> enableStaleStores() {
        return new NamedTopologyStoreQueryParameters<>(this.topologyName(), this.storeName(), this.queryableStoreType(), this.partition(), true);
    }

    public String topologyName() {
        return topologyName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        final NamedTopologyStoreQueryParameters<?> that = (NamedTopologyStoreQueryParameters<?>) o;

        return Objects.equals(topologyName, that.topologyName);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (topologyName != null ? topologyName.hashCode() : 0);
        return result;
    }
}
