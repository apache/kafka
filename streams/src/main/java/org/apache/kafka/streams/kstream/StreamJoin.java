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

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

public class StreamJoin<K, V1, V2>
    implements NamedOperation<StreamJoin<K, V1, V2>> {

    protected final Serde<K> keySerde;
    protected final Serde<V1> valueSerde;
    protected final Serde<V2> otherValueSerde;
    protected final WindowBytesStoreSupplier thisStoreSupplier;
    protected final WindowBytesStoreSupplier otherStoreSupplier;
    protected final String name;
    protected final String storeName;

    protected StreamJoin(final StreamJoin<K, V1, V2> streamJoin) {
        this(streamJoin.keySerde,
            streamJoin.valueSerde,
            streamJoin.otherValueSerde,
            streamJoin.thisStoreSupplier,
            streamJoin.otherStoreSupplier,
            streamJoin.name,
            streamJoin.storeName);
    }

    private StreamJoin(final Serde<K> keySerde,
                       final Serde<V1> valueSerde,
                       final Serde<V2> otherValueSerde,
                       final WindowBytesStoreSupplier thisStoreSupplier,
                       final WindowBytesStoreSupplier otherStoreSupplier,
                       final String name,
                       final String storeName) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.otherValueSerde = otherValueSerde;
        this.thisStoreSupplier = thisStoreSupplier;
        this.otherStoreSupplier = otherStoreSupplier;
        this.name = name;
        this.storeName = storeName;
    }


    public static <K, V1, V2> StreamJoin<K, V1, V2> with(final WindowBytesStoreSupplier storeSupplier,
                                                         final WindowBytesStoreSupplier otherStoreSupplier) {
        return new StreamJoin<>(
            null,
            null,
            null,
            storeSupplier,
            otherStoreSupplier,
            null,
            null
        );
    }

    public static <K, V1, V2> StreamJoin<K, V1, V2> as(final String storeName) {
        return new StreamJoin<>(
            null,
            null,
            null,
            null,
            null,
            null,
            storeName
        );
    }


    public static <K, V1, V2> StreamJoin<K, V1, V2> with(final Serde<K> keySerde,
                                                         final Serde<V1> valueSerde,
                                                         final Serde<V2> otherValueSerde
    ) {
        return new StreamJoin<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            null,
            null,
            null,
            null
        );
    }

    @Override
    public StreamJoin<K, V1, V2> withName(final String name) {
        return new StreamJoin<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }


    public StreamJoin<K, V1, V2> withStoreName(final String storeName) {
        return new StreamJoin<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    public StreamJoin<K, V1, V2> withKeySerde(final Serde<K> keySerde) {
        return new StreamJoin<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    public StreamJoin<K, V1, V2> withValueSerde(final Serde<V1> valueSerde) {
        return new StreamJoin<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    public StreamJoin<K, V1, V2> withOtherValueSerde(final Serde<V2> otherValueSerde) {
        return new StreamJoin<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    public StreamJoin<K, V1, V2> withStoreSupplier(final WindowBytesStoreSupplier thisStoreSupplier) {
        return new StreamJoin<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    public StreamJoin<K, V1, V2> withOtherStoreSupplier(final WindowBytesStoreSupplier otherStoreSupplier) {
        return new StreamJoin<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName
        );
    }

    @Override
    public String toString() {
        return "StreamJoin{" +
            "keySerde=" + keySerde +
            ", valueSerde=" + valueSerde +
            ", otherValueSerde=" + otherValueSerde +
            ", thisStoreSupplier=" + thisStoreSupplier +
            ", otherStoreSupplier=" + otherStoreSupplier +
            ", name='" + name + '\'' +
            ", storeName='" + storeName + '\'' +
            '}';
    }
}
