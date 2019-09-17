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
import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;

public class StreamJoined<K, V1, V2>
    implements NamedOperation<StreamJoined<K, V1, V2>> {

    protected final Serde<K> keySerde;
    protected final Serde<V1> valueSerde;
    protected final Serde<V2> otherValueSerde;
    protected final WindowBytesStoreSupplier thisStoreSupplier;
    protected final WindowBytesStoreSupplier otherStoreSupplier;
    protected final String name;
    protected final String storeName;

    protected boolean loggingEnabled;
    protected boolean cachingEnabled;
    protected Map<String, String> topicConfig;
    protected Duration retention;

    protected StreamJoined(final StreamJoined<K, V1, V2> streamJoined) {
        this(streamJoined.keySerde,
            streamJoined.valueSerde,
            streamJoined.otherValueSerde,
            streamJoined.thisStoreSupplier,
            streamJoined.otherStoreSupplier,
            streamJoined.name,
            streamJoined.storeName,
            streamJoined.loggingEnabled,
            streamJoined.cachingEnabled,
            streamJoined.topicConfig,
            streamJoined.retention);
    }

    private StreamJoined(final Serde<K> keySerde,
                         final Serde<V1> valueSerde,
                         final Serde<V2> otherValueSerde,
                         final WindowBytesStoreSupplier thisStoreSupplier,
                         final WindowBytesStoreSupplier otherStoreSupplier,
                         final String name,
                         final String storeName,
                         final boolean loggingEnabled,
                         final boolean cachingEnabled,
                         final Map<String, String> topicConfig,
                         final Duration retention) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.otherValueSerde = otherValueSerde;
        this.thisStoreSupplier = thisStoreSupplier;
        this.otherStoreSupplier = otherStoreSupplier;
        this.name = name;
        this.storeName = storeName;
        this.loggingEnabled = loggingEnabled;
        this.cachingEnabled = cachingEnabled;
        this.topicConfig = topicConfig;
        this.retention = retention;
    }


    public static <K, V1, V2> StreamJoined<K, V1, V2> as(final WindowBytesStoreSupplier storeSupplier,
                                                         final WindowBytesStoreSupplier otherStoreSupplier) {
        return new StreamJoined<>(
            null,
            null,
            null,
            storeSupplier,
            otherStoreSupplier,
            null,
            null,
            true,
            true,
            new HashMap<>(),
            null
        );
    }

    public static <K, V1, V2> StreamJoined<K, V1, V2> as(final String storeName) {
        return new StreamJoined<>(
            null,
            null,
            null,
            null,
            null,
            null,
            storeName,
            true,
            true,
            new HashMap<>(),
            null
        );
    }


    public static <K, V1, V2> StreamJoined<K, V1, V2> with(final Serde<K> keySerde,
                                                           final Serde<V1> valueSerde,
                                                           final Serde<V2> otherValueSerde
    ) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            null,
            null,
            null,
            null,
            true,
            true,
            new HashMap<>(),
            null
        );
    }

    @Override
    public StreamJoined<K, V1, V2> withName(final String name) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }


    public StreamJoined<K, V1, V2> withStoreName(final String storeName) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }

    public StreamJoined<K, V1, V2> withKeySerde(final Serde<K> keySerde) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }

    public StreamJoined<K, V1, V2> withValueSerde(final Serde<V1> valueSerde) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }

    public StreamJoined<K, V1, V2> withOtherValueSerde(final Serde<V2> otherValueSerde) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }

    public StreamJoined<K, V1, V2> withStoreSupplier(final WindowBytesStoreSupplier thisStoreSupplier) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }

    public StreamJoined<K, V1, V2> withOtherStoreSupplier(final WindowBytesStoreSupplier otherStoreSupplier) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }

    public StreamJoined<K, V1, V2> withLogging(final boolean loggingEnabled) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }

    public StreamJoined<K, V1, V2> withCaching(final boolean cachingEnabled) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }

    public StreamJoined<K, V1, V2> withTopicConfig(final Map<String, String> topicConfig) {
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }

    public StreamJoined<K, V1, V2> withRetention(final Duration retention) throws IllegalArgumentException {
        final String msgPrefix = prepareMillisCheckFailMsgPrefix(retention, "retention");
        final long retenationMs = ApiUtils.validateMillisecondDuration(retention, msgPrefix);

        if (retenationMs < 0) {
            throw new IllegalArgumentException("Retention must not be negative.");
        }
        return new StreamJoined<>(
            keySerde,
            valueSerde,
            otherValueSerde,
            thisStoreSupplier,
            otherStoreSupplier,
            name,
            storeName,
            loggingEnabled,
            cachingEnabled,
            topicConfig,
            retention
        );
    }

}
