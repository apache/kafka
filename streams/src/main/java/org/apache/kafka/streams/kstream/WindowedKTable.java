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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

public interface WindowedKTable<K, V> extends KTable<Windowed<K>, V> {
    @Override
    WindowedKTable<K, V> filter(final Predicate<? super Windowed<K>, ? super V> predicate);

    @Override
    WindowedKTable<K, V> filter(final Predicate<? super Windowed<K>, ? super V> predicate, final Materialized<Windowed<K>, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    WindowedKTable<K, V> filterNot(final Predicate<? super Windowed<K>, ? super V> predicate);

    @Override
    WindowedKTable<K, V> filterNot(final Predicate<? super Windowed<K>, ? super V> predicate, final Materialized<Windowed<K>, V, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> WindowedKTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper);

    @Override
    <VR> WindowedKTable<K, VR> mapValues(final ValueMapperWithKey<? super Windowed<K>, ? super V, ? extends VR> mapper);

    @Override
    <VR> WindowedKTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper, final Materialized<Windowed<K>, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> WindowedKTable<K, VR> mapValues(final ValueMapperWithKey<? super Windowed<K>, ? super V, ? extends VR> mapper, final Materialized<Windowed<K>, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VR> WindowedKTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super Windowed<K>, ? super V, ? extends VR> transformerSupplier, final String... stateStoreNames);

    @Override
    <VR> WindowedKTable<K, VR> transformValues(final ValueTransformerWithKeySupplier<? super Windowed<K>, ? super V, ? extends VR> transformerSupplier, final Materialized<Windowed<K>, VR, KeyValueStore<Bytes, byte[]>> materialized, final String... stateStoreNames);

    @Override
    <KR, VR> KGroupedTable<KR, VR> groupBy(final KeyValueMapper<? super Windowed<K>, ? super V, KeyValue<KR, VR>> selector);

    @Override
    <KR, VR> KGroupedTable<KR, VR> groupBy(final KeyValueMapper<? super Windowed<K>, ? super V, KeyValue<KR, VR>> selector, final Serialized<KR, VR> serialized);

    @Override
    <VO, VR> WindowedKTable<K, VR> join(final KTable<Windowed<K>, VO> other, final ValueJoiner<? super V, ? super VO, ? extends VR> joiner);

    @Override
    <VO, VR> WindowedKTable<K, VR> join(final KTable<Windowed<K>, VO> other, final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Materialized<Windowed<K>, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VO, VR> WindowedKTable<K, VR> leftJoin(final KTable<Windowed<K>, VO> other, final ValueJoiner<? super V, ? super VO, ? extends VR> joiner);

    @Override
    <VO, VR> WindowedKTable<K, VR> leftJoin(final KTable<Windowed<K>, VO> other, final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Materialized<Windowed<K>, VR, KeyValueStore<Bytes, byte[]>> materialized);

    @Override
    <VO, VR> WindowedKTable<K, VR> outerJoin(final KTable<Windowed<K>, VO> other, final ValueJoiner<? super V, ? super VO, ? extends VR> joiner);

    @Override
    <VO, VR> WindowedKTable<K, VR> outerJoin(final KTable<Windowed<K>, VO> other, final ValueJoiner<? super V, ? super VO, ? extends VR> joiner, final Materialized<Windowed<K>, VR, KeyValueStore<Bytes, byte[]>> materialized);
}
