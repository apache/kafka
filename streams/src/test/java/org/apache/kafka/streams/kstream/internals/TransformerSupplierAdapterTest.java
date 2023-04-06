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
package org.apache.kafka.streams.kstream.internals;

import java.util.Iterator;
import java.util.Set;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class TransformerSupplierAdapterTest {

    @Mock
    private ProcessorContext context;
    @Mock
    private Transformer<String, String, KeyValue<Integer, Integer>> transformer;
    @Mock
    private TransformerSupplier<String, String, KeyValue<Integer, Integer>> transformerSupplier;
    @Mock
    private Set<StoreBuilder<?>> stores;

    final String key = "Hello";
    final String value = "World";

    @Test
    public void shouldCallInitOfAdapteeTransformer() {
        when(transformerSupplier.get()).thenReturn(transformer);

        final TransformerSupplierAdapter<String, String, Integer, Integer> adapter =
            new TransformerSupplierAdapter<>(transformerSupplier);
        final Transformer<String, String, Iterable<KeyValue<Integer, Integer>>> adaptedTransformer = adapter.get();
        adaptedTransformer.init(context);

        verify(transformer).init(context);
    }

    @Test
    public void shouldCallCloseOfAdapteeTransformer() {
        when(transformerSupplier.get()).thenReturn(transformer);

        final TransformerSupplierAdapter<String, String, Integer, Integer> adapter =
            new TransformerSupplierAdapter<>(transformerSupplier);
        final Transformer<String, String, Iterable<KeyValue<Integer, Integer>>> adaptedTransformer = adapter.get();
        adaptedTransformer.close();

        verify(transformer).close();
    }

    @Test
    public void shouldCallStoresOfAdapteeTransformerSupplier() {
        when(transformerSupplier.stores()).thenReturn(stores);

        final TransformerSupplierAdapter<String, String, Integer, Integer> adapter =
            new TransformerSupplierAdapter<>(transformerSupplier);
        adapter.stores();
    }

    @Test
    public void shouldCallTransformOfAdapteeTransformerAndReturnSingletonIterable() {
        when(transformerSupplier.get()).thenReturn(transformer);
        when(transformer.transform(key, value)).thenReturn(KeyValue.pair(0, 1));

        final TransformerSupplierAdapter<String, String, Integer, Integer> adapter =
            new TransformerSupplierAdapter<>(transformerSupplier);
        final Transformer<String, String, Iterable<KeyValue<Integer, Integer>>> adaptedTransformer = adapter.get();
        final Iterator<KeyValue<Integer, Integer>> iterator = adaptedTransformer.transform(key, value).iterator();

        assertThat(iterator.hasNext(), equalTo(true));
        iterator.next();
        assertThat(iterator.hasNext(), equalTo(false));
    }

    @Test
    public void shouldCallTransformOfAdapteeTransformerAndReturnEmptyIterable() {
        when(transformerSupplier.get()).thenReturn(transformer);
        when(transformer.transform(key, value)).thenReturn(null);

        final TransformerSupplierAdapter<String, String, Integer, Integer> adapter =
            new TransformerSupplierAdapter<>(transformerSupplier);
        final Transformer<String, String, Iterable<KeyValue<Integer, Integer>>> adaptedTransformer = adapter.get();
        final Iterator<KeyValue<Integer, Integer>> iterator = adaptedTransformer.transform(key, value).iterator();

        assertThat(iterator.hasNext(), equalTo(false));
    }

    @Test
    public void shouldAlwaysGetNewAdapterTransformer() {
        @SuppressWarnings("unchecked")
        final Transformer<String, String, KeyValue<Integer, Integer>> transformer1 = mock(Transformer.class);
        @SuppressWarnings("unchecked")
        final Transformer<String, String, KeyValue<Integer, Integer>> transformer2 = mock(Transformer.class);
        @SuppressWarnings("unchecked")
        final Transformer<String, String, KeyValue<Integer, Integer>> transformer3 = mock(Transformer.class);
        when(transformerSupplier.get()).thenReturn(transformer1).thenReturn(transformer2).thenReturn(transformer3);

        final TransformerSupplierAdapter<String, String, Integer, Integer> adapter =
            new TransformerSupplierAdapter<>(transformerSupplier);
        final Transformer<String, String, Iterable<KeyValue<Integer, Integer>>> adapterTransformer1 = adapter.get();
        adapterTransformer1.init(context);
        final Transformer<String, String, Iterable<KeyValue<Integer, Integer>>> adapterTransformer2 = adapter.get();
        adapterTransformer2.init(context);
        final Transformer<String, String, Iterable<KeyValue<Integer, Integer>>> adapterTransformer3 = adapter.get();
        adapterTransformer3.init(context);

        assertThat(adapterTransformer1, not(sameInstance(adapterTransformer2)));
        assertThat(adapterTransformer2, not(sameInstance(adapterTransformer3)));
        assertThat(adapterTransformer3, not(sameInstance(adapterTransformer1)));
        verify(transformer1).init(context);
        verify(transformer2).init(context);
        verify(transformer3).init(context);
    }

}
