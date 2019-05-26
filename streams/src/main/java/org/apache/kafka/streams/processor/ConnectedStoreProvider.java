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
package org.apache.kafka.streams.processor;

import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

/**
 * Provides a set of {@link StoreBuilder}s that will be automatically added to the topology and connected to the
 * associated processor.
 * The implementor of this interface must also implement one of {@link ProcessorSupplier},
 * {@link TransformerSupplier}, {@link ValueTransformerSupplier}, or {@link ValueTransformerWithKeySupplier},
 * <p>
 * Different {@link ConnectedStoreProvider}s may provide the same instance of {@link StoreBuilder}, so that multiple
 * processors may access the same store, as shown below.
 * <pre>{@code
 * class StateSharingProcessors {
 *     StoreBuilder<KeyValueStore<String, String>> stringStoreBuilder =
 *         Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("stringStore"), Serdes.String(), Serdes.String());
 *     StoreBuilder<KeyValueStore<String, Integer>> integerStoreBuilder =
 *         Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("integerStore"), Serdes.String(), Serdes.Integer());
 *
 *     class SupplierA implements ProcessorSupplier<String, Integer>, ConnectedStoreProvider {
 *         Processor<String, Integer> get() {
 *             return new Processor() {
 *                 private StateStore stringStore;
 *                 private StateStore integerStore;
 *
 *                 void init(ProcessorContext context) {
 *                     this.stringStore = context.getStateStore("stringStore");
 *                     this.integerStore = context.getStateStore("integerStore");
 *                 }
 *
 *                 void process(String key, Integer value) {
 *                     // can access this.stringStore and this.integerStore
 *                 }
 *
 *                 void close() {
 *                     // can access this.stringStore and this.integerStore
 *                 }
 *             }
 *         }
 *
 *         Set<StoreBuilder> stores() {
 *             Set<StoreBuilder> stores = new HashSet<>();
 *             stores.add(stringStoreBuilder);
 *             stores.add(integerStoreBuilder);
 *             return stores;
 *         }
 *     }
 *
 *     class SupplierB implements ProcessorSupplier<String, String>, ConnectedStoreProvider {
 *         Processor<String, String> get() {
 *             return new Processor() {
 *                 private StateStore stringStore;
 *                 private StateStore integerStore;
 *
 *                 void init(ProcessorContext context) {
 *                     this.stringStore = context.getStateStore("stringStore");
 *                     this.integerStore = context.getStateStore("integerStore");
 *                 }
 *
 *                 void process(String key, String value) {
 *                     // can access this.stringStore and this.integerStore
 *                 }
 *
 *                 void close() {
 *                     // can access this.stringStore and this.integerStore
 *                 }
 *             }
 *         }
 *
 *         Set<StoreBuilder> stores() {
 *             Set<StoreBuilder> stores = new HashSet<>();
 *             stores.add(stringStoreBuilder);
 *             stores.add(integerStoreBuilder);
 *             return stores;
 *         }
 *     }
 * }
 * }</pre>
 * @see Topology#addProcessor(String, ProcessorSupplier, String...)
 * @see KStream#process(ProcessorSupplier, String...)
 * @see KStream#transform(TransformerSupplier, String...)
 * @see KStream#transformValues(ValueTransformerSupplier, String...)
 * @see KStream#transformValues(ValueTransformerWithKeySupplier, String...)
 * @see KStream#flatTransform(TransformerSupplier, String...)
 * @see KStream#flatTransformValues(ValueTransformerSupplier, String...)
 * @see KStream#flatTransformValues(ValueTransformerWithKeySupplier, String...)
 */
public interface ConnectedStoreProvider {

    /**
     * @return the state stores
     */
    Set<StoreBuilder> stores();
}
