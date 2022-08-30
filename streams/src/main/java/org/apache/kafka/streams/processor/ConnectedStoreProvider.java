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
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Set;

/**
 * Provides a set of {@link StoreBuilder}s that will be automatically added to the topology and connected to the
 * associated processor.
 * <p>
 * Implementing this interface is recommended when the associated processor wants to encapsulate its usage of its state
 * stores, rather than exposing them to the user building the topology.
 * <p>
 * In the event that separate but related processors may want to share the same store, different {@link ConnectedStoreProvider}s
 * may provide the same instance of {@link StoreBuilder}, as shown below.
 * <pre>{@code
 * class StateSharingProcessors {
 *     StoreBuilder<KeyValueStore<String, String>> storeBuilder =
 *         Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("myStore"), Serdes.String(), Serdes.String());
 *
 *     class SupplierA implements ProcessorSupplier<String, Integer> {
 *         Processor<String, Integer> get() {
 *             return new Processor() {
 *                 private StateStore store;
 *
 *                 void init(ProcessorContext context) {
 *                     this.store = context.getStateStore("myStore");
 *                 }
 *
 *                 void process(String key, Integer value) {
 *                     // can access this.store
 *                 }
 *
 *                 void close() {
 *                     // can access this.store
 *                 }
 *             }
 *         }
 *
 *         Set<StoreBuilder<?>> stores() {
 *             return Collections.singleton(storeBuilder);
 *         }
 *     }
 *
 *     class SupplierB implements ProcessorSupplier<String, String> {
 *         Processor<String, String> get() {
 *             return new Processor() {
 *                 private StateStore store;
 *
 *                 void init(ProcessorContext context) {
 *                     this.store = context.getStateStore("myStore");
 *                 }
 *
 *                 void process(String key, String value) {
 *                     // can access this.store
 *                 }
 *
 *                 void close() {
 *                     // can access this.store
 *                 }
 *             }
 *         }
 *
 *         Set<StoreBuilder<?>> stores() {
 *             return Collections.singleton(storeBuilder);
 *         }
 *     }
 * }
 * }</pre>
 *
 * @see Topology#addProcessor(String, org.apache.kafka.streams.processor.api.ProcessorSupplier, String...)
 * @see KStream#process(org.apache.kafka.streams.processor.api.ProcessorSupplier, String...)
 * @see KStream#process(org.apache.kafka.streams.processor.api.ProcessorSupplier, Named, String...)
 * @see KStream#transform(TransformerSupplier, String...)
 * @see KStream#transform(TransformerSupplier, Named, String...)
 * @see KStream#transformValues(ValueTransformerSupplier, String...)
 * @see KStream#transformValues(ValueTransformerSupplier, Named, String...)
 * @see KStream#transformValues(ValueTransformerWithKeySupplier, String...)
 * @see KStream#transformValues(ValueTransformerWithKeySupplier, Named, String...)
 * @see KStream#flatTransform(TransformerSupplier, String...)
 * @see KStream#flatTransform(TransformerSupplier, Named, String...)
 * @see KStream#flatTransformValues(ValueTransformerSupplier, String...)
 * @see KStream#flatTransformValues(ValueTransformerSupplier, Named, String...)
 * @see KStream#flatTransformValues(ValueTransformerWithKeySupplier, String...)
 * @see KStream#flatTransformValues(ValueTransformerWithKeySupplier, Named, String...)
 */
public interface ConnectedStoreProvider {

    /**
     * @return the state stores to be connected and added, or null if no stores should be automatically connected and added.
     */
    default Set<StoreBuilder<?>> stores() {
        return null;
    }
}