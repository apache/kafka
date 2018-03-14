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

import org.apache.kafka.streams.state.StoreBuilder;

/**
 * {@code StoreUpgradeBuilder} that provides a store as well as corresponding proxy store and converter store.
 * <p>
 * The proxy store maps from a new store API to an old store API.
 * It is responsible to map from the new API calls to the old API during store upgrade phase.
 * <p>
 * The converter store is a store for the new format.
 * Additionally, it implements a {@link RecordConverter} that is used to translate from the old storage format to the new storage format.
 *
 * @param <S> store type (proxy store must implement this store type, too)
 * @param <C> new store type that additionally implements {@link RecordConverter}
 */
public interface StoreUpgradeBuilder<S extends StateStore, C extends RecordConverterStore> extends StoreBuilder<S> {

    /**
     * Return a new instance of the proxy store.
     * @return a new instance of the proxy store
     */
    S storeProxy();

    /**
     * Return a new instance of the converter store
     * @return a new instance of converter store
     */
    C converterStore();
}
